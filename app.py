"""
AlgoFO Backend — WebSocket + REST
- Connects to Upstox WebSocket for real-time tick data
- Broadcasts live data to the frontend via its own WebSocket
- REST endpoints for option chain, positions, orders (not available on WS)
"""
import os, sys, time, requests, threading, json, struct, logging
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sock import Sock
import websocket

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("algofo")

app = Flask(__name__)
CORS(app, origins="*")
sock = Sock(app)

BASE = "https://api.upstox.com/v2"

# ── Shared live state ─────────────────────────────────────────────────────────
_state = {
    "ltp": {},          # instrument_key -> last price
    "ohlc": {},         # instrument_key -> {open,high,low,close}
    "oi": {},           # instrument_key -> open interest
    "volume": {},       # instrument_key -> volume
    "last_tick": 0,     # epoch of last WS tick
    "ws_connected": False,
}
_state_lock = threading.Lock()
_clients = set()        # connected frontend WebSocket clients
_clients_lock = threading.Lock()

# ── Cache for REST endpoints ──────────────────────────────────────────────────
_cache = {}
_cache_lock = threading.Lock()

def cache_get(key):
    with _cache_lock:
        c = _cache.get(key)
        if c and time.time() < c["exp"]:
            return c["val"]
    return None

def cache_set(key, val, ttl=5):
    with _cache_lock:
        _cache[key] = {"val": val, "exp": time.time() + ttl}

# ── Upstox REST helper ────────────────────────────────────────────────────────
def up(path, token, ttl=5):
    if ttl > 0:
        cached = cache_get(path)
        if cached is not None:
            return cached
    h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    r = requests.get(BASE + path, headers=h, timeout=10)
    r.raise_for_status()
    data = r.json()
    if ttl > 0:
        cache_set(path, data, ttl)
    return data

def up_post(path, token, body):
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
    r = requests.post(BASE + path, headers=h, json=body, timeout=10)
    return r.json()

def tok():
    return request.headers.get("Authorization", "").replace("Bearer ", "").strip()

# ── Upstox WebSocket connection ───────────────────────────────────────────────
_ws_thread = None
_ws_token = None
_ws_instruments = []
_ws_app = None

def decode_upstox_message(data):
    """Decode Upstox binary WebSocket message (protobuf-lite)"""
    try:
        # Try JSON first (some messages are JSON)
        return json.loads(data)
    except:
        pass
    # Binary protobuf — basic field extraction
    # Upstox sends: type=MarketFullFeed with LTP at specific offsets
    try:
        if isinstance(data, bytes) and len(data) > 4:
            # Return raw for now - implement full protobuf decode if needed
            return {"raw": True, "len": len(data)}
    except:
        pass
    return None

def broadcast_to_clients(msg):
    """Send data to all connected frontend WebSocket clients"""
    dead = set()
    with _clients_lock:
        clients = set(_clients)
    for client in clients:
        try:
            client.send(json.dumps(msg))
        except:
            dead.add(client)
    if dead:
        with _clients_lock:
            _clients.difference_update(dead)

def get_ws_auth_url_v3(token):
    """Get authorized WebSocket URL V3 (V2 discontinued Aug 2025)"""
    h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        # Try V3 first
        r = requests.get(f"{BASE}/feed/market-data-feed-v3/authorize", headers=h, timeout=10)
        data = r.json()
        url = data.get("data", {}).get("authorized_redirect_uri")
        if url:
            log.info(f"Got V3 WS URL")
            return url
    except Exception as e:
        log.error(f"V3 auth error: {e}")
    # Fallback to v2 (may still work for some accounts)
    try:
        r = requests.get(f"{BASE}/feed/market-data-feed/authorize", headers=h, timeout=10)
        data = r.json()
        return data.get("data", {}).get("authorized_redirect_uri")
    except:
        return None

def get_ws_auth_url(token):
    return get_ws_auth_url_v3(token)

def subscribe_instruments(ws_conn, instruments):
    """Send subscription message to Upstox WebSocket"""
    msg = {
        "guid": str(time.time()),
        "method": "sub",
        "data": {
            "mode": "full",
            "instrumentKeys": instruments
        }
    }
    ws_conn.send(json.dumps(msg))

def on_ws_message(ws_conn, message):
    global _state
    try:
        # Parse Upstox message
        if isinstance(message, bytes):
            # Binary protobuf — parse key fields
            # For now broadcast raw size info and use REST for details
            with _state_lock:
                _state["last_tick"] = time.time()
                _state["ws_connected"] = True
            broadcast_to_clients({"type": "tick", "ts": time.time()})
        else:
            data = json.loads(message)
            feeds = data.get("feeds", {})
            update = {"type": "ltp", "data": {}}
            for key, feed in feeds.items():
                ltp_val = feed.get("ff", {}).get("marketFF", {}).get("ltpc", {}).get("ltp", 0)
                oi_val = feed.get("ff", {}).get("marketFF", {}).get("oi", 0)
                vol = feed.get("ff", {}).get("marketFF", {}).get("v", 0)
                if ltp_val:
                    with _state_lock:
                        _state["ltp"][key] = ltp_val
                        _state["oi"][key] = oi_val
                        _state["volume"][key] = vol
                        _state["last_tick"] = time.time()
                        _state["ws_connected"] = True
                    update["data"][key] = {"ltp": ltp_val, "oi": oi_val, "volume": vol}
            if update["data"]:
                broadcast_to_clients(update)
    except Exception as e:
        log.error(f"WS message error: {e}")

def on_ws_error(ws_conn, error):
    log.error(f"Upstox WS error: {error}")
    with _state_lock:
        _state["ws_connected"] = False

def on_ws_close(ws_conn, close_status_code, close_msg):
    log.info(f"Upstox WS closed: {close_status_code}")
    with _state_lock:
        _state["ws_connected"] = False
    # Auto-reconnect with backoff (15s to avoid spam)
    global _ws_reconnect_count
    _ws_reconnect_count = getattr(sys.modules[__name__], '_ws_reconnect_count', 0) + 1
    delay = min(60, 15 * _ws_reconnect_count)
    log.info(f"WS reconnecting in {delay}s (attempt {_ws_reconnect_count})")
    if _ws_token:
        threading.Timer(delay, lambda: start_websocket(_ws_token, _ws_instruments)).start()

def on_ws_open(ws_conn):
    log.info("Upstox WS connected ✅")
    with _state_lock:
        _state["ws_connected"] = True
    if _ws_instruments:
        subscribe_instruments(ws_conn, _ws_instruments)

def start_websocket(token, instruments):
    global _ws_app, _ws_token, _ws_instruments
    _ws_token = token
    _ws_instruments = instruments
    try:
        ws_url = get_ws_auth_url_v3(token)
        if not ws_url:
            ws_url = get_ws_auth_url(token)
        if not ws_url:
            log.error("Could not get WS auth URL")
            return
        log.info(f"Connecting to Upstox WS: {ws_url[:60]}...")
        _ws_app = websocket.WebSocketApp(
            ws_url,
            on_open=on_ws_open,
            on_message=on_ws_message,
            on_error=on_ws_error,
            on_close=on_ws_close,
        )
        _ws_app.run_forever(ping_interval=30, ping_timeout=10)
    except Exception as e:
        log.error(f"WS start error: {e}")

# ── Frontend WebSocket endpoint ───────────────────────────────────────────────
@sock.route("/ws")
def frontend_ws(ws):
    """Frontend connects here to get real-time updates"""
    with _clients_lock:
        _clients.add(ws)
    log.info(f"Frontend WS connected. Total: {len(_clients)}")
    try:
        # Send current state immediately on connect
        with _state_lock:
            state_copy = dict(_state)
        ws.send(json.dumps({"type": "state", "data": state_copy}))
        # Keep alive — receive messages (ping/subscribe requests)
        while True:
            msg = ws.receive(timeout=60)
            if msg is None:
                break
            try:
                data = json.loads(msg)
                if data.get("type") == "ping":
                    ws.send(json.dumps({"type": "pong"}))  # Respond to keepalive
                elif data.get("type") == "subscribe" and data.get("token"):
                    instruments = data.get("instruments", [])
                    threading.Thread(
                        target=start_websocket,
                        args=(data["token"], instruments),
                        daemon=True
                    ).start()
                    ws.send(json.dumps({"type": "subscribed", "instruments": instruments}))
            except:
                pass
    except Exception as e:
        log.info(f"Frontend WS disconnected: {e}")
    finally:
        with _clients_lock:
            _clients.discard(ws)

# ── REST Routes ───────────────────────────────────────────────────────────────

@app.route("/")
def home():
    """Serve the frontend app if index.html exists, otherwise return status"""
    import os
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    if os.path.exists(html_path):
        from flask import send_file
        return send_file(html_path, mimetype="text/html")
    with _state_lock:
        ws_status = _state["ws_connected"]
        last_tick = _state["last_tick"]
    return jsonify({
        "status": "AlgoFO Server running",
        "ws_connected": ws_status,
        "last_tick": last_tick,
        "clients": len(_clients),
        "time": time.time()
    })

@app.route("/api/health")
def health():
    with _state_lock:
        return jsonify({
            "status": "ok",
            "ws_connected": _state["ws_connected"],
            "last_tick_ago": round(time.time() - _state["last_tick"], 1) if _state["last_tick"] else None,
            "frontend_clients": len(_clients),
            "time": time.time()
        })

@app.route("/api/ws-start", methods=["POST"])
def ws_start():
    """Start WebSocket connection with given token and instruments"""
    global _ws_thread
    data = request.json or {}
    token = data.get("token") or tok()
    instruments = data.get("instruments", [
        "NSE_INDEX|Nifty 50",
        "NSE_INDEX|Nifty Bank",
        "BSE_INDEX|SENSEX"
    ])
    if not token:
        return jsonify({"error": "No token"}), 400
    if _ws_thread and _ws_thread.is_alive():
        # Update instruments on existing connection
        if _ws_app:
            subscribe_instruments(_ws_app, instruments)
        return jsonify({"status": "already running", "instruments": instruments})
    _ws_thread = threading.Thread(
        target=start_websocket,
        args=(token, instruments),
        daemon=True
    )
    _ws_thread.start()
    return jsonify({"status": "started", "instruments": instruments})

@app.route("/api/ws-status")
def ws_status():
    with _state_lock:
        return jsonify({
            "connected": _state["ws_connected"],
            "last_tick": _state["last_tick"],
            "ltp_count": len(_state["ltp"]),
            "ltp": _state["ltp"]
        })

@app.route("/api/ltp")
def ltp():
    keys = request.args.get("instrument_key", "NSE_INDEX|Nifty 50")
    # Return from WS cache first (real-time), fallback to REST
    with _state_lock:
        ws_ltp = {k: v for k, v in _state["ltp"].items() if k in keys}
    if ws_ltp and _state["ws_connected"]:
        # Format like Upstox REST response
        return jsonify({"status": "success", "data": {k: {"last_price": v} for k, v in ws_ltp.items()}, "source": "websocket"})
    try:
        return jsonify(up(f"/market-quote/ltp?instrument_key={keys}", tok(), ttl=1))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/chain")
def chain():
    key = request.args.get("instrument_key", "")
    expiry = request.args.get("expiry_date", "")
    t = tok()
    try:
        # Try with the key as-is first
        data = up(f"/option/chain?instrument_key={key}&expiry_date={expiry}", t, ttl=0)
        # If empty data, log it for debugging
        if data.get("status") == "success" and not data.get("data"):
            log.warning(f"Empty chain for key={key} expiry={expiry}")
            # Try URL-encoded version
            from urllib.parse import quote
            encoded_key = quote(key, safe="")
            data2 = up(f"/option/chain?instrument_key={encoded_key}&expiry_date={expiry}", t, ttl=0)
            if data2.get("data"):
                return jsonify(data2)
        return jsonify(data)
    except Exception as e:
        log.error(f"Chain error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/chain")
def debug_chain():
    """Test endpoint to diagnose chain issues"""
    key = request.args.get("instrument_key", "NSE_INDEX|Nifty Bank")
    expiry = request.args.get("expiry_date", "2026-05-28")
    t = tok()
    result = {"key_used": key, "expiry": expiry}
    try:
        # Test 1: Option contracts to verify key works
        contracts = up(f"/option/contract?instrument_key={key}", t, ttl=0)
        result["contracts_count"] = len(contracts.get("data", []))
        result["contracts_status"] = contracts.get("status")
        if contracts.get("data"):
            sample = contracts["data"][0]
            result["sample_underlying_key"] = sample.get("underlying_key")
            result["sample_expiry"] = sample.get("expiry")
            result["sample_strike"] = sample.get("strike_price")
        # Test 2: Option chain
        chain_data = up(f"/option/chain?instrument_key={key}&expiry_date={expiry}", t, ttl=0)
        result["chain_status"] = chain_data.get("status")
        result["chain_count"] = len(chain_data.get("data", []))
        if chain_data.get("data"):
            first = chain_data["data"][0]
            result["first_strike"] = first.get("strike_price")
            result["underlying_spot"] = first.get("underlying_spot_price")
    except Exception as e:
        result["error"] = str(e)
    return jsonify(result)

@app.route("/api/expiries")
def expiries():
    key = request.args.get("instrument_key", "")
    try:
        return jsonify(up(f"/option/contract?instrument_key={key}", tok(), ttl=60))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/positions")
def positions():
    try:
        return jsonify(up("/portfolio/short-term-positions", tok(), ttl=2))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/orders")
def orders():
    try:
        return jsonify(up("/order/retrieve-all", tok(), ttl=2))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/funds")
def funds():
    try:
        return jsonify(up("/user/get-funds-and-margin", tok(), ttl=5))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/order", methods=["POST"])
def place_order():
    try:
        return jsonify(up_post("/order/place", tok(), request.json))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/market")
def full_market():
    t = tok()
    key = request.args.get("instrument_key", "NSE_INDEX|Nifty 50")
    expiry = request.args.get("expiry_date", "")
    idx_keys = "NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank,BSE_INDEX|SENSEX"
    result = {}
    def fetch(name, path, ttl):
        try: result[name] = up(path, t, ttl)
        except Exception as e: result[f"{name}_error"] = str(e)
    threads = [
        threading.Thread(target=fetch, args=("ltp", f"/market-quote/ltp?instrument_key={idx_keys}", 1)),
        threading.Thread(target=fetch, args=("positions", "/portfolio/short-term-positions", 2)),
        threading.Thread(target=fetch, args=("funds", "/user/get-funds-and-margin", 5)),
    ]
    if expiry:
        threads.append(threading.Thread(target=fetch, args=("chain", f"/option/chain?instrument_key={key}&expiry_date={expiry}", 5)))
    for th in threads: th.start()
    for th in threads: th.join(timeout=8)
    # Add WS live prices if available
    with _state_lock:
        if _state["ws_connected"] and _state["ltp"]:
            result["ws_ltp"] = _state["ltp"]
            result["ws_connected"] = True
    return jsonify(result)

def self_ping():
    """Ping self every 8 min to prevent Render free tier spin-down"""
    import time as t
    t.sleep(30)  # Wait for server to start
    # Try multiple URL sources
    possible_urls = [
        os.environ.get("RENDER_EXTERNAL_URL", ""),
        os.environ.get("RENDER_EXTERNAL_HOSTNAME", ""),
        "https://algofo-server.onrender.com",  # Hardcoded fallback
    ]
    own_url = next((u for u in possible_urls if u and "localhost" not in u), "https://algofo-server.onrender.com")
    if not own_url.startswith("http"):
        own_url = f"https://{own_url}"
    log.info(f"Self-ping URL: {own_url}")
    while True:
        try:
            r = requests.get(f"{own_url}/api/health", timeout=15)
            log.info(f"Self-ping OK: {r.status_code}")
        except Exception as e:
            log.warning(f"Self-ping failed: {e}")
        t.sleep(480)  # Every 8 minutes (Render spins down after 15 min)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    # Start self-ping thread to prevent spin-down
    ping_thread = threading.Thread(target=self_ping, daemon=True)
    ping_thread.start()
    app.run(host="0.0.0.0", port=port, debug=False)
