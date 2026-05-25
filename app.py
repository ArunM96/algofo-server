"""
AlgoFO Backend - Enhanced for MilesWeb VPS Deployment
Improvements:
1. MilesWeb VPS-optimized configuration
2. Enhanced Upstox WebSocket V3 API support
3. Better error handling & reconnection logic
4. Production-grade logging & monitoring
5. Performance optimizations
6. Graceful shutdown handling
"""

import os
import sys
import time
import requests
import threading
import json
import logging
from datetime import datetime, timedelta
from collections import deque, defaultdict
from functools import wraps

from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sock import Sock
import websocket

# ════════════════════════════════════════════════════════════════════════════
# CONFIGURATION FOR MILESWEB VPS
# ════════════════════════════════════════════════════════════════════════════

class Config:
    """Production configuration for MilesWeb VPS"""
    
    # Server
    HOST = os.environ.get('HOST', '0.0.0.0')
    PORT = int(os.environ.get('PORT', 8080))
    DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'
    ENV = os.environ.get('FLASK_ENV', 'production')
    
    # Upstox API
    UPSTOX_BASE_URL = 'https://api.upstox.com/v2'
    UPSTOX_API_KEY = os.environ.get('UPSTOX_API_KEY')
    UPSTOX_API_SECRET = os.environ.get('UPSTOX_API_SECRET')
    UPSTOX_ACCESS_TOKEN = os.environ.get('UPSTOX_ACCESS_TOKEN')
    
    # WebSocket Settings
    WS_PING_INTERVAL = int(os.environ.get('WS_PING_INTERVAL', 30))
    WS_PING_TIMEOUT = int(os.environ.get('WS_PING_TIMEOUT', 10))
    WS_BATCH_SIZE = int(os.environ.get('WS_BATCH_SIZE', 50))
    WS_MAX_BUFFER = int(os.environ.get('WS_MAX_BUFFER', 10000))
    WS_RECONNECT_DELAY = int(os.environ.get('WS_RECONNECT_DELAY', 5))
    WS_MAX_RECONNECT_ATTEMPTS = int(os.environ.get('WS_MAX_RECONNECT_ATTEMPTS', 10))
    
    # Performance
    REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', 10))
    CACHE_TTL = int(os.environ.get('CACHE_TTL', 5))
    
    # Logging
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.environ.get('LOG_FILE', 'logs/algofo.log')
    
    # Self-ping for MilesWeb (prevent spin-down on free tier)
    SELF_PING_ENABLED = os.environ.get('SELF_PING_ENABLED', 'true').lower() == 'true'
    SELF_PING_INTERVAL = int(os.environ.get('SELF_PING_INTERVAL', 480))  # 8 minutes

# ════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ════════════════════════════════════════════════════════════════════════════

def setup_logging():
    """Setup logging for production VPS"""
    os.makedirs('logs', exist_ok=True)
    
    log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format=log_format,
        handlers=[
            logging.FileHandler(Config.LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

log = setup_logging()

# ════════════════════════════════════════════════════════════════════════════
# FLASK APP INITIALIZATION
# ════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)
CORS(app, origins="*")
sock = Sock(app)

log.info(f"""
╔════════════════════════════════════════════════════════════════╗
║     AlgoFO Trading Bot - Enhanced for MilesWeb VPS             ║
║     Environment: {Config.ENV:<45}║
║     Host: {Config.HOST:<52}║
║     Port: {Config.PORT:<53}║
╚════════════════════════════════════════════════════════════════╝
""")

# ════════════════════════════════════════════════════════════════════════════
# SHARED STATE & THREAD SAFETY
# ════════════════════════════════════════════════════════════════════════════

_state = {
    "ltp": defaultdict(lambda: {
        "price": 0, "oi": 0, "vol": 0, "bid": 0, "ask": 0, 
        "timestamp": 0, "feed_type": ""
    }),
    "ohlc": defaultdict(lambda: {"open": 0, "high": 0, "low": 0, "close": 0}),
    "subscribed_instruments": set(),
    "ws_connected": False,
    "ws_authenticated": False,
    "last_tick_time": 0,
    "tick_count": 0,
    "batch_count": 0,
    "error_count": 0,
    "latencies": deque(maxlen=100),
    "last_ws_message_time": time.time(),
}

_state_lock = threading.RLock()
_clients = set()
_clients_lock = threading.Lock()

# ════════════════════════════════════════════════════════════════════════════
# CACHE LAYER
# ════════════════════════════════════════════════════════════════════════════

_cache = {}
_cache_lock = threading.Lock()

def cache_get(key):
    """Get cached value if not expired"""
    with _cache_lock:
        cached = _cache.get(key)
        if cached and time.time() < cached["exp"]:
            return cached["val"]
    return None

def cache_set(key, val, ttl=None):
    """Cache value with TTL"""
    ttl = ttl or Config.CACHE_TTL
    with _cache_lock:
        _cache[key] = {"val": val, "exp": time.time() + ttl}

def cache_clear():
    """Clear all cache"""
    with _cache_lock:
        _cache.clear()

# ════════════════════════════════════════════════════════════════════════════
# UPSTOX API HELPERS
# ════════════════════════════════════════════════════════════════════════════

def get_token():
    """Extract auth token from request"""
    token = request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    return token if token and len(token) > 20 else None

def upstox_call(path, method="GET", body=None, token=None, ttl=None):
    """Make authenticated Upstox API call with caching"""
    if not token:
        token = get_token()
    
    if not token:
        return {"error": "No auth token", "status": "error"}
    
    # Check cache
    ttl = ttl if ttl is not None else Config.CACHE_TTL
    if ttl > 0:
        cached = cache_get(f"{method}:{path}")
        if cached:
            return cached
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    try:
        url = f"{Config.UPSTOX_BASE_URL}{path}"
        
        if method == "GET":
            resp = requests.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
        elif method == "POST":
            resp = requests.post(url, headers=headers, json=body, timeout=Config.REQUEST_TIMEOUT)
        else:
            return {"error": "Invalid method"}
        
        resp.raise_for_status()
        data = resp.json()
        
        if ttl > 0:
            cache_set(f"{method}:{path}", data, ttl)
        
        return data
    
    except requests.exceptions.RequestException as e:
        log.error(f"API call failed: {e}")
        with _state_lock:
            _state["error_count"] += 1
        return {"error": str(e), "status": "error"}

# ════════════════════════════════════════════════════════════════════════════
# UPSTOX WEBSOCKET MANAGER
# ════════════════════════════════════════════════════════════════════════════

_ws_thread = None
_ws_token = None
_ws_instruments = []
_ws_app = None
_ws_reconnect_count = 0
_ws_last_message = time.time()

def get_ws_auth_url(token):
    """Get WebSocket authorization URL from Upstox V3 API"""
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        
        # Try V3 API first
        resp = requests.get(
            f"{Config.UPSTOX_BASE_URL}/feed/market-data-feed-v3/authorize",
            headers=headers,
            timeout=Config.REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()
        
        ws_url = data.get("data", {}).get("authorized_redirect_uri")
        if ws_url:
            log.info("✓ Got WebSocket URL from Upstox V3 API")
            return ws_url
    
    except Exception as e:
        log.error(f"V3 auth failed: {e}")
    
    # Fallback to V2 API (if V3 fails)
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        resp = requests.get(
            f"{Config.UPSTOX_BASE_URL}/feed/market-data-feed/authorize",
            headers=headers,
            timeout=Config.REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()
        ws_url = data.get("data", {}).get("authorized_redirect_uri")
        
        if ws_url:
            log.info("✓ Got WebSocket URL from Upstox V2 API (fallback)")
            return ws_url
    
    except Exception as e:
        log.error(f"V2 auth failed: {e}")
    
    return None

def subscribe_to_ws(ws_conn, instruments):
    """Send subscription message to Upstox WebSocket"""
    if not ws_conn or not instruments:
        return
    
    msg = {
        "guid": f"{time.time()}-{hash(tuple(instruments))}",
        "method": "sub",
        "data": {
            "mode": "full",
            "instrumentKeys": instruments
        }
    }
    
    try:
        ws_conn.send(json.dumps(msg))
        with _state_lock:
            _state["subscribed_instruments"] = set(instruments)
        log.info(f"📡 Subscribed to {len(instruments)} instruments")
        return True
    except Exception as e:
        log.error(f"Subscribe failed: {e}")
        return False

def broadcast_to_clients(msg):
    """Broadcast message to all connected frontend WebSocket clients"""
    dead = set()
    with _clients_lock:
        clients = set(_clients)
    
    for client in clients:
        try:
            client.send(json.dumps(msg))
        except Exception:
            dead.add(client)
    
    if dead:
        with _clients_lock:
            _clients.difference_update(dead)

def on_ws_message(ws_conn, message):
    """Handle incoming WebSocket message from Upstox"""
    global _ws_last_message
    
    _ws_last_message = time.time()
    
    try:
        # Parse JSON message
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message
        
        feeds = data.get("feeds", {})
        if not feeds:
            return
        
        update = {
            "type": "ltp_update",
            "data": {},
            "ts": time.time(),
            "count": len(feeds)
        }
        
        with _state_lock:
            _state["ws_connected"] = True
            _state["last_tick_time"] = time.time()
            _state["tick_count"] += len(feeds)
        
        for instrument_key, feed_data in feeds.items():
            try:
                # Extract market data
                ff = feed_data.get("ff", {})
                market_ff = ff.get("marketFF", {})
                ltpc = market_ff.get("ltpc", {})
                
                ltp = ltpc.get("ltp", 0)
                oi = market_ff.get("oi", 0)
                vol = market_ff.get("v", 0)
                bid = market_ff.get("bid", 0)
                ask = market_ff.get("ask", 0)
                
                with _state_lock:
                    _state["ltp"][instrument_key] = {
                        "price": ltp,
                        "oi": oi,
                        "vol": vol,
                        "bid": bid,
                        "ask": ask,
                        "timestamp": time.time(),
                        "feed_type": feed_data.get("type", "full")
                    }
                
                update["data"][instrument_key] = {
                    "ltp": ltp,
                    "oi": oi,
                    "volume": vol,
                    "bid": bid,
                    "ask": ask
                }
            
            except Exception as e:
                log.error(f"Feed parsing error: {e}")
                with _state_lock:
                    _state["error_count"] += 1
        
        if update["data"]:
            broadcast_to_clients(update)
    
    except Exception as e:
        log.error(f"Message processing error: {e}")
        with _state_lock:
            _state["error_count"] += 1

def on_ws_error(ws_conn, error):
    """Handle WebSocket error"""
    log.error(f"WebSocket error: {error}")
    with _state_lock:
        _state["ws_connected"] = False
        _state["error_count"] += 1

def on_ws_close(ws_conn, close_status_code, close_msg):
    """Handle WebSocket closure and auto-reconnect"""
    global _ws_reconnect_count, _ws_app
    
    log.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
    
    with _state_lock:
        _state["ws_connected"] = False
    
    _ws_reconnect_count += 1
    
    if _ws_reconnect_count <= Config.WS_MAX_RECONNECT_ATTEMPTS and _ws_token:
        delay = min(Config.WS_RECONNECT_DELAY * (_ws_reconnect_count ** 2), 60)
        log.info(f"Reconnecting in {delay}s (attempt {_ws_reconnect_count}/{Config.WS_MAX_RECONNECT_ATTEMPTS})")
        threading.Timer(delay, lambda: start_ws_connection(_ws_token, list(_ws_instruments))).start()
    else:
        log.error("Max reconnection attempts exceeded")
    
    _ws_app = None

def on_ws_open(ws_conn):
    """Handle WebSocket connection success"""
    global _ws_reconnect_count
    
    log.info("✅ WebSocket connected")
    
    with _state_lock:
        _state["ws_connected"] = True
        _state["ws_authenticated"] = True
    
    _ws_reconnect_count = 0
    
    if _ws_instruments:
        subscribe_to_ws(ws_conn, list(_ws_instruments))

def start_ws_connection(token, instruments):
    """Start Upstox WebSocket connection in background thread"""
    global _ws_app, _ws_token, _ws_instruments, _ws_reconnect_count
    
    if not token or len(token) < 20:
        log.error("Invalid token for WebSocket")
        return
    
    _ws_token = token
    _ws_instruments = instruments
    
    try:
        log.info("Connecting to Upstox WebSocket...")
        
        # Get auth URL
        ws_url = get_ws_auth_url(token)
        if not ws_url:
            raise ValueError("Failed to get WebSocket URL from Upstox API")
        
        # Create WebSocket connection
        _ws_app = websocket.WebSocketApp(
            ws_url,
            on_open=on_ws_open,
            on_message=on_ws_message,
            on_error=on_ws_error,
            on_close=on_ws_close
        )
        
        log.info(f"WebSocket URL: {ws_url[:80]}...")
        _ws_app.run_forever(
            ping_interval=Config.WS_PING_INTERVAL,
            ping_timeout=Config.WS_PING_TIMEOUT
        )
    
    except Exception as e:
        log.error(f"WebSocket connection failed: {e}")
        with _state_lock:
            _state["ws_connected"] = False
        on_ws_close(None, 1006, str(e))

# ════════════════════════════════════════════════════════════════════════════
# REST API ENDPOINTS
# ════════════════════════════════════════════════════════════════════════════

@app.route('/', methods=['GET'])
def index():
    """Root endpoint"""
    html_path = os.path.join(os.path.dirname(__file__), 'index.html')
    if os.path.exists(html_path):
        from flask import send_file
        return send_file(html_path, mimetype='text/html')
    
    with _state_lock:
        return jsonify({
            "service": "AlgoFO Trading Bot",
            "version": "2.0-VPS",
            "status": "running",
            "environment": Config.ENV,
            "ws_connected": _state["ws_connected"],
            "tick_count": _state["tick_count"],
            "endpoints": {
                "ws": "ws://YOUR_VPS_IP:8080/ws",
                "health": "GET /api/health",
                "stats": "GET /api/stats",
                "ws_start": "POST /api/ws-start",
                "ws_status": "GET /api/ws-status",
                "ltp": "GET /api/ltp",
                "positions": "GET /api/positions",
                "orders": "GET /api/orders",
                "place_order": "POST /api/order"
            }
        })

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    with _state_lock:
        now = time.time()
        time_since_tick = now - _state["last_tick_time"] if _state["last_tick_time"] else None
        
        is_healthy = (
            _state["ws_connected"] and 
            (time_since_tick is None or time_since_tick < 15) and
            _state["error_count"] < 100
        )
        
        return jsonify({
            "status": "ok" if is_healthy else "degraded",
            "ws_connected": _state["ws_connected"],
            "ws_authenticated": _state["ws_authenticated"],
            "last_tick_ago_s": round(time_since_tick, 2) if time_since_tick else None,
            "tick_count": _state["tick_count"],
            "error_count": _state["error_count"],
            "clients": len(_clients),
            "instruments": len(_state["subscribed_instruments"]),
            "timestamp": now
        }), 200 if is_healthy else 503

@app.route('/api/stats', methods=['GET'])
def stats():
    """Get performance statistics"""
    with _state_lock:
        avg_latency = (
            sum(_state["latencies"]) / len(_state["latencies"]) 
            if _state["latencies"] else 0
        )
        
        return jsonify({
            "tick_count": _state["tick_count"],
            "batch_count": _state["batch_count"],
            "error_count": _state["error_count"],
            "ws_connected": _state["ws_connected"],
            "clients": len(_clients),
            "subscribed": len(_state["subscribed_instruments"]),
            "avg_latency_ms": round(avg_latency, 2),
            "uptime_s": time.time() - app.start_time if hasattr(app, 'start_time') else 0,
            "timestamp": time.time()
        })

@app.route('/api/ws-start', methods=['POST'])
def ws_start():
    """Start WebSocket connection"""
    global _ws_thread
    
    data = request.json or {}
    token = data.get("token") or get_token()
    instruments = data.get("instruments", [
        "NSE_INDEX|Nifty 50",
        "NSE_INDEX|Nifty Bank",
        "BSE_INDEX|SENSEX"
    ])
    
    if not token:
        return jsonify({"error": "No auth token"}), 401
    
    if _ws_thread and _ws_thread.is_alive():
        if _ws_app:
            subscribe_to_ws(_ws_app, instruments)
        return jsonify({
            "status": "already running",
            "instruments": instruments,
            "connected": _state["ws_connected"]
        })
    
    _ws_thread = threading.Thread(
        target=start_ws_connection,
        args=(token, instruments),
        daemon=True
    )
    _ws_thread.start()
    
    return jsonify({
        "status": "starting",
        "instruments": instruments,
        "message": "WebSocket connection initializing..."
    })

@app.route('/api/ws-status', methods=['GET'])
def ws_status():
    """Get WebSocket status and LTP data"""
    with _state_lock:
        return jsonify({
            "connected": _state["ws_connected"],
            "authenticated": _state["ws_authenticated"],
            "last_tick_ago_s": round(time.time() - _state["last_tick_time"], 2) if _state["last_tick_time"] else None,
            "tick_count": _state["tick_count"],
            "error_count": _state["error_count"],
            "instruments": list(_state["subscribed_instruments"]),
            "ltp": {k: v for k, v in dict(_state["ltp"]).items()}
        })

@app.route('/api/ltp', methods=['GET'])
def ltp_endpoint():
    """Get LTP for specified instruments"""
    keys = request.args.get("instrument_key", "NSE_INDEX|Nifty 50")
    token = get_token()
    
    # Try WebSocket cache first (real-time)
    with _state_lock:
        if _state["ws_connected"]:
            ws_data = {k: v for k, v in dict(_state["ltp"]).items() if k in keys}
            if ws_data:
                return jsonify({
                    "status": "success",
                    "data": {k: {"last_price": v["price"]} for k, v in ws_data.items()},
                    "source": "websocket"
                })
    
    # Fallback to REST API
    if not token:
        return jsonify({"error": "No token, WebSocket not connected"}), 401
    
    return jsonify(upstox_call(f"/market-quote/ltp?instrument_key={keys}", token=token, ttl=1))

@app.route('/api/chain', methods=['GET'])
def option_chain():
    """Get option chain"""
    key = request.args.get("instrument_key", "")
    expiry = request.args.get("expiry_date", "")
    
    if not key or not expiry:
        return jsonify({"error": "instrument_key and expiry_date required"}), 400
    
    return jsonify(upstox_call(
        f"/option/chain?instrument_key={key}&expiry_date={expiry}",
        ttl=0
    ))

@app.route('/api/positions', methods=['GET'])
def positions():
    """Get current positions"""
    return jsonify(upstox_call("/portfolio/short-term-positions", ttl=2))

@app.route('/api/orders', methods=['GET'])
def orders():
    """Get all orders"""
    return jsonify(upstox_call("/order/retrieve-all", ttl=2))

@app.route('/api/funds', methods=['GET'])
def funds():
    """Get available funds"""
    return jsonify(upstox_call("/user/get-funds-and-margin", ttl=5))

@app.route('/api/order', methods=['POST'])
def place_order():
    """Place a new order"""
    return jsonify(upstox_call(
        "/order/place",
        method="POST",
        body=request.json
    ))

# ════════════════════════════════════════════════════════════════════════════
# WEBSOCKET ENDPOINT (Frontend)
# ════════════════════════════════════════════════════════════════════════════

@sock.route('/ws')
def websocket_endpoint(ws):
    """WebSocket endpoint for frontend clients"""
    with _clients_lock:
        _clients.add(ws)
    
    log.info(f"Frontend WebSocket client connected (total: {len(_clients)})")
    
    try:
        # Send initial state
        with _state_lock:
            ws.send(json.dumps({
                "type": "state",
                "data": {
                    "ltp": dict(_state["ltp"]),
                    "ws_connected": _state["ws_connected"],
                    "tick_count": _state["tick_count"]
                }
            }))
        
        # Keep connection alive
        while True:
            msg = ws.receive()
            if not msg:
                break
    
    except Exception as e:
        log.error(f"WebSocket error: {e}")
    
    finally:
        with _clients_lock:
            _clients.discard(ws)
        log.info(f"Frontend WebSocket client disconnected (total: {len(_clients)})")

# ════════════════════════════════════════════════════════════════════════════
# SELF-PING FOR MILESWEB (Prevent spin-down)
# ════════════════════════════════════════════════════════════════════════════

def keep_alive_ping():
    """Periodic self-ping to prevent VPS spin-down"""
    time.sleep(30)
    
    vps_url = os.environ.get("VPS_URL")
    if not vps_url:
        log.debug("VPS_URL not set, skipping self-ping")
        return
    
    log.info(f"Self-ping enabled: {vps_url}")
    
    while Config.SELF_PING_ENABLED:
        try:
            requests.get(f"{vps_url}/api/health", timeout=10)
            log.debug("Self-ping successful")
        except Exception as e:
            log.warning(f"Self-ping failed: {e}")
        
        time.sleep(Config.SELF_PING_INTERVAL)

# ════════════════════════════════════════════════════════════════════════════
# GRACEFUL SHUTDOWN
# ════════════════════════════════════════════════════════════════════════════

def graceful_shutdown():
    """Graceful shutdown handler"""
    log.info("Shutting down gracefully...")
    
    # Close WebSocket
    global _ws_app
    if _ws_app:
        try:
            _ws_app.close()
        except:
            pass
    
    # Close frontend clients
    with _clients_lock:
        for client in _clients:
            try:
                client.close()
            except:
                pass
    
    log.info("Shutdown complete")

import atexit
atexit.register(graceful_shutdown)

# ════════════════════════════════════════════════════════════════════════════
# STARTUP
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.start_time = time.time()
    
    # Start self-ping thread if enabled
    if Config.SELF_PING_ENABLED:
        ping_thread = threading.Thread(target=keep_alive_ping, daemon=True)
        ping_thread.start()
        log.info("Self-ping thread started")
    
    log.info(f"Starting Flask server on {Config.HOST}:{Config.PORT}")
    log.info(f"WebSocket auth endpoint: {Config.UPSTOX_BASE_URL}/feed/market-data-feed-v3/authorize")
    
    app.run(
        host=Config.HOST,
        port=Config.PORT,
        debug=Config.DEBUG,
        use_reloader=False,
        threaded=True
    )
