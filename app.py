import os, time, requests, threading, json
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins="*")

BASE = "https://api.upstox.com/v2"

# ── In-memory cache for fast repeated reads ──────────────────────────────────
_cache = {}
_cache_lock = threading.Lock()

def cache_set(key, val, ttl=3):
    """Cache with TTL in seconds"""
    with _cache_lock:
        _cache[key] = {"val": val, "exp": time.time() + ttl}

def cache_get(key):
    with _cache_lock:
        c = _cache.get(key)
        if c and time.time() < c["exp"]:
            return c["val"]
    return None

def up(path, token, cache_ttl=2):
    """Upstox GET with caching"""
    cached = cache_get(path)
    if cached is not None:
        return cached
    h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    r = requests.get(BASE + path, headers=h, timeout=10)
    r.raise_for_status()
    data = r.json()
    if cache_ttl > 0:
        cache_set(path, data, cache_ttl)
    return data

def up_post(path, token, body):
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
    r = requests.post(BASE + path, headers=h, json=body, timeout=10)
    return r.json()

def tok():
    return request.headers.get("Authorization", "").replace("Bearer ", "").strip()

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def home():
    return jsonify({"status": "AlgoFO Server running", "time": time.time()})

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "time": time.time()})

@app.route("/api/ltp")
def ltp():
    keys = request.args.get("instrument_key", "NSE_INDEX|Nifty 50")
    try:
        # LTP changes every second — cache for 1 second only
        return jsonify(up(f"/market-quote/ltp?instrument_key={keys}", tok(), cache_ttl=1))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/quote")
def quote():
    """Full market quote with bid/ask/OHLC — cache 1s"""
    keys = request.args.get("instrument_key", "")
    try:
        return jsonify(up(f"/market-quote/quotes?instrument_key={keys}", tok(), cache_ttl=1))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/chain")
def chain():
    key = request.args.get("instrument_key", "")
    expiry = request.args.get("expiry_date", "")
    try:
        # Option chain updates every ~5s — cache for 5s
        return jsonify(up(f"/option/chain?instrument_key={key}&expiry_date={expiry}", tok(), cache_ttl=5))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/expiries")
def expiries():
    key = request.args.get("instrument_key", "")
    try:
        # Expiries change rarely — cache 60s
        return jsonify(up(f"/option/contract?instrument_key={key}", tok(), cache_ttl=60))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/positions")
def positions():
    try:
        return jsonify(up("/portfolio/short-term-positions", tok(), cache_ttl=2))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/orders")
def orders():
    try:
        return jsonify(up("/order/retrieve-all", tok(), cache_ttl=2))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/funds")
def funds():
    try:
        return jsonify(up("/user/get-funds-and-margin", tok(), cache_ttl=5))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/order", methods=["POST"])
def place_order():
    try:
        # No cache for order placement
        result = up_post("/order/place", tok(), request.json)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/market")
def full_market():
    """
    Single endpoint: fetches LTP + chain + positions + funds in parallel.
    Use this for fast polling — one HTTP round-trip instead of 4.
    """
    t = tok()
    key = request.args.get("instrument_key", "NSE_INDEX|Nifty 50")
    expiry = request.args.get("expiry_date", "")
    idx_keys = "NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank,BSE_INDEX|SENSEX"

    result = {}
    errors = {}

    # Run all fetches in parallel threads
    def fetch(name, path, ttl):
        try:
            result[name] = up(path, t, cache_ttl=ttl)
        except Exception as e:
            errors[name] = str(e)

    threads = [
        threading.Thread(target=fetch, args=("ltp", f"/market-quote/ltp?instrument_key={idx_keys}", 1)),
        threading.Thread(target=fetch, args=("positions", "/portfolio/short-term-positions", 2)),
        threading.Thread(target=fetch, args=("funds", "/user/get-funds-and-margin", 5)),
    ]
    if expiry:
        threads.append(threading.Thread(target=fetch, args=(
            "chain", f"/option/chain?instrument_key={key}&expiry_date={expiry}", 5)))

    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=8)

    if errors:
        result["_errors"] = errors
    return jsonify(result)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
