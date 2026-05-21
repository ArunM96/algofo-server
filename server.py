"""
AlgoFO Backend Server
- Fetches live data from Upstox API (no CORS issues server-side)
- Serves the frontend app
- WebSocket for real-time updates
Run: pip install flask flask-cors requests && python server.py
"""
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests, os, json, time

app = Flask(__name__, static_folder=".")
CORS(app)  # Allow all origins

UPSTOX_BASE = "https://api.upstox.com/v2"

def upstox_get(path, token):
    """Direct server-side call to Upstox — no CORS blocking"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    r = requests.get(f"{UPSTOX_BASE}{path}", headers=headers, timeout=10)
    return r.json()

def upstox_post(path, token, body):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    r = requests.post(f"{UPSTOX_BASE}{path}", headers=headers, json=body, timeout=10)
    return r.json()

# ── Proxy Routes ──────────────────────────────────────────────────────────────

@app.route("/api/ltp")
def ltp():
    token = request.headers.get("Authorization","").replace("Bearer ","")
    keys = request.args.get("instrument_key","")
    try:
        data = upstox_get(f"/market-quote/ltp?instrument_key={keys}", token)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/chain")
def chain():
    token = request.headers.get("Authorization","").replace("Bearer ","")
    key = request.args.get("instrument_key","")
    expiry = request.args.get("expiry_date","")
    try:
        data = upstox_get(f"/option/chain?instrument_key={key}&expiry_date={expiry}", token)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/expiries")
def expiries():
    token = request.headers.get("Authorization","").replace("Bearer ","")
    key = request.args.get("instrument_key","")
    try:
        data = upstox_get(f"/option/contract?instrument_key={key}", token)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/positions")
def positions():
    token = request.headers.get("Authorization","").replace("Bearer ","")
    try:
        data = upstox_get("/portfolio/short-term-positions", token)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/orders")
def orders():
    token = request.headers.get("Authorization","").replace("Bearer ","")
    try:
        data = upstox_get("/order/retrieve-all", token)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/funds")
def funds():
    token = request.headers.get("Authorization","").replace("Bearer ","")
    try:
        data = upstox_get("/user/get-funds-and-margin", token)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/order", methods=["POST"])
def place_order():
    token = request.headers.get("Authorization","").replace("Bearer ","")
    body = request.json
    try:
        data = upstox_post("/order/place", token, body)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/full-market")
def full_market():
    """Get everything in one call to minimize requests"""
    token = request.headers.get("Authorization","").replace("Bearer ","")
    key = request.args.get("instrument_key","NSE_INDEX|Nifty 50")
    expiry = request.args.get("expiry_date","")
    idx_keys = "NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank,BSE_INDEX|SENSEX"
    result = {}
    try:
        result["ltp"] = upstox_get(f"/market-quote/ltp?instrument_key={idx_keys}", token)
    except: pass
    if expiry:
        try:
            result["chain"] = upstox_get(f"/option/chain?instrument_key={key}&expiry_date={expiry}", token)
        except: pass
    try:
        result["positions"] = upstox_get("/portfolio/short-term-positions", token)
    except: pass
    try:
        result["funds"] = upstox_get("/user/get-funds-and-margin", token)
    except: pass
    return jsonify(result)

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "time": time.time()})

# Serve the frontend
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"\n✅ AlgoFO Server running on port {port}")
    print(f"   Open: http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
