import os
import time
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins="*")

UPSTOX_BASE = "https://api.upstox.com/v2"


def upstox_get(path, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    r = requests.get(UPSTOX_BASE + path, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()


def upstox_post(path, token, body):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    r = requests.post(UPSTOX_BASE + path, headers=headers, json=body, timeout=15)
    return r.json()


def get_token():
    auth = request.headers.get("Authorization", "")
    return auth.replace("Bearer ", "").strip()


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
        return jsonify(upstox_get(f"/market-quote/ltp?instrument_key={keys}", get_token()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/chain")
def chain():
    key = request.args.get("instrument_key", "")
    expiry = request.args.get("expiry_date", "")
    try:
        return jsonify(upstox_get(f"/option/chain?instrument_key={key}&expiry_date={expiry}", get_token()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/expiries")
def expiries():
    key = request.args.get("instrument_key", "")
    try:
        return jsonify(upstox_get(f"/option/contract?instrument_key={key}", get_token()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/positions")
def positions():
    try:
        return jsonify(upstox_get("/portfolio/short-term-positions", get_token()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/orders")
def orders():
    try:
        return jsonify(upstox_get("/order/retrieve-all", get_token()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/funds")
def funds():
    try:
        return jsonify(upstox_get("/user/get-funds-and-margin", get_token()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/order", methods=["POST"])
def place_order():
    try:
        return jsonify(upstox_post("/order/place", get_token(), request.json))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/market")
def full_market():
    t = get_token()
    key = request.args.get("instrument_key", "NSE_INDEX|Nifty 50")
    expiry = request.args.get("expiry_date", "")
    idx_keys = "NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank,BSE_INDEX|SENSEX"
    result = {}
    try:
        result["ltp"] = upstox_get(f"/market-quote/ltp?instrument_key={idx_keys}", t)
    except Exception as e:
        result["ltp_error"] = str(e)
    if expiry:
        try:
            result["chain"] = upstox_get(
                f"/option/chain?instrument_key={key}&expiry_date={expiry}", t)
        except Exception as e:
            result["chain_error"] = str(e)
    try:
        result["positions"] = upstox_get("/portfolio/short-term-positions", t)
    except Exception as e:
        result["positions_error"] = str(e)
    try:
        result["funds"] = upstox_get("/user/get-funds-and-margin", t)
    except Exception as e:
        result["funds_error"] = str(e)
    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
