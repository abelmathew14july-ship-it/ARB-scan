"""
╔══════════════════════════════════════════════════════════════╗
║         NSE-BSE PRE-MARKET ARB SCANNER — BACKEND            ║
║         Akshit's Trading Desk                                ║
╚══════════════════════════════════════════════════════════════╝

HOW TO USE:

1. Install dependencies:
   pip install flask flask-cors requests
2. Run this script:
   python arb_backend.py
3. Open arb_scanner.html in your browser
   Go to API Config tab → set source to:
   http://localhost:5050/quote
4. Best run between 8:55 AM – 9:08 AM IST for pre-open IEP data

DATA SOURCES:

- NSE pre-open API  : free, no key needed
- BSE pre-open API  : free, no key needed
- Both APIs return IEP (Indicative Equilibrium Price) during pre-open

OPTIONAL — Kite Connect:
Set USE_KITE = True and fill in your API key + access token below.
Kite gives the cleanest real-time data but needs a ₹2000/mo subscription.
"""

import requests
import time
import json
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

# ── CONFIGURATION ──────────────────────────────────────────────

# Set to True if you have Kite Connect (₹2000/mo)
USE_KITE = False
KITE_API_KEY = "your_api_key_here"
KITE_ACCESS_TOKEN = "your_access_token_here"

# Cache duration in seconds (avoid hammering NSE/BSE APIs)
CACHE_TTL = 8

# ──────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)  # Allow arb_scanner.html to call this backend

# In-memory price cache
_cache = {}          # symbol -> {nse, bse, ts}
_nse_bulk = {}       # Full NSE pre-open data, refreshed every CACHE_TTL sec
_bse_bulk = {}       # Full BSE pre-open data
_last_nse_fetch = 0
_last_bse_fetch = 0

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.bseindia.com/",
}

# NSE session (needed for cookies)
nse_session = requests.Session()
nse_session.headers.update(NSE_HEADERS)
_nse_session_init = False


def init_nse_session():
    """Hit NSE homepage first to get cookies — required for API access."""
    global _nse_session_init
    try:
        nse_session.get("https://www.nseindia.com", timeout=10)
        _nse_session_init = True
        print("[OK] NSE session initialised")
    except Exception as e:
        print(f"[FAIL] NSE session init failed: {e}")


def fetch_nse_preopen():
    """
    Fetch NSE pre-open data for all securities.
    Returns dict: {SYMBOL: iep_price}
    """
    global _nse_bulk, _last_nse_fetch
    now = time.time()

    if now - _last_nse_fetch < CACHE_TTL and _nse_bulk:
        return _nse_bulk

    if not _nse_session_init:
        init_nse_session()

    result = {}
    try:
        # NSE pre-open endpoint — returns all pre-open securities
        url = "https://www.nseindia.com/api/market-data-pre-open?key=ALL"
        resp = nse_session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("data", []):
            meta = item.get("metadata", {})
            sym = meta.get("symbol", "")
            # During pre-open: use IEP; after open: use lastPrice
            iep = meta.get("iep") or meta.get("lastPrice") or meta.get("closePrice")
            if sym and iep:
                try:
                    result[sym.upper()] = float(iep)
                except (ValueError, TypeError):
                    pass

        _nse_bulk = result
        _last_nse_fetch = now
        print(f"[{datetime.now().strftime('%H:%M:%S')}] NSE: fetched {len(result)} symbols")

    except requests.exceptions.RequestException as e:
        print(f"[FAIL] NSE fetch error: {e}")
        # Return stale cache if available
        return _nse_bulk

    return result


def fetch_bse_preopen():
    """
    Fetch BSE pre-open data.
    Returns dict: {SYMBOL: iep_price}
    BSE uses scrip codes, mapped via symbol name.
    """
    global _bse_bulk, _last_bse_fetch
    now = time.time()

    if now - _last_bse_fetch < CACHE_TTL and _bse_bulk:
        return _bse_bulk

    result = {}
    try:
        # BSE pre-open API
        url = "https://api.bseindia.com/BseIndiaAPI/api/PreOpenMktAllscrips/w"
        resp = requests.get(url, headers=BSE_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # BSE returns list under different keys depending on session
        items = data if isinstance(data, list) else data.get("Table", data.get("data", []))

        for item in items:
            # BSE field names vary — handle both formats
            sym = (item.get("scrip_name") or item.get("ScripName") or "").strip().upper()
            iep = item.get("IEP") or item.get("iep") or item.get("LTP") or item.get("Ltp")
            if sym and iep:
                try:
                    result[sym] = float(iep)
                except (ValueError, TypeError):
                    pass

        _bse_bulk = result
        _last_bse_fetch = now
        print(f"[{datetime.now().strftime('%H:%M:%S')}] BSE: fetched {len(result)} symbols")

    except requests.exceptions.RequestException as e:
        print(f"[FAIL] BSE fetch error: {e}")
        # Fallback: try BSE equity quote for individual symbols
        return _bse_bulk

    return result


def fetch_kite_quotes(symbols):
    """
    Fetch quotes via Kite Connect API.
    Returns dict: {SYMBOL: {nse, bse}}
    """
    instruments = [f"NSE:{s}" for s in symbols] + [f"BSE:{s}" for s in symbols]
    params = "&".join([f"i={i}" for i in instruments])
    url = f"https://api.kite.trade/quote?{params}"

    resp = requests.get(url, headers={
        "X-Kite-Version": "3",
        "Authorization": f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}"
    }, timeout=10)
    resp.raise_for_status()
    data = resp.json().get("data", {})

    result = {}
    for sym in symbols:
        nse_data = data.get(f"NSE:{sym}", {})
        bse_data = data.get(f"BSE:{sym}", {})
        nse_price = nse_data.get("last_price")
        bse_price = bse_data.get("last_price")
        if nse_price or bse_price:
            result[sym] = {"nse": nse_price, "bse": bse_price}
    return result


def fetch_bse_individual(symbol):
    """
    Fallback: fetch a single BSE quote by symbol name.
    Uses BSE's quote API.
    """
    try:
        # BSE equity search to get scrip code
        search_url = (
            f"https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
            f"?Group=&Scripcode=&shname={symbol}&Type=EQ&Cat="
        )
        resp = requests.get(search_url, headers=BSE_HEADERS, timeout=8)
        items = resp.json()
        if not items:
            return None
        scrip_code = items[0].get("SCRIP_CD")
        if not scrip_code:
            return None

        # Fetch quote using scrip code
        quote_url = (
            f"https://api.bseindia.com/BseIndiaAPI/api/getScripHeaderData/w"
            f"?Debtflag=&scripcode={scrip_code}&seriesid="
        )
        q_resp = requests.get(quote_url, headers=BSE_HEADERS, timeout=8)
        q_data = q_resp.json()
        price = q_data.get("CurrRate") or q_data.get("PrevClose")
        return float(price) if price else None
    except Exception:
        return None


# ── API ROUTES ─────────────────────────────────────────────────

@app.route("/quote")
def quote():
    """
    Main endpoint called by arb_scanner.html
    Query: /quote?sym=RELIANCE&sym=SWIGGY&sym=SHADOWFAX
    Returns: {SYMBOL: {nse: price, bse: price, arb_pct: float, ts: ISO}}
    """
    symbols = [s.upper().strip() for s in request.args.getlist("sym") if s.strip()]
    if not symbols:
        return jsonify({"error": "No symbols provided"}), 400

    result = {}

    if USE_KITE:
        # Kite Connect path
        try:
            kite_data = fetch_kite_quotes(symbols)
            for sym, prices in kite_data.items():
                nse = prices.get("nse")
                bse = prices.get("bse")
                if nse and bse:
                    arb_pct = abs(nse - bse) / min(nse, bse) * 100
                    result[sym] = {
                        "nse": round(nse, 2),
                        "bse": round(bse, 2),
                        "arb_pct": round(arb_pct, 4),
                        "source": "kite",
                        "ts": datetime.now().isoformat()
                    }
        except Exception as e:
            return jsonify({"error": f"Kite API error: {str(e)}"}), 500
    else:
        # Free NSE + BSE path
        nse_data = fetch_nse_preopen()
        bse_data = fetch_bse_preopen()

        for sym in symbols:
            nse_price = nse_data.get(sym)
            bse_price = bse_data.get(sym)

            # BSE symbol names sometimes differ — try variations
            if not bse_price:
                for variant in [sym, sym.replace("-", ""), sym + " LTD", sym + " LIMITED"]:
                    if variant in bse_data:
                        bse_price = bse_data[variant]
                        break

            # Fallback: individual BSE fetch if bulk didn't have it
            if nse_price and not bse_price:
                bse_price = fetch_bse_individual(sym)

            if nse_price and bse_price:
                arb_pct = abs(nse_price - bse_price) / min(nse_price, bse_price) * 100
                result[sym] = {
                    "nse": round(nse_price, 2),
                    "bse": round(bse_price, 2),
                    "arb_pct": round(arb_pct, 4),
                    "source": "nse+bse",
                    "ts": datetime.now().isoformat()
                }
            elif nse_price:
                result[sym] = {
                    "nse": round(nse_price, 2),
                    "bse": None,
                    "arb_pct": None,
                    "source": "nse_only",
                    "ts": datetime.now().isoformat()
                }
            else:
                result[sym] = {
                    "nse": None,
                    "bse": None,
                    "arb_pct": None,
                    "source": "not_found",
                    "ts": datetime.now().isoformat()
                }

    return jsonify(result)


@app.route("/health")
def health():
    """Health check — arb_scanner.html pings this to confirm backend is live."""
    ist = datetime.utcnow()  # Add 5:30 for IST in JS
    return jsonify({
        "status": "ok",
        "mode": "kite" if USE_KITE else "nse+bse",
        "nse_symbols_cached": len(_nse_bulk),
        "bse_symbols_cached": len(_bse_bulk),
        "time_utc": ist.isoformat()
    })


@app.route("/preopen/all")
def preopen_all():
    """Return all pre-open symbols above a given arb threshold."""
    threshold = float(request.args.get("threshold", 3.0))
    nse_data = fetch_nse_preopen()
    bse_data = fetch_bse_preopen()

    opportunities = []
    common_syms = set(nse_data.keys()) & set(bse_data.keys())

    for sym in common_syms:
        nse_p = nse_data[sym]
        bse_p = bse_data[sym]
        if nse_p > 0 and bse_p > 0:
            arb_pct = abs(nse_p - bse_p) / min(nse_p, bse_p) * 100
            if arb_pct >= threshold:
                opportunities.append({
                    "symbol": sym,
                    "nse": round(nse_p, 2),
                    "bse": round(bse_p, 2),
                    "arb_pct": round(arb_pct, 4),
                    "direction": "BUY BSE · SELL NSE" if nse_p > bse_p else "BUY NSE · SELL BSE"
                })

    opportunities.sort(key=lambda x: x["arb_pct"], reverse=True)
    return jsonify({
        "count": len(opportunities),
        "threshold": threshold,
        "opportunities": opportunities,
        "ts": datetime.now().isoformat()
    })


# ── STARTUP ────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  NSE-BSE PRE-MARKET ARB BACKEND")
    print("=" * 60)
    print(f"  Mode     : {'Kite Connect API' if USE_KITE else 'NSE + BSE Free APIs'}")
    print(f"  Port     : http://localhost:5050")
    print(f"  Health   : http://localhost:5050/health")
    print(f"  All Opps : http://localhost:5050/preopen/all?threshold=3")
    print("=" * 60)
    print("\n  In arb_scanner.html -> API Config -> set source to:")
    print("  http://localhost:5050/quote\n")

    # Pre-warm NSE session
    print("Initialising NSE session...")
    init_nse_session()

    # Pre-fetch data
    print("Pre-fetching NSE pre-open data...")
    fetch_nse_preopen()
    print("Pre-fetching BSE pre-open data...")
    fetch_bse_preopen()

    print("\n[OK] Backend ready. Open arb_scanner.html in your browser.\n")
    app.run(host="0.0.0.0", port=5050, debug=False)
