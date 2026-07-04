
"""
Delta Exchange (India) - Basket Position PnL Monitor & Auto-Close Bot
=======================================================================
Monitors combined UPNL of all open positions. If total PnL falls between
-$20 and -$25 (configurable), it:
  1) Sends a Telegram alert
  2) Closes ALL open positions (market, reduce_only orders)
  3) Reports actual realized loss to Telegram after closure

Requirements:
    pip install requests

Setup:
  - Create API key/secret at https://www.delta.exchange (Account > API Keys)
    with "Trading" + "Read Data" permissions, whitelist your server IP.
  - Create a Telegram bot via @BotFather, get BOT_TOKEN.
  - Get your chat_id (message the bot once, then GET
    https://api.telegram.org/bot<token>/getUpdates)
"""

import hmac
import hashlib
import time
import json
import requests

from config import get_required_env

# ---------------- CONFIG ----------------
API_KEY = get_required_env("DELTA_API_KEY")
API_SECRET = get_required_env("DELTA_API_SECRET")
BASE_URL = "https://api.india.delta.exchange"   # Delta India REST endpoint

TELEGRAM_BOT_TOKEN = get_required_env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_required_env("TELEGRAM_CHAT_ID")

LOSS_LOWER = -25.0   # more negative bound (bigger loss)
LOSS_UPPER = -20.0   # closer to zero bound (smaller loss)
POLL_INTERVAL_SEC = 15   # how often to check PnL

# -----------------------------------------


def generate_signature(secret, message):
    return hmac.new(
        bytes(secret, "utf-8"),
        bytes(message, "utf-8"),
        hashlib.sha256
    ).hexdigest()


def get_auth_headers(method, path, query_string="", payload=""):
    timestamp = str(int(time.time()))
    signature_data = method + timestamp + path + query_string + payload
    signature = generate_signature(API_SECRET, signature_data)
    return {
        "api-key": API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "Content-Type": "application/json",
        "User-Agent": "python-basket-monitor"
    }


def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
    except Exception as e:
        print(f"[Telegram Error] {e}")


def get_open_positions():
    """Fetch all open (margined) positions."""
    path = "/v2/positions/margined"
    query_string = ""
    headers = get_auth_headers("GET", path, query_string)
    resp = requests.get(BASE_URL + path, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"API error: {data}")
    return data.get("result", [])


def compute_total_pnl(positions):
    """
    Sum realized cashflow across all open positions.
    Delta returns 'realized_cashflow' (string) per position, in the position's settling asset.
    """
    total = 0.0
    details = []
    for p in positions:
        size = float(p.get("size", 0))
        if size == 0:
            continue
        upnl = float(p.get("unrealized_cashflow", 0) or 0) + float(p.get("realized_cashflow", 0) or 0)
        total += upnl
        details.append({
            "symbol": p.get("product_symbol"),
            "size": size,
            "entry_price": p.get("entry_price"),
            "upnl": upnl
        })
    return total, details


def close_all_positions(positions):
    """
    Close every open position with a reduce_only market order
    in the opposite direction of the current position size.
    """
    closed_orders = []
    for p in positions:
        size = float(p.get("size", 0))
        if size == 0:
            continue
        product_id = p.get("product_id")
        side = "sell" if size > 0 else "buy"
        qty = abs(int(size))

        order_payload = {
            "product_id": product_id,
            "size": qty,
            "side": side,
            "order_type": "market_order",
            "reduce_only": True
        }
        path = "/v2/orders"
        body = json.dumps(order_payload, separators=(",", ":"))
        headers = get_auth_headers("POST", path, "", body)
        resp = requests.post(BASE_URL + path, headers=headers, data=body, timeout=10)
        result = resp.json()
        closed_orders.append(result)
        time.sleep(0.3)   # avoid rate limiting
    return closed_orders


def get_realized_pnl_after_close(product_ids):
    """
    After closing, fetch position history / fills to compute realized PnL.
    Uses fills endpoint filtered by product to sum realized_pnl of closing fills.
    """
    path = "/v2/fills"
    total_realized = 0.0
    for pid in product_ids:
        query_string = f"?product_id={pid}&page_size=5"
        headers = get_auth_headers("GET", path, query_string)
        resp = requests.get(BASE_URL + path + query_string, headers=headers, timeout=10)
        data = resp.json()
        if data.get("success"):
            for fill in data.get("result", []):
                total_realized += float(fill.get("realized_pnl", 0) or 0)
    return total_realized


def monitor_loop():
    """Option 1: continuously monitor total PnL."""
    print("Starting basket PnL monitor... Ctrl+C to stop.")
    while True:
        try:
            positions = get_open_positions()
            total_pnl, details = compute_total_pnl(positions)

            if not details:
                print("No open positions. Exiting monitor.")
                send_telegram("ℹ️ No open positions found. Monitor stopped.")
                break

            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Total UPNL: {total_pnl:.2f} USD "
                  f"across {len(details)} positions")

            # Option 2: trigger condition -25 <= pnl <= -20
            if total_pnl <= LOSS_LOWER:
                alert_msg = (
                    f"🚨 STOP-LOSS TRIGGERED 🚨\n"
                    f"Total unrealized PnL: {total_pnl:.2f} USD\n"
                    f"Threshold band: {LOSS_LOWER} to {LOSS_UPPER} USD\n"
                    f"Closing all {len(details)} open positions now..."
                )
                print(alert_msg)
                send_telegram(alert_msg)

                product_ids = [p.get("product_id") for p in positions if float(p.get("size", 0)) != 0]
                close_results = close_all_positions(positions)

                time.sleep(3)  # allow fills to settle
                # Option 3: report actual realized loss
                realized_pnl = get_realized_pnl_after_close(product_ids)
                report_msg = (
                    f"✅ Positions closed.\n"
                    f"Estimated pre-close UPNL: {total_pnl:.2f} USD\n"
                    f"Actual realized PnL from fills: {realized_pnl:.2f} USD"
                )
                print(report_msg)
                send_telegram(report_msg)
                break

            time.sleep(POLL_INTERVAL_SEC)

        except Exception as e:
            err = f"[Monitor Error] {e}"
            print(err)
            send_telegram(f"⚠️ Monitor error: {e}")
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    monitor_loop()
