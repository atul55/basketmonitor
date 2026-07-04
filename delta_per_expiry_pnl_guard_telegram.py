"""
Delta Exchange (India) - Per-Expiry Basket PnL Monitor & Auto-Close Bot (ASYNC + RETRY)
=========================================================================================
Monitors PnL SEPARATELY for each option expiry date (since you run multiple
expiry baskets concurrently). If ANY expiry's basket PnL breaches its own
-$22 loss limit, it:
  1) Sends a Telegram alert naming that specific expiry
  2) Closes ONLY that expiry's open positions CONCURRENTLY (async, reduce_only
     market orders), with one automatic retry per leg if a close order fails
  3) Reports actual realized loss for that expiry to Telegram after closure

Additionally, sends a periodic PnL status update to Telegram every 5 minutes,
listing every open expiry basket's current PnL.

Other expiry baskets keep being monitored independently and are unaffected.

Requirements:
    pip install aiohttp

Setup:
  - Create API key/secret at https://www.delta.exchange (Account > API Keys)
    with "Trading" + "Read Data" permissions, whitelist your server IP.
  - Create a Telegram bot via @BotFather, get BOT_TOKEN.
  - Get your chat_id (message the bot once, then GET
    https://api.telegram.org/bot<token>/getUpdates)

Expiry grouping:
  - Delta option symbols follow the pattern: {C|P}-{ASSET}-{STRIKE}-{DDMMYY}
    e.g. C-BTC-63000-050726 -> expiry = 050726 (05-Jul-2026)
  - Positions are grouped by this trailing DDMMYY token so each expiry's
    basket PnL and stop-loss are tracked independently.
"""

import hmac
import hashlib
import time
import json
import asyncio
import aiohttp
from collections import defaultdict
from config import get_required_env
# ---------------- CONFIG ----------------
API_KEY = get_required_env("DELTA_API_KEY")
API_SECRET = get_required_env("DELTA_API_SECRET")
BASE_URL = "https://api.india.delta.exchange"   # Delta India REST endpoint

TELEGRAM_BOT_TOKEN = get_required_env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_required_env("TELEGRAM_CHAT_ID")

LOSS_LIMIT_PER_EXPIRY = -22.0   # trigger close when an expiry basket's PnL <= this
POLL_INTERVAL_SEC = 15          # how often to check PnL
STATUS_UPDATE_INTERVAL_SEC = 300 # send PnL status to Telegram every 5 minutes
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10)
CLOSE_MAX_RETRIES = 1           # retry once if a close order fails
CLOSE_RETRY_DELAY_SEC = 1.0

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
        "User-Agent": "python-basket-monitor-per-expiry"
    }


async def send_telegram(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with session.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}) as resp:
            await resp.read()
    except Exception as e:
        print(f"[Telegram Error] {e}")


async def get_open_positions(session):
    """Fetch all open (margined) positions."""
    path = "/v2/positions/margined"
    headers = get_auth_headers("GET", path, "")
    async with session.get(BASE_URL + path, headers=headers) as resp:
        data = await resp.json()
    if not data.get("success"):
        raise RuntimeError(f"API error: {data}")
    return data.get("result", [])


def extract_expiry(symbol):
    """
    Extract the trailing DDMMYY expiry token from a Delta option symbol,
    e.g. 'C-BTC-63000-050726' -> '050726'. Falls back to 'UNKNOWN' if the
    symbol doesn't match the expected options format (e.g. perpetual futures).
    """
    if not symbol:
        return "Invalid Contract Symbol. Expect in the format P-BTC-63000-050726"
    parts = symbol.split("-")
    if len(parts) >= 4 and parts[-1].isdigit() and len(parts[-1]) == 6:
        return parts[-1]
    return "Invalid Contract Symbol format. Expect in the format P-BTC-63000-050726"


def group_positions_by_expiry(positions):
    """
    Group open positions (size != 0) by expiry date.
    Returns dict: { expiry_str: [position, ...] }
    """
    groups = defaultdict(list)
    for p in positions:
        size = float(p.get("size", 0))
        if size == 0:
            continue
        expiry = extract_expiry(p.get("product_symbol"))
        groups[expiry].append(p)
    return groups


def compute_basket_pnl(positions):
    """
    Sum PnL across a set of positions using unrealized_cashflow + realized_cashflow.
    """
    total = 0.0
    details = []
    for p in positions:
        pnl = float(p.get("unrealized_cashflow", 0) or 0) + float(p.get("realized_cashflow", 0) or 0)
        total += pnl
        details.append({
            "symbol": p.get("product_symbol"),
            "size": float(p.get("size", 0)),
            "entry_price": p.get("entry_price"),
            "product_id": p.get("product_id"),
            "pnl": pnl
        })
    return total, details


async def _send_close_order(session, product_id, side, qty):
    """Fire a single reduce_only market order request. Raises on failure."""
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

    async with session.post(BASE_URL + path, headers=headers, data=body) as resp:
        result = await resp.json()

    if not result.get("success", False):
        raise RuntimeError(f"Order rejected: {result}")
    return result


async def close_single_position(session, p, max_retries=CLOSE_MAX_RETRIES, retry_delay=CLOSE_RETRY_DELAY_SEC):
    """
    Fire a single reduce_only market order to flatten one leg.
    Retries once (configurable) if the initial attempt fails.
    """
    size = float(p.get("size", 0))
    if size == 0:
        return None
    product_id = p.get("product_id")
    symbol = p.get("product_symbol")
    side = "sell" if size > 0 else "buy"
    qty = abs(int(size))

    attempt = 0
    last_error = None
    while attempt <= max_retries:
        try:
            result = await _send_close_order(session, product_id, side, qty)
            return {
                "product_id": product_id,
                "symbol": symbol,
                "result": result,
                "attempts": attempt + 1
            }
        except Exception as e:
            last_error = str(e)
            attempt += 1
            if attempt <= max_retries:
                print(f"[Retry] Close order failed for {symbol} (attempt {attempt}): {last_error}. Retrying...")
                await asyncio.sleep(retry_delay)

    return {
        "product_id": product_id,
        "symbol": symbol,
        "error": last_error,
        "attempts": attempt
    }


async def close_basket_concurrently(session, positions):
    """
    Close every position in ONE expiry basket simultaneously using asyncio.gather.
    Each leg gets one retry internally if its close order fails.
    """
    tasks = [close_single_position(session, p) for p in positions if float(p.get("size", 0)) != 0]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results


async def get_realized_pnl_for_product(session, product_id):
    path = "/v2/fills"
    query_string = f"?product_id={product_id}&page_size=5"
    headers = get_auth_headers("GET", path, query_string)
    async with session.get(BASE_URL + path + query_string, headers=headers) as resp:
        data = await resp.json()
    realized = 0.0
    if data.get("success"):
        for fill in data.get("result", []):
            realized += float(fill.get("realized_pnl", 0) or 0)
    return realized


async def get_realized_pnl_after_close(session, product_ids):
    """Fetch realized PnL for all closed products in a basket concurrently and sum them."""
    tasks = [get_realized_pnl_for_product(session, pid) for pid in product_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    total_realized = 0.0
    for r in results:
        if isinstance(r, (int, float)):
            total_realized += r
    return total_realized


async def handle_expiry_breach(session, expiry, total_pnl, details):
    """
    Alert, close, and report for a SINGLE expiry basket that breached
    the per-expiry loss limit. Runs as its own coroutine so multiple
    expiries can be closed concurrently if they breach at the same time.
    """
    alert_msg = (
        f"🚨 STOP-LOSS TRIGGERED for expiry {expiry} 🚨\n"
        f"Basket PnL: {total_pnl:.2f} USD (limit: {LOSS_LIMIT_PER_EXPIRY} USD)\n"
        f"Closing all {len(details)} positions for expiry {expiry}..."
    )
    print(alert_msg)
    await send_telegram(session, alert_msg)

    positions = [
        {
            "product_id": d["product_id"],
            "product_symbol": d["symbol"],
            "size": d["size"]
        }
        for d in details
    ]

    t0 = time.perf_counter()
    close_results = await close_basket_concurrently(session, positions)
    elapsed = time.perf_counter() - t0
    print(f"[{expiry}] All close orders fired in {elapsed:.2f}s")

    failed_legs = [r for r in close_results if isinstance(r, dict) and r.get("error")]
    if failed_legs:
        fail_msg = "\n".join(
            f"- {r['symbol']}: failed after {r.get('attempts', '?')} attempt(s) - {r['error']}"
            for r in failed_legs
        )
        await send_telegram(
            session,
            f"⚠️ WARNING: expiry {expiry} has {len(failed_legs)} leg(s) still OPEN after retry:\n{fail_msg}\n"
            f"Manual intervention required immediately."
        )

    product_ids = [d["product_id"] for d in details]
    await asyncio.sleep(3)  # allow fills to settle
    realized_pnl = await get_realized_pnl_after_close(session, product_ids)
    report_msg = (
        f"✅ Expiry {expiry} positions closed in {elapsed:.2f}s.\n"
        f"Estimated pre-close basket PnL: {total_pnl:.2f} USD\n"
        f"Actual realized PnL from fills: {realized_pnl:.2f} USD"
    )
    print(report_msg)
    await send_telegram(session, report_msg)
    return expiry


async def monitor_loop():
    """
    Option 1: continuously monitor PnL PER EXPIRY.
    Option 2: if any expiry basket PnL <= LOSS_LIMIT_PER_EXPIRY, alert + close that basket.
    Option 3: report actual realized loss for that expiry to Telegram.
    Also sends a periodic PnL status update every STATUS_UPDATE_INTERVAL_SEC.
    Other expiries continue to be monitored after one closes.
    """
    print("Starting per-expiry basket PnL monitor (async)... Ctrl+C to stop.")
    closed_expiries = set()
    last_status_sent = 0.0

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        while True:
            try:
                positions = await get_open_positions(session)
                groups = group_positions_by_expiry(positions)

                if not groups:
                    print("No open positions. Exiting monitor.")
                    await send_telegram(session, "ℹ️ No open positions found. Monitor stopped.")
                    break

                breach_tasks = []
                summary_lines = []

                for expiry, pos_list in groups.items():
                    if expiry in closed_expiries:
                        continue
                    total_pnl, details = compute_basket_pnl(pos_list)
                    summary_lines.append(f"  Expiry {expiry}: {total_pnl:.2f} USD ({len(details)} legs)")

                    if total_pnl <= LOSS_LIMIT_PER_EXPIRY:
                        breach_tasks.append(handle_expiry_breach(session, expiry, total_pnl, details))

                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Per-expiry PnL:\n" + "\n".join(summary_lines))

                now = time.time()
                if summary_lines and (now - last_status_sent >= STATUS_UPDATE_INTERVAL_SEC):
                    status_msg = (
                        f"📊 PnL Status Update ({time.strftime('%Y-%m-%d %H:%M:%S')})\n"
                        + "\n".join(summary_lines)
                    )
                    await send_telegram(session, status_msg)
                    last_status_sent = now

                if breach_tasks:
                    finished_expiries = await asyncio.gather(*breach_tasks, return_exceptions=True)
                    for e in finished_expiries:
                        if isinstance(e, str):
                            closed_expiries.add(e)

                remaining_expiries = set(groups.keys()) - closed_expiries
                if not remaining_expiries:
                    print("All expiry baskets closed. Exiting monitor.")
                    break

                await asyncio.sleep(POLL_INTERVAL_SEC)

            except Exception as e:
                err = f"[Monitor Error] {e}"
                print(err)
                await send_telegram(session, f"⚠️ Monitor error: {e}")
                await asyncio.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    asyncio.run(monitor_loop())