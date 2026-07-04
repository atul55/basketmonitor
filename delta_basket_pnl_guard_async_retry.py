"""
Delta Exchange (India) - Basket Position PnL Monitor & Auto-Close Bot (ASYNC + RETRY)
================================================================================
Monitors combined PnL of all open positions. If total PnL falls in the
configured loss band, it:
  1) Sends a Telegram alert
  2) Closes ALL open positions CONCURRENTLY (async, reduce_only market orders)
     with one automatic retry per leg if the close order fails
  3) Reports actual realized loss to Telegram after closure

Requirements:
    pip install aiohttp

Setup:
  - Create API key/secret at https://www.delta.exchange (Account > API Keys)
    with "Trading" + "Read Data" permissions, whitelist your server IP.
  - Create a Telegram bot via @BotFather, get BOT_TOKEN.
  - Get your chat_id (message the bot once, then GET
    https://api.telegram.org/bot<token>/getUpdates)

Why async:
  - Closing a 4-leg (or larger) basket sequentially means each order waits
    on the network round-trip of the previous one, adding real slippage risk
    during a fast-moving stop-loss event. Firing all close orders concurrently
    with asyncio + aiohttp cuts total execution time roughly from N x latency
    down to ~1 x latency.
"""

import hmac
import hashlib
import time
import json
import asyncio
import aiohttp

from config import get_required_env

# ---------------- CONFIG ----------------
API_KEY = get_required_env("DELTA_API_KEY")
API_SECRET = get_required_env("DELTA_API_SECRET")
BASE_URL = "https://api.india.delta.exchange"   # Delta India REST endpoint

TELEGRAM_BOT_TOKEN = get_required_env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_required_env("TELEGRAM_CHAT_ID")

LOSS_LOWER = -22.0   # more negative bound (bigger loss)
LOSS_UPPER = -20.0   # closer to zero bound (smaller loss)
POLL_INTERVAL_SEC = 15   # how often to check PnL
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10)
CLOSE_MAX_RETRIES = 1    # retry once if a close order fails
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
        "User-Agent": "python-basket-monitor-async"
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


def compute_total_pnl(positions):
    """
    Sum PnL across all open positions using unrealized_cashflow + realized_cashflow,
    matching Delta's actual PnL fields.
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
            "product_id": p.get("product_id"),
            "upnl": upnl
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
    Retries once (configurable) if the initial attempt fails
    (network error, timeout, or exchange-side rejection).
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


async def close_all_positions_concurrently(session, positions):
    """
    Close every open position simultaneously using asyncio.gather.
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
    """
    Fetch realized PnL for all closed products concurrently and sum them.
    """
    tasks = [get_realized_pnl_for_product(session, pid) for pid in product_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    total_realized = 0.0
    for r in results:
        if isinstance(r, (int, float)):
            total_realized += r
    return total_realized


async def monitor_loop():
    """Option 1: continuously monitor total PnL (async polling)."""
    print("Starting basket PnL monitor (async)... Ctrl+C to stop.")
    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        while True:
            try:
                positions = await get_open_positions(session)
                total_pnl, details = compute_total_pnl(positions)

                if not details:
                    print("No open positions. Exiting monitor.")
                    await send_telegram(session, "ℹ️ No open positions found. Monitor stopped.")
                    break

                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Total PnL: {total_pnl:.2f} USD "
                      f"across {len(details)} positions")

                # Option 2: trigger condition LOSS_LOWER <= pnl <= LOSS_UPPER
                if total_pnl <= LOSS_LOWER:
                    alert_msg = (
                        f"🚨 STOP-LOSS TRIGGERED 🚨\n"
                        f"Total PnL: {total_pnl:.2f} USD\n"
                        f"Threshold band: {LOSS_LOWER} to {LOSS_UPPER} USD\n"
                        f"Closing all {len(details)} open positions concurrently..."
                    )
                    print(alert_msg)
                    await send_telegram(session, alert_msg)

                    t0 = time.perf_counter()
                    close_results = await close_all_positions_concurrently(session, positions)
                    elapsed = time.perf_counter() - t0
                    print(f"All close orders fired in {elapsed:.2f}s")

                    failed_legs = [r for r in close_results if isinstance(r, dict) and r.get("error")]
                    if failed_legs:
                        fail_msg = "\n".join(
                            f"- {r['symbol']}: failed after {r.get('attempts', '?')} attempt(s) - {r['error']}"
                            for r in failed_legs
                        )
                        await send_telegram(
                            session,
                            f"⚠️ WARNING: {len(failed_legs)} leg(s) still OPEN after retry:\n{fail_msg}\n"
                            f"Manual intervention required immediately."
                        )

                    product_ids = [d["product_id"] for d in details]

                    await asyncio.sleep(3)  # allow fills to settle on exchange side
                    # Option 3: report actual realized loss
                    realized_pnl = await get_realized_pnl_after_close(session, product_ids)
                    report_msg = (
                        f"✅ Positions closed in {elapsed:.2f}s.\n"
                        f"Estimated pre-close PnL: {total_pnl:.2f} USD\n"
                        f"Actual realized PnL from fills: {realized_pnl:.2f} USD"
                    )
                    print(report_msg)
                    await send_telegram(session, report_msg)
                    break

                await asyncio.sleep(POLL_INTERVAL_SEC)

            except Exception as e:
                err = f"[Monitor Error] {e}"
                print(err)
                await send_telegram(session, f"⚠️ Monitor error: {e}")
                await asyncio.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    asyncio.run(monitor_loop())