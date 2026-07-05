"""
Delta Exchange (India) - Per-Expiry Basket Hybrid Stop Monitor & Auto-Close Bot
=================================================================================
Monitors positions separately for each option expiry and applies a hybrid stop:
1) Primary trigger: underlying breaches computed expiry breakeven band
2) Secondary trigger: basket PnL loss exceeds a wider emergency threshold
3) Wick filter: underlying breach must persist for 3 consecutive polls

The bot still closes only the breached expiry basket, sends Telegram alerts,
and reports actual realized PnL after closure.
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

POLL_INTERVAL_SEC = 15           # how often to check PnL
STATUS_UPDATE_INTERVAL_SEC = 300 # send PnL status to Telegram every 5 minutes
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10)
CLOSE_MAX_RETRIES = 1           # retry once if a close order fails
CLOSE_RETRY_DELAY_SEC = 1.0

EMERGENCY_PNL_LIMIT_PER_EXPIRY = -60.0
BREAKEVEN_CONFIRM_POLLS = 3
BREAKEVEN_SOLVER_PAD = 5000.0
BREAKEVEN_SOLVER_ITERATIONS = 60
UNDERLYING_SYMBOL = "BTCUSD"
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
        "User-Agent": "python-basket-monitor-per-expiry",
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


async def get_spot_price(session, symbol=UNDERLYING_SYMBOL):
    path = "/v2/tickers/" + symbol
    async with session.get(BASE_URL + path) as resp:
        data = await resp.json()
    if not data.get("success"):
        raise RuntimeError(f"Spot ticker API error for {symbol}: {data}")
    result = data.get("result") or {}
    for key in ("spot_price", "mark_price", "close", "last_traded_price"):
        value = result.get(key)
        if value is not None:
            return float(value)
    raise RuntimeError(f"No usable spot field found for {symbol}: {data}")


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


def parse_option_symbol(symbol):
    parts = symbol.split("-")
    if len(parts) < 4:
        raise ValueError(f"Invalid option symbol: {symbol}")
    option_type = parts[0]
    underlying = parts[1]
    strike = float(parts[2])
    expiry = parts[3]
    if option_type not in ("C", "P"):
        raise ValueError(f"Invalid option type in symbol: {symbol}")
    return option_type, underlying, strike, expiry


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
    total = 0.0
    details = []
    for p in positions:
        pnl = float(p.get("unrealized_cashflow", 0) or 0) + float(p.get("realized_cashflow", 0) or 0)
        total += pnl
        details.append(
            {
                "symbol": p.get("product_symbol"),
                "size": float(p.get("size", 0)),
                "entry_price": float(p.get("entry_price", 0) or 0),
                "product_id": p.get("product_id"),
                "pnl": pnl,
            }
        )
    return total, details


def option_payoff_component(symbol, size, entry_price, spot):
    option_type, _, strike, _ = parse_option_symbol(symbol)
    intrinsic = max(spot - strike, 0.0) if option_type == "C" else max(strike - spot, 0.0)
    return size * (intrinsic - entry_price)


def basket_payoff_at_expiry(details, spot):
    return sum(option_payoff_component(d["symbol"], d["size"], d["entry_price"], spot) for d in details)


def find_breakevens(details):
    strikes = sorted({parse_option_symbol(d["symbol"])[2] for d in details})
    if not strikes:
        return []

    grid = [max(0.0, strikes[0] - BREAKEVEN_SOLVER_PAD)] + strikes + [strikes[-1] + BREAKEVEN_SOLVER_PAD]
    roots = []
    prev_x = grid[0]
    prev_y = basket_payoff_at_expiry(details, prev_x)

    for x in grid[1:]:
        y = basket_payoff_at_expiry(details, x)
        if prev_y == 0:
            roots.append(prev_x)
        elif y == 0:
            roots.append(x)
        elif prev_y * y < 0:
            lo, hi = prev_x, x
            flo, fhi = prev_y, y
            for _ in range(BREAKEVEN_SOLVER_ITERATIONS):
                mid = (lo + hi) / 2.0
                fmid = basket_payoff_at_expiry(details, mid)
                if flo * fmid <= 0:
                    hi, fhi = mid, fmid
                else:
                    lo, flo = mid, fmid
            roots.append((lo + hi) / 2.0)
        prev_x, prev_y = x, y

    return sorted(set(round(r, 2) for r in roots))


def breakeven_band(details):
    roots = find_breakevens(details)
    if len(roots) >= 2:
        return roots[0], roots[-1], roots
    if len(roots) == 1:
        return roots[0], roots[0], roots
    return None, None, roots


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


async def handle_expiry_breach(
    session,
    expiry,
    total_pnl,
    details,
    trigger_reason,
    spot_price=None,
    lower_be=None,
    upper_be=None,
    breach_count=0,
):
    
    reason_lines = [f"Reason: {trigger_reason}"]
    if spot_price is not None:
        reason_lines.append(f"Underlying spot: {spot_price:.2f}")
    if lower_be is not None and upper_be is not None:
        reason_lines.append(f"Breakeven band: {lower_be:.2f} - {upper_be:.2f}")
    if breach_count:
        reason_lines.append(f"Consecutive breach polls: {breach_count}")

    alert_msg = (
        f"🚨 HYBRID STOP TRIGGERED for expiry {expiry} 🚨\n"
        f"Basket PnL: {total_pnl:.2f} USD\n"
        + "\n".join(reason_lines)
        + f"\nClosing all {len(details)} positions for expiry {expiry}..."
    )
    print(alert_msg)
    await send_telegram(session, alert_msg)

    positions = [{"product_id": d["product_id"], "product_symbol": d["symbol"], "size": d["size"]} for d in details]
    t0 = time.perf_counter()
    close_results = await close_basket_concurrently(session, positions)
    elapsed = time.perf_counter() - t0
    print(f"[{expiry}] All close orders fired in {elapsed:.2f}s")

    failed_legs = [r for r in close_results if isinstance(r, dict) and r.get("error")]
    if failed_legs:
        fail_msg = "\n".join(
            f"- {r['symbol']}: failed after {r.get('attempts', '?')} attempt(s) - {r['error']}" for r in failed_legs
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
        f"Trigger: {trigger_reason}\n"
        f"Estimated pre-close basket PnL: {total_pnl:.2f} USD\n"
        f"Actual realized PnL from fills: {realized_pnl:.2f} USD"
    )
    print(report_msg)
    await send_telegram(session, report_msg)
    return expiry


async def monitor_loop():
    print("Starting per-expiry basket hybrid-stop monitor (async)... Ctrl+C to stop.")
    closed_expiries = set()
    last_status_sent = 0.0
    breach_state = defaultdict(int)

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        while True:
            try:
                positions = await get_open_positions(session)
                groups = group_positions_by_expiry(positions)
                if not groups:
                    print("No open positions. Exiting monitor.")
                    await send_telegram(session, "ℹ️ No open positions found. Monitor stopped.")
                    break

                spot_price = await get_spot_price(session)
                breach_tasks = []
                summary_lines = []
                active_expiries = set()

                for expiry, pos_list in groups.items():
                    if expiry in closed_expiries:
                        continue
                    active_expiries.add(expiry)
                    total_pnl, details = compute_basket_pnl(pos_list)
                    lower_be, upper_be, roots = breakeven_band(details)

                    breach_side = None
                    if lower_be is not None and spot_price < lower_be:
                        breach_side = "below"
                    elif upper_be is not None and spot_price > upper_be:
                        breach_side = "above"

                    if breach_side:
                        breach_state[expiry] += 1
                    else:
                        breach_state[expiry] = 0

                    be_text = " / ".join(f"{r:.2f}" for r in roots) if roots else "N/A"
                    summary_lines.append(
                        f"Expiry {expiry}: PnL {total_pnl:.2f} USD | Spot {spot_price:.2f} | BEs {be_text} | BreachCount {breach_state[expiry]}"
                    )

                    emergency_hit = total_pnl <= EMERGENCY_PNL_LIMIT_PER_EXPIRY
                    confirmed_be_breach = breach_side is not None and breach_state[expiry] >= BREAKEVEN_CONFIRM_POLLS

                    if emergency_hit:
                        breach_tasks.append(
                            handle_expiry_breach(
                                session,
                                expiry,
                                total_pnl,
                                details,
                                trigger_reason=(
                                    f"Emergency basket PnL threshold breached ({total_pnl:.2f} <= {EMERGENCY_PNL_LIMIT_PER_EXPIRY:.2f})"
                                ),
                                spot_price=spot_price,
                                lower_be=lower_be,
                                upper_be=upper_be,
                                breach_count=breach_state[expiry],
                            )
                        )
                    elif confirmed_be_breach:
                        breach_tasks.append(
                            handle_expiry_breach(
                                session,
                                expiry,
                                total_pnl,
                                details,
                                trigger_reason=(
                                    f"Underlying {breach_side} breakeven band for {BREAKEVEN_CONFIRM_POLLS} consecutive polls"
                                ),
                                spot_price=spot_price,
                                lower_be=lower_be,
                                upper_be=upper_be,
                                breach_count=breach_state[expiry],
                            )
                        )

                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Per-expiry hybrid stop status:\n" + "\n".join(summary_lines))

                now = time.time()
                if summary_lines and (now - last_status_sent >= STATUS_UPDATE_INTERVAL_SEC):
                    status_msg = f"📊 Hybrid Stop Status Update ({time.strftime('%Y-%m-%d %H:%M:%S')})\n" + "\n".join(summary_lines)
                    await send_telegram(session, status_msg)
                    last_status_sent = now

                if breach_tasks:
                    finished_expiries = await asyncio.gather(*breach_tasks, return_exceptions=True)
                    for e in finished_expiries:
                        if isinstance(e, str):
                            closed_expiries.add(e)
                            breach_state.pop(e, None)

                stale = set(breach_state.keys()) - active_expiries
                for expiry in stale:
                    breach_state.pop(expiry, None)

                remaining_expiries = active_expiries - closed_expiries
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
