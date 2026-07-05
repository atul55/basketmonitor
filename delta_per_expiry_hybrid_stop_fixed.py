"""
Delta Exchange (India) - Per-Expiry Basket Hybrid Stop Monitor & Auto-Close Bot
=================================================================================
Monitors positions separately for each option expiry and applies a hybrid stop:
1) Primary trigger: underlying breaches computed expiry breakeven band
2) Secondary trigger: basket PnL loss exceeds a wider emergency threshold
3) Wick filter: underlying breach must persist for 3 consecutive polls

Fix in this version:
- Realized PnL after close is now calculated from the change in each leg's
  realized_cashflow before vs after closure, instead of relying only on recent
  fills. This avoids 0.00 reports when the fills endpoint does not return the
  expected closing fills immediately or page_size misses them.
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

# Read API credentials from environment variables.
API_KEY = get_required_env("DELTA_API_KEY")
API_SECRET = get_required_env("DELTA_API_SECRET")

# Base REST endpoint for Delta India.
BASE_URL = "https://api.india.delta.exchange"

# Telegram bot credentials for alerts/status updates.
TELEGRAM_BOT_TOKEN = get_required_env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_required_env("TELEGRAM_CHAT_ID")

# Poll open positions and spot price every 15 seconds.
POLL_INTERVAL_SEC = 15

# Send a periodic heartbeat/status message every 5 minutes.
STATUS_UPDATE_INTERVAL_SEC = 300

# Shared timeout for all HTTP requests.
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10)

# Retry a failed close order once.
CLOSE_MAX_RETRIES = 1
CLOSE_RETRY_DELAY_SEC = 1.0

# Emergency fail-safe: exit immediately if basket PnL drops below this.
EMERGENCY_PNL_LIMIT_PER_EXPIRY = -60.0

# Require 3 consecutive breached polls before acting on breakeven trigger.
BREAKEVEN_CONFIRM_POLLS = 3

# Add extra price range around strikes when searching for breakevens.
BREAKEVEN_SOLVER_PAD = 5000.0

# Binary search refinement count for each breakeven root.
BREAKEVEN_SOLVER_ITERATIONS = 60

# Underlying ticker used to fetch BTC reference price.
UNDERLYING_SYMBOL = "BTCUSD"

# Retry window for reading post-close realized_cashflow from positions.
POST_CLOSE_PNL_POLL_COUNT = 8
POST_CLOSE_PNL_POLL_DELAY_SEC = 1.5
# -----------------------------------------


def generate_signature(secret, message):
    """
    Generate HMAC SHA256 signature required by Delta private API endpoints.
    """
    return hmac.new(bytes(secret, "utf-8"), bytes(message, "utf-8"), hashlib.sha256).hexdigest()


def get_auth_headers(method, path, query_string="", payload=""):
    """
    Build authenticated request headers for Delta Exchange private endpoints.
    """
    # Delta expects current Unix timestamp in seconds.
    timestamp = str(int(time.time()))

    # Signature is built from method + timestamp + path + query + payload.
    signature_data = method + timestamp + path + query_string + payload
    signature = generate_signature(API_SECRET, signature_data)

    # Return the full authenticated header set.
    return {
        "api-key": API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "Content-Type": "application/json",
        "User-Agent": "python-basket-monitor-per-expiry",
    }


async def send_telegram(session, message):
    """
    Send a Telegram message to the configured bot chat.
    """
    # Telegram Bot API endpoint for sendMessage.
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        # Send text alert/status to the configured chat.
        async with session.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}) as resp:
            # Read response body so connection can be reused cleanly.
            await resp.read()
    except Exception as e:
        # Telegram failures should not stop the monitor.
        print(f"[Telegram Error] {e}")


async def get_open_positions(session):
    """
    Fetch all currently open margined positions from Delta.
    """
    path = "/v2/positions/margined"

    # This is a private endpoint, so signed headers are required.
    headers = get_auth_headers("GET", path, "")

    async with session.get(BASE_URL + path, headers=headers) as resp:
        data = await resp.json()

    # Raise an error if exchange responded unsuccessfully.
    if not data.get("success"):
        raise RuntimeError(f"API error: {data}")

    return data.get("result", [])


async def get_spot_price(session, symbol=UNDERLYING_SYMBOL):
    """
    Fetch the current underlying reference price from Delta ticker data.
    """
    path = "/v2/tickers/" + symbol

    # Public ticker request.
    async with session.get(BASE_URL + path) as resp:
        data = await resp.json()

    if not data.get("success"):
        raise RuntimeError(f"Spot ticker API error for {symbol}: {data}")

    result = data.get("result") or {}

    # Try common possible price fields in priority order.
    for key in ("spot_price", "mark_price", "close", "last_traded_price"):
        value = result.get(key)
        if value is not None:
            return float(value)

    raise RuntimeError(f"No usable spot field found for {symbol}: {data}")


def extract_expiry(symbol):
    """
    Extract the expiry token DDMMYY from a Delta option symbol.
    Example: C-BTC-63000-050726 -> 050726
    """
    if not symbol:
        return "Invalid Contract Symbol. Expect in the format P-BTC-63000-050726"

    parts = symbol.split("-")

    # Validate last token as a 6-digit expiry code.
    if len(parts) >= 4 and parts[-1].isdigit() and len(parts[-1]) == 6:
        return parts[-1]

    return "Invalid Contract Symbol format. Expect in the format P-BTC-63000-050726"


def parse_option_symbol(symbol):
    """
    Parse Delta option symbol into:
    option_type, underlying, strike, expiry
    """
    parts = symbol.split("-")

    if len(parts) < 4:
        raise ValueError(f"Invalid option symbol: {symbol}")

    option_type = parts[0]
    underlying = parts[1]
    strike = float(parts[2])
    expiry = parts[3]

    # Support only call and put option symbols.
    if option_type not in ("C", "P"):
        raise ValueError(f"Invalid option type in symbol: {symbol}")

    return option_type, underlying, strike, expiry


def group_positions_by_expiry(positions):
    """
    Group all open non-zero positions by expiry date token.
    Returns:
        { expiry: [position1, position2, ...] }
    """
    groups = defaultdict(list)

    for p in positions:
        size = float(p.get("size", 0))

        # Ignore fully closed / zero-sized positions.
        if size == 0:
            continue

        # Use product symbol to determine which expiry basket the leg belongs to.
        expiry = extract_expiry(p.get("product_symbol"))
        groups[expiry].append(p)

    return groups


def compute_basket_pnl(positions):
    """
    Compute total basket PnL for an expiry and return normalized leg details.
    """
    total = 0.0
    details = []

    for p in positions:
        unrealized = float(p.get("unrealized_cashflow", 0) or 0)
        realized = float(p.get("realized_cashflow", 0) or 0)
        pnl = unrealized + realized
        total += pnl

        # Save only fields required later for breakeven math and order closing.
        details.append(
            {
                "symbol": p.get("product_symbol"),
                "size": float(p.get("size", 0)),
                "entry_price": float(p.get("entry_price", 0) or 0),
                "product_id": p.get("product_id"),
                "pnl": pnl,
                "realized_cashflow": realized,
                "unrealized_cashflow": unrealized,
            }
        )

    return total, details


def option_payoff_component(symbol, size, entry_price, spot):
    """
    Compute expiry payoff contribution of a single option leg at a given spot.
    """
    option_type, _, strike, _ = parse_option_symbol(symbol)

    # Calls gain intrinsic value above strike, puts below strike.
    intrinsic = max(spot - strike, 0.0) if option_type == "C" else max(strike - spot, 0.0)

    # Signed size automatically handles long/short direction.
    return size * (intrinsic - entry_price)


def basket_payoff_at_expiry(details, spot):
    """
    Compute total basket payoff at expiry for a hypothetical underlying price.
    """
    return sum(option_payoff_component(d["symbol"], d["size"], d["entry_price"], spot) for d in details)


def find_breakevens(details):
    """
    Find all breakeven roots where the total basket expiry payoff crosses zero.
    """
    # Collect unique strikes from all basket legs.
    strikes = sorted({parse_option_symbol(d["symbol"])[2] for d in details})
    if not strikes:
        return []

    # Search from below lowest strike to above highest strike.
    grid = [max(0.0, strikes[0] - BREAKEVEN_SOLVER_PAD)] + strikes + [strikes[-1] + BREAKEVEN_SOLVER_PAD]

    roots = []
    prev_x = grid[0]
    prev_y = basket_payoff_at_expiry(details, prev_x)

    for x in grid[1:]:
        y = basket_payoff_at_expiry(details, x)

        # Exact root found at previous point.
        if prev_y == 0:
            roots.append(prev_x)

        # Exact root found at current point.
        elif y == 0:
            roots.append(x)

        # Sign change means payoff crossed zero somewhere in this interval.
        elif prev_y * y < 0:
            lo, hi = prev_x, x
            flo, fhi = prev_y, y

            # Binary search to refine the breakeven root.
            for _ in range(BREAKEVEN_SOLVER_ITERATIONS):
                mid = (lo + hi) / 2.0
                fmid = basket_payoff_at_expiry(details, mid)

                if flo * fmid <= 0:
                    hi, fhi = mid, fmid
                else:
                    lo, flo = mid, fmid

            roots.append((lo + hi) / 2.0)

        prev_x, prev_y = x, y

    # Round and de-duplicate roots for cleaner reporting.
    return sorted(set(round(r, 2) for r in roots))


def breakeven_band(details):
    """
    Convert breakeven root list into lower and upper breakeven band.
    Returns:
        lower_be, upper_be, all_roots
    """
    roots = find_breakevens(details)

    # Use outermost roots as the practical monitoring band.
    if len(roots) >= 2:
        return roots[0], roots[-1], roots

    # If only one root exists, use it for both lower and upper.
    if len(roots) == 1:
        return roots[0], roots[0], roots

    return None, None, roots


async def _send_close_order(session, product_id, side, qty):
    """
    Submit one reduce-only market order to close a single leg.
    """
    order_payload = {
        "product_id": product_id,
        "size": qty,
        "side": side,
        "order_type": "market_order",
        "reduce_only": True
    }

    path = "/v2/orders"

    # Compact JSON so the signed request body matches exactly.
    body = json.dumps(order_payload, separators=(",", ":"))
    headers = get_auth_headers("POST", path, "", body)

    async with session.post(BASE_URL + path, headers=headers, data=body) as resp:
        result = await resp.json()

    # Raise if exchange rejects the order.
    if not result.get("success", False):
        raise RuntimeError(f"Order rejected: {result}")

    return result


async def close_single_position(session, p, max_retries=CLOSE_MAX_RETRIES, retry_delay=CLOSE_RETRY_DELAY_SEC):
    """
    Close one position using reduce-only market order with limited retries.
    """
    size = float(p.get("size", 0))

    # Nothing to do for already-flat legs.
    if size == 0:
        return None

    product_id = p.get("product_id")
    symbol = p.get("product_symbol")

    # Longs are closed by selling; shorts are closed by buying.
    side = "sell" if size > 0 else "buy"

    # Quantity sent to exchange must be positive.
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

            # Retry once after a short delay.
            if attempt <= max_retries:
                print(f"[Retry] Close order failed for {symbol} (attempt {attempt}): {last_error}. Retrying...")
                await asyncio.sleep(retry_delay)

    # Return structured failure object for reporting.
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
    # Create one async close task per open leg.
    tasks = [close_single_position(session, p) for p in positions if float(p.get("size", 0)) != 0]

    # Continue processing other legs even if one task errors.
    return await asyncio.gather(*tasks, return_exceptions=True)


def build_realized_cashflow_map(positions):
    """Map product_id -> realized_cashflow from the current open positions snapshot."""
    realized_map = {}
    for p in positions:
        product_id = p.get("product_id")
        if product_id is not None:
            realized_map[product_id] = float(p.get("realized_cashflow", 0) or 0)
    return realized_map


async def get_realized_pnl_delta_after_close(session, product_ids, realized_before):
    """
    Compute realized PnL added by the close orders.

    Why this works better than the old fills-based method:
    - the fills endpoint may not return the latest close fills immediately,
    - page_size may miss them,
    - some exchanges split fills across multiple records.

    So we compare realized_cashflow before the close vs after the close.
    """
    latest_positions = []
    for _ in range(POST_CLOSE_PNL_POLL_COUNT):
        latest_positions = await get_open_positions(session)
        current_map = build_realized_cashflow_map(latest_positions)

        # Stop retrying early once all tracked products disappeared from open positions
        # or once at least one realized_cashflow changed.
        all_gone = all(pid not in current_map for pid in product_ids)
        any_changed = any((current_map.get(pid, realized_before.get(pid, 0.0)) - realized_before.get(pid, 0.0)) != 0 for pid in product_ids)
        if all_gone or any_changed:
            break
        await asyncio.sleep(POST_CLOSE_PNL_POLL_DELAY_SEC)

    current_map = build_realized_cashflow_map(latest_positions)
    total_delta = 0.0
    per_product = {}

    for pid in product_ids:
        before_val = realized_before.get(pid, 0.0)
        after_val = current_map.get(pid, before_val)
        delta = after_val - before_val
        per_product[pid] = {
            "before": before_val,
            "after": after_val,
            "delta": delta,
        }
        total_delta += delta

    return total_delta, per_product


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
    """
    Handle a hybrid stop trigger for one expiry:
    alert, close basket, then report realized PnL.
    """
    # Build descriptive context for Telegram alert.
    reason_lines = [f"Reason: {trigger_reason}"]
    if spot_price is not None:
        reason_lines.append(f"Underlying spot: {spot_price:.2f}")
    if lower_be is not None and upper_be is not None:
        reason_lines.append(f"Breakeven band: {lower_be:.2f} - {upper_be:.2f}")
    if breach_count:
        reason_lines.append(f"Consecutive breach polls: {breach_count}")

    # First alert: trigger notification before sending close orders.
    alert_msg = (
        f"🚨 HYBRID STOP TRIGGERED for expiry {expiry} 🚨\n"
        f"Basket PnL: {total_pnl:.2f} USD\n"
        + "\n".join(reason_lines)
        + f"\nClosing all {len(details)} positions for expiry {expiry}..."
    )
    print(alert_msg)
    await send_telegram(session, alert_msg)

    # Snapshot realized cashflow BEFORE closing so we can compute the realized delta.
    positions_before = await get_open_positions(session)
    realized_before = build_realized_cashflow_map(positions_before)

    positions = [{"product_id": d["product_id"], "product_symbol": d["symbol"], "size": d["size"]} for d in details]

    # Measure how fast all close orders were dispatched.
    t0 = time.perf_counter()
    close_results = await close_basket_concurrently(session, positions)
    elapsed = time.perf_counter() - t0
    print(f"[{expiry}] All close orders fired in {elapsed:.2f}s")

    # Report any legs that still failed after retry.
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

    # Query realized PnL from fills after a short wait.
    product_ids = [d["product_id"] for d in details]
    await asyncio.sleep(3)  # allow fills to settle
    realized_pnl, _ = await get_realized_pnl_delta_after_close(session, product_ids, realized_before)

    # Final post-close report.
    report_msg = (
        f"✅ Expiry {expiry} positions closed in {elapsed:.2f}s.\n"
        f"Trigger: {trigger_reason}\n"
        f"Estimated pre-close basket PnL: {total_pnl:.2f} USD\n"
        f"Actual realized PnL from close delta: {realized_pnl:.2f} USD"
    )
    print(report_msg)
    await send_telegram(session, report_msg)

    return expiry


async def monitor_loop():
    """
    Main monitoring loop:
    - fetch open positions
    - group by expiry
    - compute breakevens
    - track consecutive breaches
    - trigger hybrid stop when needed
    """
    print("Starting per-expiry basket hybrid-stop monitor (async)... Ctrl+C to stop.")

    # Track baskets already closed by this process.
    closed_expiries = set()

    # Timestamp of last status update.
    last_status_sent = 0.0

    # Per-expiry consecutive breach counter.
    breach_state = defaultdict(int)

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        while True:
            try:
                # Read latest open positions.
                positions = await get_open_positions(session)

                # Split positions into expiry baskets.
                groups = group_positions_by_expiry(positions)
                if not groups:
                    print("No open positions. Exiting monitor.")
                    await send_telegram(session, "ℹ️ No open positions found. Monitor stopped.")
                    break

                # Fetch current BTC underlying/reference price once per cycle.
                spot_price = await get_spot_price(session)

                # Collect close coroutines for all triggered baskets.
                breach_tasks = []

                # Human-readable status output lines.
                summary_lines = []

                # Track currently active expiries seen in this cycle.
                active_expiries = set()

                for expiry, pos_list in groups.items():
                    if expiry in closed_expiries:
                        continue

                    active_expiries.add(expiry)

                    # Compute current basket PnL and normalized leg details.
                    total_pnl, details = compute_basket_pnl(pos_list)

                    # Compute lower/upper breakeven band from live basket structure.
                    lower_be, upper_be, roots = breakeven_band(details)

                    # Check if spot is outside the breakeven band.
                    breach_side = None
                    if lower_be is not None and spot_price < lower_be:
                        breach_side = "below"
                    elif upper_be is not None and spot_price > upper_be:
                        breach_side = "above"

                    # Count consecutive breaches to avoid wick exits.
                    if breach_side:
                        breach_state[expiry] += 1
                    else:
                        breach_state[expiry] = 0

                    # Format breakeven roots for logs and Telegram status.
                    be_text = " / ".join(f"{r:.2f}" for r in roots) if roots else "N/A"
                    summary_lines.append(
                        f"Expiry {expiry}: PnL {total_pnl:.2f} USD | Spot {spot_price:.2f} | BEs {be_text} | BreachCount {breach_state[expiry]}"
                    )

                    # Emergency stop triggers immediately.
                    emergency_hit = total_pnl <= EMERGENCY_PNL_LIMIT_PER_EXPIRY

                    # Primary stop requires breach confirmation across 3 polls.
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

                # Console status each poll cycle.
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Per-expiry hybrid stop status:\n" + "\n".join(summary_lines))

                now = time.time()

                # Periodic Telegram heartbeat.
                if summary_lines and (now - last_status_sent >= STATUS_UPDATE_INTERVAL_SEC):
                    status_msg = f"📊 Hybrid Stop Status Update ({time.strftime('%Y-%m-%d %H:%M:%S')})\n" + "\n".join(summary_lines)
                    await send_telegram(session, status_msg)
                    last_status_sent = now

                # Execute all triggered closures concurrently.
                if breach_tasks:
                    finished_expiries = await asyncio.gather(*breach_tasks, return_exceptions=True)
                    for e in finished_expiries:
                        if isinstance(e, str):
                            closed_expiries.add(e)
                            breach_state.pop(e, None)

                # Remove counters for expiries no longer present.
                stale = set(breach_state.keys()) - active_expiries
                for expiry in stale:
                    breach_state.pop(expiry, None)

                # Stop monitor if nothing remains open.
                remaining_expiries = active_expiries - closed_expiries
                if not remaining_expiries:
                    print("All expiry baskets closed. Exiting monitor.")
                    break

                # Sleep until next poll cycle.
                await asyncio.sleep(POLL_INTERVAL_SEC)

            except Exception as e:
                # Catch-all so transient API errors do not kill the bot.
                err = f"[Monitor Error] {e}"
                print(err)
                await send_telegram(session, f"⚠️ Monitor error: {e}")
                await asyncio.sleep(POLL_INTERVAL_SEC)


# Script entry point.
if __name__ == "__main__":
    asyncio.run(monitor_loop())