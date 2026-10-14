# Basket Monitor

Python tools for monitoring Delta Exchange options positions.

## Choose a strategy monitor

| Strategy | Program | Purpose |
| --- | --- | --- |
| Ratio Spread Strategy | `delta_per_expiry_hybrid_stop_fixed.py` | Monitor positions opened by the Ratio Spread Strategy. |
| 1 DTE Short Straddle Strategy | `delta_account_pnl_tp_sl_guard.py` | Monitor the account's open positions for the 1 DTE Short Straddle TP/SL thresholds. |

> **Account-wide monitor:** `delta_account_pnl_tp_sl_guard.py` checks all open
> Delta Exchange positions in the account and can submit reduce-only market
> orders to close them when the configured TP or SL is reached. Use it when
> those open positions are the ones intended to be managed by the 1 DTE Short
> Straddle Strategy.

## Setup

From PowerShell in the repository folder:

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Configure the required Delta Exchange API credentials and Telegram bot
credentials in `.env`. For the account-wide TP/SL monitor, also set the
positive thresholds:

```dotenv
DELTA_TAKE_PROFIT_USD=...
DELTA_STOP_LOSS_USD=...
```

## Run a monitor

Run only the monitor for the strategy whose positions you want to manage.

### Ratio Spread Strategy

```powershell
.\.venv\Scripts\python.exe .\delta_per_expiry_hybrid_stop_fixed.py
```

### 1 DTE Short Straddle Strategy

```powershell
.\.venv\Scripts\python.exe .\delta_account_pnl_tp_sl_guard.py
```

The account-wide monitor reads positions from Delta Exchange's
`/v2/positions/margined` endpoint. It calculates PnL per position as
`unrealized_cashflow + realized_cashflow`, matching the reference monitor's
`compute_basket_pnl` logic, and groups results by settlement asset.

When the account-wide TP or SL threshold is reached, it submits reduce-only
market close orders for every open position concurrently, with one retry per
failed leg. It continues polling and retrying closes until Delta reports no
open positions. If no positions are open at startup, it exits. Check Delta
Exchange after a trigger; failed close orders are reported for manual
follow-up.

The console report shows unrealized PnL, realized cashflow, combined PnL, and
the configured TP/SL thresholds on each poll. Console colors are enabled in an
interactive terminal and can be disabled by setting `NO_COLOR`.

## Live PnL dashboard

When `delta_account_pnl_tp_sl_guard.py` starts, it serves a local dashboard at:

**http://127.0.0.1:8765/**

The dashboard includes interactive PnL history grouped by settlement asset,
selectable chart series, and crosshair tooltips showing the poll time and PnL
values. It checks for new polls every five seconds and updates automatically.
The server listens only on the local computer and stops when the monitor exits.

The monitor also regenerates `delta_account_unrealized_pnl.svg` as a static
chart snapshot on each poll.

## PnL history database

Each successful poll is saved to `delta_account_pnl_history.sqlite3` in the
repository folder. The database stores a poll timestamp and position count,
plus each open position's size, settlement asset, unrealized cashflow, realized
cashflow, and combined PnL. Timestamps are stored in UTC; the console and
charts display local time.

### Query the database with Python

The `sqlite3` command-line utility is optional. Python includes SQLite support,
so no separate database program is required. From PowerShell in the repository
folder, start the project's Python interpreter:

```powershell
.\.venv\Scripts\python.exe
```

At the Python `>>>` prompt, connect to the database and list its tables:

```python
import sqlite3
db = sqlite3.connect("delta_account_pnl_history.sqlite3")
print(*db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"), sep="\n")
```

Show the latest poll's PnL by settlement asset. The columns include the raw API
unrealized cashflow, realized cashflow, and the monitor's Delta-matching PnL:

```python
sql = """
SELECT p.poll_id, p.polled_at, p.open_positions, x.settlement_asset,
       SUM(x.unrealized_cashflow), SUM(x.realized_cashflow), SUM(x.combined_pnl)
FROM pnl_polls AS p
JOIN position_pnl AS x ON x.poll_id = p.poll_id
WHERE p.poll_id = (SELECT MAX(poll_id) FROM pnl_polls)
GROUP BY p.poll_id, p.polled_at, p.open_positions, x.settlement_asset
"""
print(*db.execute(sql), sep="\n")
```

Show each position's values in the latest poll:

```python
sql = """
SELECT x.product_symbol, x.settlement_asset, x.size,
       x.unrealized_cashflow, x.realized_cashflow, x.combined_pnl
FROM position_pnl AS x
WHERE x.poll_id = (SELECT MAX(poll_id) FROM pnl_polls)
ORDER BY x.settlement_asset, x.product_symbol
"""
print(*db.execute(sql), sep="\n")
```

Show the 50 most recent poll totals by settlement asset:

```python
sql = """
SELECT p.poll_id, p.polled_at, x.settlement_asset, SUM(x.combined_pnl)
FROM pnl_polls AS p
JOIN position_pnl AS x ON x.poll_id = p.poll_id
GROUP BY p.poll_id, p.polled_at, x.settlement_asset
ORDER BY p.poll_id DESC
LIMIT 50
"""
print(*db.execute(sql), sep="\n")
```

Close the database connection and exit Python:

```python
db.close()
exit()
```
