"""Monitor account-wide Delta Exchange PnL and close positions at TP/SL."""

import asyncio
from contextlib import closing
import hashlib
import hmac
import html
import json
import math
import os
import sqlite3
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone
from pathlib import Path
from threading import Thread

import aiohttp

from config import get_required_env


def get_positive_threshold(name):
    value = get_required_env(name)
    try:
        threshold = float(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive number; got {value!r}.") from exc

    if not math.isfinite(threshold) or threshold <= 0:
        raise RuntimeError(f"{name} must be a positive finite number; got {value!r}.")

    return threshold


API_KEY = get_required_env("DELTA_API_KEY")
API_SECRET = get_required_env("DELTA_API_SECRET")
BASE_URL = "https://api.india.delta.exchange"

TELEGRAM_BOT_TOKEN = get_required_env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_required_env("TELEGRAM_CHAT_ID")

TAKE_PROFIT_USD = get_positive_threshold("DELTA_TAKE_PROFIT_USD")
STOP_LOSS_USD = get_positive_threshold("DELTA_STOP_LOSS_USD")

POLL_INTERVAL_SEC = 15
STATUS_UPDATE_INTERVAL_SEC = 300
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10)
CLOSE_MAX_RETRIES = 1
CLOSE_RETRY_DELAY_SEC = 1.0
DATABASE_PATH = Path(__file__).resolve().with_name("delta_account_pnl_history.sqlite3")
CHART_PATH = Path(__file__).resolve().with_name("delta_account_unrealized_pnl.svg")
DASHBOARD_HOST = "127.0.0.1"
DASHBOARD_PORT = 8765

COLOR_RESET = "\033[0m"
COLOR_GREEN = "\033[32m"
COLOR_RED = "\033[31m"
COLOR_YELLOW = "\033[33m"
COLOR_CYAN = "\033[36m"
COLOR_BOLD = "\033[1m"


def supports_color():
    return sys.stdout.isatty() and "NO_COLOR" not in os.environ


def colorize(text, color, enabled=None):
    if enabled is None:
        enabled = supports_color()
    if not enabled:
        return text
    return f"{color}{text}{COLOR_RESET}"


def colorize_amount(amount, enabled=None, width=0):
    color = COLOR_GREEN if amount > 0 else COLOR_RED if amount < 0 else COLOR_YELLOW
    return colorize(f"{amount:+.2f}".rjust(width), color, enabled)


def local_datetime(value=None):
    """Return a timezone-aware datetime in the machine's configured timezone."""
    if value is None:
        return datetime.now().astimezone()
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone()


def local_timezone_label():
    local_now = local_datetime()
    zone_name = local_now.tzname() or "local"
    offset = local_now.strftime("%z")
    if len(offset) == 5:
        offset = f"{offset[:3]}:{offset[3:]}"
    return f"{zone_name} (UTC{offset})"


def generate_signature(secret, message):
    return hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
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
        "User-Agent": "python-basket-monitor-account-pnl-tp-sl",
    }


async def send_telegram(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with session.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": message},
        ) as response:
            await response.read()
    except Exception as exc:
        print(f"[Telegram Error] {exc}")


async def get_open_positions(session):
    """Fetch open positions using the same endpoint as the reference monitor."""
    path = "/v2/positions/margined"
    headers = get_auth_headers("GET", path)
    async with session.get(BASE_URL + path, headers=headers) as response:
        data = await response.json()

    if not data.get("success"):
        raise RuntimeError(f"Positions API error: {data}")

    positions = data.get("result")
    if not isinstance(positions, list):
        raise RuntimeError(f"Unexpected positions response: {data}")

    return positions


def compute_total_pnl(positions):
    """Match the reference monitor's per-position cashflow PnL calculation."""
    pnl_by_asset = {}
    details = []

    for position in positions:
        size = float(position.get("size", 0) or 0)
        if size == 0:
            continue

        unrealized = float(position.get("unrealized_cashflow", 0) or 0)
        realized = float(position.get("realized_cashflow", 0) or 0)
        pnl = unrealized + realized
        if not all(math.isfinite(value) for value in (size, unrealized, realized, pnl)):
            raise RuntimeError(
                f"Non-finite position value received for {position.get('product_symbol')}."
            )

        product = position.get("product") or {}
        settling_asset = product.get("settling_asset") or {}
        asset = (
            settling_asset.get("symbol")
            or position.get("settling_asset_symbol")
            or "UNKNOWN"
        )
        pnl_by_asset[asset] = pnl_by_asset.get(asset, 0.0) + pnl
        details.append(
            {
                "product_id": position.get("product_id"),
                "symbol": position.get("product_symbol"),
                "size": size,
                "unrealized_cashflow": unrealized,
                "realized_cashflow": realized,
                "pnl": pnl,
                "settling_asset": asset,
            }
        )

    return pnl_by_asset, details


def initialize_pnl_database(database_path=DATABASE_PATH):
    """Create the poll and per-position history tables if needed."""
    with closing(sqlite3.connect(database_path)) as connection:
        with connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS pnl_polls (
                    poll_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    polled_at TEXT NOT NULL,
                    open_positions INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS position_pnl (
                    poll_id INTEGER NOT NULL REFERENCES pnl_polls(poll_id),
                    product_id INTEGER,
                    product_symbol TEXT NOT NULL,
                    size REAL NOT NULL,
                    settlement_asset TEXT NOT NULL,
                    unrealized_cashflow REAL NOT NULL,
                    realized_cashflow REAL NOT NULL,
                    combined_pnl REAL NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_position_pnl_poll_asset "
                "ON position_pnl(poll_id, settlement_asset)"
            )


def record_pnl_poll(details, database_path=DATABASE_PATH):
    """Persist this poll and the realized/unrealized values for each open leg."""
    polled_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with closing(sqlite3.connect(database_path)) as connection:
        with connection:
            connection.execute("PRAGMA foreign_keys = ON")
            cursor = connection.execute(
                "INSERT INTO pnl_polls (polled_at, open_positions) VALUES (?, ?)",
                (polled_at, len(details)),
            )
            poll_id = cursor.lastrowid
            connection.executemany(
                """
                INSERT INTO position_pnl (
                    poll_id, product_id, product_symbol, size, settlement_asset,
                    unrealized_cashflow, realized_cashflow, combined_pnl
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        poll_id,
                        detail["product_id"],
                        detail["symbol"] or "UNKNOWN",
                        detail["size"],
                        detail["settling_asset"],
                        detail["unrealized_cashflow"],
                        detail["realized_cashflow"],
                        detail["pnl"],
                    )
                    for detail in details
                ],
            )
    return local_datetime(polled_at).isoformat(timespec="seconds")


def load_dashboard_data(database_path=DATABASE_PATH):
    """Return poll history in the JSON shape used by the live dashboard."""
    with closing(sqlite3.connect(database_path)) as connection:
        poll_rows = connection.execute(
            """
            SELECT poll_id, polled_at, open_positions
            FROM pnl_polls
            ORDER BY poll_id
            """
        ).fetchall()
        value_rows = connection.execute(
            """
            SELECT polls.poll_id, positions.settlement_asset,
                   SUM(positions.combined_pnl)
            FROM pnl_polls AS polls
            JOIN position_pnl AS positions ON positions.poll_id = polls.poll_id
            GROUP BY polls.poll_id, positions.settlement_asset
            ORDER BY polls.poll_id, positions.settlement_asset
            """
        ).fetchall()

    assets = sorted({asset for _, asset, _ in value_rows})
    values_by_poll = {}
    for poll_id, asset, value in value_rows:
        values_by_poll.setdefault(poll_id, {})[asset] = value

    polls = []
    for poll_id, polled_at, open_positions in poll_rows:
        poll_values = values_by_poll.get(poll_id, {})
        polls.append(
            {
                "poll_id": poll_id,
                "timestamp": local_datetime(polled_at).isoformat(timespec="seconds"),
                "open_positions": open_positions,
                "values": {asset: poll_values.get(asset, 0.0) for asset in assets},
            }
        )

    return {
        "timezone": local_timezone_label(),
        "latest_poll_id": polls[-1]["poll_id"] if polls else None,
        "assets": assets,
        "polls": polls,
    }


LIVE_DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<title>Delta Account PnL | Live Dashboard</title>
<style>
:root{font-family:Inter,"Segoe UI",Arial,sans-serif;color:#e8eef8;background:#080d18;font-synthesis:none}
*{box-sizing:border-box}
body{margin:0;min-width:320px;background:radial-gradient(ellipse at 45% -20%,#18335a 0,transparent 48%),#080d18}
.shell{max-width:1440px;margin:auto;padding:32px clamp(16px,4vw,56px) 48px}
header{display:flex;justify-content:space-between;align-items:flex-start;gap:24px;margin-bottom:26px}
.eyebrow{font-size:11px;letter-spacing:.18em;text-transform:uppercase;color:#80a9e8;font-weight:700}
h1{font-size:clamp(25px,4vw,38px);letter-spacing:-.04em;margin:8px 0 6px}
.subtitle{color:#8d9bb2;font-size:14px}
.live{display:flex;align-items:center;gap:8px;border:1px solid #24463e;background:#10251f;color:#82e6ba;padding:9px 13px;border-radius:99px;font-size:12px;font-weight:700}
.dot{width:8px;height:8px;border-radius:50%;background:#38d99a;box-shadow:0 0 12px #38d99a}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin-bottom:16px}
.card,.panel{border:1px solid #202b3d;background:linear-gradient(145deg,rgba(19,29,46,.96),rgba(12,19,32,.96));border-radius:16px;box-shadow:0 16px 48px #0002}
.card{padding:16px 18px;min-height:94px}
.label{color:#8492a9;font-size:11px;text-transform:uppercase;letter-spacing:.11em;font-weight:700}
.value{font-variant-numeric:tabular-nums;font-size:23px;font-weight:700;letter-spacing:-.025em;margin-top:9px}
.positive{color:#55dfab}.negative{color:#ff7185}.neutral{color:#e8eef8}
.panel{padding:20px 20px 14px;overflow:hidden}
.panel-head{display:flex;justify-content:space-between;align-items:center;gap:15px;margin:0 2px 8px;padding-bottom:12px;border-bottom:1px solid #202b3d}
h2{font-size:16px;margin:0;letter-spacing:-.01em}
.meta{color:#8190a7;font-size:12px;text-align:right}
.legend{display:flex;flex-wrap:wrap;gap:8px;margin:10px 2px 3px}
.legend button{border:1px solid #2b3850;background:#111b2b;color:#d7e2f2;border-radius:99px;padding:7px 11px;font-size:12px;cursor:pointer}
.legend button.off{opacity:.42;text-decoration:line-through}
.swatch{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px}
.plot-wrap{position:relative;width:100%;height:min(62vh,540px);min-height:340px}
svg{width:100%;height:100%;overflow:visible}
.axis-label{fill:#8290a6;font-size:11px;font-variant-numeric:tabular-nums}
.grid{stroke:#263246;stroke-width:1}.zero{stroke:#7c8ba3;stroke-width:1.3;stroke-dasharray:5 5}
.axis{stroke:#536178;stroke-width:1}
.series{fill:none;stroke-width:3;stroke-linecap:round;stroke-linejoin:round;filter:drop-shadow(0 2px 5px #0008)}
.area{stroke:none}
.point{stroke:#0d1523;stroke-width:2}
.crosshair{stroke:#b7c5db;stroke-width:1;stroke-dasharray:4 5;pointer-events:none}
.focus{stroke:#f3f7ff;stroke-width:2.3;pointer-events:none}
.hit-area{fill:transparent;cursor:crosshair}
.empty{fill:#8998af;font-size:16px}
.tooltip{position:absolute;z-index:2;display:none;pointer-events:none;min-width:190px;max-width:min(320px,80vw);padding:12px 14px;background:#101827;border:1px solid #3b4b65;border-radius:10px;box-shadow:0 12px 32px #0009;font-size:12px}
.tip-time{font-weight:700;color:#f0f5ff;margin-bottom:8px}.tip-row{display:flex;justify-content:space-between;gap:22px;color:#9fadc2;margin-top:5px}.tip-val{font-variant-numeric:tabular-nums;font-weight:700;color:#eaf0fa}
.footer{color:#68778e;font-size:11px;margin:13px 3px 0}
@media(max-width:620px){.shell{padding-top:22px}.panel{padding:14px 10px}.panel-head{align-items:flex-start;flex-direction:column}.meta{text-align:left}.plot-wrap{height:380px}.live{font-size:0;padding:10px}.live .dot{margin:0}}
</style>
</head>
<body>
<main class="shell">
<header>
  <div><div class="eyebrow">Delta Exchange · Account monitor</div><h1>Unrealized PnL</h1><div class="subtitle">Live settlement-asset performance across open positions</div></div>
  <div class="live"><span class="dot"></span>LIVE · refreshes every 5 seconds</div>
</header>
<section class="cards" id="cards"></section>
<section class="panel">
  <div class="panel-head"><h2>PnL history</h2><div class="meta" id="chart-meta">Waiting for first poll…</div></div>
  <div class="legend" id="legend"></div>
  <div class="plot-wrap"><svg id="chart" viewBox="0 0 1100 520" role="img" aria-label="Interactive unrealized PnL chart"></svg><div class="tooltip" id="tooltip"></div></div>
  <div class="footer">Move the crosshair over the chart to inspect the nearest poll. Values use the monitor’s Delta-matching calculation.</div>
</section>
</main>
<script>
const NS="http://www.w3.org/2000/svg", W=1100,H=520,M={l:82,r:34,t:22,b:86};
const colors=["#6ea8fe","#f77c91","#55d6a5","#c59bff","#ffbd69","#54d2df","#ff8c42","#b6d957"];
const hidden=new Set(); let latestData=null;
const svg=document.getElementById("chart"), tip=document.getElementById("tooltip");
function node(tag,attrs={},text){const e=document.createElementNS(NS,tag);for(const [k,v] of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e}
function fmt(v){return `${v>=0?"+":""}${Number(v).toFixed(2)}`}
function cls(v){return v>0?"positive":v<0?"negative":"neutral"}
function smoothPath(points){
 if(points.length<2)return "";
 let path=`M ${points[0].x} ${points[0].y}`;
 for(let i=0;i<points.length-1;i++){
  const p0=points[Math.max(0,i-1)],p1=points[i],p2=points[i+1],p3=points[Math.min(points.length-1,i+2)];
  path+=` C ${p1.x+(p2.x-p0.x)/6} ${p1.y+(p2.y-p0.y)/6}, ${p2.x-(p3.x-p1.x)/6} ${p2.y-(p3.y-p1.y)/6}, ${p2.x} ${p2.y}`;
 }
 return path;
}
function renderCards(data){
 const cards=document.getElementById("cards");cards.replaceChildren();
 const latest=data.polls.at(-1), count=latest?latest.open_positions:0;
 const entries=[["Open positions",String(count),"neutral"],["Latest poll",latest?new Date(latest.timestamp).toLocaleTimeString():"No data","neutral"]];
 for(const asset of data.assets){entries.push([`${asset} · Combined PnL`,latest?fmt(latest.values[asset]||0):"—",latest?cls(latest.values[asset]||0):"neutral"])}
 for(const [label,value,color] of entries){const card=document.createElement("div");card.className="card";const l=document.createElement("div");l.className="label";l.textContent=label;const v=document.createElement("div");v.className=`value ${color}`;v.textContent=value;card.append(l,v);cards.append(card)}
}
function renderLegend(data){
 const legend=document.getElementById("legend");legend.replaceChildren();
 data.assets.forEach((asset,i)=>{const b=document.createElement("button");b.className=hidden.has(asset)?"off":"";const sw=document.createElement("span");sw.className="swatch";sw.style.background=colors[i%colors.length];b.append(sw,document.createTextNode(asset));b.onclick=()=>{hidden.has(asset)?hidden.delete(asset):hidden.add(asset);draw(data)};legend.append(b)})
}
function draw(data){
 latestData=data;renderCards(data);renderLegend(data);svg.replaceChildren();tip.style.display="none";
 const polls=data.polls, assets=data.assets.filter(a=>!hidden.has(a)),pw=W-M.l-M.r,ph=H-M.t-M.b;
 const meta=document.getElementById("chart-meta");
 meta.textContent=`${polls.length} polls · ${data.timezone}${polls.length?` · latest #${data.latest_poll_id}`:""}`;
 if(!polls.length){svg.append(node("text",{x:W/2,y:H/2,"text-anchor":"middle",class:"empty"},"No poll history yet"));return}
 let vals=[];for(const p of polls)for(const a of assets)vals.push(Number(p.values[a]||0));
 if(!vals.length)vals=[0];let lo=Math.min(0,...vals),hi=Math.max(0,...vals),span=hi-lo||1;lo-=span*.1;hi+=span*.1;
 const x=i=>M.l+(polls.length<2?pw/2:i*pw/(polls.length-1)),y=v=>M.t+(hi-v)*ph/(hi-lo);
 for(let j=0;j<=5;j++){const v=hi-(hi-lo)*j/5,yy=y(v);svg.append(node("line",{x1:M.l,y1:yy,x2:W-M.r,y2:yy,class:"grid"}));svg.append(node("text",{x:M.l-12,y:yy+4,"text-anchor":"end",class:"axis-label"},fmt(v)))}
 svg.append(node("line",{x1:M.l,y1:y(0),x2:W-M.r,y2:y(0),class:"zero"}));
 svg.append(node("line",{x1:M.l,y1:M.t,x2:M.l,y2:H-M.b,class:"axis"}));
 svg.append(node("line",{x1:M.l,y1:H-M.b,x2:W-M.r,y2:H-M.b,class:"axis"}));
 svg.append(node("text",{x:18,y:H/2,transform:`rotate(-90 18 ${H/2})`,"text-anchor":"middle",class:"axis-label"},"PnL by settlement asset"));
 const tickCount=Math.min(5,polls.length),ticks=[...new Set(Array.from({length:tickCount},(_,i)=>Math.round(i*(polls.length-1)/Math.max(tickCount-1,1))))];
 for(const i of ticks){
  const poll=polls[i],d=new Date(poll.timestamp),anchor=polls.length===1?"middle":i===0?"start":i===polls.length-1?"end":"middle",label={"text-anchor":anchor,class:"axis-label"};
  svg.append(node("line",{x1:x(i),y1:H-M.b,x2:x(i),y2:H-M.b+5,class:"axis"}));
  svg.append(node("text",{...label,x:x(i),y:H-M.b+20},d.toLocaleDateString(undefined,{year:"numeric",month:"short",day:"numeric"})));
  svg.append(node("text",{...label,x:x(i),y:H-M.b+35},`${d.toLocaleTimeString()} · #${poll.poll_id}`));
 }
 const defs=node("defs"),clip=node("clipPath",{id:"plot-clip"});
 clip.append(node("rect",{x:M.l,y:M.t,width:pw,height:ph}));defs.append(clip);
 assets.forEach(a=>{
  const ai=data.assets.indexOf(a),gradient=node("linearGradient",{id:`fill-${ai}`,x1:"0",y1:"0",x2:"0",y2:"1"});
  gradient.append(node("stop",{offset:"0%","stop-color":colors[ai%colors.length],"stop-opacity":".28"}),node("stop",{offset:"100%","stop-color":colors[ai%colors.length],"stop-opacity":".015"}));
  defs.append(gradient);
 });
 svg.append(defs);
 const seriesLayer=node("g",{"clip-path":"url(#plot-clip)"});svg.append(seriesLayer);
 assets.forEach(a=>{
  const ai=data.assets.indexOf(a),color=colors[ai%colors.length],coords=polls.map((p,i)=>({x:x(i),y:y(Number(p.values[a]||0))})),curve=smoothPath(coords);
  if(coords.length>1){
   seriesLayer.append(node("path",{d:`${curve} L ${coords.at(-1).x} ${y(0)} L ${coords[0].x} ${y(0)} Z`,class:"area",fill:`url(#fill-${ai})`}));
   seriesLayer.append(node("path",{d:curve,class:"series",stroke:color}));
  }
  coords.forEach(point=>seriesLayer.append(node("circle",{cx:point.x,cy:point.y,r:3.4,class:"point",fill:color})));
 });
 const cross=node("g");const vline=node("line",{class:"crosshair",y1:M.t,y2:H-M.b});const hline=node("line",{class:"crosshair",x1:M.l,x2:W-M.r});const focus=node("g");cross.append(vline,hline,focus);svg.append(cross);
 const hit=node("rect",{x:M.l,y:M.t,width:pw,height:ph,class:"hit-area"});
 hit.addEventListener("pointermove",ev=>{const matrix=svg.getScreenCTM(),pointer=svg.createSVGPoint();pointer.x=ev.clientX;pointer.y=ev.clientY;const local=pointer.matrixTransform(matrix.inverse()),px=local.x;let idx=0;for(let i=1;i<polls.length;i++)if(Math.abs(x(i)-px)<Math.abs(x(idx)-px))idx=i;vline.setAttribute("x1",x(idx));vline.setAttribute("x2",x(idx));hline.setAttribute("y1",local.y);hline.setAttribute("y2",local.y);focus.replaceChildren();assets.forEach(a=>{const ai=data.assets.indexOf(a);focus.append(node("circle",{cx:x(idx),cy:y(Number(polls[idx].values[a]||0)),r:6,class:"focus",fill:colors[ai%colors.length]}))});const r=svg.getBoundingClientRect();showTip(data,idx,ev.clientX-r.left,ev.clientY-r.top)});
 hit.addEventListener("pointerleave",()=>{tip.style.display="none";cross.setAttribute("visibility","hidden")});
 hit.addEventListener("pointerenter",()=>cross.setAttribute("visibility","visible"));cross.setAttribute("visibility","hidden");svg.append(hit);
}
function showTip(data,i,left,top){
 const poll=data.polls[i];tip.replaceChildren();const time=document.createElement("div");time.className="tip-time";time.textContent=`${new Date(poll.timestamp).toLocaleString()} · poll #${poll.poll_id} · ${poll.open_positions} open positions`;tip.append(time);
 data.assets.forEach((asset,ai)=>{const row=document.createElement("div");row.className="tip-row";const name=document.createElement("span");name.textContent=asset;name.style.color=colors[ai%colors.length];const value=document.createElement("span");value.className="tip-val";value.textContent=fmt(poll.values[asset]||0);row.append(name,value);tip.append(row)});
 const wrap=document.querySelector(".plot-wrap"),maxLeft=wrap.clientWidth-tip.offsetWidth-8;tip.style.left=`${Math.max(8,Math.min(left+16,maxLeft))}px`;tip.style.top=`${Math.max(8,Math.min(top+16,wrap.clientHeight-tip.offsetHeight-8))}px`;tip.style.display="block";
}
async function refresh(){try{const r=await fetch("/api/data",{cache:"no-store"});if(!r.ok)throw new Error(`HTTP ${r.status}`);const d=await r.json();if(!latestData||d.latest_poll_id!==latestData.latest_poll_id)draw(d);else if(!latestData.polls.length&&d.polls.length)draw(d)}catch(e){document.getElementById("chart-meta").textContent=`Dashboard data unavailable: ${e.message}`}}
refresh();setInterval(refresh,5000);
window.addEventListener("resize",()=>{if(latestData)draw(latestData)});
</script>
</body>
</html>"""


def start_dashboard_server(
    host=DASHBOARD_HOST,
    port=DASHBOARD_PORT,
    database_path=DATABASE_PATH,
):
    """Serve the live interactive dashboard and its SQLite-backed JSON API."""
    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            route = self.path.split("?", 1)[0]
            if route == "/":
                content = LIVE_DASHBOARD_HTML.encode("utf-8")
                content_type = "text/html; charset=utf-8"
            elif route == "/api/data":
                content = json.dumps(
                    load_dashboard_data(database_path),
                    separators=(",", ":"),
                ).encode("utf-8")
                content_type = "application/json; charset=utf-8"
            else:
                self.send_error(404)
                return

            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, format_string, *args):
            del format_string, args
            return

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    thread = Thread(target=server.serve_forever, name="pnl-dashboard", daemon=True)
    thread.start()
    return server


def write_unrealized_pnl_chart(
    database_path=DATABASE_PATH,
    chart_path=CHART_PATH,
):
    """Regenerate a self-contained SVG chart from all saved poll history."""
    width, height = 1100, 620
    left, right, top, bottom = 90, 250, 78, 94
    plot_width = width - left - right
    plot_height = height - top - bottom
    palette = ("#6ea8fe", "#f77c91", "#55d6a5", "#c59bff", "#ffbd69", "#54d2df")

    with closing(sqlite3.connect(database_path)) as connection:
        poll_rows = connection.execute(
            "SELECT poll_id, polled_at FROM pnl_polls ORDER BY poll_id"
        ).fetchall()
        value_rows = connection.execute(
            """
            SELECT polls.poll_id, assets.settlement_asset,
                   COALESCE(SUM(positions.combined_pnl), 0)
            FROM pnl_polls AS polls
            CROSS JOIN (
                SELECT DISTINCT settlement_asset FROM position_pnl
            ) AS assets
            LEFT JOIN position_pnl AS positions
                ON positions.poll_id = polls.poll_id
                AND positions.settlement_asset = assets.settlement_asset
            GROUP BY polls.poll_id, assets.settlement_asset
            ORDER BY polls.poll_id, assets.settlement_asset
            """
        ).fetchall()

    poll_indices = {poll_id: index for index, (poll_id, _) in enumerate(poll_rows)}
    timestamps = {
        poll_id: local_datetime(timestamp) for poll_id, timestamp in poll_rows
    }
    series = {}
    for poll_id, asset, unrealized in value_rows:
        series.setdefault(asset, []).append((poll_id, unrealized))

    values = [value for points in series.values() for _, value in points]
    if values:
        low = min(0.0, min(values))
        high = max(0.0, max(values))
        span = high - low
        if span == 0:
            span = max(abs(high), 1.0)
        padding = span * 0.1
        low -= padding
        high += padding
    else:
        low, high = -1.0, 1.0

    def x_for(poll_id):
        count = max(len(poll_rows) - 1, 1)
        return left + plot_width * poll_indices[poll_id] / count

    def y_for(value):
        return top + plot_height * (high - value) / (high - low)

    def smooth_path(coordinates):
        if len(coordinates) < 2:
            return ""
        path = [f"M {coordinates[0][0]:.1f},{coordinates[0][1]:.1f}"]
        for index in range(len(coordinates) - 1):
            previous = coordinates[max(0, index - 1)]
            current = coordinates[index]
            following = coordinates[index + 1]
            next_following = coordinates[min(len(coordinates) - 1, index + 2)]
            control_one = (
                current[0] + (following[0] - previous[0]) / 6,
                current[1] + (following[1] - previous[1]) / 6,
            )
            control_two = (
                following[0] - (next_following[0] - current[0]) / 6,
                following[1] - (next_following[1] - current[1]) / 6,
            )
            path.append(
                f"C {control_one[0]:.1f},{control_one[1]:.1f} "
                f"{control_two[0]:.1f},{control_two[1]:.1f} "
                f"{following[0]:.1f},{following[1]:.1f}"
            )
        return " ".join(path)

    def svg_text(x, y, text, *, anchor="start", fill="#d7e2f2", size=13, weight="normal"):
        return (
            f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" '
            f'font-family="Segoe UI, Arial, sans-serif" font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}">{html.escape(str(text))}</text>'
        )

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<defs><linearGradient id="background" x1="0" y1="0" x2="1" y2="1">'
        '<stop offset="0%" stop-color="#14223a"/><stop offset="100%" stop-color="#080d18"/>'
        '</linearGradient><linearGradient id="chart-panel" x1="0" y1="0" x2="0" y2="1">'
        '<stop offset="0%" stop-color="#111d30"/><stop offset="100%" stop-color="#0c1422"/>'
        '</linearGradient><clipPath id="plot-clip"><rect '
        f'x="{left}" y="{top}" width="{plot_width}" height="{plot_height}" rx="8"/>'
        '</clipPath>',
        *[
            f'<linearGradient id="series-fill-{index}" x1="0" y1="0" x2="0" y2="1">'
            f'<stop offset="0%" stop-color="{color}" stop-opacity=".28"/>'
            f'<stop offset="100%" stop-color="{color}" stop-opacity=".015"/>'
            '</linearGradient>'
            for index, color in enumerate(palette)
        ],
        '</defs>',
        f'<rect width="{width}" height="{height}" rx="18" fill="url(#background)"/>',
        f'<rect x="18" y="15" width="{width - 36}" height="{height - 30}" '
        'rx="15" fill="url(#chart-panel)" stroke="#24344c"/>',
        svg_text(left, 37, "Open-Position Unrealized PnL", fill="#edf4ff", size=22, weight="700"),
        svg_text(
            left,
            60,
            f"Settlement asset totals · {local_timezone_label()}",
            fill="#8d9bb2",
            size=13,
        ),
    ]

    for tick in range(5):
        value = high - (high - low) * tick / 4
        y = y_for(value)
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_width}" '
            f'y2="{y:.1f}" stroke="#263246" stroke-width="1"/>'
        )
        parts.append(
            svg_text(left - 12, y + 4, f"{value:+.2f}", anchor="end", fill="#9baac0")
        )

    zero_y = y_for(0.0)
    parts.append(
        f'<line x1="{left}" y1="{zero_y:.1f}" x2="{left + plot_width}" '
        f'y2="{zero_y:.1f}" stroke="#7c8ba3" stroke-width="1.5" '
        'stroke-dasharray="5 5"/>'
    )
    parts.append(
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" '
        f'stroke="#536178" stroke-width="1"/>'
    )
    parts.append(
        f'<line x1="{left}" y1="{top + plot_height}" '
        f'x2="{left + plot_width}" y2="{top + plot_height}" '
        f'stroke="#536178" stroke-width="1"/>'
    )

    if poll_rows:
        tick_count = min(5, len(poll_rows))
        label_indices = sorted(
            {
                round(index * (len(poll_rows) - 1) / max(tick_count - 1, 1))
                for index in range(tick_count)
            }
        )
        for index in label_indices:
            poll_id, _ = poll_rows[index]
            x = x_for(poll_id)
            anchor = (
                "middle"
                if len(poll_rows) == 1
                else "start"
                if index == 0
                else "end"
                if index == len(poll_rows) - 1
                else "middle"
            )
            parts.append(
                f'<line x1="{x:.1f}" y1="{top + plot_height}" '
                f'x2="{x:.1f}" y2="{top + plot_height + 5}" stroke="#536178"/>'
            )
            parts.append(
                svg_text(
                    x,
                    top + plot_height + 22,
                    timestamps[poll_id].strftime("%b %d, %Y"),
                    anchor=anchor,
                    fill="#aab7ca",
                    size=11,
                    weight="600",
                )
            )
            parts.append(
                svg_text(
                    x,
                    top + plot_height + 39,
                    f"{timestamps[poll_id].strftime('%H:%M:%S')} · #{poll_id}",
                    anchor=anchor,
                    fill="#8290a6",
                    size=10,
                )
            )

    if not series:
        parts.append(
            svg_text(
                left + plot_width / 2,
                top + plot_height / 2,
                "Waiting for open-position PnL data",
                anchor="middle",
                fill="#8d9bb2",
                size=16,
            )
        )
    else:
        for index, (asset, points) in enumerate(sorted(series.items())):
            color = palette[index % len(palette)]
            coordinates = [(x_for(poll_id), y_for(value)) for poll_id, value in points]
            parts.append('<g clip-path="url(#plot-clip)">')
            if len(coordinates) > 1:
                curve = smooth_path(coordinates)
                first_x = coordinates[0][0]
                last_x = coordinates[-1][0]
                parts.append(
                    f'<path d="{curve} L {last_x:.1f},{zero_y:.1f} '
                    f'L {first_x:.1f},{zero_y:.1f} Z" '
                    f'fill="url(#series-fill-{index % len(palette)})"/>'
                )
                parts.append(
                    f'<path d="{curve}" fill="none" stroke="{color}" '
                    'stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>'
                )
            for x, y in coordinates:
                parts.append(
                    f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.2" '
                    f'fill="{color}" stroke="#0c1422" stroke-width="1.5"/>'
                )
            parts.append("</g>")
            legend_y = top + 20 + index * 24
            parts.append(
                f'<line x1="{left + plot_width + 25}" y1="{legend_y - 4}" '
                f'x2="{left + plot_width + 48}" y2="{legend_y - 4}" '
                f'stroke="{color}" stroke-width="3" stroke-linecap="round"/>'
            )
            parts.append(svg_text(left + plot_width + 56, legend_y, asset))

    if poll_rows:
        parts.append(
            svg_text(
                left + plot_width,
                height - 23,
                "Last poll: "
                f"{timestamps[poll_rows[-1][0]].isoformat(timespec='seconds')}",
                anchor="end",
                fill="#8290a6",
                size=11,
            )
        )
    parts.append("</svg>")

    chart_path = Path(chart_path)
    temporary_path = chart_path.with_suffix(chart_path.suffix + ".tmp")
    temporary_path.write_text("\n".join(parts), encoding="utf-8")
    temporary_path.replace(chart_path)
    return chart_path


def get_trigger_reason(total_pnl):
    if total_pnl >= TAKE_PROFIT_USD:
        return "TAKE-PROFIT"
    if total_pnl <= -STOP_LOSS_USD:
        return "STOP-LOSS"
    return None


async def send_close_order(session, product_id, side, quantity):
    """Submit a reduce-only market order using the reference monitor's payload."""
    order_payload = {
        "product_id": product_id,
        "size": quantity,
        "side": side,
        "order_type": "market_order",
        "reduce_only": True,
    }
    path = "/v2/orders"
    body = json.dumps(order_payload, separators=(",", ":"))
    headers = get_auth_headers("POST", path, "", body)

    async with session.post(BASE_URL + path, headers=headers, data=body) as response:
        result = await response.json()

    if not result.get("success", False):
        raise RuntimeError(f"Order rejected: {result}")

    return result


async def close_single_position(
    session,
    position,
    max_retries=CLOSE_MAX_RETRIES,
    retry_delay=CLOSE_RETRY_DELAY_SEC,
):
    """Close one open position with a reduce-only market order and one retry."""
    size = float(position.get("size", 0) or 0)
    if size == 0:
        return None

    product_id = position.get("product_id")
    symbol = position.get("product_symbol")
    if product_id is None:
        return {
            "symbol": symbol,
            "error": "Position has no product_id; cannot submit a close order.",
            "attempts": 0,
        }

    quantity = abs(int(size))
    if quantity == 0:
        return {
            "product_id": product_id,
            "symbol": symbol,
            "error": f"Position size {size} gives a zero close quantity.",
            "attempts": 0,
        }

    side = "sell" if size > 0 else "buy"
    last_error = None

    for attempt in range(max_retries + 1):
        try:
            result = await send_close_order(session, product_id, side, quantity)
            return {
                "product_id": product_id,
                "symbol": symbol,
                "result": result,
                "attempts": attempt + 1,
            }
        except Exception as exc:
            last_error = str(exc)
            if attempt < max_retries:
                print(
                    f"[Retry] Close order failed for {symbol} "
                    f"(attempt {attempt + 1}): {last_error}. Retrying..."
                )
                await asyncio.sleep(retry_delay)

    return {
        "product_id": product_id,
        "symbol": symbol,
        "error": last_error,
        "attempts": max_retries + 1,
    }


async def close_all_positions_concurrently(session, positions):
    """Close all currently open positions concurrently, like the reference."""
    tasks = [
        close_single_position(session, position)
        for position in positions
        if float(position.get("size", 0) or 0) != 0
    ]
    return await asyncio.gather(*tasks, return_exceptions=True)


async def handle_tp_sl_trigger(session, positions, trigger_reason, total_pnl):
    """Alert and close all open positions after an account-wide TP/SL trigger."""
    alert = (
        f"🚨 {trigger_reason} TRIGGERED 🚨\n"
        f"Combined PnL: {total_pnl:.2f} USD\n"
        f"TP: +{TAKE_PROFIT_USD:.2f} USD | SL: -{STOP_LOSS_USD:.2f} USD\n"
        f"Closing all {sum(float(p.get('size', 0) or 0) != 0 for p in positions)} "
        "open positions with reduce-only market orders."
    )
    trigger_color = COLOR_GREEN if trigger_reason == "TAKE-PROFIT" else COLOR_RED
    print(colorize(alert, COLOR_BOLD + trigger_color))
    await send_telegram(session, alert)

    started_at = time.perf_counter()
    close_results = await close_all_positions_concurrently(session, positions)
    elapsed = time.perf_counter() - started_at

    failed_positions = [
        result
        for result in close_results
        if isinstance(result, Exception)
        or (isinstance(result, dict) and result.get("error"))
    ]
    successful_count = len(close_results) - len(failed_positions)
    summary = (
        f"{trigger_reason} close orders submitted in {elapsed:.2f}s: "
        f"{successful_count}/{len(close_results)} accepted."
    )
    print(colorize(summary, COLOR_GREEN if not failed_positions else COLOR_YELLOW))

    if failed_positions:
        failure_lines = []
        for result in failed_positions:
            if isinstance(result, Exception):
                failure_lines.append(f"- Close task failed: {result}")
            else:
                failure_lines.append(
                    f"- {result.get('symbol')}: "
                    f"failed after {result.get('attempts', '?')} attempt(s) - "
                    f"{result.get('error')}"
                )
        warning = (
            "⚠️ WARNING: some positions may remain open after close attempts:\n"
            + "\n".join(failure_lines)
            + "\nCheck Delta Exchange and intervene manually if needed."
        )
        print(colorize(warning, COLOR_RED))
        await send_telegram(session, warning)
    else:
        await send_telegram(session, f"✅ {summary}")


def format_amounts_by_asset(amounts, color=False):
    return " | ".join(
        f"{asset} {colorize_amount(amount, color)}"
        for asset, amount in sorted(amounts.items())
    )


def format_pnl_report(details, pnl_by_asset, total_pnl, timestamp=None, color=None):
    """Format a readable console snapshot with unrealized PnL emphasized."""
    if color is None:
        color = supports_color()

    unrealized_by_asset = {}
    realized_by_asset = {}
    for detail in details:
        asset = detail["settling_asset"]
        unrealized_by_asset[asset] = (
            unrealized_by_asset.get(asset, 0.0) + detail["unrealized_cashflow"]
        )
        realized_by_asset[asset] = (
            realized_by_asset.get(asset, 0.0) + detail["realized_cashflow"]
        )

    symbol_width = max(
        len("POSITION"),
        *(len(str(detail["symbol"] or "UNKNOWN")) for detail in details),
    )
    asset_width = max(
        len("ASSET"),
        *(len(str(detail["settling_asset"])) for detail in details),
    )
    size_width = max(
        len("SIZE"),
        *(len(f"{detail['size']:g}") for detail in details),
    )
    separator_width = (
        symbol_width + asset_width + size_width + 51
    )
    timestamp = timestamp or local_datetime().isoformat(timespec="seconds")

    lines = [
        "",
        colorize("=" * separator_width, COLOR_CYAN, color),
        colorize(
            f"ACCOUNT PNL  |  {timestamp}  |  {len(details)} open position(s)",
            COLOR_BOLD + COLOR_CYAN,
            color,
        ),
        (
            f"  TP threshold: +{TAKE_PROFIT_USD:.2f} USD  |  "
            f"SL threshold: -{STOP_LOSS_USD:.2f} USD"
        ),
        colorize("-" * separator_width, COLOR_CYAN, color),
        f"  {colorize('UNREALIZED PNL', COLOR_BOLD + COLOR_CYAN, color)}  "
        f"{format_amounts_by_asset(unrealized_by_asset, color)}",
        f"  Realized cashflow  {format_amounts_by_asset(realized_by_asset, color)}",
        f"  Combined PnL (TP/SL basis)  {format_amounts_by_asset(pnl_by_asset, color)}",
        f"  Raw combined total: {colorize_amount(total_pnl, color)} "
        "(sum across settlement assets)",
        colorize("-" * separator_width, COLOR_CYAN, color),
        (
            f"{'POSITION':<{symbol_width}}  {'ASSET':<{asset_width}}  "
            f"{'SIZE':>{size_width}}  {'UNREALIZED':>12}  "
            f"{'REALIZED':>12}  {'COMBINED':>12}"
        ),
        (
            f"{'-' * symbol_width}  {'-' * asset_width}  "
            f"{'-' * size_width}  {'-' * 12}  {'-' * 12}  {'-' * 12}"
        ),
    ]

    for detail in details:
        lines.append(
            f"{str(detail['symbol'] or 'UNKNOWN'):<{symbol_width}}  "
            f"{detail['settling_asset']:<{asset_width}}  "
            f"{detail['size']:>{size_width}g}  "
            f"{colorize_amount(detail['unrealized_cashflow'], color, 12)}  "
            f"{colorize_amount(detail['realized_cashflow'], color, 12)}  "
            f"{colorize_amount(detail['pnl'], color, 12)}"
        )

    lines.extend([colorize("=" * separator_width, COLOR_CYAN, color), ""])
    return "\n".join(lines)


async def monitor_loop():
    initialize_pnl_database()
    write_unrealized_pnl_chart()
    print(
        "Starting account-wide TP/SL monitor "
        f"(TP: +{TAKE_PROFIT_USD:.2f} USD, SL: -{STOP_LOSS_USD:.2f} USD). "
        "TP/SL triggers submit reduce-only market close orders. Ctrl+C to stop."
    )

    trigger_reason = None
    last_status_sent = 0.0
    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        while True:
            try:
                positions = await get_open_positions(session)
                pnl_by_asset, details = compute_total_pnl(positions)
                polled_at = record_pnl_poll(details)
                write_unrealized_pnl_chart()

                if not details:
                    if trigger_reason:
                        message = (
                            f"✅ {trigger_reason} close verified: Delta reports no open "
                            "positions. Monitor stopped."
                        )
                        print(colorize(message, COLOR_GREEN))
                    else:
                        message = "ℹ️ No open positions found. Monitor stopped."
                        print(colorize(message, COLOR_YELLOW))
                    await send_telegram(session, message)
                    break

                total_pnl = sum(pnl_by_asset.values())
                print(format_pnl_report(details, pnl_by_asset, total_pnl, polled_at))

                now = time.time()
                if now - last_status_sent >= STATUS_UPDATE_INTERVAL_SEC:
                    unrealized_by_asset = {}
                    realized_by_asset = {}
                    for detail in details:
                        asset = detail["settling_asset"]
                        unrealized_by_asset[asset] = (
                            unrealized_by_asset.get(asset, 0.0)
                            + detail["unrealized_cashflow"]
                        )
                        realized_by_asset[asset] = (
                            realized_by_asset.get(asset, 0.0)
                            + detail["realized_cashflow"]
                        )
                    status_message = (
                        f"Combined PnL: {format_amounts_by_asset(pnl_by_asset)}\n"
                        f"Status as of: {polled_at}\n"
                        f"Open positions: {len(details)}\n"
                        f"Unrealized PnL: {format_amounts_by_asset(unrealized_by_asset)}\n"
                        f"Realized cashflow: {format_amounts_by_asset(realized_by_asset)}"
                    )
                    await send_telegram(session, status_message)
                    last_status_sent = now

                if trigger_reason is None:
                    trigger_reason = get_trigger_reason(total_pnl)

                if trigger_reason is not None:
                    await handle_tp_sl_trigger(
                        session,
                        positions,
                        trigger_reason,
                        total_pnl,
                    )

                await asyncio.sleep(POLL_INTERVAL_SEC)

            except Exception as exc:
                error = f"[Monitor Error] {exc}"
                print(colorize(error, COLOR_RED))
                await send_telegram(session, f"⚠️ Monitor error: {exc}")
                await asyncio.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    dashboard_server = start_dashboard_server()
    dashboard_url = f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}/"
    print(f"Live PnL dashboard: {dashboard_url}")
    try:
        asyncio.run(monitor_loop())
    finally:
        dashboard_server.shutdown()
        dashboard_server.server_close()
