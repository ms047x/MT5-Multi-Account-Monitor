from __future__ import annotations
import asyncio, json, logging, os as _os_mod
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import mt5_collector
from server.alerter import check_and_alert

logger = logging.getLogger("server")

ws_clients = set()
all_accounts = {}
daily_pnl = {}
daily_balance = {}
account_snapshots = {}
_last_snapshot_date = ""

_SAVE_FILE = "daily_data.json"

def _maybe_record_snapshots():
    global _last_snapshot_date
    import datetime as _dt
    now = _dt.datetime.now()
    today = now.strftime("%Y-%m-%d")
    if now.hour >= 6 and today != _last_snapshot_date:
        tb = 0
        for key, a in all_accounts.items():
            if a.get("connected", False):
                if key not in account_snapshots:
                    account_snapshots[key] = []
                account_snapshots[key].append({
                    "date": today,
                    "balance": a.get("balance", 0) or 0,
                    "equity": a.get("equity", 0) or 0,
                    "profit": a.get("profit", 0) or 0,
                    "server": a.get("server", "--"),
                })
                # Count total balance (exclude demo, convert cent)
                nm = a.get("name", key)
                if "模拟" not in nm:
                    b = a.get("balance", 0) or 0
                    if "美分" in nm:
                        b /= 100
                    tb += b
        daily_balance[today] = round(tb, 2)
        _last_snapshot_date = today
        logger.info("Snapshots recorded for %s, balance=%.2f", today, tb)


def _save_daily():
    try:
        data = {"balance": daily_balance, "pnl": {k: v for k, v in daily_pnl.items()}, "snapshots": account_snapshots, "last_snapshot_date": _last_snapshot_date}
        with open(_SAVE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        logger.error("Save daily data failed: %s", e)

def _load_daily():
    try:
        if _os_mod.path.exists(_SAVE_FILE):
            with open(_SAVE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "balance" in data:
                daily_balance.clear()
                daily_balance.update(data["balance"])
            if "pnl" in data:
                daily_pnl.clear()
                daily_pnl.update(data["pnl"])
            if "snapshots" in data:
                account_snapshots.clear()
                account_snapshots.update(data["snapshots"])
            if "last_snapshot_date" in data:
                global _last_snapshot_date
                _last_snapshot_date = data["last_snapshot_date"]
    except Exception as e:
        logger.error("Load daily data failed: %s", e)

source_heartbeat = {}
local_collectors = {}
AUTH_TOKEN = None
CONFIG = {}

def _load_config():
    global AUTH_TOKEN, CONFIG
    try:
        with open("server_config.json", encoding="utf-8-sig") as f:
            CONFIG = json.load(f)
        AUTH_TOKEN = CONFIG.get("auth_token", "") or None
    except FileNotFoundError:
        CONFIG = {"port": 8000, "host": "0.0.0.0"}
    return CONFIG

async def _broadcast(payload: str):
    dead = []
    for ws in set(ws_clients):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.discard(ws)

async def _local_loop():
    while True:
        data = mt5_collector.read_all(local_collectors)
        if data:
            all_accounts.update(data)
            await _broadcast(json.dumps({
                "type": "update",
                "timestamp": datetime.now(tz=timezone.utc).strftime("%H:%M:%S"),
                "data": dict(all_accounts),
                "daily_pnl": dict(daily_pnl),
                "heartbeat": dict(source_heartbeat),
            }, ensure_ascii=False))
        await asyncio.sleep(0.5)

@asynccontextmanager
async def lifespan(app):
    global local_collectors
    cfg = _load_config()
    if cfg.get("local_accounts"):
        local_collectors = mt5_collector.spawn_collectors(cfg["local_accounts"], cfg.get("poll_interval", 3))
        logger.info("Local MT5 collectors started: " + str(len(local_collectors)))
    _load_daily()
    task = asyncio.create_task(_local_loop())
    yield
    task.cancel()
    _save_daily()
    mt5_collector.stop_all(local_collectors)

app = FastAPI(lifespan=lifespan)

@app.exception_handler(Exception)
async def debug_handler(request, exc):
    import traceback
    return HTMLResponse('<pre>'+traceback.format_exc()+'</pre>',status_code=500)

def _fmt(n):
    try: return "{:,.2f}".format(n or 0)
    except: return "0.00"



def _build_chart():
    """Build daily balance chart SVG."""
    db_keys = sorted(daily_balance.keys())
    if not db_keys:
        try:
            tb, _, _, _, _ = _calc_summary()
            if tb > 0:
                import datetime as _dt
                today_str = _dt.datetime.now().strftime("%Y-%m-%d")
                daily_balance[today_str] = round(tb, 2)
                db_keys = [today_str]
        except Exception:
            pass
    if not db_keys:
        return ""
    recent = db_keys[-30:]
    vals = [daily_balance[d] for d in recent]
    mn = min(vals)
    mx = max(vals)
    rg = mx - mn or 1
    pad_amt = rg * 0.1
    mn -= pad_amt
    mx += pad_amt
    rg = mx - mn
    min_rg = abs(mx) * 0.2 if mx else 1
    if rg < min_rg:
        mid = (mx + mn) / 2
        mx = mid + min_rg / 2
        mn = mid - min_rg / 2
        rg = mx - mn
    cw, ch = 800, 150
    pd = 8

    if len(vals) == 1:
        y = ch - pd - ((vals[0] - mn) / rg) * (ch - 2*pd)
        pts_str = "{:.1f},{:.1f} {:.1f},{:.1f}".format(pd, y, cw-pd, y)
        fill = ""
        step = 1
    else:
        pts = []
        for i, v in enumerate(vals):
            x = pd + (i / (len(vals)-1)) * (cw - 2*pd)
            y = ch - pd - ((v - mn) / rg) * (ch - 2*pd)
            pts.append("{:.1f},{:.1f}".format(x, y))
        pts_str = " ".join(pts)
        fill = "{:.1f},{:.1f} {} {:.1f},{:.1f}".format(pd, ch, pts_str, cw-pd, ch)
        step = max(1, len(recent)//6)

    svg  = '<div class=chart-section><div class=chart-title>总余额走势（近30日）</div>'
    svg += '<svg viewBox="0 0 ' + str(cw) + ' ' + str(ch) + '">'
    for gi in range(4):
        gy = pd + (gi/4) * (ch-2*pd)
        svg += '<line class="grid-line" x1="' + str(pd) + '" y1="' + '{:.1f}'.format(gy) + '" x2="' + str(cw-pd) + '" y2="' + '{:.1f}'.format(gy) + '" />'
    if fill:
        svg += '<polygon class="sparkfill" points="' + fill + '" />'
    svg += '<polyline class="sparkline" points="' + pts_str + '" />'
    for gi in range(4):
        val = mx - (gi/4)*rg
        gy = pd + (gi/4)*(ch-2*pd)
        svg += '<text x="' + str(pd-2) + '" y="' + '{:.1f}'.format(gy+3) + '" text-anchor="end">$' + '{:,.0f}'.format(val) + '</text>'
    if len(recent) >= 1:
        if len(recent) == 1:
            d = recent[0][5:]
            svg += '<text x="' + str(cw/2) + '" y="' + str(ch-4) + '" text-anchor="middle">' + d + '</text>'
        else:
            for i in range(0, len(recent), step):
                d = recent[i][5:]
                x = pd + (i/(len(recent)-1))*(cw-2*pd)
                svg += '<text x="' + '{:.1f}'.format(x) + '" y="' + str(ch-4) + '" text-anchor="middle">' + d + '</text>'
    svg += '</svg></div>'
    return svg

def _calc_summary():
    tb = te = tpval = 0
    tpp = 0
    ol = 0
    for key, a in all_accounts.items():
        co = a.get("connected", False)
        nm = a.get("name", key)
        dm = "模拟" in nm
        ct = "美分" in nm
        if co and not dm:
            b = a.get("balance", 0) or 0
            e = a.get("equity", 0) or 0
            p = a.get("profit", 0) or 0
            if ct:
                b /= 100; e /= 100; p /= 100
            tb += b; te += e; tpval += p
            tpp += a.get("positions_count", 0) or 0
            ol += 1
    return tb, te, tpval, tpp, ol

def _build_cards():
    cards = []
    for key, a in sorted(all_accounts.items()):
        co = a.get("connected", False)
        nm = a.get("name", key)
        ct = "美分" in nm
        pr = a.get("profit", 0) or 0
        pcls = "profit" if pr > 0 else ("loss" if pr < 0 else "neutral")
        pbar = "positive" if pr > 0 else ("negative" if pr < 0 else "neutral")
        led = "online" if co else "offline"
        max_val = max(a.get("equity", 0) or 0, a.get("balance", 0) or 0, 1)
        frac = min(abs(pr) / max_val * 100, 100) if max_val else 0
        cent_badge = ' <span class="cent-badge">美分</span>' if ct else ""
        card  = '<div class="card"><div class="card-header"><div class="card-name"><span class="led ' + led + '"></span>' + str(key) + cent_badge + '</div><div class="card-server">' + str(a.get("server","--")) + '</div></div>'
        card += '<div class="card-grid"><div><div class="metric-label">余额</div><div class="metric-value neutral">' + _fmt(a.get("balance",0)) + '</div></div>'
        card += '<div><div class="metric-label">净值</div><div class="metric-value neutral">' + _fmt(a.get("equity",0)) + '</div></div>'
        card += '<div><div class="metric-label">盈亏</div><div class="metric-value ' + pcls + '">' + _fmt(pr) + '</div></div>'
        card += '<div><div class="metric-label">持仓</div><div class="metric-value neutral">' + str(a.get("positions_count",0) or 0) + '</div></div>'
        card += '<div><div class="metric-label">保证金</div><div class="metric-value neutral">' + _fmt(a.get("margin",0)) + '</div></div>'
        card += '<div><div class="metric-label">可用</div><div class="metric-value neutral">' + _fmt(a.get("margin_free",0)) + '</div></div>'
        card += '</div><div class="profit-bar"><div class="fill ' + pbar + '" style="width:' + str(frac) + '%"></div></div>'
        if a.get("error"):
            card += '<div class="card-error">' + a.get("error","") + '</div>'
        card += '</div>'
        cards.append(card)
    return "".join(cards) if cards else "<!-- no data -->"

@app.get("/", response_class=HTMLResponse)
async def index():
    p = _os_mod.path.join(_os_mod.path.dirname(__file__), "static", "index.html")
    with open(p, encoding="utf-8") as f:
        html = f.read()

    tb, te, tpval, tpp, ol = _calc_summary()
    tl = len(all_accounts)
    tpcls = "profit" if tpval > 0 else ("loss" if tpval < 0 else "neutral")
    tps = ("+" if tpval > 0 else "") + _fmt(tpval)

    html = html.replace("{{TB}}", _fmt(tb))
    html = html.replace("{{TE}}", _fmt(te))
    html = html.replace("{{TPCLS}}", tpcls)
    html = html.replace("{{TP}}", tps)
    html = html.replace("{{TPP}}", str(int(tpp)))
    html = html.replace("{{OL}}", str(ol))
    html = html.replace("{{TL}}", str(tl))
    html = html.replace("{{CHART}}", _build_chart())
    html = html.replace("{{CARDS}}", _build_cards())
    html = html.replace("{{TIMESTAMP}}", datetime.now().strftime("%H:%M:%S"))
    html = html.replace("{{ACTIVE_DASHBOARD}}", "active")
    html = html.replace("{{ACTIVE_SINGLE}}", "")
    html = html.replace("{{SELECTOR}}", "")
    return HTMLResponse(html)


@app.get("/single", response_class=HTMLResponse)
async def single_account(account: str = ""):
    import urllib.parse
    account_key = account
    if not account_key or account_key not in all_accounts:
        act_list = sorted(all_accounts.keys())
        account_key = act_list[0] if act_list else ""
    p = _os_mod.path.join(_os_mod.path.dirname(__file__), "static", "index.html")
    with open(p, encoding="utf-8") as f:
        html = f.read()

    # Build account selector
    links = ""
    for k in sorted(all_accounts.keys()):
        nk = all_accounts[k].get("name", k)
        cls = " acct-active" if k == account_key else ""
        url = "/single?account=" + urllib.parse.quote(k, safe="")
        links += '<a href="' + url + '" class=acct-link' + cls + ' draggable=false>' + nk + '</a>'
    html = html.replace("{{SELECTOR}}", '<nav class=acct-nav>' + links + '</nav>')

    if account_key and account_key in all_accounts:
        a = all_accounts[account_key]
        bal = a.get("balance", 0) or 0
        eq = a.get("equity", 0) or 0
        pr = a.get("profit", 0) or 0
        mg = a.get("margin", 0) or 0
        mf = a.get("margin_free", 0) or 0
        pos = a.get("positions_count", 0) or 0
        co = a.get("connected", False)
        srv = a.get("server", "--")
        pcls = "profit" if pr > 0 else ("loss" if pr < 0 else "neutral")
        tps = ("+" if pr > 0 else "") + _fmt(pr)

        html = html.replace("{{TB}}", _fmt(bal))
        html = html.replace("{{TE}}", _fmt(eq))
        html = html.replace("{{TPCLS}}", pcls)
        html = html.replace("{{TP}}", tps)
        html = html.replace("{{TPP}}", str(pos))
        html = html.replace("{{OL}}", "1" if co else "0")
        html = html.replace("{{TL}}", "1")

        # Build single account card
        led = "online" if co else "offline"
        nm = a.get("name", account_key)
        ct = "美分" in nm
        cent_badge = ' <span class="cent-badge">美分</span>' if ct else ""
        card  = '<div class="card"><div class="card-header"><div class="card-name"><span class="led ' + led + '"></span>' + str(account_key) + cent_badge + '</div><div class="card-server">' + srv + '</div></div>'
        card += '<div class="card-grid"><div><div class="metric-label">余额</div><div class="metric-value neutral">$' + _fmt(bal) + '</div></div>'
        card += '<div><div class="metric-label">净值</div><div class="metric-value neutral">$' + _fmt(eq) + '</div></div>'
        card += '<div><div class="metric-label">盈亏</div><div class="metric-value ' + pcls + '">$' + _fmt(pr) + '</div></div>'
        card += '<div><div class="metric-label">持仓</div><div class="metric-value neutral">' + str(pos) + '</div></div>'
        card += '<div><div class="metric-label">保证金</div><div class="metric-value neutral">$' + _fmt(mg) + '</div></div>'
        card += '<div><div class="metric-label">可用</div><div class="metric-value neutral">$' + _fmt(mf) + '</div></div>'
        card += '</div><div class="metric-label" style="margin-top:8px">状态: <span style="color:' + ('var(--green)' if co else 'var(--red)') + '">' + ('在线' if co else '离线') + '</span></div></div>'
        html = html.replace("{{CARDS}}", card)

        # Build chart from account snapshots
        chart = ""
        snaps = account_snapshots.get(account_key, [])
        if not snaps:
            from datetime import datetime as _dt2
            snaps = [{"date": _dt2.now().strftime("%Y-%m-%d"), "balance": a.get("balance", 0) or 0}]
        if snaps:
            snap_dates = sorted(set(s["date"] for s in snaps))
            snap_keys = snap_dates[-30:]
            snap_vals = [next(s["balance"] for s in reversed(snaps) if s["date"] == d) for d in snap_keys]
            if snap_vals:
                mn = min(snap_vals)
                mx = max(snap_vals)
                rg = mx - mn or 1
                pad_amt = rg * 0.1
                mn -= pad_amt; mx += pad_amt; rg = mx - mn
                min_rg = abs(mx) * 0.2 if mx else 1
                if rg < min_rg:
                    mid = (mx+mn)/2
                    mx = mid+min_rg/2; mn = mid-min_rg/2; rg = mx-mn
                cw, ch = 800, 150
                pd = 8
                if len(snap_vals) == 1:
                    y = ch - pd - ((snap_vals[0]-mn)/rg)*(ch-2*pd)
                    pts_str = "{:.1f},{:.1f} {:.1f},{:.1f}".format(pd, y, cw-pd, y)
                    fill = ""; step=1
                else:
                    pts = []
                    for i, v in enumerate(snap_vals):
                        x = pd + (i/(len(snap_vals)-1))*(cw-2*pd)
                        y = ch-pd-((v-mn)/rg)*(ch-2*pd)
                        pts.append("{:.1f},{:.1f}".format(x,y))
                    pts_str = " ".join(pts)
                    fill = "{:.1f},{:.1f} {} {:.1f},{:.1f}".format(pd, ch, pts_str, cw-pd, ch)
                    step = max(1, len(snap_keys)//6)
                chart = '<div class=chart-section><div class=chart-title>账户余额走势（近30日）</div><svg viewBox="0 0 '+str(cw)+' '+str(ch)+'">'
                for gi in range(4):
                    gy = pd+(gi/4)*(ch-2*pd)
                    chart += '<line class=grid-line x1="'+str(pd)+'" y1="'+'{:.1f}'.format(gy)+'" x2="'+str(cw-pd)+'" y2="'+'{:.1f}'.format(gy)+'" />'
                if fill:
                    chart += '<polygon class=sparkfill points="'+fill+'" />'
                chart += '<polyline class=sparkline points="'+pts_str+'" />'
                for gi in range(4):
                    val = mx-(gi/4)*rg
                    gy = pd+(gi/4)*(ch-2*pd)
                    chart += '<text x="'+'{:.1f}'.format(pd-2)+'" y="'+'{:.1f}'.format(gy+3)+'" text-anchor=end>$'+'{:,.0f}'.format(val)+'</text>'
                if len(snap_keys) >= 1:
                    if len(snap_keys) == 1:
                        chart += '<text x="'+str(cw/2)+'" y="'+str(ch-4)+'" text-anchor=middle>'+snap_keys[0][5:]+'</text>'
                    else:
                        for i in range(0, len(snap_keys), step):
                            d = snap_keys[i][5:]
                            x = pd+(i/(len(snap_keys)-1))*(cw-2*pd)
                            chart += '<text x="'+'{:.1f}'.format(x)+'" y="'+str(ch-4)+'" text-anchor=middle>'+d+'</text>'
                chart += '</svg></div>'
        html = html.replace("{{CHART}}", chart)
    else:
        html = html.replace("{{TB}}", "0.00")
        html = html.replace("{{TE}}", "0.00")
        html = html.replace("{{TPCLS}}", "neutral")
        html = html.replace("{{TP}}", "0.00")
        html = html.replace("{{TPP}}", "0")
        html = html.replace("{{OL}}", "0")
        html = html.replace("{{TL}}", "0")
        html = html.replace("{{CHART}}", "")
        html = html.replace("{{CARDS}}", "")

    html = html.replace("{{TIMESTAMP}}", "")
    html = html.replace("{{ACTIVE_DASHBOARD}}", "")
    html = html.replace("{{ACTIVE_SINGLE}}", "active")
    return HTMLResponse(html)

@app.get("/favicon.ico")
async def favicon():
    return ""


@app.get("/api/accounts")
async def get_accounts():
    return {
        "data": dict(all_accounts),
        "heartbeat": dict(source_heartbeat),
        "daily_pnl": dict(daily_pnl),
    }

@app.post("/api/push")
async def agent_push(req: Request):
    body = await req.json()
    source = body.get("source", "unknown")
    accounts = body.get("accounts", [])
    token = body.get("token", "")
    if AUTH_TOKEN and token != AUTH_TOKEN:
        return {"ok": False, "error": "auth failed"}
    source_heartbeat[source] = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
    today = body.get("_date", "") or str(datetime.now(tz=timezone(timedelta(hours=8))).date())

    for acct in accounts:
        key = source + "/" + acct.get("name", "?")
        pro = acct.get("profit", 0)
        if key not in daily_pnl:
            daily_pnl[key] = []
        entries = daily_pnl[key]
        if entries and entries[-1]["date"] == today:
            entries[-1]["profit"] = pro
        else:
            entries.append({"date": today, "profit": pro})
        if len(entries) > 182:
            daily_pnl[key] = entries[-182:]

    for acct in accounts:
        key = source + "/" + acct.get("name", "?")
        acct["_source"] = source
        all_accounts[key] = acct
        check_and_alert(key, acct, CONFIG)

    _maybe_record_snapshots()

    # Calculate daily total balance
    tb = 0
    for k, a in all_accounts.items():
        co = a.get("connected", False)
        nm = a.get("name", k)
        dm = "模拟" in nm
        ct = "美分" in nm
        if co and not dm:
            b = a.get("balance", 0) or 0
            if ct: b /= 100
            tb += b
    daily_balance[today] = round(tb, 2)
    dk = sorted(daily_balance.keys())
    if len(dk) > 90:
        for k in dk[:-90]:
            del daily_balance[k]

    await _broadcast(json.dumps({
        "type": "update",
        "timestamp": datetime.now(tz=timezone.utc).strftime("%H:%M:%S"),
        "data": dict(all_accounts),
        "daily_pnl": dict(daily_pnl),
        "heartbeat": dict(source_heartbeat),
    }, ensure_ascii=False))
    return {"ok": True, "count": len(accounts)}

@app.get("/api/snapshots")
async def get_snapshots():
    return {"snapshots": account_snapshots, "last_date": _last_snapshot_date}

@app.get("/api/health")
async def health():
    return {"status": "ok", "ws_clients": len(ws_clients), "accounts": len(all_accounts)}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    logger.info("WS connected (total: " + str(len(ws_clients)))
    if all_accounts:
        await ws.send_text(json.dumps({
            "type": "update",
            "data": dict(all_accounts),
            "daily_pnl": dict(daily_pnl),
            "heartbeat": dict(source_heartbeat),
        }, ensure_ascii=False))
    try:
        while True:
            msg = await ws.receive_text()
            if msg == "ping":
                await ws.send_text("pong")
    except Exception:
        pass
    finally:
        ws_clients.discard(ws)

def start():
    cfg = _load_config()
    host = cfg.get("host", "0.0.0.0")
    port = int(cfg.get("port", 8000))
    uvicorn.run(app, host=host, port=port, log_level="info")
