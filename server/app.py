import asyncio, json, logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import mt5_collector

logger = logging.getLogger("server")

ws_clients = set()
all_accounts = {}
daily_pnl = {}
daily_balance = {}
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
    import os
    task = asyncio.create_task(_local_loop())
    yield
    task.cancel()
    mt5_collector.stop_all(local_collectors)

app = FastAPI(lifespan=lifespan)

@app.get("/favicon.ico")
async def favicon():
    return ""

@app.get("/test", response_class=HTMLResponse)
async def test_page():
    return HTMLResponse('<html><body><h2>Test</h2></body></html>')

@app.get("/", response_class=HTMLResponse)
async def index():
    import os
    p = os.path.join(os.path.dirname(__file__), "static", "index.html")

    with open(p, encoding="utf-8") as f:
        html = f.read()
    
    # Generate cards from template
    tmpl = html.split("<!--CARD_TMPL-->")
    if len(tmpl) > 1:
        card_t = tmpl[1].split("<!--END_CARD_TMPL-->")[0]
        cards = []
        tb = te = tp = tpp = ol = 0
        for key in sorted(all_accounts.keys()):
            a = all_accounts[key]
            co = a.get("connected", False)
            nm = a.get("name", key)
            dm = "模拟" in nm
            ct = "美分" in nm
            if co and not dm:
                bv = a.get("balance", 0) or 0
                ev = a.get("equity", 0) or 0
                pv = a.get("profit", 0) or 0
                if ct:
                    bv /= 100; ev /= 100; pv /= 100
                tb += bv; te += ev; tp += pv
                tpp += a.get("positions_count", 0) or 0
                ol += 1
            pr = a.get("profit", 0) or 0
            pcls = "profit" if pr > 0 else ("loss" if pr < 0 else "neutral")
            pbar = "positive" if pr > 0 else ("negative" if pr < 0 else "neutral")
            led = "online" if co else "offline"
            cent = ""
            if ct:
                cent = ' <span class="cent-badge">美分</span>'
            err = ""
            if a.get("error"):
                err = '<div class="card-error">' + a.get("error", "") + "</div>"
            maxv = max(a.get("equity", 0) or 0, a.get("balance", 0) or 0, 1)
            frac = min(abs(pr) / maxv * 100, 100) if maxv else 0
            cc = card_t.replace("{{KEY}}", str(key))
            cc = cc.replace("{{LED}}", led)
            cc = cc.replace("{{CENT}}", cent)
            cc = cc.replace("{{SRV}}", str(a.get("server", "--")))
            cc = cc.replace("{{BAL}}", "{:,.2f}".format(a.get("balance", 0) or 0))
            cc = cc.replace("{{EQT}}", "{:,.2f}".format(a.get("equity", 0) or 0))
            cc = cc.replace("{{PRO}}", "{:,.2f}".format(pr))
            cc = cc.replace("{{POS}}", str(a.get("positions_count", 0) or 0))
            cc = cc.replace("{{MAR}}", "{:,.2f}".format(a.get("margin", 0) or 0))
            cc = cc.replace("{{MF}}", "{:,.2f}".format(a.get("margin_free", 0) or 0))
            cc = cc.replace("{{PCLS}}", pcls)
            cc = cc.replace("{{PBAR}}", pbar)
            cc = cc.replace("{{FRC}}", str(frac))
            cc = cc.replace("{{ERR}}", err)
            cards.append(cc)
        cards_html = "\n".join(cards) if cards else ""
        html = html.replace("<!--CARDS-->", cards_html)
        html = html.replace("<!--END_CARDS-->", "")
        html = html.replace("{{TB}}", "{:,.2f}".format(tb))
        html = html.replace("{{TE}}", "{:,.2f}".format(te))
        ts = "+" + "{:,.2f}".format(tp) if tp > 0 else "{:,.2f}".format(tp)
        html = html.replace("{{TP}}", ts)
        html = html.replace("{{TPP}}", str(int(tpp)))
        html = html.replace("{{OL}}", str(ol))
        html = html.replace("{{TL}}", str(len(all_accounts)))
        from datetime import datetime
        html = html.replace("{{TIMESTAMP}}", datetime.now().strftime("%H:%M:%S"))
    
    rows = []
    for key, a in sorted(all_accounts.items()):
        b = str(a.get("balance", 0) or 0)
        p = str(a.get("profit", 0) or 0)
        rows.append("<tr><td>" + str(key) + "</td><td>" + b + "</td><td>" + p + "</td></tr>")
    data_html = "<table>" + "".join(rows) + "</table>"
    html = html.replace("<!--DATA-->", data_html)
    html = html.replace("<!--SUMMARY-->", "")
    def _fmt(n):
        try: return "{:,.2f}".format(n or 0)
        except: return "0.00"
    tb=te=tp=tpp=ol=0
    for key, a in sorted(all_accounts.items()):
        co = a.get("connected", False)
        nm = a.get("name", key)
        dm = "\u6a21\u62df" in nm
        ct = "\u7f8e\u5206" in nm
        if co and not dm:
            bv = a.get("balance",0) or 0
            ev = a.get("equity",0) or 0
            pv = a.get("profit",0) or 0
            if ct: bv/=100; ev/=100; pv/=100
            tb+=bv; te+=ev; tp+=pv
            tpp+= a.get("positions_count",0) or 0
            ol+=1
    tpcls = "profit" if tp>0 else ("loss" if tp<0 else "neutral")
    tps = ("+" if tp>0 else "") + _fmt(tp)
    summary  = '<div class="summary-item"><div class="label">\u603b\u4f59\u989d</div><div class="value neutral">' + _fmt(tb) + '</div></div>'
    summary += '<div class="summary-item"><div class="label">\u603b\u51c0\u503c</div><div class="value neutral">' + _fmt(te) + '</div></div>'
    summary += '<div class="summary-item"><div class="label">\u6d6e\u52a8\u76c8\u4e8f</div><div class="value ' + tpcls + '">' + tps + '</div></div>'
    summary += '<div class="summary-item"><div class="label">\u6301\u4ed3\u6570</div><div class="value neutral">' + str(int(tpp)) + '</div></div>'
    summary += '<div class="summary-item"><div class="label">\u5728\u7ebf</div><div class="value neutral">' + str(ol) + "/" + str(len(all_accounts)) + '</div></div>'
    cards_list = []
    for key, a in sorted(all_accounts.items()):
        co = a.get("connected", False)
        nm = a.get("name", key)
        ct = "\u7f8e\u5206" in nm
        pr = a.get("profit",0) or 0
        pcls = "profit" if pr>0 else ("loss" if pr<0 else "neutral")
        pbar = "positive" if pr>0 else ("negative" if pr<0 else "neutral")
        led = "online" if co else "offline"
        maxv = max(a.get("equity",0) or 0, a.get("balance",0) or 0, 1)
        frac = min(abs(pr)/maxv*100, 100)
        cnt = ' <span class="cent-badge">\u7f8e\u5206</span>' if ct else ""
        card  = '<div class="card"><div class="card-header"><div class="card-name"><span class="led ' + led + '"></span>' + str(key) + cnt + '</div><div class="card-server">' + str(a.get("server","--")) + '</div></div>'
        card += '<div class="card-grid"><div><div class="metric-label">\u4f59\u989d</div><div class="metric-value neutral">' + _fmt(a.get("balance",0)) + '</div></div>'
        card += '<div><div class="metric-label">\u51c0\u503c</div><div class="metric-value neutral">' + _fmt(a.get("equity",0)) + '</div></div>'
        card += '<div><div class="metric-label">\u76c8\u4e8f</div><div class="metric-value ' + pcls + '">' + _fmt(pr) + '</div></div>'
        card += '<div><div class="metric-label">\u6301\u4ed3</div><div class="metric-value neutral">' + str(a.get("positions_count",0) or 0) + '</div></div>'
        card += '<div><div class="metric-label">\u4fdd\u8bc1\u91d1</div><div class="metric-value neutral">' + _fmt(a.get("margin",0)) + '</div></div>'
        card += '<div><div class="metric-label">\u53ef\u7528</div><div class="metric-value neutral">' + _fmt(a.get("margin_free",0)) + '</div></div>'
        card += '</div><div class="profit-bar"><div class="fill ' + pbar + '" style="width:' + str(frac) + '%"></div></div>'
        if a.get("error"):
            card += '<div class="card-error">' + a.get("error","") + '</div>'
        card += "</div>"
        cards_list.append(card)
        # Generate daily balance chart
    db_keys = sorted(daily_balance.keys())
    chart_svg = ""
    if len(db_keys) >= 1:
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
        cw = 800
        ch = 150
        pd = 8
        if len(vals) == 1:
            v = vals[0]
            y = ch - pd - ((v - mn) / rg) * (ch - 2*pd)
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
            fill = "{:.1f},{:.1f} {:.1f} {:.1f},{:.1f}".format(pd, ch, pts_str, cw-pd, ch)
            step = max(1, len(recent)//6)
        chart_svg = "<div class=chart-section><div class=chart-title>\u603b\u4f59\u989d\u8d70\u52bf\uff08\u8fd130\u65e5\uff09</div><svg viewBox=\"0 0 " + str(cw) + " " + str(ch) + "\">"
        for gi in range(4):
            gy = pd + (gi/4) * (ch-2*pd)
            chart_svg += f"<line class=\"grid-line\" x1=\"{pd}\" y1=\"{gy:.1f}\" x2=\"{cw-pd}\" y2=\"{gy:.1f}\" />"
        if fill:
            chart_svg += "<polygon class=\"sparkfill\" points=\"" + fill + "\" />"
        chart_svg += "<polyline class=\"sparkline\" points=\"" + pts_str + "\" />"
        if len(recent) >= 1:
            if len(recent) == 1:
                d = recent[0][5:]
                chart_svg += "<text x=\"" + str(cw/2) + "\" y=\"" + str(ch-4) + "\" text-anchor=\"middle\">" + d + "</text>"
            else:
                for i in range(0, len(recent), step):
                    d = recent[i][5:]
                    x = pd + (i/(len(recent)-1))*(cw-2*pd)
                    chart_svg += "<text x=\"" + "{:.1f}".format(x) + "\" y=\"" + str(ch-4) + "\" text-anchor=\"middle\">" + d + "</text>"
        for gi in range(4):
            val = mx - (gi/4)*rg
            gy = pd + (gi/4)*(ch-2*pd)
            chart_svg += "<text x=\"" + str(pd-2) + "\" y=\"" + "{:.1f}".format(gy+3) + "\" text-anchor=\"end\">$" + "{:,.0f}".format(val) + "</text>"
        chart_svg += "</svg></div>"
    html = html.replace("{{CHART}}", chart_svg)
    html = html.replace("{{TB}}", _fmt(tb))
    html = html.replace("{{TE}}", _fmt(te))
    html = html.replace("{{TPCLS}}", tpcls)
    html = html.replace("{{TP}}", tps)
    html = html.replace("{{TPP}}", str(int(tpp)))
    html = html.replace("{{OL}}", str(ol))
    html = html.replace("{{TL}}", str(len(all_accounts)))
    html = html.replace("{{CARDS}}", "\n".join(cards_list) if cards_list else "<!-- no data -->")
    from datetime import datetime
    html = html.replace("{{TIMESTAMP}}", datetime.now().strftime("%H:%M:%S"))
    return HTMLResponse(html)


@app.get("/api/accounts")
async def get_accounts():
    return {
        "data": all_accounts,
        "heartbeat": source_heartbeat,
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
    # Track daily PnL
    today = body.get("_date", "") or str(datetime.now(tz=timezone.utc).date())
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
    # Broadcast to WS clients
    # Track daily total balance
    today = body.get("_date", "") or str(datetime.now(tz=timezone.utc).date())
    tb = 0
    for k, a in all_accounts.items():
        co = a.get("connected", False)
        nm = a.get("name", k)
        dm = "\u6a21\u62df" in nm
        ct = "\u7f8e\u5206" in nm
        if co and not dm:
            bv = a.get("balance", 0) or 0
            if ct: bv /= 100
            tb += bv
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