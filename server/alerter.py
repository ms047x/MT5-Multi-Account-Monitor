"""
Float loss alert system - per-account thresholds supported
"""
import json
import time
import logging
import urllib.request
from datetime import datetime

logger = logging.getLogger("alerter")
_last_alert = {}


def send_alert(account_key: str, data: dict, token: str, threshold: float):
    name = data.get("name", account_key)
    server = data.get("server", "--")
    balance = data.get("balance", 0) or 0
    profit = data.get("profit", 0) or 0
    equity = data.get("equity", 0) or 0

    lines = [
        "MT5 Float Loss Alert",
        "=" * 20,
        "Account: " + name,
        "Server: " + server,
        "Floating P&L: $" + "{:,.2f}".format(profit),
        "Threshold: $" + "{:,.2f}".format(threshold),
        "Balance: $" + "{:,.2f}".format(balance),
        "Equity: $" + "{:,.2f}".format(equity),
        "Time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ]
    content = "\n".join(lines)
    title = "Loss Alert - " + name + " $" + "{:,.2f}".format(profit)

    try:
        payload = json.dumps({
            "token": token, "title": title,
            "content": content, "template": "txt",
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://www.pushplus.plus/send", data=payload,
            headers={"Content-Type": "application/json"},
        )
        resp = urllib.request.urlopen(req, timeout=10)
        result = json.loads(resp.read().decode("utf-8"))
        if result.get("code") == 200:
            logger.warning("Alert sent for %s", account_key)
        else:
            logger.warning("PushPlus failed: %s", result)
    except Exception as e:
        logger.error("PushPlus error: %s", e)


def check_and_alert(account_key: str, data: dict, config: dict):
    ac = config.get("alerts", {})
    if not ac.get("enabled", False):
        return
    token = ac.get("pushplus_token", "")
    if not token:
        return

    # Per-account override lookup
    acct_name = data.get("name", "")
    if "模拟" in acct_name:
        return
    overrides = ac.get("accounts", {})
    override = None
    for pattern, cfg in overrides.items():
        if pattern in account_key or pattern in acct_name:
            override = cfg
            break

    threshold = ac.get("default_threshold", -200)
    interval = ac.get("interval_minutes", 30) * 60

    if override:
        if override.get("enabled") == False:
            return
        if "threshold" in override:
            threshold = override["threshold"]
        if "interval_minutes" in override:
            interval = override["interval_minutes"] * 60

    profit = data.get("profit", 0) or 0
    if profit >= threshold:
        return

    now = time.time()
    last = _last_alert.get(account_key, 0)
    if now - last < interval:
        return

    _last_alert[account_key] = now
    send_alert(account_key, data, token, threshold)
