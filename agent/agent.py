"""MT5 Agent - 采集本机 MT5 推送到 Server"""
import json, os, sys, time, logging, urllib.request, urllib.error
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mt5_collector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("agent")


def load_config(path="agent_config.json"):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("配置文件 %s 不存在", path)
        sys.exit(1)


def send_data(server_url, source, accounts, token=""):
    payload = json.dumps({"source": source, "accounts": accounts, "token": token}).encode()
    req = urllib.request.Request(
        server_url + "/api/push", data=payload,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())["ok"]
    except Exception as e:
        logger.warning("推送失败: %s", e)
        return False


def run():
    cfg = load_config()
    url = cfg.get("server_url", "").rstrip("/")
    source = cfg.get("name", "agent")
    token = cfg.get("auth_token", "")
    accounts = cfg.get("accounts", [])
    interval = cfg.get("poll_interval", 3)

    if not url:
        logger.error("server_url 未配置"); sys.exit(1)
    logger.info("Server: %s | Source: %s | Terminals: %d", url, source, len(accounts))

    collectors = mt5_collector.spawn_collectors(accounts, interval)
    if not collectors:
        logger.error("采集进程启动失败"); sys.exit(1)

    logger.info("开始推送数据...")
    while True:
        data = mt5_collector.read_all(collectors)
        if data:
            send_data(url, source, list(data.values()), token)
        time.sleep(interval)
