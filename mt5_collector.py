# MT5 多账户数据采集引擎（多进程）
import json, os, time, logging
from dataclasses import dataclass, asdict
from multiprocessing import Process, Pipe, connection

logger = logging.getLogger('mt5_collector')

@dataclass
class AccountData:
    name: str = ''
    server: str = ''
    login: int = 0
    connected: bool = False
    balance: float = 0.0
    equity: float = 0.0
    profit: float = 0.0
    margin: float = 0.0
    margin_free: float = 0.0
    margin_level: float = 0.0
    positions_count: int = 0
    error: str = ''

def _init_mt5(path, login, pwd, srv):
    import MetaTrader5 as mt5
    if login and pwd and srv:
        return mt5.initialize(path=path, login=login, password=pwd, server=srv)
    else:
        return mt5.initialize(path=path)

def collector_worker(config, pipe, poll_interval):
    import MetaTrader5 as mt5
    name = config.get('name', '?')
    path = config.get('path', '')
    srv = config.get('server', '')
    login = config.get('login', 0)
    pwd = config.get('password', '')

    ok = False
    for attempt in range(3):
        try:
            if _init_mt5(path, login, pwd, srv):
                ok = True
                break
            time.sleep(2)
        except Exception:
            time.sleep(2)

    if not ok:
        pipe.send(asdict(AccountData(name=name, server=srv, login=login, error='连接失败(3次重试)')))
        pipe.close()
        return

    logger.info('[%s] MT5 已连接 %s %s', name, srv, login)
    data = AccountData(name=name, server=srv, login=login, connected=True)

    while True:
        try:
            ai = mt5.account_info()
            if ai is None:
                mt5.shutdown()
                time.sleep(2)
                if _init_mt5(path, login, pwd, srv):
                    continue
                data.connected = False
                data.error = '重连失败'
                pipe.send(asdict(data))
                break

            a = ai._asdict()
            data.balance = a.get('balance', 0.0)
            data.equity = a.get('equity', 0.0)
            data.profit = a.get('profit', 0.0)
            data.margin = a.get('margin', 0.0)
            data.margin_free = a.get('margin_free', 0.0)
            data.margin_level = a.get('margin_level', 0.0)
            data.connected = True
            data.error = ''
            pos = mt5.positions_get()
            data.positions_count = len(pos) if pos else 0
            pipe.send(asdict(data))
        except Exception as e:
            data.error = str(e)
            try:
                pipe.send(asdict(data))
            except Exception:
                pass
            time.sleep(2)
        finally:
            time.sleep(poll_interval)

def spawn_collectors(accounts, poll_interval=3):
    collectors = {}
    for i, acct in enumerate(accounts):
        name = acct.get('name', 'unknown')
        if i > 0:
            time.sleep(1.5)
        pp, cp = Pipe(duplex=False)
        proc = Process(target=collector_worker, args=(acct, cp, poll_interval), daemon=True)
        proc.start()
        cp.close()
        collectors[name] = (proc, pp)
        logger.info('[%s] 采集进程已启动 PID=%s', name, proc.pid)
    return collectors

def read_all(collectors):
    results = {}
    for name, (proc, pipe) in collectors.items():
        try:
            if pipe.poll(0.05):
                results[name] = pipe.recv()
        except (EOFError, OSError) as e:
            results[name] = asdict(AccountData(name=name, error='Pipe: ' + str(e)))
    return results

def stop_all(collectors):
    for name, (proc, pipe) in collectors.items():
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=3)
        try:
            pipe.close()
        except Exception:
            pass
        logger.info('[%s] 采集进程已终止', name)
