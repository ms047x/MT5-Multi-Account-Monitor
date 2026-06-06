# MT5 多账户盈亏实时监控台

实时监控多个 MT5 交易账户的盈亏数据，支持跨服务器部署，Web 面板查看。

## 架构

```
┌─ 云服务器/本机 ─────────────────┐
│  MT5 终端 (已登录+跑EA)          │
│  Agent (采集 -> HTTP POST)       │──→  Server (中央监控)
└──────────────────────────────────┘     WebSocket推送
                                            ↓
┌─ 云服务器/本机 ─────────────────┐     浏览器面板
│  MT5 终端 (已登录+跑EA)          │
│  Agent (采集 -> HTTP POST)       │──→  Server
└──────────────────────────────────┘
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. Server 端（看面板的机器）

```bash
copy server_config_sample.json server_config.json
# 修改端口等配置（可选）
python run_server.py
# 浏览器打开 http://localhost:8000
```

### 3. Agent 端（有 MT5 的机器）

每台有 MT5 终端的机器各跑一个 Agent。

```bash
copy agent_config_sample.json agent_config.json
```

编辑 `agent_config.json`：

```json
{
  "name": "服务器标签",
  "server_url": "http://服务端IP:8000",
  "accounts": [
    {
      "name": "账户名",
      "path": "C:\\Program Files\\xxx MetaTrader 5\\terminal64.exe",
      "server": "",
      "login": 0,
      "password": ""
    }
  ]
}
```

> MT5 已登录时，只需填 `path`，账号密码留空。

启动 Agent：

```bash
python run_agent.py
```

### 4. 网络打通

- **同机部署**：Agent 的 `server_url` 填 `http://127.0.0.1:8000`
- **跨服务器**：推荐使用 [Tailscale](https://tailscale.com) 组建虚拟局域网，Agent 填 Server 的 Tailscale IP

## 功能

- 实时监控多台服务器的 MT5 账户
- 每日盈亏走势曲线（30日）
- 总余额、总净值、浮动盈亏汇总
- 美分账户自动换算 + 标注
- 模拟账户排除统计
- 盈亏颜色标记（绿色盈利 / 红色亏损）
- 每 10 秒自动刷新

## 技术栈

- **后端**：Python + FastAPI + Uvicorn
- **前端**：纯 HTML + CSS + SVG（服务端渲染，无需 JavaScript）
- **采集**：MetaTrader5 Python API + multiprocessing 多进程并行

## 项目结构

```
mt5-monitor/
├── server/                  # 中央监控服务
│   ├── app.py               # FastAPI 服务端
│   └── static/index.html    # Web 面板
├── agent/
│   └── agent.py             # Agent 采集器
├── mt5_collector.py         # MT5 多进程采集引擎
├── run_server.py            # 启动 Server
├── run_agent.py             # 启动 Agent
├── server_config_sample.json
├── agent_config_sample.json
└── requirements.txt
```
