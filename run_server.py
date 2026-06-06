#!/usr/bin/env python
"""启动 MT5 监控 Server"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.app import start
start()
