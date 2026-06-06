#!/usr/bin/env python
"""启动 MT5 Agent"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from agent.agent import run
run()
