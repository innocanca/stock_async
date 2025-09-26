#!/bin/bash
# -*- coding: utf-8 -*-
"""
自动设置Cron定时任务脚本
包含数据同步和策略执行的完整定时任务配置
"""

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_PATH=$(which python3)

echo "🔧 股票数据系统 - Cron定时任务配置"
echo "================================================"
echo "脚本目录: $SCRIPT_DIR"
echo "Python路径: $PYTHON_PATH"
echo ""

# 生成cron配置
CRON_CONFIG="# 股票数据系统定时任务
# 每天18:00同步当天的A股主板数据
0 18 * * 1-5 cd $SCRIPT_DIR && $PYTHON_PATH main.py --sync-today >> daily_sync.log 2>&1

# 每天18:30执行放量加速突破策略
30 18 * * 1-5 cd $SCRIPT_DIR && $PYTHON_PATH notify/volume_acceleration_notify.py >> strategy_notify.log 2>&1

# 每天18:35执行涨停股票提醒
35 18 * * 1-5 cd $SCRIPT_DIR && $PYTHON_PATH notify/limit_up_notify.py >> strategy_notify.log 2>&1

# 每天18:40执行强势回踩10日线策略  
40 18 * * 1-5 cd $SCRIPT_DIR && $PYTHON_PATH notify/pullback_ma10_notify.py >> strategy_notify.log 2>&1

# 每天18:45执行选手模式策略集合
45 18 * * 1-5 cd $SCRIPT_DIR && $PYTHON_PATH notify/master_strategies_notify.py >> strategy_notify.log 2>&1

# 每天18:50执行底部反转抄底策略
50 18 * * 1-5 cd $SCRIPT_DIR && $PYTHON_PATH notify/bottom_reversal_notify.py >> strategy_notify.log 2>&1

# 每天18:55执行强势回调低吸策略
55 18 * * 1-5 cd $SCRIPT_DIR && $PYTHON_PATH notify/strong_pullback_notify.py >> strategy_notify.log 2>&1

# 每天19:00执行高位突破跟进策略
0 19 * * 1-5 cd $SCRIPT_DIR && $PYTHON_PATH notify/breakout_follow_notify.py >> strategy_notify.log 2>&1

# 每天19:10执行每日市场复盘(带推送)
10 19 * * 1-5 cd $SCRIPT_DIR && $PYTHON_PATH notify/daily_market_review.py --notify >> daily_review.log 2>&1"

echo "📋 生成的Cron配置:"
echo "================================================"
echo "$CRON_CONFIG"
echo "================================================"
echo ""

echo "📝 安装步骤:"
echo "1. 复制上面的配置"
echo "2. 运行: crontab -e"
echo "3. 粘贴配置到文件末尾"
echo "4. 保存并退出"
echo "5. 验证: crontab -l"
echo ""

echo "📊 执行时间表:"
echo "18:00 - 数据同步（获取当天主板数据）"
echo "18:30 - 放量加速突破策略"
echo "18:35 - 涨停股票提醒"
echo "18:40 - 强势回踩10日线策略"
echo "18:45 - 选手模式策略集合"
echo "18:50 - 底部反转抄底策略（广生堂模式）"
echo "18:55 - 强势回调低吸策略（光库科技模式）"
echo "19:00 - 高位突破跟进策略（金信诺模式）"
echo "19:10 - 每日市场复盘分析（推送通知）"
echo ""

echo "🔍 监控命令:"
echo "查看数据同步日志: tail -f daily_sync.log"
echo "查看策略执行日志: tail -f strategy_notify.log"
echo "查看每日复盘日志: tail -f daily_review.log" 
echo "手动测试数据同步: python main.py --sync-today"
echo "手动测试策略: python notify/volume_acceleration_notify.py --no-notify"
echo ""

echo "✅ 复制上述配置到crontab即可完成自动化设置！"
