#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同步脚本入口文件
支持命令行参数选择性同步数据
"""

import argparse
import sys
import os

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sync_data.daily_sync import (
    sync_stock_basic,
    sync_stock_daily,
    sync_stock_weekly,
    sync_index_data,
    sync_etf_data,
    sync_ths_data,
    sync_financial_data,
    logger
)

def main():
    parser = argparse.ArgumentParser(description='股票数据同步工具')
    parser.add_argument('--all', action='store_true', help='同步所有数据')
    parser.add_argument('--stock-basic', action='store_true', help='同步股票基础信息')
    parser.add_argument('--stock-daily', action='store_true', help='同步股票日线行情')
    parser.add_argument('--stock-weekly', action='store_true', help='同步股票周线行情')
    parser.add_argument('--index', action='store_true', help='同步指数数据')
    parser.add_argument('--etf', action='store_true', help='同步ETF数据')
    parser.add_argument('--ths', action='store_true', help='同步同花顺数据')
    parser.add_argument('--financial', action='store_true', help='同步财务数据')
    parser.add_argument('--days', type=int, default=5, help='日线行情回溯天数 (默认5天)')
    parser.add_argument('--years', type=int, default=1, help='财务数据回溯年数 (默认1年)')

    args = parser.parse_args()

    # 如果没有任何参数，显示帮助信息
    if len(sys.argv) == 1:
        parser.print_help()
        return

    logger.info("🚀 同步任务启动...")

    if args.all or args.stock_basic:
        sync_stock_basic()
    
    if args.all or args.stock_daily:
        sync_stock_daily(days_back=args.days)
        
    if args.all or args.stock_weekly:
        sync_stock_weekly()
        
    if args.all or args.index:
        sync_index_data(days_back=args.days + 2) # 指数数据稍微多取一点
        
    if args.all or args.etf:
        sync_etf_data(days_back=args.days + 2)
        
    if args.all or args.ths:
        sync_ths_data()
        
    if args.all or args.financial:
        sync_financial_data(years_back=args.years)

    logger.info("✅ 所有同步任务执行完毕")

if __name__ == "__main__":
    main()

