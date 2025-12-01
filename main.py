#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目入口：通过此文件启动 FastAPI 服务。

使用方法：
    python main.py
"""

from query_low_pe_volume_surge import main as run_server


def main():
    """启动股票筛选 HTTP 服务"""
    run_server()


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""
股票数据获取主程序
使用Tushare API获取股票日线数据并存储到MySQL数据库
"""

import logging
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_data.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

from cli import StockDataCLI


def main():
    """主函数入口点"""
    cli = StockDataCLI()
    return cli.run()


if __name__ == "__main__":
    exit(main())