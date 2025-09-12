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