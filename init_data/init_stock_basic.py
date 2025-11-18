# -*- coding: utf-8 -*-
"""
初始化股票基础信息脚本
从Tushare API获取股票基础信息（包括名称）并存储到数据库中
"""

import logging
import sys
import os

# 添加父目录到Python路径，以便导入database和fetcher模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher

# 配置日志
from log_config import get_logger
logger = get_logger(__name__)


def main():
    """主函数：初始化股票基础信息"""
    logger.info("开始初始化股票基础信息...")
    
    try:
        # 初始化数据获取器和数据库
        fetcher = StockDataFetcher()
        
        with StockDatabase() as db:
            # 创建股票基础信息表
            logger.info("创建股票基础信息表...")
            if not db.create_stock_basic_table():
                logger.error("创建股票基础信息表失败")
                return 1
            
            # 获取股票基础信息
            logger.info("从Tushare API获取股票基础信息...")
            
            # 获取所有交易所股票基础信息（包括主板、创业板、科创板、北交所）
            exchanges = ['SSE', 'SZSE', 'BSE']  # 上交所、深交所、北交所
            
            all_stock_basic = []
            
            for exchange in exchanges:
                logger.info(f"获取{exchange}股票基础信息...")
                stock_basic_df = fetcher.get_stock_basic(
                    exchange=exchange,
                    list_status='L'  # 只获取上市的股票
                )
                
                if stock_basic_df is not None and not stock_basic_df.empty:
                    all_stock_basic.append(stock_basic_df)
                    logger.info(f"获取到 {len(stock_basic_df)} 只{exchange}股票信息")
                else:
                    logger.warning(f"未能获取{exchange}股票基础信息")
            
            if not all_stock_basic:
                logger.error("未能获取任何股票基础信息")
                return 1
            
            # 合并所有股票信息
            import pandas as pd
            combined_df = pd.concat(all_stock_basic, ignore_index=True)
            logger.info(f"总共获取到 {len(combined_df)} 只股票的基础信息")
            
            # 标记股票类型
            def get_stock_type(ts_code):
                if ts_code.startswith('600') or ts_code.startswith('601') or ts_code.startswith('603') or ts_code.startswith('605'):
                    return '上交所主板'
                elif ts_code.startswith('688'):
                    return '科创板'
                elif ts_code.startswith('000') or ts_code.startswith('001'):
                    return '深交所主板'
                elif ts_code.startswith('002'):
                    return '深交所中小板'
                elif ts_code.startswith('300'):
                    return '创业板'
                elif ts_code.endswith('.BJ'):
                    return '北交所'
                else:
                    return '其他'
            
            combined_df['stock_type'] = combined_df['ts_code'].apply(get_stock_type)
            
            # 显示各类型股票数量统计
            type_counts = combined_df['stock_type'].value_counts()
            logger.info("各类型股票数量统计：")
            for stock_type, count in type_counts.items():
                logger.info(f"  {stock_type}: {count} 只")
            
            # 插入数据库（包含所有类型的股票）
            logger.info("将股票基础信息插入数据库...")
            if db.insert_stock_basic(combined_df):
                logger.info(f"成功初始化 {len(combined_df)} 只股票的基础信息")
                
                # 显示示例数据
                sample_data = combined_df.head(10)
                logger.info("示例股票信息：")
                for _, row in sample_data.iterrows():
                    stock_type = row.get('stock_type', '未知')
                    logger.info(f"  {row['name']} ({row['ts_code']}) - {row['industry']} - {stock_type}")
                    
            else:
                logger.error("插入股票基础信息失败")
                return 1
                
    except Exception as e:
        logger.error(f"初始化股票基础信息失败: {e}")
        return 1
    
    logger.info("股票基础信息初始化完成！")
    return 0


if __name__ == "__main__":
    exit(main())
