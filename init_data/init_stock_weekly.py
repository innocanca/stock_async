#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票周线数据初始化脚本

功能：
1. 获取所有股票列表
2. 创建周线数据库表结构
3. 初始化一年的股票周线数据到数据库中
4. 提供数据查询和统计功能

使用方法：
python init_stock_weekly.py
"""

import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List

# 添加父目录到Python路径，以便导入database和fetcher模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher

# 使用统一日志配置
from log_config import get_logger
logger = get_logger(__name__)


def create_database_tables(db: StockDatabase) -> bool:
    """
    创建必要的数据库表
    
    Args:
        db: 数据库实例
        
    Returns:
        bool: 创建是否成功
    """
    logger.info("🔧 开始创建数据库表...")
    
    try:
        # 创建数据库（如果不存在）
        if not db.create_database():
            logger.error("❌ 创建数据库失败")
            return False
        
        # 连接数据库
        if not db.connect():
            logger.error("❌ 连接数据库失败")
            return False
            
        # 创建股票基础信息表
        if not db.create_stock_basic_table():
            logger.error("❌ 创建股票基础信息表失败")
            return False
            
        # 创建周线数据表
        if not db.create_weekly_table():
            logger.error("❌ 创建周线数据表失败")
            return False
            
        logger.info("✅ 数据库表创建成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据库表时发生错误: {e}")
        return False


def get_stock_list(fetcher: StockDataFetcher) -> List[str]:
    """
    获取股票列表
    
    Args:
        fetcher: 数据获取器实例
        
    Returns:
        List[str]: 股票代码列表
    """
    logger.info("📋 获取股票列表...")
    
    try:
        # 获取股票基础信息
        df = fetcher.get_stock_basic()
        if df is None or df.empty:
            logger.error("❌ 获取股票列表失败")
            return []
        
        # 筛选主板股票（排除ST股票和退市股票）
        # 使用多个条件进行筛选，避免正则表达式问题
        filtered_df = df[df['market'] == '主板'].copy()
        
        # 排除ST股票、退市股票等
        exclude_keywords = ['ST', '退', '*ST', 'PT', '暂停']
        for keyword in exclude_keywords:
            filtered_df = filtered_df[~filtered_df['name'].str.contains(keyword, na=False, regex=False)]
        
        main_board_stocks = filtered_df['ts_code'].tolist()
        
        logger.info(f"📈 获取到 {len(main_board_stocks)} 只主板股票")
        return main_board_stocks[:500]  # 限制为500只股票，避免初始化时间过长
        
    except Exception as e:
        logger.error(f"❌ 获取股票列表失败: {e}")
        return []


def calculate_date_range() -> tuple:
    """
    计算一年的日期范围
    
    Returns:
        tuple: (start_date, end_date) 格式为 YYYYMMDD
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    
    logger.info(f"📅 数据获取时间范围: {start_date_str} 至 {end_date_str}")
    return start_date_str, end_date_str


def fetch_and_store_weekly_data(fetcher: StockDataFetcher, db: StockDatabase, 
                                stock_codes: List[str], start_date: str, 
                                end_date: str) -> dict:
    """
    获取并存储股票周线数据
    
    Args:
        fetcher: 数据获取器实例
        db: 数据库实例
        stock_codes: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        dict: 统计信息
    """
    stats = {
        'total_stocks': len(stock_codes),
        'successful_stocks': 0,
        'total_records': 0,
        'successful_insert': False,
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    logger.info("📊 开始获取股票周线数据...")
    logger.info(f"   股票数量: {len(stock_codes)} 只")
    logger.info(f"   时间范围: {start_date} 至 {end_date}")
    
    try:
        # 批量获取周线数据
        df = fetcher.get_multiple_stocks_weekly_data(
            stock_codes=stock_codes,
            start_date=start_date,
            end_date=end_date,
            batch_size=50,
            delay=0.2  # 降低延迟，提高效率
        )
        
        if df is None or df.empty:
            logger.error("❌ 未获取到任何周线数据")
            return stats
        
        stats['total_records'] = len(df)
        stats['successful_stocks'] = df['ts_code'].nunique()
        
        logger.info(f"📈 成功获取 {stats['successful_stocks']} 只股票的 {stats['total_records']} 条周线数据")
        
        # 插入数据库
        logger.info("💾 开始插入数据到数据库...")
        
        if db.insert_weekly_data(df):
            stats['successful_insert'] = True
            logger.info("✅ 数据插入成功！")
            
            # 显示统计信息
            logger.info("📊 数据统计：")
            logger.info(f"   成功股票数量: {stats['successful_stocks']} / {stats['total_stocks']} 只")
            logger.info(f"   总记录数: {stats['total_records']} 条")
            logger.info(f"   成功率: {stats['successful_stocks']/stats['total_stocks']*100:.1f}%")
        else:
            logger.error("❌ 数据插入失败")
            
    except Exception as e:
        logger.error(f"❌ 获取和存储数据时发生错误: {e}")
        
        # 检查是否是权限问题
        if "权限" in str(e) or "积分" in str(e) or "permission" in str(e).lower():
            logger.error("💡 提示：周线数据接口需要一定积分权限")
            logger.error("   请检查您的Tushare账户积分或升级账户权限")
            logger.error("   访问 https://tushare.pro/ 查看积分和权限说明")
    
    finally:
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
    
    return stats


def query_and_display_data(db: StockDatabase) -> None:
    """
    查询并显示数据库中的周线数据
    
    Args:
        db: 数据库实例
    """
    logger.info("🔍 查询数据库中的周线数据...")
    
    try:
        # 查询最近的周线数据
        df = db.query_weekly_data(limit=20)
        
        if df is None or df.empty:
            logger.warning("⚠️ 数据库中没有周线数据")
            return
        
        # 统计总数据量
        total_df = db.query_weekly_data()
        total_records = len(total_df) if total_df is not None else 0
        total_stocks = total_df['ts_code'].nunique() if total_df is not None and not total_df.empty else 0
        
        logger.info(f"📋 数据库中共有 {total_records} 条周线记录，涵盖 {total_stocks} 只股票")
        logger.info("📖 最近20条记录示例：")
        
        for i, (_, row) in enumerate(df.head(20).iterrows(), 1):
            trade_date = row.get('trade_date', 'N/A')
            if hasattr(trade_date, 'strftime'):
                trade_date = trade_date.strftime('%Y-%m-%d')
            
            logger.info(f"   {i:2d}. {row.get('ts_code', 'N/A')} "
                       f"日期:{trade_date} "
                       f"收盘:{row.get('close', 'N/A')} "
                       f"涨幅:{row.get('pct_chg', 'N/A'):.2f}%")
        
    except Exception as e:
        logger.error(f"❌ 查询数据时发生错误: {e}")


def main():
    """主函数"""
    logger.info("🚀 股票周线数据初始化开始...")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # 初始化数据获取器
        logger.info("🔧 初始化数据获取器...")
        fetcher = StockDataFetcher()
        logger.info("✅ 数据获取器初始化成功")
        
        # 初始化数据库
        logger.info("🔧 初始化数据库连接...")
        with StockDatabase() as db:
            
            # 创建数据库表
            if not create_database_tables(db):
                logger.error("❌ 数据库表创建失败，退出程序")
                return False
            
            # 获取股票列表
            stock_codes = get_stock_list(fetcher)
            if not stock_codes:
                logger.error("❌ 获取股票列表失败，退出程序")
                return False
            
            # 计算日期范围
            start_date, end_date = calculate_date_range()
            
            # 获取并存储周线数据
            stats = fetch_and_store_weekly_data(fetcher, db, stock_codes, start_date, end_date)
            
            # 查询并显示数据（验证插入结果）
            if stats['successful_insert']:
                query_and_display_data(db)
            
            # 显示总体统计
            logger.info("\n" + "=" * 60)
            logger.info("📊 初始化完成统计：")
            logger.info(f"   📈 目标股票数量: {stats['total_stocks']} 只")
            logger.info(f"   ✅ 成功股票数量: {stats['successful_stocks']} 只")
            logger.info(f"   📊 总记录数: {stats['total_records']} 条")
            logger.info(f"   💾 数据插入状态: {'成功' if stats['successful_insert'] else '失败'}")
            logger.info(f"   ⏱️  总耗时: {stats['duration']}")
            
            if stats['successful_insert']:
                logger.info("🎉 股票周线数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 可以使用 database.py 中的 query_weekly_data() 方法查询数据")
                logger.info("   - 支持按股票代码、日期范围等条件筛选")
                logger.info("   - 数据表名: weekly_data")
                logger.info("   - 建议设置定时任务每周同步最新数据")
                return True
            else:
                logger.error("❌ 股票周线数据初始化失败")
                return False
            
    except KeyboardInterrupt:
        logger.warning("⚠️ 用户中断程序执行")
        return False
    except Exception as e:
        logger.error(f"❌ 程序执行出现异常: {e}")
        return False
    finally:
        end_time = datetime.now()
        total_duration = end_time - start_time
        logger.info(f"\n⏰ 程序总执行时间: {total_duration}")


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        sys.exit(1)
