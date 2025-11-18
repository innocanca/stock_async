#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数日线行情数据初始化脚本

功能：
1. 获取Tushare指数日线行情数据(index_daily接口)
2. 创建指数日线行情数据库表结构
3. 将数据初始化到数据库中
4. 提供数据查询和统计功能

使用方法：
python init_index_daily.py

对应Tushare文档：
https://tushare.pro/document/2?doc_id=96
"""

import logging
import sys
import os
from datetime import datetime, timedelta

# 添加父目录到Python路径，以便导入database和fetcher模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher

# 配置日志
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
            
        # 创建指数日线行情表
        if not db.create_index_daily_table():
            logger.error("❌ 创建指数日线行情表失败")
            return False
            
        logger.info("✅ 数据库表创建成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据库表时发生错误: {e}")
        return False


def fetch_and_store_index_daily_data(fetcher: StockDataFetcher, db: StockDatabase, 
                                    start_date: str = None, end_date: str = None) -> dict:
    """
    获取并存储指数日线行情数据
    
    Args:
        fetcher: 数据获取器实例
        db: 数据库实例
        start_date: 开始日期 (YYYYMMDD格式)
        end_date: 结束日期 (YYYYMMDD格式)
        
    Returns:
        dict: 统计信息
    """
    stats = {
        'total_records': 0,
        'total_indexes': 0,
        'successful_insert': False,
        'date_range': {},
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    # 默认获取最近3个月的数据
    if not start_date or not end_date:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=90)
        start_date = start_dt.strftime('%Y%m%d')
        end_date = end_dt.strftime('%Y%m%d')
    
    logger.info(f"📊 开始获取指数日线行情数据 ({start_date} 到 {end_date})...")
    
    try:
        # 获取主要指数的日线行情数据
        df = fetcher.get_major_index_daily_data(start_date, end_date)
        
        if df is None or df.empty:
            logger.error("❌ 未获取到任何指数日线行情数据")
            return stats
        
        stats['total_records'] = len(df)
        stats['total_indexes'] = df['ts_code'].nunique() if 'ts_code' in df.columns else 0
        
        # 统计日期范围
        if 'trade_date' in df.columns:
            stats['date_range'] = {
                'start_date': df['trade_date'].min(),
                'end_date': df['trade_date'].max()
            }
        
        logger.info(f"📈 成功获取 {stats['total_records']} 条指数日线行情数据")
        logger.info(f"📈 涉及指数: {stats['total_indexes']} 个")
        
        # 插入数据库
        logger.info("💾 开始插入数据到数据库...")
        
        if db.insert_index_daily(df):
            stats['successful_insert'] = True
            logger.info("✅ 数据插入成功！")
            
            # 显示统计信息
            logger.info("📊 数据统计：")
            logger.info(f"   总记录数: {stats['total_records']} 条")
            logger.info(f"   涉及指数: {stats['total_indexes']} 个")
            if stats['date_range']:
                logger.info(f"   日期范围: {stats['date_range']['start_date']} 到 {stats['date_range']['end_date']}")
            
            # 显示各指数数据量统计
            if 'ts_code' in df.columns:
                index_counts = df['ts_code'].value_counts()
                logger.info("   各指数数据量：")
                for ts_code, count in index_counts.head(5).items():
                    index_name = _get_index_name(ts_code)
                    logger.info(f"     {index_name}({ts_code}): {count} 条")
        else:
            logger.error("❌ 数据插入失败")
            
    except Exception as e:
        logger.error(f"❌ 获取和存储数据时发生错误: {e}")
    
    finally:
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
    
    return stats


def _get_index_name(ts_code: str) -> str:
    """
    获取指数中文名称
    
    Args:
        ts_code: 指数代码
        
    Returns:
        str: 中文名称
    """
    index_mapping = {
        '000001.SH': '上证综指',
        '000300.SH': '沪深300',
        '000905.SH': '中证500',
        '000016.SH': '上证50',
        '399001.SZ': '深证成指',
        '399006.SZ': '创业板指',
        '399303.SZ': '国证2000',
        '000852.SH': '中证1000',
        '000688.SH': '科创50',
    }
    return index_mapping.get(ts_code, '未知指数')


def query_and_display_data(db: StockDatabase) -> None:
    """
    查询并显示数据库中的指数日线行情数据
    
    Args:
        db: 数据库实例
    """
    logger.info("🔍 查询数据库中的指数日线行情数据...")
    
    try:
        # 查询最新10条数据
        df = db.query_index_daily(limit=10)
        
        if df is None or df.empty:
            logger.warning("⚠️ 数据库中没有指数日线行情数据")
            return
        
        logger.info(f"📋 数据库中共有 {len(df)} 条指数日线行情记录")
        logger.info("📖 最新10条记录示例：")
        
        for i, (_, row) in enumerate(df.head(10).iterrows(), 1):
            index_name = _get_index_name(row.get('ts_code', 'N/A'))
            logger.info(f"   {i:2d}. {index_name}({row.get('ts_code', 'N/A')}) "
                       f"{row.get('trade_date', 'N/A')} - 收盘:{row.get('close', 'N/A')} "
                       f"涨跌:{row.get('change_pct', 'N/A')}%")
        
        # 按指数统计
        logger.info("\n📈 按指数统计最新数据：")
        if 'ts_code' in df.columns:
            index_counts = df['ts_code'].value_counts()
            for ts_code, count in index_counts.head(10).items():
                index_name = _get_index_name(ts_code)
                logger.info(f"   {index_name}({ts_code}): {count} 条")
        
    except Exception as e:
        logger.error(f"❌ 查询数据时发生错误: {e}")


def main():
    """主函数"""
    logger.info("🚀 指数日线行情数据初始化开始...")
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
            
            # 获取并存储数据（默认最近3个月）
            stats = fetch_and_store_index_daily_data(fetcher, db)
            
            # 查询并显示数据（验证插入结果）
            if stats['successful_insert']:
                query_and_display_data(db)
            
            # 显示总体统计
            logger.info("\n" + "=" * 60)
            logger.info("📊 初始化完成统计：")
            logger.info(f"   📈 获取记录总数: {stats['total_records']} 条")
            logger.info(f"   📊 涉及指数: {stats['total_indexes']} 个")
            logger.info(f"   💾 数据插入状态: {'成功' if stats['successful_insert'] else '失败'}")
            logger.info(f"   ⏱️  总耗时: {stats['duration']}")
            
            if stats['successful_insert']:
                logger.info("🎉 指数日线行情数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 可以使用 database.py 中的 query_index_daily() 方法查询数据")
                logger.info("   - 支持按指数代码、日期范围等条件筛选")
                logger.info("   - 数据表名: index_daily")
                logger.info("   - 默认获取最近3个月的主要指数数据")
                return True
            else:
                logger.error("❌ 指数日线行情数据初始化失败")
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
