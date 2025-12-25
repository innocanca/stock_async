#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
综合财务数据初始化脚本

功能：
1. 一次性初始化利润表、现金流量表和分红送股数据
2. 创建所有必要的数据库表结构
3. 批量获取并存储财务数据
4. 提供完整的初始化流程

使用方法：
python init_all_financial.py
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
from log_config import get_logger

logger = get_logger(__name__)


def create_all_financial_tables(db: StockDatabase) -> bool:
    """
    创建所有财务数据相关的数据库表
    
    Args:
        db: 数据库实例
        
    Returns:
        bool: 创建是否成功
    """
    logger.info("🔧 开始创建财务数据库表...")
    
    try:
        # 创建数据库（如果不存在）
        if not db.create_database():
            logger.error("❌ 创建数据库失败")
            return False
        
        # 连接数据库
        if not db.connect():
            logger.error("❌ 连接数据库失败")
            return False
        
        tables_created = 0
        
        # 创建利润表数据表
        if db.create_income_table():
            tables_created += 1
            logger.info("✅ 利润表数据表创建成功")
        else:
            logger.error("❌ 创建利润表数据表失败")
            
        # 创建现金流量表数据表
        if db.create_cashflow_table():
            tables_created += 1
            logger.info("✅ 现金流量表数据表创建成功")
        else:
            logger.error("❌ 创建现金流量表数据表失败")
            
        # 创建资产负债表数据表
        if db.create_balancesheet_table():
            tables_created += 1
            logger.info("✅ 资产负债表数据表创建成功")
        else:
            logger.error("❌ 创建资产负债表数据表失败")
            
        # 创建分红送股数据表
        if db.create_dividend_table():
            tables_created += 1
            logger.info("✅ 分红送股数据表创建成功")
        else:
            logger.error("❌ 创建分红送股数据表失败")
        
        if tables_created == 4:
            logger.info("🎉 所有财务数据表创建成功")
            return True
        else:
            logger.error(f"❌ 只成功创建了 {tables_created}/4 个表")
            return False
        
    except Exception as e:
        logger.error(f"❌ 创建数据库表时发生错误: {e}")
        return False


def get_stock_list(db: StockDatabase) -> List[str]:
    """
    从数据库获取股票列表（用于财务数据初始化）
    
    Args:
        db: 数据库实例
        
    Returns:
        List[str]: 股票代码列表
    """
    logger.info("📋 从数据库获取股票列表...")
    
    try:
        # 从数据库的stock_basic表获取主板股票列表
        cursor = db.connection.cursor()

        # 查询主板股票，排除ST、退市等 - 全量获取
        query_sql = """
        SELECT DISTINCT ts_code 
        FROM stock_basic 
        WHERE list_status = 'L'
          AND name NOT LIKE '%ST%'
          AND name NOT LIKE '%退%'
          AND name NOT LIKE '%暂停%'
          AND (ts_code LIKE '60____.SH' OR ts_code LIKE '00____.SZ')
        ORDER BY ts_code
        """

        cursor.execute(query_sql)
        results = cursor.fetchall()

        if not results:
            logger.warning("⚠️ 数据库中未找到股票基础信息，使用备用列表")
            # 使用备用的知名股票列表
            return [
                '000001.SZ', '000002.SZ', '000063.SZ', '000100.SZ', '000157.SZ',
                '000333.SZ', '000858.SZ', '000895.SZ', '600000.SH', '600036.SH',
                '600519.SH', '600887.SH', '601318.SH', '601398.SH', '601939.SH'
            ]

        stock_codes = [result[0] for result in results]
        logger.info(f"📈 从数据库获取到 {len(stock_codes)} 只主板股票（全量初始化）")

        return stock_codes

    except Exception as e:
        logger.error(f"❌ 从数据库获取股票列表失败: {e}")
        # 使用备用的知名股票列表
        logger.info("使用备用股票列表")
        return [
            '000001.SZ', '000002.SZ', '000063.SZ', '000100.SZ', '000157.SZ',
            '000333.SZ', '000858.SZ', '000895.SZ', '600000.SH', '600036.SH',
            '600519.SH', '600887.SH', '601318.SH', '601398.SH', '601939.SH'
        ]


def initialize_financial_data(fetcher: StockDataFetcher, db: StockDatabase, 
                             stock_codes: List[str]) -> dict:
    """
    初始化所有财务数据
    
    Args:
        fetcher: 数据获取器实例
        db: 数据库实例
        stock_codes: 股票代码列表
        
    Returns:
        dict: 综合统计信息
    """
    overall_stats = {
        'income': {'success': False, 'records': 0, 'stocks': 0},
        'cashflow': {'success': False, 'records': 0, 'stocks': 0},
        'balancesheet': {'success': False, 'records': 0, 'stocks': 0},
        'dividend': {'success': False, 'records': 0, 'stocks': 0},
        'total_duration': None,
        'start_time': datetime.now()
    }
    
    logger.info("📊 开始初始化所有财务数据...")
    logger.info(f"   股票数量: {len(stock_codes)} 只")
    
    try:
        # 1. 初始化利润表数据
        logger.info("\n🔥 第1步：初始化利润表数据...")
        logger.info("-" * 50)
        
        income_df = fetcher.get_multiple_stocks_financial_data(
            stock_codes=stock_codes,
            data_type='income',
            years_back=3,
            batch_size=20,
            delay=0.5
        )
        
        if income_df is not None and not income_df.empty:
            if db.insert_income_data(income_df):
                overall_stats['income']['success'] = True
                overall_stats['income']['records'] = len(income_df)
                overall_stats['income']['stocks'] = income_df['ts_code'].nunique()
                logger.info(f"✅ 利润表数据初始化成功: {overall_stats['income']['stocks']}只股票, {overall_stats['income']['records']}条记录")
            else:
                logger.error("❌ 利润表数据插入失败")
        else:
            logger.error("❌ 未获取到利润表数据")
        
        # 2. 初始化现金流量表数据
        logger.info("\n💰 第2步：初始化现金流量表数据...")
        logger.info("-" * 50)
        
        cashflow_df = fetcher.get_multiple_stocks_financial_data(
            stock_codes=stock_codes,
            data_type='cashflow',
            years_back=3,
            batch_size=20,
            delay=0.5
        )
        
        if cashflow_df is not None and not cashflow_df.empty:
            if db.insert_cashflow_data(cashflow_df):
                overall_stats['cashflow']['success'] = True
                overall_stats['cashflow']['records'] = len(cashflow_df)
                overall_stats['cashflow']['stocks'] = cashflow_df['ts_code'].nunique()
                logger.info(f"✅ 现金流量表数据初始化成功: {overall_stats['cashflow']['stocks']}只股票, {overall_stats['cashflow']['records']}条记录")
            else:
                logger.error("❌ 现金流量表数据插入失败")
        else:
            logger.error("❌ 未获取到现金流量表数据")

        # 3. 初始化资产负债表数据
        logger.info("\n🏛️ 第3步：初始化资产负债表数据...")
        logger.info("-" * 50)
        
        balancesheet_df = fetcher.get_multiple_stocks_financial_data(
            stock_codes=stock_codes,
            data_type='balancesheet',
            years_back=3,
            batch_size=20,
            delay=0.5
        )
        
        if balancesheet_df is not None and not balancesheet_df.empty:
            if db.insert_balancesheet_data(balancesheet_df):
                overall_stats['balancesheet']['success'] = True
                overall_stats['balancesheet']['records'] = len(balancesheet_df)
                overall_stats['balancesheet']['stocks'] = balancesheet_df['ts_code'].nunique()
                logger.info(f"✅ 资产负债表数据初始化成功: {overall_stats['balancesheet']['stocks']}只股票, {overall_stats['balancesheet']['records']}条记录")
            else:
                logger.error("❌ 资产负债表数据插入失败")
        else:
            logger.error("❌ 未获取到资产负债表数据")
        
        # 4. 初始化分红送股数据
        logger.info("\n🎁 第4步：初始化分红送股数据...")
        logger.info("-" * 50)
        
        dividend_df = fetcher.get_multiple_stocks_financial_data(
            stock_codes=stock_codes,
            data_type='dividend',
            years_back=5,
            batch_size=30,
            delay=0.3
        )
        
        if dividend_df is not None and not dividend_df.empty:
            if db.insert_dividend_data(dividend_df):
                overall_stats['dividend']['success'] = True
                overall_stats['dividend']['records'] = len(dividend_df)
                overall_stats['dividend']['stocks'] = dividend_df['ts_code'].nunique()
                logger.info(f"✅ 分红送股数据初始化成功: {overall_stats['dividend']['stocks']}只股票, {overall_stats['dividend']['records']}条记录")
            else:
                logger.error("❌ 分红送股数据插入失败")
        else:
            logger.error("❌ 未获取到分红送股数据")
        
    except Exception as e:
        logger.error(f"❌ 财务数据初始化过程发生错误: {e}")
    
    finally:
        overall_stats['total_duration'] = datetime.now() - overall_stats['start_time']
    
    return overall_stats


def display_final_summary(stats: dict):
    """
    显示最终的初始化总结
    
    Args:
        stats: 统计信息
    """
    logger.info("\n" + "=" * 80)
    logger.info("🎉 财务数据初始化完成总结")
    logger.info("=" * 80)
    
    success_count = sum(1 for data_type, info in stats.items() 
                       if data_type != 'total_duration' and data_type != 'start_time' 
                       and info.get('success', False))
    
    logger.info(f"📊 初始化结果: {success_count}/4 个数据类型成功")
    logger.info("")
    
    # 详细统计
    for data_type, info in stats.items():
        if data_type in ['total_duration', 'start_time']:
            continue
            
        data_type_name = {
            'income': '利润表',
            'cashflow': '现金流量表',
            'balancesheet': '资产负债表',
            'dividend': '分红送股'
        }.get(data_type, data_type)
        
        status = "✅ 成功" if info.get('success', False) else "❌ 失败"
        logger.info(f"   {data_type_name:<12} {status:<8} 股票:{info.get('stocks', 0):>3}只 记录:{info.get('records', 0):>5}条")
    
    logger.info(f"\n⏱️  总耗时: {stats['total_duration']}")
    
    if success_count > 0:
        logger.info("\n💡 使用建议：")
        if stats['income']['success']:
            logger.info("   📈 利润表数据可用于盈利能力分析")
        if stats['cashflow']['success']:
            logger.info("   💰 现金流数据可用于资金状况分析")
        if stats.get('balancesheet', {}).get('success', False):
            logger.info("   🏛️ 资产负债表数据可用于资产质量分析")
        if stats['dividend']['success']:
            logger.info("   🎁 分红数据可用于股息率和分红策略分析")
        
        logger.info("\n📝 数据查询示例：")
        logger.info("   - 查询利润表: SELECT * FROM income_data WHERE ts_code='000001.SZ'")
        logger.info("   - 查询现金流: SELECT * FROM cashflow_data WHERE ts_code='000001.SZ'")
        logger.info("   - 查询资产负债表: SELECT * FROM balancesheet_data WHERE ts_code='000001.SZ'")
        logger.info("   - 查询分红: SELECT * FROM dividend_data WHERE ts_code='000001.SZ'")
    
    if success_count < 4:
        logger.error("\n⚠️  部分数据初始化失败，可能原因：")
        logger.error("   1. Tushare API权限不足")
        logger.error("   2. 网络连接问题")
        logger.error("   3. API调用频率限制")
        logger.error("   建议检查日志文件获取详细错误信息")


def main():
    """主函数"""
    logger.info("🚀 综合财务数据初始化开始...")
    logger.info("=" * 60)
    logger.info("📋 将初始化以下数据：")
    logger.info("   1. 📈 利润表数据 (最近3年)")
    logger.info("   2. 💰 现金流量表数据 (最近3年)")
    logger.info("   3. 🏛️ 资产负债表数据 (最近3年)")
    logger.info("   4. 🎁 分红送股数据 (最近5年)")
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
            
            # 创建所有财务数据表
            if not create_all_financial_tables(db):
                logger.error("❌ 数据库表创建失败，退出程序")
                return False
            
            # 获取股票列表
            stock_codes = get_stock_list(db)
            if not stock_codes:
                logger.error("❌ 获取股票列表失败，退出程序")
                return False
            
            # 初始化所有财务数据
            stats = initialize_financial_data(fetcher, db, stock_codes)
            
            # 显示最终总结
            display_final_summary(stats)
            
            # 判断整体成功状态
            success_count = sum(1 for data_type, info in stats.items() 
                               if data_type not in ['total_duration', 'start_time'] 
                               and info.get('success', False))
            
            if success_count >= 2:  # 至少2个数据类型成功
                logger.info("🎉 财务数据初始化基本成功！")
                return True
            else:
                logger.error("❌ 财务数据初始化失败")
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
