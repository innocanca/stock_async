#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF周线放量推送脚本
每日推送周线明显放量的ETF列表
"""

import logging
import sys
import os
from datetime import datetime

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 配置日志（先配置日志，以便后续错误处理可以使用）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_weekly_volume_surge_notify.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 检查并导入必要的模块
try:
    from query.strategy.query_etf_weekly_volume_surge import ETFWeeklyVolumeSurgeAnalyzer
    from send_msg import send_markdown_message
except ModuleNotFoundError as e:
    if 'config' in str(e):
        logger.error("❌ 缺少 config.py 配置文件")
        logger.error("请创建 config.py 文件，包含 MYSQL_CONFIG 配置")
        logger.error("示例配置:")
        logger.error("  MYSQL_CONFIG = {")
        logger.error("      'host': 'localhost',")
        logger.error("      'user': 'your_user',")
        logger.error("      'password': 'your_password',")
        logger.error("      'database': 'your_database',")
        logger.error("      'charset': 'utf8mb4'")
        logger.error("  }")
        sys.exit(1)
    else:
        raise


def format_etf_markdown(results: list, min_ratio: float = 1.5, lookback_weeks: int = 3) -> str:
    """
    创建ETF周线放量的markdown格式消息
    
    Args:
        results: ETF分析结果列表
        min_ratio: 最小放量倍数
        lookback_weeks: 回看周数
        
    Returns:
        str: markdown格式的消息内容
    """
    today = datetime.now().strftime('%Y-%m-%d')
    
    if not results:
        return f"""# 📊 ETF周线放量播报

**日期**: {today}

> 今日未发现周线明显放量的ETF

**筛选条件**:
- 放量倍数: ≥ {min_ratio}倍
- 回看周数: {lookback_weeks}周
- 最近一周成交额: ≥ 1.0亿元

---
*数据来源: Tushare*  
*发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""

    # 统计信息
    total_count = len(results)
    avg_ratio = sum(r['周放量倍数'] for r in results) / total_count if total_count > 0 else 0
    total_amount = sum(r['最近一周成交额(亿元)'] for r in results)
    
    # 找出放量倍数最高的ETF
    max_ratio_etf = max(results, key=lambda x: x['周放量倍数'])
    
    # 构建markdown消息
    markdown_content = f"""# 📈 ETF周线放量播报

**日期**: {today}  
**发现数量**: {total_count}只  
**平均放量倍数**: {avg_ratio:.2f}倍  
**总成交额**: {total_amount:.2f}亿元  
**最高放量**: {max_ratio_etf['名称']} ({max_ratio_etf['代码']}) - {max_ratio_etf['周放量倍数']:.2f}倍

**筛选条件**:
- 放量倍数: ≥ {min_ratio}倍
- 回看周数: {lookback_weeks}周
- 最近一周成交额: ≥ 1.0亿元

## 🏆 ETF周线放量榜单

| 排名 | ETF名称 | 代码 | 最近周线截止日 | 最近一周成交量(手) | 最近一周成交额(亿元) | 过去3周最大周成交量(手) | 周放量倍数 |
|------|---------|------|----------------|-------------------|---------------------|----------------------|-----------|"""

    # 添加ETF信息
    for idx, etf in enumerate(results, 1):
        etf_name = etf.get('名称', etf.get('代码', '未知'))
        # 截断过长的名称
        if len(etf_name) > 15:
            etf_name = etf_name[:14] + '...'
        
        code = etf.get('代码', etf.get('ts_code', '未知'))
        week_end = etf.get('最近周线截止日', '未知')
        last_week_vol = etf.get('最近一周成交量(手)', 0)
        last_week_amount = etf.get('最近一周成交额(亿元)', 0)
        max_prev_vol = etf.get('过去3周最大周成交量(手)', 0)
        volume_ratio = etf.get('周放量倍数', 0)
        
        markdown_content += f"\n| {idx} | {etf_name} | {code} | {week_end} | {last_week_vol:,.0f} | {last_week_amount:.2f} | {max_prev_vol:,.0f} | {volume_ratio:.2f} |"
        
        # 限制显示前30只
        if idx >= 30:
            remaining = total_count - 30
            if remaining > 0:
                markdown_content += f"\n| ... | ... | ... | ... | ... | ... | ... | 还有{remaining}只 |"
            break

    # 添加统计信息
    if total_count > 0:
        # 按放量倍数分组统计
        high_ratio = [r for r in results if r['周放量倍数'] >= 2.0]
        medium_ratio = [r for r in results if 1.5 <= r['周放量倍数'] < 2.0]
        
        markdown_content += f"""

## 📊 放量倍数分布

- **高倍放量(≥2.0倍)**: {len(high_ratio)}只
- **中等放量(1.5-2.0倍)**: {len(medium_ratio)}只

## 💡 说明

- **放量倍数**: 最近一周成交量 / 过去{lookback_weeks}周最大周成交量
- **成交额**: 最近一周成交额（亿元）
- 数据按放量倍数和成交额综合排序

---
*数据来源: Tushare*  
*发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""

    return markdown_content


def main():
    """主函数"""
    logger.info("=== ETF周线放量推送开始 ===")
    
    try:
        # 创建分析器
        analyzer = ETFWeeklyVolumeSurgeAnalyzer()
        
        # 先测试数据库连接
        logger.info("检查数据库连接...")
        if not analyzer.db.connect():
            logger.error("❌ 数据库连接失败，请检查 config.py 中的数据库配置")
            logger.error(f"数据库配置: host={analyzer.db.config.get('host')}, user={analyzer.db.config.get('user')}, database={analyzer.db.config.get('database')}")
            # 即使连接失败，也发送一个错误通知
            error_msg = format_etf_markdown([])
            error_msg = error_msg.replace("今日未发现周线明显放量的ETF", "⚠️ 数据库连接失败，无法获取ETF数据\n\n请检查数据库配置和连接状态")
            send_markdown_message(error_msg)
            return 1
        else:
            logger.info("✅ 数据库连接成功")
            analyzer.db.disconnect()
        
        # 获取分析结果（使用默认参数）
        logger.info("正在查询ETF周线放量数据...")
        results = analyzer.get_analysis_results(
            min_ratio=1.5,
            lookback_weeks=3,
            min_last_week_amount_yi=1.0,
        )
        
        if not results:
            logger.info("未发现符合条件的ETF")
        else:
            logger.info(f"发现 {len(results)} 只符合条件的ETF")
            # 打印前3条结果用于调试
            for i, r in enumerate(results[:3], 1):
                logger.info(f"  {i}. {r.get('名称', r.get('代码'))} - 放量倍数: {r.get('周放量倍数', 0):.2f}倍")
        
        # 生成推送消息
        markdown_msg = format_etf_markdown(results)
        
        # 发送推送
        logger.info("准备发送ETF周线放量推送消息...")
        success = send_markdown_message(markdown_msg)
        
        if success:
            logger.info("✅ ETF周线放量推送发送成功")
            if results:
                logger.info(f"推送了 {len(results)} 只符合条件的ETF")
        else:
            logger.error("❌ 推送发送失败")
            
    except Exception as e:
        logger.error(f"ETF周线放量推送失败: {e}", exc_info=True)
        # 发送错误通知
        try:
            error_msg = f"""# ❌ ETF周线放量推送失败

**错误信息**: {str(e)}

**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

请检查日志文件获取详细信息。
"""
            send_markdown_message(error_msg)
        except:
            pass
        return 1
    
    logger.info("=== ETF周线放量推送结束 ===")
    return 0


if __name__ == "__main__":
    exit(main())

