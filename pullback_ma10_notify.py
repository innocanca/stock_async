#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
强势回踩10日线股票推送脚本
筛选前3天涨幅≥25%，然后回调3-5天并接近10日线的股票
"""

import logging
import sys
from datetime import datetime
from database import StockDatabase
from send_msg import send_markdown_message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pullback_ma10_notify.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def format_stock_code(ts_code: str) -> str:
    """格式化股票代码，去掉交易所后缀"""
    return ts_code.split('.')[0] if '.' in ts_code else ts_code


def get_stock_market(ts_code: str) -> str:
    """根据股票代码获取市场名称"""
    if '.SH' in ts_code:
        return '上交所'
    elif '.SZ' in ts_code:
        return '深交所'
    elif '.BJ' in ts_code:
        return '北交所'
    else:
        return '其他'


def create_pullback_ma10_markdown(df, query_date: str, strong_rise_days: int, 
                                  min_rise_pct: float, pullback_days_range: tuple,
                                  ma10_tolerance: float) -> str:
    """
    创建强势回踩10日线的markdown格式消息
    
    Args:
        df: 符合条件的股票数据DataFrame
        query_date: 查询日期
        strong_rise_days: 强势上涨天数
        min_rise_pct: 最小上涨幅度
        pullback_days_range: 回调天数范围(min, max)
        ma10_tolerance: 10日线容忍度
        
    Returns:
        str: markdown格式的消息内容
    """
    if df.empty:
        return f"""# 📊 主板强势回踩10日线播报

**查询日期**: {query_date}
**查询范围**: 最近10个交易日
**筛选范围**: 沪深主板股票（排除创业板、科创板、北交所）
**筛选条件**: 前{strong_rise_days}天涨幅≥{min_rise_pct}%，回调{pullback_days_range[0]}-{pullback_days_range[1]}天，接近10日线(±{ma10_tolerance}%)

> 今日无符合条件的主板强势回踩股票

---
*数据来源: Tushare*  
*发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""

    # 统计信息
    total_count = len(df)
    avg_rise_pct = df['rise_pct'].mean()
    avg_pullback_pct = df['pullback_pct'].mean()
    
    # 按行业分组统计
    industry_stats = df['industry'].value_counts()
    
    # 构建markdown消息
    markdown_content = f"""# 🎯 主板强势回踩10日线播报

**查询日期**: {query_date}  
**查询范围**: 最近10个交易日  
**筛选范围**: 沪深主板股票（排除创业板、科创板、北交所）  
**筛选条件**: 前{strong_rise_days}天涨幅≥{min_rise_pct}%，回调{pullback_days_range[0]}-{pullback_days_range[1]}天，接近10日线(±{ma10_tolerance}%)  
**符合股票数**: {total_count}只  
**平均前期涨幅**: {avg_rise_pct:.1f}%  
**平均回调幅度**: {avg_pullback_pct:.1f}%

## 🚀 强势回踩股票榜单

| 排名 | 股票名称 | 代码 | 行业 | 前期涨幅 | 回调幅度 | 回调天数 | 距MA10 | 当前价格 | 10日线 |
|------|----------|------|------|----------|----------|----------|--------|----------|--------|"""

    # 添加股票信息
    for idx, (_, row) in enumerate(df.iterrows(), 1):
        stock_code = format_stock_code(row['ts_code'])
        stock_name = row.get('name', '未知')
        industry = row.get('industry', '未知')
        rise_pct = row['rise_pct']
        pullback_pct = row['pullback_pct']
        pullback_days = int(row['pullback_days'])
        distance_ma10 = row['distance_from_ma10']
        current_price = row['current_price']
        ma10 = row['ma10']
        
        # 截断过长的股票名称和行业
        if len(stock_name) > 6:
            stock_name = stock_name[:5] + '..'
        if len(industry) > 6:
            industry = industry[:5] + '..'
        
        # 距离MA10的显示
        distance_str = f"{distance_ma10:+.1f}%" if abs(distance_ma10) >= 0.1 else "0.0%"
        
        markdown_content += f"\n| {idx} | {stock_name} | {stock_code} | {industry} | {rise_pct:.1f}% | {pullback_pct:.1f}% | {pullback_days}天 | {distance_str} | {current_price:.2f} | {ma10:.2f} |"
        
        # 限制显示前20只
        if idx >= 20:
            remaining = total_count - 20
            if remaining > 0:
                markdown_content += f"\n| ... | ... | ... | ... | ... | ... | ... | ... | ... | 还有{remaining}只 |"
            break

    # 添加详细分析
    if not df.empty:
        # 统计位置分布
        above_ma10 = len(df[df['distance_from_ma10'] > 0])
        on_ma10 = len(df[abs(df['distance_from_ma10']) <= 0.5])
        below_ma10 = len(df[df['distance_from_ma10'] < 0])
        
        markdown_content += f"""

## 📊 10日线位置分布

- 🟢 **站上10日线**: {above_ma10}只 ({above_ma10/total_count*100:.1f}%)
- 🟡 **贴近10日线**: {on_ma10}只 ({on_ma10/total_count*100:.1f}%)  
- 🔴 **略破10日线**: {below_ma10}只 ({below_ma10/total_count*100:.1f}%)

## 📈 强势程度分布

"""
        # 按前期涨幅分类统计
        rise_30_plus = len(df[df['rise_pct'] >= 30])
        rise_25_30 = len(df[(df['rise_pct'] >= 25) & (df['rise_pct'] < 30)])
        
        markdown_content += f"""- 🔥 **超强势(≥30%)**: {rise_30_plus}只
- 🚀 **强势(25-30%)**: {rise_25_30}只

## 📋 回调时间分布

"""
        # 按回调天数分类统计
        pullback_3_days = len(df[df['pullback_days'] == 3])
        pullback_4_days = len(df[df['pullback_days'] == 4])
        pullback_5_days = len(df[df['pullback_days'] == 5])
        
        markdown_content += f"""- **3天回调**: {pullback_3_days}只
- **4天回调**: {pullback_4_days}只
- **5天回调**: {pullback_5_days}只"""

    # 添加行业分布统计
    if len(industry_stats) > 0:
        markdown_content += f"""

## 📊 行业分布统计

"""
        # 按股票数量排序显示前8个行业
        for idx, (industry, count) in enumerate(industry_stats.head(8).items(), 1):
            emoji = "🔥" if idx == 1 else "🚀" if idx <= 3 else "📈"
            markdown_content += f"- {emoji} **{industry}**: {count}只\n"
        
        if len(industry_stats) > 8:
            markdown_content += f"- 📋 其他行业: {len(industry_stats) - 8}个\n"

    # 市场分布统计
    if not df.empty:
        markdown_content += f"""

## 📊 市场分布统计

"""
        market_stats = df['ts_code'].apply(get_stock_market).value_counts()
        for market, count in market_stats.items():
            markdown_content += f"- **{market}**: {count}只\n"

    markdown_content += f"""

---
💡 **策略解读**: 强势上涨后适度回调至10日线，通常是较好的介入时机  
⚠️  **风险提示**: 回踩不破是强势延续信号，破位则需谨慎  
📈 **操作建议**: 结合成交量和大盘环境综合判断  
*数据来源: Tushare*  
*发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""

    return markdown_content


def main():
    """主函数"""
    logger.info("开始查询强势回踩10日线股票...")
    
    # 配置参数
    STRONG_RISE_DAYS = 3      # 前期强势上涨天数
    MIN_RISE_PCT = 25.0       # 最小上涨幅度25%
    PULLBACK_DAYS_MIN = 3     # 最小回调天数
    PULLBACK_DAYS_MAX = 5     # 最大回调天数
    MA10_TOLERANCE = 3.0      # 10日线容忍度3%
    
    try:
        # 连接数据库
        with StockDatabase() as db:
            # 获取最近交易日期
            latest_date = db.get_latest_trading_date()
            if not latest_date:
                logger.error("无法获取最近交易日期，请检查数据库中是否有数据")
                return 1
            
            logger.info(f"查询日期: {latest_date}")
            logger.info(f"查询范围: 最近10个交易日，仅沪深主板股票")
            logger.info(f"筛选条件: 前{STRONG_RISE_DAYS}天涨幅≥{MIN_RISE_PCT}%，"
                       f"回调{PULLBACK_DAYS_MIN}-{PULLBACK_DAYS_MAX}天，"
                       f"接近10日线(±{MA10_TOLERANCE}%)")
            
            # 查询强势回踩10日线股票
            pullback_stocks_df = db.get_pullback_to_ma10_stocks(
                strong_rise_days=STRONG_RISE_DAYS,
                min_rise_pct=MIN_RISE_PCT,
                pullback_days_min=PULLBACK_DAYS_MIN,
                pullback_days_max=PULLBACK_DAYS_MAX,
                ma10_tolerance=MA10_TOLERANCE
            )
            
            if pullback_stocks_df is None:
                logger.error("查询强势回踩股票失败")
                return 1
            
            if pullback_stocks_df.empty:
                logger.info("未找到符合条件的强势回踩10日线股票")
                return
                
            else:
                logger.info(f"找到 {len(pullback_stocks_df)} 只强势回踩10日线股票")
                
                # 显示前5只股票示例
                logger.info("强势回踩股票示例：")
                for idx, row in pullback_stocks_df.head(5).iterrows():
                    logger.info(f"  {row['name']}({row['ts_code']}) "
                              f"前期涨幅{row['rise_pct']:.1f}% "
                              f"回调{row['pullback_pct']:.1f}% "
                              f"距MA10: {row['distance_from_ma10']:+.1f}%")
                
                # 创建markdown消息
                markdown_msg = create_pullback_ma10_markdown(
                    pullback_stocks_df, latest_date, STRONG_RISE_DAYS,
                    MIN_RISE_PCT, (PULLBACK_DAYS_MIN, PULLBACK_DAYS_MAX),
                    MA10_TOLERANCE
                )
            
            # 发送消息
            logger.info("准备发送强势回踩股票消息...")
            send_markdown_message(markdown_msg)
            
            if not pullback_stocks_df.empty:
                # 统计信息
                total_count = len(pullback_stocks_df)
                avg_rise_pct = pullback_stocks_df['rise_pct'].mean()
                strongest_stock = pullback_stocks_df.loc[pullback_stocks_df['rise_pct'].idxmax()]
                
                logger.info(f"强势回踩股票查询完成: {total_count}只股票，"
                          f"平均前期涨幅{avg_rise_pct:.1f}%，"
                          f"最强势: {strongest_stock['name']}({strongest_stock['rise_pct']:.1f}%)")
            else:
                logger.info("强势回踩股票查询完成，未找到符合条件的股票")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
