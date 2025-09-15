# -*- coding: utf-8 -*-
"""
涨停股票查询与推送脚本
从MySQL数据库查询最近一个交易日的涨停股票，并通过企业微信机器人发送markdown格式的消息
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
        logging.FileHandler('limit_up_notify.log', encoding='utf-8'),
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


def create_limit_up_markdown(df, trade_date: str) -> str:
    """
    创建涨停股票的markdown格式消息
    
    Args:
        df: 涨停股票数据DataFrame
        trade_date: 交易日期
        
    Returns:
        str: markdown格式的消息内容
    """
    if df.empty:
        return f"""# 📊 涨停股票播报

**交易日期**: {trade_date}

> 今日无涨停股票

---
*数据来源: Tushare*  
*发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""

    # 统计信息
    total_count = len(df)
    avg_pct = df['change_pct'].mean()
    
    # 构建markdown消息
    markdown_content = f"""# 🚀 涨停股票播报

**交易日期**: {trade_date}  
**涨停数量**: {total_count}只  
**平均涨幅**: {avg_pct:.2f}%

## 📈 涨停榜单

| 排名 | 股票名称 | 代码 | 成交额(亿元) | 行业 |
|------|----------|------|-------------|------|"""

    # 添加股票信息
    for idx, (_, row) in enumerate(df.iterrows(), 1):
        stock_code = format_stock_code(row['ts_code'])
        stock_name = row.get('name', '未知')  # 获取股票名称
        industry = row.get('industry', '未知')  # 获取行业
        amount = row['amount'] / 100000  # 转换为亿元（原单位：千元）
        
        # 截断过长的股票名称和行业名称
        if len(stock_name) > 8:
            stock_name = stock_name[:7] + '...'
        if len(industry) > 8:
            industry = industry[:7] + '...'
        
        markdown_content += f"\n| {idx} | {stock_name} | {stock_code} | {amount:.2f} | {industry} |"
        
        # 限制显示前20只
        if idx >= 20:
            remaining = total_count - 20
            if remaining > 0:
                markdown_content += f"\n| ... | ... | ... | ... | 还有{remaining}只 |"
            break

    markdown_content += f"""

## 📊 市场分布

"""
    
    # 按市场统计
    market_stats = df['ts_code'].apply(get_stock_market).value_counts()
    for market, count in market_stats.items():
        markdown_content += f"- **{market}**: {count}只\n"

    # 涨幅分布统计
    pct_10_plus = len(df[df['change_pct'] >= 10])
    pct_9_5_10 = len(df[(df['change_pct'] >= 9.5) & (df['change_pct'] < 10)])
    
    markdown_content += f"""
## 📋 涨幅分布

- **涨停(≥10%)**: {pct_10_plus}只
- **准涨停(9.5%-10%)**: {pct_9_5_10}只

---
*数据来源: Tushare*  
*发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""

    return markdown_content


def main():
    """主函数"""
    logger.info("开始查询涨停股票...")
    
    try:
        # 连接数据库
        with StockDatabase() as db:
            # 获取最近交易日期
            latest_date = db.get_latest_trading_date()
            if not latest_date:
                logger.error("无法获取最近交易日期，请检查数据库中是否有数据")
                return 1
            
            logger.info(f"查询日期: {latest_date}")
            
            # 查询涨停股票 - 使用自动判断涨停条件
            limit_up_df = db.get_limit_up_stocks(trade_date=latest_date)
            
            if limit_up_df is None:
                logger.error("查询涨停股票失败")
                return 1
            
            # 创建markdown消息
            markdown_msg = create_limit_up_markdown(limit_up_df, latest_date)
            
            # 发送消息
            logger.info("准备发送涨停股票消息...")
            send_markdown_message(markdown_msg)
            
            logger.info(f"涨停股票查询完成，共找到 {len(limit_up_df)} 只涨停股票")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
