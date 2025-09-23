#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
底部放量小阳线策略推送脚本
策略：前期回调底部 + 连续小阳 + 量能逐渐放大 + 趋势向上站稳5日和10日线 + 活跃票

核心逻辑：
1. 前期回调底部：近20天内有明显回调（从高点回调>=15%），当前价格接近底部区域
2. 连续小阳：最近3-5天连续收阳线，单日涨幅在0.5%-6%之间（小阳线特征）
3. 量能逐渐放大：最近几天成交量呈递增趋势，今日成交量 > 前日成交量
4. 趋势向上：股价站稳5日线和10日线，5日线>10日线，价格在5日线上方
5. 活跃票：日成交金额 >= 5000万元，确保有足够的流动性
"""

import logging
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 添加父目录到Python路径，以便导入database和fetcher模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import StockDatabase
from send_msg import send_markdown_message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bottom_volume_bullish_notify.log', encoding='utf-8'),
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


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算技术指标"""
    if df.empty or len(df) < 15:
        return df
    
    df_calc = df.copy().sort_values('trade_date').reset_index(drop=True)
    
    # 均线系统
    df_calc['ma5'] = df_calc['close'].rolling(window=5, min_periods=1).mean()
    df_calc['ma10'] = df_calc['close'].rolling(window=10, min_periods=1).mean()
    df_calc['ma20'] = df_calc['close'].rolling(window=20, min_periods=1).mean()
    
    # 成交量均线
    df_calc['vol_ma5'] = df_calc['vol'].rolling(window=5, min_periods=1).mean()
    df_calc['vol_ma10'] = df_calc['vol'].rolling(window=10, min_periods=1).mean()
    
    # 价格相关
    df_calc['high_20d'] = df_calc['high'].rolling(window=20, min_periods=1).max()
    df_calc['low_20d'] = df_calc['low'].rolling(window=20, min_periods=1).min()
    df_calc['high_5d'] = df_calc['high'].rolling(window=5, min_periods=1).max()
    df_calc['low_5d'] = df_calc['low'].rolling(window=5, min_periods=1).min()
    
    # 计算日线特征（小阳线判断）
    df_calc['is_bullish'] = df_calc['close'] > df_calc['open']  # 阳线
    df_calc['body_pct'] = ((df_calc['close'] - df_calc['open']) / df_calc['open'] * 100).abs()  # 实体涨跌幅
    
    # 回调幅度计算（从20天高点的回调）
    df_calc['pullback_from_high'] = (df_calc['high_20d'] - df_calc['close']) / df_calc['high_20d'] * 100
    
    # 相对于底部的位置（0-100，0是最低点，100是最高点）
    df_calc['position_in_range'] = ((df_calc['close'] - df_calc['low_20d']) / 
                                   (df_calc['high_20d'] - df_calc['low_20d']) * 100)
    
    return df_calc


def check_pullback_bottom_condition(df: pd.DataFrame) -> dict:
    """
    检查前期回调底部条件
    返回：{is_qualified: bool, max_pullback: float, current_position: float, description: str}
    """
    if len(df) < 20:
        return {"is_qualified": False, "reason": "数据不足"}
    
    latest = df.iloc[-1]
    
    # 获取20天内的最大回调
    max_pullback = latest['pullback_from_high']
    current_position = latest['position_in_range']
    
    # 条件：
    # 1. 有明显回调（>=15%）
    # 2. 当前位置在底部区域（0-40%区间，表示接近底部）
    is_significant_pullback = max_pullback >= 15.0
    is_near_bottom = current_position <= 40.0
    
    is_qualified = is_significant_pullback and is_near_bottom
    
    description = f"最大回调{max_pullback:.1f}%，当前位置{current_position:.1f}%"
    
    return {
        "is_qualified": is_qualified,
        "max_pullback": max_pullback,
        "current_position": current_position,
        "description": description,
        "reason": f"回调{max_pullback:.1f}%，位置{current_position:.1f}%" if is_qualified else 
                 f"回调不足({max_pullback:.1f}%<15%)" if not is_significant_pullback else f"位置过高({current_position:.1f}%>40%)"
    }


def check_consecutive_bullish_condition(df: pd.DataFrame, min_days: int = 3) -> dict:
    """
    检查连续小阳线条件
    小阳线定义：收盘 > 开盘，涨幅在0.5%-6%之间
    """
    if len(df) < min_days:
        return {"is_qualified": False, "reason": "数据不足"}
    
    recent_data = df.tail(min_days)
    
    # 检查每一天是否符合小阳线条件
    bullish_days = []
    for _, row in recent_data.iterrows():
        is_bullish = row['is_bullish']  # 阳线
        pct_chg = row['change_pct']
        
        # 小阳线：阳线且涨幅在0.5%-6%之间
        is_small_bullish = is_bullish and 0.5 <= pct_chg <= 6.0
        
        bullish_days.append({
            'date': row['trade_date'],
            'is_bullish': is_bullish,
            'pct_chg': pct_chg,
            'is_small_bullish': is_small_bullish
        })
    
    # 计算连续小阳天数
    consecutive_count = 0
    for day in reversed(bullish_days):  # 从最近的一天往前数
        if day['is_small_bullish']:
            consecutive_count += 1
        else:
            break
    
    is_qualified = consecutive_count >= min_days
    
    avg_pct = np.mean([day['pct_chg'] for day in bullish_days if day['is_small_bullish']])
    
    return {
        "is_qualified": is_qualified,
        "consecutive_days": consecutive_count,
        "avg_pct": avg_pct,
        "description": f"连续{consecutive_count}天小阳线，平均涨幅{avg_pct:.2f}%",
        "reason": f"连续{consecutive_count}天小阳线" if is_qualified else f"连续小阳线不足({consecutive_count}<{min_days})"
    }


def check_volume_increasing_condition(df: pd.DataFrame) -> dict:
    """
    检查量能逐渐放大条件
    """
    if len(df) < 5:
        return {"is_qualified": False, "reason": "数据不足"}
    
    recent_data = df.tail(5)
    volumes = recent_data['vol'].tolist()
    
    # 检查最近3天成交量是否呈递增趋势
    recent_3_vols = volumes[-3:]
    is_increasing = all(recent_3_vols[i] < recent_3_vols[i+1] for i in range(len(recent_3_vols)-1))
    
    # 今日成交量相对于5日均量的倍数
    current_vol = volumes[-1]
    vol_ma5 = recent_data['vol_ma5'].iloc[-1]
    vol_ratio = current_vol / vol_ma5 if vol_ma5 > 0 else 0
    
    # 量能放大条件：
    # 1. 最近3天量能递增 或者
    # 2. 今日成交量 >= 5日均量的1.3倍
    is_qualified = is_increasing or vol_ratio >= 1.3
    
    return {
        "is_qualified": is_qualified,
        "is_increasing": is_increasing,
        "vol_ratio": vol_ratio,
        "description": f"量能{'递增' if is_increasing else ''}，今日/5日均量={vol_ratio:.2f}倍",
        "reason": f"量能放大(递增:{is_increasing}, {vol_ratio:.2f}倍)" if is_qualified else f"量能不足({vol_ratio:.2f}倍<1.3)"
    }


def check_trend_upward_condition(df: pd.DataFrame) -> dict:
    """
    检查趋势向上，站稳5日和10日线条件
    """
    if len(df) < 10:
        return {"is_qualified": False, "reason": "数据不足"}
    
    latest = df.iloc[-1]
    close_price = latest['close']
    ma5 = latest['ma5']
    ma10 = latest['ma10']
    
    # 条件：
    # 1. 股价站稳5日线：收盘价 >= 5日线 * 0.98（允许2%以内的偏差）
    # 2. 股价站稳10日线：收盘价 >= 10日线 * 0.95（允许5%以内的偏差）
    # 3. 5日线 > 10日线（多头排列）
    
    above_ma5 = close_price >= ma5 * 0.98
    above_ma10 = close_price >= ma10 * 0.95
    ma5_above_ma10 = ma5 > ma10
    
    ma5_distance = (close_price - ma5) / ma5 * 100 if ma5 > 0 else 0
    ma10_distance = (close_price - ma10) / ma10 * 100 if ma10 > 0 else 0
    
    is_qualified = above_ma5 and above_ma10 and ma5_above_ma10
    
    return {
        "is_qualified": is_qualified,
        "above_ma5": above_ma5,
        "above_ma10": above_ma10,
        "ma5_above_ma10": ma5_above_ma10,
        "ma5_distance": ma5_distance,
        "ma10_distance": ma10_distance,
        "description": f"距5日线{ma5_distance:.1f}%，距10日线{ma10_distance:.1f}%，5>10日线:{ma5_above_ma10}",
        "reason": f"站稳均线(5日线+{ma5_distance:.1f}%, 10日线+{ma10_distance:.1f}%)" if is_qualified else 
                 f"未站稳均线(5日线{ma5_distance:.1f}%, 10日线{ma10_distance:.1f}%, 5>10:{ma5_above_ma10})"
    }


def check_active_stock_condition(df: pd.DataFrame, min_amount: float = 50000000) -> dict:
    """
    检查活跃股票条件（成交金额 >= 5000万元）
    """
    if df.empty:
        return {"is_qualified": False, "reason": "无数据"}
    
    latest = df.iloc[-1]
    amount = latest['amount'] * 1000 if 'amount' in latest else 0  # amount单位是千元，转为元
    
    is_qualified = amount >= min_amount
    
    # 格式化显示金额
    if amount >= 1e8:
        amount_str = f"{amount/1e8:.2f}亿元"
    elif amount >= 1e4:
        amount_str = f"{amount/1e4:.0f}万元"
    else:
        amount_str = f"{amount:.0f}元"
    
    return {
        "is_qualified": is_qualified,
        "amount": amount,
        "amount_str": amount_str,
        "description": f"成交金额{amount_str}",
        "reason": f"活跃({amount_str})" if is_qualified else f"不够活跃({amount_str}<5000万)"
    }


def calculate_strategy_score(pullback_info: dict, bullish_info: dict, volume_info: dict, 
                           trend_info: dict, active_info: dict) -> float:
    """
    计算策略综合评分（0-100分）
    """
    score = 0.0
    
    # 回调底部评分（0-25分）
    if pullback_info.get('is_qualified', False):
        pullback_score = min(25, pullback_info.get('max_pullback', 0) * 0.8)  # 回调越深分数越高
        if pullback_info.get('current_position', 100) <= 20:  # 在最底部区域加分
            pullback_score += 5
        score += pullback_score
    
    # 连续小阳评分（0-25分）
    if bullish_info.get('is_qualified', False):
        consecutive_days = bullish_info.get('consecutive_days', 0)
        score += min(25, consecutive_days * 6)  # 连续天数越多分数越高
    
    # 量能放大评分（0-20分）
    if volume_info.get('is_qualified', False):
        vol_ratio = volume_info.get('vol_ratio', 0)
        is_increasing = volume_info.get('is_increasing', False)
        vol_score = min(15, vol_ratio * 8) + (5 if is_increasing else 0)
        score += vol_score
    
    # 趋势向上评分（0-20分）
    if trend_info.get('is_qualified', False):
        score += 20
        # 距离均线越近加分
        ma5_distance = trend_info.get('ma5_distance', -10)
        if 0 <= ma5_distance <= 3:
            score += 5
    
    # 活跃度评分（0-10分）
    if active_info.get('is_qualified', False):
        amount = active_info.get('amount', 0)
        active_score = min(10, (amount / 1e8) * 3)  # 成交额越大分数越高
        score += active_score
    
    return min(100.0, score)


def find_bottom_volume_bullish_stocks() -> pd.DataFrame:
    """查找符合底部放量小阳线策略的股票"""
    logger.info("🚀 开始筛选底部放量小阳线机会...")
    
    with StockDatabase() as db:
        # 获取最新交易日
        latest_data = db.query_data(limit=1)
        if latest_data is None or latest_data.empty:
            logger.error("无法获取最新交易数据")
            return pd.DataFrame()
        
        latest_trade_date = latest_data.iloc[0]['trade_date']
        end_date = latest_trade_date.strftime('%Y-%m-%d')
        start_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=30)
        start_date = start_dt.strftime('%Y-%m-%d')
        
        logger.info(f"查询日期范围: {start_date} 到 {end_date}")
        
        # 查询主板股票数据（排除创业板、科创板、北交所）
        main_board_query = """
        SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.pre_close,
               d.change_pct, d.change_amount, d.vol, d.amount,
               s.name as stock_name, s.industry, s.area
        FROM daily_data d
        LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
        WHERE d.trade_date >= %s AND d.trade_date <= %s
        AND d.ts_code NOT LIKE '300%%'
        AND d.ts_code NOT LIKE '688%%' 
        AND d.ts_code NOT LIKE '830%%'
        AND d.ts_code NOT LIKE '430%%'
        AND d.amount >= 50000
        ORDER BY d.ts_code, d.trade_date ASC
        """
        
        all_stocks_df = pd.read_sql(main_board_query, db.connection, params=[start_date, end_date])
        
        if all_stocks_df is None or all_stocks_df.empty:
            logger.error("未查询到股票数据")
            return pd.DataFrame()
        
        logger.info(f"查询到 {len(all_stocks_df)} 条主板股票记录")
        
        # 按股票分组并处理
        qualified_stocks = []
        processed_count = 0
        total_stocks = all_stocks_df['ts_code'].nunique()
        
        for ts_code, stock_df in all_stocks_df.groupby('ts_code'):
            processed_count += 1
            
            if processed_count % 500 == 0:
                logger.info(f"已处理 {processed_count}/{total_stocks} 只股票...")
            
            # 计算技术指标
            stock_df = calculate_technical_indicators(stock_df)
            
            if len(stock_df) < 20:  # 需要足够的历史数据
                continue
            
            # 检查各个策略条件
            pullback_result = check_pullback_bottom_condition(stock_df)
            bullish_result = check_consecutive_bullish_condition(stock_df, min_days=3)
            volume_result = check_volume_increasing_condition(stock_df)
            trend_result = check_trend_upward_condition(stock_df)
            active_result = check_active_stock_condition(stock_df)
            
            # 所有条件都需要满足
            if not all([
                pullback_result.get('is_qualified', False),
                bullish_result.get('is_qualified', False),
                volume_result.get('is_qualified', False),
                trend_result.get('is_qualified', False),
                active_result.get('is_qualified', False)
            ]):
                continue
            
            # 计算综合评分
            strategy_score = calculate_strategy_score(
                pullback_result, bullish_result, volume_result, trend_result, active_result
            )
            
            if strategy_score < 60:  # 评分过低过滤
                continue
            
            # 获取最新数据
            latest_row = stock_df.iloc[-1]
            
            qualified_stocks.append({
                'ts_code': ts_code,
                'stock_name': latest_row.get('stock_name', 'N/A'),
                'industry': latest_row.get('industry', 'N/A'),
                'area': latest_row.get('area', 'N/A'),
                'trade_date': latest_row['trade_date'],
                'close': latest_row['close'],
                'change_pct': latest_row['change_pct'],
                'amount': latest_row['amount'] * 1000,  # 转为元
                'vol_ratio': volume_result.get('vol_ratio', 0),
                'strategy_score': strategy_score,
                'pullback_info': pullback_result,
                'bullish_info': bullish_result,
                'volume_info': volume_result,
                'trend_info': trend_result,
                'active_info': active_result,
                'ma5': latest_row['ma5'],
                'ma10': latest_row['ma10'],
            })
        
        logger.info(f"处理完成，找到 {len(qualified_stocks)} 只符合条件的股票")
        
        if not qualified_stocks:
            return pd.DataFrame()
        
        # 转为DataFrame并按评分排序
        result_df = pd.DataFrame(qualified_stocks)
        result_df = result_df.sort_values('strategy_score', ascending=False).reset_index(drop=True)
        
        return result_df


def create_bottom_volume_bullish_markdown(df: pd.DataFrame, query_date: str) -> str:
    """创建底部放量小阳线策略的markdown格式消息（表格形式）"""
    if df.empty:
        return f"""## 📈 底部放量小阳线策略 ({query_date})

❌ **今日无符合条件的股票**

**策略说明：**
- 前期回调底部：近期有15%+回调，当前处于底部区域  
- 连续小阳线：最近3天连续收阳，涨幅0.5%-6%
- 量能放大：成交量递增或达到5日均量1.3倍
- 趋势向上：站稳5日线和10日线，5日线>10日线
- 活跃股票：日成交额≥5000万元

---
*策略提醒：仅供参考，投资需谨慎* 📊"""
    
    content = f"""## 📈 底部放量小阳线策略 ({query_date})

✅ **找到 {len(df)} 只符合条件的优质标的**

### 🎯 策略核心
🔹 前期回调底部 + 连续小阳 + 量能放大 + 趋势向上 + 活跃票

### 📊 推荐股票列表

| 排名 | 股票名称 | 代码 | 价格 | 涨跌幅 | 评分 | 成交额 | 行业 | 市场 |
|------|----------|------|------|--------|------|--------|------|------|"""
    
    # 添加股票表格数据
    for i, (_, row) in enumerate(df.head(15).iterrows(), 1):
        ts_code = row['ts_code']
        stock_name = row['stock_name']
        industry = row['industry'][:8] + "..." if len(row['industry']) > 8 else row['industry']  # 限制长度
        close = row['close']
        change_pct = row['change_pct']
        strategy_score = row['strategy_score']
        
        # 格式化成交金额
        amount = row['amount']
        if amount >= 1e8:
            amount_str = f"{amount/1e8:.1f}亿"
        elif amount >= 1e4:
            amount_str = f"{amount/1e4:.0f}万"
        else:
            amount_str = f"{amount:.0f}"
        
        market = get_stock_market(ts_code)
        stock_code = format_stock_code(ts_code)
        
        # 涨跌幅颜色标识
        pct_color = "🟢" if change_pct > 0 else "🔴" if change_pct < 0 else "⚪"
        
        content += f"""
| {i} | {stock_name} | `{stock_code}` | {close:.2f} | {pct_color}{change_pct:+.2f}% | {strategy_score:.0f}分 | {amount_str} | {industry} | {market} |"""
    
    if len(df) > 15:
        content += f"\n\n*还有 {len(df) - 15} 只股票符合条件，仅显示前15只...*\n"
    
    # 添加详细分析表格
    content += f"""

### 🔍 详细策略分析

| 股票 | 回调情况 | 小阳天数 | 量能状态 | 均线位置 |
|------|----------|----------|----------|----------|"""
    
    for i, (_, row) in enumerate(df.head(8).iterrows(), 1):
        stock_name = row['stock_name'][:6] + "..." if len(row['stock_name']) > 6 else row['stock_name']
        
        pullback_info = row['pullback_info']
        bullish_info = row['bullish_info']
        volume_info = row['volume_info']
        trend_info = row['trend_info']
        
        # 简化信息
        pullback_desc = f"{pullback_info.get('max_pullback', 0):.1f}%回调"
        bullish_desc = f"{bullish_info.get('consecutive_days', 0)}天小阳"
        volume_desc = f"{volume_info.get('vol_ratio', 0):.1f}倍" + ("📈" if volume_info.get('is_increasing', False) else "")
        trend_desc = f"5日+{trend_info.get('ma5_distance', 0):.1f}%"
        
        content += f"""
| {stock_name} | {pullback_desc} | {bullish_desc} | {volume_desc} | {trend_desc} |"""
    
    content += f"""

### 📋 策略筛选条件

| 条件 | 标准 | 说明 |
|------|------|------|
| 🎯 回调底部 | 回调≥15%，位置≤40% | 前期有明显回调，当前接近底部 |
| 🕯️ 连续小阳 | 3天小阳线，涨幅0.5%-6% | 温和上涨，不急不躁 |
| 📊 量能放大 | 递增或≥5日均量1.3倍 | 资金关注度提升 |
| 📈 趋势向上 | 站稳5日线和10日线 | 技术面转强，多头排列 |
| 💰 活跃度 | 日成交额≥5000万元 | 确保充足流动性 |

### ⚠️ 操作建议

| 项目 | 建议 |
|------|------|
| 📈 **入场时机** | 突破前高或放量突破均线压力 |
| 🎯 **目标收益** | 10%-20%，根据个股强度调整 |
| 🛑 **止损位置** | 跌破10日线或亏损5%-8% |
| ⏰ **持仓周期** | 短中期持有，1-4周 |
| 🔄 **仓位管理** | 单票不超过总仓位20% |

---
**📊 数据统计:**
- 筛选股票总数：{len(df)} 只
- 平均策略评分：{df['strategy_score'].mean():.1f} 分
- 平均成交额：{(df['amount'].mean()/1e8):.1f} 亿元

**⚠️ 风险提示:**
该策略适合有一定经验的投资者，建议结合大盘环境和个股基本面进行综合判断。投资有风险，入市需谨慎！

---
*底部放量小阳线策略 | 数据时间: {query_date} | 仅供参考* 📈
"""
    
    return content


def main():
    """主函数"""
    try:
        logger.info("=== 底部放量小阳线策略推送开始 ===")
        
        # 查找符合条件的股票
        qualified_df = find_bottom_volume_bullish_stocks()
        
        if qualified_df.empty:
            logger.info("未找到符合条件的股票")
            
        # 获取查询日期
        query_date = datetime.now().strftime('%Y-%m-%d')
        
        # 生成推送消息
        message = create_bottom_volume_bullish_markdown(qualified_df, query_date)
        
        # 发送推送
        send_result = send_markdown_message(message)
        
        if send_result:
            logger.info("✅ 底部放量小阳线策略推送发送成功")
            if not qualified_df.empty:
                logger.info(f"推送了 {len(qualified_df)} 只符合条件的股票")
        else:
            logger.error("❌ 推送发送失败")
            
    except Exception as e:
        logger.error(f"底部放量小阳线策略推送失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    logger.info("=== 底部放量小阳线策略推送结束 ===")
    return 0


if __name__ == "__main__":
    exit(main())
