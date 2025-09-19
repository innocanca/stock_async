#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
底部反转抄底策略
基于选手"广生堂681%收益"操作模式
策略：前期强势股 + 充分调整 + 远离均线 + 缩量企稳 + 底部反转
"""

import logging
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import StockDatabase
from send_msg import send_markdown_message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bottom_reversal_notify.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算技术指标"""
    if df.empty or len(df) < 20:
        return df
    
    df_calc = df.copy().sort_values('trade_date').reset_index(drop=True)
    
    # 均线系统
    df_calc['ma5'] = df_calc['close'].rolling(window=5, min_periods=1).mean()
    df_calc['ma10'] = df_calc['close'].rolling(window=10, min_periods=1).mean()
    df_calc['ma20'] = df_calc['close'].rolling(window=20, min_periods=1).mean()
    
    # 成交量均线
    df_calc['vol_ma5'] = df_calc['vol'].rolling(window=5, min_periods=1).mean()
    df_calc['vol_ma10'] = df_calc['vol'].rolling(window=10, min_periods=1).mean()
    
    # 价格位置
    df_calc['high_5d'] = df_calc['high'].rolling(window=5, min_periods=1).max()
    df_calc['low_5d'] = df_calc['low'].rolling(window=5, min_periods=1).min()
    df_calc['high_10d'] = df_calc['high'].rolling(window=10, min_periods=1).max()
    df_calc['low_10d'] = df_calc['low'].rolling(window=10, min_periods=1).min()
    
    # 涨跌幅
    df_calc['pct_1d'] = df_calc['close'].pct_change() * 100
    df_calc['pct_5d'] = df_calc['close'].pct_change(periods=5) * 100
    df_calc['pct_10d'] = df_calc['close'].pct_change(periods=10) * 100
    
    return df_calc.sort_values('trade_date', ascending=False).reset_index(drop=True)


def check_previous_strength(df: pd.DataFrame, current_idx: int = 0) -> bool:
    """
    检查前期是否为强势股
    条件：10-20天前有明显上涨行情
    """
    if len(df) < 20:
        return False
    
    # 检查10-20天前的涨幅
    if current_idx + 20 < len(df):
        price_20d_ago = df.iloc[current_idx + 20]['close']
        price_10d_ago = df.iloc[current_idx + 10]['close']
        
        # 10天内涨幅超过30%认为是强势股
        rise_pct = (price_10d_ago - price_20d_ago) / price_20d_ago * 100
        return rise_pct > 30
    
    return False


def check_sufficient_pullback(row: pd.Series) -> bool:
    """
    检查是否充分调整
    条件：距MA5 < -5% 且 在5日内位置 < 25%
    """
    close = row['close']
    ma5 = row['ma5']
    low_5d = row['low_5d']
    high_5d = row['high_5d']
    
    if pd.isna(ma5) or ma5 <= 0 or pd.isna(high_5d) or pd.isna(low_5d):
        return False
    
    # 距离MA5超过5%
    ma5_distance = (close - ma5) / ma5 * 100
    
    # 在5日内的相对位置
    if high_5d > low_5d:
        pos_in_5d = (close - low_5d) / (high_5d - low_5d) * 100
    else:
        pos_in_5d = 50
    
    return ma5_distance < -5 and pos_in_5d < 25


def check_volume_shrinkage(row: pd.Series) -> bool:
    """
    检查成交量萎缩
    条件：成交量 < 5日均量
    """
    vol = row['vol']
    vol_ma5 = row['vol_ma5']
    
    if pd.isna(vol) or pd.isna(vol_ma5) or vol_ma5 <= 0:
        return False
    
    vol_ratio = vol / vol_ma5
    return vol_ratio < 1.0  # 缩量


def check_ma_crossover_state(row: pd.Series) -> bool:
    """
    检查均线交织状态
    条件：均线不是明显的多头或空头排列
    """
    ma5 = row['ma5']
    ma10 = row['ma10']
    ma20 = row['ma20']
    
    if any(pd.isna(x) for x in [ma5, ma10, ma20]):
        return False
    
    # 不是明显的多头排列，也不是明显的空头排列
    is_bull = ma5 > ma10 > ma20
    is_bear = ma5 < ma10 < ma20
    
    return not is_bull and not is_bear


def check_bottom_reversal_signal(row: pd.Series, prev_row: pd.Series = None) -> bool:
    """
    检查底部反转信号
    条件：小幅反弹 + 下影线 + 止跌企稳
    """
    close = row['close']
    open_price = row['open']
    low = row['low']
    high = row['high']
    pct_chg = row['change_pct']
    
    # 小幅反弹（0-5%）
    if pct_chg < 0 or pct_chg > 5:
        return False
    
    # 有下影线（探底回升）
    body = abs(close - open_price)
    lower_shadow = min(open_price, close) - low
    total_range = high - low
    
    if total_range > 0:
        lower_shadow_ratio = lower_shadow / total_range
        if lower_shadow_ratio < 0.3:  # 下影线太短
            return False
    
    return True


def find_bottom_reversal_stocks(days_back: int = 20,
                               min_signal_strength: float = 70.0) -> pd.DataFrame:
    """
    查找符合底部反转抄底条件的股票
    
    Args:
        days_back: 查询历史数据天数
        min_signal_strength: 最小信号强度
        
    Returns:
        pd.DataFrame: 符合条件的股票
    """
    logger.info("开始筛选底部反转抄底机会...")
    
    with StockDatabase() as db:
        # 获取最新交易日期
        latest_data = db.query_data(limit=1)
        if latest_data is None or latest_data.empty:
            logger.warning("数据库中没有数据")
            return pd.DataFrame()
        
        latest_trade_date = latest_data.iloc[0]['trade_date']
        end_date = latest_trade_date.strftime('%Y-%m-%d')
        start_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=30)
        start_date = start_dt.strftime('%Y-%m-%d')
        
        # 查询主板股票数据
        logger.info(f"查询日期范围: {start_date} 到 {end_date} (主板股票)")
        
        main_board_query = """
        SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.pre_close,
               d.change_pct, d.change_amount, d.vol, d.amount, d.created_at, d.updated_at,
               s.name as stock_name, s.industry, s.area
        FROM daily_data d
        LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
        WHERE d.trade_date >= %s AND d.trade_date <= %s
        AND d.ts_code NOT LIKE '300%%'
        AND d.ts_code NOT LIKE '688%%'
        AND d.ts_code NOT LIKE '830%%'
        AND d.ts_code NOT LIKE '430%%'
        AND d.ts_code NOT LIKE '200%%'
        AND d.ts_code NOT LIKE '900%%'
        ORDER BY d.ts_code, d.trade_date DESC
        """
        
        all_stocks_df = pd.read_sql(main_board_query, db.connection, params=[start_date, end_date])
        
        if all_stocks_df is None or all_stocks_df.empty:
            logger.warning("没有获取到股票数据")
            return pd.DataFrame()
        
        logger.info(f"查询到 {len(all_stocks_df)} 条主板股票记录")
        
        # 按股票分组处理
        qualified_stocks = []
        total_stocks = all_stocks_df['ts_code'].nunique()
        processed_count = 0
        
        debug_stats = {
            'total_stocks': 0,
            'insufficient_data': 0,
            'low_price_or_st': 0,
            'not_previous_strong': 0,
            'insufficient_pullback': 0,
            'no_volume_shrinkage': 0,
            'no_ma_crossover': 0,
            'no_reversal_signal': 0,
            'low_signal_strength': 0,
            'qualified': 0
        }
        
        logger.info(f"开始处理 {total_stocks} 只主板股票...")
        
        for ts_code, stock_df in all_stocks_df.groupby('ts_code'):
            try:
                debug_stats['total_stocks'] += 1
                processed_count += 1
                
                if processed_count % 1000 == 0:
                    logger.info(f"处理进度: {processed_count}/{total_stocks} ({processed_count/total_stocks*100:.1f}%)")
                
                # 确保有足够的数据
                if len(stock_df) < 20:
                    debug_stats['insufficient_data'] += 1
                    continue
                
                # 计算技术指标
                stock_df_with_indicators = calculate_technical_indicators(stock_df)
                latest_row = stock_df_with_indicators.iloc[0]
                
                # 过滤价格过低或ST股票
                if latest_row['close'] < 3.0 or 'ST' in ts_code:
                    debug_stats['low_price_or_st'] += 1
                    continue
                
                # 检查前期是否为强势股
                if not check_previous_strength(stock_df_with_indicators, 0):
                    debug_stats['not_previous_strong'] += 1
                    continue
                
                # 检查是否充分调整
                if not check_sufficient_pullback(latest_row):
                    debug_stats['insufficient_pullback'] += 1
                    continue
                
                # 检查成交量萎缩
                if not check_volume_shrinkage(latest_row):
                    debug_stats['no_volume_shrinkage'] += 1
                    continue
                
                # 检查均线交织状态
                if not check_ma_crossover_state(latest_row):
                    debug_stats['no_ma_crossover'] += 1
                    continue
                
                # 检查底部反转信号
                prev_row = stock_df_with_indicators.iloc[1] if len(stock_df_with_indicators) > 1 else None
                if not check_bottom_reversal_signal(latest_row, prev_row):
                    debug_stats['no_reversal_signal'] += 1
                    continue
                
                # 计算信号强度
                signal_strength = calculate_bottom_reversal_strength(latest_row, stock_df_with_indicators)
                
                if signal_strength < min_signal_strength:
                    debug_stats['low_signal_strength'] += 1
                    continue
                
                # 满足所有条件
                debug_stats['qualified'] += 1
                
                # 计算相关指标
                ma5_distance = (latest_row['close'] - latest_row['ma5']) / latest_row['ma5'] * 100
                pos_in_5d = ((latest_row['close'] - latest_row['low_5d']) / 
                            (latest_row['high_5d'] - latest_row['low_5d']) * 100 
                            if latest_row['high_5d'] > latest_row['low_5d'] else 50)
                vol_ratio = latest_row['vol'] / latest_row['vol_ma5'] if latest_row['vol_ma5'] > 0 else 1
                
                qualified_stocks.append({
                    'ts_code': ts_code,
                    'stock_name': latest_row.get('stock_name', '未知'),
                    'industry': latest_row.get('industry', '未知'),
                    'area': latest_row.get('area', '未知'),
                    'trade_date': latest_row['trade_date'],
                    'close': latest_row['close'],
                    'pct_1d': latest_row['pct_1d'],
                    'pct_5d': latest_row['pct_5d'],
                    'pct_10d': latest_row['pct_10d'],
                    'ma5': latest_row['ma5'],
                    'ma10': latest_row['ma10'],
                    'ma5_distance': ma5_distance,
                    'pos_in_5d': pos_in_5d,
                    'vol_ratio': vol_ratio,
                    'signal_strength': signal_strength,
                    'amount_yi': latest_row['amount'] / 10000,
                })
                
            except Exception as e:
                logger.debug(f"处理股票 {ts_code} 时出错: {e}")
                continue
        
        # 输出调试统计
        logger.info("📊 底部抄底筛选统计:")
        logger.info(f"   总股票数: {debug_stats['total_stocks']}")
        logger.info(f"   数据不足: {debug_stats['insufficient_data']}")
        logger.info(f"   价格/ST过滤: {debug_stats['low_price_or_st']}")
        logger.info(f"   非前期强势: {debug_stats['not_previous_strong']}")
        logger.info(f"   调整不充分: {debug_stats['insufficient_pullback']}")
        logger.info(f"   无成交量萎缩: {debug_stats['no_volume_shrinkage']}")
        logger.info(f"   非均线交织: {debug_stats['no_ma_crossover']}")
        logger.info(f"   无反转信号: {debug_stats['no_reversal_signal']}")
        logger.info(f"   信号强度不足: {debug_stats['low_signal_strength']}")
        logger.info(f"   ✅ 最终合格: {debug_stats['qualified']}")
        
        result_df = pd.DataFrame(qualified_stocks)
        
        if not result_df.empty:
            result_df = result_df.sort_values('signal_strength', ascending=False)
            logger.info(f"找到 {len(result_df)} 只符合底部反转抄底条件的股票")
        else:
            logger.info("没有找到符合条件的股票")
        
        return result_df


def calculate_bottom_reversal_strength(row: pd.Series, df: pd.DataFrame) -> float:
    """
    计算底部反转信号强度（0-100）
    """
    score = 0.0
    
    # 前期强势评分（0-25分）
    if len(df) >= 20:
        if check_previous_strength(df, 0):
            score += 25
        else:
            score += 10
    
    # 调整充分评分（0-25分）
    ma5_distance = (row['close'] - row['ma5']) / row['ma5'] * 100 if row['ma5'] > 0 else 0
    if ma5_distance < -10:
        score += 25
    elif ma5_distance < -5:
        score += 20
    elif ma5_distance < 0:
        score += 15
    else:
        score += 5
    
    # 价格位置评分（0-25分）
    if row['high_5d'] > row['low_5d']:
        pos_in_5d = (row['close'] - row['low_5d']) / (row['high_5d'] - row['low_5d']) * 100
        if pos_in_5d < 15:
            score += 25
        elif pos_in_5d < 25:
            score += 20
        elif pos_in_5d < 40:
            score += 15
        else:
            score += 5
    
    # 成交量评分（0-25分）
    vol_ratio = row['vol'] / row['vol_ma5'] if row['vol_ma5'] > 0 else 1
    if vol_ratio < 0.8:
        score += 25
    elif vol_ratio < 1.0:
        score += 20
    elif vol_ratio < 1.2:
        score += 15
    else:
        score += 10
    
    return min(100.0, score)


def create_bottom_reversal_markdown(df: pd.DataFrame, query_date: str) -> str:
    """创建底部反转抄底的markdown消息"""
    if df.empty:
        return f"""## 📉 底部反转抄底提醒 ({query_date})

❌ **暂无符合条件的股票**

**策略条件：**
- 🎯 前期强势股：10-20天前有30%+涨幅
- 📉 充分调整：距MA5 < -5%，5日内位置 < 25%
- 🔊 成交量萎缩：成交量 < 5日均量
- 📊 均线交织：非明显多头或空头排列
- 🎯 反转信号：小幅反弹 + 下影线

等待市场出现调整机会。
"""
    
    total_count = len(df)
    avg_signal_strength = df['signal_strength'].mean()
    avg_ma5_distance = df['ma5_distance'].mean()
    avg_pos_5d = df['pos_in_5d'].mean()
    
    # 行业分布
    industry_stats = df['industry'].value_counts().head(5)
    hot_sectors = [f"{industry}({count}只)" for industry, count in industry_stats.items() if industry != '未知']
    
    markdown = f"""## 📉 底部反转抄底提醒 ({query_date})

🎯 **筛选结果：找到 {total_count} 只符合条件的抄底机会**
- 📊 平均信号强度：{avg_signal_strength:.1f}分
- 📉 平均距MA5：{avg_ma5_distance:.1f}%
- 📍 平均5日位置：{avg_pos_5d:.1f}%
- 🏢 涉及行业：{' | '.join(hot_sectors[:3])}

---

### 🏆 重点关注股票（按信号强度排序）

"""
    
    # 显示前10只股票
    for i, (_, row) in enumerate(df.head(10).iterrows(), 1):
        code = row['ts_code'].split('.')[0]
        
        markdown += f"""
**{i}. {row['stock_name']} ({code})**
- 🏢 行业板块：{row['industry']} | {row['area']}
- 💰 当前价格：{row['close']:.2f}元 ({row['pct_1d']:+.1f}%)
- 📉 调整幅度：距MA5 {row['ma5_distance']:+.1f}%，5日内位置{row['pos_in_5d']:.1f}%
- 📈 短期表现：5日{row['pct_5d']:+.1f}% | 10日{row['pct_10d']:+.1f}%
- 🔊 成交量：{row['vol_ratio']:.1f}倍（萎缩状态）
- 🎯 信号强度：{row['signal_strength']:.0f}分
- 💸 成交额：{row['amount_yi']:.1f}亿元
- 📊 均线位置：MA5({row['ma5']:.2f}) MA10({row['ma10']:.2f})
"""
    
    if total_count > 10:
        markdown += f"\\n... 还有 {total_count - 10} 只股票符合条件"
    
    markdown += f"""

---

### 📋 策略说明
**底部反转抄底策略（基于选手广生堂681%收益模式）：**
1. 🎯 **前期强势股**：历史上有过强势表现的股票
2. 📉 **充分调整**：距MA5 < -5%，深度调整到位
3. 🔊 **成交量萎缩**：抛压减轻，成交量萎缩
4. 📊 **均线交织**：多空转换的临界状态
5. 🎯 **反转信号**：出现底部企稳的技术信号

**投资逻辑：**
- 在强势股充分调整后的底部区域抄底
- 利用技术分析捕捉反转机会
- 选手实战验证：广生堂获得681%收益

**风险提示：**
- 底部难以精确判断，需要严格止损
- 市场环境变化可能影响反转效果
- 建议分批建仓，控制仓位

*策略来源：基于实战高手操作模式总结*
"""
    
    return markdown


def run_bottom_reversal_strategy(notify: bool = True, min_signal_strength: float = 70.0) -> pd.DataFrame:
    """运行底部反转抄底策略"""
    try:
        logger.info("🚀 开始执行底部反转抄底策略...")
        
        result_df = find_bottom_reversal_stocks(min_signal_strength=min_signal_strength)
        
        if not result_df.empty:
            latest_date = result_df.iloc[0]['trade_date']
            query_date = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)
        else:
            query_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"找到 {len(result_df)} 只符合底部反转抄底条件的股票")
        
        # 发送通知
        if notify:
            markdown_content = create_bottom_reversal_markdown(result_df, query_date)
            try:
                send_result = send_markdown_message(markdown_content)
                if send_result:
                    logger.info("✅ 底部反转抄底提醒已发送")
                else:
                    logger.error("❌ 发送底部反转抄底提醒失败")
            except Exception as e:
                logger.error(f"发送消息时出错: {e}")
        
        # 打印结果
        if not result_df.empty:
            print(f"\\n📉 底部反转抄底机会 ({query_date}):")
            print("=" * 100)
            print("排名  股票名称     代码      行业板块       价格    距MA5   5日位置  成交量  信号强度")
            print("-" * 100)
            
            for i, (_, row) in enumerate(result_df.head(10).iterrows(), 1):
                code = row['ts_code'].split('.')[0]
                name = row.get('stock_name', '未知')[:6]
                industry = row.get('industry', '未知')[:8]
                print(f"{i:>2}   {name:<8} {code:<8} {industry:<10} "
                      f"{row['close']:>6.2f} {row['ma5_distance']:>6.1f}% {row['pos_in_5d']:>6.1f}% "
                      f"{row['vol_ratio']:>5.1f}x {row['signal_strength']:>6.0f}分")
            
            if len(result_df) > 10:
                print(f"... 还有 {len(result_df) - 10} 只股票")
        
        return result_df
        
    except Exception as e:
        logger.error(f"执行底部反转抄底策略时出错: {e}")
        return pd.DataFrame()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='底部反转抄底策略（基于选手广生堂模式）')
    parser.add_argument('--min-signal-strength', type=float, default=70.0,
                       help='最小信号强度（默认70.0）')
    parser.add_argument('--no-notify', action='store_true',
                       help='不发送通知，仅显示结果')
    
    args = parser.parse_args()
    
    result_df = run_bottom_reversal_strategy(
        notify=not args.no_notify,
        min_signal_strength=args.min_signal_strength
    )
    
    if not result_df.empty:
        logger.info("✅ 底部反转抄底策略执行完成")
    else:
        logger.info("📊 今日无符合条件的底部反转机会")


if __name__ == "__main__":
    main()
