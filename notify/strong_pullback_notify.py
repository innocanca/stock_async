#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
强势股回调低吸策略
基于选手"光库科技246%收益"操作模式
策略：前期大涨 + 技术回调 + 均线支撑 + 缩量调整 + 企稳反弹
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
        logging.FileHandler('strong_pullback_notify.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


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
    
    # 价格相关
    df_calc['high_5d'] = df_calc['high'].rolling(window=5, min_periods=1).max()
    df_calc['low_5d'] = df_calc['low'].rolling(window=5, min_periods=1).min()
    df_calc['high_10d'] = df_calc['high'].rolling(window=10, min_periods=1).max()
    
    # 涨跌幅
    df_calc['pct_1d'] = df_calc['close'].pct_change() * 100
    df_calc['pct_5d'] = df_calc['close'].pct_change(periods=5) * 100
    df_calc['pct_10d'] = df_calc['close'].pct_change(periods=10) * 100
    
    return df_calc.sort_values('trade_date', ascending=False).reset_index(drop=True)


def check_previous_surge(df: pd.DataFrame, current_idx: int = 0, min_surge: float = 20.0) -> dict:
    """
    检查前期是否有大涨行情
    条件：前5-15天内有超过20%的涨幅
    """
    if len(df) < 15:
        return {'is_strong': False, 'max_surge': 0, 'surge_period': 0}
    
    current_price = df.iloc[current_idx]['close']
    
    # 检查前5-15天的最大涨幅
    max_surge = 0
    surge_period = 0
    
    for i in range(5, min(15, len(df))):
        if current_idx + i < len(df):
            past_price = df.iloc[current_idx + i]['close']
            surge_pct = (current_price - past_price) / past_price * 100
            
            if surge_pct > max_surge:
                max_surge = surge_pct
                surge_period = i
    
    return {
        'is_strong': max_surge > min_surge,
        'max_surge': max_surge,
        'surge_period': surge_period
    }


def check_technical_pullback(row: pd.Series) -> dict:
    """
    检查技术回调
    条件：距MA5在0-5%范围，在5日内40-70%位置
    """
    close = row['close']
    ma5 = row['ma5']
    low_5d = row['low_5d']
    high_5d = row['high_5d']
    
    if any(pd.isna(x) for x in [close, ma5, low_5d, high_5d]) or ma5 <= 0:
        return {'is_pullback': False, 'ma5_distance': 0, 'pos_in_5d': 0}
    
    # 距离MA5的百分比
    ma5_distance = (close - ma5) / ma5 * 100
    
    # 在5日内的位置
    pos_in_5d = (close - low_5d) / (high_5d - low_5d) * 100 if high_5d > low_5d else 50
    
    # 判断是否为技术回调
    is_pullback = (0 <= ma5_distance <= 8) and (40 <= pos_in_5d <= 70)
    
    return {
        'is_pullback': is_pullback,
        'ma5_distance': ma5_distance,
        'pos_in_5d': pos_in_5d
    }


def check_volume_pattern(row: pd.Series) -> dict:
    """
    检查成交量模式
    条件：缩量调整或温和放量
    """
    vol = row['vol']
    vol_ma5 = row['vol_ma5']
    pct_chg = row['change_pct']
    
    if pd.isna(vol) or pd.isna(vol_ma5) or vol_ma5 <= 0:
        return {'is_valid': False, 'vol_ratio': 0, 'pattern': '数据不足'}
    
    vol_ratio = vol / vol_ma5
    
    # 缩量调整（下跌时缩量）
    if pct_chg < 0 and vol_ratio < 1.0:
        return {'is_valid': True, 'vol_ratio': vol_ratio, 'pattern': '缩量调整'}
    
    # 温和放量（上涨时适度放量）
    elif pct_chg > 0 and 1.0 <= vol_ratio <= 2.0:
        return {'is_valid': True, 'vol_ratio': vol_ratio, 'pattern': '温和放量'}
    
    else:
        return {'is_valid': False, 'vol_ratio': vol_ratio, 'pattern': '量价不配'}


def check_trend_intact(row: pd.Series) -> bool:
    """
    检查上升趋势是否完好
    条件：MA5 > MA10 或 接近
    """
    ma5 = row['ma5']
    ma10 = row['ma10']
    
    if pd.isna(ma5) or pd.isna(ma10):
        return False
    
    # MA5 > MA10 或者 MA5略低于MA10（小于3%）
    return ma5 >= ma10 * 0.97


def calculate_strong_pullback_strength(row: pd.Series, surge_info: dict, pullback_info: dict, vol_info: dict) -> float:
    """
    计算强势回调信号强度（0-100）
    """
    score = 0.0
    
    # 前期强势评分（0-30分）
    max_surge = surge_info.get('max_surge', 0)
    if max_surge > 50:
        score += 30
    elif max_surge > 30:
        score += 25
    elif max_surge > 20:
        score += 20
    else:
        score += 10
    
    # 回调位置评分（0-25分）
    ma5_distance = pullback_info.get('ma5_distance', 0)
    pos_in_5d = pullback_info.get('pos_in_5d', 0)
    
    if 0 <= ma5_distance <= 3 and 45 <= pos_in_5d <= 60:
        score += 25  # 完美回调位置
    elif 0 <= ma5_distance <= 5 and 40 <= pos_in_5d <= 70:
        score += 20  # 良好回调位置
    elif 0 <= ma5_distance <= 8:
        score += 15  # 一般回调位置
    else:
        score += 5
    
    # 成交量评分（0-20分）
    vol_pattern = vol_info.get('pattern', '')
    if vol_pattern == '缩量调整':
        score += 20
    elif vol_pattern == '温和放量':
        score += 15
    else:
        score += 5
    
    # 趋势完好评分（0-25分）
    if check_trend_intact(row):
        score += 25
    else:
        score += 10
    
    return min(100.0, score)


def find_strong_pullback_stocks(days_back: int = 20, min_signal_strength: float = 70.0) -> pd.DataFrame:
    """查找符合强势回调低吸条件的股票"""
    logger.info("开始筛选强势股回调低吸机会...")
    
    with StockDatabase() as db:
        # 获取数据
        latest_data = db.query_data(limit=1)
        if latest_data is None or latest_data.empty:
            return pd.DataFrame()
        
        latest_trade_date = latest_data.iloc[0]['trade_date']
        end_date = latest_trade_date.strftime('%Y-%m-%d')
        start_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=25)
        start_date = start_dt.strftime('%Y-%m-%d')
        
        # 查询主板股票数据
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
        ORDER BY d.ts_code, d.trade_date DESC
        """
        
        all_stocks_df = pd.read_sql(main_board_query, db.connection, params=[start_date, end_date])
        
        if all_stocks_df is None or all_stocks_df.empty:
            return pd.DataFrame()
        
        logger.info(f"查询到 {len(all_stocks_df)} 条主板股票记录")
        
        # 筛选处理
        qualified_stocks = []
        processed_count = 0
        debug_stats = {
            'total_stocks': 0, 'insufficient_data': 0, 'low_price_or_st': 0,
            'not_previous_surge': 0, 'not_technical_pullback': 0, 'invalid_volume': 0,
            'trend_broken': 0, 'low_signal_strength': 0, 'qualified': 0
        }
        
        for ts_code, stock_df in all_stocks_df.groupby('ts_code'):
            try:
                debug_stats['total_stocks'] += 1
                processed_count += 1
                
                if processed_count % 1000 == 0:
                    logger.info(f"处理进度: {processed_count}/{debug_stats['total_stocks']}")
                
                if len(stock_df) < 15:
                    debug_stats['insufficient_data'] += 1
                    continue
                
                stock_df_with_indicators = calculate_technical_indicators(stock_df)
                latest_row = stock_df_with_indicators.iloc[0]
                
                # 基础过滤
                if latest_row['close'] < 3.0 or 'ST' in ts_code:
                    debug_stats['low_price_or_st'] += 1
                    continue
                
                # 检查前期是否大涨
                surge_info = check_previous_surge(stock_df_with_indicators, 0, 20.0)
                if not surge_info['is_strong']:
                    debug_stats['not_previous_surge'] += 1
                    continue
                
                # 检查技术回调
                pullback_info = check_technical_pullback(latest_row)
                if not pullback_info['is_pullback']:
                    debug_stats['not_technical_pullback'] += 1
                    continue
                
                # 检查成交量模式
                vol_info = check_volume_pattern(latest_row)
                if not vol_info['is_valid']:
                    debug_stats['invalid_volume'] += 1
                    continue
                
                # 检查趋势完好
                if not check_trend_intact(latest_row):
                    debug_stats['trend_broken'] += 1
                    continue
                
                # 计算信号强度
                signal_strength = calculate_strong_pullback_strength(latest_row, surge_info, pullback_info, vol_info)
                
                if signal_strength < min_signal_strength:
                    debug_stats['low_signal_strength'] += 1
                    continue
                
                debug_stats['qualified'] += 1
                
                qualified_stocks.append({
                    'ts_code': ts_code,
                    'stock_name': latest_row.get('stock_name', '未知'),
                    'industry': latest_row.get('industry', '未知'),
                    'area': latest_row.get('area', '未知'),
                    'trade_date': latest_row['trade_date'],
                    'close': latest_row['close'],
                    'pct_1d': latest_row['pct_1d'],
                    'pct_5d': latest_row['pct_5d'],
                    'previous_surge': surge_info['max_surge'],
                    'surge_period': surge_info['surge_period'],
                    'ma5_distance': pullback_info['ma5_distance'],
                    'pos_in_5d': pullback_info['pos_in_5d'],
                    'vol_ratio': vol_info['vol_ratio'],
                    'vol_pattern': vol_info['pattern'],
                    'signal_strength': signal_strength,
                    'amount_yi': latest_row['amount'] / 10000,
                    'ma5': latest_row['ma5'],
                    'ma10': latest_row['ma10']
                })
                
            except Exception as e:
                logger.debug(f"处理股票 {ts_code} 时出错: {e}")
                continue
        
        # 输出调试统计
        logger.info("📊 强势回调筛选统计:")
        for key, value in debug_stats.items():
            logger.info(f"   {key}: {value}")
        
        result_df = pd.DataFrame(qualified_stocks)
        
        if not result_df.empty:
            result_df = result_df.sort_values('signal_strength', ascending=False)
            logger.info(f"找到 {len(result_df)} 只符合强势回调低吸条件的股票")
        
        return result_df


def create_strong_pullback_markdown(df: pd.DataFrame, query_date: str) -> str:
    """创建强势回调低吸的markdown消息"""
    if df.empty:
        return f"""## 📈 强势回调低吸提醒 ({query_date})

❌ **暂无符合条件的股票**

**策略条件：**
- 🚀 前期大涨：近期有20%+涨幅
- 📉 技术回调：距MA5在0-8%，5日内40-70%位置
- 🔊 量价配合：缩量调整或温和放量
- 📊 趋势完好：MA5 >= MA10，上升趋势保持

等待强势股回调机会。
"""
    
    total_count = len(df)
    
    markdown = f"""## 📈 强势回调低吸机会 ({query_date})

🎯 **找到 {total_count} 只强势回调机会**

| 排名 | 股票名称 | 代码 | 前期涨幅 | 距MA5 | 5日位置 | 成交量 | 信号强度 |
|------|---------|------|----------|-------|---------|--------|----------|"""
    
    for i, (_, row) in enumerate(df.head(15).iterrows(), 1):
        code = row['ts_code'].split('.')[0]
        name = row['stock_name'][:6]  # 限制股票名称长度
        
        markdown += f"""
| {i:>2} | {name} | {code} | {row['previous_surge']:.1f}% | {row['ma5_distance']:+.1f}% | {row['pos_in_5d']:.1f}% | {row['vol_ratio']:.1f}x | {row['signal_strength']:.0f}分 |"""
    
    if total_count > 15:
        markdown += f"\n\n*还有 {total_count - 15} 只股票符合条件*"
    
    markdown += f"""

---

**策略说明：**
- 🚀 前期大涨：近期有20%+涨幅
- 📉 技术回调：距MA5在0-8%范围
- 🔊 量价配合：缩量调整或温和放量
- 📊 趋势完好：上升趋势保持

*基于选手光库科技246%收益模式*
"""
    
    return markdown


def run_strong_pullback_strategy(notify: bool = True, min_signal_strength: float = 70.0) -> pd.DataFrame:
    """运行强势回调低吸策略"""
    try:
        logger.info("🚀 开始执行强势回调低吸策略...")
        
        result_df = find_strong_pullback_stocks(min_signal_strength=min_signal_strength)
        
        if not result_df.empty:
            latest_date = result_df.iloc[0]['trade_date']
            query_date = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)
        else:
            query_date = datetime.now().strftime('%Y-%m-%d')
        
        # 发送通知
        markdown_content = create_strong_pullback_markdown(result_df, query_date)
        try:
            send_result = send_markdown_message(markdown_content)
            if send_result:
                logger.info("✅ 强势回调低吸提醒已发送")
        except Exception as e:
            logger.error(f"发送消息时出错: {e}")
        
        # 打印结果
        if not result_df.empty:
            print(f"\\n📈 强势回调低吸机会 ({query_date}):")
            print("=" * 110)
            print("排名  股票名称     代码      前期涨幅  距MA5   5日位置  成交量  信号强度")
            print("-" * 110)
            
            for i, (_, row) in enumerate(result_df.head(10).iterrows(), 1):
                code = row['ts_code'].split('.')[0]
                name = row.get('stock_name', '未知')[:6]
                print(f"{i:>2}   {name:<8} {code:<8} {row['previous_surge']:>7.1f}% "
                      f"{row['ma5_distance']:>6.1f}% {row['pos_in_5d']:>6.1f}% "
                      f"{row['vol_ratio']:>5.1f}x {row['signal_strength']:>6.0f}分")
        
        return result_df
        
    except Exception as e:
        logger.error(f"执行强势回调低吸策略时出错: {e}")
        return pd.DataFrame()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='强势回调低吸策略（基于选手光库科技模式）')
    parser.add_argument('--min-signal-strength', type=float, default=70.0,
                       help='最小信号强度（默认70.0）')
    parser.add_argument('--no-notify', action='store_true',
                       help='不发送通知，仅显示结果')
    
    args = parser.parse_args()
    
    result_df = run_strong_pullback_strategy(
        min_signal_strength=args.min_signal_strength
    )
    
    if not result_df.empty:
        logger.info("✅ 强势回调低吸策略执行完成")
    else:
        logger.info("📊 今日无符合条件的强势回调机会")


if __name__ == "__main__":
    main()
