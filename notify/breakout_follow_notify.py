#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高位突破跟进策略
基于选手"金信诺161%收益"操作模式
策略：高位整理 + 放量突破 + 涨停确认 + 趋势确立 + 立即跟进
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
        logging.FileHandler('breakout_follow_notify.log', encoding='utf-8'),
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
    df_calc['vol_ma10'] = df_calc['vol'].rolling(window=10, min_periods=1).mean()
    
    # 价格位置
    df_calc['high_5d'] = df_calc['high'].rolling(window=5, min_periods=1).max()
    df_calc['low_5d'] = df_calc['low'].rolling(window=5, min_periods=1).min()
    df_calc['high_10d'] = df_calc['high'].rolling(window=10, min_periods=1).max()
    
    # 涨跌幅
    df_calc['pct_1d'] = df_calc['close'].pct_change() * 100
    df_calc['pct_3d'] = df_calc['close'].pct_change(periods=3) * 100
    df_calc['pct_5d'] = df_calc['close'].pct_change(periods=5) * 100
    
    return df_calc.sort_values('trade_date', ascending=False).reset_index(drop=True)


def check_high_position(row: pd.Series, min_position: float = 70.0) -> dict:
    """
    检查是否处于高位
    条件：在5日内位置 > 70%
    """
    close = row['close']
    low_5d = row['low_5d']
    high_5d = row['high_5d']
    
    if any(pd.isna(x) for x in [close, low_5d, high_5d]) or high_5d <= low_5d:
        return {'is_high': False, 'position': 0}
    
    pos_in_5d = (close - low_5d) / (high_5d - low_5d) * 100
    
    return {
        'is_high': pos_in_5d >= min_position,
        'position': pos_in_5d
    }


def check_volume_breakout(row: pd.Series, min_vol_ratio: float = 2.0) -> dict:
    """
    检查放量突破
    条件：成交量 >= 5日均量的2倍
    """
    vol = row['vol']
    vol_ma5 = row['vol_ma5']
    
    if pd.isna(vol) or pd.isna(vol_ma5) or vol_ma5 <= 0:
        return {'is_breakout': False, 'vol_ratio': 0}
    
    vol_ratio = vol / vol_ma5
    
    return {
        'is_breakout': vol_ratio >= min_vol_ratio,
        'vol_ratio': vol_ratio
    }


def check_price_breakout(row: pd.Series, df: pd.DataFrame, current_idx: int = 0) -> dict:
    """
    检查价格突破
    条件：突破前期高点 + 当日涨幅 > 5%
    """
    close = row['close']
    pct_chg = row['change_pct']
    
    # 当日涨幅要求
    if pct_chg < 5:
        return {'is_breakout': False, 'breakout_strength': 0, 'recent_high': 0}
    
    # 获取前10日的最高价（排除当日）
    if current_idx + 10 < len(df):
        recent_data = df.iloc[current_idx+1:current_idx+11]
        recent_high = recent_data['high'].max()
        
        if recent_high > 0:
            breakout_strength = (close - recent_high) / recent_high * 100
            is_breakout = breakout_strength > 2  # 突破前期高点2%以上
            
            return {
                'is_breakout': is_breakout,
                'breakout_strength': breakout_strength,
                'recent_high': recent_high
            }
    
    return {'is_breakout': False, 'breakout_strength': 0, 'recent_high': 0}


def check_ma_trending_up(row: pd.Series) -> bool:
    """
    检查均线趋势向上
    条件：MA5 > MA10或接近，显示上升趋势
    """
    ma5 = row['ma5']
    ma10 = row['ma10']
    close = row['close']
    
    if any(pd.isna(x) for x in [ma5, ma10, close]):
        return False
    
    # 股价在MA5上方，且MA5 >= MA10*0.98（允许小幅低于）
    return close > ma5 and ma5 >= ma10 * 0.98


def calculate_breakout_strength(row: pd.Series, high_pos: dict, vol_breakout: dict, 
                              price_breakout: dict) -> float:
    """
    计算突破信号强度（0-100）
    """
    score = 0.0
    
    # 价格位置评分（0-25分）
    position = high_pos.get('position', 0)
    if position > 90:
        score += 25
    elif position > 80:
        score += 20
    elif position > 70:
        score += 15
    else:
        score += 5
    
    # 成交量突破评分（0-30分）
    vol_ratio = vol_breakout.get('vol_ratio', 0)
    if vol_ratio > 4:
        score += 30
    elif vol_ratio > 3:
        score += 25
    elif vol_ratio > 2:
        score += 20
    else:
        score += 10
    
    # 价格突破评分（0-25分）
    breakout_strength = price_breakout.get('breakout_strength', 0)
    pct_chg = row['change_pct']
    
    if pct_chg > 9:  # 涨停
        score += 25
    elif pct_chg > 7:
        score += 20
    elif pct_chg > 5:
        score += 15
    else:
        score += 10
    
    # 均线趋势评分（0-20分）
    if check_ma_trending_up(row):
        score += 20
    else:
        score += 5
    
    return min(100.0, score)


def find_breakout_follow_stocks(days_back: int = 15, min_signal_strength: float = 75.0) -> pd.DataFrame:
    """查找符合高位突破跟进条件的股票"""
    logger.info("开始筛选高位突破跟进机会...")
    
    with StockDatabase() as db:
        # 获取数据
        latest_data = db.query_data(limit=1)
        if latest_data is None or latest_data.empty:
            return pd.DataFrame()
        
        latest_trade_date = latest_data.iloc[0]['trade_date']
        end_date = latest_trade_date.strftime('%Y-%m-%d')
        start_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=20)
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
            'not_high_position': 0, 'no_volume_breakout': 0, 'no_price_breakout': 0,
            'trend_not_up': 0, 'low_signal_strength': 0, 'qualified': 0
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
                
                # 检查高位
                high_pos = check_high_position(latest_row, 70.0)
                if not high_pos['is_high']:
                    debug_stats['not_high_position'] += 1
                    continue
                
                # 检查放量突破
                vol_breakout = check_volume_breakout(latest_row, 2.0)
                if not vol_breakout['is_breakout']:
                    debug_stats['no_volume_breakout'] += 1
                    continue
                
                # 检查价格突破
                price_breakout = check_price_breakout(latest_row, stock_df_with_indicators, 0)
                if not price_breakout['is_breakout']:
                    debug_stats['no_price_breakout'] += 1
                    continue
                
                # 检查均线趋势
                if not check_ma_trending_up(latest_row):
                    debug_stats['trend_not_up'] += 1
                    continue
                
                # 计算信号强度
                signal_strength = calculate_breakout_strength(latest_row, high_pos, vol_breakout, price_breakout)
                
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
                    'pct_3d': latest_row['pct_3d'],
                    'pos_in_5d': high_pos['position'],
                    'vol_ratio': vol_breakout['vol_ratio'],
                    'breakout_strength': price_breakout['breakout_strength'],
                    'recent_high': price_breakout['recent_high'],
                    'signal_strength': signal_strength,
                    'amount_yi': latest_row['amount'] / 10000,
                    'ma5': latest_row['ma5'],
                    'ma10': latest_row['ma10']
                })
                
            except Exception as e:
                logger.debug(f"处理股票 {ts_code} 时出错: {e}")
                continue
        
        # 输出调试统计
        logger.info("📊 高位突破筛选统计:")
        for key, value in debug_stats.items():
            logger.info(f"   {key}: {value}")
        
        result_df = pd.DataFrame(qualified_stocks)
        
        if not result_df.empty:
            result_df = result_df.sort_values('signal_strength', ascending=False)
            logger.info(f"找到 {len(result_df)} 只符合高位突破跟进条件的股票")
        
        return result_df


def create_breakout_follow_markdown(df: pd.DataFrame, query_date: str) -> str:
    """创建高位突破跟进的markdown消息"""
    if df.empty:
        return f"""## 🚀 高位突破跟进提醒 ({query_date})

❌ **暂无符合条件的股票**

**策略条件：**
- 📊 高位位置：5日内位置 > 70%
- 🔊 放量突破：成交量 >= 5日均量2倍
- 📈 价格突破：突破前期高点且当日涨幅 > 5%
- 📊 均线向上：MA5 >= MA10，趋势确立

等待突破机会出现。
"""
    
    total_count = len(df)
    avg_signal_strength = df['signal_strength'].mean()
    avg_breakout_strength = df['breakout_strength'].mean()
    max_vol_ratio = df['vol_ratio'].max()
    
    markdown = f"""## 🚀 高位突破跟进提醒 ({query_date})

🎯 **筛选结果：找到 {total_count} 只突破机会**
- 📊 平均信号强度：{avg_signal_strength:.1f}分
- 🚀 平均突破强度：{avg_breakout_strength:.1f}%
- 🔊 最大放量倍数：{max_vol_ratio:.1f}倍

---

### 🏆 重点关注股票（按信号强度排序）

"""
    
    for i, (_, row) in enumerate(df.head(10).iterrows(), 1):
        code = row['ts_code'].split('.')[0]
        
        markdown += f"""
**{i}. {row['stock_name']} ({code})**
- 🏢 行业板块：{row['industry']} | {row['area']}
- 💰 突破价格：{row['close']:.2f}元 ({row['pct_1d']:+.1f}%)
- 📊 位置强度：5日内{row['pos_in_5d']:.1f}%高位
- 🚀 突破确认：突破前高{row['recent_high']:.2f}元，强度{row['breakout_strength']:+.1f}%
- 🔊 放量程度：{row['vol_ratio']:.1f}倍放量突破
- 📈 短期表现：3日{row['pct_3d']:+.1f}%
- 🎯 信号强度：{row['signal_strength']:.0f}分
- 💸 成交额：{row['amount_yi']:.1f}亿元
- 📊 均线：MA5({row['ma5']:.2f}) MA10({row['ma10']:.2f})
"""
    
    if total_count > 10:
        markdown += f"\\n... 还有 {total_count - 10} 只股票符合条件"
    
    markdown += f"""

---

### 📋 策略说明
**高位突破跟进策略（基于选手金信诺161%收益模式）：**
1. 📊 **高位位置**：在5日内70%以上的相对高位
2. 🔊 **放量突破**：成交量突破，>=5日均量2倍
3. 📈 **价格突破**：突破前期高点，当日涨幅>5%
4. 📊 **趋势确立**：均线系统配合，MA5>=MA10
5. ⚡ **立即跟进**：突破确认后立即跟进

**投资逻辑：**
- 高位放量突破往往意味着新一轮上涨开始
- 量价齐升是最强的技术确认信号
- 选手实战验证：金信诺获得161%收益

**风险提示：**
- 假突破风险，需要严格止损
- 高位追高风险，控制仓位
- 建议跌破突破点止损

*策略来源：基于实战高手操作模式总结*
"""
    
    return markdown


def run_breakout_follow_strategy(notify: bool = True, min_signal_strength: float = 75.0) -> pd.DataFrame:
    """运行高位突破跟进策略"""
    try:
        logger.info("🚀 开始执行高位突破跟进策略...")
        
        result_df = find_breakout_follow_stocks(min_signal_strength=min_signal_strength)
        
        if not result_df.empty:
            latest_date = result_df.iloc[0]['trade_date']
            query_date = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)
        else:
            query_date = datetime.now().strftime('%Y-%m-%d')
        
        # 发送通知
        if notify:
            markdown_content = create_breakout_follow_markdown(result_df, query_date)
            try:
                send_result = send_markdown_message(markdown_content)
                if send_result:
                    logger.info("✅ 高位突破跟进提醒已发送")
            except Exception as e:
                logger.error(f"发送消息时出错: {e}")
        
        # 打印结果
        if not result_df.empty:
            print(f"\\n🚀 高位突破跟进机会 ({query_date}):")
            print("=" * 110)
            print("排名  股票名称     代码      当日涨幅  5日位置  放量倍数  突破强度  信号强度")
            print("-" * 110)
            
            for i, (_, row) in enumerate(result_df.head(10).iterrows(), 1):
                code = row['ts_code'].split('.')[0]
                name = row.get('stock_name', '未知')[:6]
                print(f"{i:>2}   {name:<8} {code:<8} {row['pct_1d']:>7.1f}% "
                      f"{row['pos_in_5d']:>6.1f}% {row['vol_ratio']:>6.1f}x "
                      f"{row['breakout_strength']:>6.1f}% {row['signal_strength']:>6.0f}分")
        
        return result_df
        
    except Exception as e:
        logger.error(f"执行高位突破跟进策略时出错: {e}")
        return pd.DataFrame()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='高位突破跟进策略（基于选手金信诺模式）')
    parser.add_argument('--min-signal-strength', type=float, default=75.0,
                       help='最小信号强度（默认75.0）')
    parser.add_argument('--no-notify', action='store_true',
                       help='不发送通知，仅显示结果')
    
    args = parser.parse_args()
    
    result_df = run_breakout_follow_strategy(
        notify=not args.no_notify,
        min_signal_strength=args.min_signal_strength
    )
    
    if not result_df.empty:
        logger.info("✅ 高位突破跟进策略执行完成")
    else:
        logger.info("📊 今日无符合条件的突破机会")


if __name__ == "__main__":
    main()
