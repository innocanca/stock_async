#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
选手操作模式策略集合
基于实战高手操作风格总结的三大策略
"""

import logging
import sys
import os
from datetime import datetime
import argparse

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('master_strategies.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def run_master_strategies(notify: bool = True, strategy_filter: str = None) -> dict:
    """
    运行选手模式的三大策略
    
    Args:
        notify: 是否发送通知
        strategy_filter: 策略过滤器（可选：bottom_reversal, strong_pullback, breakout_follow）
        
    Returns:
        dict: 执行结果统计
    """
    logger.info("🚀 开始执行选手操作模式策略集合...")
    
    strategies_to_run = []
    
    # 根据过滤器决定运行哪些策略
    if strategy_filter == 'bottom_reversal' or strategy_filter is None:
        strategies_to_run.append(('bottom_reversal', '底部反转抄底'))
    
    if strategy_filter == 'strong_pullback' or strategy_filter is None:
        strategies_to_run.append(('strong_pullback', '强势回调低吸'))
    
    if strategy_filter == 'breakout_follow' or strategy_filter is None:
        strategies_to_run.append(('breakout_follow', '高位突破跟进'))
    
    results = {
        'total_strategies': len(strategies_to_run),
        'successful_strategies': 0,
        'failed_strategies': 0,
        'strategy_results': {},
        'total_opportunities': 0
    }
    
    for strategy_name, strategy_desc in strategies_to_run:
        try:
            logger.info(f"🎯 执行策略: {strategy_desc}...")
            
            if strategy_name == 'bottom_reversal':
                from bottom_reversal_notify import run_bottom_reversal_strategy
                result_df = run_bottom_reversal_strategy(notify=notify, min_signal_strength=70.0)
            
            elif strategy_name == 'strong_pullback':
                from strong_pullback_notify import run_strong_pullback_strategy
                result_df = run_strong_pullback_strategy(notify=notify, min_signal_strength=70.0)
            
            elif strategy_name == 'breakout_follow':
                from breakout_follow_notify import run_breakout_follow_strategy
                result_df = run_breakout_follow_strategy(notify=notify, min_signal_strength=75.0)
            
            opportunities_count = len(result_df) if result_df is not None else 0
            
            results['strategy_results'][strategy_name] = {
                'success': True,
                'opportunities': opportunities_count,
                'description': strategy_desc
            }
            
            results['successful_strategies'] += 1
            results['total_opportunities'] += opportunities_count
            
            logger.info(f"✅ {strategy_desc} 完成，发现 {opportunities_count} 个机会")
            
        except Exception as e:
            logger.error(f"❌ {strategy_desc} 执行失败: {e}")
            results['strategy_results'][strategy_name] = {
                'success': False,
                'error': str(e),
                'description': strategy_desc
            }
            results['failed_strategies'] += 1
    
    # 输出总结
    logger.info("📊 选手模式策略集合执行完成:")
    logger.info(f"   成功策略: {results['successful_strategies']}/{results['total_strategies']}")
    logger.info(f"   总机会数: {results['total_opportunities']}")
    
    return results


def print_strategy_summary():
    """打印策略介绍"""
    print("🎯 选手操作模式策略集合")
    print("=" * 80)
    print("基于实战高手操作风格总结的三大经典策略")
    
    strategies = [
        {
            'name': '底部反转抄底',
            'file': 'bottom_reversal_notify.py',
            'model': '广生堂681%收益模式',
            'features': [
                '前期强势股充分调整',
                '远离均线（距MA5 < -5%）',
                '成交量萎缩企稳',
                '均线交织状态',
                '底部反转信号'
            ]
        },
        {
            'name': '强势回调低吸',
            'file': 'strong_pullback_notify.py', 
            'model': '光库科技246%收益模式',
            'features': [
                '前期大涨股票（20%+）',
                '技术回调到均线（距MA5 0-8%）',
                '缩量调整或温和放量',
                '上升趋势保持完好',
                '关键支撑位企稳'
            ]
        },
        {
            'name': '高位突破跟进',
            'file': 'breakout_follow_notify.py',
            'model': '金信诺161%收益模式', 
            'features': [
                '高位位置（5日内70%+）',
                '放量突破（2倍+成交量）',
                '价格突破前期高点',
                '当日大涨（5%+涨幅）',
                '均线系统配合向上'
            ]
        }
    ]
    
    for i, strategy in enumerate(strategies, 1):
        print(f"\\n{i}. 🎯 {strategy['name']}")
        print(f"   📄 文件: {strategy['file']}")
        print(f"   📈 模式: {strategy['model']}")
        print(f"   🔍 特征:")
        for feature in strategy['features']:
            print(f"      • {feature}")
    
    print("\\n💡 使用方法:")
    print("   python master_strategies_notify.py --strategy all")
    print("   python master_strategies_notify.py --strategy bottom_reversal")
    print("   python master_strategies_notify.py --strategy strong_pullback") 
    print("   python master_strategies_notify.py --strategy breakout_follow")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='选手操作模式策略集合')
    parser.add_argument('--strategy', choices=['all', 'bottom_reversal', 'strong_pullback', 'breakout_follow'],
                       default='all', help='选择运行的策略')
    parser.add_argument('--no-notify', action='store_true', help='不发送通知，仅显示结果')
    parser.add_argument('--info', action='store_true', help='显示策略介绍')
    
    args = parser.parse_args()
    
    if args.info:
        print_strategy_summary()
        return
    
    # 运行策略
    strategy_filter = None if args.strategy == 'all' else args.strategy
    
    results = run_master_strategies(
        notify=not args.no_notify,
        strategy_filter=strategy_filter
    )
    
    # 打印执行结果
    print("\\n" + "=" * 80)
    print("📊 选手模式策略执行结果")
    print("=" * 80)
    
    for strategy_name, result in results['strategy_results'].items():
        if result['success']:
            print(f"✅ {result['description']}: {result['opportunities']} 个机会")
        else:
            print(f"❌ {result['description']}: 执行失败")
    
    print(f"\\n🎯 总计发现 {results['total_opportunities']} 个投资机会")
    
    if results['total_opportunities'] > 0:
        print("\\n💡 建议:")
        print("   1. 优先关注信号强度高的机会")
        print("   2. 结合市场环境判断")
        print("   3. 严格执行止损纪律")
        print("   4. 分批建仓控制风险")


if __name__ == "__main__":
    main()
