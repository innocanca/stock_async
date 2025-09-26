#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略运行器
统一管理所有notify目录下的策略，支持批量执行和定时调度
"""

import logging
import sys
import os
import importlib
from datetime import datetime
import argparse

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('strategy_runner.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class StrategyRunner:
    """策略运行器"""
    
    def __init__(self):
        self.strategies = {
            'volume_acceleration': {
                'module': 'volume_acceleration_notify',
                'function': 'run_volume_acceleration_strategy',
                'description': '放量加速突破策略',
                'params': {
                    'vol_multiplier': 2.0,
                    'acceleration_threshold': 1.5,
                    'min_rise_pct': 15.0,
                    'min_signal_strength': 75.0
                }
            },
            'limit_up': {
                'module': 'limit_up_notify',
                'function': 'main',
                'description': '涨停股票提醒',
                'params': {}
            },
            'pullback_ma10': {
                'module': 'pullback_ma10_notify', 
                'function': 'main',
                'description': '强势回踩10日线',
                'params': {}
            },
            'strong_sector_pullback': {
                'module': 'strong_sector_pullback_notify',
                'function': 'run_strong_sector_pullback_strategy',
                'description': '强势板块(含概念)回调回踩10日线',
                'params': {
                    'min_pullback': 0.05,
                    'max_pullback': 0.25,
                    'ma10_tolerance': 0.03,
                    'min_signal_strength': 70.0,
                    'strong_sector_pct': 0.3
                }
            },
            'bottom_reversal': {
                'module': 'bottom_reversal_notify',
                'function': 'run_bottom_reversal_strategy',
                'description': '底部反转抄底策略（选手广生堂模式）',
                'params': {
                    'min_signal_strength': 70.0
                }
            },
            'strong_pullback': {
                'module': 'strong_pullback_notify',
                'function': 'run_strong_pullback_strategy',
                'description': '强势回调低吸策略（选手光库科技模式）',
                'params': {
                    'min_signal_strength': 70.0
                }
            },
            'breakout_follow': {
                'module': 'breakout_follow_notify',
                'function': 'run_breakout_follow_strategy',
                'description': '高位突破跟进策略（选手金信诺模式）',
                'params': {
                    'min_signal_strength': 75.0
                }
            },
            'daily_review': {
                'module': 'daily_market_review',
                'function': 'run_daily_market_review',
                'description': '每日市场复盘分析（综合报告）',
                'params': {
                    'notify': True
                }
            }
        }
    
    def list_strategies(self):
        """列出所有可用策略"""
        print("📋 可用策略列表:")
        print("=" * 60)
        
        for name, config in self.strategies.items():
            print(f"\\n🎯 {name}")
            print(f"   描述: {config['description']}")
            print(f"   模块: {config['module']}")
            if config['params']:
                print(f"   参数: {config['params']}")
    
    def run_strategy(self, strategy_name: str, custom_params: dict = None, notify: bool = True) -> bool:
        """
        运行指定策略
        
        Args:
            strategy_name: 策略名称
            custom_params: 自定义参数
            notify: 是否发送通知
            
        Returns:
            bool: 是否执行成功
        """
        if strategy_name not in self.strategies:
            logger.error(f"未找到策略: {strategy_name}")
            return False
        
        try:
            strategy_config = self.strategies[strategy_name]
            
            logger.info(f"🚀 开始执行策略: {strategy_config['description']}")
            
            # 动态导入模块
            module = importlib.import_module(strategy_config['module'])
            
            # 获取执行函数
            if hasattr(module, strategy_config['function']):
                func = getattr(module, strategy_config['function'])
                
                # 准备参数
                params = strategy_config['params'].copy()
                if custom_params:
                    params.update(custom_params)
                
                # 添加notify参数
                if 'notify' in func.__code__.co_varnames:
                    params['notify'] = notify
                
                # 执行策略
                result = func(**params)
                
                logger.info(f"✅ 策略 {strategy_name} 执行完成")
                return True
            else:
                logger.error(f"策略模块 {strategy_config['module']} 中未找到函数 {strategy_config['function']}")
                return False
                
        except Exception as e:
            logger.error(f"执行策略 {strategy_name} 时出错: {e}")
            return False
    
    def run_all_strategies(self, notify: bool = True) -> dict:
        """
        运行所有策略
        
        Args:
            notify: 是否发送通知
            
        Returns:
            dict: 执行结果统计
        """
        logger.info("🚀 开始执行所有策略...")
        
        results = {
            'total': len(self.strategies),
            'success': 0,
            'failed': 0,
            'failed_strategies': []
        }
        
        for strategy_name in self.strategies.keys():
            try:
                if self.run_strategy(strategy_name, notify=notify):
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['failed_strategies'].append(strategy_name)
            except Exception as e:
                logger.error(f"策略 {strategy_name} 执行异常: {e}")
                results['failed'] += 1
                results['failed_strategies'].append(strategy_name)
        
        logger.info(f"📊 策略执行完成: 成功{results['success']}/{results['total']}")
        
        return results


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='策略运行器')
    parser.add_argument('--strategy', choices=['volume_acceleration', 'limit_up', 'pullback_ma10', 'all'],
                       help='指定要运行的策略')
    parser.add_argument('--list', action='store_true', help='列出所有可用策略')
    parser.add_argument('--no-notify', action='store_true', help='不发送通知，仅显示结果')
    
    # 放量加速策略的自定义参数
    parser.add_argument('--vol-multiplier', type=float, default=2.0,
                       help='放量倍数阈值（仅volume_acceleration策略）')
    parser.add_argument('--acceleration-threshold', type=float, default=1.5,
                       help='加速度阈值（仅volume_acceleration策略）')
    parser.add_argument('--min-rise-pct', type=float, default=15.0,
                       help='最小3日涨幅（仅volume_acceleration策略）')
    parser.add_argument('--min-signal-strength', type=float, default=75.0,
                       help='最小信号强度（仅volume_acceleration策略）')
    
    args = parser.parse_args()
    
    runner = StrategyRunner()
    
    # 列出策略
    if args.list:
        runner.list_strategies()
        return
    
    # 如果没有指定策略，默认运行放量加速策略
    if not args.strategy:
        args.strategy = 'volume_acceleration'
    
    notify = not args.no_notify
    
    if args.strategy == 'all':
        # 运行所有策略
        results = runner.run_all_strategies(notify=notify)
        print(f"\\n📊 批量执行结果: 成功{results['success']}/{results['total']}")
        if results['failed_strategies']:
            print(f"❌ 失败的策略: {', '.join(results['failed_strategies'])}")
    
    elif args.strategy == 'volume_acceleration':
        # 运行放量加速策略，支持自定义参数
        custom_params = {
            'vol_multiplier': args.vol_multiplier,
            'acceleration_threshold': args.acceleration_threshold, 
            'min_rise_pct': args.min_rise_pct,
            'min_signal_strength': args.min_signal_strength
        }
        runner.run_strategy('volume_acceleration', custom_params, notify)
    
    else:
        # 运行指定策略
        runner.run_strategy(args.strategy, notify=notify)


if __name__ == "__main__":
    main()
