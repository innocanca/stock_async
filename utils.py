# -*- coding: utf-8 -*-
"""
工具函数模块
包含日期处理、配置管理等工具函数
"""

import logging
import argparse
from datetime import datetime
from config import ARGS_CONFIG

logger = logging.getLogger(__name__)


def format_date(date_str: str) -> str:
    """
    格式化日期字符串为YYYYMMDD格式
    
    Args:
        date_str: 输入的日期字符串
        
    Returns:
        str: YYYYMMDD格式的日期字符串
        
    Raises:
        ValueError: 无法解析日期格式时抛出
    """
    if not date_str:
        return None
    
    # 尝试解析不同格式的日期
    for fmt in ['%Y-%m-%d', '%Y%m%d', '%Y/%m/%d']:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y%m%d')
        except ValueError:
            continue
    
    raise ValueError(f"无法解析日期格式: {date_str}")


def load_config_defaults(config_mode: str) -> dict:
    """
    从配置文件加载默认参数
    
    Args:
        config_mode: 配置模式名称
        
    Returns:
        dict: 配置参数字典
    """
    if config_mode not in ARGS_CONFIG:
        logger.warning(f"未找到配置模式 '{config_mode}'，使用默认配置")
        config_mode = 'default'
    
    config = ARGS_CONFIG[config_mode].copy()
    logger.info(f"加载配置模式: '{config_mode}'")
    
    return config


def merge_config_and_args(config_defaults: dict, args: argparse.Namespace) -> argparse.Namespace:
    """
    合并配置文件默认值和命令行参数，命令行参数优先级更高
    
    Args:
        config_defaults: 配置文件中的默认值
        args: 命令行参数
        
    Returns:
        argparse.Namespace: 合并后的参数
    """
    # 对于每个配置项，如果命令行参数为None或False，则使用配置文件的值
    for key, default_value in config_defaults.items():
        if hasattr(args, key):
            current_value = getattr(args, key)
            # 如果命令行参数未设置（None或False），使用配置文件的值
            if current_value is None or (isinstance(current_value, bool) and not current_value):
                setattr(args, key, default_value)
                logger.debug(f"使用配置文件默认值: {key} = {default_value}")
        else:
            # 如果命令行参数不存在该属性，添加配置文件的值
            setattr(args, key, default_value)
            logger.debug(f"添加配置文件参数: {key} = {default_value}")
    
    return args


def print_current_config(args: argparse.Namespace):
    """
    打印当前使用的配置
    
    Args:
        args: 命令行参数命名空间
    """
    print("\\n" + "=" * 60)
    print("📋 当前运行配置")
    print("=" * 60)
    
    config_items = [
        ('配置模式', getattr(args, 'config', 'default')),
        ('股票代码', '所有主板股票' if not args.codes else f"{len(args.codes)}只指定股票"),
        ('日期范围', f"{getattr(args, 'start_date', 'N/A')} 到 {getattr(args, 'end_date', '今天')}"),
        ('股票限制', f"{args.limit}只" if args.limit else '不限制'),
        ('获取模式', args.mode),
        ('批次大小', f"{args.batch_size}只"),
        ('API延迟', f"{args.delay}秒"),
        ('全市场模式', '开启' if getattr(args, 'market_mode', False) else '关闭'),
        ('分批插入', f"开启(每{getattr(args, 'batch_days', 10)}天)" if getattr(args, 'use_batch_insert', False) else '关闭'),
        ('交易所', getattr(args, 'exchange', 'SSE')),
        ('特殊操作', get_special_operations(args))
    ]
    
    for label, value in config_items:
        print(f"   {label:<8}: {value}")
    
    print("=" * 60)


def get_special_operations(args: argparse.Namespace) -> str:
    """
    获取特殊操作描述
    
    Args:
        args: 命令行参数命名空间
        
    Returns:
        str: 特殊操作的描述字符串
    """
    operations = []
    if getattr(args, 'query', False):
        operations.append('数据查询')
    if getattr(args, 'stats', False):
        operations.append('统计信息')
    if getattr(args, 'latest', False):
        operations.append('最新交易日')
    if getattr(args, 'trade_date', None):
        operations.append(f'指定交易日({args.trade_date})')
    if getattr(args, 'create_db', False):
        operations.append('创建数据库')
    
    return ', '.join(operations) if operations else '数据获取'


def estimate_execution_time(stock_count: int, delay: float, batch_size: int = 50) -> str:
    """
    预估执行时间
    
    Args:
        stock_count: 股票数量
        delay: API调用延迟（秒）
        batch_size: 批次大小
        
    Returns:
        str: 预估时间的描述字符串
    """
    # 基础时间：股票数量 × 延迟
    base_time = stock_count * delay
    
    # 批次延迟：每批次后额外2秒休眠
    batch_delays = (stock_count // batch_size) * 2
    
    # 总时间（分钟）
    total_minutes = (base_time + batch_delays) / 60
    
    if total_minutes >= 1:
        return f"{total_minutes:.1f}分钟"
    else:
        return f"{total_minutes*60:.0f}秒"


def validate_stock_codes(codes: list) -> bool:
    """
    验证股票代码格式
    
    Args:
        codes: 股票代码列表
        
    Returns:
        bool: 格式是否正确
    """
    if not codes:
        return True
    
    valid_patterns = ['.SH', '.SZ', '.BJ']  # 支持的交易所后缀
    
    for code in codes:
        if not isinstance(code, str) or len(code) < 8:
            logger.warning(f"股票代码格式错误: {code}")
            return False
        
        if not any(code.endswith(pattern) for pattern in valid_patterns):
            logger.warning(f"不支持的股票代码格式: {code}")
            return False
    
    return True


def format_number(num: int) -> str:
    """
    格式化数字，添加千位分隔符
    
    Args:
        num: 要格式化的数字
        
    Returns:
        str: 格式化后的字符串
    """
    if num is None:
        return 'N/A'
    return f"{num:,}"


def get_available_config_modes() -> list:
    """
    获取所有可用的配置模式
    
    Returns:
        list: 配置模式名称列表
    """
    return list(ARGS_CONFIG.keys())
