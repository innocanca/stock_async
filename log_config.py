#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一日志配置模块

功能：
1. 提供统一的日志配置
2. 支持日志轮转
3. 统一日志格式
4. 支持不同模块的日志标识

使用方法：
from log_config import get_logger
logger = get_logger(__name__)
"""

import logging
import logging.handlers
import os
from datetime import datetime


def setup_unified_logger(
    log_file: str = 'stock_analysis.log',
    max_bytes: int = 50 * 1024 * 1024,  # 50MB
    backup_count: int = 5,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG
):
    """
    设置统一的日志配置
    
    Args:
        log_file: 日志文件名
        max_bytes: 单个日志文件最大大小（字节）
        backup_count: 保留的备份文件数量
        console_level: 控制台日志级别
        file_level: 文件日志级别
    """
    
    # 创建根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # 清除已有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 统一的日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 文件处理器（支持轮转）
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # 记录启动信息
    logging.info(f"=" * 60)
    logging.info(f"日志系统启动 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"日志文件: {log_file}")
    logging.info(f"=" * 60)


def get_logger(name: str = None):
    """
    获取日志器
    
    Args:
        name: 日志器名称，通常使用 __name__
        
    Returns:
        logging.Logger: 配置好的日志器
    """
    # 如果根日志器没有处理器，则初始化
    if not logging.getLogger().handlers:
        setup_unified_logger()
    
    return logging.getLogger(name)


def log_function_call(func_name: str, **kwargs):
    """
    记录函数调用信息
    
    Args:
        func_name: 函数名
        **kwargs: 函数参数
    """
    logger = get_logger('function_call')
    params = ', '.join([f'{k}={v}' for k, v in kwargs.items()])
    logger.info(f"🔧 调用函数: {func_name}({params})")


def log_performance(func_name: str, duration: float, **stats):
    """
    记录性能信息
    
    Args:
        func_name: 函数名
        duration: 执行时间（秒）
        **stats: 统计信息
    """
    logger = get_logger('performance')
    stats_str = ', '.join([f'{k}={v}' for k, v in stats.items()])
    logger.info(f"⏱️  性能: {func_name} 耗时 {duration:.2f}秒 [{stats_str}]")


def log_data_operation(operation: str, table: str = None, records: int = None, **kwargs):
    """
    记录数据操作信息
    
    Args:
        operation: 操作类型（INSERT, SELECT, UPDATE, DELETE等）
        table: 表名
        records: 记录数
        **kwargs: 其他信息
    """
    logger = get_logger('data_operation')
    info_parts = [f"📊 数据操作: {operation}"]
    if table:
        info_parts.append(f"表={table}")
    if records:
        info_parts.append(f"记录数={records}")
    for k, v in kwargs.items():
        info_parts.append(f"{k}={v}")
    
    logger.info(' '.join(info_parts))


def log_error_with_context(error: Exception, context: str = None, **kwargs):
    """
    记录错误信息及上下文
    
    Args:
        error: 异常对象
        context: 错误上下文
        **kwargs: 额外信息
    """
    logger = get_logger('error')
    error_info = [f"❌ 错误: {str(error)}"]
    if context:
        error_info.append(f"上下文: {context}")
    for k, v in kwargs.items():
        error_info.append(f"{k}={v}")
    
    logger.error(' | '.join(error_info), exc_info=True)


class LoggerMixin:
    """
    日志器混入类，为类提供统一的日志功能
    """
    
    @property
    def logger(self):
        """获取类专用的日志器"""
        if not hasattr(self, '_logger'):
            self._logger = get_logger(self.__class__.__name__)
        return self._logger
    
    def log_method_call(self, method_name: str, **kwargs):
        """记录方法调用"""
        params = ', '.join([f'{k}={v}' for k, v in kwargs.items()])
        self.logger.info(f"🔧 {self.__class__.__name__}.{method_name}({params})")
    
    def log_method_result(self, method_name: str, result_type: str, count: int = None):
        """记录方法结果"""
        result_info = f"✅ {self.__class__.__name__}.{method_name} -> {result_type}"
        if count is not None:
            result_info += f" ({count}条记录)"
        self.logger.info(result_info)
    
    def log_method_error(self, method_name: str, error: Exception):
        """记录方法错误"""
        self.logger.error(f"❌ {self.__class__.__name__}.{method_name} 失败: {str(error)}", exc_info=True)


# 初始化统一日志配置
def init_project_logging():
    """初始化项目日志配置"""
    setup_unified_logger(
        log_file='stock_analysis.log',
        max_bytes=50 * 1024 * 1024,  # 50MB
        backup_count=5,
        console_level=logging.INFO,
        file_level=logging.DEBUG
    )


# 如果直接运行此模块，则初始化日志配置
if __name__ == "__main__":
    init_project_logging()
    
    # 测试日志功能
    logger = get_logger(__name__)
    logger.info("🧪 测试统一日志配置")
    log_function_call("test_function", param1="value1", param2=123)
    log_performance("test_performance", 1.23, records=100, success=True)
    log_data_operation("SELECT", table="stock_data", records=500, condition="ts_code='000001.SZ'")
    
    # 测试混入类
    class TestClass(LoggerMixin):
        def test_method(self):
            self.log_method_call("test_method", param="test")
            self.log_method_result("test_method", "DataFrame", 10)
    
    test_obj = TestClass()
    test_obj.test_method()
    
    logger.info("🎉 统一日志配置测试完成")
