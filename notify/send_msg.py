# -*- coding: utf-8 -*-
"""
消息发送模块
支持发送markdown格式的消息到各种平台
"""

import logging
import json
from typing import Optional

logger = logging.getLogger(__name__)


def send_markdown_message(content: str, webhook_url: str = None) -> bool:
    """
    发送markdown格式消息
    
    Args:
        content: markdown内容
        webhook_url: webhook地址（如企业微信机器人）
        
    Returns:
        bool: 是否发送成功
    """
    try:
        # 如果没有配置webhook，则只打印到控制台
        if not webhook_url:
            logger.info("未配置webhook，将消息打印到控制台:")
            print("=" * 80)
            print("📱 策略提醒消息")
            print("=" * 80)
            print(content)
            print("=" * 80)
            return True
        
        # 这里可以实现实际的消息发送逻辑
        # 比如企业微信、钉钉、Slack等
        logger.info("消息发送功能待实现")
        return True
        
    except Exception as e:
        logger.error(f"发送消息失败: {e}")
        return False


def send_wechat_work_message(content: str, webhook_url: str) -> bool:
    """
    发送消息到企业微信群机器人
    
    Args:
        content: markdown内容
        webhook_url: 企业微信webhook地址
        
    Returns:
        bool: 是否发送成功
    """
    try:
        import requests
        
        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        
        response = requests.post(webhook_url, json=data)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                logger.info("企业微信消息发送成功")
                return True
            else:
                logger.error(f"企业微信API返回错误: {result}")
                return False
        else:
            logger.error(f"HTTP请求失败: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"发送企业微信消息失败: {e}")
        return False


def send_console_message(content: str) -> bool:
    """
    发送消息到控制台（用于测试）
    
    Args:
        content: 消息内容
        
    Returns:
        bool: 总是返回True
    """
    print("=" * 80)
    print("📱 策略提醒")
    print("=" * 80)
    print(content)
    print("=" * 80)
    return True
