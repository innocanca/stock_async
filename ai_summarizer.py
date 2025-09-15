#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI大模型新闻总结模块

功能：
1. 集成多种大模型API（OpenAI、Claude、通义千问等）
2. 对财经新闻进行智能总结
3. 提取关键信息和投资要点
4. 支持不同总结风格和长度

支持的大模型：
- OpenAI GPT (GPT-3.5, GPT-4)
- Anthropic Claude
- 阿里云通义千问
- 本地大模型（通过Ollama等）
"""

import logging
import requests
import json
from typing import Dict, List, Optional
from datetime import datetime

# 配置日志
logger = logging.getLogger(__name__)


class AISummarizer:
    """AI大模型新闻总结器"""
    
    def __init__(self, api_provider: str = "qianwen", api_key: str = None, 
                 api_base: str = None):
        """
        初始化AI总结器
        
        Args:
            api_provider: API提供商 ("openai", "claude", "qianwen", "ollama")
            api_key: API密钥
            api_base: API基础URL
        """
        self.api_provider = api_provider.lower()
        self.api_key = api_key
        self.api_base = api_base
        
        # 默认配置
        self._setup_default_config()
        
    def _setup_default_config(self):
        """设置默认配置"""
        self.configs = {
            "openai": {
                "api_base": "https://api.openai.com/v1",
                "model": "gpt-3.5-turbo",
                "max_tokens": 1000
            },
            "claude": {
                "api_base": "https://api.anthropic.com/v1",
                "model": "claude-3-haiku-20240307",
                "max_tokens": 1000
            },
            "qianwen": {
                "api_base": "https://dashscope.aliyuncs.com/api/v1",
                "model": "qwen-turbo",
                "max_tokens": 1000
            },
            "ollama": {
                "api_base": "http://localhost:11434/api",
                "model": "qwen2.5:7b",
                "max_tokens": 1000
            }
        }
        
        # 使用用户提供的配置覆盖默认配置
        if self.api_base:
            self.configs[self.api_provider]["api_base"] = self.api_base
    
    def _create_summary_prompt(self, news_text: str, summary_type: str = "brief") -> str:
        """
        创建总结提示词
        
        Args:
            news_text: 新闻文本
            summary_type: 总结类型 ("brief", "detailed", "investment")
            
        Returns:
            str: 提示词
        """
        prompts = {
            "brief": """请对以下财经股票新闻进行简洁总结，要求：
1. 提取3-5个最重要的财经动态
2. 突出影响股市的关键信息
3. 用简洁明了的语言，每条控制在30字以内
4. 按重要性排序

新闻内容：
{news_text}

请以以下格式输出：
🔥 今日财经要闻：
1. [关键信息1]
2. [关键信息2] 
3. [关键信息3]
...""",
            
            "detailed": """请对以下财经股票新闻进行详细分析总结，要求：
1. 分析对股市的潜在影响
2. 提取关键的经济数据和政策信息
3. 识别受影响的行业和个股
4. 提供简要的投资建议

新闻内容：
{news_text}

请以以下格式输出：
📊 今日财经深度分析：

💡 市场影响：
[分析市场整体影响]

🏢 重点行业：
[受影响的重点行业]

📈 关注个股：
[值得关注的个股]

⚠️ 风险提示：
[潜在风险点]""",
            
            "investment": """作为专业投资顾问，请分析以下财经新闻并提供投资建议：

新闻内容：
{news_text}

请提供：
1. 市场机会点分析
2. 推荐关注的板块
3. 风险警示
4. 操作建议

格式要求：简洁专业，突出可操作性"""
        }
        
        template = prompts.get(summary_type, prompts["brief"])
        return template.format(news_text=news_text)
    
    def summarize_with_openai(self, news_text: str, summary_type: str = "brief") -> Optional[str]:
        """使用OpenAI API进行总结"""
        try:
            config = self.configs["openai"]
            
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            prompt = self._create_summary_prompt(news_text, summary_type)
            
            data = {
                "model": config["model"],
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": config["max_tokens"],
                "temperature": 0.7
            }
            
            response = requests.post(
                f"{config['api_base']}/chat/completions",
                headers=headers,
                json=data,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            return result["choices"][0]["message"]["content"]
            
        except Exception as e:
            logger.error(f"OpenAI API调用失败: {e}")
            return None
    
    def summarize_with_qianwen(self, news_text: str, summary_type: str = "brief") -> Optional[str]:
        """使用通义千问API进行总结"""
        try:
            config = self.configs["qianwen"]
            
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            prompt = self._create_summary_prompt(news_text, summary_type)
            
            data = {
                "model": config["model"],
                "input": {
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                },
                "parameters": {
                    "max_tokens": config["max_tokens"],
                    "temperature": 0.7
                }
            }
            
            response = requests.post(
                f"{config['api_base']}/services/aigc/text-generation/generation",
                headers=headers,
                json=data,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            return result["output"]["text"]
            
        except Exception as e:
            logger.error(f"通义千问API调用失败: {e}")
            return None
    
    def summarize_with_ollama(self, news_text: str, summary_type: str = "brief") -> Optional[str]:
        """使用Ollama本地大模型进行总结"""
        try:
            config = self.configs["ollama"]
            
            prompt = self._create_summary_prompt(news_text, summary_type)
            
            data = {
                "model": config["model"],
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "num_predict": config["max_tokens"]
                }
            }
            
            response = requests.post(
                f"{config['api_base']}/generate",
                json=data,
                timeout=60
            )
            
            response.raise_for_status()
            result = response.json()
            
            return result.get("response", "")
            
        except Exception as e:
            logger.error(f"Ollama API调用失败: {e}")
            return None
    
    def summarize_news(self, news_text: str, summary_type: str = "brief") -> str:
        """
        使用AI大模型总结新闻
        
        Args:
            news_text: 新闻文本
            summary_type: 总结类型
            
        Returns:
            str: 总结结果
        """
        logger.info(f"🤖 开始使用{self.api_provider}进行新闻总结...")
        
        try:
            summary = None
            
            if self.api_provider == "openai":
                summary = self.summarize_with_openai(news_text, summary_type)
            elif self.api_provider == "qianwen":
                summary = self.summarize_with_qianwen(news_text, summary_type)
            elif self.api_provider == "ollama":
                summary = self.summarize_with_ollama(news_text, summary_type)
            else:
                logger.error(f"不支持的API提供商: {self.api_provider}")
                return self._fallback_summary(news_text)
            
            if summary:
                logger.info("✅ AI新闻总结完成")
                return summary
            else:
                logger.warning("⚠️ AI总结失败，使用备用总结")
                return self._fallback_summary(news_text)
                
        except Exception as e:
            logger.error(f"❌ AI新闻总结失败: {e}")
            return self._fallback_summary(news_text)
    
    def _fallback_summary(self, news_text: str) -> str:
        """
        备用总结方案（当AI API不可用时）
        
        Args:
            news_text: 新闻文本
            
        Returns:
            str: 简单总结
        """
        lines = news_text.split('\n')
        news_lines = [line.strip() for line in lines if line.strip() and '.' in line and len(line) > 10]
        
        summary = "📰 今日财经新闻摘要：\n\n"
        
        # 提取前5条最重要的新闻标题
        for i, line in enumerate(news_lines[:5], 1):
            if line.startswith(str(i)):
                summary += f"{line}\n"
            else:
                summary += f"{i}. {line}\n"
        
        summary += f"\n📊 共收集 {len(news_lines)} 条财经新闻"
        summary += f"\n⏰ 更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        return summary
    
    def create_news_digest(self, news_data: Dict, summary_type: str = "brief") -> Dict:
        """
        创建新闻摘要报告
        
        Args:
            news_data: 新闻数据字典
            summary_type: 总结类型
            
        Returns:
            Dict: 包含摘要报告的字典
        """
        logger.info("📝 开始创建新闻摘要报告...")
        
        try:
            news_text = news_data.get('formatted_text', '')
            
            # 使用AI进行总结
            ai_summary = self.summarize_news(news_text, summary_type)
            
            # 创建完整报告
            report = {
                'title': f"📰 每日财经新闻摘要 - {datetime.now().strftime('%Y年%m月%d日')}",
                'ai_summary': ai_summary,
                'news_count': news_data.get('total_count', 0),
                'source_distribution': news_data.get('source_distribution', {}),
                'crawl_time': news_data.get('crawl_time', ''),
                'summary_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'raw_news': news_data.get('news_list', [])
            }
            
            logger.info("✅ 新闻摘要报告创建完成")
            return report
            
        except Exception as e:
            logger.error(f"❌ 创建新闻摘要报告失败: {e}")
            return {
                'title': f"📰 每日财经新闻摘要 - {datetime.now().strftime('%Y年%m月%d日')}",
                'ai_summary': news_data.get('formatted_text', '今日新闻获取失败'),
                'news_count': 0,
                'source_distribution': {},
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'summary_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'raw_news': [],
                'error': str(e)
            }
