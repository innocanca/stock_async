#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日财经新闻推送脚本

功能：
1. 爬取同花顺、东方财富等财经新闻
2. 使用AI大模型进行新闻总结
3. 通过企业微信推送每日新闻摘要
4. 支持多种总结风格和推送格式

使用方法：
python3 daily_news_push.py [选项]

选项：
--summary-type     总结类型 (brief/detailed/investment，默认brief)
--ai-provider      AI提供商 (openai/qianwen/ollama，默认qianwen)
--api-key          AI API密钥
--limit            每个来源的新闻数量限制（默认15）
--test-mode        测试模式（不推送消息，仅显示结果）

示例：
python3 daily_news_push.py                                    # 默认简洁总结并推送
python3 daily_news_push.py --summary-type detailed            # 详细分析总结
python3 daily_news_push.py --test-mode                        # 测试模式
python3 daily_news_push.py --ai-provider ollama --test-mode   # 使用本地大模型测试
"""
import os
# 添加父目录到Python路径，以便导入database和fetcher模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import logging
import sys
from datetime import datetime
from news_crawler import FinanceNewsCrawler
from ai_summarizer import AISummarizer
from send_msg import send_markdown_message, send_robot_message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_news_push.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def format_news_for_wechat(report: dict, include_raw_news: bool = False) -> str:
    """
    格式化新闻摘要用于企业微信推送
    
    Args:
        report: 新闻摘要报告
        include_raw_news: 是否包含原始新闻列表
        
    Returns:
        str: 格式化的markdown文本
    """
    markdown_text = f"""# {report['title']}

{report['ai_summary']}

## 📊 数据统计
- **新闻总数**: {report['news_count']} 条
- **爬取时间**: {report['crawl_time']}
- **总结时间**: {report['summary_time']}

## 📈 新闻来源
"""
    
    for source, count in report['source_distribution'].items():
        markdown_text += f"- **{source}**: {count} 条\n"
    
    if include_raw_news and report['raw_news']:
        markdown_text += "\n## 📰 新闻详情\n"
        for i, news in enumerate(report['raw_news'][:10], 1):  # 最多显示10条
            title = news.get('title', '')
            source = news.get('source', '')
            pub_time = news.get('pub_time', '')
            markdown_text += f"{i}. **[{source}]** {title}\n"
            if pub_time:
                markdown_text += f"   *时间: {pub_time}*\n"
            markdown_text += "\n"
        
        if len(report['raw_news']) > 10:
            markdown_text += f"... 还有 {len(report['raw_news']) - 10} 条新闻\n"
    
    markdown_text += f"\n---\n💡 *数据来源：同花顺、东方财富等财经网站*\n"
    markdown_text += f"🤖 *AI总结：智能分析每日财经动态*"
    
    return markdown_text


def run_daily_news_push(ai_provider: str = "qianwen", api_key: str = None,
                       summary_type: str = "brief", limit_per_source: int = 15,
                       test_mode: bool = False) -> dict:
    """
    执行每日新闻推送
    
    Args:
        ai_provider: AI提供商
        api_key: API密钥
        summary_type: 总结类型
        limit_per_source: 每个来源的新闻数量
        test_mode: 是否为测试模式
        
    Returns:
        dict: 执行结果统计
    """
    stats = {
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None,
        'crawl_success': False,
        'summary_success': False,
        'push_success': False,
        'news_count': 0,
        'error_message': None
    }
    
    try:
        logger.info("🚀 开始每日财经新闻推送任务...")
        logger.info("=" * 70)
        
        # 1. 爬取新闻
        logger.info("📰 第一步：爬取财经新闻...")
        crawler = FinanceNewsCrawler()
        news_data = crawler.get_daily_finance_news(limit_per_source=limit_per_source)
        
        if news_data['total_count'] > 0:
            stats['crawl_success'] = True
            stats['news_count'] = news_data['total_count']
            logger.info(f"✅ 新闻爬取成功，获得 {stats['news_count']} 条新闻")
        else:
            logger.error("❌ 新闻爬取失败，没有获取到新闻数据")
            stats['error_message'] = "新闻爬取失败"
            return stats
        
        # 2. AI总结
        logger.info("🤖 第二步：AI新闻总结...")
        summarizer = AISummarizer(api_provider=ai_provider, api_key=api_key)
        report = summarizer.create_news_digest(news_data, summary_type)
        
        if report and report.get('ai_summary'):
            stats['summary_success'] = True
            logger.info("✅ AI新闻总结成功")
        else:
            logger.warning("⚠️ AI总结失败，使用简单总结")
            stats['summary_success'] = False
        
        # 3. 格式化和推送
        logger.info("📱 第三步：格式化和推送消息...")
        
        if test_mode:
            # 测试模式：仅打印结果，不实际推送
            logger.info("🧪 测试模式：显示推送内容预览...")
            markdown_content = format_news_for_wechat(report, include_raw_news=True)
            
            print("\n" + "="*70)
            print("📱 企业微信推送内容预览：")
            print("="*70)
            print(markdown_content)
            print("="*70)
            
            stats['push_success'] = True
            logger.info("✅ 测试模式执行完成")
            
        else:
            # 正式推送
            try:
                markdown_content = format_news_for_wechat(report, include_raw_news=False)
                
                # 推送到企业微信
                result = send_markdown_message(markdown_content)
                
                if result:
                    stats['push_success'] = True
                    logger.info("✅ 企业微信推送成功")
                else:
                    logger.error("❌ 企业微信推送失败")
                    
            except Exception as e:
                logger.error(f"❌ 消息推送失败: {e}")
                stats['error_message'] = f"推送失败: {e}"
        
    except Exception as e:
        logger.error(f"❌ 每日新闻推送任务执行失败: {e}")
        stats['error_message'] = str(e)
    
    finally:
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
    
    return stats


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='每日财经新闻推送')
    parser.add_argument('--summary-type', choices=['brief', 'detailed', 'investment'], 
                       default='brief', help='总结类型（默认简洁）')
    parser.add_argument('--ai-provider', choices=['openai', 'qianwen', 'ollama'], 
                       default='qianwen', help='AI提供商（默认通义千问）')
    parser.add_argument('--api-key', help='AI API密钥')
    parser.add_argument('--limit', type=int, default=15, help='每个来源的新闻数量限制（默认15）')
    parser.add_argument('--test-mode', action='store_true', help='测试模式（不实际推送）')
    
    args = parser.parse_args()
    
    logger.info("🚀 每日财经新闻推送系统")
    logger.info("=" * 70)
    
    logger.info(f"⚙️ 配置参数：")
    logger.info(f"   总结类型: {args.summary_type}")
    logger.info(f"   AI提供商: {args.ai_provider}")
    logger.info(f"   新闻数量: 每源{args.limit}条")
    logger.info(f"   测试模式: {'是' if args.test_mode else '否'}")
    
    if args.ai_provider != 'ollama' and not args.api_key:
        logger.warning("⚠️ 未提供API密钥，可能影响AI总结功能")
    
    start_time = datetime.now()
    
    try:
        # 执行每日新闻推送任务
        stats = run_daily_news_push(
            ai_provider=args.ai_provider,
            api_key=args.api_key,
            summary_type=args.summary_type,
            limit_per_source=args.limit,
            test_mode=args.test_mode
        )
        
        # 显示执行结果
        logger.info("\n" + "📊 任务执行统计".center(70, "="))
        logger.info(f"   📰 新闻爬取: {'成功' if stats['crawl_success'] else '失败'}")
        logger.info(f"   🤖 AI总结: {'成功' if stats['summary_success'] else '失败'}")
        logger.info(f"   📱 消息推送: {'成功' if stats['push_success'] else '失败'}")
        logger.info(f"   📊 新闻数量: {stats['news_count']} 条")
        logger.info(f"   ⏱️ 总耗时: {stats['duration']}")
        
        if stats['error_message']:
            logger.error(f"   ❌ 错误信息: {stats['error_message']}")
        
        # 判断整体是否成功
        overall_success = stats['crawl_success'] and (stats['summary_success'] or test_mode) and stats['push_success']
        
        if overall_success:
            logger.info("🎉 每日财经新闻推送任务执行成功！")
            return True
        else:
            logger.error("❌ 每日财经新闻推送任务执行失败")
            return False
        
    except KeyboardInterrupt:
        logger.warning("⚠️ 用户中断程序执行")
        return False
    except Exception as e:
        logger.error(f"❌ 程序执行出现异常: {e}")
        return False
    finally:
        end_time = datetime.now()
        total_duration = end_time - start_time
        logger.info(f"\n⏰ 程序总执行时间: {total_duration}")


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        sys.exit(1)
