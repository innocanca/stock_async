#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
财经新闻爬虫模块

功能：
1. 从同花顺财经新闻抓取每日新闻
2. 从东方财富财经新闻抓取每日新闻
3. 新闻数据清洗和处理
4. 支持多线程并发抓取

支持网站：
- 同花顺财经：https://news.10jqka.com.cn/
- 东方财富：https://finance.eastmoney.com/news/
"""

import requests
import logging
from bs4 import BeautifulSoup
from typing import List, Dict
from datetime import datetime
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FinanceNewsCrawler:
    """财经新闻爬虫类"""
    
    def __init__(self):
        """初始化爬虫"""
        self.session = requests.Session()
        # 设置请求头，模拟浏览器访问
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
    def crawl_tonghuashun_news(self, limit: int = 20) -> List[Dict]:
        """
        爬取同花顺财经新闻
        
        Args:
            limit: 获取新闻数量限制
            
        Returns:
            List[Dict]: 新闻列表，每个新闻包含标题、链接、时间等信息
        """
        news_list = []
        
        try:
            logger.info("🔍 开始爬取同花顺财经新闻...")
            
            # 同花顺财经新闻页面
            urls = [
                'https://news.10jqka.com.cn/cjzx_list/',  # 财经资讯
                'https://news.10jqka.com.cn/stock_list/',  # 个股新闻
                'https://news.10jqka.com.cn/market_list/'  # 市场新闻
            ]
            
            for url in urls:
                try:
                    response = self.session.get(url, timeout=10)
                    response.raise_for_status()
                    response.encoding = 'utf-8'
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 解析新闻列表
                    news_items = soup.find_all('div', class_='list-item') or soup.find_all('li', class_='item')
                    
                    for item in news_items[:limit//len(urls)]:
                        try:
                            # 提取标题和链接
                            title_link = item.find('a')
                            if title_link:
                                title = title_link.get_text(strip=True)
                                link = title_link.get('href', '')
                                
                                # 补全链接
                                if link.startswith('/'):
                                    link = 'https://news.10jqka.com.cn' + link
                                elif not link.startswith('http'):
                                    continue
                                
                                # 提取时间
                                time_elem = item.find('span', class_='time') or item.find('em')
                                pub_time = time_elem.get_text(strip=True) if time_elem else datetime.now().strftime('%Y-%m-%d %H:%M')
                                
                                news_item = {
                                    'title': title,
                                    'link': link,
                                    'source': '同花顺',
                                    'pub_time': pub_time,
                                    'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                                
                                news_list.append(news_item)
                                
                        except Exception as e:
                            logger.debug(f"解析单条同花顺新闻失败: {e}")
                            continue
                            
                    time.sleep(1)  # 防止请求过快
                    
                except Exception as e:
                    logger.warning(f"爬取同花顺URL {url} 失败: {e}")
                    continue
            
            logger.info(f"✅ 成功爬取同花顺新闻 {len(news_list)} 条")
            
        except Exception as e:
            logger.error(f"❌ 爬取同花顺新闻失败: {e}")
        
        return news_list
    
    def crawl_eastmoney_news(self, limit: int = 20) -> List[Dict]:
        """
        爬取东方财富财经新闻
        
        Args:
            limit: 获取新闻数量限制
            
        Returns:
            List[Dict]: 新闻列表
        """
        news_list = []
        
        try:
            logger.info("🔍 开始爬取东方财富财经新闻...")
            
            # 东方财富新闻页面
            urls = [
                'https://finance.eastmoney.com/news/cjxw.html',  # 财经新闻
                'https://finance.eastmoney.com/news/cgsxw.html',  # 个股新闻
                'https://finance.eastmoney.com/news/cschxw.html'  # 市场新闻
            ]
            
            for url in urls:
                try:
                    response = self.session.get(url, timeout=10)
                    response.raise_for_status()
                    response.encoding = 'utf-8'
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 解析新闻列表
                    news_items = soup.find_all('div', class_='text-line') or soup.find_all('li')
                    
                    for item in news_items[:limit//len(urls)]:
                        try:
                            # 提取标题和链接
                            title_link = item.find('a')
                            if title_link:
                                title = title_link.get_text(strip=True)
                                link = title_link.get('href', '')
                                
                                # 补全链接
                                if link.startswith('/'):
                                    link = 'https://finance.eastmoney.com' + link
                                elif not link.startswith('http'):
                                    continue
                                
                                # 提取时间
                                time_elem = item.find('span', class_='time') or item.find('span', class_='date')
                                pub_time = time_elem.get_text(strip=True) if time_elem else datetime.now().strftime('%Y-%m-%d %H:%M')
                                
                                news_item = {
                                    'title': title,
                                    'link': link,
                                    'source': '东方财富',
                                    'pub_time': pub_time,
                                    'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                                
                                news_list.append(news_item)
                                
                        except Exception as e:
                            logger.debug(f"解析单条东方财富新闻失败: {e}")
                            continue
                            
                    time.sleep(1)  # 防止请求过快
                    
                except Exception as e:
                    logger.warning(f"爬取东方财富URL {url} 失败: {e}")
                    continue
            
            logger.info(f"✅ 成功爬取东方财富新闻 {len(news_list)} 条")
            
        except Exception as e:
            logger.error(f"❌ 爬取东方财富新闻失败: {e}")
        
        return news_list
    
    def crawl_sina_finance_news(self, limit: int = 15) -> List[Dict]:
        """
        爬取新浪财经新闻（备用数据源）
        
        Args:
            limit: 获取新闻数量限制
            
        Returns:
            List[Dict]: 新闻列表
        """
        news_list = []
        
        try:
            logger.info("🔍 开始爬取新浪财经新闻...")
            
            url = 'https://finance.sina.com.cn/'
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找新闻链接
            news_links = soup.find_all('a', href=True)
            
            count = 0
            for link in news_links:
                if count >= limit:
                    break
                    
                try:
                    href = link.get('href', '')
                    title = link.get_text(strip=True)
                    
                    # 筛选财经相关新闻
                    if ('finance.sina.com.cn' in href or 'stock.finance.sina.com.cn' in href) and \
                       len(title) > 10 and any(keyword in title for keyword in ['股票', '股市', '财经', '投资', '市场', '经济']):
                        
                        news_item = {
                            'title': title,
                            'link': href,
                            'source': '新浪财经',
                            'pub_time': datetime.now().strftime('%Y-%m-%d %H:%M'),
                            'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        
                        news_list.append(news_item)
                        count += 1
                        
                except Exception as e:
                    logger.debug(f"解析新浪财经新闻失败: {e}")
                    continue
            
            logger.info(f"✅ 成功爬取新浪财经新闻 {len(news_list)} 条")
            
        except Exception as e:
            logger.error(f"❌ 爬取新浪财经新闻失败: {e}")
        
        return news_list
    
    def get_all_finance_news(self, limit_per_source: int = 20) -> List[Dict]:
        """
        获取所有财经新闻（多线程并发爬取）
        
        Args:
            limit_per_source: 每个数据源的新闻数量限制
            
        Returns:
            List[Dict]: 合并后的新闻列表
        """
        logger.info("🚀 开始并发爬取所有财经新闻...")
        
        all_news = []
        
        # 使用线程池并发爬取
        with ThreadPoolExecutor(max_workers=3) as executor:
            # 提交爬取任务
            futures = {
                executor.submit(self.crawl_tonghuashun_news, limit_per_source): '同花顺',
                executor.submit(self.crawl_eastmoney_news, limit_per_source): '东方财富',
                executor.submit(self.crawl_sina_finance_news, limit_per_source): '新浪财经'
            }
            
            # 收集结果
            for future in as_completed(futures):
                source_name = futures[future]
                try:
                    news_data = future.result()
                    if news_data:
                        all_news.extend(news_data)
                        logger.info(f"✅ {source_name} 爬取完成，获得 {len(news_data)} 条新闻")
                    else:
                        logger.warning(f"⚠️ {source_name} 未获取到新闻数据")
                        
                except Exception as e:
                    logger.error(f"❌ {source_name} 爬取失败: {e}")
        
        # 去重处理（基于标题）
        seen_titles = set()
        unique_news = []
        
        for news in all_news:
            title = news.get('title', '')
            if title not in seen_titles and len(title) > 5:
                seen_titles.add(title)
                unique_news.append(news)
        
        logger.info(f"🎉 财经新闻爬取完成！")
        logger.info(f"   📊 原始新闻: {len(all_news)} 条")
        logger.info(f"   📊 去重后新闻: {len(unique_news)} 条")
        
        # 按来源统计
        source_counts = {}
        for news in unique_news:
            source = news.get('source', '未知')
            source_counts[source] = source_counts.get(source, 0) + 1
        
        logger.info("📈 新闻来源分布：")
        for source, count in source_counts.items():
            logger.info(f"   {source}: {count} 条")
        
        return unique_news
    
    def filter_stock_related_news(self, news_list: List[Dict], 
                                 stock_keywords: List[str] = None) -> List[Dict]:
        """
        筛选股票相关新闻
        
        Args:
            news_list: 新闻列表
            stock_keywords: 股票相关关键词列表
            
        Returns:
            List[Dict]: 筛选后的新闻列表
        """
        if stock_keywords is None:
            stock_keywords = [
                '股票', '股市', 'A股', '港股', '沪深', '上证', '深证', '创业板', '科创板',
                '涨停', '跌停', '牛市', '熊市', 'IPO', '重组', '并购', '分红', '配股',
                '机构', '基金', '券商', '银行', '保险', '地产', '科技', '医药', '新能源',
                '芯片', '人工智能', '5G', '新能源汽车', '锂电池', '光伏', '风电'
            ]
        
        filtered_news = []
        
        for news in news_list:
            title = news.get('title', '')
            # 检查标题是否包含股票相关关键词
            if any(keyword in title for keyword in stock_keywords):
                filtered_news.append(news)
        
        logger.info(f"📊 股票相关新闻筛选完成：{len(filtered_news)}/{len(news_list)} 条")
        
        return filtered_news
    
    def get_news_content(self, news_item: Dict) -> str:
        """
        获取新闻详细内容（可选功能）
        
        Args:
            news_item: 新闻项目
            
        Returns:
            str: 新闻内容
        """
        try:
            url = news_item.get('link', '')
            if not url:
                return ""
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 尝试多种可能的内容选择器
            content_selectors = [
                '.article-content',
                '.content',
                '.news-content', 
                '#artibody',
                '.article-body',
                'div[class*="content"]'
            ]
            
            content = ""
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    content = content_elem.get_text(strip=True)
                    break
            
            # 如果找不到内容，使用标题
            if not content:
                content = news_item.get('title', '')
            
            return content[:500]  # 限制内容长度
            
        except Exception as e:
            logger.debug(f"获取新闻内容失败: {e}")
            return news_item.get('title', '')
    
    def format_news_for_summary(self, news_list: List[Dict]) -> str:
        """
        格式化新闻用于大模型总结
        
        Args:
            news_list: 新闻列表
            
        Returns:
            str: 格式化后的新闻文本
        """
        if not news_list:
            return "今日暂无财经股票相关新闻。"
        
        formatted_text = f"今日财经股票新闻汇总（共{len(news_list)}条）：\n\n"
        
        for i, news in enumerate(news_list, 1):
            title = news.get('title', '')
            source = news.get('source', '')
            pub_time = news.get('pub_time', '')
            
            formatted_text += f"{i}. 【{source}】{title}\n"
            if pub_time:
                formatted_text += f"   时间：{pub_time}\n"
            formatted_text += "\n"
        
        return formatted_text
    
    def get_daily_finance_news(self, limit_per_source: int = 15, 
                              filter_stock: bool = True) -> Dict:
        """
        获取每日财经新闻汇总
        
        Args:
            limit_per_source: 每个数据源的新闻数量
            filter_stock: 是否只筛选股票相关新闻
            
        Returns:
            Dict: 包含新闻列表和汇总信息的字典
        """
        start_time = datetime.now()
        
        try:
            # 获取所有新闻
            all_news = self.get_all_finance_news(limit_per_source)
            
            # 筛选股票相关新闻
            if filter_stock:
                filtered_news = self.filter_stock_related_news(all_news)
            else:
                filtered_news = all_news
            
            # 格式化新闻
            formatted_text = self.format_news_for_summary(filtered_news)
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            result = {
                'news_list': filtered_news,
                'formatted_text': formatted_text,
                'total_count': len(filtered_news),
                'source_distribution': {},
                'crawl_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'duration': str(duration)
            }
            
            # 统计各来源数量
            for news in filtered_news:
                source = news.get('source', '未知')
                result['source_distribution'][source] = result['source_distribution'].get(source, 0) + 1
            
            logger.info("🎉 每日财经新闻获取完成！")
            logger.info(f"   📊 新闻总数: {result['total_count']} 条")
            logger.info(f"   ⏱️ 爬取耗时: {duration}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 获取每日财经新闻失败: {e}")
            return {
                'news_list': [],
                'formatted_text': "今日财经新闻获取失败，请稍后重试。",
                'total_count': 0,
                'source_distribution': {},
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'duration': '0',
                'error': str(e)
            }
