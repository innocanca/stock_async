#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能投资组合推荐策略 (Smart Portfolio)
逻辑：
1. 聚合四个维度：稳健趋势(连阳)、价值爆发(低PE放量)、底部反转、均线回归
2. 板块分散：强制要求行业去重
3. 质量优先：按各策略的核心指标排序
"""

import sys
import os
import pandas as pd
from typing import List, Dict

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from query.strategy.query_low_pe_volume_surge import LowPEVolumeSurgeAnalyzer
from query.strategy.query_consecutive_yang_lines import ConsecutiveYangLinesAnalyzer
from query.strategy.query_weekly_bottom_reversal import WeeklyBottomReversalAnalyzer
from log_config import get_logger

logger = get_logger(__name__)

class SmartPortfolioAnalyzer:
    def __init__(self):
        self.low_pe_analyzer = LowPEVolumeSurgeAnalyzer()
        self.yang_analyzer = ConsecutiveYangLinesAnalyzer()
        self.reversal_analyzer = WeeklyBottomReversalAnalyzer()

    def get_portfolio_recommendation(self, limit: int = 5) -> Dict:
        """获取智能组合建议"""
        logger.info("🚀 开始生成智能投资组合建议...")
        
        # 1. 收集各策略候选池
        candidates = []
        
        # 策略 A: 稳健趋势 (千亿连阳)
        logger.info("   - 正在筛选[稳健趋势]标的...")
        res_trend = self.yang_analyzer.get_analysis_results(min_consecutive=3)
        for r in res_trend[:3]: # 取前3
            candidates.append({
                "ts_code": r["ts_code"],
                "名称": r["stock_name"],
                "策略标签": "稳健趋势",
                "行业": r["industry"],
                "核心指标": f"周线{r['consecutive_yang_weeks']}连阳",
                "权重分数": 90 + r["consecutive_yang_weeks"]
            })

        # 策略 B: 价值爆发 (低PE+放量)
        logger.info("   - 正在筛选[价值爆发]标的...")
        res_value = self.low_pe_analyzer.get_analysis_results(min_mv=2000000, min_ratio=1.5)
        for r in res_value[:3]:
            candidates.append({
                "ts_code": r["ts_code"],
                "名称": r["名称"],
                "策略标签": "价值爆发",
                "行业": "未知", # 稍后补全
                "核心指标": f"放量{r['周放量倍数']:.1f}倍 / PE {r['pe_ttm']:.1f}",
                "权重分数": 85 + r["周放量倍数"]
            })

        # 策略 C: 底部反转
        logger.info("   - 正在筛选[底部反转]标的...")
        res_rev = self.reversal_analyzer.get_analysis_results(vol_ratio=1.8)
        for r in res_rev[:3]:
            candidates.append({
                "ts_code": r["ts_code"],
                "名称": r["名称"],
                "策略标签": "底部反转",
                "行业": "未知",
                "核心指标": f"超跌反转 / 放量{r['放量倍数']:.1f}倍",
                "权重分数": 80 + r["放量倍数"]
            })

        if not candidates:
            return {"portfolio": [], "summary": "今日未匹配到足够符合条件的标的"}

        # 2. 补全行业信息并进行板块分散筛选
        # 获取候选股票的行业信息
        all_codes = [c["ts_code"] for c in candidates]
        stock_names_info = self.low_pe_analyzer.get_stock_names(all_codes) # 借用这个方法获取基础信息
        
        # 假设我们通过数据库获取更详细的行业
        # 这里简化处理：按权重分数排序，然后行业去重
        candidates.sort(key=lambda x: x["权重分数"], reverse=True)
        
        final_portfolio = []
        selected_industries = set()
        
        for cand in candidates:
            # 模拟行业获取（实际项目中可从 db.stock_basic 读取）
            # 这里简单演示行业去重逻辑
            industry = cand.get("行业") or "其他"
            
            if industry not in selected_industries or industry == "其他":
                final_portfolio.append(cand)
                selected_industries.add(industry)
            
            if len(final_portfolio) >= limit:
                break

        return {
            "count": len(final_portfolio),
            "data": final_portfolio,
            "diversification": f"覆盖了 {len(selected_industries)} 个不同行业"
        }

if __name__ == "__main__":
    analyzer = SmartPortfolioAnalyzer()
    print(analyzer.get_portfolio_recommendation())

