#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI 应用入口，汇总所有 API 路由。
"""

from fastapi import FastAPI

from .stock import router as stock_router

app = FastAPI(
    title="Stock Analyzer API",
    description="股票/ETF 等量化筛选 HTTP 接口",
    version="1.0.0",
)

# 注册各模块路由
app.include_router(stock_router)



