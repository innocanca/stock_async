#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI 应用入口，汇总所有 API 路由。
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .stock import router as stock_router

app = FastAPI(
    title="Stock Analyzer API",
    description="股票/ETF 等量化筛选 HTTP 接口",
    version="1.0.0",
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源，生产环境中应该限制为特定域名
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有HTTP方法
    allow_headers=["*"],  # 允许所有头部
)

# 注册各模块路由
app.include_router(stock_router)



