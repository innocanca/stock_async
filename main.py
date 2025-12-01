#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目入口：通过此文件启动 FastAPI 服务。

使用方法：
    python main.py
"""

import uvicorn

from api import app


def main():
    """启动股票筛选 HTTP 服务"""
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=False,
    )


if __name__ == "__main__":
    main()


