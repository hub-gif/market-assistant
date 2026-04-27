"""在收集本目录测试模块之前初始化 Django，避免 ``import pipeline.jd.runner`` 触发 AppRegistryNotReady。"""
from __future__ import annotations

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402

django.setup()
