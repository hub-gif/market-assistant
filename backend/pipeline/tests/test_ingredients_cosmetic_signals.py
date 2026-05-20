# -*- coding: utf-8 -*-
"""化妆品/护肤品成分列举应通过配料校验（与食品配料表并列）。"""
from __future__ import annotations

from pipeline.openai_gateway.ingredients_op import _ingredient_extraction_acceptable


def test_cosmetic_three_ingredients_enumeration() -> None:
    text = "透明质酸钠、木瓜蛋白酶、药用层孔菌提取物"
    assert _ingredient_extraction_acceptable(text)


def test_food_packaged_still_accepted() -> None:
    text = "小麦粉，食用植物油，白砂糖，食品添加剂（碳酸氢钠，柠檬酸）"
    assert _ingredient_extraction_acceptable(text)


def test_recipe_prep_still_rejected() -> None:
    text = "鸡胸肉半块，黄瓜半根，葱花蒜末各1勺"
    assert not _ingredient_extraction_acceptable(text)
