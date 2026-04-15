"""商详 JSON → 购买者视角优惠摘要（规则抽取）。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase


class DetailBuyerExtractionTests(SimpleTestCase):
    def test_extract_minimal_dict(self) -> None:
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        dr = root / "detail"
        if str(dr) not in sys.path:
            sys.path.insert(0, str(dr))
        import jd_detail_buyer_extraction as be  # noqa: WPS433

        obj = {
            "warePriceGatherVO": {
                "priceItemList": [
                    {
                        "hitLine": False,
                        "price": "64.97",
                        "priceLabelList": [{"labelTxt": "到手价", "labelType": "finalPrice"}],
                        "priceType": "finalPrice",
                    },
                    {
                        "hitLine": True,
                        "price": "66",
                        "priceType": "jdPrice",
                    },
                ]
            },
            "bestPromotion": {"purchasePrice": "64.97", "canGetCoupon": []},
            "warmTipVO": {"tips": [{"tipTxt": "此商品不可使用东券", "order": 1}]},
            "promotion": {"prompt": ""},
            "rankInfoList": [{"rankName": "粗粮饼干热卖榜·第5名"}],
            "userInfo": {"newPeople": True},
            "bottomBtnVO": {
                "bottomBtnItems": [
                    {
                        "buttonStyle": {
                            "textFormat": {
                                "text": "新人到手价<span>¥64.97</span> 立即购买"
                            }
                        }
                    }
                ]
            },
            "stockInfo": {
                "promiseResult": "12:00前付款，预计今天送达",
            },
            "serviceTagsVO": {
                "basicNewIcons": [
                    {"text": "7天价保", "tip": "在下单后7天内，商品出现降价可享受价保服务。"},
                ]
            },
            "preferenceVO": {
                "againSharedLabel": [{"labelName": "最高返6京豆"}],
                "preferencePopUp": {
                    "expression": {
                        "basePrice": "66",
                        "discountDesc": "购买立减",
                        "discountAmount": "1.03",
                        "redAmount": "1.03",
                        "couponAmount": "0",
                        "promotionAmount": "0",
                        "govAmount": "",
                        "subtrahends": [
                            {
                                "topDesc": "红包",
                                "preferenceDesc": "红包抵¥1.03",
                                "preferenceAmount": "0",
                                "preferenceType": "5",
                            }
                        ],
                    },
                    "againSharedPreference": [
                        {"shortText": "新人包邮", "value": "包邮", "text": "新人包邮"}
                    ],
                },
            },
        }
        out = be.extract_buyer_offer_profile(obj)
        self.assertEqual(out.get("schema_version"), 1)
        self.assertIn("64.97", str(out.get("price_snapshot") or {}))
        dm = out.get("discount_mechanism") or {}
        self.assertEqual(dm.get("expression", {}).get("discount_desc"), "购买立减")
        self.assertTrue(dm.get("subtrahends"))
        lines = out.get("buyer_summary_lines") or []
        self.assertTrue(any("到手价" in x for x in lines))
        self.assertTrue(any("东券" in x for x in lines))
        self.assertTrue(any("购买立减" in x or "红包" in x for x in lines))

    def test_extract_from_real_file_if_present(self) -> None:
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        dr = root / "detail"
        if str(dr) not in sys.path:
            sys.path.insert(0, str(dr))
        import jd_detail_buyer_extraction as be  # noqa: WPS433

        sample = (
            Path(__file__).resolve().parents[3]
            / "data"
            / "JD"
            / "pipeline_runs"
            / "20260413_104252_低GI"
            / "detail"
            / "ware_100107873140_response.json"
        )
        if not sample.is_file():
            self.skipTest("sample ware JSON not in workspace")
        text = sample.read_text(encoding="utf-8")
        out = be.extract_buyer_offer_profile_from_json_text(text)
        self.assertEqual(out.get("schema_version"), 1)
        self.assertTrue(out.get("buyer_summary_lines"))
        # 样例中应有到手价与不可用东券提示
        blob = json.dumps(out, ensure_ascii=False)
        self.assertIn("64.97", blob)
        self.assertIn("东券", blob)
        self.assertTrue("购买立减" in blob or "红包" in blob)
