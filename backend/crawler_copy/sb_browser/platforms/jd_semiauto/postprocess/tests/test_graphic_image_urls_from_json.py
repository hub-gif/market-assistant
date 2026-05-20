# -*- coding: utf-8 -*-
"""从半自动 graphic 落盘 JSON 解析详情长图 URL，并可选用盘后同款 LLM 识配料。

用法（``cwd`` 任意，脚本会补 ``crawler_copy`` / ``backend`` 到 ``sys.path``）::

  python test_graphic_image_urls_from_json.py --json path/to/jd_graphic_0001_....json
  python test_graphic_image_urls_from_json.py --json path/to/....json --recognize
  python test_graphic_image_urls_from_json.py --run-dir path/to/20260519_181206_低GI --recognize

  pytest test_graphic_image_urls_from_json.py -s --json path/to/....json --recognize
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# postprocess/tests -> crawler_copy
_CRAWLER_COPY = Path(__file__).resolve().parents[5]
_BACKEND = _CRAWLER_COPY.parent
_MA_ROOT = _BACKEND.parent
for _p in (_CRAWLER_COPY, _BACKEND):
    if _p.is_dir() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))


def _load_market_assistant_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_path = _MA_ROOT / ".env"
    if env_path.is_file():
        load_dotenv(env_path)

from sb_browser.platforms.jd_semiauto.postprocess.capture_dedupe_key import (  # noqa: E402
    _detail_sku_id,
)
from sb_browser.platforms.jd_semiauto.postprocess.semiauto_detail_ingredients import (  # noqa: E402
    recognize_detail_ingredients_with_urls_joined,
    sku_to_graphic_urls_joined_from_run_dir,
    urls_joined_from_graphic_content_html,
)
from pipeline.openai_gateway.ingredients_op import parse_joined_image_urls  # noqa: E402


def _graphic_content_from_blob(blob: dict) -> str:
    parsed = blob.get("parsed")
    if not isinstance(parsed, dict):
        return ""
    data = parsed.get("data")
    if not isinstance(data, dict):
        return ""
    return str(data.get("graphicContent") or "")


def graphic_urls_joined_from_capture_blob(blob: dict) -> str:
    """与盘后单文件一致：graphic ``parsed`` → ``\"; \"`` 拼接 URL 串。"""
    from sb_browser.platforms.jd_semiauto.postprocess.semiauto_detail_ingredients import (  # noqa: WPS433
        urls_joined_from_graphic_parsed,
    )

    joined = urls_joined_from_graphic_parsed(blob.get("parsed"))
    if joined:
        return joined
    return urls_joined_from_graphic_content_html(_graphic_content_from_blob(blob))


def graphic_image_urls_from_capture_blob(blob: dict) -> list[str]:
    """``graphicContent`` → joined → ``parse_joined_image_urls``。"""
    return parse_joined_image_urls(graphic_urls_joined_from_capture_blob(blob))


def _print_urls(*, sku: str, source: str, urls: list[str]) -> None:
    print(f"sku={sku or '?'}  source={source}  count={len(urls)}")
    for i, u in enumerate(urls, start=1):
        print(f"  [{i}] {u}")
    print()


def _print_ingredients(*, sku: str, text: str, source_url: str) -> None:
    print(f"--- 配料识别 sku={sku or '?'} ---")
    print(f"source_url={source_url or '(无)'}")
    print(text or "(空)")
    print()


def recognize_ingredients_like_postprocess(urls_joined: str) -> tuple[str, str]:
    """与 ``postprocess_semiauto_capture_json_dirs`` 相同：``recognize_detail_ingredients_with_urls_joined``。"""
    _load_market_assistant_dotenv()
    return recognize_detail_ingredients_with_urls_joined(urls_joined)


def run_json_file(json_path: Path, *, recognize: bool = False) -> list[str]:
    blob = json.loads(json_path.expanduser().resolve().read_text(encoding="utf-8"))
    sku = _detail_sku_id(
        str(blob.get("resolved_sku") or ""),
        str(blob.get("url") or ""),
        blob.get("parsed"),
    )
    joined = graphic_urls_joined_from_capture_blob(blob)
    urls = parse_joined_image_urls(joined)
    _print_urls(sku=sku, source=str(json_path), urls=urls)
    if recognize:
        text, src = recognize_ingredients_like_postprocess(joined)
        _print_ingredients(sku=sku, text=text, source_url=src)
    return urls


def run_run_dir(run_dir: Path, *, recognize: bool = False) -> dict[str, list[str]]:
    rd = run_dir.expanduser().resolve()
    joined_map = sku_to_graphic_urls_joined_from_run_dir(rd)
    out: dict[str, list[str]] = {}
    for sku, joined in sorted(joined_map.items()):
        urls = parse_joined_image_urls(joined)
        out[sku] = urls
        _print_urls(sku=sku, source=f"{rd / 'graphic'}", urls=urls)
        if recognize:
            text, src = recognize_ingredients_like_postprocess(joined)
            _print_ingredients(sku=sku, text=text, source_url=src)
    if not out:
        print(f"[warn] {rd / 'graphic'} 下未解析到任何 SKU 长图 URL", file=sys.stderr)
    return out


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="从 graphic JSON 解析详情长图 URL（生产同源）")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--json", type=Path, help="单个 jd_graphic_*.json")
    g.add_argument("--run-dir", type=Path, help="半自动 run_dir（扫描 graphic/）")
    p.add_argument(
        "--recognize",
        action="store_true",
        help="解析 URL 后调用 recognize_detail_ingredients_with_urls_joined（需 .env 多模态 API）",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.recognize:
        _load_market_assistant_dotenv()
    if args.json is not None:
        if not args.json.is_file():
            print(f"文件不存在: {args.json}", file=sys.stderr)
            return 2
        run_json_file(args.json, recognize=bool(args.recognize))
        return 0
    if not args.run_dir.is_dir():
        print(f"目录不存在: {args.run_dir}", file=sys.stderr)
        return 2
    run_run_dir(args.run_dir, recognize=bool(args.recognize))
    return 0


def pytest_addoption(parser) -> None:
    parser.addoption("--json", action="store", default=None, help="graphic 落盘 JSON 路径")
    parser.addoption("--run-dir", action="store", default=None, help="半自动 run_dir")
    parser.addoption(
        "--recognize",
        action="store_true",
        default=False,
        help="调用 LLM 识配料（需 API）",
    )


def test_graphic_urls_from_json_cli_json(request) -> None:
    """pytest：``pytest ... -s --json path/to/jd_graphic_xxx.json [--recognize]``"""
    import pytest

    opt = request.config.getoption("--json", default=None)
    if not opt:
        pytest.skip("未传 --json，跳过（示例: pytest -s --json path/to/jd_graphic_xxx.json）")
    recognize = bool(request.config.getoption("--recognize", default=False))
    urls = run_json_file(Path(opt), recognize=recognize)
    assert isinstance(urls, list)


def test_urls_joined_from_css_background_and_zbview_value() -> None:
    """化妆品等类目：graphicContent 多为 CSS background-image，非 data-lazyload。"""
    html = (
        '<div id="zbViewWeChatMiniImages" value="/sku/jfs/t1/a/00d62ee328f7ff5c.jpg,'
        '/sku/jfs/t1/b/00d62ee1462b8121.jpg"></div>'
        '<style>.x{background-image:url(//img30.360buyimg.com/sku/jfs/t1/c/00d62ee55f678b69.jpg)}</style>'
    )
    joined = urls_joined_from_graphic_content_html(html)
    urls = parse_joined_image_urls(joined)
    assert len(urls) >= 3
    assert any("360buyimg.com" in u for u in urls)


def test_graphic_urls_from_json_cli_run_dir(request) -> None:
    import pytest

    opt = request.config.getoption("--run-dir", default=None)
    if not opt:
        pytest.skip("未传 --run-dir，跳过")
    recognize = bool(request.config.getoption("--recognize", default=False))
    m = run_run_dir(Path(opt), recognize=recognize)
    assert isinstance(m, dict)


if __name__ == "__main__":
    raise SystemExit(main())
