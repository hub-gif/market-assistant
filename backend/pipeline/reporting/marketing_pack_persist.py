"""营销内容包等产物落盘（任务 run_dir，便于归档与审计）。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def persist_marketing_detail_pack_v1(run_dir: str | None, payload: dict[str, Any]) -> Path | None:
    """
    写入 ``{run_dir}/marketing/marketing_detail_pack_v1.json``。
    ``payload`` 建议与 API 响应体一致（含 schema_version、job_id、core_info_card 等）。
    """
    if not run_dir or not str(run_dir).strip():
        return None
    root = Path(run_dir)
    if not root.is_dir():
        return None
    out_dir = root / "marketing"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "marketing_detail_pack_v1.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
