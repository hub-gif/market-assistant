#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys
from pathlib import Path


def _require_pipeline_migration_files() -> None:
    """0013 依赖 0012；缺文件时 MigrationLoader 报 NodeNotFoundError，此处给出可操作的提示。"""
    base = Path(__file__).resolve().parent
    required = (
        base / "pipeline" / "migrations" / "0012_job_pause_checkpoint.py",
        base / "pipeline" / "migrations" / "0013_rebuild_pipelinejobcheckpoint.py",
    )
    missing = [p for p in required if not p.is_file()]
    if not missing:
        return
    print(
        "Missing pipeline migration file(s) (clone/pull 不完整或误删):\n"
        + "\n".join(f"  - {p}" for p in missing)
        + "\n\nRestore from Git, e.g.\n"
        "  git checkout HEAD -- pipeline/migrations/0012_job_pause_checkpoint.py "
        "pipeline/migrations/0013_rebuild_pipelinejobcheckpoint.py\n",
        file=sys.stderr,
    )
    sys.exit(1)


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    _require_pipeline_migration_files()
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
