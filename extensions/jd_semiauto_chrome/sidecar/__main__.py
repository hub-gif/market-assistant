# -*- coding: utf-8 -*-
import argparse
import os

try:
    from .server import run_server
except ImportError:
    from server import run_server


def main() -> None:
    ap = argparse.ArgumentParser(description="京东半自动插件 sidecar（写 run_dir）")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=None)
    args = ap.parse_args()
    if args.port is not None:
        os.environ["JD_SEMIAUTO_SIDECAR_PORT"] = str(args.port)
    run_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
