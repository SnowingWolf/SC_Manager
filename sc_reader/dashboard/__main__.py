"""
Dashboard CLI 入口

支持命令行启动 Dashboard：
    python -m sc_reader.dashboard --port 8051 --debug
"""

import argparse
import sys


def main():
    """CLI 入口函数"""
    parser = argparse.ArgumentParser(
        description="SC Dashboard - 慢控数据可视化",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m sc_reader.dashboard
  python -m sc_reader.dashboard --port 8080
  python -m sc_reader.dashboard --config ./sc_config.json
  python -m sc_reader.dashboard --host 0.0.0.0 --port 8051 --no-debug
        """,
    )

    parser.add_argument(
        "--config", "-c",
        type=str,
        default=None,
        help="配置文件路径 (默认: ./sc_config.json)",
    )
    parser.add_argument(
        "--host", "-H",
        type=str,
        default="127.0.0.1",
        help="服务器主机地址 (默认: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=8051,
        help="服务器端口 (默认: 8051)",
    )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        default=True,
        help="启用调试模式 (默认: True)",
    )
    parser.add_argument(
        "--no-debug",
        action="store_true",
        help="禁用调试模式",
    )

    args = parser.parse_args()

    # 处理 debug 标志
    debug = args.debug and not args.no_debug

    # 延迟导入以加快 --help 响应
    from . import run_dashboard

    print(f"Starting SC Dashboard at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop")

    try:
        run_dashboard(
            config=args.config,
            host=args.host,
            port=args.port,
            debug=debug,
        )
    except KeyboardInterrupt:
        print("\nDashboard stopped")
        sys.exit(0)


if __name__ == "__main__":
    main()
