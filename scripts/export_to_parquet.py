#!/usr/bin/env python3
"""
将 MySQL 数据库导出到本地 Parquet 文件

支持两种模式：
1. 全量导出：导出所有表的全部数据
2. 增量导出：基于 watermark 只导出新数据

用法：
    # 全量导出所有表
    python export_to_parquet.py --full

    # 增量导出（首次运行会全量导出）
    python export_to_parquet.py

    # 指定输出目录
    python export_to_parquet.py --output ./data/parquet

    # 只导出指定表
    python export_to_parquet.py --tables tempdata runlidata piddata

    # 按时间范围导出
    python export_to_parquet.py --start 2025-01-01 --end 2025-01-31
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

from sc_reader import SCReader, TableSpec


class ParquetExporter:
    """Parquet 数据导出器"""

    def __init__(
        self,
        output_dir: str = "./data/parquet",
        state_file: str = "./export_watermark.json",
        config_path: Optional[str] = None,
        compact_threshold_files: int = 20,
        compact_threshold_rows: int = 2_000_000,
    ):
        """
        初始化导出器

        Args:
            output_dir: Parquet 文件输出目录
            state_file: watermark 状态文件路径
            config_path: MySQL 配置文件路径
            compact_threshold_files: 触发压实的 delta 文件数量阈值
            compact_threshold_rows: 触发压实的 delta 行数阈值（估算）
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.state_file = Path(state_file)
        self.config_path = config_path
        self.compact_threshold_files = int(compact_threshold_files)
        self.compact_threshold_rows = int(compact_threshold_rows)

        # 加载导出状态
        self._export_state = self._load_state()

    def _base_path(self, table_name: str) -> Path:
        # 保持兼容：主文件仍在 output_dir/{table}.parquet
        return self.output_dir / f"{table_name}.parquet"

    def _delta_dir(self, table_name: str) -> Path:
        # 增量分片文件目录
        return self.output_dir / "_delta" / table_name

    def _next_delta_path(self, table_name: str) -> Path:
        delta_dir = self._delta_dir(table_name)
        delta_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        return delta_dir / f"delta_{ts}.parquet"

    def _compact_table(self, table_name: str) -> int:
        """
        将 base + delta 压实到单个 base 文件。

        Returns:
            压实后总行数
        """
        table_state = self._export_state["tables"].get(table_name, {})
        base_path = Path(table_state.get("base_file", self._base_path(table_name)))
        delta_files = [Path(p) for p in table_state.get("pending_delta_files", []) if Path(p).exists()]

        dataframes = []
        if base_path.exists():
            base_df = pd.read_parquet(base_path, engine="pyarrow")
            if not isinstance(base_df.index, pd.DatetimeIndex):
                base_df.index = pd.to_datetime(base_df.index)
            dataframes.append(base_df)

        for delta_path in delta_files:
            delta_df = pd.read_parquet(delta_path, engine="pyarrow")
            if not isinstance(delta_df.index, pd.DatetimeIndex):
                delta_df.index = pd.to_datetime(delta_df.index)
            dataframes.append(delta_df)

        if not dataframes:
            return 0

        combined_df = pd.concat(dataframes, axis=0)
        combined_df = combined_df[~combined_df.index.duplicated(keep="last")]
        combined_df = combined_df.sort_index()

        base_path.parent.mkdir(parents=True, exist_ok=True)
        combined_df.to_parquet(
            base_path,
            compression="snappy",
            engine="pyarrow",
            index=True,
        )

        # 清理已压实的 delta 文件
        for delta_path in delta_files:
            try:
                delta_path.unlink()
            except OSError:
                pass

        last_idx = combined_df.index[-1]
        last_ts = last_idx.isoformat() if hasattr(last_idx, "isoformat") else pd.Timestamp(last_idx).isoformat()

        self._export_state["tables"][table_name] = {
            **table_state,
            "storage_mode": "delta_v1",
            "base_file": str(base_path),
            "delta_dir": str(self._delta_dir(table_name)),
            "pending_delta_files": [],
            "pending_delta_rows": 0,
            "last_export": datetime.now().isoformat(),
            "last_ts": last_ts,
            "rows": len(combined_df),
            "file": str(base_path),
        }
        self._save_state()
        return len(combined_df)

    def compact_tables(self, tables: Optional[List[str]] = None) -> dict:
        """手动压实指定表（或所有表）。"""
        if tables is None:
            tables = sorted(set(self._export_state.get("tables", {}).keys()))
        results = {}
        for table in tables:
            try:
                rows = self._compact_table(table)
                results[table] = rows
                print(f"压实表 {table}: {rows} 行")
            except Exception as e:
                print(f"压实表 {table} 失败: {e}")
                results[table] = -1
        return results

    def _should_compact(self, table_state: dict) -> bool:
        pending_files = len(table_state.get("pending_delta_files", []))
        pending_rows = int(table_state.get("pending_delta_rows", 0))
        return (
            pending_files >= self.compact_threshold_files
            or pending_rows >= self.compact_threshold_rows
        )

    def _load_state(self) -> dict:
        """加载导出状态"""
        if self.state_file.exists():
            return json.loads(self.state_file.read_text())
        return {"tables": {}, "last_export": None}

    def _save_state(self):
        """保存导出状态"""
        self._export_state["last_export"] = datetime.now().isoformat()
        self.state_file.write_text(json.dumps(self._export_state, indent=2))

    def _get_reader(self) -> SCReader:
        """创建 SCReader 实例"""
        if self.config_path:
            from sc_reader.config import MySQLConfig
            config = MySQLConfig.from_json(self.config_path)
            return SCReader(config=config)
        return SCReader()

    def list_tables(self) -> List[str]:
        """列出所有可导出的表"""
        reader = self._get_reader()
        try:
            return reader.list_tables()
        finally:
            reader.close()

    def export_table_full(
        self,
        table_name: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> int:
        """
        全量导出单个表

        Args:
            table_name: 表名
            start_time: 开始时间（可选）
            end_time: 结束时间（可选）
            chunksize: 分块大小，避免内存溢出

        Returns:
            导出的行数
        """
        reader = self._get_reader()
        try:
            print(f"正在导出表: {table_name}")

            # 获取表信息
            time_range = reader.get_time_range(table_name)
            print(f"  数据时间范围: {time_range['min_time']} ~ {time_range['max_time']}")

            # 查询数据
            df = reader.query_df(
                table_name,
                start_time=start_time,
                end_time=end_time,
            )

            if df.empty:
                print(f"  表 {table_name} 无数据")
                return 0

            # 保存到 Parquet
            output_path = self._base_path(table_name)
            df.to_parquet(
                output_path,
                compression="snappy",
                engine="pyarrow",
                index=True,
            )

            # 全量导出后清理 delta
            delta_dir = self._delta_dir(table_name)
            if delta_dir.exists():
                for f in delta_dir.glob("*.parquet"):
                    try:
                        f.unlink()
                    except OSError:
                        pass

            # 更新状态
            last_ts = None
            if len(df) > 0:
                last_idx = df.index[-1]
                if hasattr(last_idx, 'isoformat'):
                    last_ts = last_idx.isoformat()
                else:
                    last_ts = pd.Timestamp(last_idx).isoformat()

            self._export_state["tables"][table_name] = {
                "storage_mode": "delta_v1",
                "base_file": str(output_path),
                "delta_dir": str(delta_dir),
                "pending_delta_files": [],
                "pending_delta_rows": 0,
                "last_export": datetime.now().isoformat(),
                "last_ts": last_ts,
                "rows": len(df),
                "file": str(output_path),
            }
            self._save_state()

            print(f"  导出完成: {len(df)} 行 -> {output_path}")
            return len(df)

        finally:
            reader.close()

    def export_table_incremental(
        self,
        table_name: str,
        lookback: str = "5s",
    ) -> int:
        """
        增量导出单个表

        Args:
            table_name: 表名
            lookback: 回看窗口

        Returns:
            新增的行数
        """
        reader = self._get_reader()
        try:
            output_path = self._base_path(table_name)

            # 检查是否有已导出的数据
            table_state = self._export_state["tables"].get(table_name, {})
            last_ts = table_state.get("last_ts")

            if last_ts is None or not output_path.exists():
                # 首次导出，执行全量导出
                print(f"表 {table_name} 首次导出，执行全量导出...")
                return self.export_table_full(table_name)

            # 增量导出
            print(f"正在增量导出表: {table_name}")
            print(f"  上次导出时间: {last_ts}")

            # 读取新数据
            spec = TableSpec(table_name)

            # 设置 watermark
            reader._watermarks[table_name] = {
                "last_ts": datetime.fromisoformat(last_ts),
                "last_id": None,
            }

            new_df = reader.read_incremental(spec, lookback=lookback)

            if new_df.empty:
                print(f"  无新数据")
                return 0

            # 增量写入 delta 文件，避免每轮重写整个 base 文件
            delta_path = self._next_delta_path(table_name)
            new_df.to_parquet(
                delta_path,
                compression="snappy",
                engine="pyarrow",
                index=True,
            )

            # 更新状态
            last_idx = new_df.index[-1]
            if hasattr(last_idx, 'isoformat'):
                last_ts = last_idx.isoformat()
            else:
                last_ts = pd.Timestamp(last_idx).isoformat()

            pending_delta_files = table_state.get("pending_delta_files", [])
            pending_delta_files = [p for p in pending_delta_files if Path(p).exists()]
            pending_delta_files.append(str(delta_path))

            pending_delta_rows = int(table_state.get("pending_delta_rows", 0)) + len(new_df)
            estimated_rows = int(table_state.get("rows", 0)) + len(new_df)

            self._export_state["tables"][table_name] = {
                "storage_mode": "delta_v1",
                "base_file": str(output_path),
                "delta_dir": str(self._delta_dir(table_name)),
                "pending_delta_files": pending_delta_files,
                "pending_delta_rows": pending_delta_rows,
                "last_export": datetime.now().isoformat(),
                "last_ts": last_ts,
                "rows": estimated_rows,
                "file": str(output_path),
            }
            compacted = False
            if self._should_compact(self._export_state["tables"][table_name]):
                total_rows = self._compact_table(table_name)
                compacted = True
            else:
                total_rows = estimated_rows
                self._save_state()

            if compacted:
                print(f"  新增 {len(new_df)} 行，触发压实后总计 {total_rows} 行")
            else:
                print(
                    f"  新增 {len(new_df)} 行，base 暂不重写（pending_delta={len(pending_delta_files)}）"
                )
            return len(new_df)

        finally:
            reader.close()

    def export_all(
        self,
        tables: Optional[List[str]] = None,
        full: bool = False,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> dict:
        """
        导出多个表

        Args:
            tables: 要导出的表列表，None 表示所有表
            full: 是否全量导出
            start_time: 开始时间（仅全量导出时有效）
            end_time: 结束时间（仅全量导出时有效）

        Returns:
            导出统计 {表名: 行数}
        """
        if tables is None:
            tables = self.list_tables()

        results = {}
        total_rows = 0

        for table in tables:
            try:
                if full:
                    rows = self.export_table_full(table, start_time, end_time)
                else:
                    rows = self.export_table_incremental(table)
                results[table] = rows
                total_rows += rows
            except Exception as e:
                print(f"导出表 {table} 失败: {e}")
                results[table] = -1

        print(f"\n导出完成！共导出 {total_rows} 行数据到 {self.output_dir}")
        return results

    def get_status(self) -> dict:
        """获取导出状态"""
        return self._export_state


def main():
    parser = argparse.ArgumentParser(
        description="将 MySQL 数据库导出到本地 Parquet 文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output", "-o",
        default="./data/parquet",
        help="Parquet 文件输出目录 (默认: ./data/parquet)",
    )
    parser.add_argument(
        "--state", "-s",
        default="./export_watermark.json",
        help="watermark 状态文件路径 (默认: ./export_watermark.json)",
    )
    parser.add_argument(
        "--config", "-c",
        default=None,
        help="MySQL 配置文件路径 (默认: 使用 sc_config.json)",
    )
    parser.add_argument(
        "--tables", "-t",
        nargs="+",
        default=None,
        help="要导出的表名列表 (默认: 所有表)",
    )
    parser.add_argument(
        "--full", "-f",
        action="store_true",
        help="全量导出 (默认: 增量导出)",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="开始时间，如 2025-01-01 (仅全量导出时有效)",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="结束时间，如 2025-01-31 (仅全量导出时有效)",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="列出所有可导出的表",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="显示导出状态",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="执行压实（base + delta 合并）后退出",
    )
    parser.add_argument(
        "--compact-threshold-files",
        type=int,
        default=20,
        help="自动压实阈值：delta 文件数（默认: 20）",
    )
    parser.add_argument(
        "--compact-threshold-rows",
        type=int,
        default=2_000_000,
        help="自动压实阈值：delta 行数估算（默认: 2000000）",
    )

    args = parser.parse_args()

    exporter = ParquetExporter(
        output_dir=args.output,
        state_file=args.state,
        config_path=args.config,
        compact_threshold_files=args.compact_threshold_files,
        compact_threshold_rows=args.compact_threshold_rows,
    )

    if args.list:
        tables = exporter.list_tables()
        print(f"可导出的表 ({len(tables)} 个):")
        for t in tables:
            print(f"  - {t}")
        return

    if args.status:
        status = exporter.get_status()
        print("导出状态:")
        print(f"  上次导出: {status.get('last_export', '从未')}")
        print(f"  已导出表: {len(status.get('tables', {}))}")
        for table, info in status.get("tables", {}).items():
            pending_n = len(info.get("pending_delta_files", []))
            print(
                f"    - {table}: {info.get('rows', 0)} 行, "
                f"pending_delta={pending_n}, 最后更新 {info.get('last_export', '未知')}"
            )
        return

    if args.compact:
        exporter.compact_tables(args.tables)
        return

    # 执行导出
    exporter.export_all(
        tables=args.tables,
        full=args.full,
        start_time=args.start,
        end_time=args.end,
    )


if __name__ == "__main__":
    main()
