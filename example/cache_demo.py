#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlignedDataCache 缓存管理示例

演示如何使用 AlignedDataCache 进行时间索引数据管理：
1. 创建缓存并首次加载
2. 增量更新数据
3. 时间索引查询（切片、loc）
4. pandas 操作（resample、rolling）
5. 数据持久化（save/load）
6. 热启动模式
"""

import sys
import time
from datetime import datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def example_basic_cache():
    """
    示例 1: 基础缓存用法

    创建缓存，首次加载，查看统计信息
    """
    from sc_reader import AlignedData, SCReader, TableSpec

    print("=" * 60)
    print("示例 1: 基础缓存用法")
    print("=" * 60)

    # 创建 reader
    reader = SCReader(state_path="./cache_watermark.json")

    # 定义表规格
    specs = [
        TableSpec("tempdata", "timestamp"),
        TableSpec("runlidata", "timestamp"),
    ]

    try:
        # 创建缓存
        cache = AlignedData(
            reader,
            specs,
            anchor="tempdata",
            tolerance="1s",
            max_memory_mb=100.0,  # 限制内存 100MB
        )

        print(f"初始状态: {cache}")

        # 首次加载
        print("\n首次加载历史数据...")
        n = cache.update()
        print(f"加载 {n} 行数据")

        # 查看统计信息
        print(f"\n缓存状态: {cache}")
        print("\n统计信息:")
        stats = cache.stats
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # 查看数据概览
        if not cache.data.empty:
            print(f"\n数据列: {list(cache.columns)[:5]}...")
            print(f"时间范围: {cache.time_range}")
            print(f"\n数据预览:")
            print(cache.data.head())

    except Exception as e:
        print(f"错误: {e}")
        import traceback

        traceback.print_exc()
    finally:
        reader.close()


def example_time_indexing():
    """
    示例 2: 时间索引查询

    演示时间切片和 loc 访问
    """
    from sc_reader import AlignedData, SCReader, TableSpec

    print("\n" + "=" * 60)
    print("示例 2: 时间索引查询")
    print("=" * 60)

    reader = SCReader(state_path="./cache_watermark.json")
    specs = [
        TableSpec("tempdata", "timestamp"),
        TableSpec("runlidata", "timestamp"),
    ]

    try:
        cache = AlignedData(reader, specs, anchor="tempdata")
        cache.update()

        if cache.data.empty:
            print("缓存为空")
            return

        time_range = cache.time_range
        print(f"数据时间范围: {time_range[0]} ~ {time_range[1]}")

        # 时间切片查询
        print("\n1. 时间范围切片:")
        start_str = time_range[0].strftime("%Y-%m-%d")
        end_str = time_range[1].strftime("%Y-%m-%d")

        df_range = cache[start_str:end_str]
        print(f"  {start_str} ~ {end_str}: {len(df_range)} 行")

        # 单小时查询
        print("\n2. 单小时数据:")
        hour_start = time_range[0].replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start.replace(hour=hour_start.hour + 1)
        df_hour = cache.loc[hour_start:hour_end]
        print(f"  {hour_start} ~ {hour_end}: {len(df_hour)} 行")

        # 最近数据
        print("\n3. 最近 10 行:")
        df_recent = cache.iloc[-10:]
        print(df_recent)

    except Exception as e:
        print(f"错误: {e}")
        import traceback

        traceback.print_exc()
    finally:
        reader.close()


def example_pandas_operations():
    """
    示例 3: pandas 操作

    演示重采样、滚动统计等操作
    """
    from sc_reader import AlignedData, SCReader, TableSpec

    print("\n" + "=" * 60)
    print("示例 3: pandas 操作")
    print("=" * 60)

    reader = SCReader(state_path="./cache_watermark.json")
    specs = [
        TableSpec("tempdata", "timestamp"),
        TableSpec("runlidata", "timestamp"),
    ]

    try:
        cache = AlignedData(reader, specs, anchor="tempdata")
        cache.update()

        if cache.data.empty:
            print("缓存为空")
            return

        # 找数值列
        numeric_cols = cache.data.select_dtypes(include=["number"]).columns[:3]
        if len(numeric_cols) == 0:
            print("没有数值列")
            return

        print(f"分析列: {list(numeric_cols)}")

        # 1. 重采样
        print("\n1. 重采样（1分钟平均）:")
        resampled = cache.data[numeric_cols].resample("1min").mean()
        print(f"  原始数据: {len(cache)} 行")
        print(f"  重采样后: {len(resampled)} 行")
        print(resampled.head())

        # 2. 滚动统计
        print("\n2. 滚动标准差（10分钟窗口）:")
        rolling = cache.data[numeric_cols].rolling("10min").std()
        print(rolling.tail())

        # 3. 描述性统计
        print("\n3. 描述性统计:")
        print(cache.data[numeric_cols].describe())

        # 4. 绘图
        if len(numeric_cols) > 0:
            print("\n4. 生成时间序列图...")
            fig, axes = plt.subplots(len(numeric_cols), 1, figsize=(12, 4 * len(numeric_cols)))
            if len(numeric_cols) == 1:
                axes = [axes]

            for i, col in enumerate(numeric_cols):
                cache.data[col].plot(ax=axes[i], title=col)
                axes[i].grid(True, alpha=0.3)

            fig.tight_layout()
            output_file = "cache_timeseries.png"
            fig.savefig(output_file, dpi=150, bbox_inches="tight")
            plt.close(fig)
            print(f"  已保存: {output_file}")

    except Exception as e:
        print(f"错误: {e}")
        import traceback

        traceback.print_exc()
    finally:
        reader.close()


def example_continuous_update():
    """
    示例 4: 持续更新模式

    定期拉取增量数据并监控缓存状态
    """
    from sc_reader import AlignedData, SCReader, TableSpec

    print("\n" + "=" * 60)
    print("示例 4: 持续更新模式")
    print("=" * 60)
    print("(Ctrl+C 停止)")

    reader = SCReader(state_path="./cache_watermark.json")
    specs = [
        TableSpec("tempdata", "timestamp"),
        TableSpec("runlidata", "timestamp"),
    ]

    try:
        cache = AlignedData(
            reader,
            specs,
            anchor="tempdata",
            max_rows=10000,  # 限制最多 10000 行
        )

        # 首次加载
        cache.update()
        print(f"初始加载: {len(cache)} 行\n")

        poll_count = 0
        poll_interval = 5.0

        while True:
            poll_count += 1
            ts = datetime.now().strftime("%H:%M:%S")

            # 增量更新
            new_rows = cache.update()

            print(
                f"[{ts}] 轮询 #{poll_count}: "
                f"新增 {new_rows} 行, "
                f"总计 {len(cache)} 行, "
                f"内存 {cache.memory_usage_mb:.1f}MB"
            )

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print("\n已停止")
    finally:
        reader.close()


def example_persistence():
    """
    示例 5: 数据持久化

    演示 save/load 功能和热启动
    """
    import os

    from sc_reader import AlignedData, SCReader, TableSpec

    print("\n" + "=" * 60)
    print("示例 5: 数据持久化")
    print("=" * 60)

    reader = SCReader(state_path="./cache_watermark.json")
    specs = [
        TableSpec("tempdata", "timestamp"),
        TableSpec("runlidata", "timestamp"),
    ]

    cache_file = "./cache/aligned_data.parquet"

    try:
        # 1. 创建并保存缓存
        print("1. 创建缓存并保存...")
        cache = AlignedData(reader, specs, anchor="tempdata")
        n = cache.update()
        print(f"  加载 {n} 行")

        cache.save(cache_file)
        print(f"  已保存: {cache_file}")

        # 查看文件大小
        if os.path.exists(cache_file):
            size_mb = os.path.getsize(cache_file) / 1024 / 1024
            print(f"  文件大小: {size_mb:.1f}MB")

        # 2. 从文件加载
        print("\n2. 从文件加载缓存...")
        cache2 = AlignedData(reader, specs, anchor="tempdata")
        cache2.load(cache_file)
        print(f"  加载 {len(cache2)} 行")
        print(f"  时间范围: {cache2.time_range}")

        # 3. 热启动：加载 + 增量更新
        print("\n3. 热启动模式（加载历史 + 拉取增量）...")
        cache3 = AlignedData(reader, specs, anchor="tempdata")
        cache3.load(cache_file)
        print(f"  从文件加载: {len(cache3)} 行")

        new_rows = cache3.update()
        print(f"  增量更新: {new_rows} 行")
        print(f"  总计: {len(cache3)} 行")

        # 4. 合并加载
        print("\n4. 合并加载模式...")
        cache4 = AlignedData(reader, specs, anchor="tempdata")
        cache4.update()  # 先读取当前数据
        print(f"  当前数据: {len(cache4)} 行")

        cache4.load(cache_file, merge=True)  # 合并历史数据
        print(f"  合并后: {len(cache4)} 行")

    except Exception as e:
        print(f"错误: {e}")
        import traceback

        traceback.print_exc()
    finally:
        reader.close()


def example_memory_management():
    """
    示例 6: 内存管理

    演示三种内存管理策略
    """
    from sc_reader import AlignedData, SCReader, TableSpec

    print("\n" + "=" * 60)
    print("示例 6: 内存管理策略")
    print("=" * 60)

    reader = SCReader(state_path="./cache_watermark.json")
    specs = [
        TableSpec("tempdata", "timestamp"),
        TableSpec("runlidata", "timestamp"),
    ]

    try:
        # 1. 时间窗口限制
        print("1. 时间窗口限制（保留最近 1 天）:")
        cache1 = AlignedData(reader, specs, anchor="tempdata", time_window_days=1.0)
        cache1.update()
        print(f"  数据行数: {len(cache1)}")
        if cache1.time_range:
            print(f"  时间范围: {cache1.time_range[0]} ~ {cache1.time_range[1]}")

        # 2. 行数限制
        print("\n2. 行数限制（最多 1000 行）:")
        cache2 = AlignedData(reader, specs, anchor="tempdata", max_rows=1000)
        cache2.update()
        print(f"  数据行数: {len(cache2)}")

        # 3. 内存限制
        print("\n3. 内存限制（最多 10MB）:")
        cache3 = AlignedData(reader, specs, anchor="tempdata", max_memory_mb=10.0)
        cache3.update()
        print(f"  数据行数: {len(cache3)}")
        print(f"  内存占用: {cache3.memory_usage_mb:.1f}MB")

    except Exception as e:
        print(f"错误: {e}")
        import traceback

        traceback.print_exc()
    finally:
        reader.close()


if __name__ == "__main__":
    print("""
AlignedDataCache 缓存管理示例

请选择示例:
  1. 基础缓存用法
  2. 时间索引查询
  3. pandas 操作
  4. 持续更新模式
  5. 数据持久化
  6. 内存管理策略

缓存特性:
  - 累积历史数据到内存
  - 支持时间索引切片和 loc 访问
  - 自动增量更新和去重
  - 可选的内存管理（时间窗口/行数/内存限制）
  - Parquet 格式持久化
""")

    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = input("请输入选项 (1-6): ").strip()

    examples = {
        "1": example_basic_cache,
        "2": example_time_indexing,
        "3": example_pandas_operations,
        "4": example_continuous_update,
        "5": example_persistence,
        "6": example_memory_management,
    }

    if choice in examples:
        examples[choice]()
    else:
        print("无效选项")
