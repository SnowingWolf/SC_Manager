#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sc_reader 增量读取与时间对齐示例

演示如何使用 sc_reader 库实现：
1. 增量读取持续更新的 MySQL 表
2. 多表时间对齐合并
3. 结果可视化

数据库表结构：
- piddata: PID温度数据 (timestamp varchar, A/B/C/D/E_Temperature, A/B_Heater)
- runlidata: 运行数据 (timestamp datetime, Pressure1-6, Mass_left/right, Flux1/2, coldwater)
- statedata: 状态数据 (timestamp datetime, Watercooling, refrigerator, etc.)
- tempdata: 温度数据 (timestamp datetime, Temperature1-6)
"""

import sys
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def example_basic_incremental():
    """
    示例 1: 基础增量读取

    使用 SCReader 增量读取单表数据
    """
    from sc_reader import SCReader, TableSpec

    print("=" * 60)
    print("示例 1: 基础增量读取 - tempdata 表")
    print("=" * 60)

    reader = SCReader()

    try:
        # 读取 tempdata 表
        spec = TableSpec(
            table='tempdata',
            time_col='timestamp',  # datetime 类型
            key_col='id'           # 主键，用于精确增量
        )

        df = reader.read_incremental(spec, lookback='2s')

        print(f"读取 {len(df)} 行")
        if not df.empty:
            print(f"时间范围: {df.index.min()} ~ {df.index.max()}")
            print(f"列: {list(df.columns)}")
            print(df.head())

        # 再次调用只返回新数据
        print("\n再次读取（增量）...")
        df_new = reader.read_incremental(spec)
        print(f"增量数据: {len(df_new)} 行")

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        reader.close()


def example_multi_table_align():
    """
    示例 2: 多表时间对齐

    将 tempdata 和 runlidata 按时间对齐合并
    """
    from sc_reader import SCReader, TableSpec, collect_and_align

    print("\n" + "=" * 60)
    print("示例 2: 多表时间对齐 - tempdata + runlidata")
    print("=" * 60)

    reader = SCReader(state_path='./watermark.json')

    try:
        # 定义表规格
        specs = [
            # tempdata 作为 anchor（基准表）
            TableSpec(
                table='tempdata',
                time_col='timestamp',
                cols=['Temperature', 'Temperature2', 'Temperature3'],
                key_col='id'
            ),
            # runlidata 对齐到 tempdata
            TableSpec(
                table='runlidata',
                time_col='timestamp',
                cols=['Pressure1', 'Pressure2', 'Mass_left', 'Flux1'],
                key_col='id'
            ),
        ]

        print("Anchor: tempdata")
        print("对齐表: runlidata")
        print("容差: 1s (因为采样率不同)")

        # 一次调用完成读取和对齐
        df = collect_and_align(
            reader,
            specs,
            anchor='tempdata',
            tolerance='1s',       # 1秒容差
            direction='backward', # 向后查找最近的
            lookback='5s'
        )

        print(f"\n对齐后: {len(df)} 行, {len(df.columns)} 列")
        print(f"列名: {list(df.columns)}")

        if not df.empty:
            print("\n数据预览:")
            print(df.head(10))

            # 保存结果
            output_file = f"aligned_temp_runli_{datetime.now():%Y%m%d_%H%M%S}.csv"
            df.to_csv(output_file)
            print(f"\n已保存: {output_file}")

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        reader.close()


def example_three_table_align():
    """
    示例 3: 三表时间对齐

    将 tempdata, runlidata, statedata 对齐
    """
    from sc_reader import SCReader, TableSpec, collect_and_align

    print("\n" + "=" * 60)
    print("示例 3: 三表对齐 - tempdata + runlidata + statedata")
    print("=" * 60)

    reader = SCReader(state_path='./watermark_3table.json')

    try:
        specs = [
            TableSpec('tempdata', 'timestamp', key_col='id'),
            TableSpec('runlidata', 'timestamp', key_col='id'),
            TableSpec('statedata', 'timestamp', key_col='id'),
        ]

        df = collect_and_align(
            reader,
            specs,
            anchor='tempdata',
            tolerance='2s',
            lookback='5s'
        )

        print(f"对齐后: {len(df)} 行, {len(df.columns)} 列")

        if not df.empty:
            # 显示各表的列
            temp_cols = [c for c in df.columns if c.startswith('tempdata__')]
            runli_cols = [c for c in df.columns if c.startswith('runlidata__')]
            state_cols = [c for c in df.columns if c.startswith('statedata__')]

            print(f"\ntempdata 列 ({len(temp_cols)}): {temp_cols[:3]}...")
            print(f"runlidata 列 ({len(runli_cols)}): {runli_cols[:3]}...")
            print(f"statedata 列 ({len(state_cols)}): {state_cols[:3]}...")

            print("\n数据预览:")
            print(df.head())

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        reader.close()


def example_with_visualization():
    """
    示例 4: 增量读取 + 可视化

    读取 piddata 并绘制温度曲线
    """
    from sc_reader import SCReader, TableSpec
    from sc_reader.visualizer import plot_multi_variables, plot_correlation

    print("\n" + "=" * 60)
    print("示例 4: 可视化 - piddata 温度数据")
    print("=" * 60)

    reader = SCReader()

    try:
        # piddata 的时间列是 varchar 格式
        spec = TableSpec(
            table='piddata',
            time_col='timestamp',
            cols=['A_Temperature', 'B_Temperature', 'C_Temperature', 'D_Temperature'],
            key_col='id'
        )

        df = reader.read_incremental(spec)

        if df.empty:
            print("没有数据")
            return

        print(f"读取 {len(df)} 行数据")
        print(f"列: {list(df.columns)}")

        # 温度列
        temp_cols = ['A_Temperature', 'B_Temperature', 'C_Temperature', 'D_Temperature']
        available_cols = [c for c in temp_cols if c in df.columns]

        if len(available_cols) >= 2:
            # 时间序列图
            fig1, ax1 = plot_multi_variables(
                df, available_cols,
                title='PID Temperature Time Series'
            )
            fig1.savefig('pid_temperature.png', dpi=150, bbox_inches='tight')
            plt.close(fig1)
            print("已保存: pid_temperature.png")

            # 相关性热力图
            fig2, ax2 = plot_correlation(
                df, available_cols,
                title='Temperature Correlation'
            )
            fig2.savefig('pid_correlation.png', dpi=150, bbox_inches='tight')
            plt.close(fig2)
            print("已保存: pid_correlation.png")

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        reader.close()


def example_continuous_poll():
    """
    示例 5: 持续轮询模式

    定期增量读取 runlidata 并监控压力
    """
    import time
    from sc_reader import SCReader, TableSpec

    print("\n" + "=" * 60)
    print("示例 5: 持续轮询 - 监控 runlidata")
    print("=" * 60)
    print("(Ctrl+C 停止)")

    reader = SCReader(state_path='./watermark_poll.json')

    try:
        spec = TableSpec(
            table='runlidata',
            time_col='timestamp',
            cols=['Pressure1', 'Pressure2', 'Mass_left', 'Mass_right'],
            key_col='id'
        )

        poll_count = 0
        poll_interval = 5.0

        print("轮询表: runlidata")
        print(f"间隔: {poll_interval} 秒")
        print()

        while True:
            poll_count += 1
            ts = datetime.now().strftime('%H:%M:%S')

            df = reader.read_incremental(spec, lookback='2s')

            if df.empty:
                print(f"[{ts}] 轮询 #{poll_count}: 无新数据")
            else:
                print(f"[{ts}] 轮询 #{poll_count}: {len(df)} 行新数据")
                # 简单分析
                if 'Pressure1' in df.columns:
                    p1_mean = df['Pressure1'].mean()
                    print(f"    Pressure1 平均值: {p1_mean:.6f}")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print("\n已停止")
    finally:
        reader.close()


def example_config_demo():
    """
    示例 6: 配置管理演示
    """
    from sc_reader import MySQLConfig, DEFAULT_MYSQL_CONFIG

    print("\n" + "=" * 60)
    print("示例 6: 配置管理")
    print("=" * 60)

    print("默认 MySQL 配置:")
    print(f"  host: {DEFAULT_MYSQL_CONFIG.host}")
    print(f"  port: {DEFAULT_MYSQL_CONFIG.port}")
    print(f"  user: {DEFAULT_MYSQL_CONFIG.user}")
    print(f"  database: {DEFAULT_MYSQL_CONFIG.database}")

    print("\n从 JSON 加载配置:")
    print("  config = MySQLConfig.from_json('./sc_config.json')")

    print("\n直接创建配置:")
    custom = MySQLConfig(host='192.168.4.19')
    print("  MySQLConfig(host='192.168.4.19')")
    print(f"  URL: {custom.url}")


if __name__ == '__main__':
    print("""
sc_reader 增量读取与时间对齐示例

数据库表:
  - piddata: PID温度 (A/B/C/D/E_Temperature)
  - runlidata: 压力/质量/流量
  - statedata: 设备状态
  - tempdata: 温度传感器

请选择示例:
  1. 基础增量读取 (tempdata)
  2. 双表时间对齐 (tempdata + runlidata)
  3. 三表时间对齐 (tempdata + runlidata + statedata)
  4. 可视化 (piddata 温度)
  5. 持续轮询 (runlidata)
  6. 配置管理演示
""")

    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = input("请输入选项 (1-6): ").strip()

    examples = {
        '1': example_basic_incremental,
        '2': example_multi_table_align,
        '3': example_three_table_align,
        '4': example_with_visualization,
        '5': example_continuous_poll,
        '6': example_config_demo,
    }

    if choice in examples:
        examples[choice]()
    else:
        print("无效选项，运行示例 6（配置演示）")
        example_config_demo()
