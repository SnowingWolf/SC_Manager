#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
事件检测与窗口读取示例

演示如何使用 sc_reader 的事件检测功能：
1. 开关沿触发：statedata.Valve_N2 从 0->1 或 1->0
2. 设定值阶跃：runlidata.coldwater_Set 变化 >= 阈值

事件窗口输出：
- t_seconds: 相对事件时间的秒数
- tempdata__*: 温度传感器数据
- runlidata__*: 压力/流量数据（Pressure1-6, coldwater_Set/Cur）
- statedata__*: 设备状态（ffill 处理）
- piddata__*: PID温度数据

可用于：
- 画事件对齐曲线（多事件叠加）
- 计算响应指标（上升时间、超调、稳态误差）
- 做相关性/滞后分析
"""

import sys
import os
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd


def example_event_monitor():
    """
    示例 1: 实时事件监控

    持续监控 statedata 和 runlidata，检测事件并保存窗口数据
    """
    from sc_reader import (
        SCReader,
        EventDetector,
        TriggerType,
        WindowConfig,
        run_event_monitor,
    )

    print("=" * 60)
    print("示例 1: 实时事件监控")
    print("=" * 60)

    # 创建读取器
    reader = SCReader(state_path='./event_watermark.json')

    # 创建事件检测器
    detector = EventDetector()

    # 添加事件触发器
    # 1. Valve_N2 开启（0->1）
    detector.add_edge_trigger(
        name='valve_open',
        table='statedata',
        column='Valve_N2',
        trigger_type=TriggerType.RISING_EDGE
    )

    # 2. Valve_N2 关闭（1->0）
    detector.add_edge_trigger(
        name='valve_close',
        table='statedata',
        column='Valve_N2',
        trigger_type=TriggerType.FALLING_EDGE
    )

    # 3. coldwater_Set 变化（|Δ| >= 0.5）
    detector.add_step_trigger(
        name='coldwater_change',
        table='runlidata',
        column='coldwater_Set',
        threshold=0.5
    )

    # 窗口配置
    window_config = WindowConfig(
        pre_seconds=30.0,    # 事件前 30 秒
        post_seconds=120.0,  # 事件后 120 秒
        anchor_table='tempdata',
        tolerance='2s',
        direction='backward',
        ffill_tables=['statedata']  # statedata 做 forward-fill
    )

    # 创建输出目录
    os.makedirs('./events', exist_ok=True)

    def on_event(df: pd.DataFrame, event):
        """事件回调函数"""
        # 保存 CSV
        filename = f"./events/event_{event.event_id}_{event.event_type}.csv"
        df.to_csv(filename, index=False)
        print(f"  已保存: {filename}")

        # 显示数据概览
        print(f"  时间范围: t = {df['t_seconds'].min():.1f}s ~ {df['t_seconds'].max():.1f}s")
        print(f"  数据点数: {len(df)}")

        # 列出关键列
        key_cols = [c for c in df.columns if any(k in c for k in ['Temperature', 'Pressure', 'coldwater', 'Valve'])]
        print(f"  关键列: {key_cols[:5]}...")

    print("\n事件触发器:")
    print("  - valve_open: Valve_N2 从 0->1")
    print("  - valve_close: Valve_N2 从 1->0")
    print("  - coldwater_change: coldwater_Set 变化 >= 0.5")
    print(f"\n窗口配置: -{window_config.pre_seconds}s ~ +{window_config.post_seconds}s")
    print()

    try:
        run_event_monitor(
            reader=reader,
            detector=detector,
            on_event=on_event,
            window_config=window_config,
            poll_interval=5.0,
            lookback='2s'
        )
    finally:
        reader.close()


def example_analyze_event_data():
    """
    示例 2: 分析已保存的事件数据

    读取事件 CSV 并进行分析和可视化
    """
    from sc_reader.visualizer import plot_timeseries

    print("\n" + "=" * 60)
    print("示例 2: 分析事件数据")
    print("=" * 60)

    # 查找事件文件
    event_dir = './events'
    if not os.path.exists(event_dir):
        print("没有找到 events 目录，请先运行示例 1 采集数据")
        return

    csv_files = [f for f in os.listdir(event_dir) if f.endswith('.csv')]
    if not csv_files:
        print("没有找到事件数据文件")
        return

    print(f"找到 {len(csv_files)} 个事件文件")

    for csv_file in csv_files[:3]:  # 只处理前 3 个
        filepath = os.path.join(event_dir, csv_file)
        df = pd.read_csv(filepath)

        print(f"\n分析: {csv_file}")
        print(f"  数据点: {len(df)}")
        print(f"  时间范围: {df['t_seconds'].min():.1f}s ~ {df['t_seconds'].max():.1f}s")

        # 找温度和压力列
        temp_cols = [c for c in df.columns if 'Temperature' in c and '__' in c][:3]
        pressure_cols = [c for c in df.columns if 'Pressure' in c][:3]

        if temp_cols:
            print(f"  温度列: {temp_cols}")

            # 绘制温度响应曲线
            fig, ax = plt.subplots(figsize=(12, 6))
            for col in temp_cols:
                ax.plot(df['t_seconds'], df[col], label=col.split('__')[-1], alpha=0.8)

            ax.axvline(x=0, color='red', linestyle='--', label='Event', alpha=0.5)
            ax.set_xlabel('Time relative to event (s)')
            ax.set_ylabel('Temperature')
            ax.set_title(f'Temperature Response - {csv_file}')
            ax.legend()
            ax.grid(True, alpha=0.3)

            output_file = filepath.replace('.csv', '_temp.png')
            fig.savefig(output_file, dpi=150, bbox_inches='tight')
            plt.close(fig)
            print(f"  已保存: {output_file}")


def example_multi_event_overlay():
    """
    示例 3: 多事件叠加分析

    将多个同类型事件的响应曲线叠加在一起分析
    """
    print("\n" + "=" * 60)
    print("示例 3: 多事件叠加分析")
    print("=" * 60)

    event_dir = './events'
    if not os.path.exists(event_dir):
        print("没有找到 events 目录")
        return

    # 按事件类型分组
    event_files = {}
    for f in os.listdir(event_dir):
        if not f.endswith('.csv'):
            continue
        # 解析文件名: event_{id}_{type}.csv
        parts = f.replace('.csv', '').split('_')
        if len(parts) >= 3:
            event_type = '_'.join(parts[2:])
            if event_type not in event_files:
                event_files[event_type] = []
            event_files[event_type].append(os.path.join(event_dir, f))

    if not event_files:
        print("没有找到事件数据")
        return

    print(f"事件类型: {list(event_files.keys())}")

    for event_type, files in event_files.items():
        if len(files) < 2:
            continue

        print(f"\n叠加分析: {event_type} ({len(files)} 个事件)")

        fig, ax = plt.subplots(figsize=(12, 6))

        for i, filepath in enumerate(files[:5]):  # 最多 5 个
            df = pd.read_csv(filepath)

            # 找第一个温度列
            temp_col = next((c for c in df.columns if 'Temperature' in c and '__' in c), None)
            if temp_col:
                ax.plot(df['t_seconds'], df[temp_col], alpha=0.6, label=f'Event {i+1}')

        ax.axvline(x=0, color='red', linestyle='--', label='Event', linewidth=2)
        ax.set_xlabel('Time relative to event (s)')
        ax.set_ylabel('Temperature')
        ax.set_title(f'Multi-Event Overlay: {event_type}')
        ax.legend()
        ax.grid(True, alpha=0.3)

        output_file = f'./events/overlay_{event_type}.png'
        fig.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close(fig)
        print(f"  已保存: {output_file}")


def example_response_metrics():
    """
    示例 4: 计算响应指标

    计算上升时间、稳态值、超调量等
    """
    import numpy as np

    print("\n" + "=" * 60)
    print("示例 4: 响应指标计算")
    print("=" * 60)

    event_dir = './events'
    if not os.path.exists(event_dir):
        print("没有找到 events 目录")
        return

    csv_files = [f for f in os.listdir(event_dir) if f.endswith('.csv')]
    if not csv_files:
        print("没有找到事件数据")
        return

    for csv_file in csv_files[:2]:
        filepath = os.path.join(event_dir, csv_file)
        df = pd.read_csv(filepath)

        print(f"\n分析: {csv_file}")

        # 找温度列
        temp_col = next((c for c in df.columns if 'Temperature' in c and '__' in c), None)
        if not temp_col:
            continue

        # 分割事件前后数据
        pre_event = df[df['t_seconds'] < 0][temp_col]
        post_event = df[df['t_seconds'] >= 0][temp_col]

        if pre_event.empty or post_event.empty:
            continue

        # 计算指标
        initial_value = pre_event.mean()
        final_value = post_event.tail(10).mean()  # 最后 10 个点的平均
        peak_value = post_event.max()
        delta = final_value - initial_value

        print(f"  分析列: {temp_col}")
        print(f"  初始值: {initial_value:.3f}")
        print(f"  最终值: {final_value:.3f}")
        print(f"  变化量: {delta:.3f}")
        print(f"  峰值: {peak_value:.3f}")

        if abs(delta) > 0.01:
            overshoot = (peak_value - final_value) / abs(delta) * 100
            print(f"  超调量: {overshoot:.1f}%")

            # 计算上升时间（10% -> 90%）
            target_10 = initial_value + 0.1 * delta
            target_90 = initial_value + 0.9 * delta

            t_10 = df[(df['t_seconds'] >= 0) & (df[temp_col] >= target_10)]['t_seconds'].min()
            t_90 = df[(df['t_seconds'] >= 0) & (df[temp_col] >= target_90)]['t_seconds'].min()

            if pd.notna(t_10) and pd.notna(t_90):
                rise_time = t_90 - t_10
                print(f"  上升时间 (10%-90%): {rise_time:.1f}s")


def example_manual_event_window():
    """
    示例 5: 手动读取事件窗口

    不使用自动监控，手动指定事件时间读取窗口
    """
    from sc_reader import (
        SCReader,
        Event,
        TriggerType,
        WindowConfig,
        EventWindowReader,
        TableSpec,
    )

    print("\n" + "=" * 60)
    print("示例 5: 手动读取事件窗口")
    print("=" * 60)

    reader = SCReader()

    try:
        # 配置窗口
        window_config = WindowConfig(
            pre_seconds=30.0,
            post_seconds=120.0,
            anchor_table='tempdata',
            tolerance='2s',
            ffill_tables=['statedata']
        )

        window_reader = EventWindowReader(reader, window_config)

        # 手动创建事件（假设你知道事件发生的时间）
        event = Event(
            event_id=1,
            event_type='manual_test',
            event_time=datetime(2025, 12, 20, 12, 0, 0),  # 替换为实际时间
            source_table='manual',
            trigger_col='manual',
            trigger_type=TriggerType.RISING_EDGE,
            value_from=0,
            value_to=1
        )

        print(f"事件时间: {event.event_time}")
        print(f"窗口: -{window_config.pre_seconds}s ~ +{window_config.post_seconds}s")

        # 读取窗口
        df = window_reader.read_window(event)

        if df.empty:
            print("窗口数据为空（可能时间范围内没有数据）")
        else:
            print(f"\n读取到 {len(df)} 行数据")
            print(f"列: {list(df.columns)[:10]}...")
            print(f"\n数据预览:")
            print(df[['timestamp', 't_seconds'] + [c for c in df.columns if 'Temperature' in c][:3]].head())

            # 保存
            df.to_csv('./manual_event_window.csv', index=False)
            print("\n已保存: manual_event_window.csv")

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        reader.close()


if __name__ == '__main__':
    print("""
事件检测与窗口读取示例

请选择示例:
  1. 实时事件监控（持续运行）
  2. 分析已保存的事件数据
  3. 多事件叠加分析
  4. 响应指标计算
  5. 手动读取事件窗口

事件类型:
  - valve_open: Valve_N2 从 0->1
  - valve_close: Valve_N2 从 1->0
  - coldwater_change: coldwater_Set 变化 >= 0.5
""")

    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = input("请输入选项 (1-5): ").strip()

    examples = {
        '1': example_event_monitor,
        '2': example_analyze_event_data,
        '3': example_multi_event_overlay,
        '4': example_response_metrics,
        '5': example_manual_event_window,
    }

    if choice in examples:
        examples[choice]()
    else:
        print("无效选项")
