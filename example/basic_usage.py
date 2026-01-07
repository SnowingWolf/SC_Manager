#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
慢控数据分析脚本示例

演示如何使用 sc_reader 库进行基础数据读取和可视化分析

数据库表:
- piddata: PID温度 (A/B/C/D/E_Temperature, A/B_Heater)
- runlidata: 压力/质量/流量 (Pressure1-6, Mass_left/right, Flux1/2)
- statedata: 设备状态 (Watercooling, refrigerator, etc.)
- tempdata: 温度传感器 (Temperature1-6)
"""

import sys
from datetime import datetime

import matplotlib
matplotlib.use('Agg')  # 非交互式后端
import matplotlib.pyplot as plt

from sc_reader import SCReader
from sc_reader.visualizer import (
    plot_boxplot,
    plot_correlation,
    plot_dual_axis,
    plot_multi_variables,
    plot_subplots,
    plot_timeseries,
)


def main():
    """主函数"""

    print("=" * 60)
    print("慢控数据分析脚本")
    print("=" * 60)

    # ==================== 步骤 1: 连接数据库 ====================
    print("\n步骤 1: 连接数据库...")

    try:
        # 使用默认配置（从 sc_config.json 或内置默认值）
        reader = SCReader()
        print("✓ 数据库连接成功")
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
        return 1

    # ==================== 步骤 2: 探索数据库 ====================
    print("\n步骤 2: 探索数据库...")

    try:
        tables = reader.list_tables()
        print(f"✓ 找到 {len(tables)} 个表")
        print(f"  表列表: {tables}")

        # 使用 tempdata 表作为示例
        table_name = 'tempdata'
        print(f"\n选择表: {table_name}")

        table_info = reader.get_table_info(table_name)
        print(f"✓ 表结构:")
        print(f"  列: {list(table_info['Field'])}")

        time_range = reader.get_time_range(table_name)
        print(f"✓ 数据时间范围:")
        print(f"  最早: {time_range['min_time']}")
        print(f"  最晚: {time_range['max_time']}")

    except Exception as e:
        print(f"✗ 数据库探索失败: {e}")
        reader.close()
        return 1

    # ==================== 步骤 3: 查询数据 ====================
    print("\n步骤 3: 查询数据...")

    try:
        # 根据实际数据时间范围调整
        start_time = '2025-12-15'
        end_time = '2025-12-26'

        print(f"  查询时间范围: {start_time} 到 {end_time}")

        data = reader.query_by_time(
            table_name=table_name,
            start_time=start_time,
            end_time=end_time
        )

        print(f"✓ 查询成功")
        print(f"  数据行数: {len(data)}")
        print(f"  数据列数: {len(data.columns)}")

        numeric_columns = data.select_dtypes(include=['number']).columns.tolist()
        print(f"  数值列: {numeric_columns}")

        if len(data) == 0:
            print("⚠ 警告: 查询结果为空，请检查时间范围")
            reader.close()
            return 1

    except Exception as e:
        print(f"✗ 数据查询失败: {e}")
        reader.close()
        return 1

    # ==================== 步骤 4: 数据分析 ====================
    print("\n步骤 4: 数据分析...")

    try:
        # 温度列
        temp_cols = [c for c in numeric_columns if 'Temperature' in c]
        if temp_cols:
            stats = data[temp_cols].describe()
            print("✓ 温度统计摘要:")
            print(stats)

            stats.to_csv('temperature_stats.csv')
            print("  已保存: temperature_stats.csv")

    except Exception as e:
        print(f"✗ 数据分析失败: {e}")

    # ==================== 步骤 5: 数据可视化 ====================
    print("\n步骤 5: 生成可视化图表...")

    output_files = []

    try:
        # 选择温度列用于可视化
        cols_to_plot = [c for c in numeric_columns if 'Temperature' in c][:4]

        if not cols_to_plot:
            cols_to_plot = numeric_columns[:3]

        print(f"  可视化列: {cols_to_plot}")

        # 图表 1: 时间序列图
        if cols_to_plot:
            print("  生成时间序列图...")
            fig1, ax1 = plot_timeseries(
                data,
                column=cols_to_plot[0],
                title=f'{cols_to_plot[0]} Time Series'
            )
            filename1 = f'timeseries_{cols_to_plot[0]}.png'
            fig1.savefig(filename1, dpi=150, bbox_inches='tight')
            output_files.append(filename1)
            plt.close(fig1)
            print(f"  ✓ 已保存: {filename1}")

        # 图表 2: 多变量对比图
        if len(cols_to_plot) >= 2:
            print("  生成多变量对比图...")
            fig2, ax2 = plot_multi_variables(
                data,
                columns=cols_to_plot,
                title='Temperature Comparison'
            )
            filename2 = 'multi_temperature.png'
            fig2.savefig(filename2, dpi=150, bbox_inches='tight')
            output_files.append(filename2)
            plt.close(fig2)
            print(f"  ✓ 已保存: {filename2}")

        # 图表 3: 双Y轴图表
        if len(cols_to_plot) >= 2:
            print("  生成双Y轴图表...")
            fig3, ax3_1, ax3_2 = plot_dual_axis(
                data,
                left_column=cols_to_plot[0],
                right_column=cols_to_plot[1],
                title=f'{cols_to_plot[0]} vs {cols_to_plot[1]}'
            )
            filename3 = 'dual_axis_temperature.png'
            fig3.savefig(filename3, dpi=150, bbox_inches='tight')
            output_files.append(filename3)
            plt.close(fig3)
            print(f"  ✓ 已保存: {filename3}")

        # 图表 4: 子图布局
        if len(cols_to_plot) >= 2:
            print("  生成子图...")
            fig4, axes4 = plot_subplots(
                data,
                columns=cols_to_plot,
                ncols=2,
                figsize=(14, 8),
                suptitle='Temperature Subplots'
            )
            filename4 = 'subplots_temperature.png'
            fig4.savefig(filename4, dpi=150, bbox_inches='tight')
            output_files.append(filename4)
            plt.close(fig4)
            print(f"  ✓ 已保存: {filename4}")

        # 图表 5: 箱线图
        if cols_to_plot:
            print("  生成箱线图...")
            fig5, ax5 = plot_boxplot(
                data,
                columns=cols_to_plot,
                title='Temperature Box Plot'
            )
            filename5 = 'boxplot_temperature.png'
            fig5.savefig(filename5, dpi=150, bbox_inches='tight')
            output_files.append(filename5)
            plt.close(fig5)
            print(f"  ✓ 已保存: {filename5}")

        # 图表 6: 相关性热力图
        if len(cols_to_plot) >= 2:
            print("  生成相关性热力图...")
            fig6, ax6 = plot_correlation(
                data,
                columns=cols_to_plot,
                title='Temperature Correlation'
            )
            filename6 = 'correlation_temperature.png'
            fig6.savefig(filename6, dpi=150, bbox_inches='tight')
            output_files.append(filename6)
            plt.close(fig6)
            print(f"  ✓ 已保存: {filename6}")

        print(f"\n✓ 共生成 {len(output_files)} 个图表文件")

    except Exception as e:
        print(f"✗ 可视化生成失败: {e}")
        import traceback
        traceback.print_exc()

    # ==================== 步骤 6: 保存数据 ====================
    print("\n步骤 6: 保存数据...")

    try:
        csv_filename = 'tempdata_export.csv'
        data.to_csv(csv_filename)
        print(f"✓ 已保存数据: {csv_filename}")

    except Exception as e:
        print(f"✗ 数据保存失败: {e}")

    # ==================== 步骤 7: 清理 ====================
    print("\n步骤 7: 清理资源...")

    try:
        reader.close()
        print("✓ 数据库连接已关闭")
    except Exception as e:
        print(f"✗ 关闭连接失败: {e}")

    # ==================== 完成 ====================
    print("\n" + "=" * 60)
    print("分析完成！")
    print("=" * 60)
    print("\n生成的文件:")
    for i, filename in enumerate(output_files, 1):
        print(f"  {i}. {filename}")
    if output_files:
        print(f"  {len(output_files)+1}. temperature_stats.csv")
        print(f"  {len(output_files)+2}. tempdata_export.csv")

    return 0


if __name__ == '__main__':
    sys.exit(main())
