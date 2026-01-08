# 快速开始

本指南将在 5 分钟内带您快速上手 SC_Reader。

## 安装

```bash
pip install pymysql pandas matplotlib seaborn numpy sqlalchemy pyarrow
```

## 配置数据库连接

创建 `sc_config.json` 文件：

```json
{
  "mysql": {
    "host": "10.11.50.141",
    "port": 3306,
    "user": "read",
    "password": "your_password",
    "database": "slowcontroldata"
  }
}
```

或者使用环境变量：

```bash
export SC_CONFIG_PATH=/path/to/sc_config.json
```

## 示例 1：基础查询

最简单的使用方式 - 读取并分析数据：

```python
from sc_reader import SCReader

# 连接数据库
reader = SCReader()

# 查询数据
data = reader.query_by_time(
    table_name='tempdata',
    start_time='2025-12-15',
    end_time='2025-12-26'
)

# 查看数据
print(f"读取 {len(data)} 行数据")
print(data.head())

# 统计分析
print(data.describe())

# 关闭连接
reader.close()
```

**输出示例：**
```
读取 10000 行数据
                     Temperature  Temperature2  Temperature3
timestamp
2025-12-15 00:00:00        20.5          21.3          19.8
2025-12-15 00:00:10        20.6          21.2          19.9
...
```

## 示例 2：增量读取

持续监控数据更新：

```python
from sc_reader import SCReader, TableSpec

# 创建增量读取器（带状态保存）
reader = SCReader(state_path='./watermark.json')

# 定义表规格
spec = TableSpec('tempdata', 'timestamp')

# 首次调用：读取所有历史数据
data1 = reader.read_incremental(spec)
print(f"初始加载: {len(data1)} 行")

# 再次调用：只读取新增数据
data2 = reader.read_incremental(spec)
print(f"增量数据: {len(data2)} 行")

reader.close()
```

## 示例 3：多表时间对齐

对齐不同采样率的数据：

```python
from sc_reader import SCReader, TableSpec, collect_and_align

reader = SCReader()

# 定义多个表
specs = [
    TableSpec('tempdata', 'timestamp'),   # 高频采样
    TableSpec('runlidata', 'timestamp'),  # 低频采样
]

# 读取并对齐
aligned_data = collect_and_align(
    reader,
    specs,
    anchor='tempdata',  # 以 tempdata 为基准
    tolerance='1s'      # 容差 1 秒
)

print(f"对齐后: {len(aligned_data)} 行, {len(aligned_data.columns)} 列")
print(aligned_data.columns)

reader.close()
```

**输出示例：**
```
对齐后: 10000 行, 12 列
Index(['tempdata__Temperature', 'tempdata__Temperature2',
       'runlidata__Pressure1', 'runlidata__Pressure2', ...])
```

## 示例 4：时间索引数据缓存 ⭐

累积历史数据并进行灵活查询（新功能）：

```python
from sc_reader import SCReader, TableSpec, AlignedDataCache

reader = SCReader(state_path='./watermark.json')

specs = [
    TableSpec('tempdata', 'timestamp'),
    TableSpec('runlidata', 'timestamp'),
]

# 创建缓存
cache = AlignedDataCache(
    reader,
    specs,
    anchor='tempdata',
    tolerance='1s',
    max_memory_mb=100.0  # 限制内存 100MB
)

# 首次加载
n = cache.update()
print(f"加载 {n} 行，内存 {cache.memory_usage_mb:.1f}MB")

# 时间索引查询
df_day = cache['2025-12-15':'2025-12-16']
print(f"2025-12-15 数据: {len(df_day)} 行")

df_hour = cache.loc['2025-12-15 10:00:00':'2025-12-15 11:00:00']
print(f"10:00-11:00 数据: {len(df_hour)} 行")

# pandas 操作
resampled = cache.data.resample('1min').mean()
print(f"重采样: {len(resampled)} 行")

# 持久化
cache.save('./cache.parquet')
print("缓存已保存")

reader.close()
```

**输出示例：**
```
加载 10000 行，内存 12.5MB
2025-12-15 数据: 8640 行
10:00-11:00 数据: 360 行
重采样: 1440 行
缓存已保存
```

## 示例 5：事件检测

检测特定事件并分析：

```python
from sc_reader import (
    SCReader,
    EventDetector,
    TriggerType,
    run_event_monitor
)

reader = SCReader(state_path='./event_watermark.json')

# 创建事件检测器
detector = EventDetector()

# 添加触发器
detector.add_edge_trigger(
    'valve_open',
    'statedata',
    'Valve_N2',
    TriggerType.RISING_EDGE  # 0 -> 1
)

detector.add_step_trigger(
    'temp_change',
    'runlidata',
    'coldwater_Set',
    threshold=0.5  # 变化 >= 0.5
)

# 事件回调
def on_event(df, event):
    print(f"检测到事件: {event.event_type} at {event.event_time}")
    print(f"窗口数据: {len(df)} 行")
    df.to_csv(f'event_{event.event_id}.csv')

# 运行监控
run_event_monitor(reader, detector, on_event)
```

## 示例 6：数据可视化

快速绘制图表：

```python
from sc_reader import SCReader
from sc_reader.visualizer import plot_timeseries
import matplotlib.pyplot as plt

reader = SCReader()
data = reader.query_by_time('tempdata', '2025-12-15', '2025-12-16')

# 单变量时间序列图
fig1, ax1 = plot_timeseries(data, 'Temperature', title='Temperature Over Time')
fig1.savefig('temp_timeseries.png')

# 多变量对比图
temp_cols = ['Temperature', 'Temperature2', 'Temperature3']
fig2, ax2 = plot_timeseries(data, column=temp_cols, title='Temperature Comparison')
fig2.savefig('temp_comparison.png')

reader.close()
```

## 完整工作流示例

将上述功能组合使用：

```python
from sc_reader import SCReader, TableSpec, AlignedDataCache
import time

# 1. 创建读取器和缓存
reader = SCReader(state_path='./watermark.json')
specs = [
    TableSpec('tempdata', 'timestamp'),
    TableSpec('runlidata', 'timestamp'),
]
cache = AlignedDataCache(reader, specs, anchor='tempdata', max_rows=10000)

# 2. 首次加载
cache.update()
print(f"初始数据: {len(cache)} 行")

# 3. 持续更新循环
for i in range(10):
    time.sleep(5)

    # 增量更新
    new_rows = cache.update()
    print(f"轮询 #{i+1}: 新增 {new_rows} 行, 总计 {len(cache)} 行")

    # 分析最近数据
    recent = cache.iloc[-100:]
    print(f"  最近 100 行平均温度: {recent['tempdata__Temperature'].mean():.2f}")

# 4. 保存缓存
cache.save('./final_cache.parquet')

reader.close()
```

## 下一步

现在您已经了解了 SC_Reader 的基本用法，可以：

- 查看 [用户指南](user_guide/01_basic_usage.md) 深入学习各个功能
- 查看 [API 参考](api_reference.md) 了解完整的函数和参数
- 运行项目的 [示例脚本](../example/) 查看更多用例
- 查看 [数据缓存指南](user_guide/04_data_caching.md) 学习高级缓存功能

## 常见问题

**Q: 如何处理大数据集？**

A: 使用 `AlignedDataCache` 的内存限制选项，或使用 `chunksize` 参数分批处理。

**Q: 支持实时流式处理吗？**

A: 支持。使用 `SCReader` 配合定时器可以实现准实时处理。

**Q: 如何自定义配置？**

A: 参考 [配置说明](configuration.md) 了解所有配置选项。

**Q: 遇到连接错误怎么办？**

A: 检查数据库连接参数，确保网络可达，并查看 [安装指南](installation.md) 的故障排除部分。
