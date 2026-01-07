# SC_Reader 文档

欢迎使用 SC_Reader 文档！这是一个用于读取、分析和可视化慢控系统数据的 Python 库。

## 功能概览

### 核心功能

- **全量查询**：读取完整表或按时间范围查询
- **增量读取**：基于 watermark 机制的增量数据同步
- **多表时间对齐**：使用 `pandas.merge_asof` 对齐不同采样率的数据
- **时间索引缓存**：累积历史数据并支持 pandas 风格的时间索引访问
- **事件检测**：检测边沿触发和阶跃触发事件
- **事件窗口读取**：提取事件前后的时间窗口数据
- **数据可视化**：内置绘图函数用于常见分析

### 主要类

| 类名 | 功能 | 文档链接 |
|------|------|---------|
| `SCReader` | 全量数据读取 | [基础用法](user_guide/01_basic_usage.md) |
| `SCReader` | 增量数据读取 | [增量读取](user_guide/02_incremental_reading.md) |
| `AlignedDataCache` | 时间索引数据缓存 | [数据缓存](user_guide/04_data_caching.md) |
| `EventDetector` | 事件检测 | [事件检测](user_guide/05_event_detection.md) |

## 快速导航

### 新手入门

1. [安装指南](installation.md) - 如何安装 SC_Reader
2. [快速开始](quickstart.md) - 5 分钟快速上手
3. [配置说明](configuration.md) - 配置数据库连接

### 用户指南

详细的功能介绍和使用方法：

- [01. 基础用法](user_guide/01_basic_usage.md) - SCReader 基本使用
- [02. 增量读取](user_guide/02_incremental_reading.md) - SCReader 和 watermark 机制
- [03. 时间对齐](user_guide/03_time_alignment.md) - 多表数据对齐
- [04. 数据缓存](user_guide/04_data_caching.md) - AlignedDataCache 时间索引缓存 ⭐
- [05. 事件检测](user_guide/05_event_detection.md) - 事件检测和窗口读取
- [06. 数据可视化](user_guide/06_visualization.md) - 绘图和可视化分析

### 参考文档

- [API 参考](api_reference.md) - 完整的 API 文档
- [示例代码](examples.md) - 代码示例集合

## 典型使用场景

### 场景 1：定期数据分析

使用 `SCReader` 定期读取数据并生成报告：

```python
from sc_reader import SCReader

reader = SCReader()
data = reader.query_by_time('tempdata', '2025-12-15', '2025-12-26')
print(data.describe())
reader.close()
```

### 场景 2：实时数据监控

使用 `SCReader` 持续监控数据更新：

```python
from sc_reader import SCReader, TableSpec

reader = SCReader(state_path='./watermark.json')
spec = TableSpec('tempdata', 'timestamp')

while True:
    new_data = reader.read_incremental(spec)
    if not new_data.empty:
        print(f"New data: {len(new_data)} rows")
    time.sleep(5)
```

### 场景 3：历史数据缓存和查询

使用 `AlignedDataCache` 累积历史数据并进行灵活查询：

```python
from sc_reader import SCReader, TableSpec, AlignedDataCache

reader = SCReader()
specs = [TableSpec('tempdata', 'timestamp'), TableSpec('runlidata', 'timestamp')]
cache = AlignedDataCache(reader, specs, anchor='tempdata')

cache.update()  # 加载数据

# 时间索引查询
df = cache['2025-12-15':'2025-12-16']
resampled = cache.data.resample('1min').mean()
```

### 场景 4：事件驱动分析

使用 `EventDetector` 检测特定事件并分析响应：

```python
from sc_reader import EventDetector, TriggerType, run_event_monitor

detector = EventDetector()
detector.add_edge_trigger('valve_open', 'statedata', 'Valve_N2', TriggerType.RISING_EDGE)

def on_event(df, event):
    # 分析事件窗口数据
    print(f"Event: {event.event_type} at {event.event_time}")

run_event_monitor(reader, detector, on_event)
```

## 数据流架构

```
MySQL Database
      ↓
  SCReader (全量读取)
      ↓
  SCReader (增量读取 + watermark)
      ↓
  align_asof (多表时间对齐)
      ↓
  AlignedDataCache (时间索引缓存)
      ↓
  pandas operations (分析 / 可视化)
```

## 性能提示

1. **增量读取**：对于持续更新的表，使用 `SCReader` 而不是 `SCReader`
2. **时间对齐**：设置合适的 `tolerance` 参数以平衡精度和性能
3. **内存管理**：使用 `AlignedDataCache` 的内存限制选项避免内存溢出
4. **批量操作**：对于大量数据，使用 `chunksize` 参数分批处理
5. **数据持久化**：使用 Parquet 格式保存缓存数据，提高读写速度

## 常见问题

**Q: 如何处理不同采样率的数据？**

A: 使用 `align_asof` 函数或 `AlignedDataCache` 类，它们会自动对齐不同采样率的数据。

**Q: 增量读取会遗漏数据吗？**

A: 不会。`SCReader` 使用 lookback 窗口机制确保不遗漏乱序写入的数据。

**Q: 如何限制缓存的内存占用？**

A: 使用 `AlignedDataCache` 的 `max_memory_mb`、`max_rows` 或 `time_window_days` 参数。

**Q: 支持哪些数据库？**

A: 目前支持 MySQL/MariaDB，未来可能支持其他数据库。

## 获取帮助

- 查看 [示例代码](examples.md) 获取更多用法
- 阅读 [API 参考](api_reference.md) 了解详细参数
- 查看项目的 [GitHub Issues](https://github.com/sc-manager/sc-reader/issues)

## 下一步

- 如果您是新用户，建议从 [快速开始](quickstart.md) 开始
- 如果想深入了解某个功能，查看对应的 [用户指南](user_guide/01_basic_usage.md)
- 如果需要查找特定 API，参考 [API 参考](api_reference.md)
