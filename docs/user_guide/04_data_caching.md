# 数据缓存 - AlignedDataCache

`AlignedDataCache` 是 SC_Reader 1.3.0 引入的新功能，提供时间索引数据缓存管理，支持 pandas 风格的灵活查询。

## 概述

### 为什么需要数据缓存？

在慢控数据分析中，常见的需求包括：

1. **历史数据累积**：持续读取新数据并保留历史记录
2. **灵活查询**：按任意时间范围查询数据
3. **pandas 操作**：进行重采样、滚动统计等分析
4. **快速访问**：避免重复从数据库读取相同数据

`SCReader` 只返回增量数据，不保存历史。`AlignedDataCache` 在其基础上提供完整的缓存解决方案。

### 核心特性

- ✅ **累积历史数据**：自动合并增量数据到内存
- ✅ **时间索引查询**：支持 `cache['2025-12-15':'2025-12-16']` 语法
- ✅ **pandas 兼容**：支持 `resample()`, `rolling()` 等所有 pandas 操作
- ✅ **自动去重排序**：相同时间戳保留最新值
- ✅ **内存管理**：三种内存限制策略
- ✅ **数据持久化**：Parquet 格式保存/加载

## 基础用法

### 创建缓存

```python
from sc_reader import SCReader, TableSpec, AlignedDataCache

# 1. 创建增量读取器
reader = SCReader(state_path='./watermark.json')

# 2. 定义表规格
specs = [
    TableSpec('tempdata', 'timestamp'),
    TableSpec('runlidata', 'timestamp'),
]

# 3. 创建缓存
cache = AlignedDataCache(
    reader,
    specs,
    anchor='tempdata',      # 以 tempdata 为基准
    tolerance='1s',         # 时间对齐容差
    direction='backward',   # 对齐方向
    lookback='2s'          # 回看窗口
)
```

### 加载数据

```python
# 首次加载：读取所有历史数据
n = cache.update()
print(f"加载 {n} 行")

# 后续调用：只拉取增量数据并合并
n = cache.update()
print(f"新增 {n} 行")

# 查看缓存状态
print(cache)
# 输出: AlignedDataCache(rows=10000, cols=12, range=2025-12-15 00:00:00 to 2025-12-16 23:59:59, anchor='tempdata', memory=15.2MB)
```

## 时间索引查询

### 切片查询

```python
# 1. 日期范围切片
df_day = cache['2025-12-15':'2025-12-16']
print(f"获取 {len(df_day)} 行")

# 2. 具体时间范围
df_hour = cache['2025-12-15 10:00:00':'2025-12-15 11:00:00']

# 3. 单日数据
df_single = cache['2025-12-15']

# 4. 从某时间开始
df_from = cache['2025-12-15 12:00:00':]

# 5. 到某时间为止
df_until = cache[:'2025-12-15 12:00:00']
```

### 位置访问

```python
# 使用 loc（基于标签）
point = cache.loc['2025-12-15 10:00:00']
range_data = cache.loc['2025-12-15 10:00:00':'2025-12-15 11:00:00']

# 使用 iloc（基于位置）
first_100 = cache.iloc[:100]
last_100 = cache.iloc[-100:]
specific_rows = cache.iloc[100:200]
```

### 属性访问

```python
# 获取完整 DataFrame
df = cache.data

# 获取索引和列
index = cache.index       # DatetimeIndex
columns = cache.columns   # 列名列表
shape = cache.shape       # (行数, 列数)

# 统计信息
stats = cache.stats
print(stats)
# {
#     'total_rows': 10000,
#     'total_columns': 12,
#     'memory_mb': 15.2,
#     'time_range': (Timestamp('2025-12-15 00:00:00'), Timestamp('2025-12-16 23:59:59')),
#     'total_updates': 5,
#     'total_rows_added': 10000,
#     'last_update': datetime(2025, 12, 16, 14, 30, 0)
# }
```

## pandas 操作

由于缓存返回标准的 pandas DataFrame，您可以使用所有 pandas 功能：

### 重采样

```python
# 1 分钟平均
resampled_1min = cache.data.resample('1min').mean()

# 5 分钟最大值
resampled_5min = cache.data.resample('5min').max()

# 1 小时统计
hourly = cache.data.resample('1H').agg(['mean', 'std', 'min', 'max'])
```

### 滚动统计

```python
# 10 分钟滚动平均
rolling_mean = cache.data.rolling('10min').mean()

# 1 小时滚动标准差
rolling_std = cache.data.rolling('1H').std()

# 自定义窗口
rolling_custom = cache.data.rolling(window=100).apply(lambda x: x.max() - x.min())
```

### 数据分析

```python
# 描述性统计
print(cache.data.describe())

# 相关性分析
correlation = cache.data.corr()

# 缺失值检查
missing = cache.data.isnull().sum()

# 数据筛选
high_temp = cache.data[cache.data['tempdata__Temperature'] > 25]

# 分组聚合
grouped = cache.data.resample('1D').mean()
```

## 内存管理

`AlignedDataCache` 提供三种内存管理策略，可以单独使用或组合使用：

### 1. 时间窗口限制

只保留最近 N 天的数据：

```python
cache = AlignedDataCache(
    reader,
    specs,
    anchor='tempdata',
    time_window_days=7.0  # 只保留最近 7 天
)

cache.update()
# 超过 7 天的旧数据会自动删除
```

### 2. 行数限制

限制最大行数：

```python
cache = AlignedDataCache(
    reader,
    specs,
    anchor='tempdata',
    max_rows=100000  # 最多 100,000 行
)

cache.update()
# 超过限制时，保留最新的 100,000 行
```

### 3. 内存限制

限制最大内存占用：

```python
cache = AlignedDataCache(
    reader,
    specs,
    anchor='tempdata',
    max_memory_mb=200.0  # 最多占用 200MB 内存
)

cache.update()
# 内存超限时，自动删除旧数据并发出警告
```

### 组合使用

```python
cache = AlignedDataCache(
    reader,
    specs,
    anchor='tempdata',
    time_window_days=30.0,   # 最多保留 30 天
    max_rows=500000,         # 最多 50 万行
    max_memory_mb=500.0      # 最多 500MB
)

# 三个限制会依次应用：时间窗口 -> 行数 -> 内存
```

## 数据持久化

### 保存缓存

```python
# 保存到 Parquet 文件（推荐）
cache.save('./cache/aligned_data.parquet')

# 使用压缩
cache.save('./cache/aligned_data.parquet.gz', compression='gzip')

# 其他压缩算法
cache.save('./cache/aligned_data.parquet', compression='snappy')  # 默认，快速
cache.save('./cache/aligned_data.parquet', compression='brotli')  # 高压缩率
```

保存时会创建两个文件：
- `aligned_data.parquet` - 数据文件
- `aligned_data.meta.json` - 元数据（表信息、保存时间等）

### 加载缓存

```python
# 替换模式（清空现有数据）
cache = AlignedDataCache(reader, specs, anchor='tempdata')
cache.load('./cache/aligned_data.parquet')

# 合并模式（保留现有数据）
cache = AlignedDataCache(reader, specs, anchor='tempdata')
cache.update()  # 先加载当前数据
cache.load('./cache/aligned_data.parquet', merge=True)  # 合并历史数据
```

### 热启动模式

从文件恢复缓存并继续增量更新：

```python
cache = AlignedDataCache(reader, specs, anchor='tempdata')

# 1. 从文件加载历史数据
cache.load('./cache/aligned_data.parquet')
print(f"从文件加载: {len(cache)} 行")

# 2. 拉取最新增量数据
new_rows = cache.update()
print(f"增量更新: {new_rows} 行")

# 3. 继续正常使用
print(f"总计: {len(cache)} 行")
```

## 完整示例

### 示例 1：持续监控与缓存

```python
from sc_reader import SCReader, TableSpec, AlignedDataCache
import time

# 创建缓存
reader = SCReader(state_path='./watermark.json')
specs = [
    TableSpec('tempdata', 'timestamp'),
    TableSpec('runlidata', 'timestamp'),
]

cache = AlignedDataCache(
    reader,
    specs,
    anchor='tempdata',
    max_memory_mb=100.0,
    time_window_days=7.0
)

# 首次加载
cache.update()
print(f"初始数据: {len(cache)} 行, {cache.memory_usage_mb:.1f}MB")

# 持续监控
try:
    for i in range(100):
        time.sleep(10)  # 每 10 秒检查一次

        new_rows = cache.update()

        if new_rows > 0:
            print(f"[{i+1}] 新增 {new_rows} 行, 总计 {len(cache)} 行")

            # 分析最近 5 分钟数据
            recent = cache.iloc[-30:]
            temp_mean = recent['tempdata__Temperature'].mean()
            print(f"    最近温度平均: {temp_mean:.2f}")

        # 每 10 次循环保存一次
        if (i + 1) % 10 == 0:
            cache.save(f'./cache/backup_{i+1}.parquet')
            print(f"    已保存备份")

except KeyboardInterrupt:
    print("\n监控已停止")
finally:
    cache.save('./cache/final.parquet')
    reader.close()
```

### 示例 2：数据分析工作流

```python
from sc_reader import SCReader, TableSpec, AlignedDataCache
import matplotlib.pyplot as plt

# 1. 加载数据
reader = SCReader()
specs = [TableSpec('tempdata', 'timestamp'), TableSpec('runlidata', 'timestamp')]
cache = AlignedDataCache(reader, specs, anchor='tempdata')
cache.update()

# 2. 时间范围筛选
df_day = cache['2025-12-15']
print(f"分析 {len(df_day)} 行数据")

# 3. 重采样
hourly = df_day.resample('1H').mean()

# 4. 绘图
fig, axes = plt.subplots(2, 1, figsize=(12, 8))

# 温度曲线
hourly['tempdata__Temperature'].plot(ax=axes[0], title='Hourly Temperature')
axes[0].set_ylabel('Temperature')

# 压力曲线
hourly['runlidata__Pressure1'].plot(ax=axes[1], title='Hourly Pressure', color='orange')
axes[1].set_ylabel('Pressure')

plt.tight_layout()
plt.savefig('analysis_result.png')

reader.close()
```

### 示例 3：缓存恢复与分析

```python
from sc_reader import SCReader, TableSpec, AlignedDataCache

reader = SCReader()
specs = [TableSpec('tempdata', 'timestamp'), TableSpec('runlidata', 'timestamp')]

# 热启动
cache = AlignedDataCache(reader, specs, anchor='tempdata')
cache.load('./cache/backup.parquet')
cache.update()

print(f"缓存状态: {cache}")
print(f"时间范围: {cache.time_range}")
print(f"内存占用: {cache.memory_usage_mb:.1f}MB")

# 按月统计
monthly = cache.data.resample('1M').agg({
    'tempdata__Temperature': ['mean', 'std', 'min', 'max'],
    'runlidata__Pressure1': ['mean', 'std']
})

print("\n月度统计:")
print(monthly)

reader.close()
```

## 最佳实践

### 1. 选择合适的内存限制

```python
# 小数据集（几天）
cache = AlignedDataCache(reader, specs, anchor='tempdata', max_rows=10000)

# 中等数据集（几周）
cache = AlignedDataCache(reader, specs, anchor='tempdata', max_memory_mb=500.0)

# 大数据集（几个月）
cache = AlignedDataCache(reader, specs, anchor='tempdata',
                         time_window_days=30.0, max_memory_mb=2000.0)
```

### 2. 定期保存备份

```python
import time

while True:
    cache.update()

    # 每小时保存一次
    if time.time() % 3600 < 10:
        cache.save('./cache/hourly_backup.parquet')

    time.sleep(10)
```

### 3. 错误处理

```python
try:
    cache = AlignedDataCache(reader, specs, anchor='tempdata')
    cache.load('./cache/backup.parquet')
except FileNotFoundError:
    print("备份文件不存在，执行全量加载")
    cache = AlignedDataCache(reader, specs, anchor='tempdata')
    cache.update()
except Exception as e:
    print(f"加载失败: {e}")
    # 回退到全量加载
    cache = AlignedDataCache(reader, specs, anchor='tempdata')
    cache.update()
```

### 4. 监控内存使用

```python
cache = AlignedDataCache(reader, specs, anchor='tempdata', max_memory_mb=1000.0)

while True:
    cache.update()

    # 检查内存
    if cache.memory_usage_mb > 900:
        print(f"警告: 内存占用高 ({cache.memory_usage_mb:.1f}MB)")
        # 保存并清理
        cache.save('./cache/before_cleanup.parquet')
        cache.clear()
        print("缓存已清空")

    time.sleep(10)
```

## 与其他功能的集成

### 与事件检测结合

```python
from sc_reader import EventDetector, TriggerType

# 创建缓存
cache = AlignedDataCache(reader, specs, anchor='tempdata')
cache.update()

# 创建事件检测器
detector = EventDetector()
detector.add_edge_trigger('valve_open', 'statedata', 'Valve_N2', TriggerType.RISING_EDGE)

# 在缓存数据中检测事件
for table in ['statedata', 'runlidata']:
    # 获取该表的数据
    table_cols = [c for c in cache.columns if c.startswith(f'{table}__')]
    if table_cols:
        # 提取并检测
        events = detector.detect(cache.data[table_cols], table)
        print(f"{table}: 检测到 {len(events)} 个事件")
```

### 与可视化结合

```python
from sc_reader.visualizer import plot_multi_variables

# 提取温度列
temp_cols = [c for c in cache.columns if 'Temperature' in c]

# 绘制对比图
fig, ax = plot_multi_variables(
    cache['2025-12-15':'2025-12-16'],
    temp_cols,
    title='Temperature Comparison (Dec 15-16)'
)
fig.savefig('temp_comparison.png')
```

## 性能优化

### 1. 减少列数

只读取需要的列：

```python
specs = [
    TableSpec('tempdata', 'timestamp', cols=['Temperature', 'Temperature2']),
    TableSpec('runlidata', 'timestamp', cols=['Pressure1']),
]
```

### 2. 增加 lookback

减少数据遗漏：

```python
cache = AlignedDataCache(reader, specs, anchor='tempdata', lookback='5s')
```

### 3. 使用 Parquet 压缩

```python
cache.save('./cache/data.parquet', compression='brotli')  # 更小的文件
```

## 故障排除

### 问题 1：内存占用过高

**解决方案：**

```python
# 使用内存限制
cache = AlignedDataCache(reader, specs, anchor='tempdata', max_memory_mb=500.0)

# 或使用时间窗口
cache = AlignedDataCache(reader, specs, anchor='tempdata', time_window_days=7.0)
```

### 问题 2：数据丢失

**原因：** 内存限制导致旧数据被删除

**解决方案：**

```python
# 定期保存到文件
cache.save('./cache/backup.parquet')

# 或增加内存限制
cache = AlignedDataCache(reader, specs, anchor='tempdata', max_memory_mb=2000.0)
```

### 问题 3：查询结果为空

**原因：** 时间索引格式不正确

**解决方案：**

```python
# 使用正确的时间格式
df = cache['2025-12-15':'2025-12-16']  # 正确
# df = cache['12/15/2025':'12/16/2025']  # 错误
```

## 参考

- [API 参考 - AlignedDataCache](../api_reference.md#aligneddatacache)
- [增量读取指南](02_incremental_reading.md)
- [时间对齐指南](03_time_alignment.md)
- [示例代码](../examples.md#data-caching-examples)

## 总结

`AlignedDataCache` 提供了强大的时间索引数据缓存功能：

- ✅ 自动累积和去重
- ✅ pandas 风格的灵活查询
- ✅ 多种内存管理策略
- ✅ Parquet 格式持久化
- ✅ 热启动支持

结合 `SCReader`，您可以构建高效的实时数据分析系统。
