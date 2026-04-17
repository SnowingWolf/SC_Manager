# AlignedDataCache 性能优化说明

## 优化内容

### 1. 优化 _frame_signature() 哈希计算（高优先级）

**问题：** 原实现对整个 DataFrame 做全量哈希 `hash_pandas_object(df, index=True)`，大表下成本很高。

**优化：** 改用轻量签名 `(rows, min_ts, max_ts, last_row_hash)`，只对最后一行做哈希。

**收益：** 大表下哈希时间从 O(n) 降至 O(1)，显著降低 update() 开销。

**位置：** `sc_reader/cache.py:179-200`

---

### 2. 添加保存频率控制（高优先级）

**问题：** 如果 `auto_save=True`，每次 update() 都保存，大表下 save_s 成为瓶颈。

**优化：** 添加两个参数：
- `save_every_n_updates`: 每 N 次 update 保存一次
- `save_min_interval_s`: 最小保存间隔（秒）

**使用示例：**
```python
cache = AlignedData(
    reader, specs, anchor='tempdata',
    cache_path='./cache.parquet',
    auto_save=True,
    save_every_n_updates=10,      # 每 10 次 update 保存一次
    save_min_interval_s=60.0,     # 或至少间隔 60 秒
)
```

**位置：** `sc_reader/cache.py:70-152, 417-432`

---

### 3. 优化 read_multiple() 复用连接和缓存（高优先级）

**问题：** 原实现每个表都创建临时 SCReader 和独立连接，丢失表结构缓存（`_table_info_cache`, `_time_col_cache`），导致每次都重新探测时间列和执行 DESCRIBE。

**优化：** 改为顺序读取，复用主 reader 的连接和缓存。

**收益：** 
- 避免重复连接开销
- 复用表结构缓存，减少 DESCRIBE 查询
- 复用时间列探测结果

**权衡：** 从并行改为顺序，但实际测试中，连接和缓存复用的收益通常大于并行的收益（特别是表数量不多时）。

**位置：** `sc_reader/reader.py:805-823`

---

### 4. 减少重复规范化（中等收益）

**问题：** `read_incremental()` 已返回规范化的 DatetimeIndex，但 `_prepare_frame()` 还会重复检查/排序。

**优化：** 在 `align_asof()` 和 `_prepare_frame()` 中添加 `assume_normalized` 参数，跳过重复检查。

**使用：** `AlignedDataCache` 内部自动使用 `assume_normalized=True`。

**位置：** `sc_reader/align.py:49-73, 208-240`

---

### 5. 利用 changed_tables 优化对齐（中等收益）

**问题：** 即使只有非 anchor 表变化，也会对所有表重新对齐。

**优化：** 在 `_align_incremental()` 中利用 `changed_tables`，只对变化的表重新对齐。

**收益：** 当 anchor 无新增、只有非 anchor 表晚到数据时，减少不必要的对齐计算。

**位置：** `sc_reader/cache.py:276-318`

---

## 性能提升预期

根据优化分析文档，按优先级排序：

1. **read_multiple 连接/缓存复用**：如果 read_s 高，这是第一优先级
2. **_frame_signature 降成本**：大表下收益显著，从 O(n) 降至 O(1)
3. **保存频率控制**：如果查询频繁，save_s 很容易成为主瓶颈
4. **changed_tables 利用**：中等收益，特别是非 anchor 表晚到数据的场景
5. **减少重复规范化**：小幅优化，累积收益

---

## 数据库侧优化建议（需用户自行实施）

1. **添加索引**：给增量查询配 `(timestamp)` 或 `(timestamp, id)` 索引
   ```sql
   CREATE INDEX idx_timestamp ON tempdata(timestamp);
   ```

2. **时间列类型**：piddata 等字符串时间列改成 DATETIME 或添加 generated column + index

3. **spec.cols 优化**：尽量指定需要的列，避免 `SELECT *`

---

## 使用建议

### 高频轮询场景
```python
cache = AlignedData(
    reader, specs, anchor='tempdata',
    cache_path='./cache.parquet',
    auto_save=True,
    save_every_n_updates=10,      # 每 10 次保存
    save_min_interval_s=60.0,     # 或至少 60 秒
    timing_log=True,              # 开启性能日志
)

# 轮询
while True:
    new_rows = cache.update()
    print(f"新增 {new_rows} 行")
    time.sleep(5)
```

### 查看性能日志
```python
# 开启环境变量
import os
os.environ['SC_ALIGNEDDATA_TIMING'] = '1'

# 或初始化时指定
cache = AlignedData(..., timing_log=True)

# update() 会打印分段耗时
cache.update()
# 输出示例：
# [AlignedData.update] force_full=False read=0.1234s align=0.0567s 
# merge=0.0123s memory=0.0001s save=0.0000s total=0.1925s 
# rows_in=100 rows_out=95 new_rows=95 cache_rows=1000
```

### 查看统计信息
```python
print(cache.stats)
# 包含 last_update_timing 字段，显示上次 update 的各阶段耗时
```
