# 相图计算（P-T Phase Diagram）

本页总结 SC_Reader 中“相变线/相态判定”的**计算逻辑**，对应实现位于 `sc_reader/phase_diagram.py`。
相变线**不是从数据库读取**，而是由公式和气体常数计算得到。

## 1. 输入与单位

- 温度 `T`：开尔文（K）
- 压力 `P`：bar
- 支持气体：`argon`、`xenon`（见 `GAS_PROPERTIES`）

## 2. 气体常数与参数

`GAS_PROPERTIES` 中给出每种气体的：

- 三相点：`T_triple`、`P_triple_bar`
- 临界点：`T_crit`、`P_crit_bar`
- 升华线拟合系数：`sublimation`（A1、A2）
- 饱和线拟合系数：`saturation`（A1、A2、A3、A4）

这些系数用于计算相界线（升华/饱和线），并参考 NIST 与文献拟合参数。

## 3. 相变线计算公式

### 3.1 升华线（固-气） `psub_bar(T)`

```
θ = T / T_triple
τ = 1 - θ
P_sub = P_triple * exp( (1/θ) * (A1 * τ + A2 * τ^(1.5)) )
```

适用范围：`T <= T_triple`。

### 3.2 饱和线（液-气） `psat_bar(T)`

```
θ = 1 - T / T_crit
P_sat = P_crit * exp( (T_crit / T) * (A1 * θ + A2 * θ^(1.5) + A3 * θ^2 + A4 * θ^(4.5)) )
```

适用范围：`T_triple < T <= T_crit`。

### 3.3 相界线自动选择 `phase_boundary_bar(T)`

- `T <= T_triple`：使用升华线 `psub_bar(T)`
- `T_triple < T <= T_crit`：使用饱和线 `psat_bar(T)`
- `T > T_crit`：返回 `NaN`（不定义相界线）

## 4. 相态判定逻辑 `get_phase(T, P)`

给定温度和压力，按以下规则判断相态：

1. **超临界**：`T > T_crit` 且 `P > P_crit`
2. **三相点以下**：`T <= T_triple`
   - 若 `P > P_sub(T)` → `solid`
   - 否则 → `gas`
3. **三相点到临界点之间**：`T_triple < T <= T_crit`
   - 若 `P > P_sat(T)` → `liquid`
   - 否则 → `gas`
4. **高温低压**：其余情况 → `gas`

## 5. 相图绘制时的计算细节

`plot_pt_path()` 会在绘图前进行如下计算：

- **默认范围**：
  - `T_range = (T_triple - 5, T_triple + 20)`
  - `P_range = (0.0, 3.0)`
- **相界线采样点数**：
  - Plotly：`boundary_points = 100`
  - Matplotlib：`boundary_points = 500`
- **相区填充**：使用 `psub_bar` / `psat_bar` 生成边界，并填充 solid/liquid/gas 区域
- **路径绘制**：`P(T)` 路径可选降采样 `downsample_max_points`

## 6. 快速调用示例

```python
from sc_reader import psub_bar, psat_bar, phase_boundary_bar, get_phase

P_sub = psub_bar(84.0, gas='argon')
P_sat = psat_bar(100.0, gas='argon')
P_bnd = phase_boundary_bar([80.0, 100.0], gas='argon')
phase = get_phase(90.0, 1.0, gas='argon')
```

## 7. 常见问题

**Q: 为什么高于临界点没有相界线？**

A: 超临界区不区分气/液相，`phase_boundary_bar()` 返回 `NaN`，仅用 `get_phase()` 判断是否超临界。

**Q: 相变线和实际数据有什么关系？**

A: 相变线由公式计算得到；实际数据路径是从慢控数据中读取压力与温度列后绘制在相图上。
