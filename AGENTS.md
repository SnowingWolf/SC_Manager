# AGENTS.md

Operational notes for agents working in this repository.

## Scope

- Repo root: `/home/wxy/SC_Manager`
- Main package: `sc_reader`
- Primary interpreter: `/home/wxy/anaconda3/envs/pyroot-kernel/bin/python`

## Source Of Truth For Data

- Remote source: MySQL (via `SCReader`).
- Local raw cache: parquet export managed by `export_to_parquet.py`.
- Local aligned cache: optional parquet file managed by `AlignedData(cache_path=..., auto_load=True, auto_save=True)`.

Important behavior:

- If `AlignedData` is created without `cache_path`, update reads remote data every run.
- `update(force_full=True)` resets watermarks and performs full re-read + full alignment.
- `update(force_full=False)` performs incremental read and incremental-anchor alignment.

## Performance-Critical Architecture

### 1) Alignment engine (`sc_reader/align.py`)

- `align_asof(...)` uses a Numba JIT linear matcher.
- Supported directions: `backward`, `forward`, `nearest`.
- `nearest` tie-break prefers backward to match pandas behavior.
- Tolerance is applied in matcher output (`-1` means unmatched, later filled as `NaN`).

### 2) Incremental alignment (`sc_reader/cache.py`)

- `AlignedData.update(force_full=False)` aligns only incremental anchor timestamps.
- Optional conservative tail recompute:
- `tail_recompute` (default `False`)
- `tail_recompute_window` (defaults to `lookback` if not set)

### 3) Incremental export with delta + compaction (`scripts/export_to_parquet.py`)

- Base file: `output/<table>.parquet`
- Delta shards: `output/_delta/<table>/delta_*.parquet`
- Incremental runs append to delta shards first.
- Automatic compaction triggers when either threshold is reached:
- `--compact-threshold-files` (default `20`)
- `--compact-threshold-rows` (default `2000000`)
- Manual compaction: `--compact [--tables ...]`

## Timing Diagnostics

`AlignedData.update()` has staged timing logs.

- Enable by constructor: `timing_log=True`
- Or env var: `SC_ALIGNEDDATA_TIMING=1`
- Stages: `read`, `align`, `merge`, `memory`, `save`, `total`
- Programmatic access: `cache.stats["last_update_timing"]`

## Standard Commands

### Export status

```bash
/home/wxy/anaconda3/envs/pyroot-kernel/bin/python scripts/export_to_parquet.py \
  --status --state ./export_watermark.json --output ./data/parquet
```

### Incremental export for selected tables

```bash
/home/wxy/anaconda3/envs/pyroot-kernel/bin/python scripts/export_to_parquet.py \
  --config ./sc_config.json \
  --output ./data/parquet \
  --state ./export_watermark.json \
  --tables piddata runlidata statedata
```

### Manual compaction

```bash
/home/wxy/anaconda3/envs/pyroot-kernel/bin/python scripts/export_to_parquet.py \
  --compact --tables piddata \
  --state ./export_watermark.json \
  --output ./data/parquet
```

### Aligned cache (local-first incremental)

```python
cache = AlignedData(
    r,
    specs,
    anchor="piddata",
    lookback="10s",
    tolerance="10s",
    cache_path="/home/wxy/SC_Manager/data/parquet/aligned_piddata_cache.parquet",
    auto_load=True,
    auto_save=True,
    timing_log=True,
)
cache.update(force_full=False)
```

## Guardrails

- Keep edits focused unless user explicitly requests broader cleanup.
- Prefer compatibility with current public APIs.
- For performance issues, inspect timing logs before changing algorithms.
