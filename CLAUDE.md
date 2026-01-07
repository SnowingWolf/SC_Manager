# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SC_Manager is a Python library for reading, analyzing, and visualizing slow control system data from a MySQL database. It's designed for scientific monitoring applications tracking environmental parameters (temperature, pressure, flow rates) over time.

**Key Features:**
- Full table queries and time-range queries
- Incremental reading with watermark tracking
- Multi-table time alignment (merge_asof)
- Time-indexed data caching with pandas-style access
- Event detection (edge triggers, step triggers)
- Event window reading with relative time
- Data visualization

## Commands

```bash
# Run examples
python3 example/basic_usage.py      # Basic usage demo
python3 example/incremental.py      # Incremental reading demo
python3 example/cache_demo.py       # Cache management demo
python3 example/event_monitor.py    # Event detection demo

# Check Python syntax
python3 -m py_compile sc_reader/*.py

# Import test
python3 -c "from sc_reader import SCReader, SCReader, TableSpec; print('Success')"

# Open Jupyter tutorial
jupyter notebook tutorial_slowcontrol.ipynb
```

## Dependencies

```bash
pip install pymysql pandas matplotlib seaborn numpy sqlalchemy pyarrow
```

## Architecture

```
SC_Manager/
├── sc_reader/
│   ├── __init__.py       # Public API exports
│   ├── config.py         # Configuration (JSON + env vars)
│   ├── spec.py           # TableSpec dataclass
│   ├── reader.py         # SCReader (full queries)
│   ├── incremental.py    # SCReader (watermark-based)
│   ├── align.py          # Time alignment (merge_asof)
│   ├── cache.py          # AlignedDataCache (time-indexed caching)
│   ├── event.py          # Event detection and window reading
│   └── visualizer.py     # 9 plotting functions
├── example/
│   ├── basic_usage.py    # SCReader demo
│   ├── incremental.py    # SCReader + alignment demo
│   ├── cache_demo.py     # AlignedDataCache demo
│   └── event_monitor.py  # Event detection demo
├── connect_mysql.py      # Low-level MySQL wrapper
├── sc_config.example.json # Configuration template
└── tutorial_slowcontrol.ipynb
```

**Layer Structure:**
1. `MySQLReader` (connect_mysql.py) - Raw SQL execution
2. `SCReader` (sc_reader/reader.py) - Full table queries
3. `SCReader` (sc_reader/incremental.py) - Incremental reading with watermark
4. `align_asof` (sc_reader/align.py) - Multi-table time alignment
5. `AlignedDataCache` (sc_reader/cache.py) - Time-indexed data caching
6. `EventDetector` / `EventWindowReader` (sc_reader/event.py) - Event detection and window reading
7. Visualizer functions (sc_reader/visualizer.py) - Plotting utilities

## Configuration

Configuration supports JSON file and environment variables:

```bash
# Copy and edit config
cp sc_config.example.json sc_config.json
```

```json
{
  "mysql": {
    "host": "10.11.50.141",
    "port": 3306,
    "user": "read",
    "password": "your_password",
    "database": "slowcontroldata"
  },
  "align": {
    "tolerance": "200ms",
    "lookback": "2s"
  }
}
```

Config lookup priority:
1. Explicit path argument
2. `SC_CONFIG_PATH` environment variable
3. `./sc_config.json`
4. `~/.sc_config.json`
5. Default values

## Usage Examples

### Basic Query (SCReader)

```python
from sc_reader import SCReader

reader = SCReader()  # Uses config from JSON or defaults
tables = reader.list_tables()
data = reader.query_by_time('table_name', '2025-01-01', '2025-01-31')
reader.close()
```

### Incremental Reading + Time Alignment

```python
from sc_reader import SCReader, TableSpec, collect_and_align

reader = SCReader(state_path='./watermark.json')

specs = [
    TableSpec('temperature', 'Time(s)'),
    TableSpec('pressure', 'Time(s)'),
]

# Read incrementally and align by time
df = collect_and_align(reader, specs, anchor='temperature', tolerance='200ms')
reader.close()
```

### Custom Config

```python
from sc_reader import MySQLConfig, SCReader

config = MySQLConfig.from_json('./my_config.json')
# or: config = MySQLConfig(host='192.168.4.19')

reader = SCReader(config=config)
```

### Event Detection and Window Reading

```python
from sc_reader import (
    SCReader, EventDetector, TriggerType,
    WindowConfig, run_event_monitor
)

reader = SCReader(state_path='./event_watermark.json')

# Create event detector
detector = EventDetector()
detector.add_edge_trigger('valve_open', 'statedata', 'Valve_N2', TriggerType.RISING_EDGE)
detector.add_edge_trigger('valve_close', 'statedata', 'Valve_N2', TriggerType.FALLING_EDGE)
detector.add_step_trigger('coldwater_change', 'runlidata', 'coldwater_Set', threshold=0.5)

# Window configuration
window_config = WindowConfig(
    pre_seconds=30.0,    # 30s before event
    post_seconds=120.0,  # 120s after event
    anchor_table='tempdata',
    ffill_tables=['statedata']
)

# Monitor and handle events
def on_event(df, event):
    # df contains aligned data with t_seconds column (relative to event time)
    df.to_csv(f'event_{event.event_id}_{event.event_type}.csv')

run_event_monitor(reader, detector, on_event, window_config)
```

Event window output columns:
- `t_seconds`: time relative to event (negative = before, positive = after)
- `timestamp`: absolute timestamp
- `tempdata__Temperature1`, `runlidata__Pressure1`, etc.: prefixed columns from each table
- `statedata__*` columns are forward-filled for state continuity

### Time-Indexed Data Caching

```python
from sc_reader import SCReader, TableSpec, AlignedDataCache

reader = SCReader(state_path='./watermark.json')
specs = [
    TableSpec('tempdata', 'timestamp'),
    TableSpec('runlidata', 'timestamp'),
]

# Create cache with memory management
cache = AlignedDataCache(
    reader,
    specs,
    anchor='tempdata',
    tolerance='1s',
    max_memory_mb=100.0  # Limit memory to 100MB
)

# Load data
n = cache.update()  # Initial load
n = cache.update()  # Incremental update

# Time indexing (pandas-style)
df = cache['2025-12-15':'2025-12-16']         # Time slice
point = cache.loc['2025-12-15 10:00:00']       # Specific time

# Pandas operations
resampled = cache.data.resample('1min').mean() # Resample
rolling = cache.data.rolling('10min').std()    # Rolling stats

# Persistence
cache.save('./cache.parquet')                  # Save
cache.load('./cache.parquet')                  # Load
cache.load('./cache.parquet', merge=True)      # Merge with existing

# Stats
print(cache.stats)  # Memory, time range, updates, etc.
```

## Key Patterns

- All query methods return pandas DataFrames with timestamp as index
- Visualization functions: `(data, ...) -> Tuple[plt.Figure, plt.Axes]`
- `TableSpec` defines table name, time column, columns to read
- `SCReader` tracks watermark per table for incremental reads
- `align_asof` uses pandas merge_asof for time-based joining
- `AlignedDataCache` accumulates data with pandas-style time indexing
- `EventDetector` supports edge triggers (rising/falling) and step triggers
- `EventWindowReader` reads time windows around events with aligned data
- Matplotlib backend set to 'Agg' for server-side rendering

## Alternative Hosts

- `10.11.50.141` (default, campus network)
- `192.168.4.19` (lab internal)
