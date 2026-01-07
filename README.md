# SC_Reader

A Python library for reading, analyzing, and visualizing slow control system data from MySQL databases.

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-1.3.0-orange.svg)](https://github.com/sc-manager/sc-reader)

## Features

- **Full Table Queries**: Read complete tables or time-range subsets
- **Incremental Reading**: Watermark-based incremental data sync
- **Multi-Table Time Alignment**: Align data from multiple tables using `pandas.merge_asof`
- **Time-Indexed Data Caching**: Accumulate and query data with pandas-style time indexing
- **Event Detection**: Detect events with edge triggers and step triggers
- **Event Window Reading**: Extract time windows around detected events
- **Data Visualization**: Built-in plotting functions for common analyses

## Quick Start

### Installation

```bash
pip install pymysql pandas matplotlib seaborn numpy sqlalchemy pyarrow
```

### Basic Usage

```python
from sc_reader import SCReader

# Connect to database
reader = SCReader()

# Query data by time range
data = reader.query_by_time(
    'tempdata',
    '2025-12-15',
    '2025-12-26'
)

print(data.head())
reader.close()
```

### Incremental Reading

```python
from sc_reader import SCReader, TableSpec

reader = SCReader(state_path='./watermark.json')
spec = TableSpec('tempdata', 'timestamp')

# First call: reads all historical data
df1 = reader.read_incremental(spec)

# Second call: reads only new data
df2 = reader.read_incremental(spec)

reader.close()
```

### Time-Indexed Data Caching

```python
from sc_reader import SCReader, TableSpec, AlignedDataCache

reader = SCReader(state_path='./watermark.json')
specs = [
    TableSpec('tempdata', 'timestamp'),
    TableSpec('runlidata', 'timestamp'),
]

# Create cache
cache = AlignedDataCache(
    reader,
    specs,
    anchor='tempdata',
    max_memory_mb=100.0
)

# Load data
cache.update()

# Time indexing (pandas-style)
df = cache['2025-12-15':'2025-12-16']
point = cache.loc['2025-12-15 10:00:00']

# Pandas operations
resampled = cache.data.resample('1min').mean()
rolling = cache.data.rolling('10min').std()

# Save/load
cache.save('./cache.parquet')
cache.load('./cache.parquet')
```

### Event Detection

```python
from sc_reader import (
    SCReader, EventDetector,
    TriggerType, run_event_monitor
)

reader = SCReader(state_path='./watermark.json')

# Create event detector
detector = EventDetector()
detector.add_edge_trigger(
    'valve_open',
    'statedata',
    'Valve_N2',
    TriggerType.RISING_EDGE
)

# Monitor events
def on_event(df, event):
    print(f"Event detected: {event}")
    df.to_csv(f'event_{event.event_id}.csv')

run_event_monitor(reader, detector, on_event)
```

## Architecture

```
SC_Manager/
├── sc_reader/
│   ├── reader.py         # SCReader (full queries)
│   ├── incremental.py    # SCReader (watermark-based)
│   ├── align.py          # Time alignment (merge_asof)
│   ├── cache.py          # AlignedDataCache (time-indexed caching)
│   ├── event.py          # Event detection and window reading
│   ├── visualizer.py     # Plotting functions
│   └── config.py         # Configuration management
├── example/
│   ├── basic_usage.py    # Basic usage demo
│   ├── incremental.py    # Incremental reading demo
│   ├── cache_demo.py     # Cache management demo
│   └── event_monitor.py  # Event detection demo
└── docs/                 # Detailed documentation
```

## Documentation

- [Installation Guide](docs/installation.md)
- [Quick Start](docs/quickstart.md)
- [Configuration](docs/configuration.md)
- [API Reference](docs/api_reference.md)
- **User Guides:**
  - [Basic Usage](docs/user_guide/01_basic_usage.md)
  - [Incremental Reading](docs/user_guide/02_incremental_reading.md)
  - [Time Alignment](docs/user_guide/03_time_alignment.md)
  - [Data Caching](docs/user_guide/04_data_caching.md)
  - [Event Detection](docs/user_guide/05_event_detection.md)
  - [Visualization](docs/user_guide/06_visualization.md)

## Examples

Run the example scripts:

```bash
# Basic usage
python3 example/basic_usage.py

# Incremental reading
python3 example/incremental.py

# Data caching
python3 example/cache_demo.py

# Event monitoring
python3 example/event_monitor.py
```

## Configuration

Create a `sc_config.json` file:

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
    "direction": "backward",
    "lookback": "2s"
  }
}
```

Configuration priority:
1. Explicit path argument
2. `SC_CONFIG_PATH` environment variable
3. `./sc_config.json`
4. `~/.sc_config.json`
5. Default values

## Requirements

- Python >= 3.8
- pymysql >= 1.0.0
- pandas >= 1.3.0
- matplotlib >= 3.3.0
- seaborn >= 0.11.0
- numpy >= 1.20.0
- sqlalchemy >= 1.4.0
- pyarrow >= 10.0.0

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For bugs and feature requests, please create an issue on GitHub.

## Authors

SC_Manager Team
