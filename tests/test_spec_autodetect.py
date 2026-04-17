import pandas as pd

from sc_reader.reader import SCReader
from sc_reader.spec import TableSpec


class DummyCursor:
    def __init__(self):
        self.connection = object()

    def close(self):
        pass


class DummyConnection:
    def cursor(self):
        return DummyCursor()

    def ping(self, reconnect=True):
        return None

    def close(self):
        pass


def install_fake_connect(monkeypatch):
    monkeypatch.setattr("sc_reader.reader.pymysql.connect", lambda **kwargs: DummyConnection())


def test_read_incremental_auto_detects_time_column_when_omitted(monkeypatch):
    install_fake_connect(monkeypatch)
    reader = SCReader()
    recorded = {}

    def fake_get_time_column(table):
        recorded["table"] = table
        return "timestamp"

    def fake_query_df(sql, time_column=None, chunksize=None, time_unit=None, time_zone=None):
        recorded["sql"] = sql
        recorded["time_column"] = time_column
        return pd.DataFrame(
            {"value": [1.0, 2.0]},
            index=pd.to_datetime(["2026-04-17 12:00:00", "2026-04-17 12:00:01"]),
        ).rename_axis("timestamp")

    monkeypatch.setattr(reader, "_get_time_column", fake_get_time_column)
    monkeypatch.setattr(reader, "_query_df", fake_query_df)

    df = reader.read_incremental(TableSpec("tempdata"))

    assert recorded["table"] == "tempdata"
    assert recorded["time_column"] == "timestamp"
    assert "ORDER BY `timestamp`" in recorded["sql"]
    assert list(df.columns) == ["value"]
    assert reader._watermarks["tempdata"]["last_ts"] is not None


def test_read_multiple_preserves_auto_detect_specs(monkeypatch):
    install_fake_connect(monkeypatch)
    reader = SCReader()
    seen = []

    def fake_read_incremental(self, spec, lookback="2s", chunksize=None):
        seen.append((spec.table, spec.time_col, lookback))
        self._watermarks[spec.table] = {
            "last_ts": pd.Timestamp("2026-04-17 12:00:00").to_pydatetime(),
            "last_id": None,
        }
        return pd.DataFrame(
            {"value": [1]},
            index=pd.DatetimeIndex([pd.Timestamp("2026-04-17 12:00:00")], name="timestamp"),
        )

    monkeypatch.setattr(SCReader, "read_incremental", fake_read_incremental)

    results = reader.read_multiple([TableSpec("tempdata"), TableSpec("runlidata")], lookback="10s")

    assert set(results) == {"tempdata", "runlidata"}
    assert sorted(seen) == [
        ("runlidata", None, "10s"),
        ("tempdata", None, "10s"),
    ]
    assert set(reader._watermarks) == {"tempdata", "runlidata"}
