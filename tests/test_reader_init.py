from datetime import datetime
from pathlib import Path

from sc_reader.reader import SCReader


class DummyCursor:
    def __init__(self):
        self.connection = object()

    def close(self):
        pass


class DummyConnection:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def cursor(self):
        return DummyCursor()

    def ping(self, reconnect=True):
        return None

    def close(self):
        pass


def install_fake_connect(monkeypatch):
    calls = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return DummyConnection(**kwargs)

    monkeypatch.setattr("sc_reader.reader.pymysql.connect", fake_connect)
    return calls


def test_sc_reader_accepts_config_path_and_kwargs_override(tmp_path, monkeypatch):
    calls = install_fake_connect(monkeypatch)
    config_path = tmp_path / "sc_config.json"
    config_path.write_text(
        (
            '{\n'
            '  "mysql": {\n'
            '    "host": "db.example",\n'
            '    "port": 3307,\n'
            '    "user": "reader",\n'
            '    "password": "from-file",\n'
            '    "database": "slowcontrol",\n'
            '    "charset": "utf8mb4"\n'
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    reader = SCReader(config=config_path, password="override")

    assert calls == [
        {
            "host": "db.example",
            "port": 3307,
            "user": "reader",
            "password": "override",
            "database": "slowcontrol",
            "charset": "utf8mb4",
        }
    ]
    assert reader._host == "db.example"
    assert reader._password == "override"


def test_save_state_accepts_path_and_creates_parent_dir(tmp_path, monkeypatch):
    install_fake_connect(monkeypatch)
    state_path = tmp_path / "nested" / "watermarks" / "state.json"

    reader = SCReader(state_path=state_path)
    reader._watermarks["tempdata"] = {
        "last_ts": datetime(2026, 4, 17, 12, 30, 0),
        "last_id": 7,
    }

    reader.save_state()

    assert state_path.exists()

    restored = SCReader(state_path=Path(state_path))
    assert restored.state_path == state_path
    assert restored._watermarks["tempdata"]["last_ts"] == datetime(2026, 4, 17, 12, 30, 0)
    assert restored._watermarks["tempdata"]["last_id"] == 7
