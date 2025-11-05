import types
from typing import List, Any
from lambda_py.db import MySqlRepository, ClientRecord, DeviceRecord

class _Cursor:
    def __init__(self, recorder):
        self.recorder = recorder
    def execute(self, sql, params=None):
        self.recorder.append(("execute", sql.strip(), params))
    def executemany(self, sql, seq):
        collected = list(seq)
        self.recorder.append(("executemany", sql.strip(), collected))
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False

class _Conn:
    def __init__(self, recorder):
        self.recorder = recorder
    def cursor(self):
        return _Cursor(self.recorder)
    def close(self):
        self.recorder.append(("close", None, None))

def test_upsert_clients_and_devices(monkeypatch):
    calls: List[Any] = []

    repo = MySqlRepository(
        host="test",
        port=3306,
        user="u",
        password="p",
        database="d"
    )

    # Monkeypatch internal connection handling
    conn = _Conn(calls)
    repo._conn = conn  # type: ignore[attr-defined]

    clients: List[ClientRecord] = [
        {
            "mac_address": "AA:BB:CC:DD:EE:FF",
            "site_id": "site1",
            "client_name": "cl1",
            "ip_address": "10.0.0.1",
            "device_type": "laptop",
            "status": "Up",
            "associated_device": "ap1",
            "signal_strength": 42,
            "last_seen": "1700000000"  # epoch string
        }
    ]

    devices: List[DeviceRecord] = [
        {
            "serial_number": "SER123",
            "site_id": "site1",
            "mac_address": "11:22:33:44:55:66",
            "device_name": "ap1",
            "model": "AP-515",
            "device_type": "ap",
            "firmware_version": "8.10",
            "ip_address": "10.0.0.10",
            "status": "Up"
        }
    ]

    repo.upsert_clients(clients, batch_size=1)
    repo.upsert_devices(devices, batch_size=1)

    # Expect two executemany calls
    executemany_calls = [c for c in calls if c[0] == "executemany"]
    assert len(executemany_calls) == 2
    # Verify first batch row count
    assert len(executemany_calls[0][2]) == 1
    # Verify second batch row count
    assert len(executemany_calls[1][2]) == 1