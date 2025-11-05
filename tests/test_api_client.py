import time
from typing import Dict, Any, List
from lambda_py.api_client import ArubaApiClient

class _Resp:
    def __init__(self, status_code: int, json_data: Dict[str, Any]):
        self.status_code = status_code
        self._json = json_data
        self.text = str(json_data)
    def json(self):
        return self._json

class _Session:
    def __init__(self):
        self.calls = []
    def post(self, url, data=None, timeout=0):
        # Auth token response
        return _Resp(200, {"access_token": "TOKEN123", "expires_in": 3600})
    def request(self, method, url, params=None, json=None, headers=None, timeout=0):
        self.calls.append((method, url, params))
        # Simulate pagination: offset 0 => full page; offset >= limit => final partial
        limit = params.get("limit", 100)
        offset = params.get("offset", 0)
        if "sites" in url:
            if offset == 0:
                return _Resp(200, {"sites": [{"site_id": "s1", "site_name": "Site1"}]})
            return _Resp(200, {"sites": []})
        if "clients" in url:
            if offset == 0:
                return _Resp(200, {"clients": [{"mac_address": "AA:BB", "name": "c1"}]})
            return _Resp(200, {"clients": []})
        if "devices" in url:
            if offset == 0:
                return _Resp(200, {"devices": [{"serial": "SER1", "name": "ap1"}]})
            return _Resp(200, {"devices": []})
        return _Resp(200, {})

def test_pagination_and_auth(monkeypatch):
    client = ArubaApiClient(
        client_id="cid",
        client_secret="csec",
        customer_id="cust",
        base_url="https://example",
        page_limit=50
    )
    session = _Session()
    monkeypatch.setattr(client, "_session", session)

    # list_sites
    sites = client.list_sites()
    assert len(sites) == 1
    # list_clients
    clients = client.list_clients("s1")
    assert len(clients) == 1
    # list_devices
    devices = client.list_devices("s1")
    assert len(devices) == 1

    # Auth should have happened only once
    # First call is token POST; subsequent are GET requests
    # session.calls only records request() calls (not auth post)
    assert any("sites" in c[1] for c in session.calls)
    assert any("clients" in c[1] for c in session.calls)
    assert any("devices" in c[1] for c in session.calls)

def test_list_clients_single_site_sets_filter(monkeypatch):
    from lambda_py.api_client import ArubaApiClient
    captured = {}
    def fake_paged(endpoint, base_params=None, root_keys=None):
        captured['params'] = base_params
        return []
    client = ArubaApiClient(client_id=None, client_secret="tok", customer_id=None, base_url="https://example")
    client._paged_collect = fake_paged  # type: ignore
    client.list_clients_single_site("SITE123")
    assert 'filter' in captured['params']