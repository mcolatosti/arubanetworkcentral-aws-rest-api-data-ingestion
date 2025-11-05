"""Lambda handler to ingest device status data into MySQL.

Current Default Behavior:
- Defaults to the stable v1alpha1 devices endpoint: /network-monitoring/v1alpha1/devices
- The older experimental v2 status endpoint (/network-monitoring/v2/devices/status) can be enabled by setting
    ARUBA_DEVICE_STATUS_USE_V2=true (and optionally overriding ARUBA_DEVICE_STATUS_ENDPOINT).
- If v2 is enabled but returns 404 and ARUBA_DEVICE_STATUS_FALLBACK_V1ALPHA1=true (default), we fallback to v1alpha1 devices.

Response Shape Expectations:
- v1alpha1 devices endpoint returns a list (or object containing list) of device objects directly.
- v2 status endpoint (when available) may wrap device fields under an event object with 'data'.

Design:
- Append-only insert into device_status table (created by db.ensure_schema()).

Environment Variables (subset):
    DB_SECRET_ARN, ARUBA_API_SECRET_ARN  Secrets Manager ARNs
    ARUBA_PAGE_SIZE (default 100)
    ARUBA_PAGE_DELAY_SECONDS (default 2.0)
    ARUBA_DEVICE_STATUS_ENDPOINT (optional override)
    ARUBA_DEVICE_STATUS_FALLBACK_V1ALPHA1 (default true) attempt list_devices() if v2 404s
    ARUBA_DEVICE_STATUS_USE_V2 (default false). If set true, attempt /network-monitoring/v2/devices/status first.
    DB_CLOSE_EACH_INVOCATION (default true)

If later the official endpoint or envelope differs, adjust _fetch_device_status_events().
"""
import os, json, time, logging, base64
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_api = None
_db = None
_cached_secrets: Dict[str, Dict[str, Any]] = {}


def _get_secret_cached(arn: str) -> Dict[str, Any]:
    if arn in _cached_secrets:
        return _cached_secrets[arn]
    try:
        import boto3  # type: ignore
    except ImportError:
        boto3 = None  # type: ignore
    if boto3 is None:  # type: ignore
        raise RuntimeError("boto3 is required in the Lambda runtime but is not installed locally")
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=arn)
    if "SecretString" in resp:
        js = json.loads(resp["SecretString"])
    else:
        js = json.loads(base64.b64decode(resp["SecretBinary"]).decode())
    _cached_secrets[arn] = js
    return js


def _parse_time(val: Any) -> Optional[datetime]:
    if val in (None, "", 0, "0"):
        return None
    try:
        if isinstance(val, (int, float)):
            ts = val / 1000 if val > 1e11 else val
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if isinstance(val, str):
            if val.endswith("Z"):
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            return datetime.fromisoformat(val)
    except Exception:
        return None
    return None


def _init():
    global _api, _db
    required = ["DB_SECRET_ARN", "ARUBA_API_SECRET_ARN"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars {missing}")
    if _db is None:
        db_secret = _get_secret_cached(os.environ["DB_SECRET_ARN"])
        from db import MySqlRepository
        for key in ("host", "username", "password"):
            if key not in db_secret:
                raise RuntimeError(f"DB secret missing '{key}'")
        _db = MySqlRepository(
            host=db_secret["host"],
            port=int(db_secret.get("port", 3306)),
            user=db_secret["username"],
            password=db_secret["password"],
            database=db_secret.get("dbname", "aruba_central"),
        )
        _db.connect()
        _db.ensure_schema()
        logger.info("[init] DB ready (device_status_v2)")
    if _api is None:
        api_secret = _get_secret_cached(os.environ["ARUBA_API_SECRET_ARN"])
        for key in ("baseUrl", "clientSecret"):
            if key not in api_secret:
                raise RuntimeError(f"API secret missing '{key}'")
        from api_client import ArubaApiClient
        _api = ArubaApiClient(
            client_id=api_secret.get("clientId"),
            client_secret=api_secret["clientSecret"],
            customer_id=api_secret.get("customerId"),
            base_url=api_secret["baseUrl"],
            oauth_token_url=api_secret.get("oauthTokenUrl"),
            page_limit=int(os.getenv("ARUBA_PAGE_SIZE", "100")),
            page_delay_seconds=float(os.getenv("ARUBA_PAGE_DELAY_SECONDS", "2.0"))
        )
        logger.info("[init] API client ready (device_status_v2)")


def _normalize_event(evt: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a v2 event OR a direct device object into table row keys.

    Accepts either shape:
      {"timestamp":..., "data": {<device fields>}}
      {<device fields>}
    """
    data = evt.get("data") if isinstance(evt, dict) and "data" in evt else evt
    if not isinstance(data, dict):
        data = {}
    site_id = data.get("siteId") or data.get("site_id")
    site_name = data.get("siteName") or data.get("site_name")
    last_seen = _parse_time(data.get("lastSeenAt"))
    config_mod = _parse_time(data.get("configLastModifiedAt"))

    # convert to naive UTC for MySQL DATETIME
    def _naive(dt: Optional[datetime]):
        if not dt:
            return None
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    rec = {
        "_site_id": site_id,
        "_site_name": site_name,
        "deviceId": data.get("id") or data.get("deviceId"),
        "serialNumber": data.get("serialNumber"),
        "macAddress": data.get("macAddress") or data.get("mac"),
        "deviceName": data.get("deviceName") or data.get("name"),
        "model": data.get("model"),
        "partNumber": data.get("partNumber"),
        "status": data.get("status"),
        "softwareVersion": data.get("softwareVersion"),
        "ipv4": data.get("ipv4"),
        "ipv6": data.get("ipv6"),
        "role": data.get("role"),
        "deviceType": data.get("deviceType"),
        "deployment": data.get("deployment"),
        "persona": data.get("persona"),
        "deviceFunction": data.get("deviceFunction"),
        "uptimeInMillis": data.get("uptimeInMillis"),
        "lastSeenAt": _naive(last_seen),
        "configLastModifiedAt": _naive(config_mod),
    }
    return rec


def _fetch_device_status_events() -> List[Dict[str, Any]]:
    # Default endpoint now v1alpha1 devices; users can switch to v2 via USE_V2 flag.
    default_endpoint = "/network-monitoring/v1alpha1/devices"
    use_v2 = os.getenv("ARUBA_DEVICE_STATUS_USE_V2", "false").lower() == "true"
    if use_v2:
        endpoint = os.getenv("ARUBA_DEVICE_STATUS_ENDPOINT", "/network-monitoring/v2/devices/status")
    else:
        endpoint = os.getenv("ARUBA_DEVICE_STATUS_ENDPOINT", default_endpoint)

    # If we're not using v2 OR the endpoint clearly references v1alpha1 devices, short-circuit to devices listing.
    if (not use_v2) or "/v1alpha1/devices" in endpoint:
        logger.info(f"[device_status] using devices endpoint endpoint={endpoint} use_v2={use_v2}")
        try:
            devices = _api.list_devices()  # type: ignore[attr-defined]
            logger.info(f"[device_status] devices count={len(devices)}")
            return devices
        except Exception:
            logger.exception("[device_status] devices listing failed")
            return []
    # We call the underlying ArubaApiClient private helpers for simplicity.
    # Build paginated GETs manually similar to list_clients_single_site.
    import urllib.request, urllib.parse
    events: List[Dict[str, Any]] = []
    offset = 0
    page_limit = getattr(_api, "page_limit", 100)
    max_pages = getattr(_api, "max_pages_per_call", 60)
    page = 0
    while True:
        page += 1
        params = {"limit": page_limit, "offset": offset}
        qs = urllib.parse.urlencode(params)
        url = f"{_api.base_url}{endpoint}?{qs}"  # type: ignore[attr-defined]
        _api._ensure_token()  # type: ignore[attr-defined]
        req = urllib.request.Request(url, headers=_api._headers())  # type: ignore[attr-defined]
        try:
            raw = _api._do_request(req)  # type: ignore[attr-defined]
        except Exception as e:
            # If 404 and fallback enabled, try legacy list_devices
            if "HTTP Error 404" in str(e) and os.getenv("ARUBA_DEVICE_STATUS_FALLBACK_V1ALPHA1", "true").lower() == "true":
                logger.warning("[device_status_v2] 404 on v2 endpoint; falling back to v1alpha1 devices list")
                try:
                    legacy_devices = _api.list_devices()  # type: ignore[attr-defined]
                    logger.info(f"[device_status_v2] fallback list_devices count={len(legacy_devices)}")
                    return legacy_devices
                except Exception:
                    logger.exception("[device_status_v2] fallback list_devices failed")
            raise
        try:
            js = json.loads(raw.decode())
        except Exception:
            logger.warning(f"[device_status_v2] JSON decode failed url={url}")
            break
        page_items: List[Dict[str, Any]] = []
        if isinstance(js, list):
            page_items = js
        elif isinstance(js, dict):
            for key in ("items", "events", "data"):
                if key in js and isinstance(js[key], list):
                    page_items = js[key]
                    break
            if not page_items:
                for v in js.values():
                    if isinstance(v, list):
                        page_items = v
                        break
        cnt = len(page_items)
        logger.info(f"[device_status_v2] page={page} offset={offset} count={cnt}")
        if not cnt:
            break
        events.extend(page_items)
        if cnt < page_limit or page >= max_pages:
            break
        offset += page_limit
        time.sleep(getattr(_api, "page_delay_seconds", 1.0))  # type: ignore[attr-defined]
    logger.info(f"[device_status_v2] total_events={len(events)}")
    return events


def lambda_handler(event, context):  # noqa: D401
    start = time.time()
    _init()
    close_conn = os.getenv("DB_CLOSE_EACH_INVOCATION", "true").lower() == "true"
    try:
        raw_events = _fetch_device_status_events()
        if raw_events and os.getenv("LOG_RAW_DEVICE_STATUS_SAMPLE", "false").lower() == "true":
            logger.debug("[device_status_v2] sample_raw=%s", json.dumps(raw_events[0])[:800])
        rows = [_normalize_event(e) for e in raw_events]
        # Drop rows missing deviceId or macAddress (basic integrity)
        before = len(rows)
        rows = [r for r in rows if r.get("deviceId") or r.get("macAddress")]
        dropped = before - len(rows)
        inserted = 0
        if rows:
            inserted = _db.insert_device_status(rows)  # type: ignore[attr-defined]
        dur = round(time.time() - start, 3)
        summary = {"device_status_events": len(raw_events), "rows_inserted": inserted, "dropped": dropped, "duration_sec": dur}
        logger.info(f"[device_status_v2_summary] {json.dumps(summary, separators=(',',':'))}")
        return {"statusCode": 200, "body": json.dumps(summary)}
    except Exception as e:
        logger.exception("device_status_v2 ingestion failed")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    finally:
        if close_conn and _db:
            try:
                _db.close()
            except Exception:
                pass
