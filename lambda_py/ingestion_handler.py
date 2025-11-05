import os, json, time, logging, base64, hashlib, tracemalloc
from datetime import datetime, timezone
from typing import Dict, List, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_api = None
_db = None
_cached_secrets: Dict[str, Dict[str, Any]] = {}

def _get_secret_cached(arn: str) -> Dict[str, Any]:
    if arn in _cached_secrets:
        return _cached_secrets[arn]
    import boto3
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=arn)
    if "SecretString" in resp:
        js = json.loads(resp["SecretString"])
    else:
        js = json.loads(base64.b64decode(resp["SecretBinary"]).decode())
    _cached_secrets[arn] = js
    return js

def _parse_time(val):
    if not val:
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

def _norm_client(raw: Dict[str, Any]) -> Dict[str, Any]:
    site_id = raw.get("_site_id") or raw.get("siteId") or raw.get("site_id")
    site_name = raw.get("_site_name") or raw.get("siteName") or raw.get("site_name")
    mac = raw.get("mac") or raw.get("macAddress") or raw.get("mac_address")

    # lastSeenAt fallback logic
    last_seen_raw = raw.get("lastSeenAt") or raw.get("last_seen") or raw.get("lastSeen")
    if last_seen_raw in (None, "0", 0):
        last_seen_raw = raw.get("connectedSince") or raw.get("connected_since")

    def _parse_dt(val):
        dt = _parse_time(val)
        if dt:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return None

    last_seen_dt = _parse_dt(last_seen_raw)
    connected_since_dt = _parse_dt(raw.get("connectedSince") or raw.get("connected_since"))

    # Build JSON-aligned record
    rec: Dict[str, Any] = {
        "mac": mac,
        "_site_id": site_id,
        "_site_name": site_name,
        "name": raw.get("name") or raw.get("hostname") or raw.get("hostName") or mac,
        "status": raw.get("status"),
        "experience": raw.get("experience"),
        "statusReason": raw.get("statusReason"),
        "capabilities": raw.get("capabilities"),
        "authentication": raw.get("authentication"),
        "type": raw.get("type") or raw.get("connectionType"),
        "ipv4": raw.get("ipv4") or raw.get("ipAddress") or raw.get("ip"),
        "ipv6": raw.get("ipv6"),
        "vlanId": raw.get("vlanId"),
        "network": raw.get("network"),
        "connectedDeviceSerial": raw.get("connectedDeviceSerial"),
        "connectedTo": raw.get("connectedTo"),
        "tunnelId": raw.get("tunnelId"),
        "tunnel": raw.get("tunnel"),
        "role": raw.get("role"),
        "port": raw.get("port"),
        "keyManagement": raw.get("keyManagement"),
        "connectedSince": connected_since_dt,
        "lastSeenAt": last_seen_dt,
    }

    # Diagnostics for missing critical fields
    if os.getenv("LOG_CLIENT_FIELD_GAPS", "false").lower() == "true":
        if not rec["ipv4"] or not rec["type"]:
            try:
                logger.debug(
                    "[norm_client] field_gaps mac=%s ipv4_missing=%s type_missing=%s keys=%s",
                    rec["mac"], not bool(rec["ipv4"]), not bool(rec["type"]), list(raw.keys())
                )
            except Exception:
                pass

    return rec

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
        logger.info("[init] DB ready")
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
        logger.info("[init] API client ready")

def lambda_handler(event, context):
    logger.info("[lambda_handler] invoked with event=%s", json.dumps(event)[:500])
    tracemalloc.start()
    start = time.time()
    _init()

    site_override = os.getenv("ARUBA_SITE_ID")
    close_conn = os.getenv("DB_CLOSE_EACH_INVOCATION", "true").lower() == "true"

    inserted_clients = 0
    client_rows: List[Dict[str, Any]] = []

    try:
        logger.info(f"[lambda_handler] calling _api.list_all_clients(site_override={site_override})")
        raw_clients = _api.list_all_clients(site_override)
        raw_count = len(raw_clients)
        logger.info(f"[clients] raw_count={raw_count}")
        if raw_clients and os.getenv("LOG_RAW_CLIENT_SAMPLE", "false").lower() == "true":
            logger.debug(f"[clients] raw_sample={json.dumps(raw_clients[0], default=str)[:800]}")
        client_rows = [_norm_client(rc) for rc in raw_clients]
        missing_mac = sum(1 for c in client_rows if not c["mac"])
        missing_site = sum(1 for c in client_rows if not c["_site_id"])
        client_rows = [c for c in client_rows if c["mac"] and c["_site_id"]]
        # Diagnostics: distinct MACs and time span (using lastSeenAt)
        try:
            distinct_macs = len({c["mac"] for c in client_rows})
            times = [c["lastSeenAt"] for c in client_rows if c.get("lastSeenAt")]
            earliest = min(times).isoformat() if times else None
            latest = max(times).isoformat() if times else None
            logger.info(f"[clients] distinct_mac={distinct_macs} time_range earliest={earliest} latest={latest}")
        except Exception:
            pass
        logger.info(f"[clients] normalized={raw_count} dropped_missing_mac={missing_mac} dropped_missing_site={missing_site} retained={len(client_rows)}")
        if client_rows:
            h = hashlib.sha256(client_rows[0]["mac"].encode()).hexdigest()[:10]
            logger.debug(f"[clients] sample_norm_hash={h} sample={client_rows[0]}")
        inserted_clients = _db.insert_clients(client_rows)
        logger.info(f"[clients] inserted={inserted_clients}")

        peak_mem = tracemalloc.get_traced_memory()[1] / (1024 * 1024)
        duration = round(time.time() - start, 3)
        summary = {
            "clients_inserted": inserted_clients,
            "duration_sec": duration,
            "peak_mem_mb": round(peak_mem, 2)
        }
        logger.info(f"[summary] {json.dumps(summary, separators=(',',':'))}")
        return {"statusCode": 200, "body": json.dumps(summary)}

    except Exception as e:
        logger.exception("Ingestion failed")
        partial = {
            "clients_processed": len(client_rows)
        }
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "partial": partial})}
    finally:
        tracemalloc.stop()
        if close_conn and _db:
            try:
                _db.close()
            except Exception:
                pass