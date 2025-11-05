"""Lambda handler to ingest Aruba AP and embedded array data into MySQL.

- Discovers all APs via Aruba Central API (paginated)
- Fetches details for each AP (radios, wlans, ports, modems, ...)
- Appends all data to normalized SQL tables (ap, ap_radio, ap_wlan, ap_port, ap_modem)
- Handles secrets via AWS Secrets Manager
- Robust to API/DB errors, logs summary

Environment Variables (subset):
    DB_SECRET_ARN, ARUBA_API_SECRET_ARN  Secrets Manager ARNs
    ARUBA_PAGE_SIZE (default 100)
    ARUBA_PAGE_DELAY_SECONDS (default 2.0)
    ARUBA_APS_ENDPOINT (optional override, default /monitoring/v2/aps)
    ARUBA_AP_DETAIL_ENDPOINT (optional override, default /monitoring/v2/aps/{serial})
    DB_CLOSE_EACH_INVOCATION (default true)
"""
import os, json, time, logging, base64
from typing import Any, Dict, List

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

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
        logger.info("[init] DB ready (ap_ingestion)")
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
        logger.info("[init] API client ready (ap_ingestion)")

def _fetch_all_aps() -> List[Dict[str, Any]]:
    # Query Aruba Central API for the full list of APs
    endpoint = os.getenv("ARUBA_APS_LIST_ENDPOINT", "/network-monitoring/v1alpha1/aps")
    page_size = int(os.getenv("ARUBA_PAGE_SIZE", "100"))
    params = {"limit": page_size}
    # Use the robust Aruba API pagination helper (cursor or offset)
    aps = _api._cursor_or_offset_collect(
        endpoint=endpoint,
        root_keys=["items", "aps", "data"],
        params=params
    )
    logger.info(f"Discovered {len(aps)} APs from API list endpoint (via _cursor_or_offset_collect)")
    return aps

def _fetch_ap_details(serial: str) -> Dict[str, Any]:
    endpoint = "/network-monitoring/v1alpha1/aps/{serialNumber}"
    try:
        # Always use serialNumber as the path param
        return _api.get(endpoint.format(serialNumber=serial))
    except Exception as e:
        logger.error(f"Failed to fetch details for AP {serial}: {e}")
        return {}

def lambda_handler(event, context):
    start = time.time()
    _init()
    close_conn = os.getenv("DB_CLOSE_EACH_INVOCATION", "true").lower() == "true"
    inserted = 0
    failed = 0
    aps = _fetch_all_aps()
    for ap in aps:
        serial = ap.get("serialNumber") or ap.get("serial") or ap.get("serial_number")
        if not serial:
            logger.debug(f"Skipping AP with missing serial: {json.dumps(ap)[:300]}")
            continue
        try:
            details = _fetch_ap_details(serial)
            # Map API fields to DB schema (fall back to top-level AP fields if details missing)
            ap_row = {
                "serial": details.get("serialNumber") or serial,
                "name": details.get("deviceName") or ap.get("deviceName"),
                "mac_address": details.get("macAddress") or ap.get("macAddress"),
                "ip_address": details.get("ipv4") or ap.get("ipv4"),
                "model": details.get("model") or ap.get("model"),
                "status": details.get("status") or ap.get("status"),
                "site_id": details.get("siteId") or ap.get("siteId"),
                "site_name": details.get("siteName") or ap.get("siteName"),
                "sw_version": details.get("softwareVersion") or ap.get("softwareVersion"),
                "uptime": details.get("uptimeInMillis") or ap.get("uptimeInMillis"),
                "cluster_name": details.get("clusterName") or ap.get("clusterName"),
                "public_ip": details.get("publicIpv4") or ap.get("publicIpv4"),
            }
            _db.insert_ap(ap_row)
            # Delete all existing child records for this AP serial before inserting new ones
            _db.delete_ap_radios(serial)
            _db.delete_ap_wlans(serial)
            _db.delete_ap_ports(serial)
            _db.delete_ap_modems(serial)
            # Radios and WLANs (WLANs are nested under each radio)
            for idx, radio in enumerate(details.get("radios", [])):
                radio_row = {
                    "radio_index": idx,
                    "mac_address": radio.get("macAddress"),
                    "band": radio.get("band"),
                    "channel": radio.get("channel"),
                    "bandwidth": radio.get("bandwidth"),
                    "status": radio.get("status"),
                    "radio_number": radio.get("radioNumber"),
                    "mode": radio.get("mode"),
                    "antenna": radio.get("antenna"),
                    "spatial_stream": radio.get("spatialStream"),
                    "power": radio.get("power"),
                }
                _db.insert_ap_radio(serial, radio_row)
                # WLANs for this radio
                for wlan in radio.get("wlans", []):
                    # Extract wlan_name from WLAN object (standard logic)
                    wlan_name = wlan.get("wlanName") or wlan.get("wLanName")
                    if not wlan_name:
                        logger.debug(f"[ap_wlan] Missing wlan_name in WLAN object: {json.dumps(wlan)[:300]}")
                    wlan_row = {
                        "wlan_name": wlan_name,
                        "security": wlan.get("security"),
                        "security_level": wlan.get("securityLevel"),
                        "bssid": wlan.get("bssid"),
                        "vlan": wlan.get("vlan"),
                        "status": wlan.get("status"),
                    }
                    logger.debug(f"[ap_wlan] Insert WLAN row for AP {serial}: {json.dumps(wlan_row)}")
                    _db.insert_ap_wlan(serial, wlan_row)
            # Ports
            for port in details.get("ports", []):
                port_row = {
                    "mac_address": port.get("macAddress"),
                    "port_name": port.get("name"),
                    "port_index": port.get("portIndex"),
                    "status": port.get("status"),
                    "vlan_mode": port.get("vlanMode"),
                    "allowed_vlan": port.get("allowedVlan"),
                    "native_vlan": port.get("nativeVlan"),
                    "access_vlan": port.get("accessVlan"),
                    "speed": port.get("speed"),
                    "duplex": port.get("duplex"),
                    "connector": port.get("connector"),
                }
                _db.insert_ap_port(serial, port_row)
            # Modem (if present, single object)
            modem = details.get("modem")
            if modem:
                _db.insert_ap_modem(serial, modem)
            inserted += 1
        except Exception as e:
            logger.error(f"Failed to ingest AP {serial}: {e}")
            failed += 1
    dur = round(time.time() - start, 3)
    summary = {"aps": len(aps), "inserted": inserted, "failed": failed, "duration_sec": dur}
    logger.info(f"[ap_ingestion_summary] {json.dumps(summary, separators=(',',':'))}")
    result = {"statusCode": 200, "body": json.dumps(summary)}
    if close_conn and _db:
        try:
            _db.close()
        except Exception:
            pass
    return result
