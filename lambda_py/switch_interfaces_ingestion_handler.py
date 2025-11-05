"""
Lambda handler for ingesting Aruba Central switch interface data.
Iterates all switches, fetches interface details, and inserts into MySQL.
"""

import os, json, logging
from datetime import datetime, timezone
from typing import Dict, Any
from db import MySqlRepository
import base64, boto3
from api_client import ArubaApiClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def _get_secret_cached(arn: str) -> Dict[str, Any]:
    # Simple cache for secrets
    if not hasattr(_get_secret_cached, "_cache"):
        _get_secret_cached._cache = {}
    cache = _get_secret_cached._cache
    if arn in cache:
        return cache[arn]
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=arn)
    if "SecretString" in resp:
        js = json.loads(resp["SecretString"])
    else:
        js = json.loads(base64.b64decode(resp["SecretBinary"]).decode())
    cache[arn] = js
    return js

def _init():
    global _api, _db
    required = ["DB_SECRET_ARN", "ARUBA_API_SECRET_ARN"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars {missing}")
    if not hasattr(_init, "_db"):
        db_secret = _get_secret_cached(os.environ["DB_SECRET_ARN"])
        for key in ("host", "username", "password"):
            if key not in db_secret:
                raise RuntimeError(f"DB secret missing '{key}'")
        _init._db = MySqlRepository(
            host=db_secret["host"],
            port=int(db_secret.get("port", 3306)),
            user=db_secret["username"],
            password=db_secret["password"],
            database=db_secret.get("dbname", "aruba_central"),
        )
        _init._db.connect()
        _init._db.ensure_schema()
        logger.info("[init] DB ready")
    if not hasattr(_init, "_api"):
        api_secret = _get_secret_cached(os.environ["ARUBA_API_SECRET_ARN"])
        for key in ("baseUrl", "clientSecret"):
            if key not in api_secret:
                raise RuntimeError(f"API secret missing '{key}'")
        _init._api = ArubaApiClient(
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
    _init()
    db = _init._db
    api = _init._api
    SWITCH_INTERFACE_COLS = [
        '_site_id', '_site_name', 'switch_serial', 'created_at', 'updated_at',
        'neighbourPort', 'neighbourFamily', 'index', 'vlanMode', 'module', 'nativeVlan',
        'neighbourSerial', 'speed', 'isMultipleNeighbourClients', 'duplex', 'name', 'connector',
        'type', 'transceiverStatus', 'stpInstanceType', 'stpInstanceId', 'stpPortRole', 'stpPortState',
        'stpPortInconsistent', 'transceiverState', 'ipv4', 'transceiverProductNumber', 'transceiverModel',
        'transceiverSerial', 'errorReason', 'adminStatus', 'operStatus', 'mtu', 'status', 'transceiverType',
    'neighbourType', 'neighbourHealth', 'neighbourRole', 'lag', 'allowedVlans', 'allowedVlanIds',
        'poeStatus', 'alias', 'description', 'poeClass', 'portAlignment', 'serial', 'id', 'peerPort',
        'peerMemberId', 'uplink', 'portError', 'neighbour', 'neighbourFunction'
    ]
    def filter_row(row):
        out = {}
        for k in SWITCH_INTERFACE_COLS:
            v = row.get(k)
            # Serialize lists as JSON strings
            if isinstance(v, list):
                out[k] = json.dumps(v)
            else:
                out[k] = v
        return out
    import re
    devices = api.list_devices()
    total_inserted = 0
    serial_pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{8}$", re.IGNORECASE)
    switch_infos = []
    for device in devices:
        device_type = device.get('deviceType', '')
        serial = device.get('serialNumber') or device.get('serial')
        site_id = device.get('_site_id') or device.get('siteId') or device.get('site_id')
        name = device.get('name') or device.get('deviceName') or device.get('hostname')
        if device_type == 'SWITCH' and serial and site_id:
            switch_infos.append(f"name={name}, serial={serial}, site_id={site_id}")
    if switch_infos:
        logger.info(f"Switches found: {len(switch_infos)}")
        for info in switch_infos:
            logger.info(info)
    for device in devices:
        device_type = device.get('deviceType', '')
        if device_type != 'SWITCH':
            continue
        serial = device.get('serialNumber') or device.get('serial')
        site_id = device.get('_site_id') or device.get('siteId') or device.get('site_id')
        if not serial or not site_id:
            continue
        if not serial_pattern.match(serial):
            continue
        interfaces = api.get_switch_interfaces(serial, site_id)
        rows = []
        for iface in interfaces:
            row = {
                '_site_id': site_id,
                '_site_name': device.get('_site_name'),
                'switch_serial': serial,
                'created_at': utcnow(),
                'updated_at': utcnow(),
            }
            # Add all fields from iface
            for k, v in iface.items():
                # Serialize lists as JSON strings
                if isinstance(v, list):
                    row[k] = json.dumps(v)
                else:
                    row[k] = v
            rows.append(filter_row(row))
        if rows:
            db.insert_switch_interfacedetails(rows)
            total_inserted += len(rows)
    logger.info(f"Inserted {total_inserted} switch interface records.")
    db.close()
    return {'inserted': total_inserted}
