# Device status table
CREATE_DEVICE_STATUS_STMT = """
CREATE TABLE IF NOT EXISTS device_status (
    id BIGINT NOT NULL AUTO_INCREMENT,
    _site_id VARCHAR(64) DEFAULT NULL,
    _site_name VARCHAR(255) DEFAULT NULL,
    deviceId VARCHAR(64) DEFAULT NULL,
    serialNumber VARCHAR(64) DEFAULT NULL,
    macAddress VARCHAR(32) DEFAULT NULL,
    deviceName VARCHAR(255) DEFAULT NULL,
    model VARCHAR(64) DEFAULT NULL,
    partNumber VARCHAR(64) DEFAULT NULL,
    status VARCHAR(32) DEFAULT NULL,
    softwareVersion VARCHAR(64) DEFAULT NULL,
    ipv4 VARCHAR(45) DEFAULT NULL,
    ipv6 VARCHAR(45) DEFAULT NULL,
    role VARCHAR(64) DEFAULT NULL,
    deviceType VARCHAR(64) DEFAULT NULL,
    deployment VARCHAR(64) DEFAULT NULL,
    persona VARCHAR(64) DEFAULT NULL,
    deviceFunction VARCHAR(64) DEFAULT NULL,
    uptimeInMillis BIGINT DEFAULT NULL,
    lastSeenAt DATETIME DEFAULT NULL,
    configLastModifiedAt DATETIME DEFAULT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_dev_site (_site_id, deviceId),
    KEY idx_dev_site_status (_site_id, status),
    KEY idx_dev_site_lastSeen (_site_id, lastSeenAt),
    KEY idx_dev_site_created (_site_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

# Clients table
CREATE_CLIENTS_STMT = """
CREATE TABLE IF NOT EXISTS clients (
    id BIGINT NOT NULL AUTO_INCREMENT,
    mac VARCHAR(17) NOT NULL,
    _site_id VARCHAR(64) NOT NULL,
    _site_name VARCHAR(255) DEFAULT NULL,
    name VARCHAR(255) DEFAULT NULL,
    status VARCHAR(50) DEFAULT NULL,
    experience VARCHAR(50) DEFAULT NULL,
    statusReason VARCHAR(120) DEFAULT NULL,
    capabilities TEXT,
    authentication VARCHAR(100) DEFAULT NULL,
    type VARCHAR(40) DEFAULT NULL,
    ipv4 VARCHAR(45) DEFAULT NULL,
    ipv6 VARCHAR(45) DEFAULT NULL,
    vlanId VARCHAR(16) DEFAULT NULL,
    network VARCHAR(100) DEFAULT NULL,
    connectedDeviceSerial VARCHAR(64) DEFAULT NULL,
    connectedTo VARCHAR(255) DEFAULT NULL,
    tunnelId VARCHAR(64) DEFAULT NULL,
    tunnel VARCHAR(64) DEFAULT NULL,
    role VARCHAR(100) DEFAULT NULL,
    port VARCHAR(64) DEFAULT NULL,
    keyManagement VARCHAR(100) DEFAULT NULL,
    connectedSince DATETIME DEFAULT NULL,
    lastSeenAt DATETIME DEFAULT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_site_mac (_site_id, mac),
    KEY idx_site_lastSeenAt (_site_id, lastSeenAt),
    KEY idx_site_connectedSince (_site_id, connectedSince),
    KEY idx_site_created (_site_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

# Switch interface details table
CREATE_SWITCH_INTERFACEDETAILS_STMT = """
CREATE TABLE IF NOT EXISTS switch_interfacedetails (
    _id BIGINT NOT NULL AUTO_INCREMENT,
    _site_id VARCHAR(128) DEFAULT NULL,
    _site_name VARCHAR(255) DEFAULT NULL,
    switch_serial VARCHAR(64) DEFAULT NULL,
    created_at DATETIME DEFAULT NULL,
    updated_at DATETIME DEFAULT NULL,
    neighbourPort VARCHAR(64) DEFAULT NULL,
    neighbourFamily VARCHAR(64) DEFAULT NULL,
    `index` INT DEFAULT NULL,
    vlanMode VARCHAR(32) DEFAULT NULL,
    module VARCHAR(64) DEFAULT NULL,
    nativeVlan VARCHAR(32) DEFAULT NULL,
    neighbourSerial VARCHAR(64) DEFAULT NULL,
    speed VARCHAR(32) DEFAULT NULL,
    isMultipleNeighbourClients TINYINT DEFAULT NULL,
    duplex VARCHAR(16) DEFAULT NULL,
    name VARCHAR(128) DEFAULT NULL,
    connector VARCHAR(32) DEFAULT NULL,
    type VARCHAR(32) DEFAULT NULL,
    transceiverStatus VARCHAR(32) DEFAULT NULL,
    stpInstanceType VARCHAR(32) DEFAULT NULL,
    stpInstanceId VARCHAR(32) DEFAULT NULL,
    stpPortRole VARCHAR(32) DEFAULT NULL,
    stpPortState VARCHAR(32) DEFAULT NULL,
    stpPortInconsistent VARCHAR(32) DEFAULT NULL,
    transceiverState VARCHAR(32) DEFAULT NULL,
    ipv4 VARCHAR(64) DEFAULT NULL,
    transceiverProductNumber VARCHAR(64) DEFAULT NULL,
    transceiverModel VARCHAR(64) DEFAULT NULL,
    transceiverSerial VARCHAR(64) DEFAULT NULL,
    errorReason VARCHAR(128) DEFAULT NULL,
    adminStatus VARCHAR(32) DEFAULT NULL,
    operStatus VARCHAR(32) DEFAULT NULL,
    mtu INT DEFAULT NULL,
    status VARCHAR(32) DEFAULT NULL,
    transceiverType VARCHAR(32) DEFAULT NULL,
    neighbourType VARCHAR(32) DEFAULT NULL,
    neighbourHealth VARCHAR(32) DEFAULT NULL,
    neighbourRole VARCHAR(32) DEFAULT NULL,
    `lag` VARCHAR(32) DEFAULT NULL,
    allowedVlans TEXT,
    allowedVlanIds TEXT,
    poeStatus VARCHAR(32) DEFAULT NULL,
    alias VARCHAR(64) DEFAULT NULL,
    description VARCHAR(255) DEFAULT NULL,
    poeClass VARCHAR(32) DEFAULT NULL,
    portAlignment VARCHAR(32) DEFAULT NULL,
    serial VARCHAR(64) DEFAULT NULL,
    id VARCHAR(64) DEFAULT NULL,
    peerPort VARCHAR(64) DEFAULT NULL,
    peerMemberId VARCHAR(64) DEFAULT NULL,
    uplink VARCHAR(32) DEFAULT NULL,
    portError VARCHAR(128) DEFAULT NULL,
    neighbour VARCHAR(128) DEFAULT NULL,
    neighbourFunction VARCHAR(64) DEFAULT NULL,
    PRIMARY KEY (_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
import logging
from typing import Dict, Any
import pymysql
import os

logger = logging.getLogger(__name__)

ClientRecord = Dict[str, Any]



# AP table creation SQL statements (top-level, not indented)
CREATE_AP_STMT = """
CREATE TABLE IF NOT EXISTS ap (
    id BIGINT NOT NULL AUTO_INCREMENT,
    serial VARCHAR(64) NOT NULL,
    name VARCHAR(255) DEFAULT NULL,
    mac_address VARCHAR(32) DEFAULT NULL,
    ip_address VARCHAR(45) DEFAULT NULL,
    model VARCHAR(64) DEFAULT NULL,
    status VARCHAR(32) DEFAULT NULL,
    site_id VARCHAR(64) DEFAULT NULL,
    site_name VARCHAR(255) DEFAULT NULL,
    sw_version VARCHAR(64) DEFAULT NULL,
    uptime BIGINT DEFAULT NULL,
    cluster_name VARCHAR(255) DEFAULT NULL,
    public_ip VARCHAR(45) DEFAULT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY serial (serial),
    KEY idx_serial (serial)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

CREATE_AP_RADIO_STMT = """
CREATE TABLE IF NOT EXISTS ap_radio (
    id BIGINT NOT NULL AUTO_INCREMENT,
    ap_serial VARCHAR(64) NOT NULL,
    radio_index INT DEFAULT NULL,
    mac_address VARCHAR(32) DEFAULT NULL,
    band VARCHAR(16) DEFAULT NULL,
    channel VARCHAR(16) DEFAULT NULL,
    bandwidth VARCHAR(16) DEFAULT NULL,
    status VARCHAR(16) DEFAULT NULL,
    radio_number INT DEFAULT NULL,
    mode VARCHAR(32) DEFAULT NULL,
    antenna VARCHAR(32) DEFAULT NULL,
    spatial_stream VARCHAR(16) DEFAULT NULL,
    power INT DEFAULT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY fk_ap_radio_ap_serial (ap_serial),
    CONSTRAINT fk_ap_radio_ap_serial FOREIGN KEY (ap_serial) REFERENCES ap (serial)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

CREATE_AP_WLAN_STMT = """
CREATE TABLE IF NOT EXISTS ap_wlan (
    id BIGINT NOT NULL AUTO_INCREMENT,
    ap_serial VARCHAR(64) NOT NULL,
    wlan_name VARCHAR(64) DEFAULT NULL,
    security VARCHAR(64) DEFAULT NULL,
    security_level VARCHAR(64) DEFAULT NULL,
    bssid VARCHAR(32) DEFAULT NULL,
    vlan VARCHAR(16) DEFAULT NULL,
    status VARCHAR(16) DEFAULT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY fk_ap_wlan_ap_serial (ap_serial),
    CONSTRAINT fk_ap_wlan_ap_serial FOREIGN KEY (ap_serial) REFERENCES ap (serial)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

CREATE_AP_PORT_STMT = """
CREATE TABLE IF NOT EXISTS ap_port (
    id BIGINT NOT NULL AUTO_INCREMENT,
    ap_serial VARCHAR(64) NOT NULL,
    port_name VARCHAR(32) DEFAULT NULL,
    port_index INT DEFAULT NULL,
    mac_address VARCHAR(32) DEFAULT NULL,
    status VARCHAR(16) DEFAULT NULL,
    vlan_mode VARCHAR(16) DEFAULT NULL,
    allowed_vlan VARCHAR(32) DEFAULT NULL,
    native_vlan VARCHAR(16) DEFAULT NULL,
    access_vlan VARCHAR(16) DEFAULT NULL,
    speed VARCHAR(16) DEFAULT NULL,
    duplex VARCHAR(16) DEFAULT NULL,
    connector VARCHAR(16) DEFAULT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY fk_ap_port_ap_serial (ap_serial),
    CONSTRAINT fk_ap_port_ap_serial FOREIGN KEY (ap_serial) REFERENCES ap (serial)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

CREATE_AP_MODEM_STMT = """
CREATE TABLE IF NOT EXISTS ap_modem (
    id BIGINT NOT NULL AUTO_INCREMENT,
    ap_serial VARCHAR(64) NOT NULL,
    manufacturer VARCHAR(64) DEFAULT NULL,
    sim_state VARCHAR(32) DEFAULT NULL,
    status VARCHAR(16) DEFAULT NULL,
    state VARCHAR(16) DEFAULT NULL,
    model VARCHAR(64) DEFAULT NULL,
    imei VARCHAR(32) DEFAULT NULL,
    imsi VARCHAR(32) DEFAULT NULL,
    iccid VARCHAR(32) DEFAULT NULL,
    firmware_version VARCHAR(64) DEFAULT NULL,
    access_technology VARCHAR(32) DEFAULT NULL,
    bandwidth VARCHAR(16) DEFAULT NULL,
    band VARCHAR(16) DEFAULT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY fk_ap_modem_ap_serial (ap_serial),
    CONSTRAINT fk_ap_modem_ap_serial FOREIGN KEY (ap_serial) REFERENCES ap (serial)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

class MySqlRepository:
    def insert_device_status(self, rows):
        if not rows:
            return 0
        if not self.connection:
            self.connect()
        batch_size = int(os.getenv("ARUBA_DB_INSERT_BATCH_SIZE", "0")) or len(rows)
        cols = [
            '_site_id', '_site_name', 'deviceId', 'serialNumber', 'macAddress', 'deviceName', 'model', 'partNumber',
            'status', 'softwareVersion', 'ipv4', 'ipv6', 'role', 'deviceType', 'deployment', 'persona', 'deviceFunction',
            'uptimeInMillis', 'lastSeenAt', 'configLastModifiedAt'
        ]
        col_list = ",".join([f"`{c}`" for c in cols])
        placeholders = ",".join([f"%({c})s" for c in cols])
        sql = f"INSERT INTO device_status ({col_list}) VALUES ({placeholders})"
        def _prep(r): return {k: r.get(k) for k in cols}
        inserted = 0
        try:
            with self.connection.cursor() as c:
                for i in range(0, len(rows), batch_size):
                    chunk = [_prep(r) for r in rows[i:i+batch_size]]
                    c.executemany(sql, chunk)
                    inserted += len(chunk)
            self.connection.commit()
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass
            raise
        return inserted

    def insert_clients(self, rows):
        if not rows:
            return 0
        if not self.connection:
            self.connect()
        batch_size = int(os.getenv("ARUBA_DB_INSERT_BATCH_SIZE", "0")) or len(rows)
        cols = [
            'mac', '_site_id', '_site_name', 'name', 'status', 'experience', 'statusReason', 'capabilities',
            'authentication', 'type', 'ipv4', 'ipv6', 'vlanId', 'network', 'connectedDeviceSerial',
            'connectedTo', 'tunnelId', 'tunnel', 'role', 'port', 'keyManagement', 'connectedSince', 'lastSeenAt'
        ]
        col_list = ",".join([f"`{c}`" for c in cols])
        placeholders = ",".join([f"%({c})s" for c in cols])
        sql = f"INSERT INTO clients ({col_list}) VALUES ({placeholders})"
        def _prep(r): return {k: r.get(k) for k in cols}
        inserted = 0
        try:
            with self.connection.cursor() as c:
                for i in range(0, len(rows), batch_size):
                    chunk = [_prep(r) for r in rows[i:i+batch_size]]
                    c.executemany(sql, chunk)
                    inserted += len(chunk)
            self.connection.commit()
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass
            raise
        return inserted

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        if self.connection:
            return
        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )

    def close(self):
        if self.connection:
            try:
                self.connection.close()
            finally:
                self.connection = None

    def ensure_schema(self):
        if not self.connection:
            self.connect()
        # Create all required tables if they do not exist
        stmts = [
            CREATE_AP_STMT,
            CREATE_AP_RADIO_STMT,
            CREATE_AP_WLAN_STMT,
            CREATE_AP_PORT_STMT,
            CREATE_AP_MODEM_STMT,
            CREATE_DEVICE_STATUS_STMT,
            CREATE_CLIENTS_STMT,
            CREATE_SWITCH_INTERFACEDETAILS_STMT
        ]
        try:
            with self.connection.cursor() as c:
                for stmt in stmts:
                    c.execute(stmt)
            self.connection.commit()
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass
            raise

    def insert_switch_interfacedetails(self, rows):
        if not rows:
            return 0
        if not self.connection:
            self.connect()
        batch_size = int(os.getenv("ARUBA_DB_INSERT_BATCH_SIZE", "0")) or len(rows)
        cols = [
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
        col_list = ",".join([f"`{c}`" for c in cols])
        placeholders = ",".join([f"%({c})s" for c in cols])
        sql = f"INSERT INTO switch_interfacedetails ({col_list}) VALUES ({placeholders})"
        def _prep(r): return {k: r.get(k) for k in cols}
        inserted = 0
        try:
            with self.connection.cursor() as c:
                for i in range(0, len(rows), batch_size):
                    chunk = [_prep(r) for r in rows[i:i+batch_size]]
                    c.executemany(sql, chunk)
                    inserted += len(chunk)
            self.connection.commit()
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass
            raise
        return inserted

    def insert_ap(self, ap: dict):
        sql = """
        INSERT INTO ap (serial, name, mac_address, ip_address, model, status, site_id, site_name, sw_version, uptime, cluster_name, public_ip)
        VALUES (%(serial)s, %(name)s, %(mac_address)s, %(ip_address)s, %(model)s, %(status)s, %(site_id)s, %(site_name)s, %(sw_version)s, %(uptime)s, %(cluster_name)s, %(public_ip)s)
        ON DUPLICATE KEY UPDATE
          name=VALUES(name), mac_address=VALUES(mac_address), ip_address=VALUES(ip_address), model=VALUES(model), status=VALUES(status), site_id=VALUES(site_id), site_name=VALUES(site_name), sw_version=VALUES(sw_version), uptime=VALUES(uptime), cluster_name=VALUES(cluster_name), public_ip=VALUES(public_ip), updated_at=NOW()
        """
        data = {
            "serial": ap.get("serial"),
            "name": ap.get("name"),
            "mac_address": ap.get("mac_address"),
            "ip_address": ap.get("ip_address"),
            "model": ap.get("model"),
            "status": ap.get("status"),
            "site_id": ap.get("site_id"),
            "site_name": ap.get("site_name"),
            "sw_version": ap.get("sw_version"),
            "uptime": ap.get("uptime"),
            "cluster_name": ap.get("cluster_name"),
            "public_ip": ap.get("public_ip"),
        }
        try:
            with self.connection.cursor() as c:
                c.execute(sql, data)
            self.connection.commit()
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass
            raise

    def delete_ap_radios(self, ap_serial: str):
        sql = "DELETE FROM ap_radio WHERE ap_serial = %s"
        with self.connection.cursor() as c:
            c.execute(sql, (ap_serial,))
        self.connection.commit()

    def insert_ap_radio(self, ap_serial: str, radio: dict):
        sql = """
        INSERT INTO ap_radio (ap_serial, radio_index, mac_address, band, channel, bandwidth, status, radio_number, mode, antenna, spatial_stream, power)
        VALUES (%(ap_serial)s, %(radio_index)s, %(mac_address)s, %(band)s, %(channel)s, %(bandwidth)s, %(status)s, %(radio_number)s, %(mode)s, %(antenna)s, %(spatial_stream)s, %(power)s)
        """
        data = {
            "ap_serial": ap_serial,
            "radio_index": radio.get("radio_index"),
            "mac_address": radio.get("mac_address"),
            "band": radio.get("band"),
            "channel": radio.get("channel"),
            "bandwidth": radio.get("bandwidth"),
            "status": radio.get("status"),
            "radio_number": radio.get("radio_number"),
            "mode": radio.get("mode"),
            "antenna": radio.get("antenna"),
            "spatial_stream": radio.get("spatial_stream"),
            "power": radio.get("power"),
        }
        with self.connection.cursor() as c:
            c.execute(sql, data)
        self.connection.commit()

    def delete_ap_wlans(self, ap_serial: str):
        sql = "DELETE FROM ap_wlan WHERE ap_serial = %s"
        with self.connection.cursor() as c:
            c.execute(sql, (ap_serial,))
        self.connection.commit()

    def insert_ap_wlan(self, ap_serial: str, wlan: dict):
        sql = """
        INSERT INTO ap_wlan (ap_serial, wlan_name, security, security_level, bssid, vlan, status)
        VALUES (%(ap_serial)s, %(wlan_name)s, %(security)s, %(security_level)s, %(bssid)s, %(vlan)s, %(status)s)
        """
        data = {
            "ap_serial": ap_serial,
            "wlan_name": wlan.get("wlan_name"),
            "security": wlan.get("security"),
            "security_level": wlan.get("security_level"),
            "bssid": wlan.get("bssid"),
            "vlan": wlan.get("vlan"),
            "status": wlan.get("status"),
        }
        with self.connection.cursor() as c:
            c.execute(sql, data)
        self.connection.commit()

    def delete_ap_ports(self, ap_serial: str):
        sql = "DELETE FROM ap_port WHERE ap_serial = %s"
        with self.connection.cursor() as c:
            c.execute(sql, (ap_serial,))
        self.connection.commit()

    def insert_ap_port(self, ap_serial: str, port: dict):
        sql = """
        INSERT INTO ap_port (ap_serial, port_name, port_index, mac_address, status, vlan_mode, allowed_vlan, native_vlan, access_vlan, speed, duplex, connector)
        VALUES (%(ap_serial)s, %(port_name)s, %(port_index)s, %(mac_address)s, %(status)s, %(vlan_mode)s, %(allowed_vlan)s, %(native_vlan)s, %(access_vlan)s, %(speed)s, %(duplex)s, %(connector)s)
        """
        data = {
            "ap_serial": ap_serial,
            "port_name": port.get("port_name"),
            "port_index": port.get("port_index"),
            "mac_address": port.get("mac_address"),
            "status": port.get("status"),
            "vlan_mode": port.get("vlan_mode"),
            "allowed_vlan": port.get("allowed_vlan"),
            "native_vlan": port.get("native_vlan"),
            "access_vlan": port.get("access_vlan"),
            "speed": port.get("speed"),
            "duplex": port.get("duplex"),
            "connector": port.get("connector"),
        }
        with self.connection.cursor() as c:
            c.execute(sql, data)
        self.connection.commit()

    def delete_ap_modems(self, ap_serial: str):
        sql = "DELETE FROM ap_modem WHERE ap_serial = %s"
        with self.connection.cursor() as c:
            c.execute(sql, (ap_serial,))
        self.connection.commit()

    def insert_ap_modem(self, ap_serial: str, modem: dict):
        sql = """
        INSERT INTO ap_modem (ap_serial, manufacturer, sim_state, status, state, model, imei, imsi, iccid, firmware_version, access_technology, bandwidth, band)
        VALUES (%(ap_serial)s, %(manufacturer)s, %(sim_state)s, %(status)s, %(state)s, %(model)s, %(imei)s, %(imsi)s, %(iccid)s, %(firmware_version)s, %(access_technology)s, %(bandwidth)s, %(band)s)
        """
        data = {
            "ap_serial": ap_serial,
            "manufacturer": modem.get("manufacturer"),
            "sim_state": modem.get("simState"),
            "status": modem.get("status"),
            "state": modem.get("state"),
            "model": modem.get("model"),
            "imei": modem.get("imei"),
            "imsi": modem.get("imsi"),
            "iccid": modem.get("iccid"),
            "firmware_version": modem.get("firmwareVersion"),
            "access_technology": modem.get("accessTechnology"),
            "bandwidth": modem.get("bandwidth"),
            "band": modem.get("band"),
        }
        with self.connection.cursor() as c:
            c.execute(sql, data)
        self.connection.commit()