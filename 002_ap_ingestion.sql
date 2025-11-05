-- Aruba AP Ingestion Schema Migration
-- Adds tables for APs and all embedded arrays (radios, wlans, ports, modems)


-- Updated schema to match Aruba Central 'Get an access point details' API
CREATE TABLE IF NOT EXISTS ap (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  serial VARCHAR(64) NOT NULL UNIQUE,
  name VARCHAR(255),
  mac_address VARCHAR(32),
  ip_address VARCHAR(45),
  model VARCHAR(64),
  status VARCHAR(32),
  site_id VARCHAR(64),
  site_name VARCHAR(255),
  sw_version VARCHAR(64),
  uptime BIGINT,
  cluster_name VARCHAR(255),
  public_ip VARCHAR(45),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_serial (serial)
);

CREATE TABLE IF NOT EXISTS ap_radio (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ap_serial VARCHAR(64) NOT NULL,
  radio_index INT,
  mac_address VARCHAR(32),
  band VARCHAR(16),
  channel VARCHAR(16),
  bandwidth VARCHAR(16),
  status VARCHAR(16),
  radio_number INT,
  mode VARCHAR(32),
  antenna VARCHAR(32),
  spatial_stream VARCHAR(16),
  power INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_ap_radio_ap_serial FOREIGN KEY (ap_serial) REFERENCES ap(serial)
);

CREATE TABLE IF NOT EXISTS ap_wlan (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ap_serial VARCHAR(64) NOT NULL,
  wlan_name VARCHAR(64),
  security VARCHAR(64),
  security_level VARCHAR(64),
  bssid VARCHAR(32),
  vlan VARCHAR(16),
  status VARCHAR(16),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_ap_wlan_ap_serial FOREIGN KEY (ap_serial) REFERENCES ap(serial)
);

CREATE TABLE IF NOT EXISTS ap_port (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ap_serial VARCHAR(64) NOT NULL,
  port_name VARCHAR(32),
  port_index INT,
  mac_address VARCHAR(32),
  status VARCHAR(16),
  vlan_mode VARCHAR(16),
  allowed_vlan VARCHAR(32),
  native_vlan VARCHAR(16),
  access_vlan VARCHAR(16),
  speed VARCHAR(16),
  duplex VARCHAR(16),
  connector VARCHAR(16),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_ap_port_ap_serial FOREIGN KEY (ap_serial) REFERENCES ap(serial)
);

CREATE TABLE IF NOT EXISTS ap_modem (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ap_serial VARCHAR(64) NOT NULL,
  manufacturer VARCHAR(64),
  sim_state VARCHAR(32),
  status VARCHAR(16),
  state VARCHAR(16),
  model VARCHAR(64),
  imei VARCHAR(32),
  imsi VARCHAR(32),
  iccid VARCHAR(32),
  firmware_version VARCHAR(64),
  access_technology VARCHAR(32),
  bandwidth VARCHAR(16),
  band VARCHAR(16),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_ap_modem_ap_serial FOREIGN KEY (ap_serial) REFERENCES ap(serial)
);

-- Note: Only the AP table should use upsert (ON DUPLICATE KEY UPDATE) in ingestion code.
-- All sub-tables (radios, wlans, ports, modems) should remain append-only for history.
