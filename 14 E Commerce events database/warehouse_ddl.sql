CREATE TABLE IF NOT EXISTS dim_user (
  user_id VARCHAR(32) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_session (
  session_id VARCHAR(32) PRIMARY KEY,
  user_id VARCHAR(32),
  INDEX (user_id)
);

CREATE TABLE IF NOT EXISTS dim_product (
  product_id VARCHAR(32) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS fact_event (
  event_id VARCHAR(64) PRIMARY KEY,
  event_type VARCHAR(64),
  event_ts DATETIME,
  ingested_at DATETIME,
  user_id VARCHAR(32),
  session_id VARCHAR(32),
  source VARCHAR(32),
  product_id VARCHAR(32),
  quantity INT,
  unit_price DECIMAL(10,2),
  currency VARCHAR(8)
);

CREATE TABLE IF NOT EXISTS fact_order (
  order_id VARCHAR(64) PRIMARY KEY,
  event_id VARCHAR(64),
  order_ts DATETIME,
  ingested_at DATETIME,
  user_id VARCHAR(32),
  session_id VARCHAR(32),
  payment_id VARCHAR(64),
  total_amount DECIMAL(10,2),
  tax_amount DECIMAL(10,2),
  shipping_amount DECIMAL(10,2),
  currency VARCHAR(8)
);

CREATE TABLE IF NOT EXISTS fact_order_item (
  order_id VARCHAR(64),
  product_id VARCHAR(32),
  quantity INT,
  unit_price DECIMAL(10,2),
  PRIMARY KEY (order_id, product_id)
);

