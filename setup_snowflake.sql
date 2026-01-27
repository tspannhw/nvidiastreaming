-- Jetson Orin Snowpipe Streaming v2 setup
-- Update role/user names as needed for your account.

CREATE OR REPLACE DATABASE DEMO;
CREATE OR REPLACE SCHEMA DEMO.DEMO;

CREATE OR REPLACE TABLE DEMO.DEMO.JETSON_EDGE_STREAM (
  row_id STRING,
  host STRING,
  ip_address STRING,
  mac_address STRING,
  ts_utc TIMESTAMP_NTZ,
  ts_epoch_ms NUMBER,
  cpu_temp_c NUMBER(10, 3),
  cpu_usage_pct NUMBER(10, 3),
  mem_usage_pct NUMBER(10, 3),
  disk_usage_pct NUMBER(10, 3),
  thermal_zones VARIANT,
  edge_ai_summary STRING,
  image_path STRING,
  image_captured BOOLEAN,
  image_ai_summary STRING,
  payload VARIANT
);

-- The default streaming pipe for the table is created by Snowflake:
-- JETSON_EDGE_STREAM-STREAMING
