-- ---------------------------------------------------------------------------
-- 06-etl-control.sql — BigQuery DDL for _etl_control operational tables (3)
-- New tables required by the target architecture (not in source).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS watermarks (
  source_table                STRING NOT NULL,
  last_value                  STRING,
  last_run_ts                 TIMESTAMP,
  run_id                      STRING
);

CREATE TABLE IF NOT EXISTS dq_results (
  run_id                      STRING,
  run_date                    DATE,
  check_name                  STRING,
  target_table                STRING,
  status                      STRING,
  metric_value                FLOAT64,
  threshold                   FLOAT64,
  detail_json                 JSON,
  checked_at                  TIMESTAMP
)
PARTITION BY run_date;

CREATE TABLE IF NOT EXISTS job_audit (
  job_id                      STRING NOT NULL,
  dag_id                      STRING,
  task_id                     STRING,
  run_date                    DATE,
  start_ts                    TIMESTAMP,
  end_ts                      TIMESTAMP,
  status                      STRING,
  rows_affected               INT64,
  error_message               STRING
)
PARTITION BY run_date;
