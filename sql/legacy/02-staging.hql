-- ----------------------------------------------------------------------------
-- 02-staging: All 45 staging tables (27 sqoop + 8 delta + 10 file feeds)
-- Adapted for source_setup: hdfs:// LOCATIONs rehosted to ${SOURCE_WAREHOUSE}.
-- All original types, COMMENTs, PARTITIONED BY, CLUSTERED BY, SERDE preserved.
-- ----------------------------------------------------------------------------

-- === Sqoop mirrors (27) ===

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_crm_client (
  client_id                   BIGINT,
  client_code                 STRING,
  client_name                 STRING,
  industry                    STRING,
  hq_country                  STRING,
  status                      STRING,
  created_ts                  BIGINT COMMENT 'epoch SECONDS (legacy)',
  updated_ts                  BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_crm_client'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_crm_client_contact (
  contact_id                  BIGINT,
  client_id                   BIGINT,
  full_name                   STRING,
  email                       STRING,
  phone                       STRING,
  role                        STRING,
  is_primary                  BOOLEAN,
  created_ts                  BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_crm_client_contact'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_crm_program (
  program_id                  BIGINT,
  client_id                   BIGINT,
  program_code                STRING,
  program_name                STRING,
  line_of_business            STRING,
  channel_mix                 STRING,
  site_code                   STRING,
  status                      STRING,
  go_live_ts                  BIGINT COMMENT 'epoch SECONDS (legacy)',
  updated_ts                  BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_crm_program'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_crm_contract (
  contract_id                 BIGINT,
  client_id                   BIGINT,
  program_id                  BIGINT,
  contract_no                 STRING,
  start_dt                    STRING COMMENT 'Oracle string YYYYMMDDHH24MISS (legacy)',
  end_dt                      STRING COMMENT 'Oracle string YYYYMMDDHH24MISS (legacy)',
  billing_model               STRING,
  currency                    STRING,
  signed_dt                   STRING COMMENT 'Oracle string YYYYMMDDHH24MISS (legacy)',
  status                      STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_crm_contract'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_crm_contract_line (
  contract_line_id            BIGINT,
  contract_id                 BIGINT,
  line_no                     INT,
  service_code                STRING,
  uom                         STRING,
  unit_rate                   DECIMAL(12,4),
  min_commit                  DECIMAL(12,2),
  effective_dt                STRING COMMENT 'Oracle string YYYYMMDDHH24MISS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_crm_contract_line'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_crm_sla_target (
  sla_target_id               BIGINT,
  program_id                  BIGINT,
  queue_id                    BIGINT,
  metric_code                 STRING,
  target_value                DECIMAL(10,4),
  penalty_pct                 DECIMAL(5,2),
  effective_ts                BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_crm_sla_target'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_hr_agent (
  agent_id                    BIGINT,
  employee_no                 STRING,
  first_name                  STRING,
  last_name                   STRING,
  email                       STRING,
  org_unit_id                 BIGINT,
  job_grade                   STRING,
  employment_type             STRING,
  hire_ts                     BIGINT COMMENT 'epoch SECONDS (legacy)',
  term_ts                     BIGINT COMMENT 'epoch SECONDS (legacy)',
  status                      STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_hr_agent'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_hr_org_unit (
  org_unit_id                 BIGINT,
  parent_unit_id              BIGINT,
  unit_code                   STRING,
  unit_name                   STRING,
  unit_type                   STRING,
  site_code                   STRING,
  cost_center                 STRING,
  created_ts                  BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_hr_org_unit'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_hr_employment_event (
  event_id                    BIGINT,
  agent_id                    BIGINT,
  event_type                  STRING,
  event_ts                    BIGINT COMMENT 'epoch SECONDS (legacy)',
  from_org_unit_id            BIGINT,
  to_org_unit_id              BIGINT,
  reason_code                 STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_hr_employment_event'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_hr_skill (
  skill_id                    BIGINT,
  skill_code                  STRING,
  skill_name                  STRING,
  skill_family                STRING,
  created_ts                  BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_hr_skill'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_hr_agent_skill (
  agent_skill_id              BIGINT,
  agent_id                    BIGINT,
  skill_id                    BIGINT,
  proficiency                 INT,
  certified                   BOOLEAN,
  effective_ts                BIGINT COMMENT 'epoch SECONDS (legacy)',
  expiry_ts                   BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_hr_agent_skill'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_wfm_shift (
  shift_id                    BIGINT,
  shift_code                  STRING,
  shift_name                  STRING,
  start_hhmm                  STRING,
  end_hhmm                    STRING,
  overnight_flag              BOOLEAN,
  site_code                   STRING,
  created_epoch               BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_wfm_shift'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_wfm_schedule (
  schedule_id                 BIGINT,
  agent_id                    BIGINT,
  shift_id                    BIGINT,
  sched_date                  STRING,
  start_epoch                 BIGINT COMMENT 'epoch SECONDS (legacy)',
  end_epoch                   BIGINT COMMENT 'epoch SECONDS (legacy)',
  paid_minutes                INT,
  activity_code               STRING
)
PARTITIONED BY (load_date STRING, site_code STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_wfm_schedule'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_wfm_adherence_event (
  adherence_event_id          BIGINT,
  agent_id                    BIGINT,
  schedule_id                 BIGINT,
  exception_type              STRING,
  start_epoch                 BIGINT COMMENT 'epoch SECONDS (legacy)',
  end_epoch                   BIGINT COMMENT 'epoch SECONDS (legacy)',
  approved_flag               BOOLEAN
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_wfm_adherence_event'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_wfm_forecast (
  forecast_id                 BIGINT,
  queue_id                    BIGINT,
  interval_start_epoch        BIGINT COMMENT 'epoch SECONDS (legacy)',
  forecast_volume             INT,
  forecast_aht_sec            INT,
  required_fte                DECIMAL(8,2)
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_wfm_forecast'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_wfm_timeoff_request (
  timeoff_id                  BIGINT,
  agent_id                    BIGINT,
  request_epoch               BIGINT COMMENT 'epoch SECONDS (legacy)',
  start_date                  STRING,
  end_date                    STRING,
  timeoff_type                STRING,
  status                      STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_wfm_timeoff_request'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tel_call (
  call_id                     BIGINT,
  ani                         STRING,
  dnis                        STRING,
  queue_id                    BIGINT,
  agent_id                    BIGINT,
  program_id                  BIGINT,
  start_epoch                 BIGINT COMMENT 'epoch SECONDS (legacy)',
  answer_epoch                BIGINT COMMENT 'epoch SECONDS (legacy)',
  end_epoch                   BIGINT COMMENT 'epoch SECONDS (legacy)',
  disposition_code            STRING,
  direction                   STRING,
  recording_id                STRING
)
PARTITIONED BY (load_date STRING)
CLUSTERED BY (call_id) INTO 16 BUCKETS
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tel_call'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tel_call_segment (
  segment_id                  BIGINT,
  call_id                     BIGINT,
  segment_no                  INT,
  segment_type                STRING,
  start_epoch                 BIGINT COMMENT 'epoch SECONDS (legacy)',
  end_epoch                   BIGINT COMMENT 'epoch SECONDS (legacy)',
  agent_id                    BIGINT
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tel_call_segment'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tel_queue (
  queue_id                    BIGINT,
  queue_code                  STRING,
  queue_name                  STRING,
  program_id                  BIGINT,
  media_type                  STRING,
  priority                    INT,
  created_epoch               BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tel_queue'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tel_agent_state_event (
  state_event_id              BIGINT,
  agent_id                    BIGINT,
  state_code                  STRING,
  start_epoch                 BIGINT COMMENT 'epoch SECONDS (legacy)',
  end_epoch                   BIGINT COMMENT 'epoch SECONDS (legacy)',
  reason_code                 STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tel_agent_state_event'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tel_disposition_code (
  disposition_code            STRING,
  disposition_desc            STRING,
  category                    STRING,
  billable_flag               BOOLEAN,
  created_epoch               BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tel_disposition_code'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tkt_ticket (
  ticket_id                   BIGINT,
  ticket_no                   STRING,
  program_id                  BIGINT,
  category_id                 BIGINT,
  opened_by_agent_id          BIGINT,
  assigned_agent_id           BIGINT,
  interaction_ref             STRING,
  priority                    STRING,
  status                      STRING,
  created_ms                  BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  updated_ms                  BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tkt_ticket'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tkt_ticket_event (
  ticket_event_id             BIGINT,
  ticket_id                   BIGINT,
  event_type                  STRING,
  event_ms                    BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  actor_agent_id              BIGINT,
  old_value                   STRING,
  new_value                   STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tkt_ticket_event'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tkt_category (
  category_id                 BIGINT,
  category_code               STRING,
  category_name               STRING,
  sla_hours                   INT,
  created_ms                  BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tkt_category'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_fin_invoice (
  invoice_id                  BIGINT,
  invoice_no                  STRING,
  client_id                   BIGINT,
  program_id                  BIGINT,
  period_month                STRING,
  issued_ts_sec               BIGINT COMMENT '!! name says seconds, VALUES ARE MILLIS !!',
  due_ts_sec                  BIGINT COMMENT '!! name says seconds, VALUES ARE MILLIS !!',
  currency                    STRING,
  total_amount                DECIMAL(14,2),
  status                      STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_fin_invoice'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_fin_invoice_line (
  invoice_line_id             BIGINT,
  invoice_id                  BIGINT,
  contract_line_id            BIGINT,
  qty                         DECIMAL(12,2),
  unit_rate                   DECIMAL(12,4),
  line_amount                 DECIMAL(14,2),
  adjustment_flag             BOOLEAN,
  created_ms                  BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_fin_invoice_line'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_fin_rate_card (
  rate_card_id                BIGINT,
  program_id                  BIGINT,
  service_code                STRING,
  rate                        DECIMAL(12,4),
  currency                    STRING,
  effective_ts                BIGINT COMMENT 'epoch SECONDS (legacy)',
  expiry_ts                   BIGINT COMMENT 'epoch SECONDS (legacy)'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_fin_rate_card'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- === Delta feeds (8) ===

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_fin_timesheet_delta (
  timesheet_id                BIGINT,
  agent_id                    BIGINT,
  work_date                   STRING,
  program_id                  BIGINT,
  billable_minutes            INT,
  nonbillable_minutes         INT,
  approved_flag               BOOLEAN,
  op                          STRING,
  change_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (extract_ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_fin_timesheet_delta';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_fin_payroll_adj_delta (
  adjustment_id               BIGINT,
  agent_id                    BIGINT,
  period_month                STRING,
  adj_type                    STRING,
  amount                      DECIMAL(12,2),
  op                          STRING,
  change_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (extract_ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_fin_payroll_adj_delta';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_crm_sla_credit_delta (
  sla_credit_id               BIGINT,
  program_id                  BIGINT,
  sla_target_id               BIGINT,
  period_month                STRING,
  credit_amount               DECIMAL(12,2),
  reason                      STRING,
  op                          STRING,
  change_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (extract_ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_crm_sla_credit_delta';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tel_callback_request_delta (
  callback_id                 BIGINT,
  call_id                     BIGINT,
  queue_id                    BIGINT,
  requested_epoch             BIGINT COMMENT 'epoch SECONDS (legacy)',
  scheduled_epoch             BIGINT COMMENT 'epoch SECONDS (legacy)',
  completed_flag              BOOLEAN,
  op                          STRING,
  change_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (extract_ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tel_callback_request_delta';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_wfm_shift_swap_delta (
  swap_id                     BIGINT,
  requesting_agent_id         BIGINT,
  accepting_agent_id          BIGINT,
  schedule_id                 BIGINT,
  swap_date                   STRING,
  status                      STRING,
  op                          STRING,
  change_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (extract_ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_wfm_shift_swap_delta';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_tkt_worklog_delta (
  worklog_id                  BIGINT,
  ticket_id                   BIGINT,
  agent_id                    BIGINT,
  minutes_logged              INT,
  log_ms                      BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  note                        STRING,
  op                          STRING,
  change_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (extract_ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_tkt_worklog_delta';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_hr_attrition_event_delta (
  attrition_event_id          BIGINT,
  agent_id                    BIGINT,
  notice_epoch                BIGINT COMMENT 'epoch SECONDS (legacy)',
  last_day                    STRING,
  attrition_type              STRING,
  reason_code                 STRING,
  regrettable_flag            BOOLEAN,
  op                          STRING,
  change_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (extract_ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_hr_attrition_event_delta';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_fin_rate_card_change_delta (
  rate_change_id              BIGINT,
  rate_card_id                BIGINT,
  old_rate                    DECIMAL(12,4),
  new_rate                    DECIMAL(12,4),
  change_reason               STRING,
  op                          STRING,
  change_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (extract_ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_fin_rate_card_change_delta';

-- === File feeds (10) ===

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_interaction_export (
  interaction_ref             STRING,
  channel                     STRING,
  client_interaction_id       STRING,
  agent_email                 STRING,
  start_ms                    BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  end_ms                      BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  outcome                     STRING,
  customer_ref                STRING
)
PARTITIONED BY (client_code STRING, feed_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_interaction_export'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_survey_csat (
  survey_id                   STRING,
  interaction_ref             STRING,
  survey_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  csat_score                  INT,
  nps_score                   INT,
  fcr_claimed                 BOOLEAN,
  verbatim                    STRING
)
PARTITIONED BY (client_code STRING, feed_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_survey_csat'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_qa_forms (
  qa_form_id                  STRING,
  interaction_ref             STRING,
  evaluator_email             STRING,
  evaluated_ms                BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  form_version                STRING,
  sections                    ARRAY<STRUCT<section_code:STRING,max_points:INT,scored_points:INT>>,
  auto_fail                   BOOLEAN,
  overall_pct                 DECIMAL(5,2)
)
PARTITIONED BY (client_code STRING, feed_date STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_qa_forms'
TBLPROPERTIES ('ignore.malformed.json'='true');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_ivr_logs (
  event_ms                    BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  session_ref                 STRING,
  menu_path                   STRING,
  key_pressed                 STRING,
  raw_tail                    STRING
)
PARTITIONED BY (client_code STRING, feed_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex' = '^(\\d+)\\|([A-Z0-9-]+)\\|MENU:([^;]*);KEY:([0-9*#])\\|(.*)$')
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_ivr_logs';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_chat_transcripts (
  chat_ref                    STRING,
  queue_code                  STRING,
  agent_email                 STRING,
  started_ms                  BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  ended_ms                    BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  messages                    ARRAY<STRUCT<sender:STRING,ts_ms:BIGINT,text:STRING>>,
  metadata                    MAP<STRING,STRING>
)
PARTITIONED BY (client_code STRING, feed_date STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_chat_transcripts'
TBLPROPERTIES ('ignore.malformed.json'='true');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_roster (
  employee_no                 STRING,
  agent_email                 STRING,
  client_login                STRING,
  role_on_program             STRING,
  active_flag                 BOOLEAN,
  as_of_ms                    BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (client_code STRING, feed_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_roster'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_telco_invoice (
  telco_invoice_id            STRING,
  carrier                     STRING,
  circuit_id                  STRING,
  usage_minutes               BIGINT,
  charge_amount               DECIMAL(12,2),
  bill_period                 STRING,
  billed_ms                   BIGINT COMMENT 'epoch MILLISECONDS (legacy)'
)
PARTITIONED BY (client_code STRING, feed_date STRING)
STORED AS SEQUENCEFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_telco_invoice';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_dialer_result (
  attempt_id                  STRING,
  campaign_code               STRING,
  phone_hash                  STRING,
  agent_id                    BIGINT,
  attempt_ms                  BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  result_code                 STRING,
  talk_seconds                INT
)
PARTITIONED BY (client_code STRING, feed_date STRING)
STORED AS RCFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_dialer_result';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_email_interaction (
  email_ref                   STRING,
  mailbox                     STRING,
  agent_email                 STRING,
  received_ms                 BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  first_reply_ms              BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  resolved_ms                 BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  subject_category            STRING
)
PARTITIONED BY (client_code STRING, feed_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_email_interaction'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.stg_file_speech_analytics (
  recording_id                STRING,
  call_ref                    STRING,
  analyzed_ms                 BIGINT COMMENT 'epoch MILLISECONDS (legacy)',
  sentiment_score             DOUBLE,
  silence_pct                 DOUBLE,
  talk_over_count             INT,
  keywords                    ARRAY<STRING>
)
PARTITIONED BY (client_code STRING, feed_date STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${SOURCE_WAREHOUSE}/staging/stg_file_speech_analytics'
TBLPROPERTIES ('ignore.malformed.json'='true');
