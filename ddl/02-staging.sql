-- ---------------------------------------------------------------------------
-- 02-staging.sql — BigQuery DDL for staging layer (45 tables)
-- 27 sqoop mirrors + 8 delta feeds + 10 file feeds
-- All STRING partitions converted to DATE. 90-day partition expiration.
-- Complex types: ARRAY<STRUCT>, MAP->ARRAY<STRUCT<key,value>>, ARRAY<STRING>
-- Epoch COMMENTs preserved as column OPTIONS(description=...).
-- ---------------------------------------------------------------------------

-- === Sqoop mirrors (27) ===

CREATE TABLE IF NOT EXISTS stg_crm_client (
  client_id                     INT64,
  client_code                   STRING,
  client_name                   STRING,
  industry                      STRING,
  hq_country                    STRING,
  status                        STRING,
  created_ts                    INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  updated_ts                    INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_crm_client_contact (
  contact_id                    INT64,
  client_id                     INT64,
  full_name                     STRING,
  email                         STRING,
  phone                         STRING,
  role                          STRING,
  is_primary                    BOOL,
  created_ts                    INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_crm_program (
  program_id                    INT64,
  client_id                     INT64,
  program_code                  STRING,
  program_name                  STRING,
  line_of_business              STRING,
  channel_mix                   STRING,
  site_code                     STRING,
  status                        STRING,
  go_live_ts                    INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  updated_ts                    INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_crm_contract (
  contract_id                   INT64,
  client_id                     INT64,
  program_id                    INT64,
  contract_no                   STRING,
  start_dt                      STRING OPTIONS(description="Oracle string YYYYMMDDHH24MISS (legacy)"),
  end_dt                        STRING OPTIONS(description="Oracle string YYYYMMDDHH24MISS (legacy)"),
  billing_model                 STRING,
  currency                      STRING,
  signed_dt                     STRING OPTIONS(description="Oracle string YYYYMMDDHH24MISS (legacy)"),
  status                        STRING,
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_crm_contract_line (
  contract_line_id              INT64,
  contract_id                   INT64,
  line_no                       INT64,
  service_code                  STRING,
  uom                           STRING,
  unit_rate                     NUMERIC(12,4),
  min_commit                    NUMERIC(12,2),
  effective_dt                  STRING OPTIONS(description="Oracle string YYYYMMDDHH24MISS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_crm_sla_target (
  sla_target_id                 INT64,
  program_id                    INT64,
  queue_id                      INT64,
  metric_code                   STRING,
  target_value                  NUMERIC(10,4),
  penalty_pct                   NUMERIC(5,2),
  effective_ts                  INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_hr_agent (
  agent_id                      INT64,
  employee_no                   STRING,
  first_name                    STRING,
  last_name                     STRING,
  email                         STRING,
  org_unit_id                   INT64,
  job_grade                     STRING,
  employment_type               STRING,
  hire_ts                       INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  term_ts                       INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  status                        STRING,
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_hr_org_unit (
  org_unit_id                   INT64,
  parent_unit_id                INT64,
  unit_code                     STRING,
  unit_name                     STRING,
  unit_type                     STRING,
  site_code                     STRING,
  cost_center                   STRING,
  created_ts                    INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_hr_employment_event (
  event_id                      INT64,
  agent_id                      INT64,
  event_type                    STRING,
  event_ts                      INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  from_org_unit_id              INT64,
  to_org_unit_id                INT64,
  reason_code                   STRING,
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_hr_skill (
  skill_id                      INT64,
  skill_code                    STRING,
  skill_name                    STRING,
  skill_family                  STRING,
  created_ts                    INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_hr_agent_skill (
  agent_skill_id                INT64,
  agent_id                      INT64,
  skill_id                      INT64,
  proficiency                   INT64,
  certified                     BOOL,
  effective_ts                  INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  expiry_ts                     INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_wfm_shift (
  shift_id                      INT64,
  shift_code                    STRING,
  shift_name                    STRING,
  start_hhmm                    STRING,
  end_hhmm                      STRING,
  overnight_flag                BOOL,
  site_code                     STRING,
  created_epoch                 INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_wfm_schedule (
  schedule_id                   INT64,
  agent_id                      INT64,
  shift_id                      INT64,
  sched_date                    STRING,
  start_epoch                   INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  end_epoch                     INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  paid_minutes                  INT64,
  activity_code                 STRING,
  load_date                     DATE,
  site_code                     STRING
)
PARTITION BY load_date
CLUSTER BY site_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_wfm_adherence_event (
  adherence_event_id            INT64,
  agent_id                      INT64,
  schedule_id                   INT64,
  exception_type                STRING,
  start_epoch                   INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  end_epoch                     INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  approved_flag                 BOOL,
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_wfm_forecast (
  forecast_id                   INT64,
  queue_id                      INT64,
  interval_start_epoch          INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  forecast_volume               INT64,
  forecast_aht_sec              INT64,
  required_fte                  NUMERIC(8,2),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_wfm_timeoff_request (
  timeoff_id                    INT64,
  agent_id                      INT64,
  request_epoch                 INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  start_date                    STRING,
  end_date                      STRING,
  timeoff_type                  STRING,
  status                        STRING,
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tel_call (
  call_id                       INT64,
  ani                           STRING,
  dnis                          STRING,
  queue_id                      INT64,
  agent_id                      INT64,
  program_id                    INT64,
  start_epoch                   INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  answer_epoch                  INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  end_epoch                     INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  disposition_code              STRING,
  direction                     STRING,
  recording_id                  STRING,
  load_date                     DATE
)
PARTITION BY load_date
CLUSTER BY call_id
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tel_call_segment (
  segment_id                    INT64,
  call_id                       INT64,
  segment_no                    INT64,
  segment_type                  STRING,
  start_epoch                   INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  end_epoch                     INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  agent_id                      INT64,
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tel_queue (
  queue_id                      INT64,
  queue_code                    STRING,
  queue_name                    STRING,
  program_id                    INT64,
  media_type                    STRING,
  priority                      INT64,
  created_epoch                 INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tel_agent_state_event (
  state_event_id                INT64,
  agent_id                      INT64,
  state_code                    STRING,
  start_epoch                   INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  end_epoch                     INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  reason_code                   STRING,
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tel_disposition_code (
  disposition_code              STRING,
  disposition_desc              STRING,
  category                      STRING,
  billable_flag                 BOOL,
  created_epoch                 INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tkt_ticket (
  ticket_id                     INT64,
  ticket_no                     STRING,
  program_id                    INT64,
  category_id                   INT64,
  opened_by_agent_id            INT64,
  assigned_agent_id             INT64,
  interaction_ref               STRING,
  priority                      STRING,
  status                        STRING,
  created_ms                    INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  updated_ms                    INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tkt_ticket_event (
  ticket_event_id               INT64,
  ticket_id                     INT64,
  event_type                    STRING,
  event_ms                      INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  actor_agent_id                INT64,
  old_value                     STRING,
  new_value                     STRING,
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tkt_category (
  category_id                   INT64,
  category_code                 STRING,
  category_name                 STRING,
  sla_hours                     INT64,
  created_ms                    INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_fin_invoice (
  invoice_id                    INT64,
  invoice_no                    STRING,
  client_id                     INT64,
  program_id                    INT64,
  period_month                  STRING,
  issued_ts_sec                 INT64 OPTIONS(description="!! name says seconds, VALUES ARE MILLIS !!"),
  due_ts_sec                    INT64 OPTIONS(description="!! name says seconds, VALUES ARE MILLIS !!"),
  currency                      STRING,
  total_amount                  NUMERIC(14,2),
  status                        STRING,
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_fin_invoice_line (
  invoice_line_id               INT64,
  invoice_id                    INT64,
  contract_line_id              INT64,
  qty                           NUMERIC(12,2),
  unit_rate                     NUMERIC(12,4),
  line_amount                   NUMERIC(14,2),
  adjustment_flag               BOOL,
  created_ms                    INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_fin_rate_card (
  rate_card_id                  INT64,
  program_id                    INT64,
  service_code                  STRING,
  rate                          NUMERIC(12,4),
  currency                      STRING,
  effective_ts                  INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  expiry_ts                     INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  load_date                     DATE
)
PARTITION BY load_date
OPTIONS(partition_expiration_days=90);


-- === Delta feeds (8) ===

CREATE TABLE IF NOT EXISTS stg_fin_timesheet_delta (
  timesheet_id                  INT64,
  agent_id                      INT64,
  work_date                     STRING,
  program_id                    INT64,
  billable_minutes              INT64,
  nonbillable_minutes           INT64,
  approved_flag                 BOOL,
  op                            STRING,
  change_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  extract_ts                    DATE
)
PARTITION BY extract_ts
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_fin_payroll_adj_delta (
  adjustment_id                 INT64,
  agent_id                      INT64,
  period_month                  STRING,
  adj_type                      STRING,
  amount                        NUMERIC(12,2),
  op                            STRING,
  change_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  extract_ts                    DATE
)
PARTITION BY extract_ts
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_crm_sla_credit_delta (
  sla_credit_id                 INT64,
  program_id                    INT64,
  sla_target_id                 INT64,
  period_month                  STRING,
  credit_amount                 NUMERIC(12,2),
  reason                        STRING,
  op                            STRING,
  change_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  extract_ts                    DATE
)
PARTITION BY extract_ts
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tel_callback_request_delta (
  callback_id                   INT64,
  call_id                       INT64,
  queue_id                      INT64,
  requested_epoch               INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  scheduled_epoch               INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  completed_flag                BOOL,
  op                            STRING,
  change_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  extract_ts                    DATE
)
PARTITION BY extract_ts
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_wfm_shift_swap_delta (
  swap_id                       INT64,
  requesting_agent_id           INT64,
  accepting_agent_id            INT64,
  schedule_id                   INT64,
  swap_date                     STRING,
  status                        STRING,
  op                            STRING,
  change_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  extract_ts                    DATE
)
PARTITION BY extract_ts
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_tkt_worklog_delta (
  worklog_id                    INT64,
  ticket_id                     INT64,
  agent_id                      INT64,
  minutes_logged                INT64,
  log_ms                        INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  note                          STRING,
  op                            STRING,
  change_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  extract_ts                    DATE
)
PARTITION BY extract_ts
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_hr_attrition_event_delta (
  attrition_event_id            INT64,
  agent_id                      INT64,
  notice_epoch                  INT64 OPTIONS(description="epoch SECONDS (legacy)"),
  last_day                      STRING,
  attrition_type                STRING,
  reason_code                   STRING,
  regrettable_flag              BOOL,
  op                            STRING,
  change_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  extract_ts                    DATE
)
PARTITION BY extract_ts
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_fin_rate_card_change_delta (
  rate_change_id                INT64,
  rate_card_id                  INT64,
  old_rate                      NUMERIC(12,4),
  new_rate                      NUMERIC(12,4),
  change_reason                 STRING,
  op                            STRING,
  change_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  extract_ts                    DATE
)
PARTITION BY extract_ts
OPTIONS(partition_expiration_days=90);


-- === File feeds (10) ===

CREATE TABLE IF NOT EXISTS stg_file_interaction_export (
  interaction_ref               STRING,
  channel                       STRING,
  client_interaction_id         STRING,
  agent_email                   STRING,
  start_ms                      INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  end_ms                        INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  outcome                       STRING,
  customer_ref                  STRING,
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_file_survey_csat (
  survey_id                     STRING,
  interaction_ref               STRING,
  survey_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  csat_score                    INT64,
  nps_score                     INT64,
  fcr_claimed                   BOOL,
  verbatim                      STRING,
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_file_qa_forms (
  qa_form_id                    STRING,
  interaction_ref               STRING,
  evaluator_email               STRING,
  evaluated_ms                  INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  form_version                  STRING,
  sections                      ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>,
  auto_fail                     BOOL,
  overall_pct                   NUMERIC(5,2),
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_file_ivr_logs (
  event_ms                      INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  session_ref                   STRING,
  menu_path                     STRING,
  key_pressed                   STRING,
  raw_tail                      STRING,
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_file_chat_transcripts (
  chat_ref                      STRING,
  queue_code                    STRING,
  agent_email                   STRING,
  started_ms                    INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  ended_ms                      INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  messages                      ARRAY<STRUCT<sender STRING, ts_ms INT64, text STRING>>,
  metadata                      ARRAY<STRUCT<key STRING, value STRING>>,
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_file_roster (
  employee_no                   STRING,
  agent_email                   STRING,
  client_login                  STRING,
  role_on_program               STRING,
  active_flag                   BOOL,
  as_of_ms                      INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_file_telco_invoice (
  telco_invoice_id              STRING,
  carrier                       STRING,
  circuit_id                    STRING,
  usage_minutes                 INT64,
  charge_amount                 NUMERIC(12,2),
  bill_period                   STRING,
  billed_ms                     INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_file_dialer_result (
  attempt_id                    STRING,
  campaign_code                 STRING,
  phone_hash                    STRING,
  agent_id                      INT64,
  attempt_ms                    INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  result_code                   STRING,
  talk_seconds                  INT64,
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_file_email_interaction (
  email_ref                     STRING,
  mailbox                       STRING,
  agent_email                   STRING,
  received_ms                   INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  first_reply_ms                INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  resolved_ms                   INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  subject_category              STRING,
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);


CREATE TABLE IF NOT EXISTS stg_file_speech_analytics (
  recording_id                  STRING,
  call_ref                      STRING,
  analyzed_ms                   INT64 OPTIONS(description="epoch MILLISECONDS (legacy)"),
  sentiment_score               FLOAT64,
  silence_pct                   FLOAT64,
  talk_over_count               INT64,
  keywords                      ARRAY<STRING>,
  client_code                   STRING,
  feed_date                     DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(partition_expiration_days=90);

