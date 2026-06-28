-- ----------------------------------------------------------------------------
-- 03-ods: All 30 ODS tables (15 cleanse + 8 delta-merge + 3 SCD-2 + 4 ACID)
-- Adapted for source_setup: ACID tables created non-ACID (transactional
-- property removed) so a non-ACID read engine can read them.
-- No LOCATION on managed tables (harness manages paths).
-- ----------------------------------------------------------------------------

-- === Cleanse (15) ===

CREATE TABLE IF NOT EXISTS ods.ods_program (
  program_id                  BIGINT,
  client_id                   BIGINT,
  program_code                STRING,
  program_name                STRING,
  line_of_business            STRING,
  channel_mix                 STRING,
  site_code                   STRING,
  status                      STRING,
  go_live_ts                  TIMESTAMP,
  updated_ts                  TIMESTAMP
)
PARTITIONED BY (snapshot_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_contract (
  contract_id                 BIGINT,
  client_id                   BIGINT,
  program_id                  BIGINT,
  contract_no                 STRING,
  start_ts                    TIMESTAMP,
  end_ts                      TIMESTAMP,
  billing_model               STRING,
  currency                    STRING,
  signed_ts                   TIMESTAMP,
  status                      STRING
)
PARTITIONED BY (snapshot_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_contract_line (
  contract_line_id            BIGINT,
  contract_id                 BIGINT,
  line_no                     INT,
  service_code                STRING,
  uom                         STRING,
  unit_rate                   DECIMAL(12,4),
  min_commit                  DECIMAL(12,2),
  effective_ts                TIMESTAMP
)
PARTITIONED BY (snapshot_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_org_unit (
  org_unit_id                 BIGINT,
  parent_unit_id              BIGINT,
  unit_code                   STRING,
  unit_name                   STRING,
  unit_type                   STRING,
  site_code                   STRING,
  cost_center                 STRING,
  created_ts                  TIMESTAMP
)
PARTITIONED BY (snapshot_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_queue (
  queue_id                    BIGINT,
  queue_code                  STRING,
  queue_name                  STRING,
  program_id                  BIGINT,
  media_type                  STRING,
  priority                    INT,
  created_ts                  TIMESTAMP
)
PARTITIONED BY (snapshot_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_schedule (
  schedule_id                 BIGINT,
  agent_id                    BIGINT,
  shift_id                    BIGINT,
  shift_code                  STRING,
  start_ts                    TIMESTAMP,
  end_ts                      TIMESTAMP,
  paid_minutes                INT,
  activity_code               STRING,
  site_code                   STRING
)
PARTITIONED BY (sched_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_adherence_event (
  adherence_event_id          BIGINT,
  agent_id                    BIGINT,
  schedule_id                 BIGINT,
  exception_type              STRING,
  start_ts                    TIMESTAMP,
  end_ts                      TIMESTAMP,
  exception_minutes           INT,
  approved_flag               BOOLEAN
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_call (
  call_id                     BIGINT,
  queue_id                    BIGINT,
  agent_id                    BIGINT,
  program_id                  BIGINT,
  direction                   STRING,
  start_ts                    TIMESTAMP,
  answer_ts                   TIMESTAMP,
  end_ts                      TIMESTAMP,
  ring_seconds                INT,
  talk_seconds                INT,
  hold_seconds                INT,
  acw_seconds                 INT,
  abandoned_flag              BOOLEAN,
  disposition_code            STRING,
  recording_id                STRING
)
PARTITIONED BY (call_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_ivr_session (
  session_ref                 STRING,
  client_code                 STRING,
  first_event_ts              TIMESTAMP,
  last_event_ts               TIMESTAMP,
  menu_path_full              STRING,
  hops                        INT,
  contained_flag              BOOLEAN,
  exit_key                    STRING
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_chat_session (
  chat_ref                    STRING,
  client_code                 STRING,
  queue_code                  STRING,
  agent_email                 STRING,
  started_ts                  TIMESTAMP,
  ended_ts                    TIMESTAMP,
  message_count               INT,
  agent_message_count         INT,
  customer_message_count      INT,
  first_response_seconds      INT
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_email_interaction (
  email_ref                   STRING,
  client_code                 STRING,
  mailbox                     STRING,
  agent_email                 STRING,
  received_ts                 TIMESTAMP,
  first_reply_ts              TIMESTAMP,
  resolved_ts                 TIMESTAMP,
  reply_sla_minutes           INT,
  subject_category            STRING
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_survey_response (
  survey_id                   STRING,
  client_code                 STRING,
  interaction_ref             STRING,
  survey_ts                   TIMESTAMP,
  csat_score                  INT,
  nps_score                   INT,
  fcr_claimed                 BOOLEAN,
  verbatim                    STRING
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_qa_evaluation (
  qa_form_id                  STRING,
  client_code                 STRING,
  interaction_ref             STRING,
  evaluator_email             STRING,
  evaluated_ts                TIMESTAMP,
  form_version                STRING,
  section_count               INT,
  scored_points               INT,
  max_points                  INT,
  auto_fail                   BOOLEAN,
  overall_pct                 DECIMAL(5,2)
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_interaction (
  interaction_id              STRING,
  channel                     STRING,
  client_code                 STRING,
  program_id                  BIGINT,
  queue_id                    BIGINT,
  agent_id                    BIGINT,
  customer_ref                STRING,
  start_ts                    TIMESTAMP,
  end_ts                      TIMESTAMP,
  handle_seconds              INT,
  resolved_flag               BOOLEAN,
  source_system               STRING
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_dialer_attempt (
  attempt_id                  STRING,
  client_code                 STRING,
  campaign_code               STRING,
  agent_id                    BIGINT,
  attempt_ts                  TIMESTAMP,
  result_code                 STRING,
  connected_flag              BOOLEAN,
  talk_seconds                INT
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- === Delta-merge (8) ===

CREATE TABLE IF NOT EXISTS ods.ods_timesheet (
  timesheet_id                BIGINT,
  agent_id                    BIGINT,
  work_date                   STRING,
  program_id                  BIGINT,
  billable_minutes            INT,
  nonbillable_minutes         INT,
  approved_flag               BOOLEAN,
  last_change_ts              TIMESTAMP
)
PARTITIONED BY (work_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_payroll_adjustment (
  adjustment_id               BIGINT,
  agent_id                    BIGINT,
  adj_type                    STRING,
  amount                      DECIMAL(12,2),
  last_change_ts              TIMESTAMP
)
PARTITIONED BY (period_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_sla_credit (
  sla_credit_id               BIGINT,
  program_id                  BIGINT,
  sla_target_id               BIGINT,
  credit_amount               DECIMAL(12,2),
  reason                      STRING,
  last_change_ts              TIMESTAMP
)
PARTITIONED BY (period_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_callback_request (
  callback_id                 BIGINT,
  call_id                     BIGINT,
  queue_id                    BIGINT,
  requested_ts                TIMESTAMP,
  scheduled_ts                TIMESTAMP,
  completed_flag              BOOLEAN,
  last_change_ts              TIMESTAMP
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_shift_swap (
  swap_id                     BIGINT,
  requesting_agent_id         BIGINT,
  accepting_agent_id          BIGINT,
  schedule_id                 BIGINT,
  swap_date                   STRING,
  status                      STRING,
  last_change_ts              TIMESTAMP
)
PARTITIONED BY (swap_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_ticket_worklog (
  worklog_id                  BIGINT,
  ticket_id                   BIGINT,
  agent_id                    BIGINT,
  minutes_logged              INT,
  log_ts                      TIMESTAMP,
  note                        STRING,
  last_change_ts              TIMESTAMP
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_attrition_event (
  attrition_event_id          BIGINT,
  agent_id                    BIGINT,
  notice_ts                   TIMESTAMP,
  last_day                    STRING,
  attrition_type              STRING,
  reason_code                 STRING,
  regrettable_flag            BOOLEAN,
  last_change_ts              TIMESTAMP
)
PARTITIONED BY (event_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_rate_card (
  rate_card_id                BIGINT,
  program_id                  BIGINT,
  service_code                STRING,
  rate                        DECIMAL(12,4),
  currency                    STRING,
  effective_ts                TIMESTAMP,
  expiry_ts                   TIMESTAMP,
  last_change_ts              TIMESTAMP
)
PARTITIONED BY (snapshot_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- === SCD-2 (3) ===

CREATE TABLE IF NOT EXISTS ods.ods_agent_scd2 (
  agent_history_id            STRING,
  agent_id                    BIGINT,
  employee_no                 STRING,
  org_unit_id                 BIGINT,
  job_grade                   STRING,
  employment_type             STRING,
  status                      STRING,
  eff_from_ts                 TIMESTAMP,
  eff_to_ts                   TIMESTAMP,
  is_current                  BOOLEAN
)
PARTITIONED BY (eff_from_year INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_agent_skill_scd2 (
  agent_skill_history_id      STRING,
  agent_id                    BIGINT,
  skill_id                    BIGINT,
  skill_code                  STRING,
  proficiency                 INT,
  certified                   BOOLEAN,
  eff_from_ts                 TIMESTAMP,
  eff_to_ts                   TIMESTAMP,
  is_current                  BOOLEAN
)
PARTITIONED BY (eff_from_year INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_agent_assignment_scd2 (
  assignment_history_id       STRING,
  agent_id                    BIGINT,
  program_id                  BIGINT,
  queue_id                    BIGINT,
  role_on_program             STRING,
  eff_from_ts                 TIMESTAMP,
  eff_to_ts                   TIMESTAMP,
  is_current                  BOOLEAN
)
PARTITIONED BY (eff_from_year INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- === ACID-origin (4) — created NON-ACID for source_setup readability ===
-- Original: STORED AS ORC + TBLPROPERTIES ('transactional'='true', ...)
-- Adapted:  STORED AS ORC without transactional property.

CREATE TABLE IF NOT EXISTS ods.ods_client_acid (
  client_id                   BIGINT,
  client_code                 STRING,
  client_name                 STRING,
  industry                    STRING,
  hq_country                  STRING,
  status                      STRING,
  created_ts                  TIMESTAMP,
  updated_ts                  TIMESTAMP
)
CLUSTERED BY (client_id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_agent_acid (
  agent_id                    BIGINT,
  employee_no                 STRING,
  full_name                   STRING,
  email                       STRING,
  org_unit_id                 BIGINT,
  job_grade                   STRING,
  employment_type             STRING,
  hire_ts                     TIMESTAMP,
  term_ts                     TIMESTAMP,
  status                      STRING
)
CLUSTERED BY (agent_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_ticket_acid (
  ticket_id                   BIGINT,
  ticket_no                   STRING,
  program_id                  BIGINT,
  category_id                 BIGINT,
  assigned_agent_id           BIGINT,
  priority                    STRING,
  status                      STRING,
  created_ts                  TIMESTAMP,
  updated_ts                  TIMESTAMP,
  resolved_ts                 TIMESTAMP
)
CLUSTERED BY (ticket_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE TABLE IF NOT EXISTS ods.ods_invoice_acid (
  invoice_id                  BIGINT,
  invoice_no                  STRING,
  client_id                   BIGINT,
  program_id                  BIGINT,
  period_month                STRING,
  issued_ts                   TIMESTAMP,
  due_ts                      TIMESTAMP,
  currency                    STRING,
  total_amount                DECIMAL(14,2),
  status                      STRING
)
CLUSTERED BY (invoice_id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
