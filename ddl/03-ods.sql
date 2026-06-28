-- ---------------------------------------------------------------------------
-- 03-ods.sql — BigQuery DDL for ODS layer (30 tables)
-- 15 cleanse + 8 delta-merge + 3 SCD-2 + 4 ACID-origin
-- STRING partitions converted to DATE. SCD-2: eff_from_date added.
-- ACID CLUSTERED BY -> CLUSTER BY (no BUCKETS).
-- ---------------------------------------------------------------------------

-- === Cleanse (15) ===

CREATE TABLE IF NOT EXISTS ods_program (
  program_id                    INT64,
  client_id                     INT64,
  program_code                  STRING,
  program_name                  STRING,
  line_of_business              STRING,
  channel_mix                   STRING,
  site_code                     STRING,
  status                        STRING,
  go_live_ts                    TIMESTAMP,
  updated_ts                    TIMESTAMP,
  snapshot_date                 DATE
)
PARTITION BY snapshot_date;


CREATE TABLE IF NOT EXISTS ods_contract (
  contract_id                   INT64,
  client_id                     INT64,
  program_id                    INT64,
  contract_no                   STRING,
  start_ts                      TIMESTAMP,
  end_ts                        TIMESTAMP,
  billing_model                 STRING,
  currency                      STRING,
  signed_ts                     TIMESTAMP,
  status                        STRING,
  snapshot_date                 DATE
)
PARTITION BY snapshot_date;


CREATE TABLE IF NOT EXISTS ods_contract_line (
  contract_line_id              INT64,
  contract_id                   INT64,
  line_no                       INT64,
  service_code                  STRING,
  uom                           STRING,
  unit_rate                     NUMERIC(12,4),
  min_commit                    NUMERIC(12,2),
  effective_ts                  TIMESTAMP,
  snapshot_date                 DATE
)
PARTITION BY snapshot_date;


CREATE TABLE IF NOT EXISTS ods_org_unit (
  org_unit_id                   INT64,
  parent_unit_id                INT64,
  unit_code                     STRING,
  unit_name                     STRING,
  unit_type                     STRING,
  site_code                     STRING,
  cost_center                   STRING,
  created_ts                    TIMESTAMP,
  snapshot_date                 DATE
)
PARTITION BY snapshot_date;


CREATE TABLE IF NOT EXISTS ods_queue (
  queue_id                      INT64,
  queue_code                    STRING,
  queue_name                    STRING,
  program_id                    INT64,
  media_type                    STRING,
  priority                      INT64,
  created_ts                    TIMESTAMP,
  snapshot_date                 DATE
)
PARTITION BY snapshot_date;


CREATE TABLE IF NOT EXISTS ods_schedule (
  schedule_id                   INT64,
  agent_id                      INT64,
  shift_id                      INT64,
  shift_code                    STRING,
  start_ts                      TIMESTAMP,
  end_ts                        TIMESTAMP,
  paid_minutes                  INT64,
  activity_code                 STRING,
  site_code                     STRING,
  sched_date                    DATE
)
PARTITION BY sched_date;


CREATE TABLE IF NOT EXISTS ods_adherence_event (
  adherence_event_id            INT64,
  agent_id                      INT64,
  schedule_id                   INT64,
  exception_type                STRING,
  start_ts                      TIMESTAMP,
  end_ts                        TIMESTAMP,
  exception_minutes             INT64,
  approved_flag                 BOOL,
  event_date                    DATE
)
PARTITION BY event_date;


CREATE TABLE IF NOT EXISTS ods_call (
  call_id                       INT64,
  queue_id                      INT64,
  agent_id                      INT64,
  program_id                    INT64,
  direction                     STRING,
  start_ts                      TIMESTAMP,
  answer_ts                     TIMESTAMP,
  end_ts                        TIMESTAMP,
  ring_seconds                  INT64,
  talk_seconds                  INT64,
  hold_seconds                  INT64,
  acw_seconds                   INT64,
  abandoned_flag                BOOL,
  disposition_code              STRING,
  recording_id                  STRING,
  call_date                     DATE
)
PARTITION BY call_date;


CREATE TABLE IF NOT EXISTS ods_ivr_session (
  session_ref                   STRING,
  client_code                   STRING,
  first_event_ts                TIMESTAMP,
  last_event_ts                 TIMESTAMP,
  menu_path_full                STRING,
  hops                          INT64,
  contained_flag                BOOL,
  exit_key                      STRING,
  event_date                    DATE
)
PARTITION BY event_date;


CREATE TABLE IF NOT EXISTS ods_chat_session (
  chat_ref                      STRING,
  client_code                   STRING,
  queue_code                    STRING,
  agent_email                   STRING,
  started_ts                    TIMESTAMP,
  ended_ts                      TIMESTAMP,
  message_count                 INT64,
  agent_message_count           INT64,
  customer_message_count        INT64,
  first_response_seconds        INT64,
  event_date                    DATE
)
PARTITION BY event_date;


CREATE TABLE IF NOT EXISTS ods_email_interaction (
  email_ref                     STRING,
  client_code                   STRING,
  mailbox                       STRING,
  agent_email                   STRING,
  received_ts                   TIMESTAMP,
  first_reply_ts                TIMESTAMP,
  resolved_ts                   TIMESTAMP,
  reply_sla_minutes             INT64,
  subject_category              STRING,
  event_date                    DATE
)
PARTITION BY event_date;


CREATE TABLE IF NOT EXISTS ods_survey_response (
  survey_id                     STRING,
  client_code                   STRING,
  interaction_ref               STRING,
  survey_ts                     TIMESTAMP,
  csat_score                    INT64,
  nps_score                     INT64,
  fcr_claimed                   BOOL,
  verbatim                      STRING,
  event_date                    DATE
)
PARTITION BY event_date;


CREATE TABLE IF NOT EXISTS ods_qa_evaluation (
  qa_form_id                    STRING,
  client_code                   STRING,
  interaction_ref               STRING,
  evaluator_email               STRING,
  evaluated_ts                  TIMESTAMP,
  form_version                  STRING,
  section_count                 INT64,
  scored_points                 INT64,
  max_points                    INT64,
  auto_fail                     BOOL,
  overall_pct                   NUMERIC(5,2),
  event_date                    DATE
)
PARTITION BY event_date;


CREATE TABLE IF NOT EXISTS ods_interaction (
  interaction_id                STRING,
  channel                       STRING,
  client_code                   STRING,
  program_id                    INT64,
  queue_id                      INT64,
  agent_id                      INT64,
  customer_ref                  STRING,
  start_ts                      TIMESTAMP,
  end_ts                        TIMESTAMP,
  handle_seconds                INT64,
  resolved_flag                 BOOL,
  source_system                 STRING,
  event_date                    DATE
)
PARTITION BY event_date;


CREATE TABLE IF NOT EXISTS ods_dialer_attempt (
  attempt_id                    STRING,
  client_code                   STRING,
  campaign_code                 STRING,
  agent_id                      INT64,
  attempt_ts                    TIMESTAMP,
  result_code                   STRING,
  connected_flag                BOOL,
  talk_seconds                  INT64,
  event_date                    DATE
)
PARTITION BY event_date;


-- === Delta-merge (8) ===

CREATE TABLE IF NOT EXISTS ods_timesheet (
  timesheet_id                  INT64,
  agent_id                      INT64,
  work_date                     STRING,
  program_id                    INT64,
  billable_minutes              INT64,
  nonbillable_minutes           INT64,
  approved_flag                 BOOL,
  last_change_ts                TIMESTAMP,
  work_month                    DATE
)
PARTITION BY work_month;


CREATE TABLE IF NOT EXISTS ods_payroll_adjustment (
  adjustment_id                 INT64,
  agent_id                      INT64,
  adj_type                      STRING,
  amount                        NUMERIC(12,2),
  last_change_ts                TIMESTAMP,
  period_month                  DATE
)
PARTITION BY period_month;


CREATE TABLE IF NOT EXISTS ods_sla_credit (
  sla_credit_id                 INT64,
  program_id                    INT64,
  sla_target_id                 INT64,
  credit_amount                 NUMERIC(12,2),
  reason                        STRING,
  last_change_ts                TIMESTAMP,
  period_month                  DATE
)
PARTITION BY period_month;


CREATE TABLE IF NOT EXISTS ods_callback_request (
  callback_id                   INT64,
  call_id                       INT64,
  queue_id                      INT64,
  requested_ts                  TIMESTAMP,
  scheduled_ts                  TIMESTAMP,
  completed_flag                BOOL,
  last_change_ts                TIMESTAMP,
  event_date                    DATE
)
PARTITION BY event_date;


CREATE TABLE IF NOT EXISTS ods_shift_swap (
  swap_id                       INT64,
  requesting_agent_id           INT64,
  accepting_agent_id            INT64,
  schedule_id                   INT64,
  swap_date                     STRING,
  status                        STRING,
  last_change_ts                TIMESTAMP,
  swap_month                    DATE
)
PARTITION BY swap_month;


CREATE TABLE IF NOT EXISTS ods_ticket_worklog (
  worklog_id                    INT64,
  ticket_id                     INT64,
  agent_id                      INT64,
  minutes_logged                INT64,
  log_ts                        TIMESTAMP,
  note                          STRING,
  last_change_ts                TIMESTAMP,
  event_date                    DATE
)
PARTITION BY event_date;


CREATE TABLE IF NOT EXISTS ods_attrition_event (
  attrition_event_id            INT64,
  agent_id                      INT64,
  notice_ts                     TIMESTAMP,
  last_day                      STRING,
  attrition_type                STRING,
  reason_code                   STRING,
  regrettable_flag              BOOL,
  last_change_ts                TIMESTAMP,
  event_month                   DATE
)
PARTITION BY event_month;


CREATE TABLE IF NOT EXISTS ods_rate_card (
  rate_card_id                  INT64,
  program_id                    INT64,
  service_code                  STRING,
  rate                          NUMERIC(12,4),
  currency                      STRING,
  effective_ts                  TIMESTAMP,
  expiry_ts                     TIMESTAMP,
  last_change_ts                TIMESTAMP,
  snapshot_date                 DATE
)
PARTITION BY snapshot_date;


-- === SCD-2 (3) ===

CREATE TABLE IF NOT EXISTS ods_agent_scd2 (
  agent_history_id              STRING,
  agent_id                      INT64,
  employee_no                   STRING,
  org_unit_id                   INT64,
  job_grade                     STRING,
  employment_type               STRING,
  status                        STRING,
  eff_from_ts                   TIMESTAMP,
  eff_to_ts                     TIMESTAMP,
  is_current                    BOOL,
  eff_from_year                 INT64,
  eff_from_date                 DATE
)
PARTITION BY eff_from_date;


CREATE TABLE IF NOT EXISTS ods_agent_skill_scd2 (
  agent_skill_history_id        STRING,
  agent_id                      INT64,
  skill_id                      INT64,
  skill_code                    STRING,
  proficiency                   INT64,
  certified                     BOOL,
  eff_from_ts                   TIMESTAMP,
  eff_to_ts                     TIMESTAMP,
  is_current                    BOOL,
  eff_from_year                 INT64,
  eff_from_date                 DATE
)
PARTITION BY eff_from_date;


CREATE TABLE IF NOT EXISTS ods_agent_assignment_scd2 (
  assignment_history_id         STRING,
  agent_id                      INT64,
  program_id                    INT64,
  queue_id                      INT64,
  role_on_program               STRING,
  eff_from_ts                   TIMESTAMP,
  eff_to_ts                     TIMESTAMP,
  is_current                    BOOL,
  eff_from_year                 INT64,
  eff_from_date                 DATE
)
PARTITION BY eff_from_date;


-- === ACID-origin (4) ===

CREATE TABLE IF NOT EXISTS ods_client_acid (
  client_id                     INT64,
  client_code                   STRING,
  client_name                   STRING,
  industry                      STRING,
  hq_country                    STRING,
  status                        STRING,
  created_ts                    TIMESTAMP,
  updated_ts                    TIMESTAMP
)
CLUSTER BY client_id;


CREATE TABLE IF NOT EXISTS ods_agent_acid (
  agent_id                      INT64,
  employee_no                   STRING,
  full_name                     STRING,
  email                         STRING,
  org_unit_id                   INT64,
  job_grade                     STRING,
  employment_type               STRING,
  hire_ts                       TIMESTAMP,
  term_ts                       TIMESTAMP,
  status                        STRING
)
CLUSTER BY agent_id;


CREATE TABLE IF NOT EXISTS ods_ticket_acid (
  ticket_id                     INT64,
  ticket_no                     STRING,
  program_id                    INT64,
  category_id                   INT64,
  assigned_agent_id             INT64,
  priority                      STRING,
  status                        STRING,
  created_ts                    TIMESTAMP,
  updated_ts                    TIMESTAMP,
  resolved_ts                   TIMESTAMP
)
CLUSTER BY ticket_id;


CREATE TABLE IF NOT EXISTS ods_invoice_acid (
  invoice_id                    INT64,
  invoice_no                    STRING,
  client_id                     INT64,
  program_id                    INT64,
  period_month                  STRING,
  issued_ts                     TIMESTAMP,
  due_ts                        TIMESTAMP,
  currency                      STRING,
  total_amount                  NUMERIC(14,2),
  status                        STRING
)
CLUSTER BY invoice_id;

