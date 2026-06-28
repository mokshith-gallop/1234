-- ----------------------------------------------------------------------------
-- 04-dm-tables: 25 DM tables (9 dims + 9 facts + 7 aggregates)
-- Adapted for source_setup. Views NOT included (recursive CTE views
-- cannot be created in Hive — cross-check view shapes via expect_object_type).
-- No LOCATION on managed tables (harness manages paths).
-- ----------------------------------------------------------------------------

-- === Dimensions (9) ===

CREATE TABLE IF NOT EXISTS dm.dim_date (
  date_key                    INT,
  full_date                   STRING,
  day_of_week                 INT,
  day_name                    STRING,
  week_of_year                INT,
  month_no                    INT,
  month_name                  STRING,
  quarter_no                  INT,
  year_no                     INT,
  is_weekend                  BOOLEAN,
  is_holiday_us               BOOLEAN,
  fiscal_period               STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.dim_agent (
  agent_sk                    BIGINT,
  agent_id                    BIGINT,
  employee_no                 STRING,
  full_name                   STRING,
  job_grade                   STRING,
  employment_type             STRING,
  org_unit_id                 BIGINT,
  team_name                   STRING,
  site_code                   STRING,
  status                      STRING,
  hire_date_key               INT,
  is_current                  BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.dim_client (
  client_sk                   BIGINT,
  client_id                   BIGINT,
  client_code                 STRING,
  client_name                 STRING,
  industry                    STRING,
  hq_country                  STRING,
  primary_contact_name        STRING,
  primary_contact_email       STRING,
  status                      STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.dim_program (
  program_sk                  BIGINT,
  program_id                  BIGINT,
  program_code                STRING,
  program_name                STRING,
  client_id                   BIGINT,
  line_of_business            STRING,
  channel_mix                 STRING,
  site_code                   STRING,
  billing_model               STRING,
  status                      STRING,
  go_live_date_key            INT
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.dim_queue (
  queue_sk                    BIGINT,
  queue_id                    BIGINT,
  queue_code                  STRING,
  queue_name                  STRING,
  program_id                  BIGINT,
  media_type                  STRING,
  priority                    INT
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.dim_site (
  site_sk                     BIGINT,
  site_code                   STRING,
  site_name                   STRING,
  region                      STRING,
  country                     STRING,
  timezone                    STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.dim_shift (
  shift_sk                    BIGINT,
  shift_id                    BIGINT,
  shift_code                  STRING,
  shift_name                  STRING,
  start_hhmm                  STRING,
  end_hhmm                    STRING,
  overnight_flag              BOOLEAN,
  site_code                   STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.dim_org (
  org_sk                      BIGINT,
  org_unit_id                 BIGINT,
  unit_code                   STRING,
  unit_name                   STRING,
  unit_type                   STRING,
  level1_name                 STRING,
  level2_name                 STRING,
  level3_name                 STRING,
  level4_name                 STRING,
  site_code                   STRING,
  cost_center                 STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.dim_disposition (
  disposition_sk              BIGINT,
  disposition_code            STRING,
  disposition_desc            STRING,
  category                    STRING,
  billable_flag               BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- === Facts (9) ===

CREATE TABLE IF NOT EXISTS dm.fact_interaction (
  interaction_id              STRING,
  client_sk                   BIGINT,
  program_sk                  BIGINT,
  queue_sk                    BIGINT,
  agent_sk                    BIGINT,
  customer_ref                STRING,
  start_ts                    TIMESTAMP,
  end_ts                      TIMESTAMP,
  handle_seconds              INT,
  resolved_flag               BOOLEAN,
  source_system               STRING
)
PARTITIONED BY (date_key INT, channel STRING)
CLUSTERED BY (agent_sk) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.fact_agent_activity (
  agent_sk                    BIGINT,
  state_code                  STRING,
  state_seconds               BIGINT,
  occurrence_count            INT,
  first_state_ts              TIMESTAMP,
  last_state_ts               TIMESTAMP
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.fact_queue_interval (
  queue_sk                    BIGINT,
  interval_start_ts           TIMESTAMP,
  offered                     INT,
  answered                    INT,
  abandoned                   INT,
  answered_in_sl              INT,
  sl_threshold_sec            INT,
  avg_speed_answer_sec        DECIMAL(8,2),
  avg_handle_sec              DECIMAL(8,2)
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.fact_csat_survey (
  survey_id                   STRING,
  interaction_id              STRING,
  client_sk                   BIGINT,
  program_sk                  BIGINT,
  agent_sk                    BIGINT,
  survey_ts                   TIMESTAMP,
  csat_score                  INT,
  nps_score                   INT,
  fcr_claimed                 BOOLEAN
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.fact_qa_evaluation (
  qa_form_id                  STRING,
  interaction_id              STRING,
  agent_sk                    BIGINT,
  program_sk                  BIGINT,
  evaluated_ts                TIMESTAMP,
  scored_points               INT,
  max_points                  INT,
  overall_pct                 DECIMAL(5,2),
  auto_fail                   BOOLEAN
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.fact_billing_line (
  invoice_line_id             BIGINT,
  invoice_id                  BIGINT,
  client_sk                   BIGINT,
  program_sk                  BIGINT,
  service_code                STRING,
  qty                         DECIMAL(12,2),
  unit_rate                   DECIMAL(12,4),
  line_amount                 DECIMAL(14,2),
  adjustment_flag             BOOLEAN,
  invoice_status              STRING
)
PARTITIONED BY (period_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.fact_adherence_daily (
  agent_sk                    BIGINT,
  scheduled_minutes           INT,
  worked_minutes              INT,
  exception_minutes           INT,
  timeoff_minutes             INT,
  adherence_pct               DECIMAL(5,2),
  occupancy_pct               DECIMAL(5,2)
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.fact_ticket (
  ticket_id                   BIGINT,
  program_sk                  BIGINT,
  category_code               STRING,
  assigned_agent_sk           BIGINT,
  priority                    STRING,
  status                      STRING,
  created_ts                  TIMESTAMP,
  resolved_ts                 TIMESTAMP,
  resolution_minutes          INT,
  sla_breached_flag           BOOLEAN,
  touch_count                 INT
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.fact_ivr_path (
  session_ref                 STRING,
  client_code                 STRING,
  menu_path_full              STRING,
  hops                        INT,
  contained_flag              BOOLEAN,
  exit_key                    STRING,
  duration_seconds            INT
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- === Aggregates (7) ===

CREATE TABLE IF NOT EXISTS dm.agg_agent_daily (
  agent_sk                    BIGINT,
  site_code                   STRING,
  interactions_handled        INT,
  avg_handle_seconds          DECIMAL(8,2),
  talk_seconds                BIGINT,
  acw_seconds                 BIGINT,
  aux_seconds                 BIGINT,
  adherence_pct               DECIMAL(5,2),
  occupancy_pct               DECIMAL(5,2)
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.agg_agent_weekly (
  agent_sk                    BIGINT,
  site_code                   STRING,
  days_worked                 INT,
  interactions_handled        INT,
  avg_handle_seconds          DECIMAL(8,2),
  adherence_pct               DECIMAL(5,2),
  occupancy_pct               DECIMAL(5,2)
)
PARTITIONED BY (week_start_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.agg_program_monthly (
  client_sk                   BIGINT,
  program_sk                  BIGINT,
  line_of_business            STRING,
  interactions                BIGINT,
  avg_handle_seconds          DECIMAL(8,2),
  avg_csat                    DECIMAL(5,2),
  billed_amount               DECIMAL(14,2),
  grouping_level              INT
)
PARTITIONED BY (period_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.agg_queue_hourly (
  queue_sk                    BIGINT,
  hour_of_day                 INT,
  offered                     INT,
  answered                    INT,
  abandoned                   INT,
  sl_pct                      DECIMAL(5,2),
  forecast_volume             INT,
  volume_variance_pct         DECIMAL(7,2)
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.agg_csat_rollup_monthly (
  client_sk                   BIGINT,
  program_sk                  BIGINT,
  site_code                   STRING,
  surveys                     BIGINT,
  avg_csat                    DECIMAL(5,2),
  pct_promoters               DECIMAL(5,2),
  pct_detractors              DECIMAL(5,2),
  grouping_id                 INT
)
PARTITIONED BY (period_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.agg_billing_monthly (
  client_sk                   BIGINT,
  program_sk                  BIGINT,
  billed_amount               DECIMAL(14,2),
  sla_credit_amount           DECIMAL(12,2),
  telco_cost_amount           DECIMAL(12,2),
  net_revenue                 DECIMAL(14,2)
)
PARTITIONED BY (period_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dm.agg_site_daily (
  site_code                   STRING,
  agents_active               INT,
  interactions                BIGINT,
  avg_handle_seconds          DECIMAL(8,2),
  sl_pct                      DECIMAL(5,2),
  adherence_pct               DECIMAL(5,2)
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
