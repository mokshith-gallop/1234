# BigQuery Physical Target Schema — DDL & Validation Artifacts

## Overview

Complete BigQuery DDL for migrating the NBCS legacy warehouse (100 Hive tables + 15 views) to BigQuery, plus MVS validation specs that verify every aspect declaratively via the harness.

**Source of truth**: `/workspace/source/manifests/tables.yaml`
**Generator**: `tools/gen_bq_ddl.py` (DDL), `tools/gen_mvs_specs.py` (MVS specs)

## File Layout

### BigQuery DDL (`ddl/`)

| File | Dataset | Objects | Description |
|------|---------|---------|-------------|
| `01-datasets.sql` | all | 4 schemas | `CREATE SCHEMA` — manual/Terraform only, NOT a harness DDL step |
| `02-staging.sql` | staging | 45 tables | 27 sqoop + 8 delta + 10 file feeds |
| `03-ods.sql` | ods | 30 tables | 15 cleanse + 8 delta-merge + 3 SCD-2 + 4 ACID-origin |
| `04-dm-tables.sql` | dm | 25 tables | 9 dims + 9 facts + 7 aggs |
| `05-dm-views.sql` | dm | 15 views | Hand-translated from Hive/Impala |
| `06-etl-control.sql` | _etl_control | 3 tables | watermarks, dq_results, job_audit |

### Legacy Hive DDL for source_setup (`sql/legacy/`)

| File | Objects | Purpose |
|------|---------|---------|
| `01-create-databases.hql` | 3 databases | LOCATIONs rehosted to `${SOURCE_WAREHOUSE}` |
| `02-staging.hql` | 45 tables | All original types/COMMENTs/SERDE preserved |
| `03-ods.hql` | 30 tables | ACID tables de-transactioned for readability |
| `04-dm-tables.hql` | 25 tables | No views (can't create recursive-CTE in Hive) |

### MVS Validation Specs (`tests/schema/`)

| File | Pattern | Covers |
|------|---------|--------|
| `staging_conformance.mvs.yaml` | schema_conformance | 45 staging tables, all columns |
| `ods_conformance.mvs.yaml` | schema_conformance | 30 ODS tables, all columns |
| `dm_conformance.mvs.yaml` | schema_conformance | 25 DM tables + 15 views |
| `etl_control_conformance.mvs.yaml` | schema_conformance | 3 _etl_control tables |
| `queryability_and_perf.mvs.yaml` | query_performance | 122 smoke queries + 3 perf A/B |

## Scalar Type Mapping (Hive → BigQuery)

| Hive Type | BigQuery Type | Column Count |
|-----------|---------------|-------------|
| BIGINT | INT64 | ~180 |
| INT / SMALLINT / TINYINT | INT64 | ~60 |
| STRING | STRING | ~200 |
| DOUBLE | FLOAT64 | 2 |
| BOOLEAN | BOOL | ~30 |
| TIMESTAMP | TIMESTAMP | ~60 |
| DECIMAL(p,s) | NUMERIC(p,s) | 52 |

## DECIMAL Precision Mapping (52 Columns, 7 Distinct Pairs)

| Hive DECIMAL | BigQuery NUMERIC | Tables / Columns |
|-------------|------------------|-----------------|
| DECIMAL(14,2) | NUMERIC(14,2) | stg_fin_invoice.total_amount, stg_fin_invoice_line.line_amount, ods_invoice_acid.total_amount, fact_billing_line.line_amount, agg_program_monthly.billed_amount, agg_billing_monthly.billed_amount, agg_billing_monthly.net_revenue |
| DECIMAL(12,4) | NUMERIC(12,4) | stg_crm_contract_line.unit_rate, stg_fin_rate_card.rate, stg_fin_rate_card_change_delta.old_rate/new_rate, ods_contract_line.unit_rate, ods_rate_card.rate, fact_billing_line.unit_rate |
| DECIMAL(12,2) | NUMERIC(12,2) | stg_crm_contract_line.min_commit, stg_fin_payroll_adj_delta.amount, stg_crm_sla_credit_delta.credit_amount, stg_file_telco_invoice.charge_amount, ods_payroll_adjustment.amount, ods_sla_credit.credit_amount, ods_contract_line.min_commit, fact_billing_line.qty, agg_billing_monthly.sla_credit_amount/telco_cost_amount |
| DECIMAL(10,4) | NUMERIC(10,4) | stg_crm_sla_target.target_value |
| DECIMAL(8,2) | NUMERIC(8,2) | stg_wfm_forecast.required_fte, fact_queue_interval.avg_speed_answer_sec/avg_handle_sec, agg_agent_daily.avg_handle_seconds, agg_agent_weekly.avg_handle_seconds, agg_program_monthly.avg_handle_seconds, agg_queue_hourly.volume_variance_pct → no, that's (7,2); agg_site_daily.avg_handle_seconds |
| DECIMAL(5,2) | NUMERIC(5,2) | stg_crm_sla_target.penalty_pct, stg_file_qa_forms.overall_pct, ods_qa_evaluation.overall_pct, fact_qa_evaluation.overall_pct, fact_adherence_daily.adherence_pct/occupancy_pct, agg_agent_daily.adherence_pct/occupancy_pct, agg_agent_weekly.adherence_pct/occupancy_pct, agg_program_monthly.avg_csat, agg_queue_hourly.sl_pct, agg_csat_rollup_monthly.avg_csat/pct_promoters/pct_detractors, agg_site_daily.sl_pct/adherence_pct |
| DECIMAL(7,2) | NUMERIC(7,2) | agg_queue_hourly.volume_variance_pct |

## Complex Type Mapping (4 Columns)

| Table | Column | Hive Type | BigQuery Type |
|-------|--------|-----------|---------------|
| stg_file_qa_forms | sections | `ARRAY<STRUCT<section_code:STRING,max_points:INT,scored_points:INT>>` | `ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>` |
| stg_file_chat_transcripts | messages | `ARRAY<STRUCT<sender:STRING,ts_ms:BIGINT,text:STRING>>` | `ARRAY<STRUCT<sender STRING, ts_ms INT64, text STRING>>` |
| stg_file_chat_transcripts | metadata | `MAP<STRING,STRING>` | `ARRAY<STRUCT<key STRING, value STRING>>` |
| stg_file_speech_analytics | keywords | `ARRAY<STRING>` | `ARRAY<STRING>` (REPEATED STRING) |

## Partition Conversion Rules

### Staging (45 tables)

| Group | Source Partition | Target | Example |
|-------|----------------|--------|---------|
| Sqoop (27) | `load_date STRING` | `PARTITION BY load_date` as `DATE` | stg_crm_client |
| Sqoop (exception) | `(load_date STRING, site_code STRING)` | `PARTITION BY load_date DATE`, `CLUSTER BY (site_code)` | stg_wfm_schedule |
| Sqoop (bucketed) | `CLUSTERED BY (call_id) INTO 16 BUCKETS` | `CLUSTER BY (call_id)` (no BUCKETS) | stg_tel_call |
| Delta (8) | `extract_ts STRING` | `PARTITION BY extract_ts` as `DATE` | stg_fin_timesheet_delta |
| File feed (10) | `(client_code STRING, feed_date STRING)` | `PARTITION BY feed_date DATE`, `CLUSTER BY (client_code)` | stg_file_qa_forms |

All 45 staging tables: `OPTIONS(partition_expiration_days=90)` — 90-day TTL.

### ODS (30 tables)

| Group | Source Partition | Target |
|-------|----------------|--------|
| Cleanse (15) | `snapshot_date/sched_date/event_date/call_date STRING` | Same name as `DATE` |
| Delta-merge (8) | `work_month/period_month/event_date/swap_month/event_month/snapshot_date STRING` | Same name as `DATE` |
| SCD-2 (3) | `eff_from_year INT` | Add `eff_from_date DATE`, `PARTITION BY eff_from_date`. Keep `eff_from_year INT64` as regular column |
| ACID (4) | `CLUSTERED BY (id) INTO N BUCKETS` | `CLUSTER BY (id)` — no partition, no BUCKETS |

### DM (25 tables)

| Group | Source Partition | Target |
|-------|----------------|--------|
| Dims (9) | None | No partition, no cluster |
| Facts (9) | `date_key INT` or `(date_key INT, channel STRING)` or `period_month STRING` | Add `partition_date DATE`, `PARTITION BY partition_date`. Original partition cols kept as regular columns |
| Aggs (7) | `date_key INT` or `week_start_key INT` or `period_month STRING` | Add `partition_date DATE`, `PARTITION BY partition_date` |

Special: `fact_interaction` — `CLUSTER BY (channel, agent_sk)` from collapsed multi-col partition + bucketing.

## View Translations (15 views in `05-dm-views.sql`)

| # | View | Hive/Impala Construct | BigQuery Translation |
|---|------|----------------------|---------------------|
| 1 | vw_org_hierarchy | `WITH RECURSIVE` | Supported natively (no change) |
| 2 | vw_active_agents_ndv | `NDV()` | `APPROX_COUNT_DISTINCT()` |
| 3 | vw_csat_rollup | `GROUP BY ... WITH ROLLUP` + `GROUPING__ID` | `GROUP BY ROLLUP(...)` + `GROUPING()` |
| 4 | vw_call_driver_regex | `RLIKE`, `regexp_extract` | `REGEXP_CONTAINS()`, `REGEXP_EXTRACT()` with RE2 |
| 5 | vw_repeat_contact_window | `unix_timestamp(ts)` | `UNIX_SECONDS(ts)` |
| 6 | vw_billing_reconciliation | `from_unixtime(CAST(x/1000 AS BIGINT))` | `TIMESTAMP_MILLIS(x)` — preserves lie-millis handling |
| 7 | vw_agent_roster_current | ROW_NUMBER SCD-2 slice | No change needed |
| 8 | vw_agent_scorecard | PERCENT_RANK + NTILE | No change needed |
| 9 | vw_attrition_risk | Nested CTEs + NTILE | No change needed |
| 10 | vw_queue_sla_attainment | Layer-skip (reads staging) | Preserved: still reads `staging.stg_crm_sla_target` |
| 11 | vw_first_contact_resolution | `date_add(f.end_ts, 7)` | `TIMESTAMP_ADD(f.end_ts, INTERVAL 7 DAY)` |
| 12 | vw_occupancy_utilization | Pivot of state seconds | No change needed |
| 13 | vw_shrinkage_analysis | `from_unixtime(unix_timestamp(...))` | `PARSE_DATE('%Y%m%d', CAST(... AS STRING))` |
| 14 | vw_program_margin | `ON 1 = 1` cross join | `CROSS JOIN` |
| 15 | vw_client_executive_summary | Wide multi-fact join | No change needed |

## _etl_control Tables (New — Not in Source)

| Table | Purpose | Partition |
|-------|---------|-----------|
| `watermarks` | Incremental ingestion state | None |
| `dq_results` | DQ check outcomes | `PARTITION BY run_date` |
| `job_audit` | Pipeline execution log | `PARTITION BY run_date` |

## Epoch Column Policy

| Layer | Encoding | BigQuery Type | Description |
|-------|----------|---------------|-------------|
| Staging | epoch SECONDS | INT64 | `OPTIONS(description="epoch SECONDS (legacy)")` |
| Staging | epoch MILLISECONDS | INT64 | `OPTIONS(description="epoch MILLISECONDS (legacy)")` |
| Staging | Oracle string | STRING | `OPTIONS(description="Oracle string YYYYMMDDHH24MISS (legacy)")` |
| Staging | lie-millis | INT64 | `OPTIONS(description="!! name says seconds, VALUES ARE MILLIS !!")` |
| ODS/DM | Cleansed | TIMESTAMP | Native TIMESTAMP (no description needed) |

The `stg_fin_invoice.issued_ts_sec` and `stg_fin_invoice.due_ts_sec` columns are the lie-millis trap: the column names suggest seconds, but values are actually milliseconds.

## Acceptance Criteria → Artifact Mapping

| AC | What | Proven By |
|----|------|-----------|
| AC1 | 100 tables + 15 views CREATE with 0 errors | `expect_table_count` in schema_conformance (45+30+40+3) |
| AC2 | Per-column fidelity | Every column declared with `type`, `scale`, `source_type` for live Hive cross-check |
| AC3 | Object-type fidelity | `expect_object_type: TABLE` or `VIEW` on every object |
| AC4 | Partition + cluster intent | `partition_by` + `cluster_by` on every partitioned/clustered table |
| AC5 | FK↔PK type consistency | Matching `type: INT64` on FK/PK columns across all specs |
| AC6 | Queryability smoke | 122 `SELECT * LIMIT 0` queries + 4 tier queries in `queryability_and_perf.mvs.yaml` |
| AC7 | Integrity guards | Harness fail-fast: missing table/column → FAIL |
| AC8 | No-silent-skip | 100/100 tables + 15/15 views + 3 _etl_control exhaustively declared |
| AC9 | 90-day partition expiration | `table_options.partition_expiration_days: 90` on all 45 staging tables |
| AC10 | Physical-access performance | 3 A/B dry-run compare queries in `queryability_and_perf.mvs.yaml` |

## Running Validation

```bash
# Generate DDL from manifest
python3 tools/gen_bq_ddl.py

# Generate MVS specs from manifest
python3 tools/gen_mvs_specs.py

# Cross-validate all artifacts
python3 tools/validate_all.py

# Run via harness (requires BigQuery + Hive connections)
dmtemplate run tests/schema/staging_conformance.mvs.yaml
dmtemplate run tests/schema/ods_conformance.mvs.yaml
dmtemplate run tests/schema/dm_conformance.mvs.yaml
dmtemplate run tests/schema/etl_control_conformance.mvs.yaml
dmtemplate run tests/schema/queryability_and_perf.mvs.yaml
```
