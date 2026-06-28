# Implementation Approach

## Implementation Approach — BigQuery DDL Generation + MVS Validation Specs

### Source of Truth
Generate all BigQuery DDL programmatically from `manifests/tables.yaml` — the same manifest that drives the Hive DDL via `tools/gen_artifacts.py`. This guarantees 1:1 table/column parity between source and target.

### DDL File Structure
Output into `/workspace/project/ddl/` organized per dataset:

| File | Dataset | Contents |
|------|---------|----------|
| `01-datasets.sql` | all | `CREATE SCHEMA IF NOT EXISTS` for staging, ods, dm, _etl_control |
| `02-staging.sql` | staging | 45 tables (27 sqoop + 8 delta + 10 file feeds) |
| `03-ods.sql` | ods | 30 tables (15 cleanse + 8 delta-merge + 3 SCD-2 + 4 ACID-origin) |
| `04-dm-tables.sql` | dm | 25 tables (9 dims + 9 facts + 7 aggs) |
| `05-dm-views.sql` | dm | 15 views (hand-translated SQL, not generated) |
| `06-etl-control.sql` | _etl_control | 3 operational tables (watermarks, dq_results, job_audit) |

### Generation Rules Applied
For each table in `tables.yaml`, the generator:

1. **Maps types** per the locked Schema and Data Type Mapping decision (BIGINT→INT64, DECIMAL(p,s)→NUMERIC(p,s), MAP→ARRAY of STRUCT, etc.)
2. **Converts partitions** per the locked Performance Optimization decision (STRING→DATE, multi-col→single+cluster, INT date_key→`_partition_date DATE`)
3. **Converts bucketing** to `CLUSTER BY` (drops `INTO n BUCKETS`)
4. **Preserves column COMMENTs** as BigQuery `OPTIONS(description=...)` — especially epoch encoding comments and the lie-millis warning on `stg_fin_invoice.issued_ts_sec`/`due_ts_sec`
5. **Strips Hive directives** — `STORED AS`, `SERDE`, `LOCATION`, `TBLPROPERTIES`, `ROW FORMAT`
6. **Adds `_partition_date DATE`** synthetic column to all DM fact/agg tables partitioned by integer `date_key` or `week_start_key`
7. **Adds `eff_from_date DATE`** synthetic column to SCD-2 tables, keeps `eff_from_year INT` as regular column
8. **Sets 90-day partition expiration** on all 45 staging tables via `OPTIONS(partition_expiration_days=90)`

### _etl_control Tables (New — Not in Source)
These are new operational tables required by the target architecture:

**watermarks** — tracks incremental ingestion state (replaces filesystem watermark files):
```sql
CREATE TABLE _etl_control.watermarks (
  source_table    STRING NOT NULL,
  last_value      STRING,
  last_run_ts     TIMESTAMP,
  run_id          STRING
);
```

**dq_results** — DQ check outcomes (per locked DQ decision):
```sql
CREATE TABLE _etl_control.dq_results (
  run_id        STRING,
  run_date      DATE,
  check_name    STRING,
  target_table  STRING,
  status        STRING,
  metric_value  FLOAT64,
  threshold     FLOAT64,
  detail_json   JSON,
  checked_at    TIMESTAMP
) PARTITION BY run_date;
```

**job_audit** — pipeline execution log:
```sql
CREATE TABLE _etl_control.job_audit (
  job_id        STRING NOT NULL,
  dag_id        STRING,
  task_id       STRING,
  run_date      DATE,
  start_ts      TIMESTAMP,
  end_ts        TIMESTAMP,
  status        STRING,
  rows_affected INT64,
  error_message STRING
) PARTITION BY run_date;
```

### View Translation (05-dm-views.sql)
The 15 views are **hand-translated** (not auto-generated) because they contain deliberate SQL traps requiring dialect-specific changes:

| View | Key Translation |
|------|----------------|
| `vw_org_hierarchy` | `WITH RECURSIVE` → BigQuery supports natively, no change |
| `vw_active_agents_ndv` | `NDV()` → `APPROX_COUNT_DISTINCT()` |
| `vw_csat_rollup` | `GROUP BY ... WITH ROLLUP` + `GROUPING__ID` → `GROUP BY ROLLUP(...)` + `GROUPING()` |
| `vw_call_driver_regex` | `RLIKE` → `REGEXP_CONTAINS()`, `regexp_extract` group syntax adjusted for RE2 |
| `vw_repeat_contact_window` | `unix_timestamp(ts)` → `UNIX_SECONDS(ts)` |
| `vw_billing_reconciliation` | `from_unixtime(CAST(issued_ts_sec/1000 AS BIGINT))` → `TIMESTAMP_MILLIS(issued_ts_sec)` (preserves lie-millis handling) |
| `vw_queue_sla_attainment` | Layer-skip preserved: view still reads `staging.stg_crm_sla_target` for parity |
| `vw_first_contact_resolution` | `date_add(f.end_ts, 7)` → `DATE_ADD(f.end_ts, INTERVAL 7 DAY)` |
| `vw_shrinkage_analysis` | `from_unixtime(unix_timestamp(...))` date formatting → `PARSE_DATE` |
| `vw_program_margin` | `ON 1 = 1` → `CROSS JOIN` |

### Integration with Test Harness
The generated DDL is validated using the existing `schema_conformance` pattern in `lib/schema.py`. The execution agent produces MVS YAML specs (not Python) that declare the expected schema, and the harness introspects the live BigQuery catalog to assert correctness.
