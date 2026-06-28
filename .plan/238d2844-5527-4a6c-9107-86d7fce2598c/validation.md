# Validation

## Validation — Schema Conformance via MVS Specs Against Live BigQuery

### Harness Pattern
Use the existing `schema_conformance` pattern (`lib/schema.py`) which introspects `INFORMATION_SCHEMA.TABLES`, `INFORMATION_SCHEMA.COLUMNS`, and `TABLE_OPTIONS` on the live BigQuery catalog after DDL is applied. The source side reads from the live Hive catalog (not parsed DDL files).

### MVS Spec Structure
Generate **4 MVS YAML suite files** — one per dataset — each declaring every table's expected columns, types, partition, cluster, table options, and descriptions:

| MVS File | Dataset | Tables | Views |
|----------|---------|--------|-------|
| `tests/schema/staging_conformance.mvs.yaml` | staging | 45 | 0 |
| `tests/schema/ods_conformance.mvs.yaml` | ods | 30 | 0 |
| `tests/schema/dm_conformance.mvs.yaml` | dm | 25 | 15 |
| `tests/schema/etl_control_conformance.mvs.yaml` | _etl_control | 3 | 0 |

### AC-to-Assertion Mapping

**AC 1 — Table count with 0 DDL errors:**
- Each MVS suite declares `expect_table_count` (45, 30, 25, 3).
- Harness asserts every `CREATE TABLE`/`CREATE VIEW` landed in `INFORMATION_SCHEMA.TABLES`. Any missing object is a HARD FAIL naming the object.

**AC 2 — Per-column fidelity:**
- Every column of every table is declared in the MVS with `name`, `type`, `scale` (for NUMERIC), `description` (for epoch/lie-millis columns), and `source_type`/`source_name` for cross-engine mapping validation.
- Harness uses `normalize_type()` to compare across dialects (BIGINT→INT64, DECIMAL→NUMERIC, BOOLEAN→BOOL).
- Complex types declared with full nested signature: e.g. `type: "ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>"` — harness uses `type_signature()` for recursive comparison.
- Nullability checked via `nullable: true/false` where source has explicit nullable constraints.

**AC 3 — Object-type fidelity:**
- Tables declare `expect_object_type: TABLE`; views declare `expect_object_type: VIEW`.
- Harness reads `table_type` from `INFORMATION_SCHEMA.TABLES` and normalizes EXTERNAL/SNAPSHOT/CLONE → TABLE.

**AC 4 — Partition + cluster + key intent:**
- Each table entry includes `partition_by` (the column name) and `cluster_by` (ordered list of column names).
- Unpartitioned dims omit both fields.
- Harness reads partition and clustering metadata from the BigQuery table object and asserts exact match.

**AC 5 — Cross-dataset FK→PK type consistency:**
- Handled by declaring matching `type: INT64` on both sides of every FK→PK join path.
- The harness validates each column's type independently; if a staging FK is INT64 and the corresponding dim PK is also INT64, both pass — any divergence fails one side.
- Additionally, a dedicated FK consistency check suite can be added that reads both sides and asserts type equality.

**AC 6 — Queryability smoke:**
- A separate MVS suite using a `query_smoke` pattern (or a Python test) executes:
  - `SELECT * FROM <table> LIMIT 0` for all 115 objects
  - 4 representative tier queries (staging GROUP BY, ods GROUP BY, dm JOIN with PARSE_DATE, _etl_control SELECT)
- Validates that nested ARRAY/STRUCT types and the MAP→ARRAY<STRUCT<key,value>> conversion are queryable without type-coercion errors.

**AC 7 — Integrity guards:**
- The harness's fail-fast behavior ensures: a table present in DDL but absent from INFORMATION_SCHEMA → FAIL (never silent skip).
- A column declared in the MVS but missing from the catalog → FAIL (never "no violation found").
- The `_cross_check_source()` function in `lib/schema.py` fails if a source column is unreachable, not silently passes.

**AC 8 — No-silent-skip:**
- The MVS spec declares every table and every column. The harness counts checked objects.
- Final assertion: checked 100/100 tables + 15/15 views, C/C total columns — coverage printed in the report.
- Source side reads from live Hive metastore (via `source_database` + `source_table` in the MVS spec).

**AC 9 — Partition-expiration policies:**
- All 45 staging table entries include `table_options: { partition_expiration_days: 90 }`.
- Harness `_check_table_options()` reads `partition_expiration_days` from `get_table()` options and asserts it equals 90.

**AC 10 — Physical-access performance:**
- A separate `perf` pattern test (using `lib/perf.py`) runs:
  - `fact_interaction`: unfiltered `SELECT COUNT(*)` vs partition+cluster-filtered `WHERE _partition_date = @date AND channel = 'VOICE'` — assert filtered `totalBytesProcessed` is less.
  - `stg_tel_call`: cluster-filtered `WHERE call_id = @id` vs unfiltered — assert byte reduction.
  - `agg_agent_daily`: partition-filtered `WHERE _partition_date = @date` vs unfiltered — assert byte reduction.
- Metrics read from BigQuery job metadata, not estimated.
- This test requires seed data to be loaded first (it runs on the scratch project with fixture data).

### Source Cross-Check Configuration
Each MVS suite sets:
```yaml
connections:
  source: { engine: impala }
  target: { engine: bigquery }
```
And `source_database` points to the Hive database, so `_cross_check_source()` introspects the legacy table and validates that the declared `source_type` matches the actual Hive column type. This ensures the type mapping is not just internally consistent but externally validated against the real source catalog.

### Coverage Guarantee
The MVS specs are generated from the same `manifests/tables.yaml` that produces the DDL. This ensures:
- Every table in the DDL is validated (no table skipped)
- Every column in every table is declared and checked
- Source cross-check covers all 100 tables (not just a sample)
