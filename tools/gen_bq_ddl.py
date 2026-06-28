#!/usr/bin/env python3
"""Generate BigQuery DDL from the legacy manifests/tables.yaml.

Reads the single source of truth (tables.yaml) and emits 6 BigQuery DDL files:
  01-datasets.sql        CREATE SCHEMA for staging, ods, dm, _etl_control
  02-staging.sql         45 staging tables
  03-ods.sql             30 ODS tables
  04-dm-tables.sql       25 DM tables (dims + facts + aggs)
  05-dm-views.sql        15 DM views (hand-translated)
  06-etl-control.sql     3 operational tables

Usage:
  python3 tools/gen_bq_ddl.py
"""

import re
import sys
from pathlib import Path

import yaml

SOURCE_MANIFEST = Path("/workspace/source/manifests/tables.yaml")
OUTPUT_DIR = Path("/workspace/project/ddl")

# ---------------------------------------------------------------------------
# Type mapping: Hive -> BigQuery
# ---------------------------------------------------------------------------
SCALAR_TYPE_MAP = {
    "BIGINT": "INT64",
    "INT": "INT64",
    "SMALLINT": "INT64",
    "TINYINT": "INT64",
    "STRING": "STRING",
    "FLOAT": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "BOOLEAN": "BOOL",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "BINARY": "BYTES",
}

# Partition columns whose STRING type should be converted to DATE.
# These are names that represent dates/months/timestamps in Hive STRING partitions.
DATE_PARTITION_NAMES = {
    "load_date", "extract_ts", "feed_date",
    "snapshot_date", "sched_date", "event_date", "call_date",
    "work_month", "period_month", "swap_month", "event_month",
    "snapshot_month",
}

EPOCH_TAGS = {"epoch_sec", "epoch_ms", "ora_str", "lie_ms"}


def _epoch_comment(col_tags):
    """Return BigQuery column description for epoch-encoded columns."""
    for t in col_tags:
        if t == "epoch_sec":
            return "epoch SECONDS (legacy)"
        elif t == "epoch_ms":
            return "epoch MILLISECONDS (legacy)"
        elif t == "ora_str":
            return "Oracle string YYYYMMDDHH24MISS (legacy)"
        elif t == "lie_ms":
            return "!! name says seconds, VALUES ARE MILLIS !!"
    return None


def map_type(hive_type: str) -> str:
    """Map a Hive type string to BigQuery type string."""
    upper = hive_type.strip().upper()

    # DECIMAL(p,s) -> NUMERIC(p,s)
    m = re.match(r"DECIMAL\((\d+),(\d+)\)", upper)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        return f"NUMERIC({p},{s})"

    # MAP<K,V> -> ARRAY<STRUCT<key K_bq, value V_bq>>
    m = re.match(r"MAP<(.+),\s*(.+)>$", upper)
    if m:
        k_type = map_type(m.group(1).strip())
        v_type = map_type(m.group(2).strip())
        return f"ARRAY<STRUCT<key {k_type}, value {v_type}>>"

    # ARRAY<STRUCT<...>>
    m = re.match(r"ARRAY<STRUCT<(.+)>>$", upper)
    if m:
        fields_str = m.group(1)
        bq_fields = []
        for field in _split_struct_fields(fields_str):
            parts = field.strip().split(":")
            fname = parts[0].strip().lower()
            ftype = map_type(parts[1].strip())
            bq_fields.append(f"{fname} {ftype}")
        return f"ARRAY<STRUCT<{', '.join(bq_fields)}>>"

    # ARRAY<simple>
    m = re.match(r"ARRAY<(.+)>$", upper)
    if m:
        inner = map_type(m.group(1).strip())
        return f"ARRAY<{inner}>"

    # Scalar
    if upper in SCALAR_TYPE_MAP:
        return SCALAR_TYPE_MAP[upper]

    raise ValueError(f"Unknown Hive type: {hive_type}")


def map_partition_col_type(name: str, hive_type: str) -> str:
    """Map a Hive partition column type to its BigQuery equivalent.

    STRING partition columns that represent dates/months become DATE.
    Other STRING partition columns (like client_code, channel) stay STRING.
    INT partition columns become INT64.
    """
    upper = hive_type.strip().upper()
    if upper == "STRING":
        if name in DATE_PARTITION_NAMES:
            return "DATE"
        return "STRING"
    elif upper in ("INT", "BIGINT"):
        return "INT64"
    return SCALAR_TYPE_MAP.get(upper, upper)


def _split_struct_fields(fields_str: str) -> list:
    """Split comma-separated struct fields, respecting nested angle brackets."""
    fields = []
    depth = 0
    current = []
    for ch in fields_str:
        if ch == '<':
            depth += 1
            current.append(ch)
        elif ch == '>':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            fields.append(''.join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        fields.append(''.join(current).strip())
    return fields


# ---------------------------------------------------------------------------
# Column / table parsing from manifest
# ---------------------------------------------------------------------------
def parse_column(spec):
    """Parse a column spec from tables.yaml (string or dict form)."""
    if isinstance(spec, dict):
        return {
            "name": spec["name"],
            "type": str(spec["type"]),
            "tags": list(spec.get("tags", [])),
        }
    m = re.match(r"^([a-z0-9_]+):([A-Z0-9_(),]+?)(?::(.+))?$", spec.strip())
    if not m:
        raise ValueError(f"Cannot parse column spec: {spec!r}")
    name, ctype, tagstr = m.groups()
    tags = [t.strip() for t in tagstr.split(",") if t.strip()] if tagstr else []
    return {"name": name, "type": ctype, "tags": tags}


def parse_partition(plist):
    """Parse partition list from tables.yaml."""
    out = []
    for p in plist or []:
        if isinstance(p, dict):
            for k, v in p.items():
                out.append((k, str(v)))
        else:
            k, v = str(p).split(":")
            out.append((k.strip(), v.strip()))
    return out


def load_manifest():
    """Load tables.yaml and return (databases, tables, views)."""
    raw = yaml.safe_load(SOURCE_MANIFEST.read_text())
    tables = []
    for t in raw["tables"]:
        cols = [parse_column(c) for c in t["columns"]]
        tables.append({
            "name": t["name"],
            "db": t["db"],
            "group": t["group"],
            "format": t["format"],
            "columns": cols,
            "partition": parse_partition(t.get("partition")),
            "external": bool(t.get("external", False)),
            "bucketing": t.get("bucketing"),
        })
    return raw["databases"], tables, raw["views"]


# ---------------------------------------------------------------------------
# DDL emission helpers
# ---------------------------------------------------------------------------
def _col_description(col):
    """Get the BigQuery column description from tags."""
    return _epoch_comment(col.get("tags", []))


def _emit_column_line(col, bq_type, indent="  "):
    """Emit a single column definition line."""
    desc = _col_description(col)
    desc_opt = f' OPTIONS(description="{desc}")' if desc else ""
    return f"{indent}{col['name']:<30}{bq_type}{desc_opt}"


def _emit_plain_column(name, bq_type, indent="  "):
    """Emit a column that has no tags (synthetic/partition column)."""
    return f"{indent}{name:<30}{bq_type}"


def emit_staging_table(t):
    """Emit DDL for a staging table with partition conversion and expiration."""
    lines = [f"CREATE TABLE IF NOT EXISTS {t['name']} ("]
    col_defs = []

    # Determine partition/cluster strategy
    partitions = t["partition"]  # List of (name, hive_type)
    bucketing = t.get("bucketing")
    bq_partition_col = None
    cluster_cols = []

    if t["group"] == "sqoop_mirror":
        if t["name"] == "stg_wfm_schedule":
            # Multi-col [load_date:STRING, site_code:STRING]
            # -> PARTITION BY load_date DATE, CLUSTER BY (site_code)
            bq_partition_col = "load_date"
            cluster_cols = ["site_code"]
        else:
            # Single [load_date:STRING] -> PARTITION BY load_date DATE
            bq_partition_col = "load_date"
        # stg_tel_call also has CLUSTERED BY (call_id)
        if bucketing:
            cluster_cols.append(bucketing["by"])

    elif t["group"] == "delta":
        # [extract_ts:STRING] -> PARTITION BY extract_ts DATE
        bq_partition_col = "extract_ts"

    elif t["group"] == "file_feed":
        # [client_code:STRING, feed_date:STRING]
        # -> PARTITION BY feed_date DATE, CLUSTER BY (client_code)
        bq_partition_col = "feed_date"
        cluster_cols = ["client_code"]

    # Build regular column definitions
    for col in t["columns"]:
        bq_type = map_type(col["type"])
        col_defs.append(_emit_column_line(col, bq_type))

    # Add ALL Hive partition columns to the BigQuery schema
    for pcol_name, pcol_hive_type in partitions:
        bq_type = map_partition_col_type(pcol_name, pcol_hive_type)
        col_defs.append(_emit_plain_column(pcol_name, bq_type))

    lines.append(",\n".join(col_defs))
    lines.append(")")

    if bq_partition_col:
        lines.append(f"PARTITION BY {bq_partition_col}")

    if cluster_cols:
        lines.append(f"CLUSTER BY {', '.join(cluster_cols)}")

    lines.append("OPTIONS(partition_expiration_days=90)")

    return "\n".join(lines) + ";\n"


def emit_ods_table(t):
    """Emit DDL for an ODS table."""
    lines = [f"CREATE TABLE IF NOT EXISTS {t['name']} ("]
    col_defs = []
    bq_partition_col = None
    cluster_cols = []
    synthetic_cols = []  # extra columns to add

    if t["group"] == "cleanse":
        # STRING date partition -> DATE
        if t["partition"]:
            bq_partition_col = t["partition"][0][0]

    elif t["group"] == "delta_merge":
        # STRING month/date partition -> DATE
        if t["partition"]:
            bq_partition_col = t["partition"][0][0]

    elif t["group"] == "scd2":
        # eff_from_year INT -> keep as regular column, add eff_from_date DATE partition
        bq_partition_col = "eff_from_date"
        synthetic_cols.append(("eff_from_date", "DATE"))

    elif t["group"] == "acid":
        # CLUSTERED BY -> CLUSTER BY, no partition
        if t.get("bucketing"):
            cluster_cols = [t["bucketing"]["by"]]

    # Build regular column definitions
    for col in t["columns"]:
        bq_type = map_type(col["type"])
        col_defs.append(_emit_column_line(col, bq_type))

    # Add Hive partition columns to the schema
    for pcol_name, pcol_hive_type in t["partition"]:
        bq_type = map_partition_col_type(pcol_name, pcol_hive_type)
        col_defs.append(_emit_plain_column(pcol_name, bq_type))

    # Add synthetic columns (e.g., eff_from_date for SCD-2)
    for syn_name, syn_type in synthetic_cols:
        col_defs.append(_emit_plain_column(syn_name, syn_type))

    lines.append(",\n".join(col_defs))
    lines.append(")")

    if bq_partition_col:
        lines.append(f"PARTITION BY {bq_partition_col}")

    if cluster_cols:
        lines.append(f"CLUSTER BY {', '.join(cluster_cols)}")

    return "\n".join(lines) + ";\n"


def emit_dm_table(t):
    """Emit DDL for a DM table (dim, fact, agg)."""
    lines = [f"CREATE TABLE IF NOT EXISTS {t['name']} ("]
    col_defs = []
    bq_partition_col = None
    cluster_cols = []

    if t["group"] == "dim":
        # No partition, no cluster — dims have no partition list
        pass

    elif t["group"] == "fact":
        if t["name"] == "fact_interaction":
            # Multi-col partition (date_key INT, channel STRING) + CLUSTERED BY (agent_sk)
            # -> PARTITION BY _partition_date, CLUSTER BY (channel, agent_sk)
            bq_partition_col = "partition_date"
            cluster_cols = ["channel", "agent_sk"]
        elif t["name"] == "fact_billing_line":
            # period_month STRING -> partition_date DATE
            bq_partition_col = "partition_date"
        else:
            # date_key INT -> partition_date DATE
            if t["partition"]:
                bq_partition_col = "partition_date"

    elif t["group"] == "agg":
        # date_key INT / week_start_key INT / period_month STRING -> partition_date DATE
        if t["partition"]:
            bq_partition_col = "partition_date"

    # Build regular column definitions
    for col in t["columns"]:
        bq_type = map_type(col["type"])
        col_defs.append(_emit_column_line(col, bq_type))

    # Add original Hive partition columns to BQ schema as regular columns.
    # In DM tables, partition cols keep their original type (mapped to BQ scalar).
    # The DATE conversion is handled by the synthetic _partition_date column.
    for pcol_name, pcol_hive_type in t["partition"]:
        bq_type = map_type(pcol_hive_type)
        col_defs.append(_emit_plain_column(pcol_name, bq_type))

    # Add synthetic partition_date column for fact/agg tables
    if bq_partition_col == "partition_date":
        col_defs.append(_emit_plain_column("partition_date", "DATE"))

    lines.append(",\n".join(col_defs))
    lines.append(")")

    if bq_partition_col:
        lines.append(f"PARTITION BY {bq_partition_col}")

    if cluster_cols:
        lines.append(f"CLUSTER BY {', '.join(cluster_cols)}")

    return "\n".join(lines) + ";\n"


# ---------------------------------------------------------------------------
# Main generation
# ---------------------------------------------------------------------------
def gen_datasets():
    """Generate 01-datasets.sql."""
    return """\
-- BigQuery dataset creation for NBCS migration.
-- This file is NOT applied as a harness DDL step (CREATE SCHEMA is not
-- CREATE TABLE/VIEW).  It's provided for manual/Terraform provisioning.

CREATE SCHEMA IF NOT EXISTS staging
  OPTIONS(description='Sqoop + SFTP landing mirrors (epoch dates live here)');

CREATE SCHEMA IF NOT EXISTS ods
  OPTIONS(description='Cleansed / conformed / merged (all TIMESTAMPs)');

CREATE SCHEMA IF NOT EXISTS dm
  OPTIONS(description='Dimensional marts + all views');

CREATE SCHEMA IF NOT EXISTS _etl_control
  OPTIONS(description='Operational tables: watermarks, DQ results, job audit');
"""


def gen_staging(tables):
    """Generate 02-staging.sql."""
    staging = [t for t in tables if t["db"] == "staging"]
    sqoop = [t for t in staging if t["group"] == "sqoop_mirror"]
    delta = [t for t in staging if t["group"] == "delta"]
    file_feed = [t for t in staging if t["group"] == "file_feed"]

    header = f"""\
-- ---------------------------------------------------------------------------
-- 02-staging.sql — BigQuery DDL for staging layer ({len(staging)} tables)
-- 27 sqoop mirrors + 8 delta feeds + 10 file feeds
-- All STRING partitions converted to DATE. 90-day partition expiration.
-- Complex types: ARRAY<STRUCT>, MAP->ARRAY<STRUCT<key,value>>, ARRAY<STRING>
-- Epoch COMMENTs preserved as column OPTIONS(description=...).
-- ---------------------------------------------------------------------------

"""
    body = []
    for group_name, group_tables in [
        ("Sqoop mirrors (27)", sqoop),
        ("Delta feeds (8)", delta),
        ("File feeds (10)", file_feed),
    ]:
        body.append(f"-- === {group_name} ===\n")
        for t in group_tables:
            body.append(emit_staging_table(t))
            body.append("")

    return header + "\n".join(body)


def gen_ods(tables):
    """Generate 03-ods.sql."""
    ods = [t for t in tables if t["db"] == "ods"]
    cleanse = [t for t in ods if t["group"] == "cleanse"]
    delta_merge = [t for t in ods if t["group"] == "delta_merge"]
    scd2 = [t for t in ods if t["group"] == "scd2"]
    acid = [t for t in ods if t["group"] == "acid"]

    header = f"""\
-- ---------------------------------------------------------------------------
-- 03-ods.sql — BigQuery DDL for ODS layer ({len(ods)} tables)
-- 15 cleanse + 8 delta-merge + 3 SCD-2 + 4 ACID-origin
-- STRING partitions converted to DATE. SCD-2: eff_from_date added.
-- ACID CLUSTERED BY -> CLUSTER BY (no BUCKETS).
-- ---------------------------------------------------------------------------

"""
    body = []
    for group_name, group_tables in [
        ("Cleanse (15)", cleanse),
        ("Delta-merge (8)", delta_merge),
        ("SCD-2 (3)", scd2),
        ("ACID-origin (4)", acid),
    ]:
        body.append(f"-- === {group_name} ===\n")
        for t in group_tables:
            body.append(emit_ods_table(t))
            body.append("")

    return header + "\n".join(body)


def gen_dm_tables(tables):
    """Generate 04-dm-tables.sql."""
    dm = [t for t in tables if t["db"] == "dm"]
    dims = [t for t in dm if t["group"] == "dim"]
    facts = [t for t in dm if t["group"] == "fact"]
    aggs = [t for t in dm if t["group"] == "agg"]

    header = f"""\
-- ---------------------------------------------------------------------------
-- 04-dm-tables.sql — BigQuery DDL for DM layer ({len(dm)} tables)
-- 9 dimensions (unpartitioned) + 9 facts + 7 aggregates
-- INT date_key / week_start_key / STRING period_month converted to
-- partition_date DATE (synthetic column). Bucketing -> CLUSTER BY.
-- ---------------------------------------------------------------------------

"""
    body = []
    for group_name, group_tables in [
        ("Dimensions (9)", dims),
        ("Facts (9)", facts),
        ("Aggregates (7)", aggs),
    ]:
        body.append(f"-- === {group_name} ===\n")
        for t in group_tables:
            body.append(emit_dm_table(t))
            body.append("")

    return header + "\n".join(body)


def gen_dm_views():
    """Generate 05-dm-views.sql — hand-translated BigQuery views."""
    return """\
-- ---------------------------------------------------------------------------
-- 05-dm-views.sql — 15 BigQuery views in the dm dataset
-- Hand-translated from Hive/Impala to BigQuery SQL.
-- Dialect conversions applied:
--   NDV() -> APPROX_COUNT_DISTINCT()
--   WITH ROLLUP + GROUPING__ID -> GROUP BY ROLLUP() + GROUPING()
--   RLIKE -> REGEXP_CONTAINS()
--   regexp_extract group syntax -> RE2
--   unix_timestamp() -> UNIX_SECONDS()
--   from_unixtime(CAST(x/1000 AS BIGINT)) -> TIMESTAMP_MILLIS()
--   TIMESTAMP_ADD for date_add on TIMESTAMP columns
--   from_unixtime/unix_timestamp formatting -> PARSE_DATE/FORMAT_DATE
--   ON 1 = 1 -> CROSS JOIN
-- ---------------------------------------------------------------------------

-- 1. Org hierarchy (recursive CTE — BigQuery supports natively)
CREATE OR REPLACE VIEW vw_org_hierarchy AS
WITH RECURSIVE org_tree (org_unit_id, unit_code, unit_name, unit_type,
                         site_code, root_unit_id, depth, path_names) AS (
  SELECT o.org_unit_id, o.unit_code, o.unit_name, o.unit_type,
         o.site_code, o.org_unit_id AS root_unit_id, 0 AS depth,
         o.unit_name AS path_names
  FROM   ods.ods_org_unit o
  WHERE  o.parent_unit_id IS NULL
  UNION ALL
  SELECT c.org_unit_id, c.unit_code, c.unit_name, c.unit_type,
         c.site_code, p.root_unit_id, p.depth + 1,
         CONCAT(p.path_names, ' > ', c.unit_name)
  FROM   ods.ods_org_unit c
  JOIN   org_tree p ON c.parent_unit_id = p.org_unit_id
  WHERE  p.depth < 6
)
SELECT org_unit_id, unit_code, unit_name, unit_type, site_code,
       root_unit_id, depth, path_names
FROM   org_tree;

-- 2. Active-agent panel (NDV -> APPROX_COUNT_DISTINCT)
CREATE OR REPLACE VIEW vw_active_agents_ndv AS
SELECT f.date_key,
       a.site_code,
       APPROX_COUNT_DISTINCT(f.agent_sk)                                   AS approx_active_agents,
       APPROX_COUNT_DISTINCT(CONCAT(CAST(f.agent_sk AS STRING), '|', f.channel)) AS approx_agent_channel_pairs,
       COUNT(*)                                          AS interactions
FROM   dm.fact_interaction f
JOIN   dm.dim_agent a ON a.agent_sk = f.agent_sk
GROUP  BY f.date_key, a.site_code;

-- 3. CSAT rollup (GROUP BY ROLLUP + GROUPING instead of WITH ROLLUP + GROUPING__ID)
CREATE OR REPLACE VIEW vw_csat_rollup AS
SELECT p.client_id,
       p.program_code,
       COUNT(*)                       AS surveys,
       AVG(s.csat_score)              AS avg_csat,
       SUM(CASE WHEN s.nps_score >= 9 THEN 1 ELSE 0 END) / COUNT(*) * 100 AS pct_promoters,
       GROUPING(p.client_id) * 2 + GROUPING(p.program_code) AS grouping_level
FROM   dm.fact_csat_survey s
JOIN   dm.dim_program p ON p.program_sk = s.program_sk
GROUP  BY ROLLUP(p.client_id, p.program_code);

-- 4. Call-driver classification (RLIKE -> REGEXP_CONTAINS, regexp_extract -> REGEXP_EXTRACT with RE2)
CREATE OR REPLACE VIEW vw_call_driver_regex AS
SELECT c.call_date,
       c.queue_id,
       CASE
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(bill|invoice|charge|refund)')    THEN 'BILLING'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(password|login|locked|reset)')   THEN 'ACCESS'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(cancel|churn|retention)')        THEN 'RETENTION'
         WHEN REGEXP_EXTRACT(d.disposition_desc, r'^\\[([A-Z]{2,5})\\]') IS NOT NULL
              THEN REGEXP_EXTRACT(d.disposition_desc, r'^\\[([A-Z]{2,5})\\]')
         ELSE 'OTHER'
       END                                              AS call_driver,
       REGEXP_EXTRACT(d.disposition_desc, r'ref#(\\d+)') AS embedded_ref_no,
       COUNT(*)                                         AS calls,
       AVG(c.talk_seconds)                              AS avg_talk_seconds
FROM   ods.ods_call c
JOIN   dm.dim_disposition d ON d.disposition_code = c.disposition_code
GROUP  BY c.call_date, c.queue_id,
       CASE
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(bill|invoice|charge|refund)')    THEN 'BILLING'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(password|login|locked|reset)')   THEN 'ACCESS'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(cancel|churn|retention)')        THEN 'RETENTION'
         WHEN REGEXP_EXTRACT(d.disposition_desc, r'^\\[([A-Z]{2,5})\\]') IS NOT NULL
              THEN REGEXP_EXTRACT(d.disposition_desc, r'^\\[([A-Z]{2,5})\\]')
         ELSE 'OTHER'
       END,
       REGEXP_EXTRACT(d.disposition_desc, r'ref#(\\d+)');

-- 5. Repeat contacts within 72h (unix_timestamp -> UNIX_SECONDS)
CREATE OR REPLACE VIEW vw_repeat_contact_window AS
SELECT i.interaction_id,
       i.customer_ref,
       i.channel,
       i.start_ts,
       LAG(i.start_ts) OVER (PARTITION BY i.customer_ref ORDER BY i.start_ts) AS prev_contact_ts,
       CASE WHEN UNIX_SECONDS(i.start_ts)
               - UNIX_SECONDS(LAG(i.start_ts) OVER (PARTITION BY i.customer_ref
                                                      ORDER BY i.start_ts)) <= 259200
            THEN 1 ELSE 0 END                            AS repeat_within_72h
FROM   ods.ods_interaction i
WHERE  i.customer_ref IS NOT NULL AND i.customer_ref <> '';

-- 6. Billing reconciliation (from_unixtime(CAST(x/1000 AS BIGINT)) -> TIMESTAMP_MILLIS)
CREATE OR REPLACE VIEW vw_billing_reconciliation AS
SELECT s.invoice_no,
       s.total_amount                                    AS staged_amount,
       a.total_amount                                    AS ods_amount,
       TIMESTAMP_MILLIS(s.issued_ts_sec)                 AS staged_issued_ts,
       a.issued_ts                                       AS ods_issued_ts,
       (UNIX_SECONDS(a.issued_ts) - CAST(s.issued_ts_sec / 1000 AS INT64)) AS drift_seconds,
       CASE WHEN ABS(s.total_amount - a.total_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
            WHEN UNIX_SECONDS(a.issued_ts) <> CAST(s.issued_ts_sec / 1000 AS INT64) THEN 'TS_MISMATCH'
            ELSE 'OK' END                                AS recon_status
FROM   staging.stg_fin_invoice s
JOIN   ods.ods_invoice_acid a ON a.invoice_id = s.invoice_id;

-- 7. Current agent roster (latest SCD-2 slice via ROW_NUMBER)
CREATE OR REPLACE VIEW vw_agent_roster_current AS
SELECT latest.agent_id, latest.employee_no, latest.org_unit_id, latest.job_grade,
       latest.employment_type, latest.status, latest.eff_from_ts,
       asg.program_id AS current_program_id,
       asg.queue_id   AS current_queue_id,
       asg.role_on_program
FROM (
  SELECT h.*,
         ROW_NUMBER() OVER (PARTITION BY h.agent_id ORDER BY h.eff_from_ts DESC) AS rn
  FROM   ods.ods_agent_scd2 h
) latest
LEFT   JOIN ods.ods_agent_assignment_scd2 asg
       ON asg.agent_id = latest.agent_id AND asg.is_current = TRUE
WHERE  latest.rn = 1;

-- 8. Agent scorecard (composite ranking: PERCENT_RANK + NTILE)
CREATE OR REPLACE VIEW vw_agent_scorecard AS
WITH perf AS (
  SELECT d.agent_sk,
         AVG(d.avg_handle_seconds)  AS aht,
         AVG(d.adherence_pct)       AS adherence,
         SUM(d.interactions_handled) AS volume
  FROM   dm.agg_agent_daily d
  GROUP  BY d.agent_sk
),
qa AS (
  SELECT q.agent_sk, AVG(q.overall_pct) AS avg_qa_pct
  FROM   dm.fact_qa_evaluation q
  GROUP  BY q.agent_sk
),
skills AS (
  SELECT s.agent_id, COUNT(*) AS certified_skills
  FROM   ods.ods_agent_skill_scd2 s
  WHERE  s.is_current = TRUE AND s.certified = TRUE
  GROUP  BY s.agent_id
)
SELECT a.agent_sk, a.full_name, a.site_code, a.job_grade,
       p.aht, p.adherence, p.volume, q.avg_qa_pct,
       COALESCE(sk.certified_skills, 0) AS certified_skills,
       PERCENT_RANK() OVER (ORDER BY p.aht ASC)          AS aht_pctile,
       NTILE(4)       OVER (ORDER BY q.avg_qa_pct DESC)  AS qa_quartile
FROM   dm.dim_agent a
JOIN   perf p ON p.agent_sk = a.agent_sk
LEFT   JOIN qa q ON q.agent_sk = a.agent_sk
LEFT   JOIN skills sk ON sk.agent_id = a.agent_id
WHERE  a.is_current = TRUE;

-- 9. Attrition risk (nested CTEs + NTILE banding)
CREATE OR REPLACE VIEW vw_attrition_risk AS
WITH adh AS (
  SELECT f.agent_sk, AVG(f.adherence_pct) AS adherence_90d
  FROM   dm.fact_adherence_daily f
  GROUP  BY f.agent_sk
),
notice AS (
  SELECT e.agent_id, COUNT(*) AS notice_events
  FROM   ods.ods_attrition_event e
  GROUP  BY e.agent_id
),
wk AS (
  SELECT w.agent_sk, AVG(w.interactions_handled) AS weekly_volume
  FROM   dm.agg_agent_weekly w
  GROUP  BY w.agent_sk
),
banded AS (
  SELECT a.agent_sk, a.agent_id, a.full_name, a.site_code,
         adh.adherence_90d,
         COALESCE(n.notice_events, 0) AS notice_events,
         NTILE(5) OVER (ORDER BY adh.adherence_90d ASC) AS adherence_band
  FROM   dm.dim_agent a
  JOIN   adh    ON adh.agent_sk = a.agent_sk
  LEFT   JOIN notice n ON n.agent_id = a.agent_id
  LEFT   JOIN wk ON wk.agent_sk = a.agent_sk
  WHERE  a.is_current = TRUE AND a.status = 'ACTIVE'
)
SELECT agent_sk, agent_id, full_name, site_code, adherence_90d, notice_events,
       CASE WHEN adherence_band = 1 OR notice_events > 0 THEN 'HIGH'
            WHEN adherence_band = 2 THEN 'MEDIUM' ELSE 'LOW' END AS attrition_risk
FROM   banded;

-- 10. Queue SLA attainment (layer-skip: dm view reads staging.stg_crm_sla_target)
CREATE OR REPLACE VIEW vw_queue_sla_attainment AS
SELECT q.queue_code,
       q.media_type,
       f.date_key,
       SUM(f.answered_in_sl) / NULLIF(SUM(f.answered), 0) * 100 AS sl_pct,
       MAX(t.target_value)                                       AS sl_target,
       CASE WHEN SUM(f.answered_in_sl) / NULLIF(SUM(f.answered), 0) * 100
                 >= MAX(t.target_value) THEN 'MET' ELSE 'MISSED' END AS attainment
FROM   dm.fact_queue_interval f
JOIN   dm.dim_queue q          ON q.queue_sk = f.queue_sk
LEFT   JOIN staging.stg_crm_sla_target t
       ON t.queue_id = q.queue_id AND t.metric_code = 'SL_20_80'
GROUP  BY q.queue_code, q.media_type, f.date_key;

-- 11. First-contact resolution (date_add(ts, 7) -> TIMESTAMP_ADD(ts, INTERVAL 7 DAY))
CREATE OR REPLACE VIEW vw_first_contact_resolution AS
SELECT f.date_key,
       f.program_sk,
       COUNT(*)                                          AS resolved_interactions,
       SUM(CASE WHEN rpt.interaction_id IS NULL THEN 1 ELSE 0 END) AS fcr_count,
       SUM(CASE WHEN rpt.interaction_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100 AS fcr_pct
FROM   dm.fact_interaction f
LEFT   JOIN dm.fact_interaction rpt
       ON  rpt.customer_ref = f.customer_ref
       AND rpt.start_ts > f.end_ts
       AND rpt.start_ts <= TIMESTAMP_ADD(f.end_ts, INTERVAL 7 DAY)
WHERE  f.resolved_flag = TRUE
GROUP  BY f.date_key, f.program_sk;

-- 12. Occupancy / utilization (pivot of agent-state seconds)
CREATE OR REPLACE VIEW vw_occupancy_utilization AS
SELECT f.date_key,
       a.site_code,
       f.agent_sk,
       SUM(CASE WHEN f.state_code IN ('TALK','HOLD','ACW') THEN f.state_seconds ELSE 0 END) AS handle_seconds,
       SUM(CASE WHEN f.state_code = 'READY'                THEN f.state_seconds ELSE 0 END) AS ready_seconds,
       SUM(CASE WHEN f.state_code LIKE 'AUX%'              THEN f.state_seconds ELSE 0 END) AS aux_seconds,
       SUM(CASE WHEN f.state_code IN ('TALK','HOLD','ACW') THEN f.state_seconds ELSE 0 END)
         / NULLIF(SUM(CASE WHEN f.state_code IN ('TALK','HOLD','ACW','READY')
                           THEN f.state_seconds ELSE 0 END), 0) * 100 AS occupancy_pct
FROM   dm.fact_agent_activity f
JOIN   dm.dim_agent a ON a.agent_sk = f.agent_sk
GROUP  BY f.date_key, a.site_code, f.agent_sk;

-- 13. Shrinkage analysis (from_unixtime/unix_timestamp -> PARSE_DATE/FORMAT_DATE)
CREATE OR REPLACE VIEW vw_shrinkage_analysis AS
SELECT f.date_key,
       a.site_code,
       SUM(f.scheduled_minutes)                          AS scheduled_minutes,
       SUM(f.worked_minutes)                             AS worked_minutes,
       SUM(f.exception_minutes + f.timeoff_minutes)      AS shrinkage_minutes,
       SUM(f.exception_minutes + f.timeoff_minutes)
         / NULLIF(SUM(f.scheduled_minutes), 0) * 100     AS shrinkage_pct,
       COUNT(DISTINCT s.schedule_id)                     AS schedules,
       COUNT(DISTINCT sh.shift_sk)                       AS overnight_shifts
FROM   dm.fact_adherence_daily f
JOIN   dm.dim_agent a ON a.agent_sk = f.agent_sk
LEFT   JOIN ods.ods_schedule s
       ON s.agent_id = a.agent_id
      AND s.sched_date = PARSE_DATE('%Y%m%d', CAST(f.date_key AS STRING))
LEFT   JOIN dm.dim_shift sh ON sh.shift_id = s.shift_id AND sh.overnight_flag = TRUE
GROUP  BY f.date_key, a.site_code;

-- 14. Program margin (ON 1=1 -> CROSS JOIN)
CREATE OR REPLACE VIEW vw_program_margin AS
SELECT b.period_month,
       b.client_sk,
       b.program_sk,
       b.billed_amount,
       b.net_revenue,
       lab.billable_cost_minutes / 60.0 * 18.50          AS est_labor_cost,
       adj.total_adjustments,
       b.net_revenue - (lab.billable_cost_minutes / 60.0 * 18.50)
                     - COALESCE(adj.total_adjustments, 0) AS est_margin,
       cmt.committed_min
FROM   dm.agg_billing_monthly b
LEFT   JOIN (
  SELECT t.program_id, t.work_month, SUM(t.billable_minutes) AS billable_cost_minutes
  FROM   ods.ods_timesheet t GROUP BY t.program_id, t.work_month
) lab ON lab.work_month = b.period_month
LEFT   JOIN (
  SELECT p.period_month, SUM(p.amount) AS total_adjustments
  FROM   ods.ods_payroll_adjustment p GROUP BY p.period_month
) adj ON adj.period_month = b.period_month
CROSS  JOIN (
  SELECT SUM(cl.min_commit) AS committed_min
  FROM   ods.ods_contract_line cl
) cmt;

-- 15. Client executive summary (HUB view — wide multi-fact join)
CREATE OR REPLACE VIEW vw_client_executive_summary AS
SELECT c.client_code,
       c.client_name,
       pm.period_month,
       pr.program_code,
       pm.interactions,
       pm.avg_handle_seconds,
       pm.avg_csat,
       cs.pct_promoters,
       cs.pct_detractors,
       bm.billed_amount,
       bm.sla_credit_amount,
       bm.net_revenue,
       tk.open_tickets,
       tk.sla_breached_tickets
FROM   dm.dim_client c
JOIN   dm.dim_program pr            ON pr.client_id = c.client_id
JOIN   dm.agg_program_monthly pm    ON pm.program_sk = pr.program_sk AND pm.grouping_level = 0
LEFT   JOIN dm.agg_csat_rollup_monthly cs
       ON cs.program_sk = pr.program_sk AND cs.period_month = pm.period_month
LEFT   JOIN dm.agg_billing_monthly bm
       ON bm.program_sk = pr.program_sk AND bm.period_month = pm.period_month
LEFT   JOIN (
  SELECT t.program_sk,
         SUM(CASE WHEN t.status IN ('OPEN','PENDING') THEN 1 ELSE 0 END) AS open_tickets,
         SUM(CASE WHEN t.sla_breached_flag THEN 1 ELSE 0 END)            AS sla_breached_tickets
  FROM   dm.fact_ticket t GROUP BY t.program_sk
) tk ON tk.program_sk = pr.program_sk;
"""


def gen_etl_control():
    """Generate 06-etl-control.sql — 3 operational tables."""
    return """\
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
"""


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _dbs, tables, views = load_manifest()

    # Count tables by layer
    staging = [t for t in tables if t["db"] == "staging"]
    ods = [t for t in tables if t["db"] == "ods"]
    dm = [t for t in tables if t["db"] == "dm"]

    print(f"Manifest loaded: {len(tables)} tables, {len(views)} views")
    print(f"  staging: {len(staging)}")
    print(f"  ods:     {len(ods)}")
    print(f"  dm:      {len(dm)}")

    # Generate files
    files = {
        "01-datasets.sql": gen_datasets(),
        "02-staging.sql": gen_staging(tables),
        "03-ods.sql": gen_ods(tables),
        "04-dm-tables.sql": gen_dm_tables(tables),
        "05-dm-views.sql": gen_dm_views(),
        "06-etl-control.sql": gen_etl_control(),
    }

    for fname, content in files.items():
        path = OUTPUT_DIR / fname
        path.write_text(content)
        print(f"  wrote {fname} ({len(content)} bytes)")

    # Validation: count CREATE TABLE / CREATE VIEW statements
    total_tables = 0
    total_views = 0
    for fname, content in files.items():
        ct = len(re.findall(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS", content, re.IGNORECASE))
        ct += len(re.findall(r"CREATE\s+OR\s+REPLACE\s+TABLE", content, re.IGNORECASE))
        cv = len(re.findall(r"CREATE\s+OR\s+REPLACE\s+VIEW", content, re.IGNORECASE))
        print(f"  {fname}: {ct} tables, {cv} views")
        total_tables += ct
        total_views += cv

    print(f"\nTotal: {total_tables} tables + {total_views} views")
    if total_tables != 103:
        print(f"ERROR: Expected 103 tables (100 source + 3 etl_control), got {total_tables}")
        sys.exit(1)
    if total_views != 15:
        print(f"ERROR: Expected 15 views, got {total_views}")
        sys.exit(1)
    print("All counts verified.")


if __name__ == "__main__":
    main()
