#!/usr/bin/env python3
"""Generate MVS spec YAML files for schema_conformance + queryability/perf.

Reads /workspace/source/manifests/tables.yaml and emits:
  tests/schema/staging_conformance.mvs.yaml
  tests/schema/ods_conformance.mvs.yaml
  tests/schema/dm_conformance.mvs.yaml
  tests/schema/etl_control_conformance.mvs.yaml
  tests/schema/queryability_and_perf.mvs.yaml
"""

import re
import sys
from pathlib import Path

import yaml

SOURCE_MANIFEST = Path("/workspace/source/manifests/tables.yaml")
OUTPUT_DIR = Path("/workspace/project/tests/schema")

# ---------------------------------------------------------------------------
# Type mapping (same as gen_bq_ddl.py)
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

DATE_PARTITION_NAMES = {
    "load_date", "extract_ts", "feed_date",
    "snapshot_date", "sched_date", "event_date", "call_date",
    "work_month", "period_month", "swap_month", "event_month",
    "snapshot_month",
}

EPOCH_TAGS = {"epoch_sec", "epoch_ms", "ora_str", "lie_ms"}


def _epoch_description(tags):
    for t in tags:
        if t == "epoch_sec":
            return "epoch SECONDS (legacy)"
        elif t == "epoch_ms":
            return "epoch MILLISECONDS (legacy)"
        elif t == "ora_str":
            return "Oracle string YYYYMMDDHH24MISS (legacy)"
        elif t == "lie_ms":
            return "!! name says seconds, VALUES ARE MILLIS !!"
    return None


def map_type_bq(hive_type):
    upper = hive_type.strip().upper()
    m = re.match(r"DECIMAL\((\d+),(\d+)\)", upper)
    if m:
        return f"NUMERIC({m.group(1)},{m.group(2)})"
    m = re.match(r"MAP<(.+),\s*(.+)>$", upper)
    if m:
        k = map_type_bq(m.group(1).strip())
        v = map_type_bq(m.group(2).strip())
        return f"ARRAY<STRUCT<key {k}, value {v}>>"
    m = re.match(r"ARRAY<STRUCT<(.+)>>$", upper)
    if m:
        fields = _split_struct(m.group(1))
        parts = []
        for f in fields:
            p = f.strip().split(":")
            parts.append(f"{p[0].strip().lower()} {map_type_bq(p[1].strip())}")
        return f"ARRAY<STRUCT<{', '.join(parts)}>>"
    m = re.match(r"ARRAY<(.+)>$", upper)
    if m:
        return f"ARRAY<{map_type_bq(m.group(1).strip())}>"
    return SCALAR_TYPE_MAP.get(upper, upper)


def _split_struct(s):
    parts, depth, cur = [], 0, []
    for c in s:
        if c == '<': depth += 1; cur.append(c)
        elif c == '>': depth -= 1; cur.append(c)
        elif c == ',' and depth == 0: parts.append(''.join(cur).strip()); cur = []
        else: cur.append(c)
    if cur: parts.append(''.join(cur).strip())
    return parts


def source_type_name(hive_type):
    """Return the source type name for cross-check. For scalars, return the
    Hive type directly. For complex types, return the full type."""
    upper = hive_type.strip().upper()
    if upper in SCALAR_TYPE_MAP:
        return upper
    m = re.match(r"DECIMAL\((\d+),(\d+)\)", upper)
    if m:
        return "DECIMAL"
    # Complex types: return the full original type
    return upper


def numeric_scale(hive_type):
    """Extract scale from DECIMAL(p,s)."""
    m = re.match(r"DECIMAL\((\d+),(\d+)\)", hive_type.strip().upper())
    if m:
        return int(m.group(2))
    return None


def is_complex(hive_type):
    upper = hive_type.strip().upper()
    return upper.startswith("ARRAY") or upper.startswith("MAP") or upper.startswith("STRUCT")


# ---------------------------------------------------------------------------
# Manifest parsing
# ---------------------------------------------------------------------------
def parse_column(spec):
    if isinstance(spec, dict):
        return {"name": spec["name"], "type": str(spec["type"]),
                "tags": list(spec.get("tags", []))}
    m = re.match(r"^([a-z0-9_]+):([A-Z0-9_(),]+?)(?::(.+))?$", spec.strip())
    if not m:
        raise ValueError(f"Cannot parse: {spec!r}")
    name, ctype, tagstr = m.groups()
    tags = [t.strip() for t in tagstr.split(",") if t.strip()] if tagstr else []
    return {"name": name, "type": ctype, "tags": tags}


def parse_partition(plist):
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
    raw = yaml.safe_load(SOURCE_MANIFEST.read_text())
    tables = []
    for t in raw["tables"]:
        tables.append({
            "name": t["name"], "db": t["db"], "group": t["group"],
            "columns": [parse_column(c) for c in t["columns"]],
            "partition": parse_partition(t.get("partition")),
            "bucketing": t.get("bucketing"),
        })
    return tables, raw["views"]


# ---------------------------------------------------------------------------
# Column spec builder
# ---------------------------------------------------------------------------
def build_col_spec(col, include_source=True):
    """Build an MVS column declaration dict."""
    bq_type = map_type_bq(col["type"])
    spec = {"name": col["name"], "type": bq_type}

    # Scale for NUMERIC
    s = numeric_scale(col["type"])
    if s is not None:
        spec["scale"] = s

    # Description for epoch columns
    desc = _epoch_description(col.get("tags", []))
    if desc:
        spec["description"] = desc

    # Source type for cross-check
    if include_source:
        st = source_type_name(col["type"])
        spec["source_type"] = st

    return spec


def build_partition_col_spec(pcol_name, pcol_hive_type, is_date_partition=True, include_source=True):
    """Build an MVS column spec for a former Hive partition column."""
    if is_date_partition and pcol_hive_type.upper() == "STRING" and pcol_name in DATE_PARTITION_NAMES:
        bq_type = "DATE"
    elif pcol_hive_type.upper() in ("INT", "BIGINT"):
        bq_type = "INT64"
    else:
        bq_type = SCALAR_TYPE_MAP.get(pcol_hive_type.upper(), pcol_hive_type.upper())

    spec = {"name": pcol_name, "type": bq_type}
    if include_source:
        spec["source_type"] = pcol_hive_type.upper()
    return spec


# ---------------------------------------------------------------------------
# Staging spec
# ---------------------------------------------------------------------------
def gen_staging_spec(tables):
    staging = [t for t in tables if t["db"] == "staging"]
    tbl_specs = []

    for t in staging:
        partitions = t["partition"]
        bucketing = t.get("bucketing")

        # Determine BQ partition/cluster
        bq_part = None
        bq_cluster = []

        if t["group"] == "sqoop_mirror":
            bq_part = "load_date"
            if t["name"] == "stg_wfm_schedule":
                bq_cluster = ["site_code"]
            if bucketing:
                bq_cluster.append(bucketing["by"])
        elif t["group"] == "delta":
            bq_part = "extract_ts"
        elif t["group"] == "file_feed":
            bq_part = "feed_date"
            bq_cluster = ["client_code"]

        # Build columns
        cols = []
        for col in t["columns"]:
            cols.append(build_col_spec(col, include_source=True))

        # Add partition columns
        for pcol_name, pcol_hive_type in partitions:
            cols.append(build_partition_col_spec(pcol_name, pcol_hive_type,
                                                 is_date_partition=True, include_source=True))

        tspec = {
            "table": t["name"],
            "source_table": t["name"],
            "expect_object_type": "TABLE",
            "columns": cols,
        }
        if bq_part:
            tspec["partition_by"] = bq_part
        if bq_cluster:
            tspec["cluster_by"] = bq_cluster
        tspec["table_options"] = {"partition_expiration_days": 90}

        tbl_specs.append(tspec)

    spec = {
        "name": "staging_schema_conformance",
        "description": "Schema conformance for staging layer — 45 tables, all columns, partition/cluster/expiration.",
        "connections": {
            "source": {"engine": "impala"},
            "target": {"engine": "bigquery"},
        },
        "source_setup": {
            "location_base": "${SOURCE_WAREHOUSE:-/tmp/dmt_src}",
            "ddl": [
                "sql/legacy/01-create-databases.hql",
                "sql/legacy/02-staging.hql",
            ],
        },
        "migration": {
            "steps": [
                {"kind": "ddl", "sql": "ddl/02-staging.sql"},
            ],
        },
        "suites": [
            {
                "pattern": "schema_conformance",
                "id": "staging-schema",
                "story_id": "NBCS-TARGET-SCHEMA",
                "target_dataset": "${BUILD_DATASET}",
                "source_database": "staging",
                "expect_table_count": 45,
                "tables": tbl_specs,
            }
        ],
    }
    return spec


# ---------------------------------------------------------------------------
# ODS spec
# ---------------------------------------------------------------------------
def gen_ods_spec(tables):
    ods = [t for t in tables if t["db"] == "ods"]
    tbl_specs = []

    for t in ods:
        partitions = t["partition"]
        bucketing = t.get("bucketing")

        bq_part = None
        bq_cluster = []

        if t["group"] == "cleanse":
            if partitions:
                bq_part = partitions[0][0]
        elif t["group"] == "delta_merge":
            if partitions:
                bq_part = partitions[0][0]
        elif t["group"] == "scd2":
            bq_part = "eff_from_date"
        elif t["group"] == "acid":
            if bucketing:
                bq_cluster = [bucketing["by"]]

        cols = []
        for col in t["columns"]:
            cols.append(build_col_spec(col, include_source=True))

        # Add partition columns to schema
        for pcol_name, pcol_hive_type in partitions:
            cols.append(build_partition_col_spec(pcol_name, pcol_hive_type,
                                                 is_date_partition=True, include_source=True))

        # SCD-2: add synthetic eff_from_date
        if t["group"] == "scd2":
            cols.append({"name": "eff_from_date", "type": "DATE"})

        tspec = {
            "table": t["name"],
            "source_table": t["name"],
            "expect_object_type": "TABLE",
            "columns": cols,
        }
        if bq_part:
            tspec["partition_by"] = bq_part
        if bq_cluster:
            tspec["cluster_by"] = bq_cluster

        tbl_specs.append(tspec)

    spec = {
        "name": "ods_schema_conformance",
        "description": "Schema conformance for ODS layer — 30 tables, all columns, partition/cluster.",
        "connections": {
            "source": {"engine": "impala"},
            "target": {"engine": "bigquery"},
        },
        "source_setup": {
            "location_base": "${SOURCE_WAREHOUSE:-/tmp/dmt_src}",
            "ddl": [
                "sql/legacy/01-create-databases.hql",
                "sql/legacy/03-ods.hql",
            ],
        },
        "migration": {
            "steps": [
                {"kind": "ddl", "sql": "ddl/03-ods.sql"},
            ],
        },
        "suites": [
            {
                "pattern": "schema_conformance",
                "id": "ods-schema",
                "story_id": "NBCS-TARGET-SCHEMA",
                "target_dataset": "${BUILD_DATASET}",
                "source_database": "ods",
                "expect_table_count": 30,
                "tables": tbl_specs,
            }
        ],
    }
    return spec


# ---------------------------------------------------------------------------
# DM spec (tables + views)
# ---------------------------------------------------------------------------
def gen_dm_spec(tables, views):
    dm = [t for t in tables if t["db"] == "dm"]
    tbl_specs = []

    for t in dm:
        partitions = t["partition"]
        bucketing = t.get("bucketing")

        bq_part = None
        bq_cluster = []

        if t["group"] == "dim":
            pass  # no partition/cluster
        elif t["group"] == "fact":
            if t["name"] == "fact_interaction":
                bq_part = "partition_date"
                bq_cluster = ["channel", "agent_sk"]
            elif partitions:
                bq_part = "partition_date"
        elif t["group"] == "agg":
            if partitions:
                bq_part = "partition_date"

        cols = []
        for col in t["columns"]:
            cols.append(build_col_spec(col, include_source=True))

        # Add original partition columns as regular columns (keep original types)
        for pcol_name, pcol_hive_type in partitions:
            bq_type = SCALAR_TYPE_MAP.get(pcol_hive_type.upper(), pcol_hive_type.upper())
            spec_col = {"name": pcol_name, "type": bq_type, "source_type": pcol_hive_type.upper()}
            cols.append(spec_col)

        # Add synthetic partition_date for partitioned facts/aggs
        if bq_part == "partition_date":
            cols.append({"name": "partition_date", "type": "DATE"})

        tspec = {
            "table": t["name"],
            "source_table": t["name"],
            "expect_object_type": "TABLE",
            "columns": cols,
        }
        if bq_part:
            tspec["partition_by"] = bq_part
        if bq_cluster:
            tspec["cluster_by"] = bq_cluster

        tbl_specs.append(tspec)

    # Views — declare expect_object_type: VIEW, columns from the view output
    view_specs = _build_view_specs(views)

    all_specs = tbl_specs + view_specs
    total_count = len(dm) + len(views)  # 25 + 15 = 40

    spec = {
        "name": "dm_schema_conformance",
        "description": f"Schema conformance for DM layer — {len(dm)} tables + {len(views)} views.",
        "connections": {
            "source": {"engine": "impala"},
            "target": {"engine": "bigquery"},
        },
        "source_setup": {
            "location_base": "${SOURCE_WAREHOUSE:-/tmp/dmt_src}",
            "ddl": [
                "sql/legacy/01-create-databases.hql",
                "sql/legacy/04-dm-tables.hql",
            ],
        },
        "migration": {
            "steps": [
                {"kind": "ddl", "sql": "ddl/04-dm-tables.sql"},
                {"kind": "ddl", "sql": "ddl/05-dm-views.sql"},
            ],
        },
        "suites": [
            {
                "pattern": "schema_conformance",
                "id": "dm-schema",
                "story_id": "NBCS-TARGET-SCHEMA",
                "target_dataset": "${BUILD_DATASET}",
                "source_database": "dm",
                "expect_table_count": total_count,
                "tables": all_specs,
            }
        ],
    }
    return spec


def _build_view_specs(views):
    """Build MVS table specs for the 15 DM views.
    Views are declared with expect_object_type: VIEW.
    No source cross-check (views aren't applied to Hive source_setup).
    We declare key output columns per view for structural validation."""

    # Define the output columns for each view (derived from the hand-translated SQL)
    view_columns = {
        "vw_org_hierarchy": [
            {"name": "org_unit_id", "type": "INT64"},
            {"name": "unit_code", "type": "STRING"},
            {"name": "unit_name", "type": "STRING"},
            {"name": "unit_type", "type": "STRING"},
            {"name": "site_code", "type": "STRING"},
            {"name": "root_unit_id", "type": "INT64"},
            {"name": "depth", "type": "INT64"},
            {"name": "path_names", "type": "STRING"},
        ],
        "vw_active_agents_ndv": [
            {"name": "date_key", "type": "INT64"},
            {"name": "site_code", "type": "STRING"},
            {"name": "approx_active_agents", "type": "INT64"},
            {"name": "approx_agent_channel_pairs", "type": "INT64"},
            {"name": "interactions", "type": "INT64"},
        ],
        "vw_csat_rollup": [
            {"name": "client_id", "type": "INT64"},
            {"name": "program_code", "type": "STRING"},
            {"name": "surveys", "type": "INT64"},
            {"name": "avg_csat", "type": "FLOAT64"},
            {"name": "pct_promoters", "type": "FLOAT64"},
            {"name": "grouping_level", "type": "INT64"},
        ],
        "vw_call_driver_regex": [
            {"name": "call_date", "type": "DATE"},
            {"name": "queue_id", "type": "INT64"},
            {"name": "call_driver", "type": "STRING"},
            {"name": "embedded_ref_no", "type": "STRING"},
            {"name": "calls", "type": "INT64"},
            {"name": "avg_talk_seconds", "type": "FLOAT64"},
        ],
        "vw_repeat_contact_window": [
            {"name": "interaction_id", "type": "STRING"},
            {"name": "customer_ref", "type": "STRING"},
            {"name": "channel", "type": "STRING"},
            {"name": "start_ts", "type": "TIMESTAMP"},
            {"name": "prev_contact_ts", "type": "TIMESTAMP"},
            {"name": "repeat_within_72h", "type": "INT64"},
        ],
        "vw_billing_reconciliation": [
            {"name": "invoice_no", "type": "STRING"},
            {"name": "staged_amount", "type": "NUMERIC", "scale": 2},
            {"name": "ods_amount", "type": "NUMERIC", "scale": 2},
            {"name": "staged_issued_ts", "type": "TIMESTAMP"},
            {"name": "ods_issued_ts", "type": "TIMESTAMP"},
            {"name": "drift_seconds", "type": "INT64"},
            {"name": "recon_status", "type": "STRING"},
        ],
        "vw_agent_roster_current": [
            {"name": "agent_id", "type": "INT64"},
            {"name": "employee_no", "type": "STRING"},
            {"name": "org_unit_id", "type": "INT64"},
            {"name": "job_grade", "type": "STRING"},
            {"name": "employment_type", "type": "STRING"},
            {"name": "status", "type": "STRING"},
            {"name": "eff_from_ts", "type": "TIMESTAMP"},
            {"name": "current_program_id", "type": "INT64"},
            {"name": "current_queue_id", "type": "INT64"},
            {"name": "role_on_program", "type": "STRING"},
        ],
        "vw_agent_scorecard": [
            {"name": "agent_sk", "type": "INT64"},
            {"name": "full_name", "type": "STRING"},
            {"name": "site_code", "type": "STRING"},
            {"name": "job_grade", "type": "STRING"},
            {"name": "aht", "type": "FLOAT64"},
            {"name": "adherence", "type": "FLOAT64"},
            {"name": "volume", "type": "INT64"},
            {"name": "avg_qa_pct", "type": "FLOAT64"},
            {"name": "certified_skills", "type": "INT64"},
            {"name": "aht_pctile", "type": "FLOAT64"},
            {"name": "qa_quartile", "type": "INT64"},
        ],
        "vw_attrition_risk": [
            {"name": "agent_sk", "type": "INT64"},
            {"name": "agent_id", "type": "INT64"},
            {"name": "full_name", "type": "STRING"},
            {"name": "site_code", "type": "STRING"},
            {"name": "adherence_90d", "type": "FLOAT64"},
            {"name": "notice_events", "type": "INT64"},
            {"name": "attrition_risk", "type": "STRING"},
        ],
        "vw_queue_sla_attainment": [
            {"name": "queue_code", "type": "STRING"},
            {"name": "media_type", "type": "STRING"},
            {"name": "date_key", "type": "INT64"},
            {"name": "sl_pct", "type": "FLOAT64"},
            {"name": "sl_target", "type": "NUMERIC", "scale": 4},
            {"name": "attainment", "type": "STRING"},
        ],
        "vw_first_contact_resolution": [
            {"name": "date_key", "type": "INT64"},
            {"name": "program_sk", "type": "INT64"},
            {"name": "resolved_interactions", "type": "INT64"},
            {"name": "fcr_count", "type": "INT64"},
            {"name": "fcr_pct", "type": "FLOAT64"},
        ],
        "vw_occupancy_utilization": [
            {"name": "date_key", "type": "INT64"},
            {"name": "site_code", "type": "STRING"},
            {"name": "agent_sk", "type": "INT64"},
            {"name": "handle_seconds", "type": "INT64"},
            {"name": "ready_seconds", "type": "INT64"},
            {"name": "aux_seconds", "type": "INT64"},
            {"name": "occupancy_pct", "type": "FLOAT64"},
        ],
        "vw_shrinkage_analysis": [
            {"name": "date_key", "type": "INT64"},
            {"name": "site_code", "type": "STRING"},
            {"name": "scheduled_minutes", "type": "INT64"},
            {"name": "worked_minutes", "type": "INT64"},
            {"name": "shrinkage_minutes", "type": "INT64"},
            {"name": "shrinkage_pct", "type": "FLOAT64"},
            {"name": "schedules", "type": "INT64"},
            {"name": "overnight_shifts", "type": "INT64"},
        ],
        "vw_program_margin": [
            {"name": "period_month", "type": "STRING"},
            {"name": "client_sk", "type": "INT64"},
            {"name": "program_sk", "type": "INT64"},
            {"name": "billed_amount", "type": "NUMERIC", "scale": 2},
            {"name": "net_revenue", "type": "NUMERIC", "scale": 2},
            {"name": "est_labor_cost", "type": "FLOAT64"},
            {"name": "total_adjustments", "type": "NUMERIC", "scale": 2},
            {"name": "est_margin", "type": "FLOAT64"},
            {"name": "committed_min", "type": "NUMERIC", "scale": 2},
        ],
        "vw_client_executive_summary": [
            {"name": "client_code", "type": "STRING"},
            {"name": "client_name", "type": "STRING"},
            {"name": "period_month", "type": "STRING"},
            {"name": "program_code", "type": "STRING"},
            {"name": "interactions", "type": "INT64"},
            {"name": "avg_handle_seconds", "type": "NUMERIC", "scale": 2},
            {"name": "avg_csat", "type": "NUMERIC", "scale": 2},
            {"name": "pct_promoters", "type": "NUMERIC", "scale": 2},
            {"name": "pct_detractors", "type": "NUMERIC", "scale": 2},
            {"name": "billed_amount", "type": "NUMERIC", "scale": 2},
            {"name": "sla_credit_amount", "type": "NUMERIC", "scale": 2},
            {"name": "net_revenue", "type": "NUMERIC", "scale": 2},
            {"name": "open_tickets", "type": "INT64"},
            {"name": "sla_breached_tickets", "type": "INT64"},
        ],
    }

    specs = []
    for v in views:
        vname = v["name"]
        cols = view_columns.get(vname, [])
        specs.append({
            "table": vname,
            "expect_object_type": "VIEW",
            "columns": cols,
        })
    return specs


# ---------------------------------------------------------------------------
# ETL Control spec
# ---------------------------------------------------------------------------
def gen_etl_control_spec():
    tbl_specs = [
        {
            "table": "watermarks",
            "expect_object_type": "TABLE",
            "columns": [
                {"name": "source_table", "type": "STRING"},
                {"name": "last_value", "type": "STRING"},
                {"name": "last_run_ts", "type": "TIMESTAMP"},
                {"name": "run_id", "type": "STRING"},
            ],
        },
        {
            "table": "dq_results",
            "expect_object_type": "TABLE",
            "partition_by": "run_date",
            "columns": [
                {"name": "run_id", "type": "STRING"},
                {"name": "run_date", "type": "DATE"},
                {"name": "check_name", "type": "STRING"},
                {"name": "target_table", "type": "STRING"},
                {"name": "status", "type": "STRING"},
                {"name": "metric_value", "type": "FLOAT64"},
                {"name": "threshold", "type": "FLOAT64"},
                {"name": "detail_json", "type": "JSON"},
                {"name": "checked_at", "type": "TIMESTAMP"},
            ],
        },
        {
            "table": "job_audit",
            "expect_object_type": "TABLE",
            "partition_by": "run_date",
            "columns": [
                {"name": "job_id", "type": "STRING"},
                {"name": "dag_id", "type": "STRING"},
                {"name": "task_id", "type": "STRING"},
                {"name": "run_date", "type": "DATE"},
                {"name": "start_ts", "type": "TIMESTAMP"},
                {"name": "end_ts", "type": "TIMESTAMP"},
                {"name": "status", "type": "STRING"},
                {"name": "rows_affected", "type": "INT64"},
                {"name": "error_message", "type": "STRING"},
            ],
        },
    ]

    return {
        "name": "etl_control_schema_conformance",
        "description": "Schema conformance for _etl_control layer — 3 operational tables.",
        "connections": {
            "target": {"engine": "bigquery"},
        },
        "migration": {
            "steps": [
                {"kind": "ddl", "sql": "ddl/06-etl-control.sql"},
            ],
        },
        "suites": [
            {
                "pattern": "schema_conformance",
                "id": "etl-control-schema",
                "story_id": "NBCS-TARGET-SCHEMA",
                "target_dataset": "${BUILD_DATASET}",
                "expect_table_count": 3,
                "tables": tbl_specs,
            }
        ],
    }


# ---------------------------------------------------------------------------
# Queryability + Performance spec
# ---------------------------------------------------------------------------
def gen_queryability_perf_spec(tables, views):
    """Generate SELECT * LIMIT 0 smoke queries + A/B perf comparison queries."""
    queries = []

    # Dataset mapping for qualified table names
    dataset_map = {"staging": "staging", "ods": "ods", "dm": "dm"}

    # SELECT * LIMIT 0 for every table
    for t in tables:
        ds = dataset_map[t["db"]]
        qid = f"smoke-{ds}-{t['name']}"
        queries.append({
            "id": qid,
            "mode": "measure",
            "sql": f"SELECT * FROM `${{GCP_PROJECT}}.${{BUILD_DATASET}}.{t['name']}` LIMIT 0",
        })

    # SELECT * LIMIT 0 for every view
    for v in views:
        qid = f"smoke-dm-{v['name']}"
        queries.append({
            "id": qid,
            "mode": "measure",
            "sql": f"SELECT * FROM `${{GCP_PROJECT}}.${{BUILD_DATASET}}.{v['name']}` LIMIT 0",
        })

    # SELECT * LIMIT 0 for etl_control tables
    for tbl in ["watermarks", "dq_results", "job_audit"]:
        queries.append({
            "id": f"smoke-etl-{tbl}",
            "mode": "measure",
            "sql": f"SELECT * FROM `${{GCP_PROJECT}}.${{BUILD_DATASET}}.{tbl}` LIMIT 0",
        })

    # 4 representative tier queries
    queries.append({
        "id": "tier-staging-group-by",
        "mode": "measure",
        "sql": "SELECT load_date, COUNT(*) AS cnt FROM `${GCP_PROJECT}.${BUILD_DATASET}.stg_crm_client` GROUP BY load_date",
    })
    queries.append({
        "id": "tier-ods-group-by",
        "mode": "measure",
        "sql": "SELECT snapshot_date, COUNT(*) AS cnt FROM `${GCP_PROJECT}.${BUILD_DATASET}.ods_program` GROUP BY snapshot_date",
    })
    queries.append({
        "id": "tier-dm-join",
        "mode": "measure",
        "sql": ("SELECT d.date_key, f.interaction_id "
                "FROM `${GCP_PROJECT}.${BUILD_DATASET}.fact_interaction` f "
                "JOIN `${GCP_PROJECT}.${BUILD_DATASET}.dim_date` d "
                "ON d.date_key = CAST(FORMAT_DATE('%Y%m%d', f.partition_date) AS INT64) LIMIT 0"),
    })
    queries.append({
        "id": "tier-etl-control-select",
        "mode": "measure",
        "sql": "SELECT run_date, check_name, status FROM `${GCP_PROJECT}.${BUILD_DATASET}.dq_results` LIMIT 0",
    })

    # Performance compare queries (AC10)
    perf_queries = [
        {
            "id": "perf-fact-interaction-partition-prune",
            "mode": "compare",
            "a": {
                "sql": "SELECT COUNT(*) FROM `${GCP_PROJECT}.${BUILD_DATASET}.fact_interaction`",
                "dry_run": True,
            },
            "b": {
                "sql": ("SELECT COUNT(*) FROM `${GCP_PROJECT}.${BUILD_DATASET}.fact_interaction` "
                        "WHERE partition_date = DATE '2026-01-15' AND channel = 'VOICE'"),
                "dry_run": True,
            },
            "compare": {"bytes_scanned": "b <= a"},
        },
        {
            "id": "perf-stg-tel-call-cluster-prune",
            "mode": "compare",
            "a": {
                "sql": "SELECT COUNT(*) FROM `${GCP_PROJECT}.${BUILD_DATASET}.stg_tel_call`",
                "dry_run": True,
            },
            "b": {
                "sql": ("SELECT COUNT(*) FROM `${GCP_PROJECT}.${BUILD_DATASET}.stg_tel_call` "
                        "WHERE call_id = 12345"),
                "dry_run": True,
            },
            "compare": {"bytes_scanned": "b <= a"},
        },
        {
            "id": "perf-agg-agent-daily-partition-prune",
            "mode": "compare",
            "a": {
                "sql": "SELECT COUNT(*) FROM `${GCP_PROJECT}.${BUILD_DATASET}.agg_agent_daily`",
                "dry_run": True,
            },
            "b": {
                "sql": ("SELECT COUNT(*) FROM `${GCP_PROJECT}.${BUILD_DATASET}.agg_agent_daily` "
                        "WHERE partition_date = DATE '2026-01-15'"),
                "dry_run": True,
            },
            "compare": {"bytes_scanned": "b <= a"},
        },
    ]

    return {
        "name": "queryability_and_performance",
        "description": "AC6: queryability smoke (SELECT * LIMIT 0 on all 118 objects + 4 tier queries). AC10: partition/cluster pruning perf.",
        "connections": {
            "target": {"engine": "bigquery"},
        },
        "migration": {
            "steps": [
                {"kind": "ddl", "sql": "ddl/02-staging.sql"},
                {"kind": "ddl", "sql": "ddl/03-ods.sql"},
                {"kind": "ddl", "sql": "ddl/04-dm-tables.sql"},
                {"kind": "ddl", "sql": "ddl/05-dm-views.sql"},
                {"kind": "ddl", "sql": "ddl/06-etl-control.sql"},
            ],
        },
        "suites": [
            {
                "pattern": "query_performance",
                "id": "queryability-smoke",
                "story_id": "NBCS-QUERYABILITY",
                "queries": queries,
            },
            {
                "pattern": "query_performance",
                "id": "partition-cluster-perf",
                "story_id": "NBCS-PERF",
                "queries": perf_queries,
            },
        ],
    }


# ---------------------------------------------------------------------------
# Custom YAML representer for clean output
# ---------------------------------------------------------------------------
class CleanDumper(yaml.SafeDumper):
    pass


def _str_representer(dumper, data):
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    if any(c in data for c in '{}[],:&*#?|-<>=!%@`') or data.startswith('$'):
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


CleanDumper.add_representer(str, _str_representer)


def dump_yaml(data):
    return yaml.dump(data, Dumper=CleanDumper, default_flow_style=False,
                     sort_keys=False, width=120, allow_unicode=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tables, views = load_manifest()

    specs = {
        "staging_conformance.mvs.yaml": gen_staging_spec(tables),
        "ods_conformance.mvs.yaml": gen_ods_spec(tables),
        "dm_conformance.mvs.yaml": gen_dm_spec(tables, views),
        "etl_control_conformance.mvs.yaml": gen_etl_control_spec(),
        "queryability_and_perf.mvs.yaml": gen_queryability_perf_spec(tables, views),
    }

    for fname, spec in specs.items():
        path = OUTPUT_DIR / fname
        path.write_text(dump_yaml(spec))
        print(f"  wrote {fname}")

    # Validation: count tables/views declared
    total_tables_declared = 0
    total_views_declared = 0
    for fname, spec in specs.items():
        for suite in spec.get("suites", []):
            if suite["pattern"] == "schema_conformance":
                for t in suite.get("tables", []):
                    ot = t.get("expect_object_type", "TABLE")
                    if ot == "VIEW":
                        total_views_declared += 1
                    else:
                        total_tables_declared += 1

    print(f"\nSchema specs declare {total_tables_declared} tables + {total_views_declared} views")
    if total_tables_declared != 103:
        print(f"ERROR: Expected 103 tables (100+3), got {total_tables_declared}")
        sys.exit(1)
    if total_views_declared != 15:
        print(f"ERROR: Expected 15 views, got {total_views_declared}")
        sys.exit(1)

    # Count smoke queries
    perf_spec = specs["queryability_and_perf.mvs.yaml"]
    smoke_suite = [s for s in perf_spec["suites"] if s["id"] == "queryability-smoke"][0]
    n_queries = len(smoke_suite["queries"])
    print(f"Queryability smoke: {n_queries} queries (100 tables + 15 views + 3 etl = 118 smoke + 4 tier = 122)")

    print("All specs generated successfully.")


if __name__ == "__main__":
    main()
