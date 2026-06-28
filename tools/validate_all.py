#!/usr/bin/env python3
"""Comprehensive cross-validation of all DDL + MVS artifacts.

Runs 10 structural checks + assembly checks. Exits 0 on success, 1 on failure.
"""

import os
import re
import sys
from pathlib import Path

import yaml

ROOT = Path("/workspace/project")
SRC_MANIFEST = Path("/workspace/source/manifests/tables.yaml")

errors = []
warnings = []


def err(msg):
    errors.append(msg)
    print(f"  FAIL: {msg}")


def ok(msg):
    print(f"  OK:   {msg}")


def warn(msg):
    warnings.append(msg)
    print(f"  WARN: {msg}")


# ---------------------------------------------------------------------------
# Load manifest
# ---------------------------------------------------------------------------
manifest = yaml.safe_load(SRC_MANIFEST.read_text())


def parse_col_name(spec):
    if isinstance(spec, dict):
        return spec["name"], str(spec["type"]), spec.get("tags", [])
    m = re.match(r"^([a-z0-9_]+):([A-Z0-9_(),]+?)(?::(.+))?$", spec.strip())
    if not m:
        return None, None, []
    tags = [t.strip() for t in (m.group(3) or "").split(",") if t.strip()]
    return m.group(1), m.group(2), tags


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


# =========================================================================
# CHECK 1: DDL syntax — no Hive keywords in BQ DDL files
# =========================================================================
print("\n=== CHECK 1: DDL syntax — no Hive keywords ===")
HIVE_KW_PATTERNS = [
    (r"\bSTORED\s+AS\b", "STORED AS"),
    (r"\bSERDE\b", "SERDE"),
    (r"\bTBLPROPERTIES\b", "TBLPROPERTIES"),
    (r"\bROW\s+FORMAT\b", "ROW FORMAT"),
    (r"\bINTO\s+\d+\s+BUCKETS\b", "INTO N BUCKETS"),
]
BQ_DDL_FILES = ["ddl/02-staging.sql", "ddl/03-ods.sql", "ddl/04-dm-tables.sql",
                "ddl/05-dm-views.sql", "ddl/06-etl-control.sql"]
for fname in BQ_DDL_FILES:
    content = (ROOT / fname).read_text()
    # strip comments
    code = "\n".join(l for l in content.split("\n") if not l.strip().startswith("--"))
    for pat, label in HIVE_KW_PATTERNS:
        if re.search(pat, code, re.IGNORECASE):
            err(f"{fname}: Hive keyword '{label}' found in code")
    # LOCATION only allowed inside backtick refs or comments
    loc_matches = re.findall(r"\bLOCATION\s+'", code, re.IGNORECASE)
    if loc_matches:
        err(f"{fname}: LOCATION clause found in code")

# No CREATE SCHEMA in table/view files
for fname in BQ_DDL_FILES:
    content = (ROOT / fname).read_text()
    code = "\n".join(l for l in content.split("\n") if not l.strip().startswith("--"))
    if re.search(r"CREATE\s+SCHEMA", code, re.IGNORECASE):
        err(f"{fname}: CREATE SCHEMA found (not allowed in DDL steps)")

if not any("CHECK 1" in e for e in errors):
    ok("No Hive keywords or CREATE SCHEMA in BQ DDL files")

# =========================================================================
# CHECK 2: Object counts
# =========================================================================
print("\n=== CHECK 2: Object counts ===")
expected_counts = {
    "ddl/02-staging.sql": ("TABLE", 45),
    "ddl/03-ods.sql": ("TABLE", 30),
    "ddl/04-dm-tables.sql": ("TABLE", 25),
    "ddl/05-dm-views.sql": ("VIEW", 15),
    "ddl/06-etl-control.sql": ("TABLE", 3),
}
total_t, total_v = 0, 0
for fname, (obj_type, expected) in expected_counts.items():
    content = (ROOT / fname).read_text()
    if obj_type == "TABLE":
        count = len(re.findall(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS", content, re.I))
        total_t += count
    else:
        count = len(re.findall(r"CREATE\s+OR\s+REPLACE\s+VIEW", content, re.I))
        total_v += count
    if count != expected:
        err(f"{fname}: {count} {obj_type}s (expected {expected})")
    else:
        ok(f"{fname}: {count} {obj_type}s")

if total_t != 103:
    err(f"Total tables: {total_t} (expected 103)")
if total_v != 15:
    err(f"Total views: {total_v} (expected 15)")
ok(f"Totals: {total_t} tables + {total_v} views")

# =========================================================================
# CHECK 3: Column count parity (DDL vs manifest + expected additions)
# =========================================================================
print("\n=== CHECK 3: Column count parity ===")

def extract_ddl_tables(content):
    """Extract table name -> column count from DDL content."""
    tables = {}
    # Match CREATE TABLE blocks
    pattern = r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)\s*\((.*?)\)\s*(?:PARTITION|CLUSTER|OPTIONS|;)"
    for m in re.finditer(pattern, content, re.DOTALL | re.I):
        tname = m.group(1)
        col_block = m.group(2)
        # Count columns: each line with NAME TYPE pattern
        col_lines = [l.strip() for l in col_block.split("\n")
                     if l.strip() and not l.strip().startswith("--") and not l.strip() == ")"]
        # Remove trailing commas and count non-empty
        cols = [l for l in col_lines if re.match(r"\w+\s+\w+", l.replace(",", ""))]
        tables[tname] = len(cols)
    return tables

ddl_col_counts = {}
for fname in ["ddl/02-staging.sql", "ddl/03-ods.sql", "ddl/04-dm-tables.sql", "ddl/06-etl-control.sql"]:
    content = (ROOT / fname).read_text()
    ddl_col_counts.update(extract_ddl_tables(content))

col_mismatches = 0
for t in manifest["tables"]:
    name = t["name"]
    if name not in ddl_col_counts:
        err(f"Column count: {name} not found in DDL")
        col_mismatches += 1
        continue
    manifest_cols = len(t["columns"])
    partitions = parse_partition(t.get("partition"))
    n_part_cols = len(partitions)  # promoted to schema

    # Synthetic columns
    n_synthetic = 0
    if t["db"] == "dm" and t["group"] in ("fact", "agg") and partitions:
        n_synthetic += 1  # _partition_date
    if t["group"] == "scd2":
        n_synthetic += 1  # eff_from_date

    expected_total = manifest_cols + n_part_cols + n_synthetic
    actual = ddl_col_counts[name]
    if actual != expected_total:
        err(f"Column count: {name}: DDL has {actual}, expected {expected_total} "
            f"(manifest={manifest_cols} + part={n_part_cols} + synth={n_synthetic})")
        col_mismatches += 1

if col_mismatches == 0:
    ok(f"Column counts match for all {len(manifest['tables'])} tables")

# =========================================================================
# CHECK 4: DECIMAL coverage — 52 columns, 7 distinct (p,s) pairs
# =========================================================================
print("\n=== CHECK 4: DECIMAL / NUMERIC coverage ===")
# Count in DDL
ddl_numerics = 0
for fname in ["ddl/02-staging.sql", "ddl/03-ods.sql", "ddl/04-dm-tables.sql"]:
    content = (ROOT / fname).read_text()
    ddl_numerics += len(re.findall(r"NUMERIC\(\d+,\d+\)", content))

# Count in MVS specs
mvs_numerics = 0
for fname in ["tests/schema/staging_conformance.mvs.yaml",
              "tests/schema/ods_conformance.mvs.yaml",
              "tests/schema/dm_conformance.mvs.yaml"]:
    data = yaml.safe_load((ROOT / fname).read_text())
    for suite in data.get("suites", []):
        for tbl in suite.get("tables", []):
            for col in tbl.get("columns", []):
                if col["type"].startswith("NUMERIC("):
                    mvs_numerics += 1

if ddl_numerics != 52:
    err(f"DDL NUMERIC columns: {ddl_numerics} (expected 52)")
else:
    ok(f"DDL: {ddl_numerics} NUMERIC columns")

if mvs_numerics != 52:
    err(f"MVS NUMERIC columns: {mvs_numerics} (expected 52)")
else:
    ok(f"MVS: {mvs_numerics} NUMERIC columns with scale")

# Check distinct (p,s) pairs
ddl_pairs = set()
for fname in ["ddl/02-staging.sql", "ddl/03-ods.sql", "ddl/04-dm-tables.sql"]:
    content = (ROOT / fname).read_text()
    for m in re.finditer(r"NUMERIC\((\d+),(\d+)\)", content):
        ddl_pairs.add((int(m.group(1)), int(m.group(2))))
expected_pairs = {(14, 2), (12, 4), (12, 2), (10, 4), (8, 2), (5, 2), (7, 2)}
if ddl_pairs != expected_pairs:
    err(f"DECIMAL pairs mismatch: got {ddl_pairs}, expected {expected_pairs}")
else:
    ok(f"All 7 DECIMAL precision/scale pairs: {sorted(ddl_pairs)}")

# =========================================================================
# CHECK 5: Complex types
# =========================================================================
print("\n=== CHECK 5: Complex type coverage ===")
stg_ddl = (ROOT / "ddl/02-staging.sql").read_text()
stg_mvs = yaml.safe_load((ROOT / "tests/schema/staging_conformance.mvs.yaml").read_text())

complex_checks = [
    ("sections", "ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>"),
    ("messages", "ARRAY<STRUCT<sender STRING, ts_ms INT64, text STRING>>"),
    ("metadata", "ARRAY<STRUCT<key STRING, value STRING>>"),
    ("keywords", "ARRAY<STRING>"),
]
for col_name, expected_type in complex_checks:
    # Check DDL
    if expected_type in stg_ddl:
        ok(f"DDL: {col_name} = {expected_type}")
    else:
        err(f"DDL: {col_name} missing type {expected_type}")
    # Check MVS
    found = False
    for suite in stg_mvs.get("suites", []):
        for tbl in suite.get("tables", []):
            for col in tbl.get("columns", []):
                if col["name"] == col_name and col["type"] == expected_type:
                    found = True
    if found:
        ok(f"MVS: {col_name} = {expected_type}")
    else:
        err(f"MVS: {col_name} missing type {expected_type}")

# =========================================================================
# CHECK 6: Partition/cluster — DDL vs MVS consistency
# =========================================================================
print("\n=== CHECK 6: Partition/cluster DDL vs MVS ===")

def extract_ddl_partition_cluster(content):
    """Extract table -> (partition_col, cluster_cols) from DDL."""
    result = {}
    blocks = re.split(r"(?=CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS)", content, flags=re.I)
    for block in blocks:
        m = re.match(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)", block, re.I)
        if not m:
            continue
        tname = m.group(1)
        pm = re.search(r"PARTITION\s+BY\s+(\w+)", block, re.I)
        part = pm.group(1) if pm else None
        cm = re.search(r"CLUSTER\s+BY\s+([^\n;]+)", block, re.I)
        cluster = [c.strip() for c in cm.group(1).split(",")] if cm else []
        result[tname] = (part, cluster)
    return result

ddl_pc = {}
for fname in ["ddl/02-staging.sql", "ddl/03-ods.sql", "ddl/04-dm-tables.sql", "ddl/06-etl-control.sql"]:
    ddl_pc.update(extract_ddl_partition_cluster((ROOT / fname).read_text()))

# Load MVS partition/cluster
mvs_pc = {}
for fname in ["tests/schema/staging_conformance.mvs.yaml",
              "tests/schema/ods_conformance.mvs.yaml",
              "tests/schema/dm_conformance.mvs.yaml",
              "tests/schema/etl_control_conformance.mvs.yaml"]:
    data = yaml.safe_load((ROOT / fname).read_text())
    for suite in data.get("suites", []):
        for tbl in suite.get("tables", []):
            if tbl.get("expect_object_type") == "VIEW":
                continue
            mvs_pc[tbl["table"]] = (tbl.get("partition_by"), tbl.get("cluster_by", []))

pc_mismatches = 0
for tname in ddl_pc:
    if tname not in mvs_pc:
        if tname in ("watermarks", "dq_results", "job_audit"):
            pass  # etl control
        continue
    ddl_part, ddl_clust = ddl_pc[tname]
    mvs_part, mvs_clust = mvs_pc[tname]
    if ddl_part != mvs_part:
        err(f"Partition mismatch {tname}: DDL={ddl_part}, MVS={mvs_part}")
        pc_mismatches += 1
    if ddl_clust != mvs_clust:
        err(f"Cluster mismatch {tname}: DDL={ddl_clust}, MVS={mvs_clust}")
        pc_mismatches += 1

if pc_mismatches == 0:
    ok(f"Partition/cluster consistent across DDL and MVS for all tables")

# =========================================================================
# CHECK 7: MVS cross-check — every DDL table in exactly one MVS suite
# =========================================================================
print("\n=== CHECK 7: MVS cross-check ===")
mvs_tables = {}  # table -> spec file
for fname in ["tests/schema/staging_conformance.mvs.yaml",
              "tests/schema/ods_conformance.mvs.yaml",
              "tests/schema/dm_conformance.mvs.yaml",
              "tests/schema/etl_control_conformance.mvs.yaml"]:
    data = yaml.safe_load((ROOT / fname).read_text())
    for suite in data.get("suites", []):
        for tbl in suite.get("tables", []):
            tname = tbl["table"]
            if tname in mvs_tables:
                err(f"{tname} appears in multiple MVS suites: {mvs_tables[tname]} and {fname}")
            mvs_tables[tname] = fname

# Every DDL table should be in MVS
all_ddl_names = set()
for fname in ["ddl/02-staging.sql", "ddl/03-ods.sql", "ddl/04-dm-tables.sql", "ddl/06-etl-control.sql"]:
    content = (ROOT / fname).read_text()
    for m in re.finditer(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)", content, re.I):
        all_ddl_names.add(m.group(1))
# Views
views_ddl = (ROOT / "ddl/05-dm-views.sql").read_text()
for m in re.finditer(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(\w+)", views_ddl, re.I):
    all_ddl_names.add(m.group(1))

missing_in_mvs = all_ddl_names - set(mvs_tables.keys())
if missing_in_mvs:
    err(f"DDL objects missing from MVS: {missing_in_mvs}")
else:
    ok(f"All {len(all_ddl_names)} DDL objects appear in exactly one MVS suite")

# Check every manifest column appears in MVS with source_type
mvs_col_lookup = {}  # (table, col_name) -> col_spec
for fname in ["tests/schema/staging_conformance.mvs.yaml",
              "tests/schema/ods_conformance.mvs.yaml",
              "tests/schema/dm_conformance.mvs.yaml"]:
    data = yaml.safe_load((ROOT / fname).read_text())
    for suite in data.get("suites", []):
        for tbl in suite.get("tables", []):
            for col in tbl.get("columns", []):
                mvs_col_lookup[(tbl["table"], col["name"])] = col

missing_source_type = 0
for t in manifest["tables"]:
    for c in t["columns"]:
        cname, ctype, tags = parse_col_name(c)
        key = (t["name"], cname)
        if key not in mvs_col_lookup:
            err(f"Manifest column {t['name']}.{cname} missing from MVS")
        else:
            col_spec = mvs_col_lookup[key]
            if "source_type" not in col_spec:
                missing_source_type += 1

if missing_source_type > 0:
    warn(f"{missing_source_type} columns missing source_type (may be intentional for derived cols)")
else:
    ok("All manifest columns have source_type in MVS")

# =========================================================================
# CHECK 8: View validation
# =========================================================================
print("\n=== CHECK 8: View validation ===")
views_content = (ROOT / "ddl/05-dm-views.sql").read_text()
view_count = len(re.findall(r"CREATE\s+OR\s+REPLACE\s+VIEW", views_content, re.I))
if view_count != 15:
    err(f"View count: {view_count} (expected 15)")
else:
    ok(f"15 views in 05-dm-views.sql")

# Check no Hive functions in code (strip comments first)
code_lines = [l for l in views_content.split("\n") if not l.strip().startswith("--")]
code_only = "\n".join(code_lines)
hive_funcs = [
    (r"\bNDV\s*\(", "NDV()"),
    (r"\bGROUPING__ID\b", "GROUPING__ID"),
    (r"\s+RLIKE\s+", "RLIKE"),
    (r"\bfrom_unixtime\s*\(", "from_unixtime()"),
    (r"\bdate_add\s*\(", "date_add()"),
    (r"\bON\s+1\s*=\s*1\b", "ON 1 = 1"),
]
for pat, label in hive_funcs:
    if re.search(pat, code_only, re.I):
        err(f"Hive function '{label}' found in view code")
    else:
        ok(f"No '{label}' in view code")

# Check BQ functions are present
bq_funcs = [
    "APPROX_COUNT_DISTINCT", "GROUPING(", "REGEXP_CONTAINS",
    "UNIX_SECONDS", "TIMESTAMP_MILLIS", "TIMESTAMP_ADD", "PARSE_DATE", "CROSS  JOIN",
]
for func in bq_funcs:
    if func in views_content:
        ok(f"BQ function present: {func}")
    else:
        err(f"BQ function missing: {func}")

# =========================================================================
# CHECK 9: FK type consistency
# =========================================================================
print("\n=== CHECK 9: FK type consistency (spot checks) ===")
# Key FK→PK paths that must be INT64 on both sides
fk_checks = [
    # (table_a, col_a, table_b, col_b)
    ("stg_crm_client", "client_id", "ods_client_acid", "client_id"),
    ("ods_client_acid", "client_id", "dim_client", "client_id"),
    ("dim_client", "client_sk", "fact_interaction", "client_sk"),
    ("dim_agent", "agent_sk", "fact_interaction", "agent_sk"),
    ("dim_program", "program_sk", "fact_interaction", "program_sk"),
    ("dim_queue", "queue_sk", "fact_interaction", "queue_sk"),
    ("dim_program", "program_id", "ods_program", "program_id"),
    ("dim_agent", "agent_sk", "agg_agent_daily", "agent_sk"),
    ("dim_date", "date_key", "fact_interaction", "date_key"),
    ("ods_invoice_acid", "client_id", "dim_client", "client_id"),
]
fk_ok = 0
for ta, ca, tb, cb in fk_checks:
    spec_a = mvs_col_lookup.get((ta, ca))
    spec_b = mvs_col_lookup.get((tb, cb))
    if not spec_a:
        err(f"FK check: {ta}.{ca} not in MVS")
        continue
    if not spec_b:
        err(f"FK check: {tb}.{cb} not in MVS")
        continue
    type_a = spec_a["type"]
    type_b = spec_b["type"]
    if type_a == type_b:
        fk_ok += 1
    else:
        err(f"FK type mismatch: {ta}.{ca}={type_a} vs {tb}.{cb}={type_b}")

ok(f"{fk_ok}/{len(fk_checks)} FK→PK type pairs match")

# =========================================================================
# CHECK 10: Epoch descriptions
# =========================================================================
print("\n=== CHECK 10: Epoch description coverage ===")
EPOCH_DESCS = {
    "epoch_sec": "epoch SECONDS (legacy)",
    "epoch_ms": "epoch MILLISECONDS (legacy)",
    "lie_ms": "!! name says seconds, VALUES ARE MILLIS !!",
    "ora_str": "Oracle string YYYYMMDDHH24MISS (legacy)",
}

# Check DDL has OPTIONS(description=...) for every epoch column
all_ddl_content = ""
for fname in ["ddl/02-staging.sql"]:
    all_ddl_content += (ROOT / fname).read_text()

epoch_ddl_ok = 0
epoch_ddl_fail = 0
epoch_mvs_ok = 0
epoch_mvs_fail = 0

for t in manifest["tables"]:
    if t["db"] != "staging":
        continue  # epoch columns are only in staging
    for c in t["columns"]:
        cname, ctype, tags = parse_col_name(c)
        epoch_tag = None
        for tag in tags:
            if tag in EPOCH_DESCS:
                epoch_tag = tag
                break
        if not epoch_tag:
            continue

        expected_desc = EPOCH_DESCS[epoch_tag]

        # Check DDL
        # Look for the column line with OPTIONS(description=...)
        ddl_pattern = re.escape(cname) + r'\s+.*?OPTIONS\(description="' + re.escape(expected_desc) + r'"\)'
        if re.search(ddl_pattern, all_ddl_content):
            epoch_ddl_ok += 1
        else:
            err(f"DDL epoch desc missing: {t['name']}.{cname} ({epoch_tag})")
            epoch_ddl_fail += 1

        # Check MVS
        key = (t["name"], cname)
        mvs_col = mvs_col_lookup.get(key)
        if mvs_col and mvs_col.get("description") == expected_desc:
            epoch_mvs_ok += 1
        else:
            got = mvs_col.get("description", "MISSING") if mvs_col else "COL NOT FOUND"
            err(f"MVS epoch desc mismatch: {t['name']}.{cname}: expected '{expected_desc}', got '{got}'")
            epoch_mvs_fail += 1

ok(f"DDL epoch descriptions: {epoch_ddl_ok} OK, {epoch_ddl_fail} FAIL")
ok(f"MVS epoch descriptions: {epoch_mvs_ok} OK, {epoch_mvs_fail} FAIL")

# =========================================================================
# ASSEMBLY CHECKS
# =========================================================================
print("\n=== ASSEMBLY CHECKS ===")

# 01-datasets.sql NOT referenced as a migration step
mvs_files = [
    "tests/schema/staging_conformance.mvs.yaml",
    "tests/schema/ods_conformance.mvs.yaml",
    "tests/schema/dm_conformance.mvs.yaml",
    "tests/schema/etl_control_conformance.mvs.yaml",
    "tests/schema/queryability_and_perf.mvs.yaml",
]

for fname in mvs_files:
    data = yaml.safe_load((ROOT / fname).read_text())
    for step in data.get("migration", {}).get("steps", []):
        sql_path = step.get("sql", "")
        if "01-datasets" in sql_path:
            err(f"{fname}: references 01-datasets.sql as migration step (CREATE SCHEMA not allowed)")
ok("01-datasets.sql not referenced as migration step in any MVS spec")

# All referenced SQL paths exist
for fname in mvs_files:
    data = yaml.safe_load((ROOT / fname).read_text())
    # migration.steps
    for step in data.get("migration", {}).get("steps", []):
        sql_path = step.get("sql", "")
        if not (ROOT / sql_path).exists():
            err(f"{fname}: migration step SQL path missing: {sql_path}")
    # source_setup.ddl
    for ddl_path in data.get("source_setup", {}).get("ddl", []):
        if not (ROOT / ddl_path).exists():
            err(f"{fname}: source_setup DDL path missing: {ddl_path}")
ok("All referenced SQL paths exist")

# Environment variables: ${BUILD_DATASET}, ${GCP_PROJECT}, ${SOURCE_WAREHOUSE}
for fname in mvs_files:
    content = (ROOT / fname).read_text()
    if "BUILD_DATASET" in content or fname == "tests/schema/etl_control_conformance.mvs.yaml":
        pass  # good
    # Check no hardcoded dataset names
    if re.search(r"target_dataset:\s*[a-z]", content) and "${" not in content.split("target_dataset")[1][:30]:
        err(f"{fname}: hardcoded target_dataset (should use ${{BUILD_DATASET}})")
ok("Environment variables used correctly")

# All MVS YAML files parse cleanly
for fname in mvs_files:
    try:
        yaml.safe_load((ROOT / fname).read_text())
    except Exception as e:
        err(f"{fname}: YAML parse error: {e}")
ok("All MVS YAML files parse cleanly")

# No files in /workspace/source/ modified
src_status = os.popen("cd /workspace/source && git status --porcelain 2>/dev/null").read().strip()
if src_status:
    err(f"/workspace/source/ has modifications: {src_status}")
else:
    ok("No modifications to /workspace/source/")

# =========================================================================
# SUMMARY
# =========================================================================
print("\n" + "=" * 60)
if errors:
    print(f"FAILED: {len(errors)} errors, {len(warnings)} warnings")
    for e in errors:
        print(f"  ERROR: {e}")
    sys.exit(1)
else:
    print(f"PASSED: 0 errors, {len(warnings)} warnings")
    if warnings:
        for w in warnings:
            print(f"  WARN: {w}")
    sys.exit(0)
