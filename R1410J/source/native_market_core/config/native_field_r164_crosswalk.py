#!/usr/bin/env python3
"""R164: offline source-vs-160-ledger crosswalk. No live claims, no writes."""
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
EVIDENCE = Path("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018")
SOURCE = HERE / "native_trading.yaml"
BASELINE = HERE / "native_trading_operator_baseline_r64.yaml"
SCHEMA = HERE / "native_field_schema.json"
R148 = EVIDENCE / "ASTRA_A5_160_FIELD_R148_20261020.json"
R149 = EVIDENCE / "ASTRA_A5_160_FIELD_R149_20261020.json"
CSV = EVIDENCE / "ASTRA_A5_160_FIELD_R149_20261020.csv"
OVERLAY = HERE / "native_field_evidence_r149_entry_telegram_review.json"
VALID_GROUPS = {
    "ENTRY_RISK_AND_MARKET": (31, 30, 1),
    "SAFETY_AND_RECOVERY": (53, 53, 0),
    "OPTIONAL_TELEMETRY_AND_REPORTING": (76, 24, 52),
}
def require(condition, problem):
    if not condition:
        raise ValueError("R164 denial: " + problem)

def sha(data):
    return hashlib.sha256(data).hexdigest()

def leaf_paths(tree, prefix=""):
    require(isinstance(tree, dict), "YAML mapping not a mapping")
    flat = {}
    for key, item in tree.items():
        require(isinstance(key, str) and key and "." not in key,
                "invalid YAML path component")
        full = prefix + "." + key if prefix else key
        if isinstance(item, dict):
            flat.update(leaf_paths(item, full))
        else:
            flat[full] = item
    return flat

def valid_type(kind, val):
    if kind == "boolean":
        return type(val) is bool
    if kind == "integer":
        return type(val) is int
    if kind == "number":
        return type(val) in (int, float)
    if kind == "string":
        return type(val) is str
    if kind == "sequence":
        return type(val) is list
    return False

def verify(raw_source, raw_baseline, raw_schema, raw_r148, raw_r149,
           raw_csv, raw_overlay, evidence_dir=HERE):
    a = yaml.safe_load(raw_source)
    b = yaml.safe_load(raw_baseline)
    fields = json.loads(raw_schema)
    parent = json.loads(raw_r148)
    ledger = json.loads(raw_r149)
    overlay = json.loads(raw_overlay)
    current = leaf_paths(a)
    baseline = leaf_paths(b)
    require(isinstance(fields, dict) and len(fields) == 160, "schema count")
    require(len(current) == 160 and len(baseline) == 160,
            "original YAML key count")
    require(set(fields) == set(current) == set(baseline),
            "source, baseline and schema paths differ")
    for name, kind in fields.items():
        require(valid_type(kind, current[name]),
                "source syntax type " + name)
        require(valid_type(kind, baseline[name]),
                "baseline syntax type " + name)
    require(ledger.get("schema") ==
            "astra_a5_original_160_research_overlay_r149_v1",
            "cumulative ledger schema")
    require(ledger.get("original_field_count") == 160, "cumulative ledger field count")
    require(parent.get("schema") ==
            "astra_a5_original_160_research_overlay_r148_v1",
            "prior R148 ledger schema")
    require(ledger.get("r148_parent_sha256") == sha(raw_r148),
            "R148 parent chain checksum mismatch")
    require(ledger.get("r149_overlay_sha256") == sha(raw_overlay),
            "R149 overlay checksum mismatch")
    require(ledger.get("previous_research_consumed") == 98 and
            ledger.get("new_distinct_research_consumed") == 9 and
            ledger.get("total_research_consumed") == 107 and
            ledger.get("research_not_evidenced") == 53 and
            ledger.get("deployed_paper_fields_proven") == 0,
            "R149 totals unsupported")
    require(ledger.get("real_execution_authorized") is False and
            ledger.get("installed_paper_source_attested") is False and
            ledger.get("telegram_delivered") is False,
            "R149 unsupported runtime authority")
    rows = ledger.get("fields", [])
    require(len(rows) == 160 and len({r.get("field") for r in rows}) == 160
            and {r["field"] for r in rows} == set(fields),
            "duplicate, missing, or invented ledger path")
    csv_rows = list(csv.DictReader(raw_csv.decode("utf-8-sig").splitlines()))
    require(len(csv_rows) == 160, "R149 CSV count")
    require(csv_rows == [{k: str(v) for k, v in row.items()}
                         for row in rows], "R149 CSV is not same evidence as JSON")
    groups = Counter()
    evidence = Counter()
    unproven = []
    for row in rows:
        name = row["field"]
        require(row["schema_type"] == fields[name],
                "ledger syntax diverged " + name)
        require(row["installed_paper_a342540"] == "NOT_PROVEN" and
                row["real_execution_consumption"] == "NOT_PROVEN",
                "false installed or real claim " + name)
        group = row["priority_group"]
        require(group in VALID_GROUPS, "unknown priority group")
        status = row["research_evidence"]
        require(status in ("TESTED_RESEARCH_READONLY", "NOT_EVIDENCED"),
                "unknown research scope")
        groups[(group, "total")] += 1
        if status == "TESTED_RESEARCH_READONLY":
            groups[(group, "research")] += 1
            evidence[row["research_sources"]] += 1
            require(bool(row["research_sources"]), "credited without source " + name)
            origin = row["research_sources"].split(" (", 1)[0]
            if origin.startswith("native_field_evidence_"):
                claim_path = evidence_dir / origin
                require(claim_path.is_file(), "missing source claim " + origin)
                claim = json.loads(claim_path.read_bytes())
                require(name in claim.get("fields", []),
                        "unsupported source claim " + name)
                require(claim.get("active_binary_proven") is False,
                        "fake installed evidence " + origin)
        else:
            groups[(group, "missing")] += 1
            unproven.append(name)
    for group, (total, research, missing) in VALID_GROUPS.items():
        require(tuple(groups[(group, kind)] for kind in
                      ("total", "research", "missing")) ==
                (total, research, missing),
                "group drift " + group)
    require(len(unproven) == 53 and
            "spread.history_window" in unproven,
            "unproven original history_window was falsely credited")
    require(parent.get("total_research_consumed") == 98 and
            parent.get("deployed_paper_fields_proven") == 0,
            "R148 parent count/installation altered")
    require(set(overlay.get("fields", [])) ==
            {r["field"] for r in rows if
             r["research_sources"].startswith(
                 "native_field_evidence_r149_entry_telegram_review")},
            "R149 overlay credit mismatch")
    require(len(overlay.get("fields", [])) == 9, "R149 overlay count")

    changed = [key for key in sorted(fields) if current[key] != baseline[key]]
    flags = {
        "source_native_real_authorized":
            a["native_runtime"]["real_execution_authorized"],
        "source_native_production_cutover_authorized":
            a["native_runtime"]["production_cutover_authorized"],
        "source_native_mutation_capability":
            a["native_runtime"]["mutation_capability_enabled"],
        "source_general_trade_enabled":
            a["general"]["trade_enabled"],
        "source_paper_entries_enabled":
            a["general"]["paper_entries_enabled"],
        "source_real_trial_enabled":
            a["real_trial"]["enabled"],
        "source_real_trial_operator_approved":
            a["real_trial"]["operator_approved"],
        "source_real_trial_autonomous_enabled":
            a["real_trial"]["autonomous_real_enabled"],
        "source_paper_daily_limit":a["limits"]["paper_max_trades_per_day"],
        "source_analytics_enabled":a["analytics"]["enabled"],
        "source_scale_in_enabled":a["scale_in_hints"]["enabled"],
    }
    require(all(flags[k] is False for k in (
        "source_native_real_authorized",
        "source_native_production_cutover_authorized",
        "source_native_mutation_capability")),
        "native REAL safety toggles changed")
    return {
        "schema":"ASTRA/P1-06/R164/SOURCE-CHAIN-CROSSWALK/V1",
        "scope":"read_only_source_yaml_and_offline_research_evidence",
        "original_160_paths_identical_in_both_yaml_and_schema":True,
        "original_field_count":160,
        "yaml_source_sha256":sha(raw_source),
        "operator_baseline_sha256":sha(raw_baseline),
        "cumulative_ledger_sha256":sha(raw_r149),
        "parent_r148_sha256":sha(raw_r148),
        "baseline_value_changed_paths":changed,
        "baseline_value_changed_count":len(changed),
        "research_supported_claims":107,
        "research_unproven":53,
        "research_unproven_paths":sorted(unproven),
        "source_evidence_groups":dict(sorted(evidence.items())),
        "installed_paper_fields_proven":0,
        "real_trading_authorized":False,
        "source_flags":flags,
        "limitations":[
            "Counts are evidence-ledger claims, NOT runtime execution",
            "R130_BASELINE claims are not individually re-proved here",
            "No supervisor PID or installed ELF SHA checked",
            "No original YAML installed application proved",
            "Original real_trial flags are inert for Rust REAL authorization",
            "Unsupported analytics and scale_in_hints still block opt-in PAPER"
        ]
    }

def load():
    return verify(SOURCE.read_bytes(), BASELINE.read_bytes(),
                  SCHEMA.read_bytes(), R148.read_bytes(),
                  R149.read_bytes(), CSV.read_bytes(), OVERLAY.read_bytes(),
                  evidence_dir=HERE)

if __name__ == "__main__":
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument("--offline-only",action="store_true",required=True)
    args=parser.parse_args()
    print(json.dumps(load(), indent=2, ensure_ascii=False))
