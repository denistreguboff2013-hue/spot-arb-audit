#!/usr/bin/env python3
"""Astra A5 offline 160-field ledger: strict RESEARCH vs deployed evidence.

Pass the original dated R130 CSV and output directory. No server/service/API
access, no private data, no YAML value export and no mutation of operator YAML.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
SCHEMA = HERE / "native_field_schema.json"
LEGACY = HERE / "native_trading.yaml"
CANDIDATE = HERE / "native_trading_operator_baseline_r64.yaml"
OVERLAYS = (
    HERE / "native_field_evidence_r132_paper_temporal_review.json",
    HERE / "native_field_evidence_r133_paper_exit_target_review.json",
    HERE / "native_field_evidence_r142_analytics_ai_review.json",
)
TOTAL = 160
R142 = {
    "analytics.enabled", "analytics.mode", "analytics.database_path",
    "analytics.mirror_database_path", "analytics.max_db_mb",
    "analytics.max_wal_mb", "analytics.raw_retention_days",
}


def require(ok, why):
    if not ok:
        raise ValueError(why)


def leaves(node, prefix=""):
    require(isinstance(node, dict), "expected YAML mapping")
    result = {}
    for key, value in node.items():
        require(isinstance(key, str), "YAML key is not string")
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            result.update(leaves(value, path))
        else:
            result[path] = value
    return result


class StrictYamlLoader(yaml.SafeLoader):
    """Audit YAML forbids duplicate mapping keys hidden by yaml.safe_load."""


def unique_mapping(loader, node):
    seen = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node)
        require(isinstance(key, (str, int, bool)) and key not in seen,
                "duplicate/non-scalar YAML mapping key")
        seen.add(key)
    return loader.construct_mapping(node)


StrictYamlLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping)


def schema_type_ok(kind, value):
    return {
        "boolean": lambda x: type(x) is bool,
        "integer": lambda x: type(x) is int,
        "number": lambda x: type(x) in (int, float),
        "string": lambda x: isinstance(x, str),
        "sequence": lambda x: isinstance(x, list),
    }[kind](value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-csv", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    with args.baseline_csv.open(encoding="utf-8", newline="") as f:
        original = list(csv.DictReader(f))
    require(len(original) == TOTAL, "baseline not 160 fields")
    keys = [r["field"] for r in original]
    require(len(set(keys)) == TOTAL, "duplicate original field")
    require(all(r["active_binary_proven"] == "False" for r in original),
            "unexpected source claim about deployed binary")
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    require(set(schema) == set(keys), "original CSV and 160-field schema disagree")
    legacy = leaves(yaml.load(LEGACY.read_text(encoding="utf-8"), Loader=StrictYamlLoader))
    candidate = leaves(yaml.load(CANDIDATE.read_text(encoding="utf-8"), Loader=StrictYamlLoader))
    missing_legacy = sorted(set(keys) - set(legacy))
    missing_candidate = sorted(set(keys) - set(candidate))
    require(not missing_legacy and not missing_candidate,
            f"fields absent from YAML: legacy={missing_legacy}, candidate={missing_candidate}")
    for label, values in (("legacy", legacy), ("candidate", candidate)):
        bad = [k for k in keys if not schema_type_ok(schema[k], values[k])]
        require(not bad, f"{label} YAML original schema type mismatch: {bad}")
    original_research = {r["field"] for r in original
                         if r["research_consumer_claimed"] == "True"}
    provenance = {f: ["R130_BASELINE"] for f in original_research}
    covered = set(original_research)
    for path in OVERLAYS:
        claim = json.loads(path.read_text(encoding="utf-8"))
        require(claim.get("active_binary_proven") is False
                and claim.get("real_order_authority") is False,
                f"unsafe permission claim in {path.name}")
        require(claim.get("scope", "").startswith("rust_research_"),
                f"overlay not research-only: {path.name}")
        fields = claim["fields"]
        require(len(fields) == len(set(fields)), f"duplicate within {path.name}")
        require(set(fields) <= set(keys), f"nonoriginal field in {path.name}")
        if "r142" in path.name:
            require(set(fields) == R142, "R142 exact seven-key analytics set changed")
            require(claim.get("sqlite_writer_running_proven") is False
                    and claim.get("original_exchange_authenticated") is False,
                    "R142 overstated source truth")
        for name in fields:
            provenance.setdefault(name, []).append(path.name)
        covered.update(fields)
    prior = set(original_research)
    for path in OVERLAYS[:-1]:
        prior.update(json.loads(path.read_text(encoding="utf-8"))["fields"])
    require(len(prior) == 36, f"historical A5 36-field baseline drift: {len(prior)}")
    require(not (R142 & prior), "R142 already included in historical 36, double count")
    require(len(covered) == 43 and len(set(keys) - covered) == 117,
            f"unexpected 160-field ledger {len(covered)}")
    require(not any(r["active_binary_proven"] == "True" for r in original),
            "cannot claim deployed provenance from research overlays")

    groups = {}
    for r in original:
        category = r["priority_group"]
        summary = groups.setdefault(category, {"total": 0, "research": 0, "missing": 0})
        summary["total"] += 1
        summary["research" if r["field"] in covered else "missing"] += 1
    require(sum(g["total"] for g in groups.values()) == 160
            and sum(g["missing"] for g in groups.values()) == 117,
            "priority-group inventory no longer totals 160/117")
    rows = []
    for r in sorted(original, key=lambda item: item["field"]):
        name = r["field"]
        rows.append({
            "field": name,
            "schema_type": schema[name],
            "priority_group": r["priority_group"],
            "r130_status": r["original_source_status"],
            "research_evidence": "TESTED_RESEARCH_READONLY" if name in covered else "NOT_EVIDENCED",
            "research_sources": ";".join(provenance.get(name, [])),
            "installed_paper_a342540": "NOT_PROVEN",
            "real_execution_consumption": "NOT_PROVEN",
        })
    report = {
        "schema": "astra_a5_original_160_research_overlay_r142_v1",
        "original_field_count": TOTAL,
        "original_ledger_sha256": hashlib.sha256(args.baseline_csv.read_bytes()).hexdigest(),
        "legacy_yaml_sha256": hashlib.sha256(LEGACY.read_bytes()).hexdigest(),
        "operator_candidate_yaml_sha256": hashlib.sha256(CANDIDATE.read_bytes()).hexdigest(),
        "baseline_research_consumed": len(prior),
        "new_distinct_research_consumed": len(R142),
        "total_research_consumed": len(covered),
        "research_not_evidenced": TOTAL - len(covered),
        "priority_group_breakdown": groups,
        "deployed_paper_fields_proven": 0,
        "installed_paper_yaml_source_attested": False,
        "installed_binary_runtime_consumption_attested": False,
        "analytics_sqlite_writer_proven": False,
        "rust_ai_config_auto_apply_allowed": False,
        "real_execution_authorized": False,
        "legacy_yaml_additional_non_schema_paths": len(set(legacy) - set(keys)),
        "candidate_yaml_additional_non_schema_paths": len(set(candidate) - set(keys)),
        "fields": rows,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "ASTRA_A5_160_FIELD_R142_20261020.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    with (args.out / "ASTRA_A5_160_FIELD_R142_20261020.csv").open(
            "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    md = [
        "# Astra A5 — 160 original YAML keys / strict R142 evidence (2026-10-20)",
        "",
        "Offline research source cross-check, **NOT a runtime attestation**.",
        f"- Baseline research-covered: **{len(prior)}/160**",
        f"- New distinct R142 analytics typed consumers: **{len(R142)}**",
        f"- Updated strict research evidence: **{len(covered)}/160**",
        f"- No strict research consumer evidence: **{TOTAL-len(covered)}/160**",
        "- Proven in installed PAPER a342540: **0/160**",
        "- Real trading authorized: **NO**; installed YAML cutover: **NOT PROVEN**",
        "- Rust analytics config parsing and read-only AI report are NOT SQLite",
        "  writer, mirror, retention, paper runtime, or REAL strategy parity.",
        "",
        "## Original Astra R130 groups, after R142 strict evidence",
        "",
        "| Group | Total | Research | Still missing |",
        "|---|---:|---:|---:|",
        *[f"| {group} | {counts['total']} | {counts['research']} | {counts['missing']} |"
           for group, counts in sorted(groups.items())],
        "",
        "## All 160 original fields",
        "",
        "| Field | Research | Sources | Installed PAPER |",
        "|---|---|---|---|",
    ]
    for row in rows:
        md.append(f"| {row['field']} | {row['research_evidence']} | "
                  f"{row['research_sources'] or '—'} | NOT PROVEN |")
    (args.out / "ASTRA_A5_160_FIELD_R142_20261020.md").write_text(
        "\n".join(md) + "\n", encoding="utf-8")
    print(f"A5_R142 PASS: total={TOTAL}, prior={len(prior)}, new={len(R142)}, "
          f"research={len(covered)}, no_evidence={TOTAL-len(covered)}, deployed=0")
    print("No operator YAML, installed service, DB or APIs modified.")


if __name__ == "__main__":
    main()
