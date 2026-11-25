#!/usr/bin/env python3
"""Astra P1-06: exact 8-source read-only execution overlay on R143."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

from native_field_r143_incremental_audit import require

EXECUTION = {
    "execution.spot_entry_mode", "execution.fut_entry_mode",
    "execution.unwind_mode", "execution.spot_limit_ticks",
    "execution.spot_retry_n", "execution.spot_retry_ms",
    "execution.fut_retry_n", "execution.fut_retry_ms",
}


def audit(parent_path, claim_path, out):
    original = parent_path.read_bytes()
    prior = json.loads(original)
    claim_raw = claim_path.read_bytes()
    claim = json.loads(claim_raw)
    require(prior.get("schema") == "astra_a5_original_160_research_overlay_r143_v1"
            and prior.get("original_field_count") == 160
            and prior.get("total_research_consumed") == 45
            and prior.get("research_not_evidenced") == 115
            and prior.get("deployed_paper_fields_proven") == 0
            and prior.get("real_execution_authorized") is False
            and prior.get("durable_distributed_entry_lock_proven") is False,
            "R143 parent invalid: 45/115/0/REAL hard bound")
    require(claim.get("schema") == "R144_EIGHT_EXECUTION_ORIGINAL_YAML_SOURCE_OFFLINE_REVIEW_V1"
            and claim.get("scope") == "rust_research_readonly_only"
            and claim.get("active_binary_proven") is False
            and claim.get("real_order_authority") is False
            and claim.get("original_exchange_acknowledged") is False
            and claim.get("retry_after_unknown_ack_permitted") is False
            and claim.get("order_submission_permitted") is False
            and claim.get("installed_paper_source_attested") is False,
            "R144 cannot escalate local source to exchange/installed/REAL")
    names = claim.get("fields", [])
    require(len(names) == len(set(names)) and set(names) == EXECUTION,
            "unexpected, duplicate or missing execution YAML field")
    old = prior.get("fields", [])
    require(len(old) == 160 and len({r["field"] for r in old}) == 160
            and sum(r["research_evidence"] == "TESTED_RESEARCH_READONLY"
                    for r in old) == 45, "160/45 original ledger inconsistent")
    original_names = {r["field"]: r for r in old}
    require(all(n in original_names
                and original_names[n]["research_evidence"] == "NOT_EVIDENCED"
                and original_names[n]["priority_group"] == "SAFETY_AND_RECOVERY"
                for n in EXECUTION), "R144 field is already credited/nonoriginal")
    rows = []
    for orig in old:
        row = dict(orig)
        require(row.get("installed_paper_a342540") == "NOT_PROVEN"
                and row.get("real_execution_consumption") == "NOT_PROVEN",
                "prior field falsely claimed installed or REAL")
        if row["field"] in EXECUTION:
            row["research_evidence"] = "TESTED_RESEARCH_READONLY"
            row["research_sources"] = claim_path.name
        rows.append(row)
    groups = {}
    for r in rows:
        g = groups.setdefault(r["priority_group"],
            {"total": 0, "research": 0, "missing": 0})
        g["total"] += 1
        g["research" if r["research_evidence"] == "TESTED_RESEARCH_READONLY"
          else "missing"] += 1
    require(sum(g["total"] for g in groups.values()) == 160
            and sum(g["research"] for g in groups.values()) == 53
            and sum(g["missing"] for g in groups.values()) == 107
            and groups["SAFETY_AND_RECOVERY"] == {
                "total": 53, "research": 8, "missing": 45},
            "R144 53/160 and safety group 8/53 not proven")
    result = {
        "schema": "astra_a5_original_160_research_overlay_r144_v1",
        "r143_parent_sha256": hashlib.sha256(original).hexdigest(),
        "r144_overlay_sha256": hashlib.sha256(claim_raw).hexdigest(),
        "original_field_count": 160, "previous_research_consumed": 45,
        "new_distinct_research_consumed": 8, "total_research_consumed": 53,
        "research_not_evidenced": 107, "deployed_paper_fields_proven": 0,
        "installed_binary_runtime_consumption_attested": False,
        "real_execution_authorized": False,
        "unknown_ack_retry_authorized": False,
        "execution_source_timing_is_review_only": True,
        "priority_group_breakdown": groups,
        "fields": rows,
    }
    out.mkdir(parents=True, exist_ok=True)
    stem = "ASTRA_A5_160_FIELD_R144_20261020"
    (out / (stem + ".json")).write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    with (out / (stem + ".csv")).open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    md = [
        "# Astra P1-06 R144 — 160 YAML source-field evidence",
        "", "Strict offline research evidence, **not installed PAPER or REAL**.",
        "", "- R143 prior: **45/160**",
        "- R144 new source-bound execution review: **8**",
        "- New research covered: **53/160**",
        "- Without strict research evidence: **107/160**",
        "- Proven installed: **0/160**",
        "- UNKNOWN ACK resubmission permission: **NO**",
        "- This timing schedule does not submit orders or prove exchange truth.",
        "", "| Group | Total | Research | Missing |",
        "|---|---:|---:|---:|",
        *[f"| {k} | {v['total']} | {v['research']} | {v['missing']} |"
          for k, v in sorted(groups.items())],
        "", "## All original YAML fields", "",
        "| Source | Research | Installed PAPER |",
        "|---|---|---|",
        *[f"| {row['field']} | {row['research_evidence']} | NOT PROVEN |"
          for row in rows],
    ]
    (out / (stem + ".md")).write_text(
        "\n".join(md) + "\n", encoding="utf-8")
    print("A5_R144 PASS: original160 prior45+new8=53, missing107, "
          "safety=8/53, deployed=0, REAL=false")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--r143", required=True, type=Path)
    p.add_argument("--claim", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    args = p.parse_args()
    audit(args.r143, args.claim, args.out)
