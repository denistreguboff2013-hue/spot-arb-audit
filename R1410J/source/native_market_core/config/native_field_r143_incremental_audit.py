#!/usr/bin/env python3
"""Strict additive P1-06 R143 entry evidence on independently tested R142."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

ENTRY = {"entry_lock_ttl_sec", "limits.top_n_per_scan"}
PENDING = {"general.trade_enabled", "limits.max_trades_per_day",
           "spread.history_window"}


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def build(r142_path, claim_path, output_dir):
    old_bytes = r142_path.read_bytes()
    old = json.loads(old_bytes)
    claim = json.loads(claim_path.read_text(encoding="utf-8"))
    require(old.get("schema") == "astra_a5_original_160_research_overlay_r142_v1"
            and old.get("original_field_count") == 160
            and old.get("total_research_consumed") == 43
            and old.get("research_not_evidenced") == 117
            and old.get("deployed_paper_fields_proven") == 0
            and old.get("real_execution_authorized") is False
            and old.get("analytics_sqlite_writer_proven") is False,
            "R142 evidence parent not trustworthy")
    require(claim.get("active_binary_proven") is False
            and claim.get("real_order_authority") is False
            and claim.get("network_order_permitted") is False
            and claim.get("distributed_lock_proven") is False
            and claim.get("scope") == "rust_research_readonly_only",
            "R143 attempts to grant execution/installed authority")
    claimed = claim.get("fields", [])
    require(len(claimed) == len(set(claimed))
            and set(claimed) == ENTRY, "incorrect/duplicate R143 field set")
    require(set(claim.get("pending_original_entry_fields", [])) == PENDING,
            "original remaining entry constraints changed")
    fields = old.get("fields", [])
    require(len(fields) == 160
            and len({r["field"] for r in fields}) == 160,
            "R142 source keys not 160 unique")
    lookup = {r["field"]: r for r in fields}
    require(ENTRY | PENDING <= lookup.keys(), "original entry key absent")
    require(all(lookup[f]["research_evidence"] == "NOT_EVIDENCED"
                for f in ENTRY | PENDING),
            "R143 duplicating credited source field")
    require(sum(r["research_evidence"] == "TESTED_RESEARCH_READONLY"
                for r in fields) == 43, "original 43 claims changed")
    rows = []
    for item in fields:
        row = dict(item)
        if row["field"] in ENTRY:
            row["research_evidence"] = "TESTED_RESEARCH_READONLY"
            row["research_sources"] = claim_path.name
        require(row["installed_paper_a342540"] == "NOT_PROVEN"
                and row["real_execution_consumption"] == "NOT_PROVEN",
                "R143 may not infer deployed or REAL consumer")
        rows.append(row)
    require(sum(row["research_evidence"] == "TESTED_RESEARCH_READONLY"
                for row in rows) == 45, "strict 45/160 count failed")
    breakdown = {}
    for item in rows:
        g = breakdown.setdefault(item["priority_group"],
            {"total": 0, "research": 0, "missing": 0})
        g["total"] += 1
        g["research" if item["research_evidence"] == "TESTED_RESEARCH_READONLY"
          else "missing"] += 1
    require(sum(g["missing"] for g in breakdown.values()) == 115,
            "strict 115 unproven count failed")
    result = {
        "schema": "astra_a5_original_160_research_overlay_r143_v1",
        "r142_parent_sha256": hashlib.sha256(old_bytes).hexdigest(),
        "r143_overlay_sha256": hashlib.sha256(claim_path.read_bytes()).hexdigest(),
        "original_field_count": 160,
        "prior_research_consumed": 43,
        "new_distinct_research_consumed": 2,
        "total_research_consumed": 45,
        "research_not_evidenced": 115,
        "deployed_paper_fields_proven": 0,
        "installed_binary_runtime_consumption_attested": False,
        "real_execution_authorized": False,
        "durable_distributed_entry_lock_proven": False,
        "priority_group_breakdown": breakdown,
        "remaining_original_entry_fields": sorted(PENDING),
        "fields": rows,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = "ASTRA_A5_160_FIELD_R143_20261020"
    (output_dir / (stem + ".json")).write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    with (output_dir / (stem + ".csv")).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    md = [
        "# Astra P1-06 R143 — 160 original YAML fields (2026-10-20)",
        "", "**Research diagnostic source only, NOT installed PAPER.**", "",
        "- Previous strict research coverage: **43/160**",
        "- New distinct bounded entry fields: **2**",
        "- Updated research coverage: **45/160**",
        "- Without strict research evidence: **115/160**",
        "- Proven in installed PAPER: **0/160**",
        "- REAL permission: **NO**",
        "- Local entry lock is an untrusted timing preview, not a durable lock.",
        "", "| Category | Total | Research | Missing |",
        "|---|---:|---:|---:|",
        *[f"| {k} | {v['total']} | {v['research']} | {v['missing']} |"
          for k, v in sorted(breakdown.items())],
        "", "## Remaining three entry/risk source fields (not credited)",
        *[f"- {f}" for f in sorted(PENDING)], "",
        "## All 160 original source fields", "",
        "| Source key | Research evidence | Installed PAPER |",
        "|---|---|---|",
        *[f"| {r['field']} | {r['research_evidence']} | NOT PROVEN |"
          for r in rows],
    ]
    (output_dir / (stem + ".md")).write_text(
        "\n".join(md) + "\n", encoding="utf-8")
    print("R143 PASS: 160 original, 43 prior+2=45 research, 115 unproven, deployed=0")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--r142", type=Path, required=True)
    parser.add_argument("--r143-claim", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    build(args.r142, args.r143_claim, args.out)


if __name__ == "__main__":
    main()
