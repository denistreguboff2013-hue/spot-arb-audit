#!/usr/bin/env python3
"""Astra A5: strict 18-key exit/recovery source diagnostics over R144."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

EXIT = {
    "exit_safety.enabled", "exit_safety.hedge_recover",
    "exit_safety.leg_gap_ms", "exit_safety.spot_chase.enabled",
    "exit_safety.spot_chase.hard_deadline_ms",
    "exit_safety.spot_chase.limit_ticks_start",
    "exit_safety.spot_chase.limit_ticks_step",
    "exit_safety.spot_chase.max_steps",
    "exit_safety.spot_chase.step_wait_ms",
}
EMERGENCY = {
    "futures_emergency_stop.enabled",
    "futures_emergency_stop.notify_on_place",
    "futures_emergency_stop.order_type",
    "futures_emergency_stop.paper_simulate",
    "futures_emergency_stop.place_in_real",
    "futures_emergency_stop.retry_ms",
    "futures_emergency_stop.retry_n",
    "futures_emergency_stop.stop_spread_bps",
    "futures_emergency_stop.working_type",
}
ALL = EXIT | EMERGENCY


def require(condition, message):
    if not condition:
        raise ValueError(message)


def generate(parent_file, overlay_file, destination):
    prior_bytes = parent_file.read_bytes()
    claim_bytes = overlay_file.read_bytes()
    previous = json.loads(prior_bytes)
    claim = json.loads(claim_bytes)
    require(previous.get("schema") == "astra_a5_original_160_research_overlay_r144_v1"
            and previous.get("original_field_count") == 160
            and previous.get("total_research_consumed") == 53
            and previous.get("research_not_evidenced") == 107
            and previous.get("deployed_paper_fields_proven") == 0
            and previous.get("real_execution_authorized") is False
            and previous.get("unknown_ack_retry_authorized") is False,
            "untrusted R144 source evidence")
    require(claim.get("schema") == "R145_EIGHTEEN_EXIT_AND_EMERGENCY_SOURCE_READONLY_OVERLAY_V1"
            and claim.get("scope") == "rust_research_readonly_only",
            "R145 is not the expected source-only contract")
    for flag in ("active_binary_proven", "real_order_authority",
                 "authenticated_exchange_stop_trigger",
                 "actual_hedge_recovery_proven", "unknown_ack_retry_authorized",
                 "order_submission_allowed", "telegram_delivery_proven",
                 "distributed_exit_lock_proven",
                 "installed_paper_source_attested"):
        require(claim.get(flag) is False, f"forged authority: {flag}")
    fields = claim.get("fields", [])
    require(len(fields) == 18 and len(set(fields)) == 18
            and set(fields) == ALL and len(EXIT) == 9 and len(EMERGENCY) == 9,
            "expected exactly nine exit and nine emergency source paths")
    prior_rows = previous.get("fields", [])
    require(len(prior_rows) == 160 and
            len({row["field"] for row in prior_rows}) == 160,
            "R144 field set malformed")
    counted = {row["field"] for row in prior_rows
               if row["research_evidence"] == "TESTED_RESEARCH_READONLY"}
    require(len(counted) == 53 and not counted.intersection(ALL),
            "R145 would duplicate or replace earlier field credit")
    require(all(row["installed_paper_a342540"] == "NOT_PROVEN"
                and row["real_execution_consumption"] == "NOT_PROVEN"
                for row in prior_rows),
            "installed binary or REAL misrepresentation in parent")
    orig = {row["field"]: row for row in prior_rows}
    require(ALL.issubset(orig) and all(orig[k]["priority_group"] == "SAFETY_AND_RECOVERY"
                                       and orig[k]["research_evidence"] == "NOT_EVIDENCED"
                                       for k in ALL),
            "R145 field outside original safety group or already proven")
    rows = []
    for row in prior_rows:
        r = dict(row)
        if r["field"] in ALL:
            r["research_evidence"] = "TESTED_RESEARCH_READONLY"
            r["research_sources"] = overlay_file.name
        rows.append(r)
    groups = {}
    for row in rows:
        b = groups.setdefault(row["priority_group"], {
            "total": 0, "research": 0, "missing": 0})
        b["total"] += 1
        b["research" if row["research_evidence"] == "TESTED_RESEARCH_READONLY"
          else "missing"] += 1
    require(sum(g["total"] for g in groups.values()) == 160
            and sum(g["research"] for g in groups.values()) == 71
            and sum(g["missing"] for g in groups.values()) == 89
            and groups["SAFETY_AND_RECOVERY"] == {
                "total": 53, "research": 26, "missing": 27},
            "71/160 or safety 26/53 expected coverage broken")
    result = {
        "schema": "astra_a5_original_160_research_overlay_r145_v1",
        "r144_parent_sha256": hashlib.sha256(prior_bytes).hexdigest(),
        "r145_overlay_sha256": hashlib.sha256(claim_bytes).hexdigest(),
        "original_field_count": 160,
        "prior_research_consumed": 53,
        "new_distinct_research_consumed": 18,
        "total_research_consumed": 71,
        "research_not_evidenced": 89,
        "deployed_paper_fields_proven": 0,
        "actual_exchange_protective_stop_verified": False,
        "installed_runtime_source_attested": False,
        "real_execution_authorized": False,
        "unknown_ack_retry_authorized": False,
        "telegram_delivery_proven": False,
        "priority_group_breakdown": groups,
        "fields": rows,
    }
    destination.mkdir(parents=True, exist_ok=True)
    stem = "ASTRA_A5_160_FIELD_R145_20261020"
    (destination / (stem + ".json")).write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    with (destination / (stem + ".csv")).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    notes = [
        "# Astra P1-06: R145 / original 160 YAML source keys",
        "", "**Read-only source consumer proof, NOT exchange order execution.**",
        "", "- Prior research only: **53/160**",
        "- Source-typed exit safety: **9**; futures emergency config: **9**",
        "- Updated strict research only: **71/160**",
        "- Research fields without qualifying evidence: **89/160**",
        "- Installed PAPER source consumption proven: **0/160**",
        "- REAL stop order/hedge/Telegram actions: **NOT AUTHORIZED OR PROVEN**",
        "- UNKNOWN ACK automatically retried: **NO**",
        "", "| Group | Total | Research | No strict evidence |",
        "|---|---:|---:|---:|",
        *[f"| {name} | {group['total']} | {group['research']} | {group['missing']} |"
          for name, group in sorted(groups.items())],
        "", "## All 160 source fields", "",
        "| Original key | Strict research evidence | Active PAPER |",
        "|---|---|---|",
        *[f"| {row['field']} | {row['research_evidence']} | NOT PROVEN |"
          for row in rows],
    ]
    (destination / (stem + ".md")).write_text(
        "\n".join(notes) + "\n", encoding="utf-8")
    print("A5_R145 PASS: 160 original, 53+18=71 research, "
          "89 without evidence, 26/53 safety, 0 deployed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--r144", type=Path, required=True)
    parser.add_argument("--claim", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    generate(args.r144, args.claim, args.out)
