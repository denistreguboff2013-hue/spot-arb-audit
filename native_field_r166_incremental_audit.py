#!/usr/bin/env python3
"""R166 strict additive OFFLINE 16-field source evidence, no installed proof."""
import argparse,csv,hashlib,json
from collections import Counter
from pathlib import Path
import native_field_r164_crosswalk as r164

EXPECTED=16
PREFIX="fast_market_shadow."
PARENT_SCHEMA="astra_a5_original_160_research_overlay_r149_v1"
CLAIM_SCHEMA="ASTRA_R166_SHADOW_16_STATIC_RESEARCH_CONSUMER_V1"

def require(condition,msg):
    if not condition:raise ValueError("R166 deny: "+msg)

def generate(parent_path,claim_path,out_path):
    prior_bytes=parent_path.read_bytes()
    old=json.loads(prior_bytes)
    claim_bytes=claim_path.read_bytes()
    claim=json.loads(claim_bytes)
    # R164 has checked the actual linked YAMLs, R148 SHA chain, types and
    # cumulative R149 CSV; all evidence remains RESEARCH ONLY.
    source_check=r164.load()
    require(source_check["cumulative_ledger_sha256"] ==
            hashlib.sha256(prior_bytes).hexdigest(),
            "R164 crosswalk does not attest same R149 parent")
    require(old.get("schema")==PARENT_SCHEMA and
            old.get("original_field_count")==160 and
            old.get("total_research_consumed")==107 and
            old.get("research_not_evidenced")==53 and
            old.get("deployed_paper_fields_proven")==0 and
            old.get("real_execution_authorized") is False and
            old.get("installed_paper_source_attested") is False,
            "R149 baseline or real-trading scope drift")
    require(claim.get("schema")==CLAIM_SCHEMA and
            claim.get("scope")=="rust_offline_caller_supplied_shadow_envelope_only" and
            claim.get("source_module")=="src/native_shadow_config_review.rs" and
            claim.get("source_test")==
               "r166_all_sixteen_original_keys_change_offline_decision_without_io" and
            claim.get("offline_rust_tests_executed_and_passed") is True,
            "R166 source or tests differ from audit")
    for key in ("source_consumer_semantics_equal_to_installed_python",
        "actual_websocket_fleet_launched",
        "installed_native_binary_consumption_proven","active_binary_proven",
        "private_exchange_used","real_order_authority","telegram_sent",
        "external_peer_attested"):
        require(claim.get(key) is False, "forged operational claim: "+key)
    schema=json.loads((r164.HERE/"native_field_schema.json").read_bytes())
    required={k for k in schema if k.startswith(PREFIX)}
    fields=claim.get("fields",[])
    require(len(fields)==EXPECTED and len(set(fields))==EXPECTED
            and set(fields)==required,
            "original 16 shadow keys not exact or duplicate")
    rust_source=(r164.HERE.parent/"src/native_shadow_config_review.rs").read_text()
    require("pub fn project_bounded_shadow_review_r166(" in rust_source and
            claim["source_test"] in rust_source and
            "configuration_values_are_applied_to_installed_ws" in rust_source,
            "review-only method/test source missing")
    rows=old.get("fields",[])
    require(len(rows)==160 and len({r["field"] for r in rows})==160,
            "R149 rows lost or duplicated")
    credits={x["field"] for x in rows if
             x.get("research_evidence")=="TESTED_RESEARCH_READONLY"}
    require(len(credits)==107 and not(set(fields)&credits),
            "double research credit")
    for row in rows:
        require(row["installed_paper_a342540"]=="NOT_PROVEN" and
                row["real_execution_consumption"]=="NOT_PROVEN",
                "installed or REAL claims forbidden")
    updated=[]
    for row in rows:
        x=dict(row)
        if x["field"] in required:
            require(x["research_evidence"]=="NOT_EVIDENCED" and
                    x["priority_group"]=="OPTIONAL_TELEMETRY_AND_REPORTING",
                    "R166 only 16 previously unproven diagnostic keys")
            x["research_evidence"]="TESTED_RESEARCH_READONLY"
            x["research_sources"]=claim_path.name+" (ISOLATED_SIMULATOR_ONLY)"
        updated.append(x)
    counter=Counter(row["research_evidence"] for row in updated)
    require(counter["TESTED_RESEARCH_READONLY"]==123
            and counter["NOT_EVIDENCED"]==37,
            "R166 incorrect additive total")
    groups={}
    for row in updated:
        g=groups.setdefault(row["priority_group"],
          {"total":0,"research":0,"missing":0})
        g["total"]+=1
        g["research" if row["research_evidence"]==
          "TESTED_RESEARCH_READONLY" else "missing"]+=1
    require(groups=={
        "ENTRY_RISK_AND_MARKET":{"total":31,"research":30,"missing":1},
        "SAFETY_AND_RECOVERY":{"total":53,"research":53,"missing":0},
        "OPTIONAL_TELEMETRY_AND_REPORTING":{"total":76,"research":40,"missing":36}},
        "priority breakdown drift")
    result={
        "schema":"astra_a5_original_160_research_overlay_r166_v1",
        "original_field_count":160,
        "r149_parent_sha256":hashlib.sha256(prior_bytes).hexdigest(),
        "r166_claim_sha256":hashlib.sha256(claim_bytes).hexdigest(),
        "previous_research_consumed":107,
        "new_distinct_research_consumed":16,
        "new_evidence_scope":"source_driven_offline_shadow_simulation_only",
        "total_research_consumed":123,
        "research_not_evidenced":37,
        "deployed_paper_fields_proven":0,
        "source_simulator_is_installed_paper":False,
        "real_execution_authorized":False,
        "exchange_authenticated":False,
        "public_ws_running_proven":False,
        "telegram_delivery_proven":False,
        "r155_stale_ingress_fixed":False,
        "original_spread_history_window_proven":False,
        "priority_group_breakdown":groups,
        "fields":updated
    }
    out_path.mkdir(parents=True,exist_ok=True)
    stem="ASTRA_A5_160_FIELD_R166_20261020"
    (out_path/(stem+".json")).write_text(
        json.dumps(result,indent=2,ensure_ascii=False)+"\n")
    with (out_path/(stem+".csv")).open("w",encoding="utf-8",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=list(updated[0]))
        writer.writeheader();writer.writerows(updated)
    lines=["# Astra P1-06 R166 — offline 160 source fields",
        "","**Source-only simulation, NOT installed PAPER consumption.**",
        "","- Previous: 107/160 source research claims",
        "- New: 16 distinct fast_market_shadow fields in offline diagnostic envelope",
        "- Research: **123/160**; unproven research: **37/160**",
        "- Installed PAPER: **0/160 proven**; REAL not authorized",
        "- Original spread.history_window still not wired to trading strategy",
        "- R155 stale-ticker ingress remains OPEN",
        "","| Priority | Total | Research | Missing |",
        "|---|---:|---:|---:|",
        *[f"| {k} | {v['total']} | {v['research']} | {v['missing']} |"
          for k,v in sorted(groups.items())],
        "","## Still unproven original keys",
        *[f"- {row['field']}" for row in updated if
          row["research_evidence"]=="NOT_EVIDENCED"],
        "","## Full original 160",
        "| Original YAML field | Research claim | Installed PAPER |",
        "|---|---|---|",
        *[f"| {row['field']} | {row['research_evidence']} | NOT_PROVEN |"
          for row in updated]]
    (out_path/(stem+".md")).write_text("\n".join(lines)+"\n")
    print("R166 PASS 160 original 107+16=123 source-only, 37 missing, installed=0")
    return result

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--r149",type=Path,required=True)
    p.add_argument("--claim",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    args=p.parse_args()
    generate(args.r149,args.claim,args.out)
