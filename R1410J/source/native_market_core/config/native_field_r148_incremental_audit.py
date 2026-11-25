#!/usr/bin/env python3
"""Astra A5 R148: nine original YAML paths, isolated Rust test proof only."""
import argparse,csv,hashlib,json
from pathlib import Path
NINE={
 "real_trial.allowed_spot_inventory_bases","real_trial.autonomous_real_enabled",
 "real_trial.kill_switch_path","real_trial.operator_approved",
 "real_trial.preflight_path","real_trial.resolved_incident_path",
 "real_trial.supervised_retrial_enabled","real_trial.supervised_retrial_permit_path",
 "real_trial.supervised_retrial_plan_path",
}
NO_AUTH=[
 "active_binary_proven","linked_into_main_rust_library",
 "installed_paper_source_attested","real_order_authority",
 "source_path_files_opened","filesystem_symlinks_verified",
 "inventory_account_owner_verified","authenticated_retrial_incident_verified",
 "preflight_validity_proven","kill_switch_disengaged_proven",
 "real_launch_authorized",
]
def need(ok,msg):
 if not ok:raise ValueError(msg)
def generate(parent,proof,out):
 raw=parent.read_bytes();old=json.loads(raw)
 proof_data=proof.read_bytes();claim=json.loads(proof_data)
 need(old.get("schema")=="astra_a5_original_160_research_overlay_r147_v1"
  and old.get("original_field_count")==160
  and old.get("total_research_consumed")==89
  and old.get("research_not_evidenced")==71
  and old.get("deployed_paper_fields_proven")==0
  and old.get("real_execution_authorized") is False,
  "R147 exact research-only starting state invalid")
 need(claim.get("schema")=="R148_NINE_REAL_TRIAL_PATH_FLAGS_INVENTORY_RESEARCH_V1"
  and claim.get("scope")=="rust_research_isolated_integration_test_only",
  "claim is not isolated offline test")
 for name in NO_AUTH:
  need(claim.get(name) is False,"R148 authority overclaim: "+name)
 fields=claim.get("fields",[])
 need(isinstance(fields,list) and len(fields)==9 and len(set(fields))==9
  and set(fields)==NINE,"wrong or duplicate nine original R148 paths")
 rows=old.get("fields",[])
 need(len(rows)==160 and len(set(x["field"] for x in rows))==160
  and sum(x["research_evidence"]=="TESTED_RESEARCH_READONLY" for x in rows)==89,
  "original 160-field ledger 89 coverage compromised")
 by={r["field"]:r for r in rows}
 for path in NINE:
  need(path in by and by[path]["research_evidence"]=="NOT_EVIDENCED"
   and by[path]["priority_group"]=="SAFETY_AND_RECOVERY",
   "R148 overlapping or misplaced research source: "+path)
 need(all(r["installed_paper_a342540"]=="NOT_PROVEN"
  and r["real_execution_consumption"]=="NOT_PROVEN" for r in rows),
  "parent false deployed/real claim")
 updated=[]
 for original in rows:
  r=dict(original)
  if r["field"] in NINE:
   r["research_evidence"]="TESTED_RESEARCH_READONLY"
   r["research_sources"]=proof.name+" (ISOLATED_TEST_ONLY)"
  updated.append(r)
 groups={}
 for r in updated:
  x=groups.setdefault(r["priority_group"],{"total":0,"research":0,"missing":0})
  x["total"]+=1
  x["research" if r["research_evidence"]=="TESTED_RESEARCH_READONLY"
    else "missing"]+=1
 need(sum(g["total"] for g in groups.values())==160
  and sum(g["research"] for g in groups.values())==98
  and sum(g["missing"] for g in groups.values())==62
  and groups["SAFETY_AND_RECOVERY"]=={"total":53,"research":53,"missing":0},
  "R148 98/160 or 53/53 safety research invariant false")
 result={
  "schema":"astra_a5_original_160_research_overlay_r148_v1",
  "original_field_count":160,
  "r147_sha256":hashlib.sha256(raw).hexdigest(),
  "r148_claim_sha256":hashlib.sha256(proof_data).hexdigest(),
  "previous_research_consumed":89,"new_isolated_test_only_research_consumed":9,
  "total_research_consumed":98,"research_not_evidenced":62,
  "deployed_paper_fields_proven":0,
  "all_safety_fields_attested_in_installed_paper":False,
  "source_real_trading_authorized":False,
  "real_execution_authorized":False,
  "source_account_or_file_data_authenticated":False,
  "new_nine_wired_to_main_library":False,
  "priority_group_breakdown":groups,"fields":updated,
 }
 out.mkdir(parents=True,exist_ok=True)
 stem="ASTRA_A5_160_FIELD_R148_20261020"
 (out/(stem+".json")).write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n")
 with (out/(stem+".csv")).open("w",encoding="utf-8",newline="") as f:
  w=csv.DictWriter(f,fieldnames=list(updated[0]));w.writeheader();w.writerows(updated)
 md=["# SPOT-ARB Astra P1-06 R148 — exact 160 original YAML keys","",
 "**New R148 nine keys proven only in isolated Rust integration test, NOT the normal linked library.**","",
 "- Previous strict research: **89/160**",
 "- New isolated Rust research test consumers: **9**",
 "- Source-research coverage: **98/160** (including isolated tests)",
 "- Remaining without qualifying source research: **62/160**",
 "- SAFETY_AND_RECOVERY: **53/53 source-research evidence only**",
 "- Installed PAPER original YAML source fields attested: **0/160**",
 "- Real order, live preflight, kill switch, account owner, filesystem link truth: **NONE**","",
 "| Group | Total | Research | Without evidence |",
 "|---|---:|---:|---:|",
 *[f"| {k} | {v['total']} | {v['research']} | {v['missing']} |" for k,v in sorted(groups.items())],
 "","## Original 160 paths","","| Original path | Research status | Installed PAPER |",
 "|---|---|---|",
 *[f"| {r['field']} | {r['research_evidence']} | NOT PROVEN |" for r in updated]]
 (out/(stem+".md")).write_text("\n".join(md)+"\n")
 print("R148 PASS original=160 prior=89 +9 isolated=98, research missing=62, safety=53/53, installed=0")
if __name__=="__main__":
 a=argparse.ArgumentParser()
 a.add_argument("--r147",type=Path,required=True)
 a.add_argument("--overlay",type=Path,required=True)
 a.add_argument("--out",type=Path,required=True)
 x=a.parse_args();generate(x.r147,x.overlay,x.out)
