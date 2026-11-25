#!/usr/bin/env python3
"""Astra A5 R147: exact eight original real_trial fields, offline only."""
import argparse,csv,hashlib,json
from pathlib import Path
SOURCE_FIELDS={
"real_trial.enabled",
"real_trial.bootstrap_first_trade",
"real_trial.entry_cooldown_sec",
"real_trial.preflight_max_age_sec",
"real_trial.max_mirror_lag_records",
"real_trial.kill_switch_disengaged_max_age_sec",
"real_trial.kill_switch_drill_max_age_sec",
"real_trial.telegram_delivery_max_age_sec",
}
DENY_FLAGS=(
"active_binary_proven","real_order_authority","original_exchange_authenticated",
"preflight_read_or_verified","kill_switch_loaded_or_disengaged",
"mirror_state_verified","durable_cooldown_verified",
"telegram_delivery_verified","real_trade_allowed",
)
def check(cond,msg):
 if not cond:raise ValueError(msg)
def generate(parent_file,claim_file,out):
 src=parent_file.read_bytes();prior=json.loads(src)
 proof=claim_file.read_bytes();claim=json.loads(proof)
 check(prior.get("schema")=="astra_a5_original_160_research_overlay_r146_v1"
  and prior.get("original_field_count")==160
  and prior.get("total_research_consumed")==81
  and prior.get("research_not_evidenced")==79
  and prior.get("deployed_paper_fields_proven")==0
  and prior.get("real_execution_authorized") is False
  and prior.get("unknown_ack_retry_authorized") is False,
  "R146 baseline 81/160/0 untrusted")
 check(claim.get("schema")=="R147_EIGHT_REAL_TRIAL_WINDOW_CONFIG_FIELDS_READONLY_OVERLAY_V1"
  and claim.get("scope")=="rust_research_readonly_only",
  "R147 claim scope invalid")
 for key in DENY_FLAGS:
  check(claim.get(key) is False,"R147 falsely authorizes "+key)
 fields=claim.get("fields",[])
 check(len(fields)==8 and len(set(fields))==8 and set(fields)==SOURCE_FIELDS,
  "wrong/duplicate/non-original R147 field set")
 rows=prior.get("fields",[])
 check(len(rows)==160 and len(set(r["field"] for r in rows))==160,
  "original 160 paths missing/duplicate")
 check(sum(r["research_evidence"]=="TESTED_RESEARCH_READONLY" for r in rows)==81,
  "R146 already-sourced research count mismatched")
 lookup={r["field"]:r for r in rows}
 for path in SOURCE_FIELDS:
  check(path in lookup and lookup[path]["research_evidence"]=="NOT_EVIDENCED"
   and lookup[path]["priority_group"]=="SAFETY_AND_RECOVERY",
   "R147 overlap/incorrect source field "+path)
 check(all(r["installed_paper_a342540"]=="NOT_PROVEN"
  and r["real_execution_consumption"]=="NOT_PROVEN" for r in rows),
  "prior ledger falsely claims installed PAPER/REAL")
 updated=[]
 for v in rows:
  x=dict(v)
  if x["field"] in SOURCE_FIELDS:
   x["research_evidence"]="TESTED_RESEARCH_READONLY"
   x["research_sources"]=claim_file.name
  updated.append(x)
 groups={}
 for v in updated:
  g=groups.setdefault(v["priority_group"],{"total":0,"research":0,"missing":0})
  g["total"]+=1
  g["research" if v["research_evidence"]=="TESTED_RESEARCH_READONLY" else "missing"]+=1
 check(sum(g["total"] for g in groups.values())==160
  and sum(g["research"] for g in groups.values())==89
  and sum(g["missing"] for g in groups.values())==71
  and groups["SAFETY_AND_RECOVERY"]=={"total":53,"research":44,"missing":9},
  "R147 89/160 or safety 44/53 invariant drift")
 result={
  "schema":"astra_a5_original_160_research_overlay_r147_v1",
  "original_field_count":160,
  "prior_r146_sha256":hashlib.sha256(src).hexdigest(),
  "overlay_r147_sha256":hashlib.sha256(proof).hexdigest(),
  "previous_research_consumed":81,"new_distinct_research_consumed":8,
  "total_research_consumed":89,"research_not_evidenced":71,
  "deployed_paper_fields_proven":0,
  "original_exchange_account_authenticated":False,
  "real_kill_switch_or_preflight_read":False,
  "installed_paper_source_attested":False,
  "real_execution_authorized":False,
  "unknown_ack_retry_authorized":False,
  "priority_group_breakdown":groups,"fields":updated,
 }
 out.mkdir(parents=True,exist_ok=True)
 stem="ASTRA_A5_160_FIELD_R147_20261020"
 (out/(stem+".json")).write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n")
 with (out/(stem+".csv")).open("w",newline="",encoding="utf-8") as f:
  w=csv.DictWriter(f,fieldnames=list(updated[0]));w.writeheader();w.writerows(updated)
 md=["# Astra P1-06 R147 — original 160 YAML source keys","",
  "**Research-only, no installed PAPER or REAL proof.**","",
  "- Prior 81 distinct original source-consumer paths",
  "- New R147 eight distinct real_trial read-only guard windows",
  "- Strict research: **89/160**",
  "- No strict research proof: **71/160**",
  "- SAFETY_AND_RECOVERY: **44/53**, 9 remain",
  "- Deployed PAPER source application attested: **0/160**",
  "- REAL, kill switch disengagement, signed preflight, account inventory: NOT PROVEN","",
  "| Category | Total | Research | Missing |","|---|---:|---:|---:|",
  *[f"| {k} | {v['total']} | {v['research']} | {v['missing']} |" for k,v in sorted(groups.items())],
  "","## All 160 original paths","","| Path | Research evidence | Installed PAPER |",
  "|---|---|---|",*[f"| {r['field']} | {r['research_evidence']} | NOT PROVEN |" for r in updated]]
 (out/(stem+".md")).write_text("\n".join(md)+"\n",encoding="utf-8")
 print("R147 PASS: 160 original, prior81 +8 =89 research, 71 unproven, 44/53 safety, 0 deployed")
if __name__=="__main__":
 p=argparse.ArgumentParser()
 p.add_argument("--r146",type=Path,required=True)
 p.add_argument("--overlay",type=Path,required=True)
 p.add_argument("--out",type=Path,required=True)
 a=p.parse_args();generate(a.r146,a.overlay,a.out)
