#!/usr/bin/env python3
"""Exact ten-field R146 read-only hard-stop/finance addition to R145."""
import argparse,csv,hashlib,json
from pathlib import Path
FIELDS={
"native_runtime.config_schema_version",
"native_runtime.mutation_capability_enabled",
"native_runtime.production_cutover_authorized",
"native_runtime.real_execution_authorized",
"real_trial.max_trades_per_day","real_trial.max_open_positions",
"real_trial.deny_on_unknown","real_trial.deny_on_untracked_assets",
"limits.max_daily_loss_usdt","limits.max_okx_fees_per_day_usdt",
}
def need(condition,why):
 if not condition:raise ValueError(why)
def generate(parent,overlay,out):
 old=parent.read_bytes();original=json.loads(old)
 claim_b=overlay.read_bytes();claim=json.loads(claim_b)
 need(original.get("schema")=="astra_a5_original_160_research_overlay_r145_v1"
   and original.get("original_field_count")==160
   and original.get("total_research_consumed")==71
   and original.get("research_not_evidenced")==89
   and original.get("deployed_paper_fields_proven")==0
   and original.get("real_execution_authorized") is False
   and original.get("unknown_ack_retry_authorized") is False,"R145 source parent invalid")
 need(claim.get("schema")=="R146_TEN_HARDSTOP_AND_FINANCIAL_FIELDS_READONLY_OVERLAY_V1"
   and claim.get("scope")=="rust_research_readonly_only","wrong R146 overlay")
 for k in ("active_binary_proven","real_order_authority",
   "account_history_complete_proven","independent_daily_loss_verified",
   "independent_daily_okx_fees_verified","unknown_ack_retry_authorized",
   "may_open_real","installed_paper_source_attested"):
  need(claim.get(k) is False,"cannot grant authority: "+k)
 f=claim.get("fields")
 need(isinstance(f,list) and len(f)==10 and len(set(f))==10
   and set(f)==FIELDS,"unexpected or duplicate R146 source path")
 rows=original.get("fields")
 need(isinstance(rows,list) and len(rows)==160
   and len(set(row["field"] for row in rows))==160
   and sum(row["research_evidence"]=="TESTED_RESEARCH_READONLY" for row in rows)==71,
      "R145 row set or 71 source credits drift")
 lookup={r["field"]:r for r in rows}
 for key in FIELDS:
  need(key in lookup and lookup[key]["research_evidence"]=="NOT_EVIDENCED"
    and lookup[key]["priority_group"]=="SAFETY_AND_RECOVERY",
    "R146 original field already credited or not in safety: "+key)
 need(all(r["installed_paper_a342540"]=="NOT_PROVEN"
   and r["real_execution_consumption"]=="NOT_PROVEN" for r in rows),
   "R145 has false active claim")
 updated=[]
 for prior in rows:
  r=dict(prior)
  if r["field"] in FIELDS:
   r["research_evidence"]="TESTED_RESEARCH_READONLY"
   r["research_sources"]=overlay.name
  updated.append(r)
 groups={}
 for r in updated:
  x=groups.setdefault(r["priority_group"],{"total":0,"research":0,"missing":0})
  x["total"]+=1
  x["research" if r["research_evidence"]=="TESTED_RESEARCH_READONLY" else "missing"]+=1
 need(sum(x["total"] for x in groups.values())==160
  and sum(x["research"] for x in groups.values())==81
  and sum(x["missing"] for x in groups.values())==79
  and groups["SAFETY_AND_RECOVERY"]=={"total":53,"research":36,"missing":17},
  "R146 coverage 81/160 and safety 36/53 invariant broken")
 output={
  "schema":"astra_a5_original_160_research_overlay_r146_v1",
  "original_field_count":160,
  "parent_r145_sha256":hashlib.sha256(old).hexdigest(),
  "overlay_r146_sha256":hashlib.sha256(claim_b).hexdigest(),
  "previous_research_consumed":71,"new_distinct_research_consumed":10,
  "total_research_consumed":81,"research_not_evidenced":79,
  "deployed_paper_fields_proven":0,
  "entire_real_account_day_authenticated":False,
  "installed_paper_source_attested":False,
  "real_execution_authorized":False,
  "unknown_ack_retry_authorized":False,
  "priority_group_breakdown":groups,"fields":updated,
 }
 out.mkdir(parents=True,exist_ok=True)
 stem="ASTRA_A5_160_FIELD_R146_20261020"
 (out/(stem+".json")).write_text(json.dumps(output,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
 with (out/(stem+".csv")).open("w",encoding="utf-8",newline="") as fd:
  writer=csv.DictWriter(fd,fieldnames=list(updated[0]))
  writer.writeheader();writer.writerows(updated)
 md=[
 "# Astra P1-06 R146 — exact original 160 YAML source keys","",
 "**Rust research only; not installed PAPER or exchange-authenticated evidence.**","",
 "- Previous strict research: **71/160**",
 "- R146 new: **10** (8 mandatory source-denial knobs, 2 read-only financial ceilings)",
 "- Updated strict research: **81/160**",
 "- Without qualifying source-consumer tests: **79/160**",
 "- SAFETY_AND_RECOVERY: **36/53**, 17 remain",
 "- Active PAPER original-field provenance proven: **0/160**",
 "- UNKNOWN ACK retry, real trading, authenticated account day: **NO**","",
 "| Original group | Total | Research | Missing |",
 "|---|---:|---:|---:|",
 *[f"| {k} | {v['total']} | {v['research']} | {v['missing']} |" for k,v in sorted(groups.items())],
 "","## Original 160 fields","","| Field | Research | Installed PAPER |",
 "|---|---|---|",
 *[f"| {r['field']} | {r['research_evidence']} | NOT PROVEN |" for r in updated],
 ]
 (out/(stem+".md")).write_text("\n".join(md)+"\n",encoding="utf-8")
 print("R146 PASS original=160, prior=71, added=10, research=81, missing=79, safety=36/53, deployed=0")
if __name__=="__main__":
 p=argparse.ArgumentParser()
 p.add_argument("--r145",type=Path,required=True)
 p.add_argument("--overlay",type=Path,required=True)
 p.add_argument("--out",type=Path,required=True)
 a=p.parse_args();generate(a.r145,a.overlay,a.out)
