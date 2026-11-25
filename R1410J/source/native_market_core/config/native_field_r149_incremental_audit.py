#!/usr/bin/env python3
"""R149 original YAML 2 entry + 7 existing Rust observation consumers."""
import argparse,csv,hashlib,json
from pathlib import Path
ENTRY={"general.trade_enabled","limits.max_trades_per_day"}
TELEMETRY={"general.heartbeat_interval_sec","telegram.enabled","telegram.send_signals","telegram.send_errors","telegram.send_heartbeat","telemetry.service.send_tg","telemetry.service.interval_sec"}
FIELDS=ENTRY|TELEMETRY
FORBID=["real_order_authority","active_binary_proven","linked_to_installed_paper","authenticated_exchange_count","telegram_delivery_proven","telegram_network_actions","replay_fence_durable","installed_paper_source_attested"]
def need(p,why):
 if not p:raise ValueError(why)
def generate(parent,overlay,out):
 b=parent.read_bytes();prev=json.loads(b)
 cb=overlay.read_bytes();claim=json.loads(cb)
 need(prev.get("schema")=="astra_a5_original_160_research_overlay_r148_v1" and prev.get("original_field_count")==160 and prev.get("total_research_consumed")==98 and prev.get("research_not_evidenced")==62 and prev.get("deployed_paper_fields_proven")==0 and prev.get("real_execution_authorized") is False,"R148 baseline wrong")
 need(claim.get("schema")=="R149_TWO_ENTRY_SEVEN_TELEGRAM_EXISTING_RUST_CONSUMERS_V1" and claim.get("scope")=="rust_research_lib_tests_no_io_only" and claim.get("unproven_entry_field")=="spread.history_window","R149 claim scope wrong")
 for k in FORBID:need(claim.get(k) is False,"fake authority "+k)
 names=claim.get("fields",[])
 need(len(names)==9 and len(set(names))==9 and set(names)==FIELDS,"wrong nine paths")
 rows=prev.get("fields",[])
 need(len(rows)==160 and len({r["field"] for r in rows})==160 and sum(r["research_evidence"]=="TESTED_RESEARCH_READONLY" for r in rows)==98,"160/98 parent drift")
 original={r["field"]:r for r in rows}
 for key in FIELDS:
  cat="ENTRY_RISK_AND_MARKET" if key in ENTRY else "OPTIONAL_TELEMETRY_AND_REPORTING"
  need(key in original and original[key]["research_evidence"]=="NOT_EVIDENCED" and original[key]["priority_group"]==cat,"duplicate or missing original field "+key)
 need(original["spread.history_window"]["research_evidence"]=="NOT_EVIDENCED","unproven history got credit")
 need(all(r["installed_paper_a342540"]=="NOT_PROVEN" and r["real_execution_consumption"]=="NOT_PROVEN" for r in rows),"false deployed source proof")
 updated=[]
 for a in rows:
  row=dict(a)
  if row["field"] in FIELDS:
   row["research_evidence"]="TESTED_RESEARCH_READONLY"
   row["research_sources"]=overlay.name+" (RUST_REVIEW_ONLY)"
  updated.append(row)
 groups={}
 for row in updated:
  x=groups.setdefault(row["priority_group"],{"total":0,"research":0,"missing":0})
  x["total"]+=1
  x["research" if row["research_evidence"]=="TESTED_RESEARCH_READONLY" else "missing"]+=1
 need(sum(x["total"] for x in groups.values())==160 and sum(x["research"] for x in groups.values())==107 and sum(x["missing"] for x in groups.values())==53,"bad 107/53 accounting")
 need(groups["ENTRY_RISK_AND_MARKET"]=={"total":31,"research":30,"missing":1} and groups["SAFETY_AND_RECOVERY"]=={"total":53,"research":53,"missing":0} and groups["OPTIONAL_TELEMETRY_AND_REPORTING"]=={"total":76,"research":24,"missing":52},"wrong three groups")
 report={"schema":"astra_a5_original_160_research_overlay_r149_v1","original_field_count":160,"r148_parent_sha256":hashlib.sha256(b).hexdigest(),"r149_overlay_sha256":hashlib.sha256(cb).hexdigest(),"previous_research_consumed":98,"new_distinct_research_consumed":9,"total_research_consumed":107,"research_not_evidenced":53,"deployed_paper_fields_proven":0,"general_legacy_trade_enabled_is_real_authority":False,"general_max_trades_applied_to_installed_paper":False,"telegram_delivered":False,"replay_fence_is_durable":False,"installed_paper_source_attested":False,"real_execution_authorized":False,"original_history_window_entry_logic_proven":False,"priority_group_breakdown":groups,"fields":updated}
 out.mkdir(parents=True,exist_ok=True)
 stem="ASTRA_A5_160_FIELD_R149_20261020"
 (out/(stem+".json")).write_text(json.dumps(report,indent=2,ensure_ascii=False)+"\n")
 with (out/(stem+".csv")).open("w",encoding="utf-8",newline="") as f:
  w=csv.DictWriter(f,fieldnames=list(updated[0]));w.writeheader();w.writerows(updated)
 lines=["# Astra P1-06 R149 — original 160 YAML paths","","Strict offline research only, not installed PAPER.","","- Previous 98, new nine distinct source consumers, research 107/160, missing 53/160","- Entry 30/31, safety 53/53, optional/telemetry 24/76","- spread.history_window remains unproven; no original Rust strategy consumer","- Installed PAPER original YAML proof 0/160, REAL false, Telegram not delivered","", "| Category | Total | Research | Missing |","|---|---:|---:|---:|",*[f"| {k} | {v['total']} | {v['research']} | {v['missing']} |" for k,v in sorted(groups.items())],"","## 160 original paths","","| Source path | Research | Installed PAPER |","|---|---|---|",*[f"| {row['field']} | {row['research_evidence']} | NOT PROVEN |" for row in updated]]
 (out/(stem+".md")).write_text("\n".join(lines)+"\n")
 print("A5_R149 PASS 160 original, previous98+9=107 research, missing53, entry30/31, telemetry24/76, installed0")
if __name__=="__main__":
 p=argparse.ArgumentParser()
 p.add_argument("--r148",type=Path,required=True)
 p.add_argument("--claim",type=Path,required=True)
 p.add_argument("--out",type=Path,required=True)
 q=p.parse_args();generate(q.r148,q.claim,q.out)
