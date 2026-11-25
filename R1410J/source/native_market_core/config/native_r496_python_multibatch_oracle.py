"""R496: independent two-batch original Python prune_raw/Decimal oracle, in-memory SQLite."""
import ast
import importlib.util
import json
import re
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace

SOURCE=Path("/home/tmpadmin/spotbot/ai/collector.py")
MIRROR=Path("/home/tmpadmin/spotbot/ai/mirror_outbox.py")
src=SOURCE.read_text(encoding="utf-8")
tree=ast.parse(src)
names={"_finite_number","_rollup_raw_rows","prune_raw"}
funcs=[f for f in tree.body if isinstance(f,ast.FunctionDef) and f.name in names]
assert {f.name for f in funcs}==names
spec=importlib.util.spec_from_file_location("r496_orig_mirror",MIRROR)
mirror=importlib.util.module_from_spec(spec);spec.loader.exec_module(mirror)
scope={"json":json,"Decimal":Decimal,"InvalidOperation":InvalidOperation,
       "mirror_outbox":mirror}
exec(compile(ast.Module(body=funcs,type_ignores=[]),str(SOURCE),"exec"),scope)
schema=re.search(
 r'con\.execute\("""(CREATE TABLE IF NOT EXISTS hourly_raw_rollups[\s\S]*?)"""\)',src)
assert schema
NOW=1_792_660_000_000
OLD=NOW-8*86_400_000
events=[
 {"id":"r496_s1","cat":"signals","at":OLD,
 "payload":{"raw_spread_bps":"1.20","net_spread_bps":"0.50",
 "telemetry_age_sec":"2.0","depth_spot_usdt":"100.50","depth_fut_usdt":"25.00"}},
 {"id":"r496_s2","cat":"signals","at":OLD+1000,
 "payload":{"raw_spread_bps":"2.30","net_spread_bps":"-1.00",
 "telemetry_age_sec":"3.5","depth_spot_usdt":"25.00","depth_fut_usdt":"30.00"}},
 {"id":"r496_s3","cat":"signals","at":OLD+2000,
 "payload":{"raw_spread_bps":"-0.30","net_spread_bps":"0.25",
 "telemetry_age_sec":"1.50","depth_spot_usdt":"10.25","depth_fut_usdt":"15.75"}},
 {"id":"r496_d1","cat":"decisions","at":OLD+3600000,
 "payload":{"decision":"skip","skip_reason":"spread"}}
]
def execute(initial):
 con=sqlite3.connect(":memory:");con.row_factory=sqlite3.Row
 con.execute("""CREATE TABLE events(event_id TEXT PRIMARY KEY,
 occurred_at_ms INTEGER,received_at_ms INTEGER,mode TEXT,base TEXT,
 category TEXT,name TEXT,reason_code TEXT,correlation_id TEXT,signal_id TEXT,
 payload_json TEXT)""")
 con.execute("CREATE TABLE decisions(event_id TEXT PRIMARY KEY,decision TEXT,skip_reason TEXT)")
 con.execute(schema.group(1))
 con.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)")
 con.execute("INSERT INTO metadata VALUES('rolled_up_raw_events',?)",(str(initial),))
 for i,e in enumerate(events):
  category=e["cat"]; name="SIGNAL_SEEN" if category=="signals" else "DECISION_MADE"
  con.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?)",
   (e["id"],e["at"],OLD+i,"paper","SOL",category,name,None,None,None,
    json.dumps(e["payload"],ensure_ascii=False)))
  if category=="decisions":
   con.execute("INSERT INTO decisions VALUES(?,?,?)",
    (e["id"],e["payload"]["decision"],e["payload"]["skip_reason"]))
 steps=[]
 for _ in range(2):
  ops=[]
  count=scope["prune_raw"](con,SimpleNamespace(raw_retention_days=7,prune_batch=2),
     timestamp=NOW,pressure=False,operations=ops)
  assert count==2
  rows=[dict(x) for x in con.execute(
    "SELECT * FROM hourly_raw_rollups ORDER BY hour_start_ms,base,category,name")]
  meta=con.execute("SELECT value FROM metadata WHERE key='rolled_up_raw_events'").fetchone()[0]
  steps.append({"deleted":count,"operations":ops,
    "operations_canonical":mirror.canonical(ops),
    "operations_sha256":mirror.sha256(mirror.canonical(ops)),
    "rollups":rows,"rolled_up_raw_events":meta})
 assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0]==0
 con.close()
 return {"initial":initial,"steps":steps}
out={"events":events,"now_ms":NOW,
     "baseline_zero":execute(0),"baseline_five":execute(5),
     "source":"original collector.py prune_raw twice + mirror_outbox.py"}
Path(__file__).with_name("native_r496_python_multibatch_oracle.json").write_text(
 json.dumps(out,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8")
print("R496_ORACLE_PASS",[(k,[step["rolled_up_raw_events"] for step in out[k]["steps"]])
     for k in ("baseline_zero","baseline_five")])
