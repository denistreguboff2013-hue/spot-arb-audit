"""Execute unmodified collector prune_raw and mirror_outbox functions in memory."""
import ast
import importlib.util
import json
import re
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace

SRC = Path("/home/tmpadmin/spotbot/ai/collector.py")
MIRROR = Path("/home/tmpadmin/spotbot/ai/mirror_outbox.py")
source = SRC.read_text(encoding="utf-8")
tree = ast.parse(source)
names = {"_finite_number", "_rollup_raw_rows", "prune_raw"}
funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
assert {f.name for f in funcs} == names
spec = importlib.util.spec_from_file_location("original_mirror_outbox", MIRROR)
mirror = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mirror)
namespace = {"Decimal":Decimal,"InvalidOperation":InvalidOperation,
    "json":json,"mirror_outbox":mirror}
exec(compile(ast.Module(body=funcs,type_ignores=[]),str(SRC),"exec"),namespace)
schema = re.search(
 r'con\.execute\("""(CREATE TABLE IF NOT EXISTS hourly_raw_rollups[\s\S]*?)"""\)',
 source)
assert schema
NOW=1792660000000
OLD=NOW-8*86400000
def e(id,cat,correlation=None):
    stage = {"signals":"SIGNAL_SEEN","decisions":"DECISION_MADE",
             "entries":"ENTRY_ABORT"}[cat]
    payload = (
      {"raw_spread_bps":"1.25","net_spread_bps":"0.50",
       "depth_spot_usdt":"10.00"} if cat=="signals"
      else {"decision":"skip","skip_reason":"spread"} if cat=="decisions" else {})
    return {"id":id,"category":cat,"name":stage,"base":"SOL",
     "correlation_id":correlation,"signal_id":None,
     "occurred_at_ms":OLD,"received_at_ms":OLD,"payload":payload}

cases = {
 "plain":{"events":[e("r486_sig","signals"),e("r486_dec","decisions")]},
 "linked":{"events":[e("r487_protected","signals","corr_link"),
  e("r487_entry","entries","corr_link"),e("r487_dec","decisions")]},
}
def run(case):
    con=sqlite3.connect(":memory:")
    con.row_factory=sqlite3.Row
    con.execute("""CREATE TABLE events(
     event_id TEXT PRIMARY KEY,occurred_at_ms INTEGER,received_at_ms INTEGER,
     mode TEXT,base TEXT,category TEXT,name TEXT,reason_code TEXT,
     correlation_id TEXT,signal_id TEXT,payload_json TEXT)""")
    con.execute("CREATE TABLE decisions(event_id TEXT PRIMARY KEY,decision TEXT,skip_reason TEXT)")
    con.execute(schema.group(1))
    con.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)")
    con.execute("INSERT INTO metadata VALUES('rolled_up_raw_events','0')")
    for idx,row in enumerate(case["events"]):
        row["received_at_ms"]=OLD+idx
        con.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?)",(
         row["id"],row["occurred_at_ms"],row["received_at_ms"],
         "paper",row["base"],row["category"],row["name"],None,
         row["correlation_id"],row["signal_id"],
         json.dumps(row["payload"],ensure_ascii=False)))
        if row["category"]=="decisions":
            con.execute("INSERT INTO decisions VALUES(?,?,?)",(
             row["id"],row["payload"]["decision"],row["payload"]["skip_reason"]))
    operations=[]
    pruned=namespace["prune_raw"](con,SimpleNamespace(raw_retention_days=7,prune_batch=500),
      timestamp=NOW,pressure=False,operations=operations)
    output={"now_ms":NOW,"events":case["events"],"pruned":pruned,
      "operations":operations,
      "operations_canonical":mirror.canonical(operations),
      "operations_sha256":mirror.sha256(mirror.canonical(operations)),
      "remaining_event_ids":[x["event_id"] for x in
        con.execute("SELECT event_id FROM events ORDER BY event_id")],
      "rollups":[dict(r) for r in con.execute(
        "SELECT * FROM hourly_raw_rollups ORDER BY hour_start_ms,base,category,name")],
      "metadata":dict(con.execute("SELECT key,value FROM metadata").fetchone())}
    assert len(operations)==len(output["rollups"])+2*pruned+1
    assert output["metadata"]["value"]==str(pruned)
    con.close()
    return output

data={"plain":run(cases["plain"]),"linked":run(cases["linked"]),
      "source":"collector.py:prune_raw+_rollup_raw_rows; mirror_outbox.py:after+deleted"}
out=Path(__file__).with_name("native_r486_python_retention_operations_oracle.json")
out.write_text(json.dumps(data,ensure_ascii=False,sort_keys=True,indent=2)+"\n",
               encoding="utf-8")
print("ORACLE_PASS", [(k,len(data[k]["operations"]),data[k]["pruned"])
                       for k in ("plain","linked")])
