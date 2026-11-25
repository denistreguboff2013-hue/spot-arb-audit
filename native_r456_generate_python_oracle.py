"""Independent Python SQLite oracle from original collector AST. No live DB."""
import ast
import json
import re
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path

SOURCE = Path("/home/tmpadmin/spotbot/ai/collector.py")
source = SOURCE.read_text(encoding="utf-8")
tree = ast.parse(source)
names = {"_finite_number", "_rollup_raw_rows"}
functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
assert {f.name for f in functions} == names
namespace = {"Decimal": Decimal, "InvalidOperation": InvalidOperation, "json": json}
exec(compile(ast.Module(body=functions, type_ignores=[]), str(SOURCE), "exec"), namespace)
match = re.search(
    r'con\.execute\("""(CREATE TABLE IF NOT EXISTS hourly_raw_rollups[\s\S]*?)"""\)',
    source,
)
assert match, "original schema not found"

now = 1792660000000
old = now - 8*86400000
samples = [
    {"id":"r456_s1", "category":"signals", "name":"SIGNAL_SEEN",
     "base":"SOL", "reason_code":None, "occurred":old,
     "payload":{"raw_spread_bps":"1.20", "net_spread_bps":"-0.25",
       "telemetry_age_sec":"2.0", "depth_spot_usdt":"100.50",
       "depth_fut_usdt":"95.25"}},
    {"id":"r456_s2", "category":"signals", "name":"SIGNAL_SEEN",
     "base":"SOL", "reason_code":None, "occurred":old+1000,
     "payload":{"raw_spread_bps":"2.30", "net_spread_bps":"0.75",
       "telemetry_age_sec":"3.5", "depth_spot_usdt":"25.00",
       "depth_fut_usdt":"30.00"}},
    {"id":"r456_s3", "category":"signals", "name":"SIGNAL_SEEN",
     "base":"SOL", "reason_code":None, "occurred":old+2000,
     "payload":{"raw_spread_bps":"not_numeric", "net_spread_bps":None,
       "telemetry_age_sec":"NaN", "depth_spot_usdt":"Infinity",
       "depth_fut_usdt":"-0.25"}},
    {"id":"r456_d1", "category":"decisions", "name":"DECISION_MADE",
     "base":"SOL", "reason_code":"SPREAD_COLLAPSED", "occurred":old+3600000,
     "payload":{"decision":"skip","skip_reason":"spread"}},
]
con = sqlite3.connect(":memory:")
con.row_factory = sqlite3.Row
con.execute("""CREATE TABLE events(
 event_id TEXT PRIMARY KEY,occurred_at_ms INTEGER,mode TEXT,base TEXT,
 category TEXT,name TEXT,reason_code TEXT,payload_json TEXT)""")
con.execute("CREATE TABLE decisions(event_id TEXT PRIMARY KEY,decision TEXT,skip_reason TEXT)")
con.execute(match.group(1))
for item in samples:
    con.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?,?)", (
      item["id"],item["occurred"],"paper",item["base"],item["category"],
      item["name"],item["reason_code"],json.dumps(item["payload"],ensure_ascii=False)))
    if item["category"] == "decisions":
        con.execute("INSERT INTO decisions VALUES(?,?,?)",(
          item["id"],item["payload"].get("decision"),item["payload"].get("skip_reason")))
assert namespace["_rollup_raw_rows"](con,[x["id"] for x in samples])==len(samples)
rows = [dict(x) for x in con.execute(
    "SELECT * FROM hourly_raw_rollups ORDER BY hour_start_ms,mode,base,category,name")]
out={"now_ms":now,"samples":samples,"rows":rows,"source_function":"collector.py:_rollup_raw_rows"}
Path(__file__).with_name("native_r456_python_rollup_oracle.json").write_text(
    json.dumps(out,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8")
print("ORACLE_PASS rows=",len(rows),"events=",len(samples))
