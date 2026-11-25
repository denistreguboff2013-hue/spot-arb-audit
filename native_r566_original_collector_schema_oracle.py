"""R566 exact original Python SQL schema oracle; only sqlite3 :memory: copies."""
import ast
import hashlib
import importlib.util
import json
import re
import sqlite3
from pathlib import Path

BASE=Path("/home/tmpadmin/spotbot/ai")
source_paths={name:BASE/name for name in (
 "collector.py","schemas.py","approval_ledger.py","mirror_outbox.py")}
raw={name:p.read_text(encoding="utf-8") for name,p in source_paths.items()}
def assignment(file,name):
 tree=ast.parse(raw[file])
 return ast.literal_eval(next(node.value for node in tree.body
  if isinstance(node,ast.Assign)
  and any(isinstance(t,ast.Name) and t.id==name for t in node.targets)))
fields=assignment("collector.py","FIELDS")
categories=assignment("schemas.py","CATEGORIES")
approval_schema=assignment("approval_ledger.py","SCHEMA")
table_keys=assignment("mirror_outbox.py","TABLE_KEYS")
assert len(table_keys)==17 and len(categories)==10
spec=importlib.util.spec_from_file_location("r566_orig_mirror",source_paths["mirror_outbox.py"])
mirror=importlib.util.module_from_spec(spec);spec.loader.exec_module(mirror)
main=re.search(r'        schema = """(.*?)"""',raw["collector.py"],re.S)
hourly=re.search(r'con\.execute\("""(CREATE TABLE IF NOT EXISTS hourly_raw_rollups.*?)"""\)',raw["collector.py"],re.S)
assert main and hourly
def build():
 con=sqlite3.connect(":memory:")
 con.row_factory=sqlite3.Row
 con.execute("PRAGMA foreign_keys=ON")
 for statement in main.group(1).split(";"):
  if statement.strip():con.execute(statement)
 for category in categories:
  columns=",".join('"' +f+ '" TEXT' for f in fields[category])
  con.execute(f"""CREATE TABLE IF NOT EXISTS {category}(
   event_id TEXT PRIMARY KEY, mode TEXT NOT NULL,
   is_exchange_verified INTEGER NOT NULL,{columns},
   FOREIGN KEY(event_id,mode,is_exchange_verified)
   REFERENCES events(event_id,mode,is_exchange_verified) ON DELETE CASCADE)""")
 con.execute(hourly.group(1))
 for statement in approval_schema:con.execute(statement)
 mirror.ensure_schema(con)
 con.execute("PRAGMA user_version=4")
 return con
def canonical(o):
 return json.dumps(o,sort_keys=True,ensure_ascii=False,
                   separators=(",",":"),allow_nan=False)
def sha(o):return hashlib.sha256(canonical(o).encode()).hexdigest()
def table_meta(con,name):
 quoted='"'+name+'"'
 ddl=con.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                 (name,)).fetchone()
 if not ddl:raise AssertionError("missing table "+name)
 columns=[list(r) for r in con.execute(f"PRAGMA table_info({quoted})")]
 fks=[list(r) for r in con.execute(f"PRAGMA foreign_key_list({quoted})")]
 indexes=[{"name":r[1],"unique":r[2],"origin":r[3],
  "partial":r[4],"definition":(
   (con.execute("SELECT sql FROM sqlite_master WHERE name=?",(r[1],)).fetchone() or [None])[0]),
  "parts":[list(x) for x in con.execute(f'PRAGMA index_xinfo("{r[1]}")')]}
  for r in con.execute(f"PRAGMA index_list({quoted})")]
 triggers=[list(r) for r in con.execute(
  "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? ORDER BY name",(name,))]
 key=list(table_keys.get(name,()))
 if key:
  known=[c[1] for c in columns]
  assert all(x in known for x in key)
 return {"table":name,"ddl":ddl[0],"columns":columns,"foreign_keys":fks,
  "indexes":sorted(indexes,key=lambda v:v["name"]),
  "triggers":triggers,"mirror_key":key}
def snapshot(con):
 tables=[]
 for name in sorted(table_keys):
  schema=table_meta(con,name)
  key=table_keys[name]
  quoted=", ".join('"'+k+'"' for k in key)
  records=[{"key":[row[k] for k in key],"row":dict(row)}
   for row in con.execute('SELECT * FROM "'+name+'" ORDER BY '+quoted)]
  tables.append({"schema":schema,"row_count":len(records),"rows":records})
 return {"user_version":con.execute("PRAGMA user_version").fetchone()[0],
  "table_count":len(tables),"total_rows":sum(t["row_count"] for t in tables),
  "tables":tables}
def populate(con,reverse=False):
 stmts=[
  ("INSERT INTO metadata VALUES (?,?)",("rolled_up_raw_events","2")),
  ("INSERT INTO attempts VALUES (?,?)",("corr-r566","paper")),
  ("INSERT INTO collector_runs VALUES (?,?,?,?,?)",
   ("run-r566",100,101,0,'{"events":1}')),
  ("INSERT INTO events(event_id,schema_version,mode,category,name,source,run_id,"
   "occurred_at_ms,received_at_ms,correlation_id,verification_status,"
   "is_exchange_verified,payload_json,verification_json,content_hash)"
   " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
   ("r566_event",1,"paper","decisions","DECISION_MADE","oracle","run-r566",
    100,101,"corr-r566","observed",0,'{"decision":"skip"}','{}',"sha256-fixture")),
  ("INSERT INTO decisions(event_id,mode,is_exchange_verified,decision,skip_reason)"
   " VALUES (?,?,?,?,?)",("r566_event","paper",0,"skip","spread")),
  ("INSERT INTO hourly_raw_rollups(hour_start_ms,mode,base,category,name,event_count)"
   " VALUES (?,?,?,?,?,?)",(0,"paper","SOL","decisions","DECISION_MADE",1)),
 ]
 # Reverse changes independent-key table insertion, not FK prerequisites.
 if reverse:stmts[:3]=list(reversed(stmts[:3]))
 for statement,params in stmts:con.execute(statement,params)
db1=build();db2=build()
populate(db1);populate(db2,reverse=True)
base1=snapshot(db1);base2=snapshot(db2)
assert base1==base2 and base1["table_count"]==17
assert base1["total_rows"]==6
baseline=sha(base1)
tests={}
tests["same_all_17_tables"]=base1==base2
tests["independent_insert_order"]=sha(base1)==sha(base2)
tests["source_version"]=base1["user_version"]==4
tests["mirrored_keys"]={x["schema"]["table"]:x["schema"]["mirror_key"]
                        for x in base1["tables"]}
assert tests["mirrored_keys"]["hourly_raw_rollups"]==list(table_keys["hourly_raw_rollups"])
assert len(tests["mirrored_keys"])==17
def check_mutation(sql,expected="mismatch"):
 con=build();populate(con);con.commit()
 before=sha(snapshot(con))
 try:
  con.execute("BEGIN IMMEDIATE")
  con.execute(sql)
  during=sha(snapshot(con))
  assert during!=baseline
  con.rollback()
 except:
  con.rollback()
  raise
 assert sha(snapshot(con))==before==baseline
 # Commit the same mutation to test persistent divergence.
 con.execute(sql)
 result=sha(snapshot(con))!=baseline
 con.close()
 return result
tests["missing_row"]=check_mutation(
 "DELETE FROM metadata WHERE key='rolled_up_raw_events'")
tests["changed_equal_count"]=check_mutation(
 "UPDATE metadata SET value='3' WHERE key='rolled_up_raw_events'")
tests["schema_column_altered"]=check_mutation(
 "ALTER TABLE metadata ADD COLUMN extra TEXT")
tests["index_removed"]=check_mutation("DROP INDEX events_signal")
tests["immutable_trigger_removed"]=check_mutation(
 "DROP TRIGGER advisor_transitions_no_delete")
tests["composite_pk_row_tamper"]=check_mutation(
 "UPDATE hourly_raw_rollups SET reason_code='changed'")
tests["receiver_extra_row"]=check_mutation(
 "INSERT INTO metadata VALUES('unmatched','value')")
# A staged identity based only on an event outbox prefix cannot authenticate
# a complete content+schema snapshot. Every changed row/schema changes digest.
result={"source_sha256":{name:hashlib.sha256(data.encode()).hexdigest()
                        for name,data in raw.items()},
 "schema_version":4,"mirrored_tables":17,"source_keys":tests["mirrored_keys"],
 "control_tables":["mirror_outbox","mirror_control"],
 "table_schema":[part["schema"] for part in base1["tables"]],
 "fixture_rows":base1["total_rows"],"fixture_snapshot_sha256":baseline,
 "fixture_snapshot":base1,
 "checks":{k:v for k,v in tests.items() if k!="mirrored_keys"},
 "scope":"Original Python source SQL assembled solely in two independent in-memory sqlite3 DBs; synthetic rows, no production DB access"}
assert all(result["checks"].values())
dest=Path(__file__).with_name("native_r566_original_collector_schema_oracle.json")
dest.write_text(json.dumps(result,indent=2,sort_keys=True,ensure_ascii=False)+"\n",
                encoding="utf-8")
print("R566_SCHEMA_ORACLE_PASS",len(result["table_schema"]),
      base1["total_rows"],len(result["checks"]),baseline)
db1.close();db2.close()
