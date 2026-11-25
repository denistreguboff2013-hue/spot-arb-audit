"""R516 independent Python mirror_outbox oracle, entirely sqlite3 :memory:."""
import importlib.util
import json
import sqlite3
from pathlib import Path

SOURCE=Path("/home/tmpadmin/spotbot/ai/mirror_outbox.py")
spec=importlib.util.spec_from_file_location("r516_original_python_mirror",SOURCE)
mirror=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mirror)
con=sqlite3.connect(":memory:")
con.row_factory=sqlite3.Row
con.execute("BEGIN IMMEDIATE")
mirror.ensure_schema(con)
con.execute("""CREATE TABLE attempts(
 correlation_id TEXT PRIMARY KEY,mode TEXT NOT NULL)""")
con.execute("""CREATE TABLE events(
 event_id TEXT PRIMARY KEY,mode TEXT NOT NULL,category TEXT NOT NULL,
 name TEXT NOT NULL,payload_json TEXT NOT NULL)""")
con.execute("""CREATE TABLE decisions(
 event_id TEXT PRIMARY KEY,decision TEXT,skip_reason TEXT)""")
con.execute("INSERT INTO attempts VALUES ('attempt_r516','paper')")
con.execute("""INSERT INTO events VALUES
 ('r516_one','paper','decisions','DECISION_MADE','{"decision":"hold"}')""")
con.execute("INSERT INTO decisions VALUES('r516_one','hold','spread')")
operations=[
 mirror.after(con,"attempts",{"correlation_id":"attempt_r516"},"insert"),
 mirror.after(con,"events",{"event_id":"r516_one"},"insert"),
 mirror.after(con,"decisions",{"event_id":"r516_one"},"insert")]
frame={"protocol_version":mirror.PROTOCOL_VERSION,
 "schema_generation":mirror.SCHEMA_GENERATION,
 "tx_kind":"event_append","operations":operations}
canonical=mirror.canonical(frame)
rowhash=mirror.sha256(canonical)
# Python .record original logic, bypass only its disk reserve guard because
# :memory: has no file-backed path; NO production memory/disk semantics claimed.
saved_guard=mirror._storage_reason
mirror._storage_reason=lambda *args,**kwargs: None
try:
 seq=mirror.record(con,"event_append",operations)
finally:
 mirror._storage_reason=saved_guard
record=dict(con.execute("""SELECT seq,prev_seq,tx_kind,schema_generation,
 payload_json,payload_hash,payload_bytes,feed_generation FROM mirror_outbox
 WHERE seq=?""",(seq,)).fetchone())
control=dict(con.execute("""SELECT latest_seq,retained_bytes,retained_records,
 state,feed_generation FROM mirror_control WHERE id=1""").fetchone())
assert seq==1 and rowhash==record["payload_hash"]
assert record["payload_bytes"]==len(canonical.encode("utf-8"))+384
assert control["retained_records"]==1
out={"frame":frame,"operations":operations,"canonical":canonical,
 "sha256":rowhash,"payload_bytes":record["payload_bytes"],
 "record":record,"control":control,
 "protocol_version":mirror.PROTOCOL_VERSION,
 "schema_generation":mirror.SCHEMA_GENERATION,
 "states":["BOOTSTRAP_REQUIRED","NORMAL","REBUILD_REQUIRED"],
 "note":"Original Python canonical/after/record/ensure_schema; only private disk guard overridden for in-memory SQLite"}
dest=Path(__file__).with_name("native_r516_python_mirror_feed_oracle.json")
dest.write_text(json.dumps(out,indent=2,sort_keys=True,ensure_ascii=False)+"\n",encoding="utf-8")
print("R516_PYTHON_ORACLE_OK",seq,len(operations),record["payload_bytes"])
con.rollback()
con.close()
