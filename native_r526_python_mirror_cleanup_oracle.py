"""Original Python mirror_outbox cleanup/state limits on in-memory SQLite only."""
import importlib.util
import json
import sqlite3
from pathlib import Path
SOURCE=Path("/home/tmpadmin/spotbot/ai/mirror_outbox.py")
spec=importlib.util.spec_from_file_location("r526_original_mirror",SOURCE)
mirror=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mirror)
db=sqlite3.connect(":memory:")
db.row_factory=sqlite3.Row
db.execute("BEGIN IMMEDIATE")
mirror.ensure_schema(db)
db.commit()
def control():
 return dict(db.execute("""SELECT state,reason,latest_seq,cleanup_through,
 retained_bytes,retained_records,failure_seq,failure_reason
 FROM mirror_control WHERE id=1""").fetchone())
def count():
 return db.execute("SELECT COUNT(*) FROM mirror_outbox").fetchone()[0]
empty=control()
assert empty["state"]=="BOOTSTRAP_REQUIRED"
# The in-memory source lacks a file path; disable ONLY source disk-reserve
# guard. This oracle cannot certify filesystem pressure behavior.
original_guard=mirror._storage_reason
mirror._storage_reason=lambda *args,**kwargs: None
try:
 for i in range(130):
  with db:
   db.execute('BEGIN IMMEDIATE')
   seq=mirror.record(db,"event_append",[{
    "table":"events","op":"insert","key":{"event_id":f"synthetic_{i:03}"},
    "row":{"event_id":f"synthetic_{i:03}"},
    "after_hash":mirror.sha256(mirror.canonical(
      {"event_id":f"synthetic_{i:03}"}) )
   }])
   assert seq==i+1
finally:
 mirror._storage_reason=original_guard
before=control()
assert before["latest_seq"]==130 and before["retained_records"]==130
assert count()==130
try:
 mirror.cleanup(db,130)
except ValueError as exc:
 no_transaction=str(exc)
else: raise AssertionError("cleanup without BEGIN allowed")
assert count()==130
db.execute("BEGIN IMMEDIATE")
rollback_count=mirror.cleanup(db,130)
assert rollback_count==128
assert count()==2
db.rollback()
assert count()==130 and control()==before
with db:
 db.execute('BEGIN IMMEDIATE')
 first=mirror.cleanup(db,129)
post_first=control()
assert first==128 and count()==2
assert post_first["cleanup_through"]==128 and post_first["retained_records"]==2
with db:
 db.execute('BEGIN IMMEDIATE')
 second=mirror.cleanup(db,129)
post_second=control()
assert second==1 and count()==1 and post_second["cleanup_through"]==129
with db:
 db.execute('BEGIN IMMEDIATE')
 third=mirror.cleanup(db,130)
post_third=control()
assert third==1 and count()==0 and post_third["cleanup_through"]==130
assert post_third["retained_records"]==0 and post_third["retained_bytes"]==0
with db:
 db.execute('BEGIN IMMEDIATE')
 fourth=mirror.cleanup(db,130)
assert fourth==0
# REBUILD_REQUIRED: source suppresses mirror commits, not trade DB1 changes.
with db:
 db.execute("""UPDATE mirror_control SET state='REBUILD_REQUIRED',
 reason='SOURCE_TEST',failure_seq=130,failure_reason='SOURCE_TEST' WHERE id=1""")
rebuild_before=control()
with db:
 db.execute('BEGIN IMMEDIATE')
 suppressed=mirror.record(db,"event_append",[{"table":"events","op":"delete",
  "key":{"event_id":"ignore"}}])
rebuild_after=control()
assert suppressed is None and rebuild_after==rebuild_before and count()==0
# >4 MiB source payload forces REBUILD_REQUIRED *before* outbox INSERT.
with db:
 db.execute("UPDATE mirror_control SET state='NORMAL',reason=NULL WHERE id=1")
huge=[{"table":"events","op":"insert","key":{"event_id":"over"},
 "row":{"event_id":"over","text":"x"*(mirror.MAX_TRANSACTION_BYTES+8)},
 "after_hash":"synthetic"}]
with db:
 db.execute('BEGIN IMMEDIATE')
 oversize=mirror.record(db,"event_append",huge)
oversize_after=control()
assert oversize is None
assert oversize_after["state"]=="REBUILD_REQUIRED"
assert oversize_after["reason"]=="OUTBOX_TRANSACTION_TOO_LARGE"
assert oversize_after["failure_seq"]==130 and count()==0
# Source mirror_outbox.cleanup trusts supplied applied_seq, and does NOT
# authenticate DB2 receipt inside this function. Explicit release blocker.
result={"initial":empty,"before":before,"rollback_count":rollback_count,
 "no_transaction_error":no_transaction,
 "first":{"deleted":first,"control":post_first},
 "second":{"deleted":second,"control":post_second},
 "third":{"deleted":third,"control":post_third},
 "fourth_deleted":fourth,"remaining":count(),"rebuild_before":rebuild_before,
 "rebuild_after":rebuild_after,"suppressed":suppressed,
 "oversize":oversize_after,"oversize_suppressed":oversize,
 "cleanup_batch":mirror.CLEANUP_BATCH,
 "max_tx_bytes":mirror.MAX_TRANSACTION_BYTES,
 "critical_retained_bytes":mirror.CRITICAL_RETAINED_BYTES,
 "max_retained_bytes":mirror.MAX_RETAINED_BYTES,
 "source_note":"Original Python ensure_schema/record/cleanup in-memory. Storage guard bypass only for normal 130 inserts; oversize checked normally. DB2 ACK is caller responsibility."}
target=Path(__file__).with_name("native_r526_python_mirror_cleanup_oracle.json")
target.write_text(json.dumps(result,indent=2,sort_keys=True)+"\n",encoding="utf-8")
print("R526_PYTHON_OK",first,second,third,oversize_after["state"])
db.close()
