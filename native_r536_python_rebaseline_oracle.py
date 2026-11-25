"""Independent ORIGINAL mirror_outbox state transition and storage oracle.
All SQLite mutations only in sqlite3 :memory:, no real DB1/DB2.
"""
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

src=Path("/home/tmpadmin/spotbot/ai/mirror_outbox.py")
spec=importlib.util.spec_from_file_location("r536_original_mirror",src)
mirror=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mirror)
con=sqlite3.connect(":memory:")
con.row_factory=sqlite3.Row
con.execute("BEGIN IMMEDIATE")
mirror.ensure_schema(con)
con.commit()
def control():
 return dict(con.execute("""SELECT state,reason,latest_seq,cleanup_through,
 retained_bytes,retained_records,feed_generation,baseline_seq,
 failure_seq,failure_reason,baseline_source_identity,
 baseline_schema_version,baseline_user_version,baseline_db1_bytes,
 baseline_db1_mtime_ns FROM mirror_control WHERE id=1""").fetchone())
original=control()
try:
 with con:
  con.execute("BEGIN IMMEDIATE")
  mirror.prepare_rebaseline(con,source_identity="sha256:old",schema_version=1,
   user_version=426,db1_bytes=4096,db1_mtime_ns=1234)
except ValueError as exc:
 not_rebuild_error=str(exc)
else: raise AssertionError("prepare_rebaseline allowed from BOOTSTRAP_REQUIRED")
assert not_rebuild_error=="rebaseline requires REBUILD_REQUIRED state"
guard=mirror._storage_reason
mirror._storage_reason=lambda *args,**kwargs:None
try:
 with con:
  con.execute("BEGIN IMMEDIATE")
  first=mirror.record(con,"event_append",
   [{"table":"events","op":"delete","key":{"event_id":"r536_one"}}])
finally:
 mirror._storage_reason=guard
assert first==1
before_rebuild=control()
with con:
 con.execute("""UPDATE mirror_control SET state='REBUILD_REQUIRED',
   reason='MIRROR_COMMIT_GAP',failure_seq=1,
   failure_reason='MIRROR_COMMIT_GAP' WHERE id=1""")
rebuild=control()
try:
 with con:
  con.execute("BEGIN IMMEDIATE")
  mirror.prepare_rebaseline(con,source_identity="",schema_version=1,
   user_version=426,db1_bytes=4096,db1_mtime_ns=1234)
except ValueError as exc: bad_meta_error=str(exc)
else: raise AssertionError("missing source identity accepted")
assert bad_meta_error=="invalid immutable rebaseline metadata"
assert control()==rebuild
with con:
 con.execute("BEGIN IMMEDIATE")
 result=mirror.prepare_rebaseline(con,
   source_identity="sha256:db1-frozen-example",schema_version=4,
   user_version=426,db1_bytes=32768,db1_mtime_ns=1700000000123456789)
pending=control()
assert pending["state"]=="BOOTSTRAP_REQUIRED"
assert pending["reason"]=="REBASELINE_PENDING"
assert pending["baseline_seq"]==1 and pending["latest_seq"]==1
assert pending["failure_seq"]==1
assert pending["failure_reason"]=="MIRROR_COMMIT_GAP"
assert pending["baseline_source_identity"]=="sha256:db1-frozen-example"
assert pending["baseline_schema_version"]==4
assert pending["baseline_user_version"]==426
assert pending["baseline_db1_bytes"]==32768
assert pending["baseline_db1_mtime_ns"]==1700000000123456789
assert result["feed_generation"].startswith("rebase-")
assert result["baseline_seq"]==1
assert pending["feed_generation"]==result["feed_generation"]
assert con.execute("SELECT COUNT(*) FROM mirror_outbox").fetchone()[0]==1
guard=mirror._storage_reason
mirror._storage_reason=lambda *args,**kwargs:None
try:
 with con:
  con.execute("BEGIN IMMEDIATE")
  next_seq=mirror.record(con,"event_append",
   [{"table":"events","op":"delete","key":{"event_id":"r536_two"}}])
finally: mirror._storage_reason=guard
assert next_seq==2
frame=dict(con.execute("SELECT seq,prev_seq,feed_generation,payload_hash FROM mirror_outbox WHERE seq=2").fetchone())
assert frame["prev_seq"]==1 and frame["feed_generation"]==pending["feed_generation"]
assert con.execute("SELECT feed_generation FROM mirror_outbox WHERE seq=1").fetchone()[0]=="legacy"
after=control()
assert after["baseline_seq"]==1 and after["latest_seq"]==2
# Directly testing original guard reasons with a fake DB path and mocked
# free-space value: never access real disk usage or real DB1.
real_source_path=mirror._source_path
real_disk_usage=mirror.shutil.disk_usage
mirror._source_path=lambda _:Path("/synthetic/in-memory-rebaseline.db")
mirror.shutil.disk_usage=lambda _:SimpleNamespace(total=20_000_000_000,used=0,free=20_000_000_000)
try:
 byte_limit=mirror._storage_reason(con,1,mirror.MAX_RETAINED_BYTES)
 no_pressure=mirror._storage_reason(con,128,0)
 mirror.shutil.disk_usage=lambda _:SimpleNamespace(total=20_000_000_000,used=19_000_000_000,free=1_000_000_000)
 low_free=mirror._storage_reason(con,128,0)
finally:
 mirror._source_path=real_source_path
 mirror.shutil.disk_usage=real_disk_usage
assert byte_limit=="OUTBOX_BYTE_LIMIT" and no_pressure is None
assert low_free=="FILESYSTEM_RESERVE"
out={"original":original,"before_rebuild":before_rebuild,
 "rebuild":rebuild,"pending":pending,"after":after,
 "rebaseline_result":result,"new_frame":frame,"first_seq":first,
 "new_seq":next_seq,"not_rebuild_error":not_rebuild_error,
 "bad_metadata_error":bad_meta_error,"byte_limit_reason":byte_limit,
 "storage_reserve_reason":low_free,"no_pressure_reason":no_pressure,
 "max_retained_bytes":mirror.MAX_RETAINED_BYTES,
 "critical_retained_bytes":mirror.CRITICAL_RETAINED_BYTES,
 "warning_retained_bytes":mirror.WARNING_RETAINED_BYTES,
 "free_reserve_bytes":mirror.FREE_RESERVE_BYTES,
 "max_transaction_bytes":mirror.MAX_TRANSACTION_BYTES,
 "source":"Original mirror_outbox.py ensure_schema,prepare_rebaseline,record,_storage_reason. Synthetic in-memory sqlite and fake filesystem."}
Path(__file__).with_name("native_r536_python_rebaseline_oracle.json").write_text(
 json.dumps(out,indent=2,ensure_ascii=False,sort_keys=True)+"\n",encoding="utf-8")
print("R536_PYTHON_OK",first,next_seq,pending["state"],byte_limit,low_free)
con.close()
