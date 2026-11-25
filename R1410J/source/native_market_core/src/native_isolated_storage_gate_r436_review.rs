// R436-R445: private isolated SQLite PAPER admission.
// Independent of R47 read-only production YAML review; never authorizes trading.
const R436_DB_LIMIT:u64=128*1024*1024;
const R436_WAL_LIMIT:u64=16*1024*1024;
const R436_FREE_RESERVE:u64=512*1024*1024;
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
struct R436StorageSample{
 used_db:u64, wal:u64, available:u64,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
enum R436StorageState{Normal,Warning,High,Critical}
fn r436_admit_sample(s:R436StorageSample)
 ->Result<R436StorageState,EventOutboxR366Error>{
 let ratio=s.used_db as u128*100;
 let cap=R436_DB_LIMIT as u128;
 let level=if ratio>=cap*95{R436StorageState::Critical}
  else if ratio>=cap*85{R436StorageState::High}
  else if ratio>=cap*70{R436StorageState::Warning}
  else{R436StorageState::Normal};
 // Stricter than Python: critical page usage denies BEFORE SQLite full.
 // No blind checkpoint when a reader could be pinning WAL.
 if s.available<R436_FREE_RESERVE||
    s.wal>=R436_WAL_LIMIT-1_048_576||
    level==R436StorageState::Critical{
  return Err(EventOutboxR366Error::Pressure)
 }
 Ok(level)
}
fn r436_private_sample(path:&Path)->Result<R436StorageSample,EventOutboxR366Error>{
 // r366_open enforces owned /tmp directory, 0600 file and no hard links.
 let db=r366_open(path)?;
 let page=r366_query_num(&db,"PRAGMA page_size")?;
 let count=r366_query_num(&db,"PRAGMA page_count")?;
 let free_pages=r366_query_num(&db,"PRAGMA freelist_count")?;
 if page==0||free_pages>count{return Err(EventOutboxR366Error::Corrupt)}
 let used_db=count.checked_sub(free_pages)
  .and_then(|n|n.checked_mul(page)).ok_or(EventOutboxR366Error::Corrupt)?;
 // Keep connection alive while measuring WAL; last-close can checkpoint it.
 let wal_path=path.with_file_name("astra-r366-event-outbox.sqlite-wal");
 let wal=match fs::symlink_metadata(&wal_path){
  Ok(meta)=>{
   if !meta.file_type().is_file()||meta.nlink()!=1||
    meta.uid()!=unsafe{libc::geteuid()}||
    meta.mode()&0o077!=0{return Err(EventOutboxR366Error::Path)}
   meta.len()
  },
  Err(e) if e.kind()==std::io::ErrorKind::NotFound=>0,
  Err(_)=>return Err(EventOutboxR366Error::Path)
 };
 let parent=path.parent().ok_or(EventOutboxR366Error::Path)?;
 let c=std::ffi::CString::new(parent.to_string_lossy().as_bytes())
  .map_err(|_|EventOutboxR366Error::Path)?;
 let mut disk:libc::statvfs=unsafe{std::mem::zeroed()};
 if unsafe{libc::statvfs(c.as_ptr(),&mut disk)}!=0{
  return Err(EventOutboxR366Error::Path)
 }
 let available=(disk.f_bavail as u128)*(disk.f_frsize as u128);
 let available=u64::try_from(available).map_err(|_|EventOutboxR366Error::Schema)?;
 Ok(R436StorageSample{used_db,wal,available})
}
fn r436_admit_private(path:&Path)->Result<(),EventOutboxR366Error>{
 r436_admit_sample(r436_private_sample(path)?)?;
 Ok(())
}
fn r436_install_private_quotas(db:&Db)->Result<(),EventOutboxR366Error>{
 let page=r366_query_num(db,"PRAGMA page_size")?;
 if page<512||page>65_536{return Err(EventOutboxR366Error::Schema)}
 let pages=R436_DB_LIMIT/page;
 if pages==0{return Err(EventOutboxR366Error::Schema)}
 exec(db,&format!("PRAGMA max_page_count={pages}; PRAGMA journal_size_limit={R436_WAL_LIMIT};"))
  .map_err(r366_error)?;
 let effective=r366_query_num(db,"PRAGMA max_page_count")?;
 if effective>pages{return Err(EventOutboxR366Error::Schema)}
 Ok(())
}
#[cfg(test)]
mod r436_tests{
 use super::*;
 use super::r366_tests::{fixture_root,record};
 use serde_json::json;
 fn good(used_db:u64,wal:u64,available:u64)->R436StorageSample{
  R436StorageSample{used_db,wal,available}
 }
 #[test]fn r436_exact_python_pressure_levels_and_stricter_write_stop(){
  for (percent,level) in [(0,R436StorageState::Normal),
    (69,R436StorageState::Normal),(70,R436StorageState::Warning),
    (84,R436StorageState::Warning),(85,R436StorageState::High),
    (94,R436StorageState::High)]{
   let bytes=((R436_DB_LIMIT as u128*percent+99)/100) as u64;
   assert_eq!(r436_admit_sample(good(bytes,0,R436_FREE_RESERVE)),Ok(level));
  }
  for p in [95_u128,100,150]{
   let bytes=((R436_DB_LIMIT as u128*p+99)/100) as u64;
   assert_eq!(r436_admit_sample(good(bytes,0,R436_FREE_RESERVE)),
      Err(EventOutboxR366Error::Pressure));
  }
 }
 #[test]fn r437_wal_threshold_refuses_at_exact_limit_minus_mib(){
  let cut=R436_WAL_LIMIT-1_048_576;
  assert_eq!(r436_admit_sample(good(0,cut-1,R436_FREE_RESERVE)),
   Ok(R436StorageState::Normal));
  for wal in [cut,cut+1,R436_WAL_LIMIT]{
   assert_eq!(r436_admit_sample(good(0,wal,R436_FREE_RESERVE)),
    Err(EventOutboxR366Error::Pressure));
  }
 }
 #[test]fn r438_missing_free_reserve_denies_without_disk_writes(){
  assert_eq!(r436_admit_sample(good(0,0,R436_FREE_RESERVE-1)),
    Err(EventOutboxR366Error::Pressure));
  assert_eq!(r436_admit_sample(good(0,0,R436_FREE_RESERVE)),
    Ok(R436StorageState::Normal));
 }
 #[test]fn r439_isolated_sqlite_page_quota_and_observation(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let db=r366_open(&w.path).unwrap();
  let page=r366_query_num(&db,"PRAGMA page_size").unwrap();
  let max=r366_query_num(&db,"PRAGMA max_page_count").unwrap();
  assert!(page>=512&&max*page<=R436_DB_LIMIT);
  drop(db);
  let snapshot=r436_private_sample(&w.path).unwrap();
  assert!(snapshot.used_db<R436_DB_LIMIT);
  assert!(snapshot.available>R436_FREE_RESERVE);
  assert!(r436_admit_private(&w.path).is_ok());
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r440_preflight_used_by_both_private_writer_paths(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.r435_run_change("start","r440_run",1_792_660_000_000,&json!({})).unwrap();
  w.append(&record("r440_decision","decisions"),1_792_660_000_001,false).unwrap();
  w.r435_run_change("close","r440_run",1_792_660_000_002,
    &json!({"committed":1})).unwrap();
  drop(w);
  let re=IsolatedEventOutboxR366::reopen(&root).unwrap();
  assert_eq!((re.audit().unwrap().events,re.audit().unwrap().outbox),(1,1));
  drop(re);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r441_nonisolated_path_rejected_before_disk_observation(){
  assert_eq!(r436_private_sample(&std::env::temp_dir().join("unowned.sqlite")),
    Err(EventOutboxR366Error::Path));
  assert_eq!(r436_admit_private(&std::env::temp_dir().join("unowned.sqlite")),
    Err(EventOutboxR366Error::Path));
 }
}
