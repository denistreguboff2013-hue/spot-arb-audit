//! Permanent supervisor for PUBLIC WS paper episodes; no exchange credentials.
 //! Episodes deliberately do NOT claim recovered paper fills/positions.
 //! A broken episode exhausts the remaining paper-entry allowance for the
 //! EKB calendar day. No raw WebSocket storage. Bounded owned retention.
use serde_json::{Value,json};
use spotarb_native_market_core::native_paper_continuity_checkpoint_review::{
    inspect_planned_seal,inspect_checkpoint_model_alignment,
    inspect_interrupted_virtual_exposure,InterruptedVirtualExposure,
    load_planned_handoff,
};
use std::{
  ffi::CString,fs::{self,File,OpenOptions},
  io::{self,Write},os::fd::AsRawFd,os::unix::ffi::OsStrExt,
  path::Path,process::{Command,Stdio},
  thread,time::{Duration,SystemTime,UNIX_EPOCH},
};
const ROOT:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const EXEC:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/bin/current";
const KILL:&str="/home/tmpadmin/spotbot/data/real_kill_switch.json";
const SDB:&str="/mnt/spotarb_db_test";
const SDC:&str="/mnt/spotarb_disk2";
const KEEP_SECS:u64=30*86_400;
const CHECKPOINT:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/portfolio_checkpoint.json";
const MAX_SESSION_BYTES:u64=2*1024*1024;
const MAX_HISTORY_BYTES:u64=96*1024*1024;
const DAY_CAP:u64=500;
// R81: a day boundary must never silently clear a failed virtual handoff.
// Marker is outside rotating sessions and NOT removed by retention cleanup.
const UNRESOLVED_HOLD:&str="unresolved_virtual_crossday_hold.json";
const HOLD_SCHEMA:&str="spotarb-r81-unresolved-virtual-crossday-v1";
fn now_s()->u64{SystemTime::now().duration_since(UNIX_EPOCH)
    .unwrap_or_default().as_secs()}
fn yek_day(t:u64)->u64{(t+5*3600)/86_400}
fn until_yek_midnight(t:u64)->u64{86_400-(t+5*3600)%86_400}
fn fail(s:&str)->io::Error{io::Error::new(io::ErrorKind::Other,s)}
fn atomic_json(path:&Path,v:&Value)->io::Result<()>{
    let temp=path.with_extension("new");
    let bytes=serde_json::to_vec_pretty(v)?;
    let mut f=OpenOptions::new().create_new(true).write(true).open(&temp)?;
    f.write_all(&bytes)?;f.sync_all()?;drop(f);
    fs::rename(&temp,path)?;
    File::open(path.parent().ok_or_else(||fail("no parent"))?)?.sync_all()?;
    Ok(())
}
fn read_json(path:&Path)->io::Result<Value>{
    serde_json::from_slice(&fs::read(path)?)
        .map_err(|_|fail("bad json"))
}
fn mount_present(mount:&str)->bool{
    fs::read_to_string("/proc/self/mountinfo").map(|m|m.lines().any(|l|
       l.split_whitespace().nth(4)==Some(mount))).unwrap_or(false)
}
fn disk_ok(path:&str,min_free_gb:u64,max_used_pct:u64)->bool{
    let Ok(raw)=CString::new(Path::new(path).as_os_str().as_bytes()) else{return false};
    let mut st:libc::statvfs=unsafe{std::mem::zeroed()};
    if unsafe{libc::statvfs(raw.as_ptr(),&mut st)}!=0{return false}
    let total=st.f_blocks as u128*st.f_frsize as u128;
    let available=st.f_bavail as u128*st.f_frsize as u128;
    let free_inodes=st.f_favail as u128;
    let inode_total=st.f_files as u128;
    total>0&&available>=min_free_gb as u128*1024*1024*1024&&
      100*(total-available)/total <max_used_pct as u128&&
      (inode_total==0||free_inodes*100/inode_total>=10)
}
fn storage_ok()->bool{
    mount_present(SDB)&&mount_present(SDC)&&disk_ok("/",3,85)
       &&disk_ok(SDB,5,80)&&disk_ok(SDC,5,85)
}
fn safety_ok()->bool{
    read_json(Path::new(KILL)).ok()
      .and_then(|v|v.get("state").and_then(Value::as_str).map(str::to_owned))
      .as_deref()==Some("ENGAGED")
}
// Durable, LOCAL-ONLY quarantine: neither day rollover nor retention
// cleanup can release this incident; there is intentionally no auto-clear.
fn hold_exists(root:&Path)->io::Result<bool>{
 let p=root.join(UNRESOLVED_HOLD);
 let meta=match fs::symlink_metadata(&p){
  Err(e) if e.kind()==io::ErrorKind::NotFound=>return Ok(false),
  Err(e)=>return Err(e),Ok(m)=>m,
 };
 if !meta.file_type().is_file()||meta.len()==0||meta.len()>8_192{
  return Err(fail("untrusted unresolved PAPER hold file"))
 }
 let v=read_json(&p)?;
 if v["schema"]!=HOLD_SCHEMA||v["quarantined"]!=true||
    v["no_real_exchange_execution"]!=true||
    v["automatic_release_allowed"]!=false||
    v["incident_session"].as_str().unwrap_or("").is_empty(){
  return Err(fail("unresolved PAPER hold cannot be overridden by JSON"))
 }
 Ok(true)
}
fn write_hold_once(root:&Path,incident_day:u64,session:&str,
 evidence:&str,open_bases:&[String])->io::Result<()>{
 if hold_exists(root)?{return Ok(())}
 if session.is_empty()||open_bases.len()>10{
  return Err(fail("untrusted modeled incident identity"))
 }
 atomic_json(&root.join(UNRESOLVED_HOLD),&json!({
  "schema":HOLD_SCHEMA,"quarantined":true,
  "incident_day_yek":incident_day,"incident_session":session,
  "reason":"UNRESOLVED_VIRTUAL_PAPER_REQUIRES_MANUAL_REVIEW",
  "evidence_kind":evidence,"unresolved_model_bases":open_bases,
  "unresolved_count_unknown":open_bases.is_empty(),
  "no_real_exchange_execution":true,"automatic_release_allowed":false,
  "created_wall_s":now_s()
 }))
}
fn failed_handoff(v:&Value)->bool{
 v["handoff_check_result"]=="DENIED"&&(
   v["killed_for_safety"]==true||
   v["unrecovered_virtual_open_count_unknown"]==true||
   v["observed_unrecovered_virtual_open_positions"].as_u64().unwrap_or(0)>0||
   matches!(v["reason"].as_str(),
    Some("incomplete_or_failed_source_no_paper_entries_until_next_day"
      |"unrecovered_virtual_positions_paper_entries_quarantined"))
 )
}
// Also migrate evidence created BEFORE the R81 marker existed.
// Never infer "flat" from the old R80 pre-fix numeric zero.
fn unresolved_crossday_hold(root:&Path)->io::Result<bool>{
 if hold_exists(root)?{return Ok(true)}
 let mut examined=0usize;
 for entry in fs::read_dir(root.join("sessions"))?{
  let entry=entry?;
  if !entry.file_type()?.is_dir(){continue}
  let session=entry.file_name().to_string_lossy().to_string();
  if !session.starts_with("session_"){continue}
  examined+=1;
  if examined>512{return Err(fail("too many PAPER sessions for quarantine audit"))}
  let folder=entry.path();
  let report=match read_json(&folder.join("supervisor_summary.json")){
   Ok(v)=>v,
   Err(e) if e.kind()==io::ErrorKind::NotFound=>continue,
   Err(e)=>return Err(e),
  };
  if !failed_handoff(&report){continue}
  let day=report["day"].as_u64()
     .ok_or_else(||fail("failed PAPER episode has no day"))?;
  if report["session"].as_str()!=Some(session.as_str()){
   return Err(fail("failed PAPER episode session mismatch"))
  }
  let started_ms=session.rsplit('_').next()
     .and_then(|v|v.parse::<i64>().ok())
     .and_then(|sec|sec.checked_mul(1000)).unwrap_or(0);
  let evidence=inspect_interrupted_virtual_exposure(
     &folder.join("checkpoint_at_exit.json"),day as i64,started_ms);
  let bases=evidence.as_ref().map(|v|v.open_bases.clone())
      .unwrap_or_default();
  let kind=if evidence.is_ok(){
     "PRESERVED_UNSEALED_CHECKPOINT_LOCAL_ONLY"
  }else{"FAILED_PAPER_HANDOFF_SOURCE_INCOMPLETE"};
  write_hold_once(root,day,&session,kind,&bases)?;
  return Ok(true)
 }
 Ok(false)
}
// R82: explicit operator authorization is PAPER-only, never trading.
// The user authorized immediate isolated research epochs instead of
// waiting for the next Yekaterinburg calendar day after a failed source.
fn isolated_paper_research_authorized()->bool{
 std::env::var("PAPER_PUBLIC_ISOLATED_RESTART").ok().as_deref()
  ==Some("ALLOW_NEW_PUBLIC_PAPER_GENERATION_ONLY")
}
// Reconstruct *modeled* same-day entry high-water from historical
// CHECKPOINTS, not a synthetic DAY_CAP crash sentinel. Never claim
// real exchange volume or combine modeled PnL across generations.
fn modeled_day_highwater(root:&Path,day:u64)->io::Result<Option<u64>>{
 let mut best:Option<u64>=None;
 let mut visited=0usize;
 for entry in fs::read_dir(root.join("sessions"))?{
  let entry=entry?;
  if !entry.file_type()?.is_dir(){continue}
  let session=entry.file_name().to_string_lossy().to_string();
  if !session.starts_with(&format!("session_{day}_")){continue}
  visited+=1;
  if visited>512{return Err(fail("too many same-day PAPER sessions"))}
  let report=match read_json(&entry.path().join("supervisor_summary.json")){
   Ok(v)=>v,
   Err(e) if e.kind()==io::ErrorKind::NotFound=>Value::Null,
   Err(e)=>return Err(e),
  };
  if report["day"].as_u64().is_some_and(|d|d!=day){
   return Err(fail("PAPER historical session day mismatch"))
  }
  // The synthetic 500-entry restart is not genuine modeled activity.
  if report["started_with_entries"].as_u64()==Some(DAY_CAP){continue}
  let start_ms=session.rsplit('_').next()
    .and_then(|x|x.parse::<i64>().ok())
    .and_then(|sec|sec.checked_mul(1000)).unwrap_or(0);
  let evidence=inspect_interrupted_virtual_exposure(
     &entry.path().join("checkpoint_at_exit.json"),day as i64,start_ms);
  let n=match evidence{
   Ok(c)=>Some(c.observed_entries),
   Err(_)=>report["finished_with_entries"].as_u64().filter(|n|{
      *n<=DAY_CAP&&report["handoff_check_result"]=="PASS"
   }),
  };
  if let Some(n)=n{best=Some(best.unwrap_or(0).max(n));}
 }
 Ok(best)
}
// Genuine 500 per EKT day is retained. A failed-source *synthetic*
// cap can be reconstructed, but only with explicit PAPER-only opt-in.
fn isolated_paper_budget(root:&Path,ledger:&Path,day:u64,authorized:bool)
 ->io::Result<(u64,bool)>{
 let raw=match read_json(ledger){
  Err(e) if e.kind()==io::ErrorKind::NotFound=>return Ok((0,false)),
  Err(e)=>return Err(e),Ok(v)=>v,
 };
 if raw["day"].as_u64()!=Some(day){return Ok((0,false))}
 let current=raw["entries"].as_u64()
   .filter(|n|*n<=DAY_CAP).ok_or_else(||fail("untrusted PAPER daily budget"))?;
 let bad=matches!(raw["reason"].as_str(),
  Some("incomplete_or_failed_source_no_paper_entries_until_next_day"
   |"unrecovered_virtual_positions_paper_entries_quarantined"
   |"supervisor_io_error"|"failed_spawn_unknown"
   |"uncertain_restart_prior_paper_positions_quarantined"));
 if current!=DAY_CAP||!bad{return Ok((current,false))}
 if !authorized{return Ok((DAY_CAP,false))}
 let model_floor=modeled_day_highwater(root,day)?;
 if let Some(n)=model_floor{
  if n>=DAY_CAP{return Ok((DAY_CAP,false))}
  return Ok((n,true))
 }
 // A completely missing local modeled census cannot be presented as
 // a trustworthy calendar-day count. Treat as a NEW research generation.
 // This is PAPER simulation only, not a real-risk cap or certified day PnL.
 Ok((0,true))
}
fn paper_daily_budget_full(observed_entries:u64)->bool{
    observed_entries>=DAY_CAP
}
fn daily(path:&Path,day:u64)->u64{
    match read_json(path){
       Err(e) if e.kind()==io::ErrorKind::NotFound=>0,
       Err(_)=>DAY_CAP,
       Ok(v)=>{
           if v["day"].as_u64()==Some(day){
               v["entries"].as_u64().filter(|x|*x<=DAY_CAP).unwrap_or(DAY_CAP)
           }else{0}
       }
    }
}
fn save_daily(path:&Path,day:u64,n:u64,why:&str)->io::Result<()>{
    atomic_json(path,&json!({"day":day,"entries":n.min(DAY_CAP),
       "reason":why,"wall_s":now_s(),"paper_only":true}))
}
fn cleanup_sessions(root:&Path)->io::Result<()>{
    let dir=root.join("sessions");
    let mut kept=Vec::new();let time=now_s();
    for item in fs::read_dir(&dir)?{
       let x=item?;let p=x.path();
       if !x.file_name().to_string_lossy().starts_with("session_")||
          !fs::symlink_metadata(&p)?.file_type().is_dir(){continue}
       let modified=fs::metadata(&p)?.modified()?.duration_since(UNIX_EPOCH)
          .unwrap_or_default().as_secs();
       if time.saturating_sub(modified)>KEEP_SECS{
           fs::remove_dir_all(&p)?;continue
       }
       let mut bytes=0u64;
       for sub in fs::read_dir(&p)?{
          let file=sub?;
          let meta=fs::symlink_metadata(file.path())?;
          if meta.file_type().is_file(){bytes=bytes.saturating_add(meta.len())}
       }
       kept.push((modified,p,bytes));
    }
    kept.sort_by_key(|e|e.0);
    let mut sum:u64=kept.iter().map(|x|x.2).sum();
    for (_,path,size) in kept {
       if sum<=MAX_HISTORY_BYTES {break}
       fs::remove_dir_all(&path)?;
       sum=sum.saturating_sub(size);
    }
    Ok(())
}
// R1085: admission of a stopped PAPER episode MUST happen before any
// new session files, daily budget writes or source sockets. A previously
// stopped portfolio is not an empty book, even when >180s stale.
fn r1085_planned_handoff_preflight(root:&Path,baseline:u64)->io::Result<Value>{
 let carry=root.join("planned_handoff.json");
 let ckpt=root.join("portfolio_checkpoint.json");
 if !carry.exists(){
  if ckpt.exists(){
   return Err(fail("R1085 modeled checkpoint exists without planned handoff; no flat restart"))
  }
  return Ok(json!({"gate":"NEW_EMPTY_NO_CHECKPOINT","ready":true}))
 }
 if !ckpt.exists(){return Err(fail("R1085 handoff exists without checkpoint"))}
 let m=read_json(&carry)?;
 let old_day=m["day"].as_u64().ok_or_else(||fail("R1085 old day missing"))?;
 let old_count=m["entries"].as_u64().ok_or_else(||fail("R1085 old entries missing"))?;
 if m["validated_seal"]!=true||m["no_real_orders"]!=true||
    old_count>DAY_CAP||m["last_session"].as_str().map_or(true,str::is_empty){
  return Err(fail("R1085 unsigned/malformed planned virtual handoff"))
 }
 inspect_planned_seal(&ckpt,old_day as i64,old_count)
  .map_err(|_|fail("R1085 checkpoint seal/counters invalid"))?;
 let recovered=load_planned_handoff(
    &ckpt,spotarb_native_market_core::transport::monotonic_ns(),baseline)
    .map_err(|e|fail(&format!("R1085 stale/unsafe normal handoff: {e:?}")))?;
 if recovered.portfolio.stats.closed>recovered.portfolio.stats.entries||
    recovered.carried_open_positions>10||
    recovered.portfolio.positions.len()!=recovered.carried_open_positions{
  return Err(fail("R1085 recovered positions inconsistent"))
 }
 Ok(json!({"gate":"NORMAL_PLANNED_RESTORE","ready":true,
   "previous_revision":recovered.previous_revision,
   "carried_virtual_open":recovered.carried_open_positions,
   "original_entries":old_count,
   "next_day_budget":baseline,
   "real_orders_allowed":false}))
}
// R80: a missing final.json must not convert an unsealed, source-checked
// virtual HEDGED checkpoint into a reported ZERO outstanding positions.
// None deliberately means UNKNOWN, never an independently verified flat.
fn interrupted_open_census(
 final_open:Option<u64>,unsealed:Option<&InterruptedVirtualExposure>
)->Option<u64>{
 let checkpoint_open=unsealed.map(|d|d.open_bases.len() as u64);
 let observed=match (final_open,checkpoint_open){
  (Some(a),Some(b))=>Some(a.max(b)),
  (Some(a),None)=>Some(a),
  (None,Some(b))=>Some(b),
  (None,None)=>None,
 };
 // An unsealed zero is never proof of flatness after a crash.
 observed.filter(|count|*count>0)
}
fn one_run(root:&Path,ledger:&Path,day:u64,baseline:u64,
   run_seconds:u64,isolate_new:bool)->io::Result<()>{
    if baseline>DAY_CAP{return Err(fail("bad budget"))}
    // Re-validate at the last point before any session files are created.
    // A stale sealed position must not become a zero-position new run.
    r1085_planned_handoff_preflight(root,baseline)?;
    let session_name=format!("session_{}_{}",day,now_s());
    let dir=root.join("sessions").join(&session_name);
    fs::create_dir(&dir)?;
    let status=dir.join("status.json");
    let final_path=dir.join("final.json");
    let model_journal=dir.join("modeled_four_leg_journal.jsonl");
    let stop_request=dir.join("graceful_stop_request.json");
    let active=root.join("active.json");
    if active.exists(){return Err(fail("prior paper episode is unresolved"))}
    let carry=root.join("planned_handoff.json");
    // An interrupted generation is evidence, NOT a restored book.
    // Explicit isolated restart never consumes an old planned handoff.
    if isolate_new&&carry.exists(){fs::remove_file(&carry)?;}
    let resume=if carry.exists(){
        let m=read_json(&carry)?;
        let old_day=m["day"].as_u64().ok_or_else(||fail("invalid prior PAPER day"))?;
        let old_count=m["entries"].as_u64().ok_or_else(||fail("invalid prior PAPER count"))?;
        if old_day>day||day-old_day>1||
           (old_day==day&&old_count!=baseline)||
           old_count>DAY_CAP||
           m["validated_seal"]!=true||
           inspect_planned_seal(Path::new(CHECKPOINT),
                old_day as i64,old_count).is_err(){
            return Err(fail("planned PAPER handoff seal is not trustworthy"))
        }
        true
    }else{false};
    if isolate_new{
       atomic_json(&dir.join("research_generation_boundary.json"),&json!({
         "schema":"SPOTARB/PUBLIC-PAPER/ISOLATED-GENERATION/V1",
         "session":session_name,"day_yek":day,"modeled_entry_floor":baseline,
         "historical_virtual_exposure_separately_archived":true,
         "inherits_original_unsealed_positions":false,
         "inherits_prior_modeled_realized_pnl":false,
         "exchange_orders_allowed":false,"private_exchange_api_used":false,
         "no_telegram_delivery_replay":true,
         "operator_paper_only_permission":true
       }))?;
    }
    atomic_json(&active,&json!({"day":day,"session":session_name,
        "started":now_s(),"baseline":baseline,"duration_s":run_seconds,
        "status_path":status,"final_path":final_path,
        "paper_virtual_continuity_requested":resume,
        "isolated_public_paper_generation":isolate_new,
        "real_orders":false}))?;
    atomic_json(&root.join("current_session.json"),&json!({
       "day":day,"session":session_name,"status_path":status,
       "final_path":final_path,"started":now_s(),"baseline":baseline,
       "paper_only":true,"planned_handoff_requested":resume,
       "isolated_public_paper_generation":isolate_new,
       "graceful_stop_request_path":stop_request,
       "virtual_positions_not_exchange_attested":true}))?;
    if resume{fs::remove_file(&carry)?;}
    let out=File::create(dir.join("stdout.log"))?;
    let err=File::create(dir.join("stderr.log"))?;
    let child=Command::new(EXEC)
       .env_clear()
       .env("PATH","/usr/bin:/bin")
       .env("R21_SECONDS",run_seconds.to_string())
       .env("PAPER_STATUS_JSON",&status)
       .env("R21_OUTPUT",&final_path)
       .env("PAPER_PREVIOUS_DAY_ENTRIES",baseline.to_string())
       .env("PAPER_CONTINUITY_PATH",CHECKPOINT)
       .env("PAPER_VIRTUAL_FOUR_LEG_JOURNAL_PATH",&model_journal)
       .env("PAPER_VIRTUAL_FOUR_LEG_SESSION_ID",&session_name)
       .env("PAPER_PLANNED_RESTORE",if resume{"1"}else{"0"})
       .env("PAPER_GRACEFUL_STOP_PATH",&stop_request)
       .env("PAPER_GRACEFUL_SESSION_ID",&session_name)
       .env("R21_KILL_SWITCH",KILL)
       .env("R21_PREFLIGHT","/home/tmpadmin/spotbot/reports/real_micro_preflight_latest.json")
       .env_remove("R21_CAPTURE")
       .stdout(Stdio::from(out)).stderr(Stdio::from(err))
       .spawn();
    let mut child=match child {
       Ok(p)=>p,
       Err(e)=>{
          save_daily(ledger,day,DAY_CAP,"failed_spawn_unknown")?;
          fs::remove_file(active)?;
          return Err(e)
       }
    };
    println!("PAPER_START {} pid={} seconds={} daily_baseline={}",
         session_name,child.id(),run_seconds,baseline);
    let mut killed_for_safety=false;
    let exit=loop{
       if let Some(st)=child.try_wait()?{break st}
       let stdout_bytes=fs::metadata(dir.join("stdout.log")).map(|m|m.len())
            .unwrap_or(MAX_SESSION_BYTES+1);
       let stderr_bytes=fs::metadata(dir.join("stderr.log")).map(|m|m.len())
            .unwrap_or(MAX_SESSION_BYTES+1);
       // Guarantee active logs are bounded too, not only after completion.
       // A burst can overshoot by one 15-second polling interval; the
       // volume-wide 80% / free-GiB guard is the independent hard stop.
       if !storage_ok()||!safety_ok()||
          stdout_bytes>MAX_SESSION_BYTES||stderr_bytes>MAX_SESSION_BYTES{
          killed_for_safety=true;let _=child.kill();
          break child.wait()?
       }
       thread::sleep(Duration::from_secs(15));
    };
    let out_bytes=fs::metadata(dir.join("stdout.log")).map(|m|m.len()).unwrap_or(0);
    let err_bytes=fs::metadata(dir.join("stderr.log")).map(|m|m.len()).unwrap_or(0);
    let mut next=DAY_CAP;
    let mut why="incomplete_or_failed_source_no_paper_entries_until_next_day";
    if !killed_for_safety&&out_bytes<MAX_SESSION_BYTES&&
        err_bytes<MAX_SESSION_BYTES{
       if let Ok(value)=read_json(&final_path){
          let n=value["paper_portfolio"]["counters"]["entries"].as_u64();
          if value["trading"]==false&&value["private_order_path_linked"]==false&&
             value["hard_safety"]["allowed"]==false&&
             value["hard_safety"]["real_launch_authorized"]==false{
             if let Some(n)=n.filter(|v|*v>=baseline&&*v<=DAY_CAP){
                next=n;
                why=if exit.success(){"complete_public_paper_episode"}
                else if exit.code()==Some(2){
                    "completed_public_data_quality_warnings"
                }else{"completed_report_with_errors_no_live_trading"};
             }
          }
       }
    }
    // Exit code 2 is the public WS-quality diagnostic (e.g. harmless
    // reconnects), NOT an incomplete virtual-state write. Accept a handoff
    // only when this exact exit has a completed, checked native PAPER seal.
    let managed_exit=exit.success()||exit.code()==Some(2);
    let final_declares_seal=read_json(&final_path).ok().is_some_and(|v|
       v["virtual_paper_continuity"]["checkpoint_completed"]==true&&
       v["virtual_paper_continuity"]["real_orders"]==false&&
       v["hard_safety"]["allowed"]==false&&
       v["trading"]==false);
    // Keep exact evidence for why a handoff is denied. A bool by itself
    // concealed seven virtual hedges that were abandoned on 13 October.
    let seal_check=inspect_planned_seal(Path::new(CHECKPOINT),day as i64,next);
    let seal_error=seal_check.as_ref().err().map(|e|format!("{e:?}"));
    let model_audit_check=
       spotarb_native_market_core::native_virtual_four_leg_provenance_review::
           inspect_model_journal(&model_journal);
    let model_audit_verified=model_audit_check.as_ref().ok().is_some_and(|audit|{
        let value=read_json(&final_path).unwrap_or(Value::Null);
        audit.sealed&&audit.session==session_name&&
        audit.entries==next&&
        audit.closed==value["paper_portfolio"]["counters"]["closed"].as_u64().unwrap_or(u64::MAX)&&
        audit.open==value["paper_portfolio"]["open_positions"].as_u64().unwrap_or(u64::MAX) as usize&&
        value["modeled_four_leg_journal"]["sealed"]==true&&
        value["modeled_four_leg_journal"]["exchange_fills_attested"]==false&&
        value["modeled_four_leg_journal"]["real_order_authority"]==false&&
        value["modeled_four_leg_journal"]["last_hash"].as_str()==Some(audit.last_hash.as_str())
    });
    let cross_file_alignment=inspect_checkpoint_model_alignment(
        Path::new(CHECKPOINT),&model_journal,&session_name,true);
    let seal_ready=managed_exit&&!killed_for_safety&&next<=DAY_CAP&&
       final_declares_seal&&seal_check.is_ok()&&model_audit_verified&&
       cross_file_alignment.is_ok();
    // Archive the exact virtual checkpoint BEFORE the next episode can
    // overwrite the rotating slot. Archived files are subject to existing
    // 30-day / 96MiB per-session retention.
    if fs::metadata(CHECKPOINT).ok().is_some_and(|m|m.len()<=256*1024){
       let _=fs::copy(CHECKPOINT,dir.join("checkpoint_at_exit.json"));
    }
    let complete_report=read_json(&final_path).unwrap_or(Value::Null);
    let session_start_ms=session_name.rsplit('_').next()
        .and_then(|x|x.parse::<i64>().ok())
        .and_then(|sec|sec.checked_mul(1000)).unwrap_or(0);
    let interrupted=if !seal_ready{
        Some(inspect_interrupted_virtual_exposure(
            Path::new(CHECKPOINT),day as i64,session_start_ms))
    }else{None};
    let unsealed=interrupted.as_ref().and_then(|result|result.as_ref().ok());
    let final_open=complete_report["paper_portfolio"]["open_positions"].as_u64();
    let unresolved=interrupted_open_census(final_open,unsealed);
    let prior_open=unresolved.unwrap_or(0);
    let unknown_outstanding=!seal_ready&&unresolved.is_none();
    if !seal_ready&&(prior_open>0||unknown_outstanding){
       // This is a virtual exposure discontinuity, NOT a close.
       // Continuing to count modeled PnL or opening new paper trades would
       // pretend those uncarried hedges had vanished. Preserve and freeze.
       atomic_json(&dir.join("unrecovered_virtual_positions.json"),&json!({
           "day":day,"session":session_name,
           "lost_from_next_simulated_session":prior_open,
           "pending_positions":if final_open.is_some(){
               complete_report["paper_portfolio"]["pending_positions"].clone()
           }else{
               json!(unsealed.map(|p|p.open_bases.iter().map(|base|
                   json!({"base":base,"status":"UNSEALED_MODEL_ONLY"}))
                   .collect::<Vec<_>>()))
           },
           "source":"FINAL_OR_UNSEALED_CHECKPOINT_DIAGNOSTIC_ONLY",
           "unsealed_checkpoint_revision":unsealed.map(|p|p.checkpoint_revision),
           "unrecovered_open_count_unknown":unknown_outstanding,
           "checkpoint_absent_or_untrusted":unsealed.is_none(),
           "checkpoint_error":interrupted.as_ref()
               .and_then(|r|r.as_ref().err()).map(|e|format!("{e:?}")),
           "seal_diagnostic":seal_error,"final_declares_seal":final_declares_seal,
           "real_exposure":false,"paper_positions_not_closed":true,
           "model_pnl_across_sessions_requires_reconciliation":true,
       }))?;
       next=DAY_CAP;
       why="unrecovered_virtual_positions_paper_entries_quarantined";
       // Persist outside session cleanup BEFORE budget handoff/carry changes.
       let bases=unsealed.map(|p|p.open_bases.clone()).unwrap_or_default();
       write_hold_once(root,day,&session_name,
           "UNSEALED_OR_UNKNOWN_SESSION_AFTER_STOP",&bases)?;
    }
    save_daily(ledger,day,next,why)?;
    if seal_ready {
       atomic_json(&carry,&json!({"day":day,"entries":next,
          "last_session":session_name,"validated_seal":true,
          "no_real_orders":true}))?;
    } else {
       // A previously completed virtual PAPER position must never resume
       // after an unknown process outcome, even when the hash looks valid.
       let _=fs::remove_file(&carry);
    }
    atomic_json(&dir.join("supervisor_summary.json"),&json!({
       "day":day,"session":session_name,"started_with_entries":baseline,
       "finished_with_entries":next,"exit_code":exit.code(),
       "killed_for_safety":killed_for_safety,"reason":why,
       "stdout_bytes":out_bytes,"stderr_bytes":err_bytes,
       "planned_virtual_handoff_started":resume,
       "isolated_public_paper_generation":isolate_new,
       "graceful_early_stop":complete_report["virtual_paper_continuity"]["graceful_early_stop"]==true,
       "next_session_can_restore_virtual_positions":seal_ready,
       "handoff_check_result":if seal_ready{"PASS"}else{"DENIED"},
       "handoff_check_error":seal_error,
       "modeled_four_leg_audit_verified":model_audit_verified,
       "checkpoint_four_leg_cross_file_match":cross_file_alignment.is_ok(),
       "checkpoint_four_leg_cross_file_error":cross_file_alignment.as_ref().err()
           .map(|e|format!("{e:?}")),
       "modeled_four_leg_audit_diagnostic":model_audit_check.as_ref().err()
           .map(|e|format!("{e:?}")),
       "observed_unrecovered_virtual_open_positions":if seal_ready{0}else{prior_open},
       "unrecovered_virtual_open_count_unknown":unknown_outstanding,
       "unsealed_checkpoint_revision":unsealed.map(|p|p.checkpoint_revision),
       "unsealed_checkpoint_open_bases":unsealed.map(|p|&p.open_bases),
       "real_execution_verified":false,
       "real_trading":false}))?;
    fs::remove_file(&active)?;
    File::open(root)?.sync_all()?;
    cleanup_sessions(root)?;
    println!("PAPER_END {} {:?} day_entries={} reason={}",
       session_name,exit.code(),next,why);
    Ok(())
}
fn main()->io::Result<()>{
    // R1114: a private, offline bridge from an injected supervisor
    // failure to the local R37 queue (never R38 network dispatch).
    if std::env::args_os().nth(1).as_deref()==Some(
         std::ffi::OsStr::new("--r1114-private-telegram-quarantine")){
       let args=std::env::args_os().collect::<Vec<_>>();
       if args.len()!=3{return Err(fail("R1114 exact private root required"))}
       let proof=spotarb_native_market_core::
         native_r1114_quarantine_telegram_bridge::
         quarantined_fault_to_local_notice(Path::new(&args[2]))
         .map_err(|e|fail(&e))?;
       println!("{}",serde_json::to_string(&proof)?);
       return Ok(())
    }
    // R1113: explicit OFFLINE Rust-supervised joint PAPER proof.
    // A completely separate mode: no original service session, no
    // public WebSocket, no legacy checkpoint mutation, no Telegram.
    let args=std::env::args_os().collect::<Vec<_>>();
    if args.get(1).is_some_and(|x|x=="--r1113-private-offline-join"){
       if args.len()!=3{return Err(fail("R1113 exact private root required"))}
       let private_root=Path::new(&args[2]);
       let proof=spotarb_native_market_core::native_r1113_offline_supervisor_join::
         run(private_root).map_err(|e|fail(&e))?;
       println!("{}",serde_json::to_string(&proof)?);
       return Ok(())
    }
    let root=Path::new(ROOT);
    // Read-only operator audit (no root/session/ledger mutations).
    if std::env::args_os().nth(1).as_deref()==Some(
      std::ffi::OsStr::new("--r1085-stopped-handoff-audit")){
      if std::env::args_os().len()!=2{
       return Err(fail("R1085 audit accepts exactly one argument"))
      }
      let day=yek_day(now_s());
      let baseline=daily(&root.join("daily_budget.json"),day);
      let safe=safety_ok();
      let check=r1085_planned_handoff_preflight(root,baseline);
      println!("{}",serde_json::to_string_pretty(&json!({
       "schema":"SPOTARB/R1085/READONLY-STOPPED-START-GATE/V1",
       "safe_to_start":safe&&check.is_ok(),
       "real_kill_engaged":safe,"baseline_daily_budget":baseline,
       "source_handoff":check.as_ref().ok(),
       "reason":check.as_ref().err().map(|e|e.to_string()),
       "no_files_modified":true,"real_orders":false
      }))?);
      return Ok(())
    }
    if std::env::args_os().len()!=1 {
     return Err(fail("R1085 supervisor denies unknown arguments"))
    }
    fs::create_dir_all(root.join("sessions"))?;
    fs::create_dir_all(root.join("bin"))?;
    fs::set_permissions(root,fs::Permissions::from_mode(0o700))?;
    let lock_path=root.join("supervisor.lock");
    let lock=OpenOptions::new().read(true).write(true).create(true)
      .open(&lock_path)?;
    if unsafe{libc::flock(lock.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0{
       return Err(fail("continuous paper supervisor already running"))
    }
    let ledger=root.join("daily_budget.json");
    let active=root.join("active.json");
    if active.exists(){
       let prior=read_json(&active).unwrap_or(Value::Null);
       let incident_day=prior["day"].as_u64()
           .ok_or_else(||fail("prior interrupted episode lacks day"))?;
       let incident_session=prior["session"].as_str()
           .ok_or_else(||fail("prior interrupted episode lacks session"))?;
       write_hold_once(root,incident_day,incident_session,
           "UNEXPECTED_SUPERVISOR_RESTART_WITH_ACTIVE_CHILD",&[])?;
       save_daily(&ledger,incident_day,DAY_CAP,
           "uncertain_restart_prior_paper_positions_quarantined")?;
       fs::rename(&active,root.join("interrupted_previous.json"))?;
       let _=fs::remove_file(root.join("planned_handoff.json"));
       // Retain the original last status and never claim that an open position
       // automatically closed; the new session is a separate virtual book.
    }
    println!("PAPER_SUPERVISOR_START public_ws_only=true");
    loop{
       let t=now_s();let day=yek_day(t);
       let until=until_yek_midnight(t);
       if until<=90 {thread::sleep(Duration::from_secs(25));continue}
       if !storage_ok()||!safety_ok(){
          println!("PAPER_PAUSED storage_or_kill_guard, no new WS session");
          thread::sleep(Duration::from_secs(60));continue
       }
       // R1085 MUST precede any isolated rebaseline/daily ledger edits.
       // No session, files or sockets until stale sealed exposure is
       // independently reconciled; never translate a failed normal
       // handoff into a fresh virtual book or 500-entry budget poison.
       let original_budget=daily(&ledger,day);
       if let Err(e)=r1085_planned_handoff_preflight(root,original_budget){
          eprintln!("PAPER_R1085_STOPPED_RESTORE_HOLD {e}");
          thread::sleep(Duration::from_secs(60));continue
       }
       // R82: an old unsealed model is NEVER continued or marked flat.
       // Operator can explicitly authorize a SEPARATE public-PAPER
       // experiment after recovering a bounded, isolated entry budget.
       let operator=isolated_paper_research_authorized();
       let historic_hold=match unresolved_crossday_hold(root){
         Ok(v)=>v,
         Err(e)=>{
           eprintln!("PAPER_UNRESOLVED_HISTORY_AUDIT_DENIED {:?}",e);
           thread::sleep(Duration::from_secs(60));continue
         },
       };
       if historic_hold&&!operator{
          println!("PAPER_UNRESOLVED_RESEARCH_HOLD; operator boundary absent");
          thread::sleep(Duration::from_secs(60));continue
       }
       // bounded 3h per segment, no EKB-midnight crossing
       let run_seconds=until.saturating_sub(65).min(10_800);
       if run_seconds<10{thread::sleep(Duration::from_secs(30));continue}
       let (n,repair)=match isolated_paper_budget(root,&ledger,day,
              historic_hold&&operator){
         Ok(v)=>v,
         Err(e)=>{
           eprintln!("PAPER_MODELED_BUDGET_REVIEW_DENIED {:?}",e);
           thread::sleep(Duration::from_secs(60));continue
         },
       };
       if repair{
          // The original 500 sentinel is retained in an immutable
          // local per-generation audit BEFORE the current ledger changes.
          let prior=read_json(&ledger).unwrap_or(Value::Null);
          let marker=root.join(format!(
             "paper_isolated_rebaseline_{}_{}.json",day,now_s()));
          if let Err(e)=atomic_json(&marker,&json!({
             "schema":"SPOTARB/PUBLIC-PAPER/ISOLATED-REBASELINE/V1",
             "day_yek":day,"modeled_reconstructed_count":n,
             "prior_daily_budget":prior,
             "historical_fills_are_virtual_and_unresolved":true,
             "old_model_pnl_not_transferred":true,
             "real_order_permission":false,"operator_paper_only":true
          })).and_then(|_|save_daily(&ledger,day,n,
                 "operator_public_paper_isolated_rebaseline")){
             eprintln!("PAPER_ISOLATED_REBASELINE_DENIED {:?}",e);
             thread::sleep(Duration::from_secs(60));continue
          }
          println!("PAPER_MODEL_SENTINEL_REPLACED_WITH_ARCHIVED_MODEL_HIGHWATER {}",n);
       }
       if paper_daily_budget_full(n){
          // A verified full PAPER daily budget is an intentional STOP.
          // Do not create endless new failed 0-entry WS episodes.
          println!("PAPER_DAILY_LIMIT_PAUSED current={} cap={}, until next Yekaterinburg day",n,DAY_CAP);
          thread::sleep(Duration::from_secs(60));
          continue
       }
       let isolated_new=repair||
           (historic_hold&&operator&&!root.join("planned_handoff.json").exists());
       if let Err(e)=one_run(root,&ledger,day,n,run_seconds,isolated_new){
          eprintln!("PAPER_SUPERVISOR_ERROR {:?}, paper budget quarantined",e);
          save_daily(&ledger,day,DAY_CAP,"supervisor_io_error")?;
          if active.exists(){
             eprintln!("PAPER_PAUSED unresolved ACTIVE episode; no duplicate runner");
             thread::sleep(Duration::from_secs(300));
             continue
          }
       }
       thread::sleep(Duration::from_secs(15));
    }
}
use std::os::unix::fs::PermissionsExt;

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn r1085_new_empty_source_only_and_unreferenced_checkpoint_refused(){
       let root=std::env::temp_dir().join(format!(
          "r1085_gate_empty_{}_{}",std::process::id(),now_s()));
       fs::create_dir(&root).unwrap();
       assert_eq!(r1085_planned_handoff_preflight(&root,0).unwrap()["ready"],true);
       // Even a present-but-unreferenced checkpoint cannot be implicitly
       // interpreted as proof of a flat, zero-position old portfolio.
       fs::write(root.join("portfolio_checkpoint.json"),b"unknown").unwrap();
       assert!(r1085_planned_handoff_preflight(&root,0).is_err());
       fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn r1085_fresh_normal_planned_stop_accepts_and_mutation_refuses(){
       use spotarb_native_market_core::{
         native_paper_portfolio_review::PaperPortfolio,
         native_paper_continuity_checkpoint_review::save_checkpoint,
         transport::monotonic_ns,
       };
       let root=std::env::temp_dir().join(format!(
          "r1085_gate_fresh_{}_{}",std::process::id(),monotonic_ns()));
       fs::create_dir(&root).unwrap();
       let now=(now_s()*1000) as i64;
       let mut p=PaperPortfolio::new(now).unwrap();
       p.stats.entries=7;p.stats.closed=7;
       let c=root.join("portfolio_checkpoint.json");
       save_checkpoint(&c,&p,4,monotonic_ns(),true).unwrap();
       let carry=root.join("planned_handoff.json");
       let day=yek_day(now_s());
       atomic_json(&carry,&json!({"day":day,"entries":7,
         "last_session":"session_1085_safe","no_real_orders":true,
         "validated_seal":true})).unwrap();
       let good=r1085_planned_handoff_preflight(&root,7).unwrap();
       assert_eq!(good["ready"],true);
       assert_eq!(good["original_entries"],7);
       assert!(r1085_planned_handoff_preflight(&root,0).is_err(),
          "same-day daily entries may not silently roll back");
       atomic_json(&carry,&json!({"day":day,"entries":7,
         "last_session":"session_1085_safe","no_real_orders":false,
         "validated_seal":true})).unwrap();
       assert!(r1085_planned_handoff_preflight(&root,7).is_err(),
          "handoff marked real must refuse");
       fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn r82_poisoned_budget_is_repaired_only_with_paper_opt_in(){
       use spotarb_native_market_core::{
         native_paper_portfolio_review::PaperPortfolio,
         native_paper_continuity_checkpoint_review::save_checkpoint,
         transport::monotonic_ns
       };
       let day=yek_day(now_s());
       let root=std::env::temp_dir().join(format!(
          "astra_r82_budget_{}_{}",std::process::id(),now_s()));
       let ledger=root.join("daily_budget.json");
       let id=format!("session_{}_{}",day,now_s().saturating_sub(2));
       let session=root.join("sessions").join(&id);
       fs::create_dir_all(&session).unwrap();
       let mut model=PaperPortfolio::new((now_s()*1000) as i64).unwrap();
       model.stats.entries=14;
       model.stats.closed=13;
       save_checkpoint(&session.join("checkpoint_at_exit.json"),
          &model,473,monotonic_ns(),false).unwrap();
       atomic_json(&session.join("supervisor_summary.json"),&json!({
         "session":id,"day":day,"started_with_entries":13,
         "finished_with_entries":500,"handoff_check_result":"DENIED",
         "reason":"incomplete_or_failed_source_no_paper_entries_until_next_day",
         "killed_for_safety":true
       })).unwrap();
       save_daily(&ledger,day,500,
          "incomplete_or_failed_source_no_paper_entries_until_next_day").unwrap();
       assert_eq!(isolated_paper_budget(&root,&ledger,day,false).unwrap(),
          (500,false));
       assert_eq!(isolated_paper_budget(&root,&ledger,day,true).unwrap(),
          (14,true));
       assert_eq!(daily(&ledger,day),500,
          "read-only inspection never silently edits a PAPER budget");
       save_daily(&ledger,day,500,"complete_public_paper_episode").unwrap();
       assert_eq!(isolated_paper_budget(&root,&ledger,day,true).unwrap(),
          (500,false),"real modeled 500 cap remains in force");
       fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn r82_unknown_budget_uses_explicit_separate_generation_not_false_close(){
       let root=std::env::temp_dir().join(format!(
          "astra_r82_unknown_{}_{}",std::process::id(),now_s()));
       fs::create_dir_all(root.join("sessions")).unwrap();
       let ledger=root.join("daily_budget.json");
       let day=yek_day(now_s());
       save_daily(&ledger,day,500,"supervisor_io_error").unwrap();
       assert_eq!(isolated_paper_budget(&root,&ledger,day,true).unwrap(),
          (0,true),"only isolated PAPER, with uncertified day count");
       assert_eq!(isolated_paper_budget(&root,&ledger,day,false).unwrap(),
          (500,false),"no operator authorization remains fail-closed");
       fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn r81_successful_sealed_episode_does_not_create_a_hold(){
        let root=std::env::temp_dir().join(format!(
          "astra_r81_good_{}_{}",std::process::id(),now_s()));
        let folder=root.join("sessions").join("session_20742_1792154000");
        fs::create_dir_all(&folder).unwrap();
        atomic_json(&folder.join("supervisor_summary.json"),&json!({
          "day":20742,"session":"session_20742_1792154000",
          "reason":"complete_public_paper_episode",
          "handoff_check_result":"PASS",
          "killed_for_safety":false,
          "observed_unrecovered_virtual_open_positions":0
        })).unwrap();
        assert!(!unresolved_crossday_hold(&root).unwrap());
        assert!(!hold_exists(&root).unwrap());
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn r81_old_denied_session_survives_midnight_and_retention(){
        let root=std::env::temp_dir().join(format!(
          "astra_r81_hold_{}_{}",std::process::id(),now_s()));
        let sessions=root.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let incident="session_20742_1792154820";
        let prior=sessions.join(incident);
        fs::create_dir(&prior).unwrap();
        atomic_json(&prior.join("supervisor_summary.json"),&json!({
          "day":20742,"session":incident,
          "reason":"incomplete_or_failed_source_no_paper_entries_until_next_day",
          "handoff_check_result":"DENIED",
          "killed_for_safety":true,
          "observed_unrecovered_virtual_open_positions":0
        })).unwrap();
        assert!(unresolved_crossday_hold(&root).unwrap(),
           "old failed session must require durable hold across day boundary");
        assert!(hold_exists(&root).unwrap());
        assert_eq!(daily(&root.join("daily.json"),20743),0,
           "daily quota would reset without a persistent independent gate");
        fs::remove_dir_all(&sessions).unwrap();
        assert!(unresolved_crossday_hold(&root).unwrap(),
           "hold must survive session cleanup and calendar rollover");
        let mut tampered=read_json(&root.join(UNRESOLVED_HOLD)).unwrap();
        tampered["quarantined"]=json!(false);
        fs::write(root.join(UNRESOLVED_HOLD),
                  serde_json::to_vec(&tampered).unwrap()).unwrap();
        assert!(unresolved_crossday_hold(&root).is_err(),
           "malformed hold cannot be treated as permission to resume");
        fs::remove_dir_all(&root).unwrap();
    }
    #[test]
    fn r80_missing_final_does_not_erase_unsealed_hedged_checkpoint(){
        let unsealed=InterruptedVirtualExposure{
            checkpoint_revision:473,observed_entries:14,observed_closed:13,
            open_bases:vec!["VELODROME".to_string()],
        };
        assert_eq!(interrupted_open_census(None,Some(&unsealed)),Some(1));
        assert_eq!(interrupted_open_census(Some(0),Some(&unsealed)),Some(1));
        assert_eq!(interrupted_open_census(Some(2),Some(&unsealed)),Some(2));
        assert_eq!(interrupted_open_census(None,None),None);
        assert_eq!(interrupted_open_census(Some(0),None),None);
        let empty=InterruptedVirtualExposure{open_bases:Vec::new(),..unsealed};
        assert_eq!(interrupted_open_census(Some(0),Some(&empty)),None);
        // UNKNOWN is intentionally not counted as verified flat.
    }
    #[test]
    fn ekb_midnight_day_rollover_is_strict(){
        // 2026-10-12 18:59:59 UTC == 2026-10-12 23:59:59 EKB.
        let t=1_760_000_000u64;
        let boundary=t+(86_400-(t+5*3600)%86_400);
        assert_eq!(until_yek_midnight(boundary-1),1);
        assert_eq!(until_yek_midnight(boundary),86_400);
        assert_eq!(yek_day(boundary-1)+1,yek_day(boundary));
    }
    #[test]
    fn full_paper_budget_does_not_start_another_session(){
        assert!(!paper_daily_budget_full(0));
        assert!(!paper_daily_budget_full(DAY_CAP-1));
        assert!(paper_daily_budget_full(DAY_CAP));
        assert!(paper_daily_budget_full(DAY_CAP+1));
        let root=std::env::temp_dir().join(format!(
          "astra_supervisor_capacity_{}",std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let ledger=root.join("daily.json");
        let day=yek_day(now_s());
        save_daily(&ledger,day,DAY_CAP,"daily_cap_fixture").unwrap();
        assert!(paper_daily_budget_full(daily(&ledger,day)));
        assert!(!paper_daily_budget_full(daily(&ledger,day+1)),
          "EKT new day resets PAPER quota");
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn invalid_daily_state_denies_next_paper_entries(){
        let d=std::env::temp_dir().join(format!(
            "astra_supervisor_budget_{}",std::process::id()));
        fs::create_dir_all(&d).unwrap();
        let f=d.join("daily.json");
        let today=yek_day(now_s());
        assert_eq!(daily(&f,today),0);
        fs::write(&f,"{").unwrap();
        assert_eq!(daily(&f,today),DAY_CAP);
        fs::remove_file(&f).unwrap();
        save_daily(&f,today,DAY_CAP+1,"test").unwrap();
        assert_eq!(daily(&f,today),DAY_CAP);
        fs::remove_dir_all(d).unwrap();
    }
}
