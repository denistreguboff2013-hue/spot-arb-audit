use serde_json::{json,Value};
use spotarb_native_market_core::{
    ingress_runtime_r1205::{run_r1246_volatile,Config,Summary},
    market_data_ingress_r1205::{Authority,Output},
};
use std::{collections::HashMap,fs,path::PathBuf,sync::{Arc,RwLock},time::Duration};
use tokio::sync::mpsc;

fn scrub_capture_environment()->Result<(),String>{
    std::env::remove_var("R1268_CAPTURE");
    std::env::remove_var("R1268_CAPTURE_DIR");
    if std::env::var_os("R1268_CAPTURE").is_some()||std::env::var_os("R1268_CAPTURE_DIR").is_some(){
        return Err("R1326E_CAPTURE_ENV_NOT_ISOLATED".into())
    }
    Ok(())
}
fn percentile(samples:&[u64])->Value{
    if samples.is_empty(){return json!({"samples":0})}
    let mut sorted=samples.to_vec();sorted.sort_unstable();
    let q=|p:usize|sorted[((sorted.len()-1)*p+99)/100];
    json!({"samples":sorted.len(),"p50":q(50),"p95":q(95),"p99":q(99)})
}
fn readiness_row(v:&Value)->Value{
    fn source(v:&Value)->Value{json!({
        "scope":v["scope"],"session":v["session"],"generation":v["generation"],
        "selected":v["selected"],"failed_predicates":v["failed_predicates"],
        "manager_state":v["manager_state"],"manager_subscribed":v["manager_subscribed"],
        "manager_good_samples":v["manager_good_samples"],"manager_errors":v["manager_errors"],
        "manager_last_receive_age_ms":v["manager_last_receive_age_ms"],
        "manager_last_valid_exchange_age_ms":v["manager_last_valid_exchange_age_ms"],
        "book_state":v["book_state"],"book_fresh":v["book_fresh"],"book_exchange_age_ms":v["book_exchange_age_ms"]
    })}
    json!({"kind":v["kind"],"mono_ns":v["mono_ns"],"scope":v["scope"],"session":v["session"],
        "ready_sources":v["ready_sources"],"source_count":v["source_count"],
        "lost_predicates":v["lost_predicates"],"gained_predicates":v["gained_predicates"],
        "snapshot":if v["snapshot"].is_object(){source(&v["snapshot"])}else{Value::Null},
        "before":if v["before"].is_object(){source(&v["before"])}else{Value::Null},
        "after":if v["after"].is_object(){source(&v["after"])}else{Value::Null},
        "sources":v["sources"].as_array().map(|a|a.iter().map(source).collect::<Vec<_>>())})
}
fn audit_counts(rows:&[Value],field:&str)->Value{
    let mut counts=std::collections::BTreeMap::<String,u64>::new();
    for row in rows{if let Some(value)=row[field].as_str(){*counts.entry(value.to_owned()).or_default()+=1}}
    json!(counts)
}
fn compact(summary:&Summary)->Value{
    let sockets=summary.sockets.iter().map(|s|json!({
        "session":s.session,"venue":s.venue,"url":s.url,"connection_epoch":s.connection_epoch,
        "local":s.local,"peer":s.peer,"dns_ips":s.dns_ips,
        "established_ns":s.established_ns,"received":s.received,"stale":s.stale,"connects":s.connects,"end":s.end,
        "reader_queue_depth_max":s.reader_queue_depth_max,"reader_queue_overflow":s.reader_queue_overflow,
        "age_ms":percentile(&s.age_samples.iter().filter_map(|v|u64::try_from(*v).ok()).collect::<Vec<_>>()),
        "queue_send_wait_ns":percentile(&s.queue_send_wait_ns),
        "reader_queue_push_ns":percentile(&s.reader_queue_push_ns)
    })).collect::<Vec<_>>();
    json!({
        "sockets":sockets,"counts":summary.counts,"coverage_time":summary.coverage_time,
        "diagnostics":{
            "readiness_audit_total":summary.readiness_audit.len(),
            "readiness_audit_kinds":audit_counts(&summary.readiness_audit,"kind"),
            "readiness_audit_tail":summary.readiness_audit.iter().rev().take(24).map(readiness_row).collect::<Vec<_>>(),
            "selection_audit_total":summary.selection_audit.len(),
            "selection_audit_tail":summary.selection_audit.iter().rev().take(4).map(|v|json!({"scope":v["scope"],"reason":v["reason"],"manager_proposal_reason":v["manager_proposal_reason"],"book_fresh":v["book_fresh"],"generation":v["generation"],"epoch":v["epoch"]})).collect::<Vec<_>>(),
            "okx_books_invalid_total":summary.okx_books_invalid_audit.len(),
            "okx_books_invalid_reasons":audit_counts(&summary.okx_books_invalid_audit,"failure_reason"),
            "okx_books_invalid_tail":summary.okx_books_invalid_audit.iter().rev().take(4).map(|v|json!({"failure_reason":v["failure_reason"],"source":v["source"],"instrument":v["instrument"],"channel":v["channel"],"action":v["action"],"snapshot":v["snapshot"],"seq_id":v["seq_id"],"prev_seq_id":v["prev_seq_id"],"expected_previous_seq":v["expected_previous_seq"],"event_age_ms":v["event_age_ms"],"checksum_validation":v["checksum_validation"]})).collect::<Vec<_>>(),
            "stale_frame_audit_total":summary.stale_frame_audit.len(),
            "stale_frame_audit_tail":summary.stale_frame_audit.iter().rev().take(4).map(|v|json!({"category":v["category"],"freshness_reject_reason":v["freshness_reject_reason"],"venue":v["venue"],"instrument":v["instrument"],"exchange_age_at_t1_ms":v["exchange_age_at_t1_ms"],"exchange_age_at_t4_ms":v["exchange_age_at_t4_ms"]})).collect::<Vec<_>>(),
            "session_events":summary.events.iter().filter(|e|matches!(e["kind"].as_str(),Some("connected"|"subscribed"|"okx_symbol_resubscribe_requested"))).rev().take(12).cloned().collect::<Vec<_>>()
        },
        "engine_counts":{
            "received":summary.engine["received"],"duplicates":summary.engine["duplicates"],
            "invalid":summary.engine["invalid"],"switches":summary.engine["switches"],
            "blocked":summary.engine["blocked"],"submitted":summary.engine["submitted"],
            "hot_deficits":summary.engine["hot_deficits"],"reorder":summary.engine["reorder"],
            "audit_evictions":summary.engine["audit_evictions"],
            "invalid_categories":summary.engine["invalid_categories"]
        },
        "reader_received_total":summary.reader_received_total,
        "reader_queue_overflow_events":summary.reader_queue_overflow_events,
        "peak_reader_frame_queue_depth":summary.peak_reader_frame_queue_depth,
        "central_frame_queue_capacity":summary.central_frame_queue_capacity,
        "reader_frame_queue_capacity_per_session":summary.reader_frame_queue_capacity_per_session,
        "central_queue_depth_max":summary.central_queue_depth_max,
        "engine_queue_age_ns":percentile(&summary.engine_queue_age_samples_ns),
        "engine_batch_service_ns":percentile(&summary.engine_batch_service_samples_ns),
        "output_send_wait_ns":percentile(&summary.output_send_wait_samples_ns),
        "diagnostic_event_evictions":summary.diagnostic_event_evictions,
        "r1279_engine_trace_dropped":summary.r1279_engine_trace_dropped
    })
}
fn safe_output(arg:&str)->Result<PathBuf,String>{
    let p=PathBuf::from(arg);
    let parent=p.parent().ok_or("output parent missing")?.canonicalize().map_err(|e|e.to_string())?;
    let allowed="/mnt/spotarb_disk2/SPOT_ARB_R1326_TRADE_TRIGGERED_DURABILITY_20261118/R1326E_LIVE_SHADOW";
    if parent.to_string_lossy()!=allowed{return Err("output outside isolated R1326E shadow directory".into())}
    let out=parent.join(p.file_name().ok_or("output name missing")?);
    if out.exists(){return Err("output already exists".into())}
    Ok(out)
}
fn kill_is_engaged()->Result<(),String>{
    let p="/home/tmpadmin/spotbot/data/real_kill_switch.json";
    let v:Value=serde_json::from_slice(&fs::read(p).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
    if v["state"]!="ENGAGED"{return Err("real kill switch not ENGAGED".into())}
    Ok(())
}
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main()->Result<(),String>{
    let args=std::env::args().skip(1).collect::<Vec<_>>();
    if args.len()!=2{return Err("usage: r1326e_bounded_public_shadow SECONDS OUTPUT_JSON".into())}
    let seconds:u64=args[0].parse().map_err(|_|"invalid duration")?;
    if !matches!(seconds,60|300|3600){return Err("duration must be exactly 60, 300 or 3600 seconds".into())}
    scrub_capture_environment()?;kill_is_engaged()?;
    let output=safe_output(&args[1])?;
    let bases=vec!["BTC".to_owned(),"ETH".to_owned()];
    std::env::set_var("R1248_BINANCE_GROUP_SIZE","10");
    let authority:Authority=Arc::new(RwLock::new(HashMap::new()));
    let (tx,mut rx)=mpsc::channel::<Output>(64);
    let drain=tokio::spawn(async move{
        let mut counts=HashMap::<String,u64>::new();
        while let Some(item)=rx.recv().await{
            let key=match item{
                Output::Reset{venue,base,..}=>format!("reset:{venue:?}:{base}"),
                Output::Frame(_)=>"frame".into(),Output::Book(_,_)=>"book".into(),
                Output::Resubscribe{session,base,..}=>format!("resubscribe:{session}:{base}"),
                Output::Blocked{venue,base,..}=>format!("blocked:{venue:?}:{base}")
            };
            *counts.entry(key).or_default()+=1;
        }
        counts
    });
    let config=Config{shard:0,bases,copies:2,duration:Duration::from_secs(seconds),
        controlled_close_after:None,endpoint_variant:0,probe_rotation_after:None};
    let started=std::time::SystemTime::now();
    let summary=run_r1246_volatile(config,authority,tx).await?;
    let drained=drain.await.map_err(|e|e.to_string())?;
    let result=json!({
        "schema":"SPOTARB/R1326E/BOUNDED_PUBLIC_SHADOW/V1","duration_requested_s":seconds,
        "started":format!("{started:?}"),"kill_switch":"ENGAGED","real_orders":false,"paper":false,
        "private_api":false,"db1_db2":"unused","universum":"untouched","capture":"disabled_in_process",
        "drained_outputs":drained,"metrics":compact(&summary)
    });
    let bytes=serde_json::to_vec_pretty(&result).map_err(|e|e.to_string())?;
    if bytes.len()>65_536{return Err("shadow report exceeded 64 KiB limit".into())}
    let mut f=std::fs::OpenOptions::new().write(true).create_new(true).open(&output).map_err(|e|e.to_string())?;
    use std::io::Write;f.write_all(&bytes).map_err(|e|e.to_string())?;f.sync_all().map_err(|e|e.to_string())?;
    println!("{}",json!({"output":output,"duration_s":seconds,"bytes":bytes.len(),"orders":0}));
    Ok(())
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]fn r1326e_capture_yes_is_scrubbed_before_runtime(){
        let old=std::env::var_os("R1268_CAPTURE");let old_dir=std::env::var_os("R1268_CAPTURE_DIR");
        std::env::set_var("R1268_CAPTURE","YES");std::env::set_var("R1268_CAPTURE_DIR","/tmp/forbidden-r1326e-capture");
        assert!(scrub_capture_environment().is_ok());
        assert!(std::env::var_os("R1268_CAPTURE").is_none());
        assert!(std::env::var_os("R1268_CAPTURE_DIR").is_none());
        if let Some(v)=old{std::env::set_var("R1268_CAPTURE",v)}else{std::env::remove_var("R1268_CAPTURE")}
        if let Some(v)=old_dir{std::env::set_var("R1268_CAPTURE_DIR",v)}else{std::env::remove_var("R1268_CAPTURE_DIR")}
    }
}
