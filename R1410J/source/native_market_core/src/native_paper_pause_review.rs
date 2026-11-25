//! R42: distinguish supervisor-confirmed PAPER daily-quota pause
//! from an unsafe stale public-data feed. No network, no orders.
use serde_json::Value;
const DAY_MS:u64=86_400_000;
const YEK_MS:u64=18_000_000;
const DAY_CAP:u64=500;

pub fn verified_paper_day_cap_pause(
 budget:&Value,now_ms:u64,pending_notices:usize
)->bool{
 if now_ms==0||pending_notices!=0{return false}
 let day=match now_ms.checked_add(YEK_MS){Some(t)=>t/DAY_MS,None=>return false};
 let reason=budget["reason"].as_str().unwrap_or("");
 let quarantined=matches!(
  reason,
  "incomplete_or_failed_source_no_paper_entries_until_next_day"|
  "uncertain_restart_prior_paper_positions_quarantined"|
  "supervisor_io_error"|"failed_spawn_unknown"|
  "daily_cap_reached"|"complete_public_paper_episode"
 );
 budget["paper_only"]==true&&
 budget["day"].as_u64()==Some(day)&&
 budget["entries"].as_u64()==Some(DAY_CAP)&&quarantined&&
 budget["wall_s"].as_u64().is_some_and(|s|{
  let now_s=now_ms/1000;
  s>0&&s<=now_s.saturating_add(30)&&
  now_s.saturating_sub(s)<=86_400
 })
}

#[cfg(test)]
mod tests{
 use super::*;
 use serde_json::json;
 const DAY:u64=20_742;
 fn b(reason:&str)->Value{
  json!({"day":DAY,"entries":500,"paper_only":true,
   "reason":reason,"wall_s":(DAY*DAY_MS-YEK_MS+1_000)/1000})
 }
 #[test]
 fn verified_paused_quota_allows_no_send_only(){
  let now=DAY*DAY_MS-YEK_MS+5_000;
  assert!(verified_paper_day_cap_pause(&b(
   "incomplete_or_failed_source_no_paper_entries_until_next_day"),now,0));
  assert!(!verified_paper_day_cap_pause(&b(
   "incomplete_or_failed_source_no_paper_entries_until_next_day"),now,1));
 }
 #[test]
 fn never_mask_feed_failure_without_a_verified_full_cap(){
  let now=DAY*DAY_MS-YEK_MS+5_000;
  let mut v=b("incomplete_or_failed_source_no_paper_entries_until_next_day");
  v["entries"]=json!(499);
  assert!(!verified_paper_day_cap_pause(&v,now,0));
  v["entries"]=json!(500);v["paper_only"]=json!(false);
  assert!(!verified_paper_day_cap_pause(&v,now,0));
  v["paper_only"]=json!(true);v["reason"]=json!("unknown");
  assert!(!verified_paper_day_cap_pause(&v,now,0));
  v["reason"]=json!("incomplete_or_failed_source_no_paper_entries_until_next_day");
  assert!(!verified_paper_day_cap_pause(&v,now+DAY_MS,0));
  assert!(!verified_paper_day_cap_pause(&v,0,0));
 }
 #[test]
 fn malformed_or_future_budget_is_not_trusted(){
  let now=DAY*DAY_MS-YEK_MS+5_000;
  assert!(!verified_paper_day_cap_pause(&Value::Null,now,0));
  let mut v=b("daily_cap_reached");
  v["wall_s"]=json!(now/1000+1_000);
  assert!(!verified_paper_day_cap_pause(&v,now,0));
 }
}
