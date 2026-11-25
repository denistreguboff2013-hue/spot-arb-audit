//! Astra R173: public read-only YAML operational selectors.
//! Research preview only; not installed PAPER or execution authority.
//! No network, disk, environment or clock access.
use crate::native_trading_config::NativeTradingConfig;
use serde_yaml::Value;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum R173Error{Source,Missing,UnsupportedZone,Clock,Path}
fn source_section<'a>(cfg:&'a NativeTradingConfig,name:&str)
 ->Result<&'a Value,R173Error>{
 if !cfg.source_projection_unchanged()||
    cfg.policy.real_execution_authorized||cfg.policy.real_daily_trades!=0{
  return Err(R173Error::Source)
 }
 cfg.raw.get(name).ok_or(R173Error::Missing)
}
fn source_str<'a>(obj:&'a Value,key:&str)->Result<&'a str,R173Error>{
 obj.get(key).and_then(Value::as_str).ok_or(R173Error::Missing)
}
/// Pure schedule preview; supports UTC and fixed UTC+5 Yekaterinburg for
/// 2020..2035. Other timezones including DST explicitly unsupported.
pub fn next_report_utc_s(cfg:&NativeTradingConfig,now_utc_s:i64)
 ->Result<i64,R173Error>{
 const START_2020:i64=1_577_836_800;
 const START_2036:i64=2_082_758_400;
 if !(START_2020..START_2036).contains(&now_utc_s){
  return Err(R173Error::Clock)
 }
 let general=source_section(cfg,"general")?;
 let shift=match source_str(general,"report_timezone")?{
  "UTC"=>0_i64,
  "Asia/Yekaterinburg"=>5*3600,
  _=>return Err(R173Error::UnsupportedZone),
 };
 let value=source_str(general,"report_time")?.as_bytes();
 if value.len()!=5||value[2]!=b':'||
  [0_usize,1,3,4].iter().any(|i|!value[*i].is_ascii_digit()){
  return Err(R173Error::Clock)
 }
 let hh=i64::from(value[0]-b'0')*10+i64::from(value[1]-b'0');
 let mm=i64::from(value[3]-b'0')*10+i64::from(value[4]-b'0');
 if hh>23||mm>59{return Err(R173Error::Clock)}
 let local=now_utc_s+shift;
 let day=local.div_euclid(86_400);
 let target_local_s=hh*3600+mm*60;
 let next_day=day+i64::from(local.rem_euclid(86_400)>=target_local_s);
 Ok(next_day*86_400+target_local_s-shift)
}
fn absolute_clean_path(p:&str)->bool{
 !p.is_empty()&&p.len()<=512&&p.starts_with('/')&&
 !p.contains('\0')&&!p.contains('\r')&&!p.contains('\n')&&
 !p.split('/').any(|part|part==".."||part==".")
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct PathsPreview{
 pub scanner_telemetry:String,
 pub scanner_binance_cache:String,
 pub scanner_okx_cache:String,
 pub strategy_telemetry:String,
 pub read_or_written_files:bool,
}
/// Source selection only, no filesystem access. Scanner's data directory
/// and strategy's telemetry path have independent YAML owners.
pub fn preview_cache_paths(cfg:&NativeTradingConfig)
 ->Result<PathsPreview,R173Error>{
 let p=source_section(cfg,"paths")?;
 let h=source_section(cfg,"scale_in_hints")?;
 let raw_root=source_str(p,"data_dir")?;
 if !absolute_clean_path(raw_root){return Err(R173Error::Path)}
 let root=raw_root.trim_end_matches('/');
 let root=if root.is_empty(){"/"}else{root};
 let explicit=source_str(h,"telemetry_path")?;
 let strategy_path=if explicit.is_empty(){
  "/var/lib/spotbot/cache/telemetry.json"
 }else{explicit};
 if !absolute_clean_path(strategy_path){return Err(R173Error::Path)}
 let root=root.trim_end_matches('/');
 Ok(PathsPreview{
  scanner_telemetry:format!("{root}/cache/telemetry.json"),
  scanner_binance_cache:format!("{root}/cache/tickers_binance.json"),
  scanner_okx_cache:format!("{root}/cache/tickers_okx.json"),
  strategy_telemetry:strategy_path.to_owned(),
  read_or_written_files:false,
 })
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct ProxyChoice{
 pub from:&'static str,
 pub value:Option<String>,
 pub network_called:bool,
}
/// Environment values are caller-supplied fixtures, never read from OS.
pub fn preview_proxy(cfg:&NativeTradingConfig,
 tg_proxy_env:Option<&str>,telegram_proxy_env:Option<&str>)
 ->Result<ProxyChoice,R173Error>{
 let tg=source_section(cfg,"telegram")?;
 let configured=source_str(tg,"proxy_url")?;
 let (from,value)=if !configured.is_empty(){
  ("yaml",Some(configured))
 }else if let Some(v)=tg_proxy_env.filter(|x|!x.is_empty()){
  ("TG_PROXY_URL",Some(v))
 }else if let Some(v)=telegram_proxy_env.filter(|x|!x.is_empty()){
  ("TELEGRAM_PROXY_URL",Some(v))
 }else{("none",None)};
 Ok(ProxyChoice{from,value:value.map(str::to_owned),network_called:false})
}

#[cfg(test)]
mod tests{
 use super::*;
 const BASE:&str=include_str!("../config/native_trading_operator_baseline_r64.yaml");
 const NOW:i64=1_792_606_800; // 2026-10-21 18:20 UTC
 fn cfg(text:&str)->NativeTradingConfig{
  NativeTradingConfig::parse(text).expect("valid research-only YAML")
 }
 fn changed(section:&str,field:&str,replacement:&str)->String{
  let mut active=String::new();let mut count=0;let mut out=String::new();
  for line in BASE.lines(){
   let trimmed=line.trim();
   if !line.starts_with(' ')&&!trimmed.starts_with('#')&&trimmed.ends_with(':'){
    active=trimmed.trim_end_matches(':').to_owned();
   }
   let prefix=line.chars().take_while(|c|c.is_whitespace()).collect::<String>();
   if active==section&&trimmed.starts_with(&format!("{field}:")){
    out.push_str(&format!("{prefix}{field}: {replacement}\n"));count+=1;
   }else{out.push_str(line);out.push('\n')}
  }
  assert_eq!(count,1);
  out
 }
 #[test]fn r173_report_time_changes_schedule(){
  let base=next_report_utc_s(&cfg(BASE),NOW).unwrap();
  assert_eq!(base,1_792_609_140);
  let change=cfg(&changed("general","report_time","'20:00'"));
  assert_eq!(next_report_utc_s(&change,NOW),Ok(1_792_681_200));
  assert_ne!(next_report_utc_s(&change,NOW),Ok(base));
 }
 #[test]fn r173_timezone_changes_schedule_fail_closed_unknown(){
  let utc=cfg(&changed("general","report_timezone","UTC"));
  assert_eq!(next_report_utc_s(&utc,NOW),Ok(1_792_627_140));
  let dst=cfg(&changed("general","report_timezone","Europe/Helsinki"));
  assert_eq!(next_report_utc_s(&dst,NOW),Err(R173Error::UnsupportedZone));
  assert_eq!(next_report_utc_s(&cfg(BASE),0),Err(R173Error::Clock));
  assert_eq!(next_report_utc_s(&cfg(BASE),1_792_609_140),
   Ok(1_792_695_540));
 }
 #[test]fn r173_data_dir_changes_scanner_only(){
  let a=preview_cache_paths(&cfg(BASE)).unwrap();
  let b=preview_cache_paths(&cfg(&changed("paths","data_dir",
   "/var/lib/spotbot-r173"))).unwrap();
  assert_ne!(a.scanner_telemetry,b.scanner_telemetry);
  assert_eq!(b.scanner_telemetry,
   "/var/lib/spotbot-r173/cache/telemetry.json");
  assert_eq!(a.strategy_telemetry,b.strategy_telemetry);
  assert!(!b.read_or_written_files);
 }
 #[test]fn r173_telemetry_path_is_independent(){
  let a=preview_cache_paths(&cfg(BASE)).unwrap();
  let b=preview_cache_paths(&cfg(&changed("scale_in_hints",
   "telemetry_path","/tmp/r173-telemetry.json"))).unwrap();
  assert_eq!(b.strategy_telemetry,"/tmp/r173-telemetry.json");
  assert_eq!(a.scanner_telemetry,b.scanner_telemetry);
  let traversal=cfg(&changed("paths","data_dir","/tmp/../private"));
  assert_eq!(preview_cache_paths(&traversal),Err(R173Error::Path));
 }
 #[test]fn r173_proxy_precedence_and_no_network(){
  let explicit=cfg(&changed("telegram","proxy_url",
   "'http://local.invalid:1234'"));
  let x=preview_proxy(&explicit,Some("http://one.invalid"),
   Some("http://two.invalid")).unwrap();
  assert_eq!(x.from,"yaml");
  assert_eq!(x.value.as_deref(),Some("http://local.invalid:1234"));
  assert!(!x.network_called);
  let none=cfg(&changed("telegram","proxy_url","''"));
  assert_eq!(preview_proxy(&none,Some("a"),Some("b")).unwrap().from,
   "TG_PROXY_URL");
  assert_eq!(preview_proxy(&none,Some(""),Some("b")).unwrap().from,
   "TELEGRAM_PROXY_URL");
  assert_eq!(preview_proxy(&none,None,None).unwrap().from,"none");
 }
 #[test]fn r173_mutated_unparsed_projection_denied(){
  let mut forged=cfg(BASE);
  forged.raw["general"]["report_time"]=Value::from("18:00");
  assert_eq!(next_report_utc_s(&forged,NOW),Err(R173Error::Source));
  assert_eq!(preview_cache_paths(&forged),Err(R173Error::Source));
  assert_eq!(preview_proxy(&forged,None,None),Err(R173Error::Source));
 }
}
