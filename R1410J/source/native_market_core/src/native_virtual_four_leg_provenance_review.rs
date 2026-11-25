//! Independent, bounded, disk-fsynced PUBLIC DEPTH MODEL audit of four
//! *simulated* PAPER transitions. Deliberately NOT Native PaperOwner exchange
//! fill truth, and NEVER trading or signed external antirollback authority.
use crate::native_paper_portfolio_review::{PaperPortfolio,MAX_CONCURRENT_PAPER,MAX_DAILY_PAPER};
use crate::native_paper_market_execution_review::PaperPhase;
use openssl::sha::sha256;
use serde::{Deserialize,Serialize};
use serde_json::{json,Value};
use std::{collections::{BTreeMap,BTreeSet},fs::{self,File,OpenOptions},
    io::{self,Read,Write},path::{Path,PathBuf}};
const MAX_FILE:u64=8*1024*1024;
const SCHEMA:&str="SPOTARB/PUBLIC-PAPER/MODELED-FOUR-LEG/V1";
const ZERO:&str="0000000000000000000000000000000000000000000000000000000000000000";
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum AuditError{Io(io::ErrorKind),Denied(&'static str)}
impl From<io::Error> for AuditError{
 fn from(e:io::Error)->Self{Self::Io(e.kind())}
}
fn deny(x:&'static str)->AuditError{AuditError::Denied(x)}
fn hash(seq:u64,prev:&str,body:&str)->String{
 let bytes=serde_json::to_vec(&(1_u32,seq,prev,body)).expect("primitive audit hash");
 sha256(&bytes).iter().map(|b|format!("{b:02x}")).collect()
}
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Line{seq:u64,previous_hash:String,body:String,checksum:String}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAuditPayload{
 pub schema:String,pub session:String,pub stage:String,pub base:String,
 pub entries:u64,pub closed:u64,pub open:usize,
 pub modeled_realized_usdt:f64,pub details:Value,
 pub source:String,pub exchange_fills_attested:bool,
 pub real_orders_authorized:bool,pub native_owner_verified_closed:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct AuditState{
 pub sequence:u64,pub last_hash:String,pub entries:u64,pub closed:u64,
 pub open:usize,pub sealed:bool,pub model_phase_by_base:BTreeMap<String,String>,
 pub session:String,pub modeled_realized_bits:u64,
 pub strategy_config_sha256:Option<String>,
 pub strategy_quarantined_bases:BTreeSet<String>,
}
fn phase(x:&PaperPhase)->Result<&'static str,AuditError>{
 match x{
  PaperPhase::AwaitingHedge(_)=>Ok("SPOT_BUY_MODELED"),
  PaperPhase::Hedged(_)=>Ok("FUTURES_SHORT_MODELED"),
  PaperPhase::AwaitingSpotExit{..}=>Ok("FUTURES_COVER_MODELED"),
  _=>Err(deny("terminal or flat position in active PAPER map")),
 }
}
fn phase_map(p:&PaperPortfolio)->Result<BTreeMap<String,String>,AuditError>{
 let mut out=BTreeMap::new();
 for (base,m) in &p.positions{
  if base.len()>100||base.is_empty()||!base.bytes().all(|b|
     b.is_ascii_uppercase()||b.is_ascii_digit())||
     out.insert(base.to_owned(),phase(&m.phase)?.into()).is_some(){
       return Err(deny("invalid model active position"))
  }
 }
 Ok(out)
}
fn valid_account(p:&PaperPortfolio)->bool{
 p.stats.entries<=MAX_DAILY_PAPER as u64&&
 p.stats.closed<=p.stats.entries&&
 p.positions.len()<=MAX_CONCURRENT_PAPER&&
 p.stats.modeled_realized_usdt.is_finite()&&
 p.stats.closed+p.positions.len() as u64<=p.stats.entries
}
fn validate_payload(p:&ModelAuditPayload)->Result<(),AuditError>{
 if p.schema!=SCHEMA||p.source!="PUBLIC_DEPTH_MODEL_ONLY"||
    p.exchange_fills_attested||p.real_orders_authorized||
    p.native_owner_verified_closed||
    p.session.len()>100||!p.session.starts_with("session_")||
    !p.session.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_')||
    p.entries>MAX_DAILY_PAPER as u64||p.closed>p.entries||
    p.open>MAX_CONCURRENT_PAPER||p.closed+p.open as u64>p.entries||
    !p.modeled_realized_usdt.is_finite(){
    return Err(deny("invalid modeled provenance or counters"))
 }
 Ok(())
}
// R106: A strategy view is one extra event in THIS existing modeled
// four-leg journal, not a second writer/source of model portfolio truth.
// This checks consistency only. A local SHA is not an authenticated floor.
fn validate_strategy_view(
 image:&crate::native_paper_strategy_frame_review::StrategyReviewImage,
 state:&AuditState
)->Result<(),AuditError>{
 let p=&image.portfolio;
 if image.schema!=1||image.real_trading_authorized
    ||!crate::native_paper_strategy_frame_review::strategy_review_image_shape_valid(image)
    ||image.config_sha256.len()!=64
    ||!image.config_sha256.bytes().all(|c|c.is_ascii_hexdigit()&&!c.is_ascii_uppercase())
    ||p.stats.entries!=state.entries
    ||p.stats.closed!=state.closed
    ||p.positions.len()!=state.open
    ||p.stats.modeled_realized_usdt.to_bits()!=state.modeled_realized_bits
    ||phase_map(p)?!=state.model_phase_by_base
    ||p.stats.futures_hedges>p.stats.entries
    ||p.stats.futures_covers>p.stats.futures_hedges
    ||p.stats.closed>p.stats.futures_covers
    ||p.stats.closed_wins.checked_add(p.stats.closed_losses)!=Some(p.stats.closed)
    ||p.closed_bases.iter().any(|b|p.positions.contains_key(b))
    ||state.strategy_config_sha256.as_deref().is_some_and(|old|
        old!=image.config_sha256)
    ||!state.strategy_quarantined_bases.is_subset(&image.quarantined_bases)
    ||image.quarantined_bases.iter().any(|b|!p.positions.contains_key(b))
    ||image.previous_public_spreads.iter().any(|(base,(spread,seconds))|
       !p.positions.contains_key(base)
       ||!spread.is_finite()||spread.abs()>1_000_000.0
       ||!seconds.is_finite()||*seconds<0.0){
   return Err(deny("strategy view does not match journal's modeled owner"))
 }
 Ok(())
}
fn apply(state:&mut Option<AuditState>,p:&ModelAuditPayload,
 seq:u64,digest:&str)->Result<(),AuditError>{
 validate_payload(p)?;
 if state.is_none(){
  if seq!=1||p.stage!="SESSION_START"||!p.base.is_empty(){
    return Err(deny("missing modeled session-start genesis"))
  }
  let phases: BTreeMap<String,String>=serde_json::from_value(
       p.details["active_virtual_phases"].clone())
       .map_err(|_|deny("invalid initial modeled phases"))?;
  if phases.len()!=p.open||phases.values().any(|x|
    !matches!(x.as_str(),"SPOT_BUY_MODELED"|"FUTURES_SHORT_MODELED"|"FUTURES_COVER_MODELED")){
    return Err(deny("incorrect initial simulated position count"))
  }
  *state=Some(AuditState{
     sequence:seq,last_hash:digest.into(),entries:p.entries,closed:p.closed,
     open:p.open,sealed:false,model_phase_by_base:phases,session:p.session.clone(),
     modeled_realized_bits:p.modeled_realized_usdt.to_bits(),
     strategy_config_sha256:None,
     strategy_quarantined_bases:BTreeSet::new(),
  });
  return Ok(())
 }
 let s=state.as_mut().unwrap();
 if s.sealed||s.session!=p.session||seq!=s.sequence+1{
   return Err(deny("bad audit progression or immutable seal"))
 }
 // R107: a quarantined model position cannot advance through any
 // modeled four-leg stage. Check at the shared writer/COLD-reader state
 // machine so an attacker cannot bypass record_change with a re-hashed row.
 if s.strategy_quarantined_bases.contains(&p.base)
    &&matches!(p.stage.as_str(),"SPOT_BUY_MODELED"|
     "FUTURES_SHORT_MODELED"|"FUTURES_COVER_MODELED"|"SPOT_SELL_MODELED"){
   return Err(deny("quarantined modeled owner cannot advance without reconciliation"))
 }
 let prior=s.model_phase_by_base.get(&p.base).map(String::as_str);
 let (expected_entries,expected_closed)=match p.stage.as_str(){
   "SPOT_BUY_MODELED" if prior.is_none()&&!p.base.is_empty()=>{
      s.model_phase_by_base.insert(p.base.clone(),p.stage.clone());
      (s.entries.checked_add(1).ok_or(deny("entry overflow"))?,s.closed)
   }
   "FUTURES_SHORT_MODELED" if prior==Some("SPOT_BUY_MODELED")=>{
      s.model_phase_by_base.insert(p.base.clone(),p.stage.clone());
      (s.entries,s.closed)
   }
   "FUTURES_COVER_MODELED" if prior==Some("FUTURES_SHORT_MODELED")=>{
      s.model_phase_by_base.insert(p.base.clone(),p.stage.clone());
      (s.entries,s.closed)
   }
   "SPOT_SELL_MODELED" if prior==Some("FUTURES_COVER_MODELED")=>{
      s.model_phase_by_base.remove(&p.base);
      (s.entries,s.closed.checked_add(1).ok_or(deny("close overflow"))?)
   }
   "STRATEGY_VIEW_MODELED_V1" if p.base.is_empty()=>{
      let image:crate::native_paper_strategy_frame_review::StrategyReviewImage=
         serde_json::from_value(p.details["strategy_image"].clone())
           .map_err(|_|deny("malformed source-bound strategy image"))?;
      if p.details["review_only"]!=true{
         return Err(deny("strategy anchor cannot authorize real trades"))
      }
      validate_strategy_view(&image,s)?;
      s.strategy_config_sha256=Some(image.config_sha256);
      s.strategy_quarantined_bases=image.quarantined_bases;
      (s.entries,s.closed)
   }
   "SESSION_SEAL" if p.base.is_empty()=>(s.entries,s.closed),
   _=>return Err(deny("invalid model four-leg order")),
 };
 if p.entries!=expected_entries||p.closed!=expected_closed||
    p.open!=s.model_phase_by_base.len(){
    return Err(deny("audit counters or virtual exposure diverged"))
 }
 s.entries=p.entries;s.closed=p.closed;s.open=p.open;
 s.sealed=p.stage=="SESSION_SEAL";
 s.sequence=seq;s.last_hash=digest.into();
 s.modeled_realized_bits=p.modeled_realized_usdt.to_bits();
 Ok(())
}
#[derive(Clone,Debug,PartialEq)]
pub struct ModeledTradeNoticeRow{
 pub seq:u64,pub event_hash:String,pub session:String,pub base:String,
 pub stage:String,pub entries:u64,pub closed:u64,pub open:usize,
 pub model_pnl_delta:f64,pub cumulative_model_pnl:f64,
 pub model_quote:Option<f64>,
}
fn read_all_with_notices(path:&Path)->Result<(AuditState,Vec<ModeledTradeNoticeRow>),AuditError>{
 let size=fs::metadata(path)?.len();
 if size==0||size>MAX_FILE{return Err(deny("modeled journal missing or oversized"))}
 let mut buf=Vec::new();
 File::open(path)?.take(MAX_FILE+1).read_to_end(&mut buf)?;
 if buf.last()!=Some(&b'\n')||buf.len() as u64>MAX_FILE{
  return Err(deny("model audit journal partial tail"))
 }
 let mut next=1;let mut prior=ZERO.to_string();let mut state=None;
 let mut notices=Vec::new();
 let mut previous_modeled_pnl:Option<f64>=None;
 for line in buf.split(|b|*b==b'\n').filter(|l|!l.is_empty()){
  let rec:Line=serde_json::from_slice(line).map_err(|_|deny("invalid audit row"))?;
  if rec.seq!=next||rec.previous_hash!=prior||
      rec.checksum!=hash(rec.seq,&prior,&rec.body){
    return Err(deny("modeled provenance checksum or revision mismatch"))
  }
  let payload:ModelAuditPayload=serde_json::from_str(&rec.body)
       .map_err(|_|deny("invalid model provenance body"))?;
  if let Some(earlier)=previous_modeled_pnl{
     let delta=payload.modeled_realized_usdt-earlier;
     if payload.stage=="SPOT_SELL_MODELED"{
        let declared=payload.details.get("modeled_pnl_delta")
           .and_then(Value::as_f64)
           .ok_or(deny("missing modeled close PnL delta"))?;
        if !delta.is_finite()||!declared.is_finite()||
           (delta-declared).abs()>1e-8{
           return Err(deny("modeled final sell PnL delta mismatch"))
        }
     }else if !delta.is_finite()||delta.abs()>1e-8{
        return Err(deny("modeled PnL changed outside final spot sell"))
     }
  }
  apply(&mut state,&payload,rec.seq,&rec.checksum)?;
  if matches!(payload.stage.as_str(),
     "FUTURES_SHORT_MODELED"|"SPOT_SELL_MODELED"){
   let closing=payload.stage=="SPOT_SELL_MODELED";
   let quote=if closing{
    payload.details["derived_spot_sell_quote_from_model_not_exchange_fill"]
      .as_f64()
   }else{
    payload.details["next_virtual_phase"]["Hedged"]
      ["futures_short_proceeds_quote"].as_f64()
   };
   if quote.is_some_and(|x|!x.is_finite()||x<0.0){
    return Err(deny("modeled quote malformed"))
   }
   let delta=if closing{
    payload.details["modeled_pnl_delta"].as_f64()
      .ok_or(deny("missing modeled close pnl delta"))?
   }else{0.0};
   if !delta.is_finite(){return Err(deny("nonfinite model pnl"))}
   notices.push(ModeledTradeNoticeRow{
    seq:rec.seq,event_hash:rec.checksum.clone(),
    session:payload.session.clone(),base:payload.base.clone(),
    stage:payload.stage.clone(),entries:payload.entries,
    closed:payload.closed,open:payload.open,
    model_pnl_delta:delta,
    cumulative_model_pnl:payload.modeled_realized_usdt,
    model_quote:quote,
   });
  }
  previous_modeled_pnl=Some(payload.modeled_realized_usdt);
  next+=1;prior=rec.checksum;
 }
 Ok((state.ok_or(deny("no model genesis"))?,notices))
}
fn read_all(path:&Path)->Result<AuditState,AuditError>{
 read_all_with_notices(path).map(|(state,_)|state)
}
pub fn inspect_model_trade_notices(path:&Path)
 ->Result<(AuditState,Vec<ModeledTradeNoticeRow>),AuditError>{
 read_all_with_notices(path)
}
pub fn inspect_model_journal(path:&Path)->Result<AuditState,AuditError>{read_all(path)}
/// A verified read-only view of the EXISTING modeled journal tip.
/// Neither this record nor a local hash proves independent currentness.
#[derive(Clone,Debug)]
pub struct StrategyJournalReadOnly{
 pub image:crate::native_paper_strategy_frame_review::StrategyReviewImage,
 pub audited_tip:AuditState,
 pub may_resume_paper:bool,
 pub may_trade_real:bool,
}
/// Caller MUST supply an exact hash/sequence floor from an independent
/// trust domain, plus the expected immutable parsed YAML source identity.
/// Never guess the floor from the same potentially rolled-back journal.
pub fn inspect_strategy_view_at_supplied_floor(
 path:&Path,expected_seq:u64,expected_hash:&str,expected_config_sha256:&str,
)->Result<StrategyJournalReadOnly,AuditError>{
 if expected_seq<2||expected_hash.len()!=64
    ||expected_config_sha256.len()!=64
    ||[expected_hash,expected_config_sha256].iter().any(|hex|
       !hex.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())){
   return Err(deny("missing independent strategy journal floor or config"))
 }
 // Hash chain, stage order, all counters and every strategy anchor are
 // checked by the EXISTING journal reader before extracting the tip.
 let state=inspect_model_journal(path)?;
 if state.sealed||state.sequence!=expected_seq||state.last_hash!=expected_hash{
   return Err(deny("modeled journal revision does not match supplied floor"))
 }
 let bytes=fs::read(path)?;
 let last=bytes.split(|b|*b==b'\n')
   .filter(|line|!line.is_empty()).last()
   .ok_or(deny("missing strategy anchor tip"))?;
 let event:Line=serde_json::from_slice(last)
   .map_err(|_|deny("unreadable strategy tip"))?;
 let data:ModelAuditPayload=serde_json::from_str(&event.body)
   .map_err(|_|deny("invalid strategy tip body"))?;
 if event.seq!=state.sequence||event.checksum!=state.last_hash
    ||data.stage!="STRATEGY_VIEW_MODELED_V1"||!data.base.is_empty()
    ||data.details["review_only"]!=true{
   return Err(deny("strategy view is not the latest journal event"))
 }
 let image:crate::native_paper_strategy_frame_review::StrategyReviewImage=
   serde_json::from_value(data.details["strategy_image"].clone())
     .map_err(|_|deny("invalid strategy image at tip"))?;
 if image.config_sha256!=expected_config_sha256{
   return Err(deny("strategy source does not match supplied config"))
 }
 validate_strategy_view(&image,&state)?;
 Ok(StrategyJournalReadOnly{image,audited_tip:state,
   may_resume_paper:false,may_trade_real:false})
}
pub struct ModeledFourLegJournal{
 file:PathBuf, state:AuditState, realized:f64,
}
impl ModeledFourLegJournal{
 pub fn start(path:&Path,session:&str,p:&PaperPortfolio,
     carried:usize)->Result<Self,AuditError>{
   if !valid_account(p)||carried>p.positions.len(){
      return Err(deny("invalid initial modeled portfolio"))
   }
   let phases=phase_map(p)?;
   let details=json!({
      "active_virtual_phases":phases,
      "initial_virtual_positions":p.positions,
      "carried_from_planned_session":carried,
      "modeled_pnl_is_not_exchange_realized":true,
   });
   let mut me=Self{
     file:path.to_owned(),
     state:AuditState{sequence:0,last_hash:ZERO.into(),
        entries:0,closed:0,open:0,sealed:false,
        model_phase_by_base:BTreeMap::new(),session:session.into(),
        modeled_realized_bits:p.stats.modeled_realized_usdt.to_bits(),
        strategy_config_sha256:None,
        strategy_quarantined_bases:BTreeSet::new()},
     realized:p.stats.modeled_realized_usdt,
   };
   let rec=me.payload("SESSION_START","",p,details);
   // create_new intentionally refuses a stale or reused identity.
   let parent=path.parent().ok_or(deny("model journal needs parent"))?;
   fs::create_dir_all(parent)?;
   let _=OpenOptions::new().create_new(true).write(true).open(path)?;
   me.append(rec)?;
   File::open(parent)?.sync_all()?;
   Ok(me)
 }
 fn payload(&self,stage:&str,base:&str,p:&PaperPortfolio,details:Value)
      ->ModelAuditPayload{
   ModelAuditPayload{
     schema:SCHEMA.into(),session:self.state.session.clone(),
     stage:stage.into(),base:base.into(),
     entries:p.stats.entries,closed:p.stats.closed,
     open:p.positions.len(),modeled_realized_usdt:p.stats.modeled_realized_usdt,
     details,source:"PUBLIC_DEPTH_MODEL_ONLY".into(),
     exchange_fills_attested:false,real_orders_authorized:false,
     native_owner_verified_closed:false,
   }
 }
 fn append(&mut self,payload:ModelAuditPayload)->Result<(),AuditError>{
   let next=self.state.sequence+1;
   let body=serde_json::to_string(&payload).map_err(|_|deny("invalid modeled fields"))?;
   let digest=hash(next,&self.state.last_hash,&body);
   let mut trial=if self.state.sequence==0{None}else{Some(self.state.clone())};
   apply(&mut trial,&payload,next,&digest)?;
   let rec=Line{seq:next,previous_hash:self.state.last_hash.clone(),
        body,checksum:digest};
   let mut bytes=serde_json::to_vec(&rec).map_err(|_|deny("audit row encoding failed"))?;
   bytes.push(b'\n');
   if fs::metadata(&self.file)?.len().saturating_add(bytes.len() as u64)>MAX_FILE{
      return Err(deny("simulated journal size cap reached"))
   }
   let mut f=OpenOptions::new().append(true).open(&self.file)?;
   f.write_all(&bytes)?;f.sync_all()?;
   self.state=trial.unwrap();
   Ok(())
 }
 pub fn record_change(&mut self,base:&str,before:&PaperPortfolio,
    after:&PaperPortfolio)->Result<bool,AuditError>{
   // Belt-and-braces denial before even preparing an event. The
   // authoritative check is also in apply() for cold journal replay.
   if self.state.strategy_quarantined_bases.contains(base){
      return Err(deny("quarantined modeled owner cannot mutate"))
   }
   if self.state.sealed||!valid_account(after)||after.day_yek!=before.day_yek||
      after.started_wall_ms!=before.started_wall_ms||
      before.stats.entries!=self.state.entries||
      before.stats.closed!=self.state.closed||
      phase_map(before)?!=self.state.model_phase_by_base{

     return Err(deny("unexpected session change in model journal"))
   }
   let old=before.positions.get(base).map(|m|&m.phase);
   let new=after.positions.get(base).map(|m|&m.phase);
   let stage=match (old,new){
    (None,Some(PaperPhase::AwaitingHedge(_)))=>"SPOT_BUY_MODELED",
    (Some(PaperPhase::AwaitingHedge(_)),Some(PaperPhase::Hedged(_)))=>"FUTURES_SHORT_MODELED",
    (Some(PaperPhase::Hedged(_)),Some(PaperPhase::AwaitingSpotExit{..}))=>"FUTURES_COVER_MODELED",
    (Some(PaperPhase::AwaitingSpotExit{..}),None)
       if after.stats.closed==before.stats.closed+1=>"SPOT_SELL_MODELED",
    _=>return Err(deny("unsupported model position transition")),
   };
   if base.is_empty()||base.len()>100||!base.bytes().all(|b|
      b.is_ascii_uppercase()||b.is_ascii_digit()){
      return Err(deny("invalid modeled base"))
   }
   let pnl_delta=after.stats.modeled_realized_usdt-before.stats.modeled_realized_usdt;
   if !pnl_delta.is_finite()||(stage!="SPOT_SELL_MODELED"&&pnl_delta.abs()>1e-9){
       return Err(deny("modeled PnL may change only on final spot sell"))
   }
   let derived_sell=if stage=="SPOT_SELL_MODELED"{
      if let Some(PaperPhase::AwaitingSpotExit{pos,cover_quote,..})=old{
       let short=pos.futures_short_proceeds_quote
          .ok_or(deny("no futures model proceeds"))?;
       Some(pnl_delta+pos.spot_paid_quote-short+cover_quote+
           pos.spot_paid_quote*before.assumptions.other_cost_bps/10000.0)
      }else{None}
   }else{None};
   if derived_sell.is_some_and(|v|!v.is_finite()||v<=0.0){
       return Err(deny("invalid derived final spot sell quote"))
   }
   let details=json!({
      "prior_virtual_phase":old,
      "next_virtual_phase":new,
      "modeled_pnl_delta":pnl_delta,
      "derived_spot_sell_quote_from_model_not_exchange_fill":derived_sell,
   });
   self.append(self.payload(stage,base,after,details))?;
   self.realized=after.stats.modeled_realized_usdt;
   Ok(true)
 }
 /// Append a read-only strategy state to the SAME modeled four-leg
 /// hash-chain and fsync it. It cannot change four-leg counters or positions.
 /// Cold reader still requires a separately supplied, current journal floor.
 pub fn record_strategy_view(
  &mut self,
  image:&crate::native_paper_strategy_frame_review::StrategyReviewImage,
 )->Result<AuditState,AuditError>{
  if self.state.sealed{
   return Err(deny("cannot add strategy view to sealed modeled journal"))
  }
  validate_strategy_view(image,&self.state)?;
  let details=json!({"strategy_image":image,"review_only":true});
  self.append(self.payload("STRATEGY_VIEW_MODELED_V1","",&image.portfolio,details))?;
  Ok(self.state.clone())
 }
 pub fn seal(&mut self,p:&PaperPortfolio)->Result<AuditState,AuditError>{
    if p.stats.entries!=self.state.entries||
       p.stats.closed!=self.state.closed||
       p.positions.len()!=self.state.open||
       !p.stats.modeled_realized_usdt.is_finite()||
       (p.stats.modeled_realized_usdt-self.realized).abs()>1e-8||
       phase_map(p)?!=self.state.model_phase_by_base{
      return Err(deny("modeled session seal does not match journal tip"))
    }
    self.append(self.payload("SESSION_SEAL","",p,
         json!({"virtual_positions":p.positions,
                "model_pnl_not_realized_on_exchange":true})))?;
    let readback=inspect_model_journal(&self.file)?;
    if readback!=self.state||!readback.sealed{
      return Err(deny("modeled journal readback mismatch"))
    }
    Ok(readback)
 }
 pub fn sequence(&self)->u64{self.state.sequence}
 pub fn journal_path(&self)->&Path{&self.file}
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::native_paper_market_execution_review::{
  PaperMachine,PaperAssumptions,PaperPosition,
 };
 fn new_state()->PaperPortfolio{PaperPortfolio::new(1_791_783_600_000).unwrap()}
 fn pos()->PaperPosition{
  PaperPosition{base:"ONE".into(),signal_ns:1,spot_buy_ns:1,
   signal_estimated_net_bps:52.3,spot_gross_base:4.0,spot_delivered_base:4.0,
   spot_paid_quote:10.0,futures_short_ns:None,futures_short_proceeds_quote:None,
   futures_short_base:None,hedge_entry_estimated_net_bps:None}
 }
 fn dir()->PathBuf{std::env::temp_dir().join(format!(
  "virtual_fourleg_{}_{}",std::process::id(),crate::transport::monotonic_ns()))}
 fn strategy_fixture()->crate::native_paper_strategy_frame_review::SupervisedStrategyPaper{
   use crate::native_trading_config::NativeTradingConfig;
   let src=include_str!("../config/native_trading_operator_baseline_r64.yaml");
   let yaml=src.replacen("paper_max_trades_per_day: 3000",
      "paper_max_trades_per_day: 500",1)
     .replacen("scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false",1)
     .replacen("analytics:\n  enabled: true","analytics:\n  enabled: false",1);
   assert_ne!(yaml,src);
   let cfg=NativeTradingConfig::parse(&yaml).unwrap();
   let mut session=crate::native_paper_strategy_frame_review::SupervisedStrategyPaper::
       new(cfg,1_791_783_600_000).unwrap();
   let mut machine=PaperMachine::new(session.portfolio.assumptions.clone()).unwrap();
   machine.phase=PaperPhase::Hedged(PaperPosition{
     base:"ONE".into(),signal_ns:1_000_000_000,spot_buy_ns:10_000_000_000,
     signal_estimated_net_bps:52.3,spot_gross_base:0.1,
     spot_delivered_base:0.1,spot_paid_quote:10.0,
     futures_short_ns:Some(10_000_000_001),
     futures_short_proceeds_quote:Some(10.2),
     futures_short_base:Some(0.1),hedge_entry_estimated_net_bps:Some(52.3),
   });
   session.portfolio.stats.entries=1;
   session.portfolio.stats.futures_hedges=1;
   session.portfolio.positions.insert("ONE".into(),machine);
   session
 }
 fn public_book(ns:u64,bid:f64,ask:f64)->crate::Book{
   crate::Book{bids:vec![(bid,10.0)],asks:vec![(ask,10.0)],
    received_ns:ns,received_wall_ms:1_791_783_600_000,
    exchange_ts_ms:None,changed_ns:ns,epoch:1,sequence:Some(ns as i64)}
 }
 #[test]
 fn p106_r106_strategy_view_one_existing_journal_quarantine_and_floor(){
   use crate::Venue;
   let root=dir();let file=root.join("model.jsonl");
   let mut paper=strategy_fixture();
   let t=40_000_000_000;
   assert!(paper.on_frame_review(
     "ONE",Venue::BinanceFutures,t,1_791_783_600_000,
     Some(&public_book(t,100.0,100.0)),
     Some(&public_book(t,99.4,100.0)),None,None).is_err());
   let img=paper.replay_image().unwrap();
   assert!(img.quarantined_bases.contains("ONE"));
   let mut journal=ModeledFourLegJournal::start(
      &file,"session_106",&paper.portfolio,1).unwrap();
   let floor=journal.record_strategy_view(&img).unwrap();
   assert_eq!(floor.sequence,2);
   let read=inspect_strategy_view_at_supplied_floor(
     &file,floor.sequence,&floor.last_hash,&img.config_sha256).unwrap();
   assert_eq!(read.audited_tip,floor);
   assert!(read.image.quarantined_bases.contains("ONE"));
   assert!(!read.may_resume_paper&&!read.may_trade_real);
   assert_eq!(read.image.portfolio.stats.entries,1);
   assert_eq!(read.image.portfolio.stats.closed,0);
   assert_eq!(inspect_model_journal(&file).unwrap(),floor);
   assert!(inspect_strategy_view_at_supplied_floor(
     &file,floor.sequence-1,&floor.last_hash,&img.config_sha256).is_err());
   assert!(inspect_strategy_view_at_supplied_floor(
     &file,floor.sequence,&"f".repeat(64),&img.config_sha256).is_err());
   assert!(inspect_strategy_view_at_supplied_floor(
     &file,floor.sequence,&floor.last_hash,&"f".repeat(64)).is_err());
   fs::remove_dir_all(root).unwrap();
 }
 #[test]
 fn p106_r106_quarantine_and_config_cannot_regress_inside_same_journal(){
   let root=dir();let file=root.join("model.jsonl");
   let paper=strategy_fixture();
   let mut image=paper.replay_image().unwrap();
   image.quarantined_bases.insert("ONE".into());
   let mut journal=ModeledFourLegJournal::start(
      &file,"session_106_immutable",&paper.portfolio,1).unwrap();
   let first=journal.record_strategy_view(&image).unwrap();
   assert!(first.strategy_quarantined_bases.contains("ONE"));
   let mut disappeared=image.clone();
   disappeared.quarantined_bases.clear();
   assert!(journal.record_strategy_view(&disappeared).is_err());
   let mut swapped=image.clone();
   swapped.config_sha256="b".repeat(64);
   assert!(journal.record_strategy_view(&swapped).is_err());
   assert_eq!(journal.sequence(),first.sequence);
   let cold=inspect_model_journal(&file).unwrap();
   assert_eq!(cold,first);
   fs::remove_dir_all(root).unwrap();
 }
 #[test]
 fn p106_r107_emergency_freezes_modeled_cover_even_if_pnl_unchanged(){
  let root=dir();let file=root.join("model.jsonl");
  let paper=strategy_fixture();
  let mut image=paper.replay_image().unwrap();
  image.quarantined_bases.insert("ONE".into());
  let mut journal=ModeledFourLegJournal::start(
   &file,"session_107_freeze",&paper.portfolio,1).unwrap();
  let safe=journal.record_strategy_view(&image).unwrap();
  let before=paper.portfolio.clone();
  let mut after=before.clone();
  let pos=match &before.positions["ONE"].phase{
   PaperPhase::Hedged(p)=>p.clone(),_=>panic!("fixture expected hedge"),
  };
  after.stats.futures_covers=1;
  after.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
   pos,cover_quote:10.0,cover_ns:20_000_000_000,
   close_estimate_quote:0.0,
  };
  assert!(journal.record_change("ONE",&before,&after).is_err());
  assert_eq!(journal.sequence(),safe.sequence);
  assert_eq!(inspect_model_journal(&file).unwrap(),safe);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]
 fn p106_r107_cold_reader_denies_hash_valid_quarantined_trade(){
  let root=dir();let file=root.join("model.jsonl");
  let paper=strategy_fixture();
  let mut image=paper.replay_image().unwrap();
  image.quarantined_bases.insert("ONE".into());
  let mut journal=ModeledFourLegJournal::start(
   &file,"session_107_cold",&paper.portfolio,1).unwrap();
  let safe=journal.record_strategy_view(&image).unwrap();
  let next=safe.sequence+1;
  // Deliberately bypass writer and forge a correctly re-hashed row.
  // Reader must reject on SEMANTICS, not only SHA, seq or parent.
  let mut payload=journal.payload("FUTURES_COVER_MODELED","ONE",
   &paper.portfolio,json!({"attack":"ignore existing quarantine"}));
  payload.entries=safe.entries;payload.closed=safe.closed;payload.open=safe.open;
  let body=serde_json::to_string(&payload).unwrap();
  let checksum=hash(next,&safe.last_hash,&body);
  let row=Line{seq:next,previous_hash:safe.last_hash.clone(),body,checksum};
  let mut bytes=serde_json::to_vec(&row).unwrap();
  bytes.push(b'\n');
  {
   let mut out=OpenOptions::new().append(true).open(&file).unwrap();
   out.write_all(&bytes).unwrap();out.sync_all().unwrap();
  }
  assert!(inspect_model_journal(&file).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]
 fn p106_r108_reject_mismatched_hedge_image_at_writer_and_cold_reader(){
  let root=dir();let file=root.join("model.jsonl");
  let paper=strategy_fixture();
  let mut malformed=paper.replay_image().unwrap();
  if let PaperPhase::Hedged(ref mut pos)=
     malformed.portfolio.positions.get_mut("ONE").unwrap().phase{
   pos.futures_short_base=Some(0.05);
  }
  let mut journal=ModeledFourLegJournal::start(
   &file,"session_108_writer",&paper.portfolio,1).unwrap();
  assert!(journal.record_strategy_view(&malformed).is_err());
  assert_eq!(journal.sequence(),1);
  // A bypass of the application writer with recomputed SHA still fails.
  let genesis=inspect_model_journal(&file).unwrap();
  let payload=journal.payload("STRATEGY_VIEW_MODELED_V1","",
   &malformed.portfolio,json!({"strategy_image":malformed,"review_only":true}));
  let next=genesis.sequence+1;
  let body=serde_json::to_string(&payload).unwrap();
  let checksum=hash(next,&genesis.last_hash,&body);
  let row=Line{seq:next,previous_hash:genesis.last_hash,body,checksum};
  let mut bytes=serde_json::to_vec(&row).unwrap();bytes.push(b'\n');
  {
   let mut f=OpenOptions::new().append(true).open(&file).unwrap();
   f.write_all(&bytes).unwrap();f.sync_all().unwrap();
  }
  assert!(inspect_model_journal(&file).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]
 fn p106_r106_partial_tail_and_forged_state_never_publish_handoff(){
   let root=dir();let file=root.join("model.jsonl");
   let paper=strategy_fixture();
   let good=paper.replay_image().unwrap();
   let mut journal=ModeledFourLegJournal::start(
     &file,"session_106_fault",&paper.portfolio,1).unwrap();
   let mut forged=good.clone();
   forged.portfolio.stats.entries=2;
   assert!(journal.record_strategy_view(&forged).is_err());
   assert_eq!(journal.sequence(),1);
   let mut forged=good.clone();
   forged.quarantined_bases.insert("MISSING".to_string());
   assert!(journal.record_strategy_view(&forged).is_err());
   assert_eq!(journal.sequence(),1);
   let tip=journal.record_strategy_view(&good).unwrap();
   assert!(inspect_strategy_view_at_supplied_floor(
     &file,tip.sequence,&tip.last_hash,&good.config_sha256).is_ok());
   {
     let mut out=OpenOptions::new().append(true).open(&file).unwrap();
     out.write_all(b"{\\\"unfinished\\\":").unwrap();
     out.sync_all().unwrap();
   }
   assert!(inspect_strategy_view_at_supplied_floor(
      &file,tip.sequence,&tip.last_hash,&good.config_sha256).is_err());
   assert!(inspect_model_journal(&file).is_err());
   fs::remove_dir_all(root).unwrap();
 }
 #[test]
 fn p106_r106_strategy_view_stays_non_authoritative_after_later_transition(){
   let root=dir();let file=root.join("model.jsonl");
   let paper=strategy_fixture();
   let img=paper.replay_image().unwrap();
   let mut journal=ModeledFourLegJournal::start(
     &file,"session_106_next",&paper.portfolio,1).unwrap();
   let old=journal.record_strategy_view(&img).unwrap();
   let before=paper.portfolio.clone();
   let mut after=before.clone();
   let pos=match &before.positions["ONE"].phase{
     PaperPhase::Hedged(p)=>p.clone(),_=>panic!("fixture not hedged"),
   };
   after.stats.futures_covers=1;
   after.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
      pos,cover_quote:10.0,cover_ns:20_000_000_000,
      close_estimate_quote:0.1,
   };
   journal.record_change("ONE",&before,&after).unwrap();
   assert_eq!(journal.sequence(),old.sequence+1);
   assert!(inspect_strategy_view_at_supplied_floor(
     &file,old.sequence,&old.last_hash,&img.config_sha256).is_err());
   let current=inspect_model_journal(&file).unwrap();
   assert_eq!(current.model_phase_by_base["ONE"],"FUTURES_COVER_MODELED");
   assert!(inspect_strategy_view_at_supplied_floor(
     &file,current.sequence,&current.last_hash,&img.config_sha256).is_err());
   fs::remove_dir_all(root).unwrap();
 }
 #[test]fn full_four_stage_simulation_has_bounded_verified_cold_history(){
   let root=dir();let file=root.join("model.jsonl");
   let mut p=new_state();
   let mut j=ModeledFourLegJournal::start(&file,"session_20739_1",&p,0).unwrap();
   let mut before=p.clone();
   p.stats.entries=1;
   p.positions.insert("ONE".into(),PaperMachine{
      config:PaperAssumptions::default(),phase:PaperPhase::AwaitingHedge(pos())});
   j.record_change("ONE",&before,&p).unwrap();
   before=p.clone();
   let mut hedged=pos();
   hedged.futures_short_ns=Some(2);
   hedged.futures_short_base=Some(4.0);
   hedged.futures_short_proceeds_quote=Some(10.2);
   hedged.hedge_entry_estimated_net_bps=Some(180.0);
   p.stats.futures_hedges=1;
   p.positions.get_mut("ONE").unwrap().phase=PaperPhase::Hedged(hedged.clone());
   j.record_change("ONE",&before,&p).unwrap();
   before=p.clone();
   p.stats.futures_covers=1;
   p.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
      pos:hedged,cover_quote:10.0,cover_ns:3,close_estimate_quote:0.2};
   j.record_change("ONE",&before,&p).unwrap();
   before=p.clone();
   p.stats.closed=1;
   p.stats.modeled_realized_usdt=0.3;
   p.positions.remove("ONE");
   j.record_change("ONE",&before,&p).unwrap();
   let v=j.seal(&p).unwrap();
   assert_eq!(v.sequence,6);assert_eq!(v.closed,1);
   assert_eq!(v.open,0);assert!(v.sealed);
   assert_eq!(inspect_model_journal(&file).unwrap(),v);
   // Hash-consistent tampering is still a semantic violation: signing
   // local rows by SHA alone cannot authorize a made-up PnL delta.
   let original=fs::read(&file).unwrap();
   let mut rows:Vec<Line>=original.split(|b|*b==b'\n')
       .filter(|line|!line.is_empty())
       .map(|line|serde_json::from_slice(line).unwrap()).collect();
   let close_row=rows.iter_mut().find(|row|{
      serde_json::from_str::<ModelAuditPayload>(&row.body)
        .unwrap().stage=="SPOT_SELL_MODELED"
   }).unwrap();
   let mut payload:ModelAuditPayload=serde_json::from_str(&close_row.body).unwrap();
   payload.details["modeled_pnl_delta"]=json!(77.0);
   close_row.body=serde_json::to_string(&payload).unwrap();
   let mut prev=ZERO.to_string();
   for row in rows.iter_mut(){
      row.previous_hash=prev.clone();
      row.checksum=hash(row.seq,&prev,&row.body);
      prev=row.checksum.clone();
   }
   let forged=rows.iter().flat_map(|row|{
      let mut encoded=serde_json::to_vec(row).unwrap();
      encoded.push(b'\n');encoded
   }).collect::<Vec<u8>>();
   fs::write(&file,forged).unwrap();
   assert!(matches!(inspect_model_journal(&file),
       Err(AuditError::Denied("modeled final sell PnL delta mismatch"))));
   fs::write(&file,&original).unwrap();
   // The SESSION_SEAL row can be checksum-consistent yet must never
   // invent additional modeled profit after the closing leg.
   let mut rows:Vec<Line>=original.split(|b|*b==b'\n')
       .filter(|line|!line.is_empty())
       .map(|line|serde_json::from_slice(line).unwrap()).collect();
   let seal=rows.last_mut().unwrap();
   let mut payload:ModelAuditPayload=serde_json::from_str(&seal.body).unwrap();
   payload.modeled_realized_usdt+=1.0;
   seal.body=serde_json::to_string(&payload).unwrap();
   seal.checksum=hash(seal.seq,&seal.previous_hash,&seal.body);
   let forged=rows.iter().flat_map(|row|{
      let mut encoded=serde_json::to_vec(row).unwrap();
      encoded.push(b'\n');encoded
   }).collect::<Vec<u8>>();
   fs::write(&file,forged).unwrap();
   assert!(matches!(inspect_model_journal(&file),
       Err(AuditError::Denied("modeled PnL changed outside final spot sell"))));
   fs::write(&file,&original).unwrap();
   assert_eq!(inspect_model_journal(&file).unwrap(),v);
   let mut bytes=fs::read(&file).unwrap();
   let n=bytes.iter().position(|b|*b==b'0').unwrap();
   bytes[n]=b'9';fs::write(&file,bytes).unwrap();
   assert!(inspect_model_journal(&file).is_err());
   fs::remove_dir_all(root).unwrap();
 }
 #[test]fn model_does_not_accept_fake_hedge_or_reused_session(){
   let root=dir();let file=root.join("x.jsonl");
   let mut p=new_state();
   let mut j=ModeledFourLegJournal::start(&file,"session_1",&p,0).unwrap();
   assert!(ModeledFourLegJournal::start(&file,"session_1",&p,0).is_err());
   p.stats.entries=1;
   p.positions.insert("ONE".into(),PaperMachine{
       config:PaperAssumptions::default(),phase:PaperPhase::Hedged(pos())});
   assert!(j.record_change("ONE",&new_state(),&p).is_err());
   assert_eq!(inspect_model_journal(&file).unwrap().sequence,1);
   fs::remove_dir_all(root).unwrap();
 }
}

include!("native_r968_exact_modeled_phase_review.rs");
