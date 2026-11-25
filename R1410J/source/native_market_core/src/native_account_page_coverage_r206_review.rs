// R206: offline completeness audit for claimed two-venue, multi-page snapshots.
// Cannot prove exchange ownership, identity or actual zero positions via mock input.
#[derive(Debug,Clone)]
pub struct AccountPageR206{
 pub venue:EvidenceVenueR189,pub account_owner:String,
 pub page_number:u32,pub total_pages:u32,
 pub next_page:bool,pub snapshot_nonce:String,pub exchange_time_ms:u64,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct PaginationAuditR206{
 pub pages:usize,pub both_venues:bool,
 pub complete_shape:bool,pub exchange_authenticated:bool,pub may_trade:bool,
}
pub fn audit_account_pages_r206(pages:&[AccountPageR206],now:u64)
 ->Result<PaginationAuditR206,PaperAdapterErrorR156>{
 use std::collections::HashSet;
 if pages.len()<2||pages.len()>64{return Err(PaperAdapterErrorR156::Limit)}
 let owner=&pages[0].account_owner;
 if reject_identity_r154(owner){return Err(PaperAdapterErrorR156::Source)}
 let mut seen=HashSet::new();let mut venues=HashSet::new();
 for p in pages{
  if p.account_owner!=*owner||p.total_pages==0||p.total_pages>32
   ||p.page_number==0||p.page_number>p.total_pages
   ||p.next_page!=(p.page_number<p.total_pages)
   ||p.snapshot_nonce.len()!=64||!valid_hex_r156(&p.snapshot_nonce)
   ||p.exchange_time_ms==0||p.exchange_time_ms>now.saturating_add(1000)
   ||now.saturating_sub(p.exchange_time_ms)>10_000
   ||!seen.insert((p.venue,p.page_number)){
   return Err(PaperAdapterErrorR156::Source)
  }
  venues.insert(p.venue);
 }
 if venues.len()!=2{return Err(PaperAdapterErrorR156::Source)}
 for venue in venues{
  let subset:Vec<_>=pages.iter().filter(|p|p.venue==venue).collect();
  let first=subset[0];
  if subset.len()!=first.total_pages as usize
   ||subset.iter().any(|p|p.total_pages!=first.total_pages
     ||p.snapshot_nonce!=first.snapshot_nonce
     ||p.exchange_time_ms!=first.exchange_time_ms){
   return Err(PaperAdapterErrorR156::Source)
  }
 }
 Ok(PaginationAuditR206{
  pages:pages.len(),both_venues:true,complete_shape:true,
  exchange_authenticated:false,may_trade:false
 })
}
#[cfg(test)]
mod r206_tests{
 use super::*;
 const NOW:u64=1_792_280_000_000;
 fn fixture()->Vec<AccountPageR206>{
  let mut v=Vec::new();
  for venue in [EvidenceVenueR189::OkxSpot,EvidenceVenueR189::BinanceUsdtMFutures]{
   for page in 1..=2{
    v.push(AccountPageR206{
     venue,account_owner:"mock-user".into(),page_number:page,total_pages:2,
     next_page:page<2,snapshot_nonce:"a".repeat(64),
     exchange_time_ms:NOW-2000
    });
   }
  }
  v
 }
 #[test]fn r206_two_venues_all_pages_present_is_not_authoritative_proof(){
  let pages=fixture();
  let report=audit_account_pages_r206(&pages,NOW).unwrap();
  assert_eq!(report.pages,4);
  assert!(report.complete_shape);
  assert!(!report.exchange_authenticated&&!report.may_trade);
 }
 #[test]fn r206_missing_page_repeated_cursor_and_stale_snapshot_rejected(){
  let mut pages=fixture();pages.remove(0);
  assert!(audit_account_pages_r206(&pages,NOW).is_err());
  pages=fixture();pages[0].page_number=2;
  assert!(audit_account_pages_r206(&pages,NOW).is_err());
  pages=fixture();pages[3].snapshot_nonce="b".repeat(64);
  assert!(audit_account_pages_r206(&pages,NOW).is_err());
  pages=fixture();pages[1].exchange_time_ms=NOW-12_000;
  assert!(audit_account_pages_r206(&pages,NOW).is_err());
  pages=fixture();pages[0].next_page=false;
  assert!(audit_account_pages_r206(&pages,NOW).is_err());
 }
}
