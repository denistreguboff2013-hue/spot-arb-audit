use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct Book {
    pub bids: HashMap<String, String>,
    pub asks: HashMap<String, String>,
    pub last_id: Option<u64>,
    pub valid: bool,
    pub invalid_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Delta {
    pub first_id: u64,
    pub final_id: u64,
    pub previous_final_id: u64,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

impl Book {
    pub fn from_snapshot(last_id: u64, bids: Vec<(String,String)>, asks: Vec<(String,String)>) -> Self {
        let mut b = Self { bids: HashMap::new(), asks: HashMap::new(), last_id: Some(last_id), valid: true, invalid_reason: None };
        b.apply_levels(true, bids);
        b.apply_levels(false, asks);
        b
    }

    fn apply_levels(&mut self, is_bid: bool, levels: Vec<(String,String)>) {
        let side = if is_bid { &mut self.bids } else { &mut self.asks };
        for (price, qty) in levels {
            if qty.parse::<f64>().unwrap_or(0.0) == 0.0 { side.remove(&price); }
            else { side.insert(price, qty); }
        }
    }

    pub fn bridge(&mut self, snapshot_id: u64, event: &Delta) -> Result<(), String> {
        if !(event.first_id <= snapshot_id && event.final_id >= snapshot_id) {
            return Err(format!("no snapshot bridge: U={} L={} u={}", event.first_id, snapshot_id, event.final_id));
        }
        self.apply_levels(true, event.bids.clone());
        self.apply_levels(false, event.asks.clone());
        self.last_id = Some(event.final_id);
        self.validate_crossed()
    }

    pub fn apply(&mut self, event: &Delta) -> Result<bool, String> {
        if !self.valid { return Err(self.invalid_reason.clone().unwrap_or_else(|| "book invalid".into())); }
        let last = self.last_id.ok_or_else(|| "snapshot missing".to_string())?;
        if event.final_id <= last { return Ok(false); }
        if event.previous_final_id != last {
            let reason = format!("sequence gap/reorder: pu={} expected={} U={} u={}", event.previous_final_id, last, event.first_id, event.final_id);
            self.valid = false;
            self.invalid_reason = Some(reason.clone());
            return Err(reason);
        }
        self.apply_levels(true, event.bids.clone());
        self.apply_levels(false, event.asks.clone());
        self.last_id = Some(event.final_id);
        self.validate_crossed()?;
        Ok(true)
    }

    fn validate_crossed(&mut self) -> Result<(), String> {
        let bid = self.bids.keys().filter_map(|x| x.parse::<f64>().ok()).fold(None, |m: Option<f64>, x| Some(m.map_or(x, |v| v.max(x))));
        let ask = self.asks.keys().filter_map(|x| x.parse::<f64>().ok()).fold(None, |m: Option<f64>, x| Some(m.map_or(x, |v| v.min(x))));
        if let (Some(b),Some(a)) = (bid,ask) {
            if b >= a {
                self.valid = false;
                self.invalid_reason = Some(format!("crossed/locked book: bid={b} ask={a}"));
                return Err(self.invalid_reason.clone().unwrap());
            }
        }
        Ok(())
    }

    pub fn top(&self, is_bid: bool, depth: usize) -> Vec<(String,String)> {
        let map = if is_bid { &self.bids } else { &self.asks };
        let mut rows: Vec<_> = map.iter().map(|(p,q)|(p.clone(),q.clone())).collect();
        rows.sort_by(|a,b| {
            let ap=a.0.parse::<f64>().unwrap_or(0.0); let bp=b.0.parse::<f64>().unwrap_or(0.0);
            if is_bid { bp.total_cmp(&ap) } else { ap.total_cmp(&bp) }
        });
        rows.truncate(depth);
        rows
    }

    pub fn sweep_quote(&self, is_buy: bool, quote_notional: f64) -> Option<(f64,f64,f64,usize)> {
        if !self.valid || quote_notional <= 0.0 { return None; }
        let levels=self.top(!is_buy, usize::MAX);
        let mut remain=quote_notional; let mut filled=0.0; let mut base=0.0; let mut used=0;
        for (p,q) in levels {
            let price=p.parse::<f64>().ok()?; let qty=q.parse::<f64>().ok()?;
            if price <= 0.0 || qty < 0.0 { return None; }
            let level_quote=price*qty; let take=remain.min(level_quote);
            if take > 0.0 { base += take/price; filled += take; remain -= take; used += 1; }
            if remain <= quote_notional*1e-12 { break; }
        }
        let vwap=if base>0.0 {filled/base} else {0.0};
        Some((filled,base,vwap,used))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(u:u64, pu:u64, bids:Vec<(&str,&str)>, asks:Vec<(&str,&str)>) -> Delta {
        Delta { first_id:u, final_id:u, previous_final_id:pu, bids:bids.into_iter().map(|(a,b)|(a.into(),b.into())).collect(), asks:asks.into_iter().map(|(a,b)|(a.into(),b.into())).collect() }
    }

    #[test]
    fn snapshot_bridge_and_absolute_updates() {
        let mut b=Book::from_snapshot(100,vec![("99".into(),"2".into())],vec![("101".into(),"3".into())]);
        let bridge=Delta { first_id:99, final_id:101, previous_final_id:98, bids:vec![("99".into(),"4".into())], asks:vec![("102".into(),"1".into())] };
        assert!(b.bridge(100,&bridge).is_ok());
        assert_eq!(b.last_id,Some(101));
        let next=d(102,101,vec![("99","0")],vec![("101","1")]);
        assert_eq!(b.apply(&next),Ok(true));
        assert!(!b.bids.contains_key("99"));
        assert_eq!(b.asks.get("101").unwrap(),"1");
    }

    #[test]
    fn duplicate_is_ignored_without_invalidating() {
        let mut b=Book::from_snapshot(100,vec![("99".into(),"2".into())],vec![("101".into(),"3".into())]);
        let duplicate=d(100,99,vec![],vec![]);
        assert_eq!(b.apply(&duplicate),Ok(false));
        assert!(b.valid);
    }

    #[test]
    fn gap_reorder_and_delayed_generation_fail_closed() {
        let mut b=Book::from_snapshot(100,vec![("99".into(),"2".into())],vec![("101".into(),"3".into())]);
        let bad=d(102,99,vec![("99","5")],vec![]);
        assert!(b.apply(&bad).is_err());
        assert!(!b.valid);
        let later=d(101,100,vec![("99","7")],vec![]);
        assert!(b.apply(&later).is_err());
        assert_eq!(b.bids.get("99").unwrap(),"2");
    }

    #[test]
    fn resnapshot_starts_a_clean_generation_after_gap() {
        let mut old=Book::from_snapshot(100,vec![("99".into(),"2".into())],vec![("101".into(),"3".into())]);
        assert!(old.apply(&d(102,99,vec![("99","5")],vec![])).is_err());
        let mut fresh=Book::from_snapshot(200,vec![("98".into(),"4".into())],vec![("102".into(),"5".into())]);
        let bridge=Delta { first_id:199, final_id:201, previous_final_id:198, bids:vec![("98".into(),"6".into())], asks:vec![] };
        assert!(fresh.bridge(200,&bridge).is_ok());
        assert!(fresh.valid);
        assert_eq!(fresh.last_id,Some(201));
        assert_eq!(fresh.bids.get("98").unwrap(),"6");
        assert_eq!(old.bids.get("99").unwrap(),"2");
    }

    #[test]
    fn crossed_book_invalidates() {
        let mut b=Book::from_snapshot(10,vec![("100".into(),"1".into())],vec![("101".into(),"1".into())]);
        let bad=d(11,10,vec![("102","1")],vec![]);
        assert!(b.apply(&bad).is_err());
        assert!(!b.valid);
    }

    #[test]
    fn sweep_is_depth_limited_and_never_invents_fill() {
        let b=Book::from_snapshot(10,vec![("99".into(),"1".into()),("98".into(),"1".into())],vec![("101".into(),"1".into()),("102".into(),"1".into())]);
        let (fill,base,vwap,levels)=b.sweep_quote(true,150.0).unwrap();
        assert!((fill-150.0).abs()<1e-9);
        assert!(base>0.0);
        assert!(vwap>=101.0 && vwap<102.0);
        assert_eq!(levels,2);
        let (partial,_,_,_)=b.sweep_quote(true,500.0).unwrap();
        assert!(partial<500.0);
    }
}
