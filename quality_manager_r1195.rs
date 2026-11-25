//! R1195 experimental, execution-free, source-only Binance WS quality manager.
//! Not wired to PAPER/REAL, DB1, DB2 or order placement. Fails closed.
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

pub const MAX_AGE_MS: i64 = 800;
/// Shared Futures partial-snapshot progress contract: u must increase; E must not regress.
pub fn futures_partial_advances<T:Ord>(old:T,new:T,old_e:i64,new_e:i64)->bool{new>old&&new_e>=old_e}
const HISTORY_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const MAX_HISTORY_PEERS: usize = 128;
const MAX_SAMPLES: usize = 2048;
const QUALIFY_SAMPLES_PER_SYMBOL: u8 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Tier { Candidate, Warm, Hot, Active, Suspect, Quarantined }

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub event_ms: i64,
    pub receive_mono_ms: u64,
    pub sequence: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct Session {
    pub id: u64,
    pub peer: String,
    pub symbols: Vec<String>,
    pub tier: Tier,
    pub quotes: HashMap<String, Snapshot>,
    pub last_accepted_seq: HashMap<String,u64>,
    pub last_accepted_event_ms: HashMap<String,i64>,
    pub consecutive_good: HashMap<String, u8>,
    pub first_good_mono: HashMap<String, u64>,
    pub seen: HashMap<String, u64>,
    pub transport_errors: u32,
    pub rtt_ms: Option<f64>,
    pub ooo_ratio: Option<f64>,
    pub samples: VecDeque<(u64, bool)>,
    // A bounded 10s witness per symbol; never carry market payloads here.
    recent_bad_by_symbol: HashMap<String, VecDeque<u64>>,
}

impl Session {
    fn is_ready(&self, symbols: &[String], now_utc: i64, now_mono: u64) -> bool {
        !matches!(self.tier, Tier::Quarantined | Tier::Suspect) &&
        symbols.iter().all(|symbol| {
            let Some(q) = self.quotes.get(symbol) else { return false; };
            let enough = self.consecutive_good.get(symbol).copied().unwrap_or(0) >= QUALIFY_SAMPLES_PER_SYMBOL;
            let spread = self.first_good_mono.get(symbol).map(|t| now_mono.saturating_sub(*t) >= 750).unwrap_or(false);
            enough && spread && fresh(q, now_utc, now_mono)
        })
    }
    pub fn recent_bad_fraction(&self) -> f64 {
        if self.samples.is_empty() { return 1.0; }
        self.samples.iter().filter(|(_, ok)| !ok).count() as f64 / self.samples.len() as f64
    }
}

fn fresh(q: &Snapshot, now_utc: i64, now_mono: u64) -> bool {
    let age = now_utc.checked_sub(q.event_ms);
    matches!(age, Some(0..=MAX_AGE_MS)) &&
        now_mono >= q.receive_mono_ms &&
        now_mono - q.receive_mono_ms <= MAX_AGE_MS as u64
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerRecord {
    pub peer: String,
    pub updated_utc_ms: i64,
    pub tested_frames: u64,
    pub stale_frames: u64,
    pub median_rtt_ms: Option<f64>,
    pub ooo_ratio: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct History {
    pub version: u32,
    pub peers: Vec<PeerRecord>,
}
impl Default for History {
    fn default() -> Self { Self { version: 1, peers: Vec::new() } }
}
impl History {
    pub fn load(path: &Path, now_utc: i64) -> Result<Self, String> {
        if !path.exists() { return Ok(Self::default()); }
        let bytes = fs::read(path).map_err(|e| format!("history read: {e}"))?;
        if bytes.len() > 1024 * 1024 { return Err("history too large".into()); }
        let mut h: Self = serde_json::from_slice(&bytes).map_err(|e| format!("history parse: {e}"))?;
        if h.version != 1 { return Err("unknown history schema".into()); }
        h.peers.retain(|p| p.updated_utc_ms <= now_utc &&
            now_utc.saturating_sub(p.updated_utc_ms) <= HISTORY_TTL_MS &&
            p.median_rtt_ms.map(|v| v.is_finite() && v >= 0.0).unwrap_or(true) &&
            p.ooo_ratio.map(|v| v.is_finite() && v >= 0.0).unwrap_or(true));
        h.peers.sort_by_key(|p| std::cmp::Reverse(p.updated_utc_ms));
        h.peers.truncate(MAX_HISTORY_PEERS);
        Ok(h)
    }
    /// Small atomic JSON snapshot; never record per-frame payloads.
    pub fn save_atomic(&self, path: &Path) -> Result<(), String> {
        let parent = path.parent().ok_or("no parent dir")?;
        // Never reuse a fixed .pending pathname: a crash before rename must not
        // permanently prevent a later cold process from saving its history.
        let nonce=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|e|e.to_string())?.as_nanos();
        let temp=path.with_extension(format!("json.{}.{}.pending",std::process::id(),nonce));
        let bytes = serde_json::to_vec_pretty(self).map_err(|e| e.to_string())?;
        if bytes.len() > 1024 * 1024 { return Err("history too large".into()); }
        let mut f = OpenOptions::new().write(true).create_new(true).open(&temp)
            .map_err(|e| format!("history create: {e}"))?;
        if let Err(e) = f.write_all(&bytes).and_then(|_| f.sync_all()) {
            let _ = fs::remove_file(&temp);
            return Err(format!("history fsync: {e}"));
        }
        if let Err(e)=fs::rename(&temp,path) {
            let _=fs::remove_file(&temp);
            return Err(format!("history rename: {e}"));
        }
        let parent_fd = fs::File::open(parent).map_err(|e| e.to_string())?;
        parent_fd.sync_all().map_err(|e| e.to_string())
    }
    pub fn remember(&mut self, record: PeerRecord) {
        self.peers.retain(|p| p.peer != record.peer);
        self.peers.push(record);
        self.peers.sort_by_key(|p| std::cmp::Reverse(p.updated_utc_ms));
        self.peers.truncate(MAX_HISTORY_PEERS);
    }
    pub fn preferred(&self) -> Vec<String> {
        let mut v = self.peers.clone();
        v.sort_by(|a,b| {
            let score = |r:&PeerRecord| {
                let bad = (r.stale_frames as f64) / (r.tested_frames.max(1) as f64);
                (bad * 1000.0) + r.median_rtt_ms.unwrap_or(1000.0)/100.0 +
                    r.ooo_ratio.unwrap_or(1.0)*100.0
            };
            score(a).total_cmp(&score(b))
        });
        v.into_iter().map(|p| p.peer).collect()
    }
}

#[derive(Clone, Debug)]
pub struct Selection {
    pub session_id: u64,
    pub epoch: u64,
}
#[derive(Clone, Debug)]
pub struct Publish {
    pub symbol: String,
    pub event_ms: i64,
    pub session_id: u64,
    pub epoch: u64,
}
#[derive(Default)]
pub struct Manager {
    pub sessions: HashMap<u64, Session>,
    pub active: HashMap<String, Selection>,
    pub group_symbols: HashMap<String, Vec<String>>,
    last_published: HashMap<String, i64>,
    last_published_seq:HashMap<String,u64>,
    epoch: u64,
    pub history: History,
}

impl Manager {
    pub fn define_group(&mut self, key: &str, symbols: Vec<String>) -> Result<(), String> {
        if key.is_empty() || symbols.is_empty() || symbols.len() > 8 {
            return Err("invalid group width".into());
        }
        let mut uniq = symbols.clone();
        uniq.sort(); uniq.dedup();
        if uniq.len() != symbols.len() || symbols.iter().any(|s| s.is_empty()) {
            return Err("duplicate or empty symbol".into());
        }
        if self.group_symbols.contains_key(key) { return Err("group redefinition".into()); }
        self.group_symbols.insert(key.to_string(), symbols);
        Ok(())
    }

    pub fn add_session(&mut self, id: u64, peer: &str, group: &str) -> Result<(), String> {
        if self.sessions.contains_key(&id) { return Err("session ID reuse".into()); }
        let symbols = self.group_symbols.get(group).ok_or("unknown group")?.clone();
        let s = Session { id, peer: peer.into(), symbols, tier: Tier::Candidate,
            quotes: HashMap::new(),last_accepted_seq: HashMap::new(),
            last_accepted_event_ms:HashMap::new(),consecutive_good: HashMap::new(),
            first_good_mono: HashMap::new(), seen: HashMap::new(),
            transport_errors: 0, rtt_ms: None, ooo_ratio: None, samples: VecDeque::new(),
            recent_bad_by_symbol: HashMap::new() };
        self.sessions.insert(id,s);
        Ok(())
    }

    pub fn observe(&mut self, id:u64, symbol:&str, event_ms:i64, recv_utc:i64,
        recv_mono:u64, sequence:Option<u64>, valid_full_book:bool) -> Result<bool,String> {
        let selected_active=self.active.values().any(|selection|selection.session_id==id);
        let s=self.sessions.get_mut(&id).ok_or("unknown session")?;
        if !s.symbols.iter().any(|v|v==symbol) {return Err("unsubscribed symbol".into());}
        if matches!(s.tier,Tier::Quarantined) { return Ok(false) }
        if let Some(old_mono)=s.seen.get(symbol) {
            if recv_mono<*old_mono {return Err("monotonic clock moved backward".into());}
        }
        let age=recv_utc.checked_sub(event_ms);
        let seq_ok=match (s.last_accepted_seq.get(symbol).copied(), sequence) {
            (Some(old),Some(new))=>new>old,
            (Some(_),None)=>false,
            _=>true
        };
        let event_ok=s.last_accepted_event_ms.get(symbol).map_or(true,|old|if sequence.is_some(){event_ms>=*old}else{event_ms>*old});
        let ok=valid_full_book && matches!(age,Some(0..=MAX_AGE_MS)) && seq_ok && event_ok;
        s.seen.insert(symbol.to_string(),recv_mono);
        s.samples.push_back((recv_mono,ok));
        while s.samples.len()>MAX_SAMPLES ||
            s.samples.front().map(|(t,_)|recv_mono.saturating_sub(*t)>60_000).unwrap_or(false) {
            s.samples.pop_front();
        }
        if ok {
            let old_count=s.consecutive_good.get(symbol).copied().unwrap_or(0);
            if old_count==0 {s.first_good_mono.insert(symbol.to_string(),recv_mono);}
            s.consecutive_good.insert(symbol.to_string(),old_count.saturating_add(1));
            s.last_accepted_event_ms.insert(symbol.to_string(),event_ms);
            if let Some(id)=sequence {s.last_accepted_seq.insert(symbol.to_string(),id);}
            s.quotes.insert(symbol.to_string(),Snapshot {event_ms,
                receive_mono_ms:recv_mono,sequence});
        } else {
            s.consecutive_good.insert(symbol.to_string(),0);
            s.first_good_mono.remove(symbol);
            // An invalid frame must immediately remove this symbol's snapshot.
            s.quotes.remove(symbol);
            let recent=s.recent_bad_by_symbol.entry(symbol.to_string()).or_default();
            recent.push_back(recv_mono);
            while recent.len()>64 || recent.front().is_some_and(|t|
                recv_mono.saturating_sub(*t)>10_000){recent.pop_front();}
        }
        // One repeatedly stale symbol is not evidence of a failed whole
        // connection. Require >=3 recent bad frames on at least half the
        // group (and at least two symbols) without a fresh replacement.
        // The corroborated watchdog separately requires two fresh peers.
        let needed=((s.symbols.len()+1)/2).max(2);
        let degraded=s.recent_bad_by_symbol.iter().filter(|(sym,times)|
            times.iter().filter(|t|recv_mono>=**t && recv_mono-**t<=10_000).count()>=3 &&
            !s.quotes.get(*sym).is_some_and(|q|fresh(q,recv_utc,recv_mono))).count();
        let cohort_bad=s.symbols.len()>=2 && degraded>=needed;
        if s.samples.len()>=20 && s.recent_bad_fraction()>0.10 {
            if s.tier!=Tier::Active || cohort_bad {s.tier=Tier::Suspect;}
        } else if s.tier==Tier::Suspect && s.recent_bad_fraction()<=0.02 {
            s.tier=Tier::Candidate;
        }
        if s.tier==Tier::Candidate && s.is_ready(&s.symbols,recv_utc,recv_mono) {
            // A selected socket suspended without quorum can recover only
            // after full, multi-sample fresh requalification on that same
            // transport. No new session or stale prior book is invented.
            s.tier=if selected_active {Tier::Active} else {Tier::Hot};
        }
        Ok(ok)
    }

    pub fn update_tcp(&mut self, id:u64, rtt_ms:f64, ooo_ratio:f64) -> Result<(),String> {
        if !rtt_ms.is_finite() || !(0.0..=10_000.0).contains(&rtt_ms) ||
            !ooo_ratio.is_finite() || !(0.0..=1.0).contains(&ooo_ratio) {
            return Err("invalid network metrics".into());
        }
        let s=self.sessions.get_mut(&id).ok_or("unknown session")?;
        s.rtt_ms=Some(rtt_ms);
        s.ooo_ratio=Some(ooo_ratio);
        Ok(())
    }

    pub fn promote(&mut self, group:&str, id:u64, utc:i64, mono:u64)->Result<u64,String>{
        let symbols=self.group_symbols.get(group).ok_or("unknown group")?.clone();
        let s=self.sessions.get(&id).ok_or("unknown session")?;
        if s.symbols != symbols || !s.is_ready(&symbols,utc,mono) {
            return Err("reserve not ready for full group".into());
        }
        for symbol in &symbols {
            let quote=s.quotes.get(symbol).ok_or("missing valid snapshot")?;
            if self.last_published_seq.get(symbol).zip(quote.sequence).is_some_and(|(old,new)|new<*old){return Err("backup older than published update ID".into());}
            if self.last_published.get(symbol).is_some_and(|past|quote.event_ms < *past){
                return Err("backup older than last published event".into());
            }
        }
        if let Some(prev)=self.active.get(group) {
            if prev.session_id==id {return Ok(prev.epoch)}
            if let Some(old)=self.sessions.get_mut(&prev.session_id){
                if old.tier==Tier::Active {old.tier=Tier::Hot;}
            }
        }
        self.epoch=self.epoch.checked_add(1).ok_or("epoch overflow")?;
        self.active.insert(group.to_string(),Selection{session_id:id,epoch:self.epoch});
        self.sessions.get_mut(&id).expect("verified above").tier=Tier::Active;
        Ok(self.epoch)
    }

    /// Fail-closed publication: only active source with a fresh, validated,
    /// monotonically newer exchange event. The caller owns DB1 -> DB2 -> ACK.
    pub fn publish(&mut self, group:&str, symbol:&str, utc:i64, mono:u64)->Option<Publish>{
        let selected=self.active.get(group)?.clone();
        let s=self.sessions.get(&selected.session_id)?;
        if s.tier!=Tier::Active || !s.symbols.iter().any(|v|v==symbol) {return None;}
        let q=s.quotes.get(symbol)?;
        if !fresh(q,utc,mono) {return None;}
        if self.last_published.get(symbol).is_some_and(|old|q.event_ms<*old){return None;}
        if let Some(seq)=q.sequence{
            if self.last_published_seq.get(symbol).is_some_and(|old|seq<=*old){return None;}
            self.last_published_seq.insert(symbol.into(),seq);
        }else if self.last_published.get(symbol).is_some_and(|old|q.event_ms<=*old){return None;}
        let event_ms=q.event_ms;
        self.last_published.insert(symbol.to_string(),event_ms);
        Some(Publish{symbol:symbol.into(),event_ms,session_id:s.id,epoch:selected.epoch})
    }

    /// Stop all publication for a failed session. Never auto-use unqualified reserve.
    pub fn invalidate(&mut self, id:u64)->Result<(),String> {
        let s=self.sessions.get_mut(&id).ok_or("unknown session")?;
        s.tier=Tier::Quarantined;
        s.quotes.clear();
        for selection in self.active.values_mut() {
            if selection.session_id==id {selection.session_id=0;}
        }
        Ok(())
    }

    pub fn record_quality(&mut self, id:u64, utc:i64)->Result<(),String>{
        let s=self.sessions.get(&id).ok_or("unknown session")?;
        self.history.remember(PeerRecord {
            peer:s.peer.clone(),updated_utc_ms:utc,tested_frames:s.samples.len() as u64,
            stale_frames:s.samples.iter().filter(|(_,ok)|!ok).count() as u64,
            median_rtt_ms:s.rtt_ms,ooo_ratio:s.ooo_ratio
        });
        Ok(())
    }

    fn promotable(&self, s:&Session, symbols:&[String], utc:i64, mono:u64)->bool {
        s.symbols==symbols && s.is_ready(symbols,utc,mono) &&
        symbols.iter().all(|symbol|s.quotes.get(symbol).is_some_and(|q|
            self.last_published.get(symbol).map_or(true,|past|q.event_ms>=*past)&&
            self.last_published_seq.get(symbol).zip(q.sequence).map_or(true,|(old,new)|new>=*old)))
    }

    pub fn suggest_ready_reserve(&self, group:&str, utc:i64, mono:u64)->Option<u64>{
        let sym=self.group_symbols.get(group)?;
        let current=self.active.get(group).map(|x|x.session_id);
        self.sessions.values().filter(|s|current!=Some(s.id) &&
            self.promotable(s,sym,utc,mono))
            .min_by(|a,b| {
                a.recent_bad_fraction().total_cmp(&b.recent_bad_fraction())
                    .then_with(||a.rtt_ms.unwrap_or(10000.0)
                        .total_cmp(&b.rtt_ms.unwrap_or(10000.0)))
                    .then_with(||a.id.cmp(&b.id))
            }).map(|s|s.id)
    }
}

/// R1198, fail-closed stale-with-open-socket monitor.
/// A stale required symbol suspends the entire selected group and only a
/// fully qualified matching hot reserve can take over. Run on periodic tick,
/// independent of read arrivals; no extra Binance network traffic.
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum WatchdogOutcome {
    NoActive,
    Healthy,
    Switched { old_id:u64, new_id:u64, new_epoch:u64 },
    Blocked { old_id:u64 },
    /// R1199: active source untrustworthy but no independent healthy quorum;
    /// per-symbol 800ms publisher freshness still denies stale data.
    WaitingForEvidence { old_id:u64, silent_ms:u64, qualified_backups:usize },
}
impl Manager {
    pub fn watchdog(&mut self, group:&str, now_utc:i64, now_mono:u64)
        -> Result<WatchdogOutcome,String>
    {
        if !self.group_symbols.contains_key(group) {return Err("unknown group".into());}
        let Some(active)=self.active.get(group).cloned() else {
            return Ok(WatchdogOutcome::NoActive);
        };
        if active.session_id==0 {return Ok(WatchdogOutcome::NoActive);}
        // Separate symbol eligibility from connection health. A quiet symbol
        // is not evidence of a failed entire TCP socket. Per-symbol publisher
        // still refuses old E stamps via fresh(q), but group failover needs
        // a SILENT entire session or sustained actual late frame evidence.
        // 1500ms is a provisional pilot-only detection threshold for the
        // actively updating 8-symbol Binance futures cohort.
        let good=self.sessions.get(&active.session_id)
            .map(|s| {
                let last=s.seen.values().copied().max();
                s.tier==Tier::Active && last.is_some_and(|t|
                    now_mono>=t && now_mono.saturating_sub(t)<=1500)
            }).unwrap_or(false);
        if good{return Ok(WatchdogOutcome::Healthy)}
        let candidate=self.suggest_ready_reserve(group,now_utc,now_mono);
        // Quarantine first: a late/invalid active frame must never race
        // with replacement through the publisher.
        self.invalidate(active.session_id)?;
        if let Some(id)=candidate {
            if let Ok(epoch)=self.promote(group,id,now_utc,now_mono) {
                return Ok(WatchdogOutcome::Switched{
                    old_id:active.session_id,new_id:id,new_epoch:epoch});
            }
        }
        Ok(WatchdogOutcome::Blocked{old_id:active.session_id})
    }
}

/// R1199 experimental no-extra-network-policy: a silent active session is
/// not automatically a bad *route*. Demand corroboration from two distinct
/// live qualified routes; never blacklist solely on a quiet instrument.
impl Manager {
    pub fn watchdog_correlated_r1199(&mut self, group:&str, now_utc:i64, now_mono:u64)
        -> Result<WatchdogOutcome,String>
    {
        // This provisional threshold intentionally exceeds the old 1500ms,
        // which prematurely switched during R1198 run A.
        const SILENCE_MS:u64=2500;
        const HOT_PEER_SEEN_MS:u64=500;
        if !self.group_symbols.contains_key(group) {return Err("unknown group".into());}
        let Some(selected)=self.active.get(group).cloned() else {return Ok(WatchdogOutcome::NoActive);};
        if selected.session_id==0 {return Ok(WatchdogOutcome::NoActive);}
        let active=self.sessions.get(&selected.session_id).ok_or("active session missing")?;
        let last=active.seen.values().copied().max();
        let silent_ms=last.map(|t|now_mono.saturating_sub(t)).unwrap_or(u64::MAX);
        if active.tier==Tier::Active && silent_ms<SILENCE_MS {
            return Ok(WatchdogOutcome::Healthy);
        }
        let symbols=self.group_symbols.get(group).expect("known group");
        let eligible:Vec<u64>=self.sessions.values()
            .filter(|s|s.id!=selected.session_id && s.symbols==*symbols &&
                self.promotable(s,symbols,now_utc,now_mono) &&
                s.seen.values().copied().max().is_some_and(|t|
                    now_mono>=t && now_mono-t<=HOT_PEER_SEEN_MS))
            .map(|s|s.id).collect();
        // Use distinct remote peers as evidence, not multiple labels of one route.
        let distinct_peers=eligible.iter().map(|id|self.sessions.get(id).map(|s|s.peer.as_str()).unwrap_or(""))
            .collect::<std::collections::HashSet<_>>().len();
        if distinct_peers<2 {
            return Ok(WatchdogOutcome::WaitingForEvidence {
                old_id:selected.session_id,silent_ms,qualified_backups:distinct_peers
            });
        }
        let Some(backup)=self.suggest_ready_reserve(group,now_utc,now_mono) else {
            return Ok(WatchdogOutcome::WaitingForEvidence {
                old_id:selected.session_id,silent_ms,qualified_backups:distinct_peers
            });
        };
        self.invalidate(selected.session_id)?;
        match self.promote(group,backup,now_utc,now_mono) {
            Ok(epoch)=>Ok(WatchdogOutcome::Switched {
                old_id:selected.session_id,new_id:backup,new_epoch:epoch
            }),
            Err(_)=>Ok(WatchdogOutcome::Blocked {old_id:selected.session_id})
        }
    }
}
