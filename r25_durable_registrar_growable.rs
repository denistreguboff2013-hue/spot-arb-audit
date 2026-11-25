//! R25 isolated durable registrar with growable preallocation. No decision or DB materialization.
use openssl::sha::sha256;
use serde::{Deserialize,Serialize};
use serde_json::{json,Value};
use std::{collections::HashMap,ffi::CString,fs::{self,File,OpenOptions},
    io::{self,Read,Seek,SeekFrom,Write},os::unix::{fs::OpenOptionsExt,io::AsRawFd},
    os::unix::fs::MetadataExt,
    path::{Path,PathBuf},thread,time::Duration};

const MAGIC:&[u8;8]=b"SPREG001";
const END:&[u8;8]=b"ENDREG01";
const FLOOR:&[u8;8]=b"FLOOR001";
const HEADER:usize=56;
const TRAILER:usize=40;
const MAX_LINE:usize=2_100_000;
const MAX_PAYLOAD:usize=2_097_152;
const MAX_EVENTS:usize=65_536;
const MAX_JOURNAL:u64=512*1024*1024;
const PREALLOC_CHUNK:u64=16*1024*1024;
const RESERVE:u64=7_868_000_000;
type Result<T>=std::result::Result<T,String>;
fn stamp()->u64{
    let mut t=libc::timespec{tv_sec:0,tv_nsec:0};
    unsafe{libc::clock_gettime(libc::CLOCK_MONOTONIC,&mut t);}
    t.tv_sec as u64*1_000_000_000+t.tv_nsec as u64
}
fn hex(bytes:&[u8])->String{bytes.iter().map(|b|format!("{b:02x}")).collect()}
fn ioerr(e:io::Error)->String{format!("IO_FAILURE: {e}")}
#[derive(Clone,Deserialize,Serialize)]
struct Event{
    seq:u64,event_id:String,content_hash:String,record_json:String,
    event_created_ns:u64,admitted_ns:u64,
}
#[derive(Deserialize)]
struct Request{op:String,#[serde(default)] events:Vec<Event>}
#[derive(Clone)]
struct Floor{seq:u64,offset:u64,chain:[u8;32]}
struct Registrar{
    case:PathBuf,journal_parent:PathBuf,file:File,floor_file:File,events:Vec<Event>,ids:HashMap<String,u64>,chain:[u8;32],
    offset:u64,preallocated:u64,journal_writes:u64,floor_writes:u64,sync_calls:u64,journal_bytes:u64,
    tail_removed:u64,recovery_ns:u64,failpoint:String,
}
fn read_floor(file:&mut File)->Result<Floor>{
    file.seek(SeekFrom::Start(0)).map_err(ioerr)?;
    let mut b=Vec::new();file.read_to_end(&mut b).map_err(ioerr)?;
    if b.len()!=88||&b[..8]!=FLOOR||sha256(&b[..56])!=b[56..88]{
        return Err("corrupt durable floor; fail closed".into())
    }
    Ok(Floor{seq:u64::from_le_bytes(b[8..16].try_into().unwrap()),
        offset:u64::from_le_bytes(b[16..24].try_into().unwrap()),chain:b[24..56].try_into().unwrap()})
}
fn save_floor(file:&mut File,f:&Floor)->Result<()> {
    let mut b=FLOOR.to_vec();b.extend(f.seq.to_le_bytes());b.extend(f.offset.to_le_bytes());
    b.extend(f.chain);let digest=sha256(&b);b.extend(digest);
    if file.metadata().map_err(ioerr)?.len()!=88{return Err("IO_FAILURE: floor size changed".into())}
    file.seek(SeekFrom::Start(0)).map_err(ioerr)?;
    file.write_all(&b).map_err(ioerr)?;file.sync_all().map_err(ioerr)
}
fn open_floor(path:&Path,journal_size:u64)->Result<(File,Floor)>{
    let initial=Floor{seq:0,offset:0,chain:[0;32]};
    let mut created=false;
    let mut file=match OpenOptions::new().read(true).write(true)
        .custom_flags(libc::O_NOFOLLOW).open(&path){
        Ok(file)=>file,
        Err(e) if e.kind()==io::ErrorKind::NotFound && journal_size==0=>{
            created=true;
            OpenOptions::new().read(true).write(true).create_new(true)
                .custom_flags(libc::O_NOFOLLOW).open(&path).map_err(ioerr)?
        },
        Err(e) if e.kind()==io::ErrorKind::NotFound=>
            return Err("nonempty journal without durable floor; fail closed".into()),
        Err(e)=>return Err(ioerr(e)),
    };
    let fd_meta=file.metadata().map_err(ioerr)?;
    let path_meta=fs::symlink_metadata(&path).map_err(ioerr)?;
    if !fd_meta.file_type().is_file()||!path_meta.file_type().is_file()
        ||fd_meta.dev()!=path_meta.dev()||fd_meta.ino()!=path_meta.ino()
        ||(!created && fd_meta.len()!=88){
        return Err("corrupt durable floor type/inode/size; fail closed".into())
    }
    let floor=if created{
        let mut b=FLOOR.to_vec();b.extend(initial.seq.to_le_bytes());b.extend(initial.offset.to_le_bytes());
        b.extend(initial.chain);b.extend(sha256(&b));
        file.write_all(&b).map_err(ioerr)?;file.sync_all().map_err(ioerr)?;
        File::open(path.parent().ok_or("floor parent missing")?).map_err(ioerr)?.sync_all().map_err(ioerr)?;
        initial
    }else{read_floor(&mut file)?};
    Ok((file,floor))
}
fn prealloc_target(required_end:u64)->Result<u64>{
    if required_end>MAX_JOURNAL{return Err("journal byte bound exceeded".into())}
    let required=required_end.max(1);
    let chunks=required.checked_add(PREALLOC_CHUNK-1).ok_or("journal preallocation overflow")?/PREALLOC_CHUNK;
    Ok((chunks*PREALLOC_CHUNK).min(MAX_JOURNAL))
}
fn ensure_preallocated(file:&File,case:&Path,required_end:u64)->Result<u64>{
    let target=prealloc_target(required_end)?;
    let meta=file.metadata().map_err(ioerr)?;
    if meta.len()>MAX_JOURNAL{return Err("journal byte bound exceeded".into())}
    let allocated=meta.blocks()*512;
    if allocated>=target{return Ok(target)}
    let path=CString::new(case.as_os_str().as_encoded_bytes()).map_err(|e|e.to_string())?;
    let mut stat=unsafe{std::mem::zeroed::<libc::statvfs>()};
    if unsafe{libc::statvfs(path.as_ptr(),&mut stat)}!=0{return Err("preallocation statvfs failed".into())}
    let additional=target.saturating_sub(allocated);
    if stat.f_bavail as u64*stat.f_frsize as u64<=RESERVE+additional+16*1024*1024{
        return Err("preallocation disk reserve".into())
    }
    if unsafe{libc::fallocate(file.as_raw_fd(),libc::FALLOC_FL_KEEP_SIZE,0,target as libc::off_t)}!=0{
        return Err(ioerr(io::Error::last_os_error()))
    }
    let meta=file.metadata().map_err(ioerr)?;
    if meta.len()>MAX_JOURNAL||meta.blocks()*512<target{
        return Err("preallocated journal extent incomplete; fail closed".into())
    }
    Ok(target)
}
fn validate(e:&Event)->Result<()>{
    if e.seq==0||e.event_id.is_empty()||e.event_id.len()>100||e.record_json.len()>65_536
        ||e.content_hash!=hex(&sha256(e.record_json.as_bytes())){
        return Err("event identity/content hash/size mismatch".into())
    }
    let r:Value=serde_json::from_str(&e.record_json).map_err(|e|e.to_string())?;
    if r["event_id"]!=e.event_id||!r["payload_json"].is_string()
        ||!["SIGNAL_SEEN","DECISION_MADE"].contains(&r["name"].as_str().unwrap_or("")){
        return Err("canonical envelope identity/type mismatch".into())
    }
    Ok(())
}
fn index(events:&mut Vec<Event>,ids:&mut HashMap<String,u64>,batch:Vec<Event>)->Result<()>{
    if events.len()+batch.len()>MAX_EVENTS{return Err("bounded journal index full".into())}
    for e in batch{
        validate(&e)?;
        if e.seq!=events.len() as u64+1||ids.contains_key(&e.event_id){
            return Err("noncontinuous or duplicate journal sequence/identity".into())
        }
        ids.insert(e.event_id.clone(),e.seq);events.push(e);
    }
    Ok(())
}
impl Registrar{
    fn open(case:PathBuf,failpoint:String)->Result<Self>{
        let start=stamp();let case=fs::canonicalize(case).map_err(ioerr)?;
        if !case.starts_with("/home/tmpadmin/native_rust_registrar_review_20261002"){
            return Err("explicit isolated registrar root required".into())
        }
        let journal_path=std::env::var_os("NATIVE_SPLIT_JOURNAL_PATH").map(PathBuf::from)
            .unwrap_or_else(||case.join("registrar.journal"));
        let floor_path=std::env::var_os("NATIVE_SPLIT_FLOOR_PATH").map(PathBuf::from)
            .unwrap_or_else(||case.join("commit_floor.bin"));
        for path in [&journal_path,&floor_path]{
            let text=path.to_string_lossy();
            if !text.starts_with("/mnt/spotarb_db_test/r22_") {
                return Err("R22 split storage must stay under /mnt/spotarb_db_test/r22_*".into())
            }
        }
        for path in [&journal_path,&floor_path]{
            let parent=path.parent().ok_or("storage parent missing")?;
            fs::create_dir_all(parent).map_err(ioerr)?;
        }
        if !journal_path.exists()&&floor_path.exists(){
            return Err("journal missing with durable floor; fail closed".into())
        }
        let mut file=OpenOptions::new().read(true).write(true).create(true)
            .open(&journal_path).map_err(ioerr)?;
        if unsafe{libc::flock(file.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0{
            return Err("single registrar journal lock held".into())
        }
        let size=file.metadata().map_err(ioerr)?.len();
        if size>MAX_JOURNAL{return Err("journal byte bound exceeded".into())}
        let preallocated=ensure_preallocated(&file,journal_path.parent().ok_or("journal parent missing")?,size)?;
        file.sync_all().map_err(ioerr)?;File::open(journal_path.parent().ok_or("journal parent missing")?).map_err(ioerr)?.sync_all().map_err(ioerr)?;
        let (mut floor_file,floor)=open_floor(&floor_path,size)?;
        if floor.offset>size{return Err("journal truncated below durable floor; fail closed".into())}
        let mut events=Vec::new();let mut ids=HashMap::new();let mut chain=[0;32];let mut offset=0u64;
        let mut floor_seen=floor.seq==0&&floor.offset==0&&floor.chain==[0;32];
        while offset<size{
            if size-offset<HEADER as u64{break}
            let mut header=[0;HEADER];file.read_exact(&mut header).map_err(ioerr)?;
            if &header[..8]!=MAGIC{return Err("corrupt frame header; fail closed".into())}
            let length=u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
            let first=u64::from_le_bytes(header[12..20].try_into().unwrap());
            let count=u32::from_le_bytes(header[20..24].try_into().unwrap()) as usize;
            if length>MAX_PAYLOAD||count==0||count>32||first!=events.len() as u64+1
                ||header[24..56]!=chain{return Err("corrupt frame identity/length/chain; fail closed".into())}
            let end=offset+(HEADER+length+TRAILER) as u64;
            if end>size{break}
            let mut payload=vec![0;length];file.read_exact(&mut payload).map_err(ioerr)?;
            let mut trailer=[0;TRAILER];file.read_exact(&mut trailer).map_err(ioerr)?;
            let mut signed=header.to_vec();signed.extend(&payload);let digest=sha256(&signed);
            if digest!=trailer[..32]||&trailer[32..]!=END{
                return Err("corrupt complete frame checksum; fail closed".into())
            }
            let batch:Vec<Event>=serde_json::from_slice(&payload).map_err(|e|e.to_string())?;
            if batch.len()!=count{return Err("frame count mismatch".into())}
            index(&mut events,&mut ids,batch)?;chain=digest;offset=end;
            if offset==floor.offset{
                if events.len() as u64!=floor.seq||chain!=floor.chain{return Err("durable floor prefix mismatch".into())}
                floor_seen=true;
            }
        }
        if !floor_seen||offset<floor.offset{return Err("durable floor absent from valid prefix; fail closed".into())}
        let tail_removed=size-offset;
        if tail_removed>0{file.set_len(offset).map_err(ioerr)?;}
        file.seek(SeekFrom::Start(offset)).map_err(ioerr)?;file.sync_all().map_err(ioerr)?;
        // Complete unacknowledged frames may be recovered, but are synced again before ready/ACK.
        // A clean restart must not rewrite an already authoritative in-place marker.
        if floor.seq!=events.len() as u64||floor.offset!=offset||floor.chain!=chain{
            save_floor(&mut floor_file,&Floor{seq:events.len() as u64,offset,chain})?;
        }
        let journal_parent=journal_path.parent().ok_or("journal parent missing")?.to_path_buf();
        Ok(Self{case,journal_parent,file,floor_file,events,ids,chain,offset,preallocated,journal_writes:0,floor_writes:0,sync_calls:0,
            journal_bytes:0,tail_removed,recovery_ns:stamp()-start,failpoint})
    }
    fn pressure(&self)->Result<()>{
        for root in [std::env::var_os("NATIVE_SPLIT_JOURNAL_PATH"),std::env::var_os("NATIVE_SPLIT_FLOOR_PATH")].into_iter().flatten(){
            let p=PathBuf::from(root);let parent=p.parent().ok_or("storage parent missing")?;
            let path=CString::new(parent.as_os_str().as_encoded_bytes()).map_err(|e|e.to_string())?;
            let mut stat=unsafe{std::mem::zeroed::<libc::statvfs>()};
            if unsafe{libc::statvfs(path.as_ptr(),&mut stat)}!=0{return Err("statvfs failed".into())}
            if stat.f_bavail as u64*stat.f_frsize as u64<=RESERVE+16*1024*1024{return Err("fixture disk reserve".into())}
        }
        Ok(())
    }
    fn checkpoint(&self,name:&str)->Result<()>{
        if self.failpoint=="hold_first_after_journal_sync"&&name=="after_journal_sync"&&self.events.is_empty(){
            fs::write(self.case.join("registrar_phase.json"),json!({"phase":name,"pid":std::process::id(),"at_ns":stamp()}).to_string()).map_err(ioerr)?;
            while !self.case.join("release_registrar").exists(){thread::sleep(Duration::from_millis(2));}
            fs::remove_file(self.case.join("release_registrar")).map_err(ioerr)?;
        }
        if self.failpoint==name{
            fs::write(self.case.join("registrar_phase.json"),json!({"phase":name,"pid":std::process::id(),"at_ns":stamp()}).to_string()).map_err(ioerr)?;
            loop{thread::sleep(Duration::from_millis(2));}
        }
        Ok(())
    }
    fn write_bytes(&mut self,bytes:&[u8])->Result<()>{
        let mut done=0;
        while done<bytes.len(){
            match self.file.write(&bytes[done..]){
                Ok(0)=>return Err("IO_FAILURE: zero journal write".into()),
                Ok(n)=>{done+=n;self.journal_writes+=1;self.journal_bytes+=n as u64;},
                Err(e) if e.kind()==io::ErrorKind::Interrupted=>continue,
                Err(e)=>return Err(ioerr(e)),
            }
        }
        Ok(())
    }
    fn batch(&mut self,batch:Vec<Event>,received:u64)->Result<Value>{
        if batch.is_empty()||batch.len()>32{return Err("bounded batch size 1..32 required".into())}
        self.pressure()?;
        let before=self.events.len() as u64;
        let mut expected=before+1;let mut new=Vec::new();let mut pending_ids=HashMap::new();
        for (i,e) in batch.iter().enumerate(){
            validate(e)?;
            if i>0&&e.seq!=batch[i-1].seq+1{return Err("batch sequence gap".into())}
            if e.seq<=before{
                let old=&self.events[e.seq as usize-1];
                if e.event_id!=old.event_id||e.content_hash!=old.content_hash||e.record_json!=old.record_json{
                    return Err("conflicting replay; no ACK".into())
                }
            }else{
                if e.seq!=expected||self.ids.contains_key(&e.event_id)||pending_ids.contains_key(&e.event_id){
                    return Err("new sequence gap or identity conflict".into())
                }
                pending_ids.insert(e.event_id.clone(),e.seq);expected+=1;new.push(e.clone());
            }
        }
        if self.events.len()+new.len()>MAX_EVENTS{return Err("bounded journal index full".into())}
        let write_start=stamp();let mut journal_write_ns=0;let mut floor_ns=0;let mut append_ns=0;let mut journal_fsync_ns=0;
        if !new.is_empty(){
            let payload=serde_json::to_vec(&new).map_err(|e|e.to_string())?;
            if payload.len()>MAX_PAYLOAD{return Err("bounded batch bytes".into())}
            let mut bytes=MAGIC.to_vec();bytes.extend((payload.len() as u32).to_le_bytes());
            bytes.extend(new[0].seq.to_le_bytes());bytes.extend((new.len() as u32).to_le_bytes());
            bytes.extend(self.chain);bytes.extend(payload);let chain=sha256(&bytes);bytes.extend(chain);bytes.extend(END);
            let required_end=self.offset.checked_add(bytes.len() as u64).ok_or("journal byte bound exceeded")?;
            if required_end>MAX_JOURNAL{return Err("journal byte bound exceeded".into())}
            if required_end>self.preallocated{
                self.preallocated=ensure_preallocated(&self.file,&self.journal_parent,required_end)?;
            }
            self.checkpoint("before_write")?;
            if self.failpoint=="partial_write_error"{
                self.write_bytes(&bytes[..bytes.len()/2])?;self.file.sync_all().map_err(ioerr)?;
                return Err("IO_FAILURE: injected partial journal write error".into())
            }
            if self.failpoint=="half_write"{
                self.write_bytes(&bytes[..bytes.len()/2])?;self.file.sync_all().map_err(ioerr)?;
                self.checkpoint("half_write")?;
            }
            self.write_bytes(&bytes)?;let append_end=stamp();append_ns=append_end-write_start;
            self.checkpoint("before_journal_sync")?;
            if self.failpoint=="journal_sync_error"{return Err("IO_FAILURE: injected journal fsync error".into())}
            self.file.sync_all().map_err(ioerr)?;journal_fsync_ns=stamp()-append_end;self.sync_calls+=1;
            journal_write_ns=stamp()-write_start;
            self.checkpoint("after_journal_sync")?;
            let floor_start=stamp();
            let next_floor=Floor{seq:expected-1,offset:self.offset+bytes.len() as u64,chain};
            if self.failpoint=="floor_partial_write_error"{
                let mut b=FLOOR.to_vec();b.extend(next_floor.seq.to_le_bytes());
                self.floor_file.seek(SeekFrom::Start(0)).map_err(ioerr)?;
                self.floor_file.write_all(&b).map_err(ioerr)?;
                return Err("IO_FAILURE: injected partial floor write".into())
            }
            if self.failpoint=="floor_sync_error"{
                self.floor_file.seek(SeekFrom::Start(0)).map_err(ioerr)?;
                let mut b=FLOOR.to_vec();b.extend(next_floor.seq.to_le_bytes());
                b.extend(next_floor.offset.to_le_bytes());b.extend(next_floor.chain);
                b.extend(sha256(&b));self.floor_file.write_all(&b).map_err(ioerr)?;
                return Err("IO_FAILURE: injected floor fsync error".into())
            }
            save_floor(&mut self.floor_file,&next_floor)?;
            self.floor_writes+=1;self.sync_calls+=1;floor_ns=stamp()-floor_start;
            self.offset+=bytes.len() as u64;self.chain=chain;
            index(&mut self.events,&mut self.ids,new)?;
            self.checkpoint("after_durable_before_ack")?;
        }
        let durable=stamp();
        Ok(json!({"ok":true,"phase":"journal_durable","through_seq":self.events.len(),
            "batch_first":batch[0].seq,"batch_last":batch.last().unwrap().seq,"batch_count":batch.len(),
            "new_count":self.events.len() as u64-before,"registrar_receive_ns":received,
            "durable_ns":durable,"write_start_ns":write_start,"journal_sync_ns":journal_write_ns,
            "floor_sync_ns":floor_ns,"append_ns":append_ns,"journal_fsync_ns":journal_fsync_ns,"chain_hash":hex(&self.chain),"journal_offset":self.offset,
            "journal_write_syscalls":self.journal_writes,"floor_writes":self.floor_writes,
            "sync_calls":self.sync_calls,"journal_bytes_written":self.journal_bytes,
            "items":batch.iter().map(|e|json!({"seq":e.seq,"event_id":e.event_id,"content_hash":e.content_hash})).collect::<Vec<_>>()}))
    }
    fn status(&self)->Value{json!({"ok":true,"phase":"status","pid":std::process::id(),
        "through_seq":self.events.len(),"journal_offset":self.offset,"chain_hash":hex(&self.chain),
        "journal_write_syscalls":self.journal_writes,"floor_writes":self.floor_writes,
        "sync_calls":self.sync_calls,"journal_bytes_written":self.journal_bytes,
        "tail_removed_bytes":self.tail_removed,"recovery_ns":self.recovery_ns,
        "journal_preallocated_bytes":self.preallocated,"journal_byte_bound":MAX_JOURNAL,
        "queue_bound_events":128,"batch_bound":32,"journal_event_bound":MAX_EVENTS})}
}
fn reply(out:&mut impl Write,value:Value)->Result<()>{
    serde_json::to_writer(&mut *out,&value).map_err(|e|e.to_string())?;
    out.write_all(b"\n").map_err(ioerr)?;out.flush().map_err(ioerr)
}
// Read only bytes already present when choosing the next natural group.
struct Input{bytes:Vec<u8>,eof:bool}
impl Input{
    fn next(&mut self,blocking:bool)->Result<Option<Vec<u8>>>{
        loop{
            if let Some(end)=self.bytes.iter().position(|&b|b==b'\n'){
                if end+1>MAX_LINE{return Err("bounded request bytes".into())}
                return Ok(Some(self.bytes.drain(..=end).collect()))
            }
            if self.bytes.len()>MAX_LINE{return Err("bounded request bytes".into())}
            if self.eof{
                if self.bytes.is_empty(){return Ok(None)}
                return Err("bounded request bytes: unterminated line".into())
            }
            let mut p=libc::pollfd{fd:libc::STDIN_FILENO,events:libc::POLLIN,revents:0};
            let ready=unsafe{libc::poll(&mut p,1,if blocking{-1}else{0})};
            if ready==0{return Ok(None)}
            if ready<0{
                let e=io::Error::last_os_error();
                if e.kind()==io::ErrorKind::Interrupted{continue}
                return Err(ioerr(e))
            }
            let mut chunk=[0u8;65_536];
            let count=unsafe{libc::read(libc::STDIN_FILENO,chunk.as_mut_ptr().cast(),chunk.len())};
            if count==0{self.eof=true;continue}
            if count<0{
                let e=io::Error::last_os_error();
                if e.kind()==io::ErrorKind::Interrupted{continue}
                return Err(ioerr(e))
            }
            self.bytes.extend_from_slice(&chunk[..count as usize]);
        }
    }
}
fn run()->Result<()>{
    let args:Vec<String>=std::env::args().collect();
    if args.len()<2{return Err("usage: native_durable_registrar ISOLATED_CASE [FAILPOINT]".into())}
    let mut r=Registrar::open(PathBuf::from(&args[1]),args.get(2).cloned().unwrap_or_default())?;
    let mut input=Input{bytes:Vec::new(),eof:false};let stdout=io::stdout();let mut output=stdout.lock();
    let mut deferred:Option<Vec<u8>>=None;
    reply(&mut output,json!({"ready":true,"status":r.status()}))?;
    loop{
        let bytes=match deferred.take(){Some(b)=>b,None=>match input.next(true)?{
            Some(b)=>b,None=>break,
        }};
        let received=stamp();let req:Request=match serde_json::from_slice(&bytes){
            Ok(v)=>v,Err(e)=>{reply(&mut output,json!({"ok":false,"error":e.to_string()}))?;continue;}
        };
        if req.op=="batch"{
            let mut groups=vec![req.events];
            let mut count=groups[0].len();
            let mut size:usize=groups[0].iter().map(|e|e.record_json.len()+256).sum();
            while count<32{
                let raw=match input.next(false)?{Some(b)=>b,None=>break};
                let candidate:Request=match serde_json::from_slice(&raw){
                    Ok(v)=>v,Err(_)=>{deferred=Some(raw);break}
                };
                let previous=groups.last().and_then(|g|g.last());
                let next_size:usize=candidate.events.iter().map(|e|e.record_json.len()+256).sum();
                if candidate.op!="batch"||candidate.events.is_empty()
                    ||candidate.events.len()+count>32||size+next_size>MAX_PAYLOAD
                    ||previous.zip(candidate.events.first())
                        .map(|(a,b)|a.seq.checked_add(1)!=Some(b.seq)).unwrap_or(true){
                    deferred=Some(raw);break
                }
                count+=candidate.events.len();size+=next_size;groups.push(candidate.events);
            }
            let before=r.events.len() as u64;
            let combined=groups.iter().flat_map(|g|g.iter().cloned()).collect();
            let result=r.batch(combined,received);
            match result{
                Ok(row)=>{
                    // Advisory writer-free signal follows both physical syncs. It never
                    // acknowledges an identity or advances a publication frontier.
                    if std::env::var("NATIVE_COMBINED_GROUPING").ok().as_deref()==Some("1"){
                        reply(&mut output,json!({"writer_ready":true,"through_seq":r.events.len(),
                            "durable_ns":row["durable_ns"]}))?;
                    }
                    let physical_group_id=hex(&sha256(format!("{}:{}:{}",r.offset,received,r.events.len()).as_bytes()));
                    for (i,events) in groups.iter().enumerate(){
                        let mut ack=row.clone();
                        let first=events.first().map(|e|e.seq).unwrap_or(0);
                        let last=events.last().map(|e|e.seq).unwrap_or(0);
                        ack["batch_first"]=json!(first);ack["batch_last"]=json!(last);
                        ack["batch_count"]=json!(events.len());
                        ack["through_seq"]=json!(if row["new_count"]==0{r.events.len() as u64}else{last});
                        ack["new_count"]=json!(events.iter().filter(|e|e.seq>before).count());
                        ack["items"]=json!(events.iter().map(|e|json!({"seq":e.seq,"event_id":e.event_id,"content_hash":e.content_hash})).collect::<Vec<_>>());
                        ack["physical_group_id"]=json!(physical_group_id);
                        ack["natural_group_count"]=json!(groups.len());
                        ack["natural_group_events"]=json!(count);
                        ack["natural_group_index"]=json!(i);
                        if i>0{
                            ack["journal_sync_ns"]=json!(0);
                            ack["floor_sync_ns"]=json!(0);
                            ack["append_ns"]=json!(0);
                            ack["journal_fsync_ns"]=json!(0);
                        }
                        reply(&mut output,ack)?;
                    }
                },
                Err(error)=>{
                    for _ in &groups{
                        reply(&mut output,json!({"ok":false,"error":error,"through_seq":r.events.len()}))?;
                    }
                    if error.contains("reserve")||error.contains("journal bytes")||error.contains("IO_FAILURE")
                        ||error.contains("No space")||error.contains("Input/output"){return Err(error)}
                }
            }
            continue
        }
        let result:Result<Value>=match req.op.as_str(){
            "status"=>Ok(r.status()),
            "stop"=>{reply(&mut output,r.status())?;break;},
            _=>Err("unknown bounded command".into()),
        };
        match result{
            Ok(row)=>reply(&mut output,row)?,
            Err(error)=>reply(&mut output,json!({"ok":false,"error":error,"through_seq":r.events.len()}))?,
        }
    }
    Ok(())
}
fn main(){if let Err(error)=run(){eprintln!("{error}");std::process::exit(2);}}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preallocation_grows_in_fixed_chunks() {
        assert_eq!(prealloc_target(0).unwrap(), PREALLOC_CHUNK);
        assert_eq!(prealloc_target(1).unwrap(), PREALLOC_CHUNK);
        assert_eq!(prealloc_target(PREALLOC_CHUNK).unwrap(), PREALLOC_CHUNK);
        assert_eq!(prealloc_target(PREALLOC_CHUNK + 1).unwrap(), 2 * PREALLOC_CHUNK);
        assert_eq!(prealloc_target(2 * PREALLOC_CHUNK).unwrap(), 2 * PREALLOC_CHUNK);
    }

    #[test]
    fn preallocation_respects_hard_journal_bound() {
        assert_eq!(prealloc_target(MAX_JOURNAL).unwrap(), MAX_JOURNAL);
        assert!(prealloc_target(MAX_JOURNAL + 1).is_err());
    }
}
