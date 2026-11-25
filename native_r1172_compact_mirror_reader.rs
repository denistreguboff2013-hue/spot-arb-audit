//! R1172 read-only Rust validator of PRIVATE isolated compact-PAPER SQLite lab.
//! Does not write source, does not connect to exchanges, NEVER authorizes REAL.
use std::{ffi::{CString,c_char,c_int,c_void},path::Path,fs,os::unix::fs::MetadataExt};
use serde::Deserialize;
use openssl::sha::sha256;
const LAB:&str="/mnt/spotarb_disk2/r1172_compact_r1162_mirror_integrated_20261028";
const SQLITE_OK:c_int=0;
const SQLITE_ROW:c_int=100;
const SQLITE_DONE:c_int=101;
const READONLY_NOFOLLOW:c_int=1|0x10000|0x01000000;
#[link(name="libsqlite3.so.0",kind="dylib",modifiers="+verbatim")]
extern "C"{
 fn sqlite3_open_v2(f:*const c_char,db:*mut *mut c_void,flags:c_int,v:*const c_char)->c_int;
 fn sqlite3_close(db:*mut c_void)->c_int;
 fn sqlite3_prepare_v2(db:*mut c_void,q:*const c_char,n:c_int,
   stmt:*mut *mut c_void,tail:*mut *const c_char)->c_int;
 fn sqlite3_step(stmt:*mut c_void)->c_int;
 fn sqlite3_finalize(stmt:*mut c_void)->c_int;
 fn sqlite3_column_count(stmt:*mut c_void)->c_int;
 fn sqlite3_column_text(stmt:*mut c_void,i:c_int)->*const u8;
 fn sqlite3_column_bytes(stmt:*mut c_void,i:c_int)->c_int;
}
struct Db(*mut c_void);
impl Drop for Db{fn drop(&mut self){unsafe{sqlite3_close(self.0);}}}
struct Stmt(*mut c_void);
impl Drop for Stmt{fn drop(&mut self){unsafe{sqlite3_finalize(self.0);}}}
fn open(root:&Path,filename:&str)->Result<Db,String>{
 if root!=Path::new(LAB){return Err("noncanonical_lab_path".into())}
 let dir=fs::symlink_metadata(root).map_err(|_|"missing_lab")?;
 if !dir.is_dir()||dir.file_type().is_symlink()||dir.mode()&0o077!=0{
  return Err("unsafe_private_lab".into())
 }
 let path=root.join(filename);
 let m=fs::symlink_metadata(&path).map_err(|_|"missing_sqlite_file")?;
 if !m.is_file()||m.file_type().is_symlink()||m.nlink()!=1{
  return Err("unsafe_sqlite_file".into())
 }
 let name=CString::new(path.to_string_lossy().as_bytes()).map_err(|_|"path_nul")?;
 let mut raw=std::ptr::null_mut();
 let rc=unsafe{sqlite3_open_v2(name.as_ptr(),&mut raw,
   READONLY_NOFOLLOW,std::ptr::null())};
 if rc!=SQLITE_OK||raw.is_null(){
  if !raw.is_null(){unsafe{sqlite3_close(raw);}}
  return Err("sqlite_readonly_open_failed".into())
 }
 Ok(Db(raw))
}
fn rows(db:&Db,sql:&str)->Result<Vec<Vec<String>>,String>{
 let query=CString::new(sql).map_err(|_|"bad_query")?;
 let mut raw=std::ptr::null_mut();
 if unsafe{sqlite3_prepare_v2(db.0,query.as_ptr(),-1,
    &mut raw,std::ptr::null_mut())}!=SQLITE_OK||raw.is_null(){
  return Err("sqlite_prepare_failed".into())
 }
 let st=Stmt(raw);let mut result=Vec::new();
 loop{
  let rc=unsafe{sqlite3_step(st.0)};
  if rc==SQLITE_DONE{break}
  if rc!=SQLITE_ROW{return Err("sqlite_query_failed".into())}
  let n=unsafe{sqlite3_column_count(st.0)};
  let mut r=Vec::with_capacity(n as usize);
  for i in 0..n{
   let ptr=unsafe{sqlite3_column_text(st.0,i)};
   let len=unsafe{sqlite3_column_bytes(st.0,i)};
   if ptr.is_null()||!(0..=65536).contains(&len){
    return Err("null_or_oversize_field".into())
   }
   let b=unsafe{std::slice::from_raw_parts(ptr,len as usize)};
   r.push(String::from_utf8(b.to_vec()).map_err(|_|"bad_utf8")?);
  }
  result.push(r);
  if result.len()>10000{return Err("sqlite_rows_unbounded".into())}
 }
 Ok(result)
}
fn hex(b:&[u8])->String{
 const ABC:&[u8;16]=b"0123456789abcdef";
 let mut s=String::with_capacity(b.len()*2);
 for &x in b{s.push(ABC[(x>>4) as usize] as char);s.push(ABC[(x&15) as usize] as char)}
 s
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactBook{
 venue:String,base:String,epoch:u64,sequence:Option<i64>,
 exchange_ts_ms:i64,received_ns:u64,received_wall_ms:i64,changed_ns:u64,
 bids:Vec<(f64,f64)>,asks:Vec<(f64,f64)>
}
fn verify_book(v:&str,b:&str,body:&str,hash:&str)->Result<(),String>{
 if hex(&sha256(body.as_bytes()))!=hash{return Err("book_checksum_mismatch".into())}
 let book:CompactBook=serde_json::from_str(body).map_err(|_|"bad_book_json")?;
 if book.venue!=v||book.base!=b||book.epoch==0||book.exchange_ts_ms<=0||
  book.received_ns==0||book.changed_ns>book.received_ns||
  book.bids.is_empty()||book.asks.is_empty()||book.bids.len()>20||book.asks.len()>20||
  !book.bids.iter().chain(&book.asks).all(|(p,q)|p.is_finite()&&*p>0.&&q.is_finite()&&*q>0.){
   return Err("invalid_book_fields".into())
 }
 let _=book.sequence;
 let _=book.received_wall_ms;
 Ok(())
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct CompactProof{
 pub source_batches:u64,pub source_frames:u64,
 pub current_books:usize,pub minute_rows:usize,
 pub db2_cursor_equal:bool,pub may_trade_real:bool,pub may_restore_quotes_as_fresh:bool,
}
pub fn check_r1172_lab()->Result<CompactProof,String>{
 let root=Path::new(LAB);
 let a=open(root,"db1.sqlite")?;let b=open(root,"db2.sqlite")?;
 let control_sql="SELECT seq,frames,chain FROM control WHERE id=1";
 let ah=rows(&a,control_sql)?;let bh=rows(&b,control_sql)?;
 if ah.len()!=1||ah!=bh{return Err("source_mirror_cursors_differ".into())}
 let seq=ah[0][0].parse::<u64>().map_err(|_|"bad_source_seq")?;
 let frames=ah[0][1].parse::<u64>().map_err(|_|"bad_source_frames")?;
 if seq!=2289||frames!=59560||ah[0][2].len()!=64{
  return Err("unexpected_R1162_source_identity".into())
 }
 let book_sql="SELECT venue,base,book,book_hash FROM latest ORDER BY venue,base";
 let ab=rows(&a,book_sql)?;let bb=rows(&b,book_sql)?;
 if ab!=bb||ab.len()!=466{return Err("source_mirror_books_differ".into())}
 for book in &ab{verify_book(&book[0],&book[1],&book[2],&book[3])?}
 let summary=[
  "SELECT venue,base,minute,count FROM minutes ORDER BY venue,base,minute",
  "SELECT venue,base,hour,count FROM hours ORDER BY venue,base,hour",
  "SELECT venue,base,day,count FROM days ORDER BY venue,base,day",
  "SELECT latest_minute,archived_through,expired_count FROM retention WHERE id=1"
 ];
 let mut summary_n=0u64;let mut minute_rows=0;
 for (i,sql) in summary.iter().enumerate(){
  let left=rows(&a,sql)?;let right=rows(&b,sql)?;
  if left!=right{return Err("source_mirror_summary_divergence".into())}
  if i==0{minute_rows=left.len()}
  if i<=2{
   for row in left{summary_n+=row[3].parse::<u64>().map_err(|_|"bad_count")?;}
  }else if left.len()==1{
   summary_n+=left[0][2].parse::<u64>().map_err(|_|"bad_expiration_counter")?;
  }else{return Err("missing_retention_meta".into())}
 }
 if summary_n!=frames||minute_rows!=1389{
  return Err("source_to_rollup_count_mismatch".into())
 }
 if !rows(&a,"SELECT seq FROM pending")?.is_empty()||
    !rows(&b,"SELECT seq FROM pending")?.is_empty(){
   return Err("unacknowledged_source_batch".into())
 }
 Ok(CompactProof{source_batches:seq,source_frames:frames,current_books:ab.len(),
   minute_rows,db2_cursor_equal:true,may_trade_real:false,
   may_restore_quotes_as_fresh:false})
}
#[cfg(test)]
mod tests{
 #[test]fn r1172_native_reader_confirms_real_sqlite_pair(){
  let v=super::check_r1172_lab().unwrap();
  assert_eq!(v.source_batches,2289);
  assert_eq!(v.source_frames,59560);
  assert_eq!(v.current_books,466);
  assert!(!v.may_trade_real&&!v.may_restore_quotes_as_fresh);
 }
}
