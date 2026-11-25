// R158: private stable snapshot for read-only modeled PAPER audit.
fn snapshot_paper_source_r158(path:&Path)
 ->Result<(Vec<u8>,crate::native_virtual_four_leg_provenance_review::AuditState),PaperAdapterErrorR156>{
 use std::io::{Read,Write};
 use std::os::unix::fs::{MetadataExt,OpenOptionsExt,DirBuilderExt};
 use std::sync::atomic::{AtomicU64,Ordering};
 static SERIAL:AtomicU64=AtomicU64::new(0);
 let pre=fs::symlink_metadata(path).map_err(|_|PaperAdapterErrorR156::Path)?;
 if !pre.file_type().is_file()||pre.nlink()!=1||pre.len()==0||pre.len()>8*1024*1024{
  return Err(PaperAdapterErrorR156::Path)
 }
 let fd=fs::OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(path)
  .map_err(|_|PaperAdapterErrorR156::Path)?;
 let meta=fd.metadata().map_err(|_|PaperAdapterErrorR156::Path)?;
 if meta.dev()!=pre.dev()||meta.ino()!=pre.ino()||meta.len()!=pre.len()
  ||meta.mtime()!=pre.mtime()||meta.mtime_nsec()!=pre.mtime_nsec(){
  return Err(PaperAdapterErrorR156::Path)
 }
 let mut buf=Vec::new();
 fd.take(8*1024*1024+1).read_to_end(&mut buf)
  .map_err(|_|PaperAdapterErrorR156::Path)?;
 let post=fs::symlink_metadata(path).map_err(|_|PaperAdapterErrorR156::Path)?;
 if buf.len() as u64!=pre.len()||post.dev()!=pre.dev()||post.ino()!=pre.ino()
  ||post.len()!=pre.len()||post.mtime()!=pre.mtime()
  ||post.mtime_nsec()!=pre.mtime_nsec(){
  return Err(PaperAdapterErrorR156::Journal)
 }
 let root=std::env::temp_dir().join(format!("astra_r158_snapshot_{}_{}",std::process::id(),SERIAL.fetch_add(1,Ordering::Relaxed)));
 fs::DirBuilder::new().mode(0o700).create(&root).map_err(|_|PaperAdapterErrorR156::Path)?;
 struct Scratch(std::path::PathBuf);
 impl Drop for Scratch{
  fn drop(&mut self){
   let _=fs::remove_file(self.0.join("modeled_four_leg_journal.jsonl"));
   let _=fs::remove_dir(&self.0);
  }
 }
 let _scratch=Scratch(root.clone());
 let snap=root.join("modeled_four_leg_journal.jsonl");
 let mut f=fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&snap)
  .map_err(|_|PaperAdapterErrorR156::Path)?;
 f.write_all(&buf).map_err(|_|PaperAdapterErrorR156::Path)?;
 drop(f);
 let tip=crate::native_virtual_four_leg_provenance_review::inspect_model_journal(&snap)
  .map_err(|_|PaperAdapterErrorR156::Journal)?;
 Ok((buf,tip))
}
