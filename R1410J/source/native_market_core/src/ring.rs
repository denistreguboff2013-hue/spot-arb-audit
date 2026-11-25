//! Single producer, single consumer shared memory ring for fixed-size raw frames.
//! Capacity is explicit. Full returns Pressure and cannot overwrite a historical
//! frame. Publication is release/acquire; only one writer touches each counter.
use std::{mem, ptr, slice};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::CoreError;
const CAPACITY: usize = 128;
const FRAME_BYTES: usize = 4096;
#[repr(C)]
struct Header { head: AtomicU64, tail: AtomicU64, producer_tid: AtomicU64, consumer_tid: AtomicU64 }
#[repr(C)]
struct Slot { len: u32, bytes: [u8; FRAME_BYTES] }
pub struct SharedRing { pointer: *mut u8, total: usize }
unsafe impl Send for SharedRing {}
unsafe impl Sync for SharedRing {}
impl SharedRing {
    pub fn new() -> Result<Self, CoreError> {
        let total = mem::size_of::<Header>() + CAPACITY * mem::size_of::<Slot>();
        let p = unsafe { libc::mmap(ptr::null_mut(), total, libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_SHARED | libc::MAP_ANONYMOUS, -1, 0) };
        if p == libc::MAP_FAILED { return Err(CoreError::Invalid("mmap")); }
        unsafe { ptr::write(p.cast::<Header>(), Header {
            head: AtomicU64::new(0), tail: AtomicU64::new(0),
            producer_tid:AtomicU64::new(0),consumer_tid:AtomicU64::new(0)
        }); }
        Ok(Self {pointer:p.cast(),total})
    }
    fn header(&self)->&Header {unsafe {&*self.pointer.cast::<Header>()}}
    fn slot(&self, i:u64)->*mut Slot {
        unsafe {self.pointer.add(mem::size_of::<Header>() + i as usize % CAPACITY * mem::size_of::<Slot>()).cast()}
    }
    fn claim(slot:&AtomicU64)->Result<(),CoreError>{
        let tid=unsafe{libc::syscall(libc::SYS_gettid)} as u64;
        match slot.compare_exchange(0,tid,Ordering::AcqRel,Ordering::Acquire){
            Ok(_)=>Ok(()),Err(owner) if owner==tid=>Ok(()),
            Err(_)=>Err(CoreError::WrongOwner)
        }
    }
    pub fn capacity(&self)->usize {CAPACITY}
    pub fn depth(&self)->u64 {
        let tail=self.header().tail.load(Ordering::Acquire);
        let head=self.header().head.load(Ordering::Acquire);
        tail-head
    }
    /// Exactly one producer; a full ring fails closed.
    pub fn try_push(&self, bytes:&[u8])->Result<(),CoreError>{
        Self::claim(&self.header().producer_tid)?;
        if bytes.len()>FRAME_BYTES {return Err(CoreError::Invalid("frame slot bound"));}
        let tail=self.header().tail.load(Ordering::Relaxed);
        let head=self.header().head.load(Ordering::Acquire);
        if tail-head>=CAPACITY as u64{return Err(CoreError::HistoryPressure);}
        unsafe {
            let slot=&mut *self.slot(tail);
            slot.len=bytes.len() as u32;
            ptr::copy_nonoverlapping(bytes.as_ptr(),slot.bytes.as_mut_ptr(),bytes.len());
        }
        self.header().tail.store(tail+1,Ordering::Release);
        Ok(())
    }
    /// Exactly one consumer; returns None only when empty.
    pub fn try_pop(&self)->Option<Vec<u8>>{
        // A second consumer is a caller bug. Panicking before touching a slot
        // prevents races through safe Rust.
        Self::claim(&self.header().consumer_tid).expect("ring has one consumer");
        let head=self.header().head.load(Ordering::Relaxed);
        let tail=self.header().tail.load(Ordering::Acquire);
        if head==tail{return None;}
        let data=unsafe{
            let slot=&*self.slot(head);
            assert!((slot.len as usize)<=FRAME_BYTES,"corrupt ring slot");
            slice::from_raw_parts(slot.bytes.as_ptr(),slot.len as usize).to_vec()
        };
        self.header().head.store(head+1,Ordering::Release);
        Some(data)
    }
}
impl Drop for SharedRing {
    fn drop(&mut self) {unsafe{libc::munmap(self.pointer.cast(),self.total);}}
}
#[cfg(test)]
mod tests {
 use super::*;
 #[test] fn full_never_overwrites_and_fifo() {
  let ring=SharedRing::new().unwrap();
  for i in 0..CAPACITY {ring.try_push(&(i as u64).to_le_bytes()).unwrap();}
  assert_eq!(ring.depth(),CAPACITY as u64);
  assert_eq!(ring.try_push(b"overflow"),Err(CoreError::HistoryPressure));
  for i in 0..CAPACITY {let x=ring.try_pop().unwrap();assert_eq!(u64::from_le_bytes(x.try_into().unwrap()),i as u64);}
  assert!(ring.try_pop().is_none());
 }
 #[test] fn second_producer_fails_closed() {
  let ring=std::sync::Arc::new(SharedRing::new().unwrap());
  ring.try_push(b"a").unwrap();
  let second=ring.clone();
  assert_eq!(std::thread::spawn(move||second.try_push(b"b")).join().unwrap(),
             Err(CoreError::WrongOwner));
  assert_eq!(ring.try_pop().unwrap(),b"a");
 }
 #[test] fn producer_consumer_threads() {
  let ring=std::sync::Arc::new(SharedRing::new().unwrap());
  let child=ring.clone();
  let worker=std::thread::spawn(move||{
   for i in 0..5000_u64 {loop {
    if child.try_push(&i.to_le_bytes()).is_ok(){break;}
    std::thread::yield_now();
   }}
  });
  for i in 0..5000_u64 {let bytes=loop {
   if let Some(x)=ring.try_pop(){break x;}
   std::thread::yield_now();
  };assert_eq!(u64::from_le_bytes(bytes.try_into().unwrap()),i);}
  worker.join().unwrap();
 }
}
