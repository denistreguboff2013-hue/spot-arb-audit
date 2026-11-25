// R178: opt-in actual SIGKILL research checkpoint. NEVER on live services.
// Requires test-only environment marker; caller still must use PRIVATE /tmp roots.
fn r178_sigkill_at(fault:Option<&str>,phase:&str)
 ->Result<(),PaperAdapterErrorR156>{
 if fault!=Some(phase){return Ok(())}
 if std::env::var("ASTRA_R178_RESEARCH_CHILD").ok().as_deref()!=Some("ISOLATED_ONLY"){
  return Err(PaperAdapterErrorR156::Source)
 }
 use std::io::Write;
 println!("R178_CHILD_REACHED {phase} in isolated process");
 let _=std::io::stdout().flush();
 unsafe{libc::raise(libc::SIGKILL);}
 Err(PaperAdapterErrorR156::Mirror)
}
