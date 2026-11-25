//! Cross-journal completion evidence. Fields cannot be fabricated by the binary adapter.
use serde::{Deserialize,Serialize};
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct DecisionBoundary {
 pub(crate) canonical:String, pub(crate) source_sequence:u64, pub(crate) source_chain:String,
 pub(crate) symbol:String,pub(crate) epoch:u64,pub(crate) input_hash:String, pub(crate) generation:u64,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
 pub(crate) schema:u32, pub(crate) boundary:DecisionBoundary,
 pub(crate) execution_sequence:u64, pub(crate) execution_previous_hash:String,
 pub(crate) identity_hash:String, pub(crate) state_hash:String, pub(crate) result_hash:String,
}

/// Constant-size replay summary of the existing execution journal, never a second log.
pub struct CompletionHistory {
 pub(crate) count:u64,pub(crate) chain:String,pub(crate) prior_chain:String,
 pub(crate) last:Option<Completion>,pub(crate) uncompleted_effects:bool,
}
#[derive(Serialize)]
pub(crate) struct CanonicalReceipt<'a>{pub input_hash:&'a str,pub outcome:&'a str,pub result_hash:&'a str}
