//! R22 durable non-network execution/recovery/lifecycle drill.
//! Creates physical PREPARED state but never signs or dispatches HTTP.

use serde_json::json;
use spotarb_native_market_core::{
    execution::{Action,IntentState,Leg},
    execution_store::DurableMutationGate,
    position_lifecycle::{
        DurableLifecycleStore, LifecycleAction, LifecycleState, PositionLifecycle, RecoveryEvidence,
    },
};
use std::{fs,io,path::PathBuf};

fn lifecycle<T>(r:Result<T,spotarb_native_market_core::position_lifecycle::LifecycleError>)->Result<T,Box<dyn std::error::Error>>{
    r.map_err(|e|Box::new(io::Error::other(format!("{e:?}"))) as Box<dyn std::error::Error>)
}

fn main()->Result<(),Box<dyn std::error::Error>>{
    let root=PathBuf::from(std::env::args().nth(1).ok_or("usage: r22_durable_execution_drill <isolated-dir>")?);
    if root.exists(){fs::remove_dir_all(&root)?}
    fs::create_dir_all(&root)?;

    let entry_path=root.join("entry_intents.json");
    let mut entry=DurableMutationGate::open(&entry_path)?;
    let a=entry.begin("r22-entry-1",Leg::Spot,"sab00000000000000000000000000001")?;
    assert!(matches!(a,Action::PersistPrepare{..}));
    let permission=entry.after_durable_prepare("r22-entry-1")?;
    assert!(matches!(permission,Action::NetworkMutation{leg:Leg::Spot,..}));
    drop(entry);

    let mut reopened=DurableMutationGate::open(&entry_path)?;
    assert!(reopened.after_durable_prepare("r22-entry-1").is_err());
    assert!(reopened.begin("r22-entry-2",Leg::Futures,"sab00000000000000000000000000002").is_err());
    assert_eq!(reopened.store.load()?[0].state,IntentState::Prepared);
    reopened.store.persist_reconciled("r22-entry-1",true)?;
    assert!(reopened.begin("r22-entry-2",Leg::Spot,"sab00000000000000000000000000002").is_ok());
    drop(reopened);

    let lifecycle_store=DurableLifecycleStore::open(root.join("lifecycle.json"))?;
    let mut close_gate=DurableMutationGate::open(root.join("close_intents.json"))?;
    let mut life=lifecycle(PositionLifecycle::new_hedged(10,7))?;
    lifecycle(life.start_close())?;
    lifecycle_store.persist(&life)?;
    assert_eq!(life.next_action(),LifecycleAction::CloseFuturesReduceOnly);

    let plan=lifecycle(life.prepare_close_mutation(
        &lifecycle_store,&mut close_gate,
        "r22-close-fut-1","sab00000000000000000000000000003",
        "BNT/USDT:USDT",7,7))?;
    assert!(matches!(plan.network_action,Action::NetworkMutation{leg:Leg::Futures,..}));
    assert!(plan.reduce_only);
    assert_eq!(plan.requested_units,10);
    drop(close_gate);drop(life);

    let mut close_gate=DurableMutationGate::open(root.join("close_intents.json"))?;
    let mut life=lifecycle_store.load_for_recovery()?.ok_or("missing lifecycle")?;
    assert_eq!(life.state,LifecycleState::RecoveryRequired);
    assert_eq!(life.next_action(),LifecycleAction::ReconcilePending);
    assert!(close_gate.after_durable_prepare("r22-close-fut-1").is_err());
    assert!(close_gate.begin("blind-retry",Leg::Futures,"sab00000000000000000000000000004").is_err());

    lifecycle(life.recover_pending(
        &lifecycle_store,&mut close_gate,true,RecoveryEvidence::ConfirmedNotFound))?;
    assert_eq!(life.state,LifecycleState::ClosingFutures);
    assert_eq!(life.next_action(),LifecycleAction::CloseFuturesReduceOnly);
    assert!(life.pending.is_none());
    assert_eq!(close_gate.store.load()?[0].state,IntentState::Reconciled);

    let retry=lifecycle(life.prepare_close_mutation(
        &lifecycle_store,&mut close_gate,
        "r22-close-fut-2","sab00000000000000000000000000005",
        "BNT/USDT:USDT",7,7))?;
    assert!(matches!(retry.network_action,Action::NetworkMutation{leg:Leg::Futures,..}));
    assert_eq!(retry.requested_units,10);

    let out=json!({
        "r22_durable_execution_drill":"PASS",
        "entry":{
            "prepared_survived_restart":true,
            "blind_resend_blocked":true,
            "evidence_reconciliation_released_barrier":true
        },
        "position_lifecycle":{
            "futures_first":true,
            "prepared_survived_restart":true,
            "restart_state":"RECOVERY_REQUIRED",
            "blind_retry_blocked":true,
            "confirmed_not_found_released_same_leg":true,
            "new_intent_requires_distinct_identity":true
        },
        "network":{"private_http_called":false,"signed_request_created":false,"order_sent":false},
        "trading":false
    });
    fs::write(root.join("RESULT.json"),serde_json::to_vec_pretty(&out)?)?;
    println!("{}",serde_json::to_string(&out)?);
    Ok(())
}
