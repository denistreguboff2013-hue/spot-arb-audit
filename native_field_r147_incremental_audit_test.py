"""R147 offline evidence invariants: no false REAL, credentials, or duplicate source credit."""
import json,tempfile,unittest
from pathlib import Path
import native_field_r147_incremental_audit as audit
ROOT=Path("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018")
HERE=Path(__file__).resolve().parent
class AuditR147(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(prefix="spotarb-r147-a5-")
  self.work=Path(self.t.name);self.parent=self.work/"r146.json";self.claim=self.work/"r147.json"
  self.parent.write_bytes((ROOT/"ASTRA_A5_160_FIELD_R146_20261020.json").read_bytes())
  self.claim.write_bytes((HERE/"native_field_evidence_r147_real_trial_guard_review.json").read_bytes())
 def tearDown(self):self.t.cleanup()
 def run_gate(self):
  audit.generate(self.parent,self.claim,self.work/"out")
  return json.loads((self.work/"out"/"ASTRA_A5_160_FIELD_R147_20261020.json").read_text())
 def claim_mutate(self,change):
  x=json.loads(self.claim.read_text());change(x);self.claim.write_text(json.dumps(x))
 def prior_mutate(self,change):
  x=json.loads(self.parent.read_text());change(x);self.parent.write_text(json.dumps(x))
 def test_exact_89_research_and_no_deployed_proof(self):
  x=self.run_gate()
  self.assertEqual((x["original_field_count"],x["previous_research_consumed"],
   x["new_distinct_research_consumed"],x["total_research_consumed"],
   x["research_not_evidenced"],x["deployed_paper_fields_proven"]),
   (160,81,8,89,71,0))
  self.assertEqual(x["priority_group_breakdown"]["SAFETY_AND_RECOVERY"],
   {"total":53,"research":44,"missing":9})
  self.assertFalse(x["real_execution_authorized"])
  self.assertFalse(x["real_kill_switch_or_preflight_read"])
  self.assertFalse(x["original_exchange_account_authenticated"])
 def test_fake_real_flag_rejected(self):
  self.claim_mutate(lambda x:x.update(real_trade_allowed=True))
  with self.assertRaises(ValueError):self.run_gate()
 def test_fake_kill_switch_attestation_rejected(self):
  self.claim_mutate(lambda x:x.update(kill_switch_loaded_or_disengaged=True))
  with self.assertRaises(ValueError):self.run_gate()
 def test_fake_preflight_validation_rejected(self):
  self.claim_mutate(lambda x:x.update(preflight_read_or_verified=True))
  with self.assertRaises(ValueError):self.run_gate()
 def test_fake_installed_binary_evidence_rejected(self):
  self.claim_mutate(lambda x:x.update(active_binary_proven=True))
  with self.assertRaises(ValueError):self.run_gate()
 def test_fake_telegram_evidence_rejected(self):
  self.claim_mutate(lambda x:x.update(telegram_delivery_verified=True))
  with self.assertRaises(ValueError):self.run_gate()
 def test_unknown_field_and_duplicate_credit_rejected(self):
  self.claim_mutate(lambda x:x["fields"].append("real_trial.false_owner_check"))
  with self.assertRaises(ValueError):self.run_gate()
  self.claim.write_bytes((HERE/"native_field_evidence_r147_real_trial_guard_review.json").read_bytes())
  self.claim_mutate(lambda x:x["fields"].append("real_trial.enabled"))
  with self.assertRaises(ValueError):self.run_gate()
 def test_prior_ledger_double_count_rejected(self):
  self.prior_mutate(lambda x:next(r for r in x["fields"]
    if r["field"]=="real_trial.entry_cooldown_sec").update(
     research_evidence="TESTED_RESEARCH_READONLY"))
  with self.assertRaises(ValueError):self.run_gate()
 def test_fake_original_paper_proof_rejected(self):
  self.prior_mutate(lambda x:next(r for r in x["fields"]
    if r["field"]=="real_trial.enabled").update(installed_paper_a342540="PROVEN"))
  with self.assertRaises(ValueError):self.run_gate()
 def test_baseline_81_overcredit_rejected(self):
  self.prior_mutate(lambda x:x.update(total_research_consumed=82))
  with self.assertRaises(ValueError):self.run_gate()
if __name__=="__main__":unittest.main()
