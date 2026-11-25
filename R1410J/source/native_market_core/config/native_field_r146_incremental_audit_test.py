"""R146 source-only 160 original field overlay: negative regressions."""
import json,tempfile,unittest
from pathlib import Path
import native_field_r146_incremental_audit as audit
ROOT=Path("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018")
HERE=Path(__file__).resolve().parent
class R146Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(prefix="spotarb-r146-")
  self.base=Path(self.tmp.name)
  self.old=self.base/"r145.json";self.claim=self.base/"r146.json"
  self.old.write_bytes((ROOT/"ASTRA_A5_160_FIELD_R145_20261020.json").read_bytes())
  self.claim.write_bytes((HERE/"native_field_evidence_r146_hardstop_financial_review.json").read_bytes())
 def tearDown(self):self.tmp.cleanup()
 def run_gate(self):
  audit.generate(self.old,self.claim,self.base/"out")
  return json.loads((self.base/"out"/"ASTRA_A5_160_FIELD_R146_20261020.json").read_text())
 def change_claim(self,change):
  x=json.loads(self.claim.read_text());change(x);self.claim.write_text(json.dumps(x))
 def change_parent(self,change):
  x=json.loads(self.old.read_text());change(x);self.old.write_text(json.dumps(x))
 def test_expected_original_fields_no_deploy_claim(self):
  r=self.run_gate()
  self.assertEqual((r["original_field_count"],r["previous_research_consumed"],
      r["new_distinct_research_consumed"],r["total_research_consumed"],
      r["research_not_evidenced"],r["deployed_paper_fields_proven"]),
     (160,71,10,81,79,0))
  self.assertEqual(r["priority_group_breakdown"]["SAFETY_AND_RECOVERY"],
    {"total":53,"research":36,"missing":17})
  self.assertFalse(r["entire_real_account_day_authenticated"])
  self.assertFalse(r["real_execution_authorized"])
  self.assertFalse(r["unknown_ack_retry_authorized"])
 def test_cannot_forge_deployed_binary(self):
  self.change_claim(lambda x:x.update(active_binary_proven=True))
  with self.assertRaises(ValueError):self.run_gate()
 def test_cannot_forge_real_trade(self):
  self.change_claim(lambda x:x.update(may_open_real=True))
  with self.assertRaises(ValueError):self.run_gate()
 def test_cannot_forge_authenticated_day_history(self):
  self.change_claim(lambda x:x.update(account_history_complete_proven=True))
  with self.assertRaises(ValueError):self.run_gate()
 def test_cannot_forge_independent_fee_economics(self):
  self.change_claim(lambda x:x.update(independent_daily_okx_fees_verified=True))
  with self.assertRaises(ValueError):self.run_gate()
 def test_nonoriginal_and_duplicate_field_denied(self):
  self.change_claim(lambda x:x["fields"].append("real_trial.unknown_limit"))
  with self.assertRaises(ValueError):self.run_gate()
  self.claim.write_bytes((HERE/"native_field_evidence_r146_hardstop_financial_review.json").read_bytes())
  self.change_claim(lambda x:x["fields"].append("native_runtime.real_execution_authorized"))
  with self.assertRaises(ValueError):self.run_gate()
 def test_overlap_with_prior_denied(self):
  self.change_parent(lambda x:next(r for r in x["fields"]
    if r["field"]=="limits.max_daily_loss_usdt").update(
      research_evidence="TESTED_RESEARCH_READONLY"))
  with self.assertRaises(ValueError):self.run_gate()
 def test_false_active_paper_proof_in_parent_denied(self):
  self.change_parent(lambda x:next(r for r in x["fields"]
    if r["field"]=="real_trial.deny_on_unknown").update(
      installed_paper_a342540="PROVEN"))
  with self.assertRaises(ValueError):self.run_gate()
 def test_bad_previous_coverage_denied(self):
  self.change_parent(lambda x:x.update(total_research_consumed=72))
  with self.assertRaises(ValueError):self.run_gate()
if __name__=="__main__":unittest.main()
