"""R148 isolated Rust research source evidence audit regression."""
import json,tempfile,unittest
from pathlib import Path
import native_field_r148_incremental_audit as audit
ROOT=Path("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018")
HERE=Path(__file__).resolve().parent
class R148Audit(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(prefix="spotarb-a5-r148-")
  self.dir=Path(self.tmp.name)
  self.parent=self.dir/"r147.json";self.claim=self.dir/"r148.json"
  self.parent.write_bytes((ROOT/"ASTRA_A5_160_FIELD_R147_20261020.json").read_bytes())
  self.claim.write_bytes((HERE/"native_field_evidence_r148_real_trial_artifacts.json").read_bytes())
 def tearDown(self):self.tmp.cleanup()
 def go(self):
  audit.generate(self.parent,self.claim,self.dir/"out")
  return json.loads((self.dir/"out"/"ASTRA_A5_160_FIELD_R148_20261020.json").read_text())
 def edit(self,p,callback):
  x=json.loads(p.read_text());callback(x);p.write_text(json.dumps(x))
 def test_counts_and_isolation(self):
  x=self.go()
  self.assertEqual((x["original_field_count"],x["previous_research_consumed"],
   x["new_isolated_test_only_research_consumed"],x["total_research_consumed"],
   x["research_not_evidenced"],x["deployed_paper_fields_proven"]),
   (160,89,9,98,62,0))
  self.assertEqual(x["priority_group_breakdown"]["SAFETY_AND_RECOVERY"],
   {"total":53,"research":53,"missing":0})
  self.assertFalse(x["new_nine_wired_to_main_library"])
  self.assertFalse(x["real_execution_authorized"])
 def test_cannot_fake_main_library_link(self):
  self.edit(self.claim,lambda x:x.update(linked_into_main_rust_library=True))
  with self.assertRaises(ValueError):self.go()
 def test_cannot_fake_account_authentication(self):
  self.edit(self.claim,lambda x:x.update(inventory_account_owner_verified=True))
  with self.assertRaises(ValueError):self.go()
 def test_cannot_fake_kill_or_preflight(self):
  self.edit(self.claim,lambda x:x.update(kill_switch_disengaged_proven=True))
  with self.assertRaises(ValueError):self.go()
 def test_cannot_fake_real(self):
  self.edit(self.claim,lambda x:x.update(real_launch_authorized=True))
  with self.assertRaises(ValueError):self.go()
 def test_wrong_scope(self):
  self.edit(self.claim,lambda x:x.update(scope="rust_production_executable"))
  with self.assertRaises(ValueError):self.go()
 def test_unknown_and_duplicate_fields(self):
  self.edit(self.claim,lambda x:x["fields"].append("real_trial.not_original"))
  with self.assertRaises(ValueError):self.go()
  self.claim.write_bytes((HERE/"native_field_evidence_r148_real_trial_artifacts.json").read_bytes())
  self.edit(self.claim,lambda x:x["fields"].append("real_trial.preflight_path"))
  with self.assertRaises(ValueError):self.go()
 def test_double_research_credit(self):
  self.edit(self.parent,lambda x:next(r for r in x["fields"]
   if r["field"]=="real_trial.kill_switch_path").update(
    research_evidence="TESTED_RESEARCH_READONLY"))
  with self.assertRaises(ValueError):self.go()
 def test_forged_deployed_field(self):
  self.edit(self.parent,lambda x:next(r for r in x["fields"]
   if r["field"]=="real_trial.operator_approved").update(
    installed_paper_a342540="PROVEN"))
  with self.assertRaises(ValueError):self.go()
 def test_bad_prior_count(self):
  self.edit(self.parent,lambda x:x.update(total_research_consumed=90))
  with self.assertRaises(ValueError):self.go()
if __name__=="__main__":unittest.main()
