"""Astra R149 provenance negative tests: real/telegram and double-count denied."""
import json,tempfile,unittest
from pathlib import Path
import native_field_r149_incremental_audit as audit
ROOT=Path("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018")
HERE=Path(__file__).resolve().parent
class R149Provenance(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(prefix="a5-r149-")
  self.dir=Path(self.tmp.name)
  self.parent=self.dir/"r148.json"
  self.claim=self.dir/"claim.json"
  self.parent.write_bytes((ROOT/"ASTRA_A5_160_FIELD_R148_20261020.json").read_bytes())
  self.claim.write_bytes((HERE/"native_field_evidence_r149_entry_telegram_review.json").read_bytes())
 def tearDown(self):self.tmp.cleanup()
 def generate(self):
  audit.generate(self.parent,self.claim,self.dir/"out")
  return json.loads((self.dir/"out"/"ASTRA_A5_160_FIELD_R149_20261020.json").read_text())
 def mutate(self,path,fn):
  x=json.loads(path.read_text());fn(x);path.write_text(json.dumps(x))
 def test_strict_160_107_53_not_installed(self):
  r=self.generate()
  self.assertEqual((r["original_field_count"],r["previous_research_consumed"],
    r["new_distinct_research_consumed"],r["total_research_consumed"],
    r["research_not_evidenced"],r["deployed_paper_fields_proven"]),
    (160,98,9,107,53,0))
  self.assertEqual(r["priority_group_breakdown"]["ENTRY_RISK_AND_MARKET"],
    {"total":31,"research":30,"missing":1})
  self.assertEqual(r["priority_group_breakdown"]["OPTIONAL_TELEMETRY_AND_REPORTING"],
    {"total":76,"research":24,"missing":52})
  self.assertFalse(r["telegram_delivered"])
  self.assertFalse(r["real_execution_authorized"])
 def test_no_real_authority(self):
  self.mutate(self.claim,lambda x:x.update(real_order_authority=True))
  with self.assertRaises(ValueError):self.generate()
 def test_no_telegram_delivery(self):
  self.mutate(self.claim,lambda x:x.update(telegram_delivery_proven=True))
  with self.assertRaises(ValueError):self.generate()
 def test_no_installed_binding(self):
  self.mutate(self.claim,lambda x:x.update(linked_to_installed_paper=True))
  with self.assertRaises(ValueError):self.generate()
 def test_original_spread_history_still_missing(self):
  self.mutate(self.claim,lambda x:x["fields"].append("spread.history_window"))
  with self.assertRaises(ValueError):self.generate()
 def test_duplicate_and_unknown_field_denied(self):
  self.mutate(self.claim,lambda x:x["fields"].append("telegram.enabled"))
  with self.assertRaises(ValueError):self.generate()
  self.claim.write_bytes((HERE/"native_field_evidence_r149_entry_telegram_review.json").read_bytes())
  self.mutate(self.claim,lambda x:x["fields"].append("telegram.not_existing"))
  with self.assertRaises(ValueError):self.generate()
 def test_prior_field_double_credit_denied(self):
  self.mutate(self.parent,lambda x:next(r for r in x["fields"]
   if r["field"]=="limits.max_trades_per_day").update(
   research_evidence="TESTED_RESEARCH_READONLY"))
  with self.assertRaises(ValueError):self.generate()
 def test_forged_prior_deployed_field_denied(self):
  self.mutate(self.parent,lambda x:next(r for r in x["fields"]
   if r["field"]=="telegram.enabled").update(installed_paper_a342540="PROVEN"))
  with self.assertRaises(ValueError):self.generate()
 def test_parent_98_baseline_change_denied(self):
  self.mutate(self.parent,lambda x:x.update(total_research_consumed=99))
  with self.assertRaises(ValueError):self.generate()
if __name__=="__main__":unittest.main()
