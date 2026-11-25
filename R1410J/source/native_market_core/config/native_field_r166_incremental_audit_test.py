"""R166 negative tests: offline 16-key additive proof only."""
import json,tempfile,unittest
from pathlib import Path
import native_field_r166_incremental_audit as audit
import native_field_r164_crosswalk as r164

class R166Provenance(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix="r166-shadow-audit-")
  self.dir=Path(self.temp.name)
  self.parent=self.dir/"r149.json"
  self.claim=self.dir/"claim.json"
  self.parent.write_bytes(audit_path().read_bytes())
  self.claim.write_bytes((r164.HERE/
   "native_field_evidence_r166_shadow_source_review.json").read_bytes())
 def tearDown(self):self.temp.cleanup()
 def run_audit(self):
  return audit.generate(self.parent,self.claim,self.dir/"out")
 def mutate(self,path,fn):
  v=json.loads(path.read_text());fn(v)
  path.write_text(json.dumps(v))
 def test_123_37_counter_stays_research_only(self):
  r=self.run_audit()
  self.assertEqual((r["original_field_count"],r["previous_research_consumed"],
    r["new_distinct_research_consumed"],r["total_research_consumed"],
    r["research_not_evidenced"],r["deployed_paper_fields_proven"]),
    (160,107,16,123,37,0))
  self.assertFalse(r["source_simulator_is_installed_paper"])
  self.assertFalse(r["real_execution_authorized"])
  self.assertFalse(r["r155_stale_ingress_fixed"])
  self.assertFalse(r["original_spread_history_window_proven"])
  fields=[x for x in r["fields"] if x["research_evidence"]=="NOT_EVIDENCED"]
  self.assertIn("spread.history_window",{x["field"] for x in fields})
  rows=json.loads((self.dir/"out"/
    "ASTRA_A5_160_FIELD_R166_20261020.json").read_text())["fields"]
  self.assertEqual(len(rows),160)
 def test_reject_source_fields_double_credit(self):
  self.mutate(self.parent,lambda a:next(x for x in a["fields"]
    if x["field"]=="fast_market_shadow.enabled").update(
       research_evidence="TESTED_RESEARCH_READONLY"))
  with self.assertRaises(ValueError):self.run_audit()
 def test_reject_forged_installed_r166_claim(self):
  self.mutate(self.claim,lambda a:a.update(active_binary_proven=True))
  with self.assertRaises(ValueError):self.run_audit()
 def test_reject_claim_of_live_ws_fleet(self):
  self.mutate(self.claim,lambda a:a.update(actual_websocket_fleet_launched=True))
  with self.assertRaises(ValueError):self.run_audit()
 def test_reject_fake_real_order_permission(self):
  self.mutate(self.claim,lambda a:a.update(real_order_authority=True))
  with self.assertRaises(ValueError):self.run_audit()
 def test_reject_15_or_repeated_shadow_fields(self):
  self.mutate(self.claim,lambda a:a["fields"].pop())
  with self.assertRaises(ValueError):self.run_audit()
  self.claim.write_bytes((r164.HERE/
   "native_field_evidence_r166_shadow_source_review.json").read_bytes())
  self.mutate(self.claim,lambda a:a["fields"].__setitem__(
    -1,a["fields"][0]))
  with self.assertRaises(ValueError):self.run_audit()
 def test_reject_nonshadow_field_falsely_credited(self):
  self.mutate(self.claim,lambda a:a["fields"].__setitem__(
    -1,"spread.history_window"))
  with self.assertRaises(ValueError):self.run_audit()
 def test_reject_r149_parent_modified(self):
  self.mutate(self.parent,lambda a:a.update(total_research_consumed=108))
  with self.assertRaises(ValueError):self.run_audit()
 def test_reject_original_claim_of_python_equivalence(self):
  self.mutate(self.claim,lambda a:a.update(
   source_consumer_semantics_equal_to_installed_python=True))
  with self.assertRaises(ValueError):self.run_audit()

def audit_path():
 return Path("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018/"
  "ASTRA_A5_160_FIELD_R149_20261020.json")

if __name__=="__main__":unittest.main()
