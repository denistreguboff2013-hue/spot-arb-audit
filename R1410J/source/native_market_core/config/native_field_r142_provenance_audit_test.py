"""Offline regression tests: Astra A5 160-source ledger must fail closed."""
import contextlib
import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import native_field_r142_provenance_audit as a

BASELINE=Path(
 "/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018/"
 "ASTRA_P106_R130_ALL_160_FIELDS_CLASSIFIED_20261018.csv")


class LedgerR142Tests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix="spotarb-a5-r142-")
  self.root=Path(self.temp.name)
  self.baseline=self.root/"original.csv"
  self.baseline.write_bytes(BASELINE.read_bytes())
  self.out=self.root/"out"
  self.prior_overlays=a.OVERLAYS
  self.prior_legacy=a.LEGACY

 def tearDown(self):
  a.OVERLAYS=self.prior_overlays
  a.LEGACY=self.prior_legacy
  self.temp.cleanup()

 def run_ledger(self):
  with patch.object(sys,"argv",["a5", "--baseline-csv",str(self.baseline),
                                "--out",str(self.out)]):
   with contextlib.redirect_stdout(io.StringIO()):
    a.main()

 def corrupt_r142(self,mutate):
  prior=list(a.OVERLAYS)
  value=json.loads(prior[-1].read_text())
  mutate(value)
  new=self.root/"native_field_evidence_r142_analytics_ai_review.json"
  new.write_text(json.dumps(value))
  a.OVERLAYS=tuple(prior[:-1]+[new])

 def test_pass_preserves_160_truth_and_no_deployed_claim(self):
  self.run_ledger()
  result=json.loads((self.out/"ASTRA_A5_160_FIELD_R142_20261020.json").read_text())
  self.assertEqual((result["original_field_count"],
                    result["baseline_research_consumed"],
                    result["new_distinct_research_consumed"],
                    result["total_research_consumed"],
                    result["research_not_evidenced"],
                    result["deployed_paper_fields_proven"]),(160,36,7,43,117,0))
  groups=result["priority_group_breakdown"]
  self.assertEqual(sum(g["total"] for g in groups.values()),160)
  self.assertEqual(sum(g["research"] for g in groups.values()),43)
  self.assertEqual(sum(g["missing"] for g in groups.values()),117)
  self.assertEqual(len(result["fields"]),160)
  self.assertEqual(len({x["field"] for x in result["fields"]}),160)
  self.assertEqual(len({x["field"] for x in result["fields"]
     if x["research_evidence"]=="TESTED_RESEARCH_READONLY"}),43)
  self.assertFalse(result["analytics_sqlite_writer_proven"])
  self.assertFalse(result["installed_binary_runtime_consumption_attested"])
  self.assertFalse(result["real_execution_authorized"])
  self.assertTrue((self.out/"ASTRA_A5_160_FIELD_R142_20261020.csv").exists())

 def test_spoofed_active_binary_evidence_is_rejected(self):
  self.corrupt_r142(lambda x:x.update(active_binary_proven=True))
  with self.assertRaises(ValueError):
   self.run_ledger()

 def test_unsupported_field_is_rejected(self):
  self.corrupt_r142(lambda x:x["fields"].append("ai.auto_apply_strategy"))
  with self.assertRaises(ValueError):
   self.run_ledger()

 def test_duplicate_field_is_rejected(self):
  self.corrupt_r142(lambda x:x["fields"].append("analytics.enabled"))
  with self.assertRaises(ValueError):
   self.run_ledger()

 def test_research_double_count_is_rejected(self):
  with self.baseline.open(newline="") as f:
   rows=list(csv.DictReader(f))
  next(x for x in rows if x["field"]=="analytics.enabled")[
   "research_consumer_claimed"]="True"
  with self.baseline.open("w",newline="") as f:
   out=csv.DictWriter(f,fieldnames=list(rows[0]))
   out.writeheader()
   out.writerows(rows)
  with self.assertRaises(ValueError):
   self.run_ledger()

 def test_spoofed_deployed_proof_in_baseline_is_rejected(self):
  with self.baseline.open(newline="") as f:
   rows=list(csv.DictReader(f))
  rows[0]["active_binary_proven"]="True"
  with self.baseline.open("w",newline="") as f:
   out=csv.DictWriter(f,fieldnames=list(rows[0]))
   out.writeheader()
   out.writerows(rows)
  with self.assertRaises(ValueError):
   self.run_ledger()

 def test_original_yaml_schema_type_change_is_rejected(self):
  yaml_path=self.root/"wrong-legacy.yaml"
  source=self.prior_legacy.read_text()
  target="max_wal_mb: 64"
  self.assertIn(target,source)
  yaml_path.write_text(source.replace(target,"max_wal_mb: bad",1))
  a.LEGACY=yaml_path
  with self.assertRaises(ValueError):
   self.run_ledger()


 def test_duplicate_yaml_mapping_key_is_rejected(self):
  yaml_path=self.root/"duplicate-legacy.yaml"
  source=self.prior_legacy.read_text()
  target="max_wal_mb: 64"
  self.assertIn(target,source)
  yaml_path.write_text(source.replace(
   target,"max_wal_mb: 64"+chr(10)+"  max_wal_mb: 128",1))
  a.LEGACY=yaml_path
  with self.assertRaises(ValueError):
   self.run_ledger()

if __name__=="__main__":
 unittest.main()
