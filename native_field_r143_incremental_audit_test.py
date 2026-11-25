"""Offline fail-closed incremental Astra R143 evidence gate."""
import json
import tempfile
import unittest
from pathlib import Path

import native_field_r143_incremental_audit as audit

ROOT=Path("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018")
HERE=Path(__file__).resolve().parent


class R143Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix="astra-r143-local-")
        self.folder=Path(self.temp.name)
        self.parent=self.folder/"r142.json"
        self.claim=self.folder/"r143.json"
        self.parent.write_bytes((ROOT/"ASTRA_A5_160_FIELD_R142_20261020.json").read_bytes())
        self.claim.write_bytes((HERE/"native_field_evidence_r143_entry_review.json").read_bytes())

    def tearDown(self):
        self.temp.cleanup()

    def run_gate(self):
        audit.build(self.parent,self.claim,self.folder/"out")
        return json.loads((self.folder/"out"/"ASTRA_A5_160_FIELD_R143_20261020.json").read_text())

    def mutate_claim(self,change):
        item=json.loads(self.claim.read_text())
        change(item)
        self.claim.write_text(json.dumps(item))

    def mutate_parent(self,change):
        item=json.loads(self.parent.read_text())
        change(item)
        self.parent.write_text(json.dumps(item))

    def test_45_distinct_only_research_two_new_and_zero_deployed(self):
        result=self.run_gate()
        self.assertEqual((result["original_field_count"],
                          result["prior_research_consumed"],
                          result["new_distinct_research_consumed"],
                          result["total_research_consumed"],
                          result["research_not_evidenced"],
                          result["deployed_paper_fields_proven"]),(160,43,2,45,115,0))
        self.assertEqual(result["priority_group_breakdown"]["ENTRY_RISK_AND_MARKET"],
                         {"total":31,"research":28,"missing":3})
        self.assertEqual(len(result["fields"]),160)
        self.assertFalse(result["durable_distributed_entry_lock_proven"])
        self.assertFalse(result["real_execution_authorized"])

    def test_forged_deployed_authority_denied(self):
        self.mutate_claim(lambda x:x.update(active_binary_proven=True))
        with self.assertRaises(ValueError): self.run_gate()

    def test_forged_lock_authority_denied(self):
        self.mutate_claim(lambda x:x.update(distributed_lock_proven=True))
        with self.assertRaises(ValueError): self.run_gate()

    def test_unknown_or_duplicate_claim_denied(self):
        self.mutate_claim(lambda x:x["fields"].append("limits.not_original"))
        with self.assertRaises(ValueError): self.run_gate()
        self.claim.write_bytes((HERE/"native_field_evidence_r143_entry_review.json").read_bytes())
        self.mutate_claim(lambda x:x["fields"].append("limits.top_n_per_scan"))
        with self.assertRaises(ValueError): self.run_gate()

    def test_double_credit_denied(self):
        self.mutate_parent(lambda x:next(row for row in x["fields"]
           if row["field"]=="entry_lock_ttl_sec").update(research_evidence="TESTED_RESEARCH_READONLY"))
        with self.assertRaises(ValueError): self.run_gate()

    def test_spoofed_installed_paper_source_denied(self):
        self.mutate_parent(lambda x:next(row for row in x["fields"]
           if row["field"]=="limits.top_n_per_scan").update(installed_paper_a342540="PROVEN"))
        with self.assertRaises(ValueError): self.run_gate()

    def test_r142_baseline_drift_denied(self):
        self.mutate_parent(lambda x:x.update(total_research_consumed=44))
        with self.assertRaises(ValueError): self.run_gate()


if __name__=="__main__":
    unittest.main()
