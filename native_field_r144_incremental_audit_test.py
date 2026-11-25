"""Offline R144 provenance: no fabricated source, binary, or REAL proof."""
import json
import tempfile
import unittest
from pathlib import Path
import native_field_r144_incremental_audit as a

ROOT = Path("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018")
HERE = Path(__file__).resolve().parent


class ProvenanceR144(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="spotarb-r144-")
        self.work = Path(self.tmp.name)
        self.parent = self.work / "r143.json"
        self.claim = self.work / "claim.json"
        self.parent.write_bytes(
            (ROOT / "ASTRA_A5_160_FIELD_R143_20261020.json").read_bytes())
        self.claim.write_bytes(
            (HERE / "native_field_evidence_r144_execution_review.json").read_bytes())

    def tearDown(self):
        self.tmp.cleanup()

    def check(self):
        a.audit(self.parent, self.claim, self.work / "result")
        return json.loads(
            (self.work / "result" / "ASTRA_A5_160_FIELD_R144_20261020.json").read_text())

    def claim_change(self, fn):
        x = json.loads(self.claim.read_text())
        fn(x)
        self.claim.write_text(json.dumps(x))

    def parent_change(self, fn):
        x = json.loads(self.parent.read_text())
        fn(x)
        self.parent.write_text(json.dumps(x))

    def test_original_160_53_107_no_live_claim(self):
        x = self.check()
        self.assertEqual(
            (x["original_field_count"],x["previous_research_consumed"],
             x["new_distinct_research_consumed"],x["total_research_consumed"],
             x["research_not_evidenced"],x["deployed_paper_fields_proven"]),
            (160,45,8,53,107,0))
        self.assertEqual(x["priority_group_breakdown"]["SAFETY_AND_RECOVERY"],
                         {"total":53,"research":8,"missing":45})
        self.assertFalse(x["unknown_ack_retry_authorized"])
        self.assertFalse(x["real_execution_authorized"])

    def test_faked_deployment_denied(self):
        self.claim_change(lambda x:x.update(active_binary_proven=True))
        with self.assertRaises(ValueError): self.check()

    def test_faked_unknown_retry_denied(self):
        self.claim_change(lambda x:x.update(retry_after_unknown_ack_permitted=True))
        with self.assertRaises(ValueError): self.check()

    def test_faked_order_submission_denied(self):
        self.claim_change(lambda x:x.update(order_submission_permitted=True))
        with self.assertRaises(ValueError): self.check()

    def test_unknown_and_duplicate_source_fields_denied(self):
        self.claim_change(lambda x:x["fields"].append("execution.dangerous_extra"))
        with self.assertRaises(ValueError): self.check()
        self.claim.write_bytes(
            (HERE / "native_field_evidence_r144_execution_review.json").read_bytes())
        self.claim_change(lambda x:x["fields"].append("execution.spot_retry_n"))
        with self.assertRaises(ValueError): self.check()

    def test_double_credit_denied(self):
        self.parent_change(lambda x:next(row for row in x["fields"]
            if row["field"]=="execution.spot_retry_n").update(
            research_evidence="TESTED_RESEARCH_READONLY"))
        with self.assertRaises(ValueError): self.check()

    def test_faked_installed_field_denied(self):
        self.parent_change(lambda x:next(row for row in x["fields"]
            if row["field"]=="execution.spot_retry_n").update(
            installed_paper_a342540="PROVEN"))
        with self.assertRaises(ValueError): self.check()

    def test_bad_parent_count_denied(self):
        self.parent_change(lambda x:x.update(total_research_consumed=46))
        with self.assertRaises(ValueError): self.check()


if __name__ == "__main__":
    unittest.main()
