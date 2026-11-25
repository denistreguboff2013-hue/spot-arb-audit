"""R145 offline 160-field evidence regression: fail closed on false claims."""
import json
import tempfile
import unittest
from pathlib import Path
import native_field_r145_incremental_audit as a

ROOT = Path("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018")
HERE = Path(__file__).resolve().parent


class R145Audit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="spotarb-r145-a5-")
        self.dir = Path(self.tmp.name)
        self.parent = self.dir / "original_r144.json"
        self.claim = self.dir / "claim_r145.json"
        self.parent.write_bytes(
            (ROOT / "ASTRA_A5_160_FIELD_R144_20261020.json").read_bytes())
        self.claim.write_bytes(
            (HERE / "native_field_evidence_r145_exit_emergency_review.json").read_bytes())

    def tearDown(self):
        self.tmp.cleanup()

    def run_audit(self):
        a.generate(self.parent, self.claim, self.dir / "out")
        return json.loads(
            (self.dir / "out" / "ASTRA_A5_160_FIELD_R145_20261020.json").read_text())

    def edit_claim(self, modify):
        data = json.loads(self.claim.read_text())
        modify(data)
        self.claim.write_text(json.dumps(data))

    def edit_parent(self, modify):
        data = json.loads(self.parent.read_text())
        modify(data)
        self.parent.write_text(json.dumps(data))

    def test_original_160_new18_only_safety_not_installed(self):
        out = self.run_audit()
        self.assertEqual((
            out["original_field_count"], out["prior_research_consumed"],
            out["new_distinct_research_consumed"], out["total_research_consumed"],
            out["research_not_evidenced"], out["deployed_paper_fields_proven"]
        ), (160, 53, 18, 71, 89, 0))
        self.assertEqual(out["priority_group_breakdown"]["SAFETY_AND_RECOVERY"],
                         {"total": 53, "research": 26, "missing": 27})
        self.assertFalse(out["actual_exchange_protective_stop_verified"])
        self.assertFalse(out["unknown_ack_retry_authorized"])
        self.assertFalse(out["real_execution_authorized"])
        self.assertEqual(len(out["fields"]), 160)

    def test_forged_real_permission_refused(self):
        self.edit_claim(lambda x: x.update(real_order_authority=True))
        with self.assertRaises(ValueError):
            self.run_audit()

    def test_forged_exchange_order_refused(self):
        self.edit_claim(lambda x: x.update(authenticated_exchange_stop_trigger=True))
        with self.assertRaises(ValueError):
            self.run_audit()

    def test_forged_hedge_or_telegram_refused(self):
        self.edit_claim(lambda x: x.update(actual_hedge_recovery_proven=True))
        with self.assertRaises(ValueError):
            self.run_audit()
        self.claim.write_bytes(
            (HERE / "native_field_evidence_r145_exit_emergency_review.json").read_bytes())
        self.edit_claim(lambda x: x.update(telegram_delivery_proven=True))
        with self.assertRaises(ValueError):
            self.run_audit()

    def test_unknown_field_and_repeated_claim_refused(self):
        self.edit_claim(lambda x: x["fields"].append("exit_safety.trade_without_source"))
        with self.assertRaises(ValueError):
            self.run_audit()
        self.claim.write_bytes(
            (HERE / "native_field_evidence_r145_exit_emergency_review.json").read_bytes())
        self.edit_claim(lambda x: x["fields"].append("futures_emergency_stop.retry_n"))
        with self.assertRaises(ValueError):
            self.run_audit()

    def test_parent_double_credit_refused(self):
        self.edit_parent(lambda x: next(item for item in x["fields"]
            if item["field"] == "exit_safety.leg_gap_ms").update(
                research_evidence="TESTED_RESEARCH_READONLY"))
        with self.assertRaises(ValueError):
            self.run_audit()

    def test_false_deployed_proof_refused(self):
        self.edit_parent(lambda x: next(item for item in x["fields"]
            if item["field"] == "exit_safety.enabled").update(
                installed_paper_a342540="PROVEN"))
        with self.assertRaises(ValueError):
            self.run_audit()

    def test_wrong_parent_field_count_refused(self):
        self.edit_parent(lambda x: x.update(total_research_consumed=54))
        with self.assertRaises(ValueError):
            self.run_audit()


if __name__ == "__main__":
    unittest.main()
