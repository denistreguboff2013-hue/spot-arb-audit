"""R164 offline integrity regression; all mutations are IN MEMORY."""
import json
import unittest
from pathlib import Path
import yaml

import native_field_r164_crosswalk as check

class R164SourceIntegrity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = [
            check.SOURCE.read_bytes(),check.BASELINE.read_bytes(),
            check.SCHEMA.read_bytes(),check.R148.read_bytes(),
            check.R149.read_bytes(),check.CSV.read_bytes(),
            check.OVERLAY.read_bytes()
        ]
    def verify(self,values=None):
        return check.verify(*(self.original if values is None else values),
                            evidence_dir=check.HERE)
    def replacement(self,index,func,json_format=True):
        args=list(self.original)
        val=json.loads(args[index]) if json_format else yaml.safe_load(args[index])
        func(val)
        args[index]=(json.dumps(val) if json_format
             else yaml.safe_dump(val,sort_keys=False)).encode()
        return args
    def test_160_schema_values_and_evidence_scope(self):
        value=self.verify()
        self.assertTrue(value["original_160_paths_identical_in_both_yaml_and_schema"])
        self.assertEqual(value["research_supported_claims"],107)
        self.assertEqual(value["research_unproven"],53)
        self.assertEqual(value["installed_paper_fields_proven"],0)
        self.assertFalse(value["real_trading_authorized"])
        self.assertEqual(value["baseline_value_changed_paths"],[
            "general.paper_entries_enabled","general.trade_enabled",
            "real_trial.autonomous_real_enabled","real_trial.enabled"])
        self.assertIn("spread.history_window",value["research_unproven_paths"])
    def test_source_missing_path_denied(self):
        args=self.replacement(0,lambda a:a["spread"].pop("history_window"),
                              json_format=False)
        with self.assertRaises(ValueError):self.verify(args)
    def test_source_bool_type_denied(self):
        args=self.replacement(0,lambda a:a["analytics"].update(enabled=1),
                              json_format=False)
        with self.assertRaises(ValueError):self.verify(args)
    def test_baseline_extra_path_denied(self):
        args=self.replacement(1,lambda a:a["limits"].update(forbidden=123),
                              json_format=False)
        with self.assertRaises(ValueError):self.verify(args)
    def test_r149_fake_installation_denied(self):
        args=self.replacement(4,lambda a:a["fields"][0].update(
            installed_paper_a342540="PROVEN"))
        with self.assertRaises(ValueError):self.verify(args)
    def test_r149_false_field_credit_denied(self):
        def edit(a):
            item=next(x for x in a["fields"] if x["field"]=="spread.history_window")
            item["research_evidence"]="TESTED_RESEARCH_READONLY"
            item["research_sources"]="R130_BASELINE"
        args=self.replacement(4,edit)
        with self.assertRaises(ValueError):self.verify(args)
    def test_r148_chain_mutation_denied(self):
        args=self.replacement(3,lambda a:a.update(total_research_consumed=99))
        with self.assertRaises(ValueError):self.verify(args)
    def test_r149_csv_and_json_mismatch_denied(self):
        args=list(self.original)
        args[5]=args[5].replace(b"adaptive_entry.enabled",b"forged_field.enabled",1)
        with self.assertRaises(ValueError):self.verify(args)
    def test_overlay_tamper_denied(self):
        args=self.replacement(6,lambda a:a["fields"].append("spread.history_window"))
        with self.assertRaises(ValueError):self.verify(args)
    def test_source_native_real_flag_denied(self):
        args=self.replacement(0,lambda a:a["native_runtime"].update(
            real_execution_authorized=True),json_format=False)
        with self.assertRaises(ValueError):self.verify(args)

if __name__=="__main__":
    unittest.main()
