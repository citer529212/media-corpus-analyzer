import tempfile
import unittest
from pathlib import Path

import pandas as pd

import media_analyzer_referent as rc


class MarkerSystemTests(unittest.TestCase):
    def _mk_context(self, text: str, ref: str = "China") -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "context_id": "ctx_001",
                    "doc_id": "doc_1",
                    "media_country": "Malaysia",
                    "outlet_name": "TestOutlet",
                    "date": "2026-01-01",
                    "title": "Test title",
                    "ref_country": ref,
                    "matched_keywords": ref,
                    "context_text": text,
                    "previous_sentence": "",
                    "target_sentence": text,
                    "next_sentence": "",
                }
            ]
        )

    def test_idi_marker_detection(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            rc.ensure_default_dictionaries(d)
            ctx = self._mk_context("China defends national sovereignty against foreign interference.")
            scored, traces = rc.apply_metrics(ctx, d, "suggested", None, None, return_traces=True)
            self.assertGreaterEqual(int(scored.iloc[0]["N_ideol"]), 1)
            idi_tr = traces[traces["indicator"] == "IDI"]
            self.assertFalse(idi_tr.empty)

    def test_emi_marker_detection(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            rc.ensure_default_dictionaries(d)
            ctx = self._mk_context("The US pressure raised serious concerns.", ref="USA")
            scored, traces = rc.apply_metrics(ctx, d, "suggested", None, None, return_traces=True)
            self.assertGreaterEqual(int(scored.iloc[0]["N_e_w"] + scored.iloc[0]["N_e_m"] + scored.iloc[0]["N_e_s"]), 1)
            emi_tr = traces[traces["indicator"] == "EMI"]
            self.assertFalse(emi_tr.empty)

    def test_mti_marker_detection(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            rc.ensure_default_dictionaries(d)
            ctx = self._mk_context("China is the engine of regional growth.")
            scored, traces = rc.apply_metrics(ctx, d, "suggested", None, None, return_traces=True)
            mti_tr = traces[(traces["indicator"] == "MTI") & (traces["term_found"].str.contains("engine", case=False, na=False))]
            self.assertFalse(mti_tr.empty)

    def test_evi_rubric_values(self):
        row = {
            "context_text": "A reliable partner supports development and stability.",
            "target_sentence": "A reliable partner supports development and stability.",
            "title": "Partnership and growth",
        }
        out = rc.calc_evi_rubric("China", pd.Series(row))
        self.assertTrue(-10 <= int(out["evi_raw"]) <= 10)
        self.assertAlmostEqual(float(out["evi_norm"]), float(out["evi_raw"]) / 10.0, places=6)

    def test_technical_mention_salience(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            rc.ensure_default_dictionaries(d)
            ctx = self._mk_context("Reporting by John Smith in Washington.", ref="USA")
            scored, traces = rc.apply_metrics(ctx, d, "suggested", None, None, return_traces=True)
            self.assertEqual(float(scored.iloc[0]["referent_salience"]), 0.0)
            self.assertTrue(bool(scored.iloc[0]["is_technical_mention"]))
            self.assertTrue(((traces["indicator"] == "S_r") & (traces["context_id"] == "ctx_001")).any())

    def test_n_content(self):
        txt = "The state strongly supports regional development in Asia."
        n = rc.compute_n_content(txt)
        self.assertGreaterEqual(n, 4)

    def test_marker_trace_exists_for_counted_markers(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            rc.ensure_default_dictionaries(d)
            ctx = self._mk_context("China faces threat and pressure in a political battle.")
            scored, traces = rc.apply_metrics(ctx, d, "suggested", None, None, return_traces=True)
            counted = int(scored.iloc[0]["N_ideol"] + scored.iloc[0]["N_e_w"] + scored.iloc[0]["N_e_m"] + scored.iloc[0]["N_e_s"])
            self.assertGreaterEqual(len(traces), 1)
            self.assertGreaterEqual(counted, 1)

    def test_lexicon_validation_flags_bad_weight(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            rc.ensure_default_dictionaries(d)
            p = d / "emotional_markers.csv"
            df = pd.read_csv(p).fillna("")
            if not df.empty:
                df.loc[df.index[0], "intensity_level"] = "strong"
                df.loc[df.index[0], "weight"] = 0.2
                df.to_csv(p, index=False)
            qdf, _ = rc.validate_lexicons(d)
            self.assertTrue((qdf["issue"] == "weight_mismatch").any())

    def test_dictionary_approval_workflow(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            rc.ensure_default_dictionaries(d)
            cand = pd.DataFrame(
                [
                    {
                        "candidate_id": "cand_000001",
                        "candidate_term": "test_hegemony_term",
                        "lemma": "test_hegemony_term",
                        "language": "en",
                        "proposed_dictionary": "ideological_markers",
                        "proposed_category": "hegemony",
                        "frequency": 9,
                        "contexts_count": 4,
                        "example_contexts": "",
                        "cooccurring_ref_countries": "China",
                        "polarity_hint": "negative",
                        "confidence_score": 0.9,
                        "status": "new",
                    }
                ]
            )
            cand.to_csv(d / "candidate_terms.csv", index=False)
            out = rc.approve_candidate("cand_000001", d)
            self.assertIsNotNone(out)
            ideol = pd.read_csv(d / "ideological_markers.csv").fillna("")
            term_col = "term" if "term" in ideol.columns else "marker"
            self.assertTrue((ideol[term_col].astype(str).str.casefold() == "test_hegemony_term").any())
            log = pd.read_csv(d / "dictionary_change_log.csv").fillna("")
            self.assertTrue((log["action"].astype(str) == "approve_candidate").any())


if __name__ == "__main__":
    unittest.main()
