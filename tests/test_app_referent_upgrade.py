import tempfile
import unittest
from pathlib import Path

import pandas as pd

import corpus_analyzer_webapp.app as app_mod


class AppReferentUpgradeTests(unittest.TestCase):
    def test_small_raw_values_not_zero(self):
        df = pd.DataFrame(
            {
                "IDI_raw": [0.000432],
                "EMI_raw": [0.001203],
                "MTI_raw": [0.000904],
                "media_country": ["Malaysia"],
                "ref_country": ["USA"],
            }
        )
        out = app_mod._assign_percentiles(df, "IDI_raw", "IDI_percentile", "full corpus")
        self.assertAlmostEqual(float(out.iloc[0]["IDI_raw"]), 0.000432, places=6)
        self.assertGreaterEqual(float(out.iloc[0]["IDI_percentile"]), 0.0)

    def test_updated_ip_formula_context(self):
        idi, emi, mti, evi_norm = 0.001, 0.002, 0.003, 0.8
        disc = idi + emi + mti
        ip_i = evi_norm * (1.0 + disc)
        self.assertAlmostEqual(ip_i, 0.8048, places=6)

    def test_weighted_aggregation_example(self):
        ip = pd.Series([1.0, -0.5])
        w = pd.Series([1.0, 0.5])
        ip_final = float((ip * w).sum() / w.sum())
        ip_abs_final = float((ip.abs() * w).sum() / w.sum())
        self.assertAlmostEqual(ip_final, 0.5, places=6)
        self.assertAlmostEqual(ip_abs_final, 0.8333333333, places=6)

    def test_percentile_rank_example(self):
        df = pd.DataFrame(
            {
                "IDI_raw": [0.001, 0.002, 0.003, 0.004, 0.005],
                "media_country": ["M"] * 5,
                "ref_country": ["USA"] * 5,
            }
        )
        out = app_mod._assign_percentiles(df, "IDI_raw", "IDI_percentile", "full corpus")
        p = float(out.loc[out["IDI_raw"] == 0.004, "IDI_percentile"].iloc[0])
        self.assertGreaterEqual(p, 79.0)
        self.assertLessEqual(p, 81.0)

    def test_calibration_report(self):
        cal = pd.DataFrame(
            [
                {"calibration_id": "c1", "text": "Partner cooperation and stability.", "calibration_type": "neutral_news"},
                {"calibration_id": "c2", "text": "Aggression and threat create crisis.", "calibration_type": "crisis_text"},
            ]
        )
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            app_mod.referent_core.ensure_default_dictionaries(d)
            det, rep = app_mod._compute_calibration_report(cal, d, "updated: EVI_norm * (1 + IDI + EMI + MTI)")
            self.assertEqual(len(det), 2)
            self.assertIn("mean_IP_abs", rep.columns)


if __name__ == "__main__":
    unittest.main()

