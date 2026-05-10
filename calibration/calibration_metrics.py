from __future__ import annotations

from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd


INDICATORS = ["IDI_raw", "EMI_raw", "MTI_raw", "EVI_raw", "EVI_norm", "IP_context", "IP_abs_context"]


def get_percentile_rank(value: float, distribution_values: Sequence[float]) -> float:
    s = pd.to_numeric(pd.Series(list(distribution_values)), errors="coerce").dropna()
    if s.empty:
        return 0.0
    return float((s <= float(value)).sum() / len(s) * 100.0)


def get_empirical_level(percentile: float) -> str:
    p = float(percentile)
    if p <= 10:
        return "minimal"
    if p <= 20:
        return "very_low"
    if p <= 35:
        return "low"
    if p <= 50:
        return "moderately_low"
    if p <= 65:
        return "medium"
    if p <= 80:
        return "elevated"
    if p <= 90:
        return "high"
    if p <= 97:
        return "very_high"
    return "extreme"


def _stats(s: pd.Series) -> Dict[str, float]:
    v = pd.to_numeric(s, errors="coerce").dropna()
    if v.empty:
        return {k: float("nan") for k in ["min", "max", "mean", "median", "std", "p10", "p25", "p50", "p75", "p90", "p95", "p99"]} | {"count": 0}
    return {
        "count": int(v.size),
        "min": float(v.min()),
        "max": float(v.max()),
        "mean": float(v.mean()),
        "median": float(v.median()),
        "std": float(v.std(ddof=0)),
        "p10": float(v.quantile(0.10)),
        "p25": float(v.quantile(0.25)),
        "p50": float(v.quantile(0.50)),
        "p75": float(v.quantile(0.75)),
        "p90": float(v.quantile(0.90)),
        "p95": float(v.quantile(0.95)),
        "p99": float(v.quantile(0.99)),
    }


def build_distributions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["group_scope", "group_value", "metric", "count", "min", "max", "mean", "median", "std", "p10", "p25", "p50", "p75", "p90", "p95", "p99"])

    scopes: List[Tuple[str, List[str]]] = [
        ("full_calibration_corpus", []),
        ("by_calibration_type", ["calibration_type"]),
        ("by_expected_indicator_focus", ["expected_indicator_focus"]),
        ("by_language", ["language"]),
        ("by_ref_country", ["ref_country"]),
        ("by_calibration_type_language", ["calibration_type", "language"]),
        ("by_calibration_type_ref_country", ["calibration_type", "ref_country"]),
    ]

    rows: List[Dict[str, object]] = []
    for scope, cols in scopes:
        if not cols:
            groups = [("all", df)]
        else:
            cols_ok = [c for c in cols if c in df.columns]
            if len(cols_ok) != len(cols):
                continue
            groups = []
            for key, part in df.groupby(cols_ok, dropna=False):
                if not isinstance(key, tuple):
                    key = (key,)
                label = "|".join([f"{c}={v}" for c, v in zip(cols_ok, key)])
                groups.append((label, part))
        for gval, part in groups:
            for metric in INDICATORS:
                if metric not in part.columns:
                    continue
                row: Dict[str, object] = {"group_scope": scope, "group_value": gval, "metric": metric}
                row.update(_stats(part[metric]))
                rows.append(row)
    return pd.DataFrame(rows)


def add_percentiles(df: pd.DataFrame, baseline_df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    mapping = {
        "IDI_raw": "IDI",
        "EMI_raw": "EMI",
        "MTI_raw": "MTI",
        "IP_context": "IP",
        "IP_abs_context": "IP_abs",
    }
    for raw_col, name in mapping.items():
        values = pd.to_numeric(baseline_df.get(raw_col, pd.Series(dtype=float)), errors="coerce").dropna().tolist()
        pcol = f"{name}_percentile"
        lcol = f"{name}_empirical_level"
        raw_vals = out.get(raw_col, pd.Series([0.0] * len(out), index=out.index))
        if not isinstance(raw_vals, pd.Series):
            raw_vals = pd.Series([raw_vals] * len(out), index=out.index)
        out[pcol] = pd.to_numeric(raw_vals, errors="coerce").fillna(0.0).apply(lambda x: get_percentile_rank(float(x), values))
        out[lcol] = out[pcol].apply(get_empirical_level)
    return out
