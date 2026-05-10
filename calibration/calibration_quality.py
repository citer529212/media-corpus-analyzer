from __future__ import annotations

from difflib import SequenceMatcher
from typing import Dict, List

import pandas as pd


CORE_CATEGORIES = [
    "neutral_news",
    "standard_political_news",
    "analytical_article",
    "crisis_report",
    "opinion_editorial",
    "ideological_mobilization",
]


def _near_duplicates(texts: List[str], jac_thr: float = 0.92, seq_thr: float = 0.98) -> List[int]:
    token_sets = [set(t.lower().split()) for t in texts]
    idxs: List[int] = []
    n = len(texts)
    for i in range(n):
        for j in range(i + 1, n):
            a = token_sets[i]
            b = token_sets[j]
            if not a or not b:
                continue
            jac = len(a & b) / max(1, len(a | b))
            if jac >= jac_thr or SequenceMatcher(None, texts[i], texts[j]).ratio() >= seq_thr:
                idxs.append(j)
    return sorted(set(idxs))


def build_quality_flags(texts_df: pd.DataFrame, contexts_df: pd.DataFrame) -> pd.DataFrame:
    flags: List[Dict[str, str]] = []
    if texts_df.empty:
        return pd.DataFrame(columns=["calibration_id", "issue_type", "severity", "explanation", "recommended_action"])

    for _, r in texts_df.iterrows():
        cid = str(r.get("calibration_id", ""))
        txt = str(r.get("text", ""))
        words = int(r.get("text_length_words", 0) or 0)
        status = str(r.get("fetch_status", "ok"))
        if words < 100 and txt.strip():
            flags.append({"calibration_id": cid, "issue_type": "too_short", "severity": "medium", "explanation": "text_length_words < 100", "recommended_action": "use longer text"})
        if status != "ok":
            flags.append({"calibration_id": cid, "issue_type": "fetch_failed", "severity": "high", "explanation": "fetch_status != ok", "recommended_action": "replace source or use local text"})
        if not txt.strip():
            flags.append({"calibration_id": cid, "issue_type": "missing_text", "severity": "high", "explanation": "text is empty", "recommended_action": "provide text content"})

    dup_mask = texts_df["text"].fillna("").astype(str).str.strip().duplicated(keep="first") if "text" in texts_df.columns else pd.Series(dtype=bool)
    if not dup_mask.empty:
        for idx in texts_df[dup_mask].index.tolist():
            flags.append({"calibration_id": str(texts_df.loc[idx, "calibration_id"]), "issue_type": "duplicate", "severity": "medium", "explanation": "exact duplicate text", "recommended_action": "remove duplicate"})

    if "text" in texts_df.columns:
        near_ids = _near_duplicates(texts_df["text"].fillna("").astype(str).tolist())
        for idx in near_ids:
            flags.append({"calibration_id": str(texts_df.iloc[idx]["calibration_id"]), "issue_type": "near_duplicate", "severity": "low", "explanation": "near duplicate text", "recommended_action": "review and deduplicate"})

    # category imbalance and minimums
    counts = texts_df["calibration_type"].value_counts(dropna=False).to_dict() if "calibration_type" in texts_df.columns else {}
    if counts:
        max_c = max(counts.values())
        min_c = min(counts.values())
        if min_c > 0 and max_c >= 3 * min_c:
            flags.append({"calibration_id": "GLOBAL", "issue_type": "category_imbalance", "severity": "medium", "explanation": f"max={max_c}, min={min_c}", "recommended_action": "balance categories"})

    for c in CORE_CATEGORIES:
        if counts.get(c, 0) < 5:
            flags.append({"calibration_id": "GLOBAL", "issue_type": "category_imbalance", "severity": "medium", "explanation": f"core category '{c}' has <5 texts", "recommended_action": "add at least 5 texts"})

    # salience warnings
    if not contexts_df.empty and "S_r" in contexts_df.columns:
        ratio_zero = float((pd.to_numeric(contexts_df["S_r"], errors="coerce").fillna(0.0) == 0.0).mean())
        if ratio_zero > 0.6:
            flags.append({"calibration_id": "GLOBAL", "issue_type": "too_many_technical_mentions", "severity": "medium", "explanation": f"S_r=0 share={ratio_zero:.2f}", "recommended_action": "add texts with explicit referent evaluation"})

    # no ref country detection
    if "ref_country" in texts_df.columns:
        miss = texts_df[texts_df["ref_country"].fillna("").astype(str).str.strip() == ""]
        for _, r in miss.iterrows():
            flags.append({"calibration_id": str(r.get("calibration_id", "")), "issue_type": "no_ref_country_detected", "severity": "medium", "explanation": "ref_country is empty", "recommended_action": "provide ref_country or text with referent mention"})

    # outlier rule: >p99 and >3*median (median>0)
    for metric in ["IDI_raw", "EMI_raw", "MTI_raw", "IP_abs_context"]:
        if metric not in contexts_df.columns or contexts_df.empty:
            continue
        s = pd.to_numeric(contexts_df[metric], errors="coerce").dropna()
        if s.empty:
            continue
        p99 = float(s.quantile(0.99))
        med = float(s.median())
        mask = s > p99
        if med > 0:
            mask = mask & (s > 3.0 * med)
        flagged_idx = s[mask].index.tolist()
        for idx in flagged_idx:
            cid = str(contexts_df.loc[idx, "calibration_id"]) if "calibration_id" in contexts_df.columns else ""
            flags.append({"calibration_id": cid, "issue_type": "outlier", "severity": "medium", "explanation": f"{metric} > p99 and >3*median", "recommended_action": "manual review only"})

    # quality gate summary
    if len(texts_df) < 50:
        flags.append({"calibration_id": "GLOBAL", "issue_type": "missing_metadata", "severity": "high", "explanation": "total_texts < 50", "recommended_action": "increase corpus to 50+"})

    return pd.DataFrame(flags)
