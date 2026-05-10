from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from analysis_core.lexicon_io import append_rows, load_csv, save_csv

IDEOLOGY_SEEDS = {
    "sovereignty",
    "hegemony",
    "interference",
    "legitimacy",
    "delegitimization",
    "betrayal",
    "national",
    "order",
    "regime",
    "democracy",
    "rights",
    "security",
    "colonial",
    "crisis",
    "development",
}
EMOTION_SEEDS = {
    "threat": ("fear", "strong", 1.0, "negative"),
    "fear": ("fear", "medium", 0.67, "negative"),
    "catastrophe": ("fear", "strong", 1.0, "negative"),
    "outrage": ("anger", "strong", 1.0, "negative"),
    "hope": ("hope", "medium", 0.67, "positive"),
    "support": ("trust", "weak", 0.33, "positive"),
    "panic": ("fear", "strong", 1.0, "negative"),
}
METAPHOR_SEEDS = {
    "engine": ("economy_as_machine", "machine", "politics/economy", "conventional", "medium"),
    "battle": ("politics_as_war", "war", "politics", "conventional", "strong"),
    "frontline": ("politics_as_war", "war", "politics", "conventional", "strong"),
    "path": ("politics_as_path", "path", "politics", "conventional", "weak"),
    "wave": ("influence_as_flow", "flow", "influence", "conventional", "medium"),
    "storm": ("crisis_as_weather", "weather", "crisis", "conventional", "strong"),
}
TECHNICAL_PATTERNS = [
    "reporting by .* in washington",
    "by .* in washington",
    "reporting from washington",
    "reuters in washington",
]


LEXICON_SCHEMAS: Dict[str, List[str]] = {
    "ideological_markers.csv": [
        "term","lemma","language","category","semantic_zone","polarity_hint","strength_hint","context_dependent","examples","exclude_patterns","source","verified",
    ],
    "emotional_markers.csv": [
        "term","lemma","language","emotion_type","intensity_level","weight","polarity_hint","context_dependent","examples","exclude_patterns","source","verified",
    ],
    "metaphor_markers.csv": [
        "term","lemma","language","metaphor_model","source_domain","target_domain","conventionality","default_strength","context_dependent","examples","exclude_patterns","source","verified",
    ],
    "evi_lexicon.csv": [
        "term","lemma","language","evaluation_type","polarity","strength","category","context_dependent","examples","exclude_patterns","source","verified",
    ],
    "actor_actions.csv": [
        "verb_or_phrase","lemma","language","action_polarity","action_strength","typical_subject","typical_object","examples","verified",
    ],
    "consequence_markers.csv": [
        "term_or_phrase","language","consequence_polarity","consequence_domain","strength","examples","verified",
    ],
    "ideological_frames.csv": [
        "frame_name","frame_type","polarity","keywords","examples","verified",
    ],
    "salience_patterns.csv": [
        "pattern","pattern_type","salience_value","examples","verified",
    ],
    "technical_mention_patterns.csv": [
        "pattern","technical_type","salience_value","examples","verified",
    ],
    "candidate_terms.csv": [
        "candidate_term","lemma","language","proposed_dictionary","proposed_category","frequency","contexts_count","example_contexts","cooccurring_ref_countries","polarity_hint","confidence_score","status","emotion_type","intensity_level","weight","metaphor_model","source_domain","target_domain","conventionality","default_strength","pattern_type","salience_value",
    ],
    "verified_terms.csv": [
        "candidate_term","lemma","language","proposed_dictionary","proposed_category","frequency","contexts_count","example_contexts","cooccurring_ref_countries","polarity_hint","confidence_score","status","emotion_type","intensity_level","weight","metaphor_model","source_domain","target_domain","conventionality","default_strength","pattern_type","salience_value","note",
    ],
    "rejected_terms.csv": [
        "candidate_term","lemma","language","proposed_dictionary","proposed_category","frequency","contexts_count","example_contexts","cooccurring_ref_countries","polarity_hint","confidence_score","status","emotion_type","intensity_level","weight","metaphor_model","source_domain","target_domain","conventionality","default_strength","pattern_type","salience_value","note",
    ],
    "dictionary_change_log.csv": [
        "timestamp","user_action","term","dictionary","old_value","new_value","reason","examples","source",
    ],
}


def ensure_lexicon_files(lexicons_dir: Path) -> None:
    lexicons_dir.mkdir(parents=True, exist_ok=True)
    for name, cols in LEXICON_SCHEMAS.items():
        p = lexicons_dir / name
        if not p.exists():
            pd.DataFrame(columns=cols).to_csv(p, index=False)


def _tok(text: str) -> List[str]:
    return [t.strip(".,:;!?()[]{}\\\"'`").lower() for t in str(text).split() if t.strip()]


def extract_candidate_terms(contexts_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    if contexts_df.empty:
        return pd.DataFrame(columns=LEXICON_SCHEMAS["candidate_terms.csv"])

    # lexical term frequencies by context
    for _, r in contexts_df.iterrows():
        text = str(r.get("context_text", ""))
        ref = str(r.get("ref_country", ""))
        lang = str(r.get("language", "en"))
        tokens = _tok(text)
        low_text = " ".join(tokens)

        for t in sorted(set(tokens)):
            if len(t) < 4:
                continue
            if t in IDEOLOGY_SEEDS:
                rows.append({
                    "candidate_term": t,
                    "lemma": t,
                    "language": lang,
                    "proposed_dictionary": "ideological_markers",
                    "proposed_category": "ideological",
                    "frequency": tokens.count(t),
                    "contexts_count": 1,
                    "example_contexts": text[:280],
                    "cooccurring_ref_countries": ref,
                    "polarity_hint": "mixed",
                    "confidence_score": 0.72,
                    "status": "candidate",
                })
            if t in EMOTION_SEEDS:
                emo, intensity, weight, pol = EMOTION_SEEDS[t]
                rows.append({
                    "candidate_term": t,
                    "lemma": t,
                    "language": lang,
                    "proposed_dictionary": "emotional_markers",
                    "proposed_category": "emotional",
                    "frequency": tokens.count(t),
                    "contexts_count": 1,
                    "example_contexts": text[:280],
                    "cooccurring_ref_countries": ref,
                    "polarity_hint": pol,
                    "confidence_score": 0.75,
                    "status": "candidate",
                    "emotion_type": emo,
                    "intensity_level": intensity,
                    "weight": weight,
                })
            if t in METAPHOR_SEEDS:
                model, src, tgt, conv, strength = METAPHOR_SEEDS[t]
                rows.append({
                    "candidate_term": t,
                    "lemma": t,
                    "language": lang,
                    "proposed_dictionary": "metaphor_markers",
                    "proposed_category": "metaphor",
                    "frequency": tokens.count(t),
                    "contexts_count": 1,
                    "example_contexts": text[:280],
                    "cooccurring_ref_countries": ref,
                    "polarity_hint": "mixed",
                    "confidence_score": 0.74,
                    "status": "candidate",
                    "metaphor_model": model,
                    "source_domain": src,
                    "target_domain": tgt,
                    "conventionality": conv,
                    "default_strength": strength,
                })

        # EVI/action/consequence/frame heuristics
        for t in tokens:
            if t in {"aggression", "violation", "support", "cooperation", "threat", "interference"}:
                rows.append({
                    "candidate_term": t,
                    "lemma": t,
                    "language": lang,
                    "proposed_dictionary": "evi_lexicon",
                    "proposed_category": "evaluation",
                    "frequency": tokens.count(t),
                    "contexts_count": 1,
                    "example_contexts": text[:280],
                    "cooccurring_ref_countries": ref,
                    "polarity_hint": "negative" if t in {"aggression", "violation", "threat", "interference"} else "positive",
                    "confidence_score": 0.70,
                    "status": "candidate",
                })

        # technical mention patterns candidates
        low_full = str(r.get("context_text", "")).lower()
        for pat in TECHNICAL_PATTERNS:
            if pat.replace(".*", "")[:8] in low_full and "washington" in low_full:
                rows.append({
                    "candidate_term": pat,
                    "lemma": pat,
                    "language": lang,
                    "proposed_dictionary": "technical_mention_patterns",
                    "proposed_category": "technical_pattern",
                    "frequency": 1,
                    "contexts_count": 1,
                    "example_contexts": str(r.get("context_text", ""))[:280],
                    "cooccurring_ref_countries": ref,
                    "polarity_hint": "neutral",
                    "confidence_score": 0.8,
                    "status": "candidate",
                    "pattern_type": "technical_mention",
                    "salience_value": 0.0,
                })

    if not rows:
        return pd.DataFrame(columns=LEXICON_SCHEMAS["candidate_terms.csv"])

    df = pd.DataFrame(rows).fillna("")
    group_cols = [c for c in ["candidate_term", "lemma", "language", "proposed_dictionary", "proposed_category", "polarity_hint", "emotion_type", "intensity_level", "weight", "metaphor_model", "source_domain", "target_domain", "conventionality", "default_strength", "pattern_type", "salience_value"] if c in df.columns]
    agg = (
        df.groupby(group_cols, dropna=False, as_index=False)
        .agg(
            frequency=("frequency", "sum"),
            contexts_count=("contexts_count", "sum"),
            example_contexts=("example_contexts", lambda x: " || ".join(pd.Series(x).astype(str).head(3).tolist())),
            cooccurring_ref_countries=("cooccurring_ref_countries", lambda x: ";".join(sorted(set(pd.Series(x).astype(str).tolist())))),
            confidence_score=("confidence_score", "mean"),
            status=("status", "first"),
        )
    )

    # ensure schema columns
    for c in LEXICON_SCHEMAS["candidate_terms.csv"]:
        if c not in agg.columns:
            agg[c] = ""
    return agg[LEXICON_SCHEMAS["candidate_terms.csv"]]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def approve_candidate(lexicons_dir: Path, candidate_term: str, dictionary: str, reason: str = "manual approve", note: str = "") -> None:
    ensure_lexicon_files(lexicons_dir)
    cand = load_csv(lexicons_dir / "candidate_terms.csv")
    if cand.empty:
        return
    row = cand[(cand["candidate_term"].astype(str) == str(candidate_term)) & (cand["proposed_dictionary"].astype(str) == str(dictionary))]
    if row.empty:
        return
    row = row.iloc[[0]].copy()
    row["status"] = "approved"
    row["note"] = note
    append_rows(lexicons_dir / "verified_terms.csv", row.to_dict(orient="records"))

    append_rows(
        lexicons_dir / "dictionary_change_log.csv",
        [{
            "timestamp": _now(),
            "user_action": "approve",
            "term": candidate_term,
            "dictionary": dictionary,
            "old_value": "candidate",
            "new_value": "approved",
            "reason": reason,
            "examples": str(row.iloc[0].get("example_contexts", "")),
            "source": "calibration_corpus",
        }],
    )


def reject_candidate(lexicons_dir: Path, candidate_term: str, dictionary: str, reason: str = "manual reject", note: str = "") -> None:
    ensure_lexicon_files(lexicons_dir)
    cand = load_csv(lexicons_dir / "candidate_terms.csv")
    if cand.empty:
        return
    row = cand[(cand["candidate_term"].astype(str) == str(candidate_term)) & (cand["proposed_dictionary"].astype(str) == str(dictionary))]
    if row.empty:
        return
    row = row.iloc[[0]].copy()
    row["status"] = "rejected"
    row["note"] = note
    append_rows(lexicons_dir / "rejected_terms.csv", row.to_dict(orient="records"))

    append_rows(
        lexicons_dir / "dictionary_change_log.csv",
        [{
            "timestamp": _now(),
            "user_action": "reject",
            "term": candidate_term,
            "dictionary": dictionary,
            "old_value": "candidate",
            "new_value": "rejected",
            "reason": reason,
            "examples": str(row.iloc[0].get("example_contexts", "")),
            "source": "calibration_corpus",
        }],
    )


def apply_verified_terms_to_lexicons(lexicons_dir: Path, target_dict_dir: Path, version_tag: str) -> str:
    ensure_lexicon_files(lexicons_dir)
    verified = load_csv(lexicons_dir / "verified_terms.csv")
    target_dict_dir.mkdir(parents=True, exist_ok=True)

    # map to referent_core dict formats where possible
    ideol_rows = []
    emo_rows = []
    meta_rows = []
    for _, r in verified.iterrows():
        d = str(r.get("proposed_dictionary", ""))
        term = str(r.get("candidate_term", "")).strip()
        if not term:
            continue
        if d == "ideological_markers":
            ideol_rows.append({"marker": term})
        elif d == "emotional_markers":
            emo_rows.append({"marker": term, "intensity": str(r.get("intensity_level", "medium")) or "medium"})
        elif d == "metaphor_markers":
            meta_rows.append({"marker": term})

    if ideol_rows:
        p = target_dict_dir / "ideological_markers.csv"
        old = pd.read_csv(p).fillna("") if p.exists() else pd.DataFrame(columns=["marker"])
        out = pd.concat([old, pd.DataFrame(ideol_rows)], ignore_index=True).drop_duplicates(subset=["marker"])
        out.to_csv(p, index=False)

    if emo_rows:
        p = target_dict_dir / "emotional_markers.csv"
        old = pd.read_csv(p).fillna("") if p.exists() else pd.DataFrame(columns=["marker", "intensity"])
        out = pd.concat([old, pd.DataFrame(emo_rows)], ignore_index=True).drop_duplicates(subset=["marker", "intensity"])
        out.to_csv(p, index=False)

    if meta_rows:
        p = target_dict_dir / "metaphor_candidates.csv"
        old = pd.read_csv(p).fillna("") if p.exists() else pd.DataFrame(columns=["marker"])
        out = pd.concat([old, pd.DataFrame(meta_rows)], ignore_index=True).drop_duplicates(subset=["marker"])
        out.to_csv(p, index=False)

    version = f"lexicon_{version_tag}"
    append_rows(
        lexicons_dir / "dictionary_change_log.csv",
        [{
            "timestamp": _now(),
            "user_action": "reload_lexicons",
            "term": "*",
            "dictionary": "all",
            "old_value": "",
            "new_value": version,
            "reason": "reload verified terms",
            "examples": "",
            "source": "calibration_corpus",
        }],
    )
    return version
