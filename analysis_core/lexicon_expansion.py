from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

DEFAULT_COLUMNS = {
    "candidate_terms.csv": [
        "candidate_id", "candidate_term", "lemma", "language", "proposed_dictionary", "proposed_category",
        "frequency", "contexts_count", "example_contexts", "cooccurring_ref_countries", "polarity_hint",
        "confidence_score", "status",
    ],
    "verified_terms.csv": [
        "candidate_id", "term", "lemma", "language", "dictionary", "category", "approved_at", "notes",
    ],
    "rejected_terms.csv": [
        "candidate_id", "term", "lemma", "language", "proposed_dictionary", "reason", "rejected_at",
    ],
    "dictionary_change_log.csv": [
        "timestamp", "action", "term", "lemma", "dictionary", "category", "status", "details",
    ],
}


def _ensure_csv(path: Path, columns: List[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=columns).to_csv(path, index=False)


def ensure_lexicon_workflow_files(lex_dir: Path) -> None:
    lex_dir.mkdir(parents=True, exist_ok=True)
    for fname, cols in DEFAULT_COLUMNS.items():
        _ensure_csv(lex_dir / fname, cols)


def extract_candidate_terms(calibration_contexts: pd.DataFrame, lex_dir: Path) -> pd.DataFrame:
    ensure_lexicon_workflow_files(lex_dir)
    rows = []
    if calibration_contexts is None or calibration_contexts.empty:
        out = pd.DataFrame(columns=DEFAULT_COLUMNS["candidate_terms.csv"])
        out.to_csv(lex_dir / "candidate_terms.csv", index=False)
        return out

    token_bag: Dict[str, int] = {}
    for txt in calibration_contexts.get("context_text", pd.Series(dtype=str)).fillna(""):
        for raw in str(txt).split():
            t = raw.strip(".,;:!?()[]{}\"'").casefold()
            if len(t) < 4:
                continue
            token_bag[t] = token_bag.get(t, 0) + 1

    i = 0
    for term, freq in sorted(token_bag.items(), key=lambda kv: kv[1], reverse=True):
        if freq < 3:
            continue
        i += 1
        rows.append(
            {
                "candidate_id": f"cand_{i:06d}",
                "candidate_term": term,
                "lemma": term,
                "language": "mixed",
                "proposed_dictionary": suggest_dictionary(term),
                "proposed_category": "auto",
                "frequency": int(freq),
                "contexts_count": int(freq),
                "example_contexts": "",
                "cooccurring_ref_countries": "",
                "polarity_hint": "",
                "confidence_score": round(score_candidate_term(term, freq), 4),
                "status": "new",
            }
        )
    out = pd.DataFrame(rows, columns=DEFAULT_COLUMNS["candidate_terms.csv"])
    out.to_csv(lex_dir / "candidate_terms.csv", index=False)
    return out


def score_candidate_term(term: str, frequency: int = 1) -> float:
    t = str(term).casefold()
    base = min(1.0, max(0.0, frequency / 50.0))
    if any(k in t for k in ["threat", "aggress", "cooper", "sovereign", "hegem", "stability", "crisis"]):
        base += 0.2
    if len(t) > 20:
        base -= 0.05
    return max(0.0, min(1.0, base))


def suggest_dictionary(term: str) -> str:
    t = str(term).casefold()
    if any(k in t for k in ["threat", "sovereign", "hegem", "democr", "regime", "law", "order"]):
        return "ideological_markers"
    if any(k in t for k in ["fear", "anger", "outrage", "concern", "support", "crisis"]):
        return "emotional_markers"
    if any(k in t for k in ["engine", "front", "wave", "path", "bridge", "chess", "battle"]):
        return "metaphor_markers"
    return "evi_lexicon"


def _append_row(path: Path, row: Dict[str, object], columns: List[str]) -> None:
    _ensure_csv(path, columns)
    df = pd.read_csv(path).fillna("")
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(path, index=False)


def write_dictionary_change_log(lex_dir: Path, action: str, term: str, lemma: str = "", dictionary: str = "", category: str = "", status: str = "", details: str = "") -> None:
    ensure_lexicon_workflow_files(lex_dir)
    row = {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "action": action,
        "term": term,
        "lemma": lemma,
        "dictionary": dictionary,
        "category": category,
        "status": status,
        "details": details,
    }
    _append_row(lex_dir / "dictionary_change_log.csv", row, DEFAULT_COLUMNS["dictionary_change_log.csv"])


def approve_candidate(candidate_id: str, lex_dir: Path) -> Optional[Dict[str, object]]:
    ensure_lexicon_workflow_files(lex_dir)
    cpath = lex_dir / "candidate_terms.csv"
    cdf = pd.read_csv(cpath).fillna("")
    m = cdf[cdf["candidate_id"].astype(str) == str(candidate_id)]
    if m.empty:
        return None
    row = m.iloc[0].to_dict()
    term = str(row.get("candidate_term", "")).strip()
    lemma = str(row.get("lemma", term)).strip() or term
    dictionary = str(row.get("proposed_dictionary", "evi_lexicon")).strip() or "evi_lexicon"
    category = str(row.get("proposed_category", "auto")).strip() or "auto"

    # verified ledger
    vrow = {
        "candidate_id": candidate_id,
        "term": term,
        "lemma": lemma,
        "language": str(row.get("language", "mixed")),
        "dictionary": dictionary,
        "category": category,
        "approved_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "notes": "approved_from_candidate",
    }
    _append_row(lex_dir / "verified_terms.csv", vrow, DEFAULT_COLUMNS["verified_terms.csv"])

    # append to destination dictionary (best-effort schema)
    dst = lex_dir / f"{dictionary}.csv"
    if dst.exists():
        ddf = pd.read_csv(dst).fillna("")
    else:
        ddf = pd.DataFrame()
    if "term" in ddf.columns:
        col_term = "term"
    elif "marker" in ddf.columns:
        col_term = "marker"
    else:
        col_term = "term"
        ddf[col_term] = ""
    if "lemma" not in ddf.columns:
        ddf["lemma"] = ""
    if col_term not in ddf.columns:
        ddf[col_term] = ""

    already = set(ddf[col_term].astype(str).str.casefold().tolist())
    if term.casefold() not in already:
        new_row = {c: "" for c in ddf.columns}
        new_row[col_term] = term
        new_row["lemma"] = lemma
        if "verified" in ddf.columns:
            new_row["verified"] = True
        ddf = pd.concat([ddf, pd.DataFrame([new_row])], ignore_index=True)
        ddf.to_csv(dst, index=False)

    cdf.loc[cdf["candidate_id"].astype(str) == str(candidate_id), "status"] = "approved"
    cdf.to_csv(cpath, index=False)
    write_dictionary_change_log(lex_dir, "approve_candidate", term, lemma, dictionary, category, "approved", "candidate promoted to lexicon")
    return row


def reject_candidate(candidate_id: str, lex_dir: Path, reason: str = "not_relevant") -> Optional[Dict[str, object]]:
    ensure_lexicon_workflow_files(lex_dir)
    cpath = lex_dir / "candidate_terms.csv"
    cdf = pd.read_csv(cpath).fillna("")
    m = cdf[cdf["candidate_id"].astype(str) == str(candidate_id)]
    if m.empty:
        return None
    row = m.iloc[0].to_dict()
    term = str(row.get("candidate_term", "")).strip()
    rrow = {
        "candidate_id": candidate_id,
        "term": term,
        "lemma": str(row.get("lemma", term)),
        "language": str(row.get("language", "mixed")),
        "proposed_dictionary": str(row.get("proposed_dictionary", "")),
        "reason": reason,
        "rejected_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    _append_row(lex_dir / "rejected_terms.csv", rrow, DEFAULT_COLUMNS["rejected_terms.csv"])
    cdf.loc[cdf["candidate_id"].astype(str) == str(candidate_id), "status"] = "rejected"
    cdf.to_csv(cpath, index=False)
    write_dictionary_change_log(lex_dir, "reject_candidate", term, str(row.get("lemma", term)), str(row.get("proposed_dictionary", "")), str(row.get("proposed_category", "")), "rejected", reason)
    return row


def mark_context_dependent(term_id: str, lex_dir: Path) -> None:
    ensure_lexicon_workflow_files(lex_dir)
    cpath = lex_dir / "candidate_terms.csv"
    cdf = pd.read_csv(cpath).fillna("")
    mask = cdf["candidate_id"].astype(str) == str(term_id)
    if not mask.any():
        return
    cdf.loc[mask, "status"] = "context_dependent"
    cdf.to_csv(cpath, index=False)
    t = str(cdf.loc[mask, "candidate_term"].iloc[0])
    write_dictionary_change_log(lex_dir, "mark_context_dependent", t, dictionary="candidate_terms", status="context_dependent", details="requires manual context verification")
