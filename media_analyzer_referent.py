#!/usr/bin/env python3
"""Referent-context media analyzer for dissertation methodology.

Core principles implemented:
1) The full corpus is only a source base.
2) Metrics are computed on referent-bound context subcorpora (China/USA/Russia).
3) One document can contribute to multiple referents independently.
4) EVI is always estimated per referent, never globally.
5) Outputs preserve full traceability for manual verification.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd


REQUIRED_FIELDS = ["doc_id", "media_country", "outlet_name", "date", "title", "text"]
REF_COUNTRIES = ["China", "USA", "Russia"]
EVI_ALLOWED = {-2, -1, 0, 1, 2}

TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁёІіЇїЄє'\-]+")
SENT_SPLIT_RE = re.compile(r"(?<=[\.\!\?])\s+")

STOPWORDS = {
    # English
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "at", "by", "from", "is", "are", "was",
    "were", "be", "been", "being", "as", "that", "this", "these", "those", "it", "its", "their", "his", "her",
    "they", "them", "he", "she", "we", "you", "i", "not", "but", "if", "then", "than", "into", "about", "after",
    "before", "during", "up", "down", "out", "over", "have", "has", "had", "do", "does", "did", "can", "could",
    "will", "would", "may", "might", "must", "should", "am", "is", "are", "was", "were",
    # Indonesian/Malay
    "dan", "atau", "yang", "di", "ke", "dari", "untuk", "dengan", "pada", "adalah", "itu", "ini", "dalam", "oleh",
    "sebagai", "juga", "akan", "telah", "lebih", "karena", "serta", "tidak", "bagi", "antara", "agar", "namun",
    "ia", "mereka", "kami", "kita", "saya", "anda", "usai", "jadi", "sebut", "kata", "menurut", "tersebut", "hingga",
    "kepada", "dapat", "boleh", "perlu", "harus", "masih", "atas", "setelah", "sebelum", "ketika", "bukan",
    # Russian minimal functional set
    "и", "а", "но", "или", "в", "во", "на", "по", "с", "со", "к", "ко", "из", "за", "для", "о", "об", "от", "до",
    "это", "как", "что", "бы", "же", "ли", "не", "ни", "он", "она", "они", "мы", "вы", "я", "его", "ее", "их",
}

DEFAULT_REF_KEYWORDS = {
    "China": [
        "China", "Chinese", "Beijing", "Xi Jinping", "Communist Party of China", "CPC", "CCP", "PRC", "yuan",
        "Belt and Road", "BRI", "Chinese economy", "Chinese culture", "Huawei", "Taiwan", "Tiongkok", "Cina",
        "South China Sea", "Xinjiang", "Hong Kong",
        "Китай", "китайский", "Пекин", "Си Цзиньпин", "КПК", "КНР", "китайская экономика", "китайская культура",
    ],
    "USA": [
        "United States", "US", "U.S.", "America", "American", "Washington", "White House", "Biden", "Trump",
        "dollar", "Pentagon", "Congress", "NATO", "Amerika Serikat", "Amerika Syarikat",
        "США", "Соединенные Штаты", "Америка", "американский", "Вашингтон", "Белый дом", "Пентагон", "Конгресс",
    ],
    "Russia": [
        "Russia", "Russian", "Moscow", "Kremlin", "Putin", "ruble", "Russian economy", "Russian military",
        "Ukraine war", "sanctions against Russia", "Rusia",
        "Россия", "российский", "Москва", "Кремль", "Путин", "рубль", "российская экономика", "санкции против России",
    ],
}

EVAL_POS = {
    "support", "stability", "cooperation", "progress", "benefit", "peace", "growth", "successful", "heroic",
    "defends sovereignty", "protects national interests",
}
EVAL_NEG = {
    "threat", "aggression", "catastrophe", "disaster", "collapse", "betrayal", "violates international law",
    "dictatorship", "hegemony", "regime", "authoritarian", "tension", "conflict", "sanctions",
}


@dataclass
class ContextRow:
    context_id: str
    doc_id: str
    media_country: str
    outlet_name: str
    date: str
    title: str
    ref_country: str
    matched_keywords: str
    context_text: str
    previous_sentence: str
    target_sentence: str
    next_sentence: str


def normalize_token(s: str) -> str:
    return s.casefold().strip("-'_ ")


def tokenize(text: str) -> List[str]:
    return [normalize_token(t) for t in TOKEN_RE.findall(text) if normalize_token(t)]


def is_content_token(tok: str) -> bool:
    if not tok or tok in STOPWORDS:
        return False
    if tok.isdigit() or len(tok) < 2:
        return False
    return True


def load_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    elif ext == ".json":
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # best effort: dictionary of rows
            df = pd.DataFrame(data.get("rows", data))
        else:
            raise ValueError("Unsupported JSON structure")
    else:
        raise ValueError(f"Unsupported input format: {ext}")
    return df


def ensure_required_fields(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_FIELDS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    out = df.copy()
    out["doc_id"] = out["doc_id"].astype(str)
    for c in ["media_country", "outlet_name", "date", "title", "text"]:
        out[c] = out[c].fillna("").astype(str)
    return out


def ensure_default_dictionaries(dict_dir: Path) -> None:
    dict_dir.mkdir(parents=True, exist_ok=True)

    p_ideo = dict_dir / "ideological_markers.csv"
    if not p_ideo.exists():
        markers = [
            "democracy", "sovereignty", "freedom", "authoritarianism", "communism", "imperialism", "colonialism",
            "neocolonialism", "multipolarity", "rules-based order", "regime", "dictatorship", "hegemony",
            "communist party", "liberal order", "authoritarian state", "violates international law",
            "defends sovereignty", "threatens regional stability", "protects national interests",
        ]
        pd.DataFrame({"marker": markers}).to_csv(p_ideo, index=False)

    p_emot = dict_dir / "emotional_markers.csv"
    if not p_emot.exists():
        rows = []
        rows += [{"marker": m, "intensity": "weak"} for m in ["concern", "problem", "support", "challenge", "tension"]]
        rows += [{"marker": m, "intensity": "medium"} for m in ["threat", "anger", "fear", "pressure", "conflict", "criticism"]]
        rows += [{"marker": m, "intensity": "strong"} for m in ["catastrophe", "betrayal", "aggression", "triumph", "heroic", "disaster", "collapse"]]
        pd.DataFrame(rows).to_csv(p_emot, index=False)

    p_meta = dict_dir / "metaphor_candidates.csv"
    if not p_meta.exists():
        rows = [
            ("battle", "war"), ("fight", "war"), ("attack", "war"), ("defense", "war"), ("frontline", "war"),
            ("healthy economy", "organism"), ("sick system", "organism"), ("recovery", "organism"), ("virus", "organism"),
            ("growth path", "movement"), ("wave", "movement"), ("flow", "movement"), ("collapse", "movement"),
            ("player", "game"), ("move", "game"), ("strategy", "game"), ("chessboard", "game"),
            ("wants", "personification"), ("fears", "personification"), ("pressures", "personification"),
        ]
        pd.DataFrame(rows, columns=["marker", "type"]).to_csv(p_meta, index=False)


def load_ref_keywords(dict_dir: Path) -> Dict[str, List[str]]:
    p = dict_dir / "ref_keywords.csv"
    if not p.exists():
        rows = []
        for country, kws in DEFAULT_REF_KEYWORDS.items():
            rows.extend([{"ref_country": country, "keyword": kw} for kw in kws])
        pd.DataFrame(rows).to_csv(p, index=False)
        return {k: list(v) for k, v in DEFAULT_REF_KEYWORDS.items()}

    df = pd.read_csv(p).fillna("")
    out: Dict[str, List[str]] = {c: [] for c in REF_COUNTRIES}
    for _, r in df.iterrows():
        c = str(r.get("ref_country", "")).strip()
        kw = str(r.get("keyword", "")).strip()
        if c in out and kw:
            out[c].append(kw)
    for c in REF_COUNTRIES:
        if not out[c]:
            out[c] = list(DEFAULT_REF_KEYWORDS[c])
    return out


def load_ideological_markers(dict_dir: Path) -> List[str]:
    df = pd.read_csv(dict_dir / "ideological_markers.csv").fillna("")
    return [str(x).strip() for x in df["marker"].tolist() if str(x).strip()]


def load_emotional_markers(dict_dir: Path) -> Dict[str, List[str]]:
    df = pd.read_csv(dict_dir / "emotional_markers.csv").fillna("")
    out = {"weak": [], "medium": [], "strong": []}
    for _, r in df.iterrows():
        marker = str(r.get("marker", "")).strip()
        intensity = str(r.get("intensity", "")).strip().lower()
        if marker and intensity in out:
            out[intensity].append(marker)
    return out


def load_metaphor_candidates(dict_dir: Path) -> List[str]:
    df = pd.read_csv(dict_dir / "metaphor_candidates.csv").fillna("")
    return [str(x).strip() for x in df["marker"].tolist() if str(x).strip()]


def compile_keyword_patterns(ref_keywords: Dict[str, List[str]]) -> Dict[str, List[Tuple[str, re.Pattern[str]]]]:
    out: Dict[str, List[Tuple[str, re.Pattern[str]]]] = {}
    for c, kws in ref_keywords.items():
        patterns = []
        for kw in kws:
            p = re.compile(rf"\b{re.escape(kw)}\b", flags=re.IGNORECASE)
            patterns.append((kw, p))
        out[c] = patterns
    return out


def split_sentences(text: str) -> List[str]:
    if not text.strip():
        return []
    sents = [s.strip() for s in SENT_SPLIT_RE.split(text) if s.strip()]
    return sents


def merge_windows(indices: List[int]) -> List[Tuple[int, int]]:
    if not indices:
        return []
    raw = []
    for i in sorted(set(indices)):
        raw.append((i - 1, i + 1))
    merged: List[Tuple[int, int]] = []
    for lo, hi in raw:
        if not merged:
            merged.append((lo, hi))
            continue
        plo, phi = merged[-1]
        if lo <= phi:
            merged[-1] = (plo, max(phi, hi))
        else:
            merged.append((lo, hi))
    return merged


def find_country_hits_by_sentence(sents: List[str], patterns: List[Tuple[str, re.Pattern[str]]]) -> Dict[int, List[str]]:
    hits: Dict[int, List[str]] = {}
    for i, s in enumerate(sents):
        matched = []
        for kw, pat in patterns:
            if pat.search(s):
                matched.append(kw)
        if matched:
            hits[i] = sorted(set(matched))
    return hits


def extract_context_rows(df: pd.DataFrame, ref_patterns: Dict[str, List[Tuple[str, re.Pattern[str]]]]) -> pd.DataFrame:
    rows: List[ContextRow] = []
    for _, r in df.iterrows():
        doc_id = str(r["doc_id"])
        media_country = str(r["media_country"])
        outlet_name = str(r["outlet_name"])
        date = str(r["date"])
        title = str(r["title"])
        text = str(r["text"])
        sents = split_sentences(text)
        if not sents:
            continue

        for ref_country in REF_COUNTRIES:
            hits_by_sent = find_country_hits_by_sentence(sents, ref_patterns[ref_country])
            if not hits_by_sent:
                continue

            windows = merge_windows(list(hits_by_sent.keys()))
            for w_i, (lo_raw, hi_raw) in enumerate(windows, start=1):
                lo = max(0, lo_raw)
                hi = min(len(sents) - 1, hi_raw)
                span_sents = sents[lo:hi + 1]

                matched = []
                target_sents = []
                for j in range(lo, hi + 1):
                    if j in hits_by_sent:
                        matched.extend(hits_by_sent[j])
                        target_sents.append(sents[j])
                matched = sorted(set(matched))

                prev_sent = sents[lo - 1] if lo - 1 >= 0 else ""
                next_sent = sents[hi + 1] if hi + 1 < len(sents) else ""
                context_id = f"{doc_id}__{ref_country}__{w_i:03d}"
                row = ContextRow(
                    context_id=context_id,
                    doc_id=doc_id,
                    media_country=media_country,
                    outlet_name=outlet_name,
                    date=date,
                    title=title,
                    ref_country=ref_country,
                    matched_keywords="; ".join(matched),
                    context_text=" ".join(span_sents).strip(),
                    previous_sentence=prev_sent,
                    target_sentence=" ".join(target_sents).strip(),
                    next_sentence=next_sent,
                )
                rows.append(row)

    out = pd.DataFrame([r.__dict__ for r in rows])
    return out


def count_marker_hits(text: str, markers: Iterable[str]) -> Tuple[int, List[str]]:
    total = 0
    found: List[str] = []
    for m in markers:
        p = re.compile(rf"\b{re.escape(m)}\b", flags=re.IGNORECASE)
        c = len(p.findall(text))
        if c > 0:
            total += c
            found.append(m)
    return total, sorted(set(found))


def compute_n_content(text: str) -> int:
    toks = tokenize(text)
    return sum(1 for t in toks if is_content_token(t))


def suggest_evi(context_text: str, ref_keywords: List[str]) -> Tuple[int, str]:
    ref_hit = False
    for kw in ref_keywords:
        if re.search(rf"\b{re.escape(kw)}\b", context_text, flags=re.IGNORECASE):
            ref_hit = True
            break
    if not ref_hit:
        return 0, "No clear referent cue in context."

    pos_hits, _ = count_marker_hits(context_text, EVAL_POS)
    neg_hits, _ = count_marker_hits(context_text, EVAL_NEG)
    score = pos_hits - neg_hits
    if score <= -3:
        return -2, f"Strong negative evaluative cues: pos={pos_hits}, neg={neg_hits}"
    if score < 0:
        return -1, f"Moderate negative cues: pos={pos_hits}, neg={neg_hits}"
    if score == 0:
        return 0, "Balanced or informational context."
    if score < 3:
        return 1, f"Moderate positive cues: pos={pos_hits}, neg={neg_hits}"
    return 2, f"Strong positive evaluative cues: pos={pos_hits}, neg={neg_hits}"


def apply_metrics(
    contexts: pd.DataFrame,
    dict_dir: Path,
    evi_mode: str,
    evi_manual_path: Path | None,
    metaphor_review_path: Path | None,
) -> pd.DataFrame:
    ideol_markers = load_ideological_markers(dict_dir)
    emot_markers = load_emotional_markers(dict_dir)
    metaphors = load_metaphor_candidates(dict_dir)
    ref_kw = load_ref_keywords(dict_dir)

    manual_evi = {}
    if evi_manual_path and evi_manual_path.exists():
        mdf = pd.read_csv(evi_manual_path).fillna("")
        for _, r in mdf.iterrows():
            key = (str(r.get("context_id", "")), str(r.get("ref_country", "")))
            try:
                val = int(r.get("EVI"))
            except Exception:
                continue
            expl = str(r.get("explanation", "")).strip()
            if val in EVI_ALLOWED:
                manual_evi[key] = (val, expl if expl else "Manual EVI")

    metaphor_review = {}
    if metaphor_review_path and metaphor_review_path.exists():
        rdf = pd.read_csv(metaphor_review_path).fillna("")
        for _, r in rdf.iterrows():
            key = (str(r.get("context_id", "")), str(r.get("ref_country", "")), str(r.get("marker", "")).strip())
            is_met = str(r.get("is_metaphor", "")).strip().lower() in {"1", "true", "yes", "y"}
            metaphor_review[key] = is_met

    out_rows = []
    for _, row in contexts.iterrows():
        context_id = str(row["context_id"])
        ref_country = str(row["ref_country"])
        ctx = str(row["context_text"])
        n_content = compute_n_content(ctx)
        notes = []

        n_ideol, found_ideol = count_marker_hits(ctx, ideol_markers)
        n_w, found_w = count_marker_hits(ctx, emot_markers["weak"])
        n_m, found_m = count_marker_hits(ctx, emot_markers["medium"])
        n_s, found_s = count_marker_hits(ctx, emot_markers["strong"])
        n_met_candidates, found_met_candidates = count_marker_hits(ctx, metaphors)
        pos_hits, found_pos = count_marker_hits(ctx, EVAL_POS)
        neg_hits, found_neg = count_marker_hits(ctx, EVAL_NEG)
        evi_score_raw = pos_hits - neg_hits

        # Semi-automatic metaphor handling
        n_met = 0
        if metaphor_review:
            for m in found_met_candidates:
                if metaphor_review.get((context_id, ref_country, m), False):
                    n_met += 1
            if found_met_candidates and n_met == 0:
                notes.append("metaphor_candidates_present_but_not_confirmed")
        else:
            n_met = n_met_candidates
            if n_met > 0:
                notes.append("metaphor_needs_manual_verification")

        if n_content <= 0:
            idi = emi = mti = 0.0
            notes.append("invalid_n_content_zero")
        else:
            idi = n_ideol / n_content
            emi = ((n_w / 3.0) + (2.0 * n_m / 3.0) + n_s) / n_content
            mti = n_met / n_content

        idi = max(0.0, min(1.0, idi))
        emi = max(0.0, min(1.0, emi))
        mti = max(0.0, min(1.0, mti))

        key = (context_id, ref_country)
        if evi_mode == "manual":
            if key in manual_evi:
                evi, evi_expl = manual_evi[key]
            else:
                evi = 0
                evi_expl = "Manual EVI missing -> default 0"
                notes.append("manual_evi_missing")
        else:
            evi, evi_expl = suggest_evi(ctx, ref_kw[ref_country])

        if evi not in EVI_ALLOWED:
            notes.append("invalid_evi_value")
            evi = 0

        ip = (idi + emi + mti) * evi
        if evi == 0:
            ip = 0.0
        ip = max(-6.0, min(6.0, ip))

        if (emi >= 0.12) and evi == 0:
            notes.append("high_emi_but_evi_zero")
        if n_content < 8:
            notes.append("low_n_content")

        out = row.to_dict()
        out.update(
            {
                "N_content": int(n_content),
                "N_ideol": int(n_ideol),
                "N_e_w": int(n_w),
                "N_e_m": int(n_m),
                "N_e_s": int(n_s),
                "N_met": int(n_met),
                "IDI": round(idi, 6),
                "EMI": round(emi, 6),
                "MTI": round(mti, 6),
                "EVI": int(evi),
                "IP": round(ip, 6),
                "found_ideol_markers": "; ".join(found_ideol),
                "found_emotional_markers": "; ".join(sorted(set(found_w + found_m + found_s))),
                "found_metaphor_markers": "; ".join(found_met_candidates),
                "evi_pos_hits": int(pos_hits),
                "evi_neg_hits": int(neg_hits),
                "evi_score_raw": int(evi_score_raw),
                "evi_pos_markers": "; ".join(found_pos),
                "evi_neg_markers": "; ".join(found_neg),
                "explanation": evi_expl,
                "notes": "; ".join(sorted(set(notes))),
            }
        )
        out_rows.append(out)
    return pd.DataFrame(out_rows)


def add_multicountry_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    key = ["doc_id", "context_text"]
    counts = out.groupby(key)["ref_country"].nunique().reset_index(name="ref_count")
    out = out.merge(counts, on=key, how="left")
    out["multi_country_context"] = out["ref_count"] > 1
    out.drop(columns=["ref_count"], inplace=True)
    return out


def dominant_evi(series: pd.Series) -> int:
    vals = []
    for v in series.dropna().tolist():
        try:
            iv = int(round(float(v)))
        except Exception:
            continue
        if iv in EVI_ALLOWED:
            vals.append(iv)
    if not vals:
        return 0
    vc = pd.Series(vals).value_counts()
    top_count = int(vc.max())
    top_vals = [int(x) for x in vc[vc == top_count].index.tolist()]
    if len(top_vals) == 1:
        return top_vals[0]
    mean_v = sum(vals) / len(vals)
    top_vals.sort(key=lambda x: (abs(x - mean_v), -abs(x)))
    return int(top_vals[0])


def aggregate_outputs(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    def aggregate_group(keys: List[str]) -> pd.DataFrame:
        grouped = []
        for key_vals, g in df.groupby(keys, dropna=False):
            if not isinstance(key_vals, tuple):
                key_vals = (key_vals,)
            row = {k: v for k, v in zip(keys, key_vals)}
            row.update(
                {
                    "IDI": float(g["IDI"].mean()),
                    "EMI": float(g["EMI"].mean()),
                    "MTI": float(g["MTI"].mean()),
                    "EVI": int(dominant_evi(g["EVI"])),
                    "IP": float(g["IP"].mean()),
                    "number_of_contexts": int(len(g)),
                }
            )
            grouped.append(row)
        return pd.DataFrame(grouped)

    by_article = aggregate_group(["doc_id", "ref_country", "media_country", "outlet_name"])
    by_outlet = aggregate_group(["outlet_name", "media_country", "ref_country"])
    by_media_ref = aggregate_group(["media_country", "ref_country"])

    art_counts = (
        df.groupby(["media_country", "ref_country"])["doc_id"]
        .nunique()
        .reset_index(name="number_of_articles")
    )
    by_media_ref = by_media_ref.merge(art_counts, on=["media_country", "ref_country"], how="left")

    return by_article, by_outlet, by_media_ref, build_summary_matrix(by_media_ref)


def build_summary_matrix(by_media_ref: pd.DataFrame) -> pd.DataFrame:
    # Wide matrix with row=media country and columns country_metric.
    metrics = ["IDI", "EMI", "MTI", "EVI", "IP", "number_of_contexts", "number_of_articles"]
    rows = []
    for media in ["Malaysia", "Indonesia"]:
        row = {"media_country": media}
        sub = by_media_ref[by_media_ref["media_country"].str.lower() == media.lower()]
        for ref in REF_COUNTRIES:
            s = sub[sub["ref_country"] == ref]
            if s.empty:
                for m in metrics:
                    row[f"{ref}_{m}"] = 0
            else:
                for m in metrics:
                    row[f"{ref}_{m}"] = float(s.iloc[0][m]) if m in s.columns else 0
        rows.append(row)
    return pd.DataFrame(rows)


def build_flagged_cases(df: pd.DataFrame) -> pd.DataFrame:
    flagged = []
    for _, r in df.iterrows():
        reasons = []
        notes = str(r.get("notes", ""))
        if notes:
            reasons.extend([x.strip() for x in notes.split(";") if x.strip()])
        if bool(r.get("multi_country_context", False)):
            reasons.append("multi_country_context")
        if float(r.get("IP", 0)) < -6 or float(r.get("IP", 0)) > 6:
            reasons.append("ip_out_of_expected_range")
        if int(r.get("EVI", 0)) not in EVI_ALLOWED:
            reasons.append("invalid_evi")
        if float(r.get("IDI", 0)) < 0 or float(r.get("IDI", 0)) > 1:
            reasons.append("invalid_idi")
        if float(r.get("EMI", 0)) < 0 or float(r.get("EMI", 0)) > 1:
            reasons.append("invalid_emi")
        if float(r.get("MTI", 0)) < 0 or float(r.get("MTI", 0)) > 1:
            reasons.append("invalid_mti")
        if reasons:
            out = r.to_dict()
            out["flag_reasons"] = "; ".join(sorted(set(reasons)))
            flagged.append(out)
    return pd.DataFrame(flagged)


def save_outputs(
    contexts_full: pd.DataFrame,
    by_article: pd.DataFrame,
    by_outlet: pd.DataFrame,
    by_media_ref: pd.DataFrame,
    summary_matrix: pd.DataFrame,
    flagged: pd.DataFrame,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    contexts_full.to_csv(output_dir / "contexts_full.csv", index=False)
    by_article.to_csv(output_dir / "aggregated_by_article.csv", index=False)
    by_outlet.to_csv(output_dir / "aggregated_by_outlet.csv", index=False)
    by_media_ref.to_csv(output_dir / "aggregated_by_media_country_and_ref_country.csv", index=False)
    flagged.to_csv(output_dir / "flagged_cases.csv", index=False)

    with pd.ExcelWriter(output_dir / "summary_matrix.xlsx", engine="openpyxl") as xw:
        summary_matrix.to_excel(xw, index=False, sheet_name="summary_matrix")
        by_media_ref.to_excel(xw, index=False, sheet_name="long_table")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Referent-context analyzer (China/USA/Russia)")
    p.add_argument("--input", required=True, help="Input CSV/XLSX/JSON path")
    p.add_argument("--output-dir", required=True, help="Output directory")
    p.add_argument("--dict-dir", default="referent_dicts", help="Directory for editable marker dictionaries")
    p.add_argument(
        "--evi-mode",
        default="manual",
        choices=["manual", "suggested"],
        help="manual: prefer evi_manual.csv; suggested: auto EVI proposal",
    )
    p.add_argument("--evi-manual", default="", help="Optional CSV with context_id,ref_country,EVI,explanation")
    p.add_argument("--metaphor-review", default="", help="Optional CSV with context_id,ref_country,marker,is_metaphor")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    dict_dir = Path(args.dict_dir)
    ensure_default_dictionaries(dict_dir)

    raw = load_table(input_path)
    docs = ensure_required_fields(raw)
    ref_keywords = load_ref_keywords(dict_dir)
    ref_patterns = compile_keyword_patterns(ref_keywords)

    contexts = extract_context_rows(docs, ref_patterns)
    if contexts.empty:
        raise RuntimeError("No referent-bound contexts extracted. Check input fields and keyword dictionaries.")

    scored = apply_metrics(
        contexts=contexts,
        dict_dir=dict_dir,
        evi_mode=args.evi_mode,
        evi_manual_path=Path(args.evi_manual) if args.evi_manual else None,
        metaphor_review_path=Path(args.metaphor_review) if args.metaphor_review else None,
    )
    scored = add_multicountry_flags(scored)
    scored = scored[scored["ref_country"].isin(REF_COUNTRIES)].copy()

    # Mandatory QA constraints
    scored.loc[(scored["N_content"] <= 0), ["IDI", "EMI", "MTI", "IP"]] = 0.0
    scored.loc[(~scored["EVI"].isin(EVI_ALLOWED)), "EVI"] = 0
    scored.loc[(scored["EVI"] == 0), "IP"] = 0.0
    for c in ["IDI", "EMI", "MTI"]:
        scored[c] = scored[c].clip(lower=0.0, upper=1.0)
    scored["IP"] = scored["IP"].clip(lower=-6.0, upper=6.0)

    by_article, by_outlet, by_media_ref, matrix = aggregate_outputs(scored)
    flagged = build_flagged_cases(scored)
    save_outputs(scored, by_article, by_outlet, by_media_ref, matrix, flagged, output_dir)

    print("=" * 88)
    print("REFERENT ANALYSIS COMPLETE")
    print("=" * 88)
    print(f"Input docs: {len(docs)}")
    print(f"Extracted contexts: {len(scored)}")
    print(f"Output dir: {output_dir.resolve()}")
    print("Files:")
    print(f"- {output_dir / 'contexts_full.csv'}")
    print(f"- {output_dir / 'aggregated_by_article.csv'}")
    print(f"- {output_dir / 'aggregated_by_outlet.csv'}")
    print(f"- {output_dir / 'aggregated_by_media_country_and_ref_country.csv'}")
    print(f"- {output_dir / 'summary_matrix.xlsx'}")
    print(f"- {output_dir / 'flagged_cases.csv'}")


if __name__ == "__main__":
    main()
