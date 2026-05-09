#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import hashlib
import io
import json
import re
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from docx import Document
from pypdf import PdfReader

# Reuse your strict analyzer core
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import corpus_analysis_strict_method as core
try:
    import media_analyzer_referent as referent_core
except Exception:
    referent_core = None

APP_BUILD = "2026-05-08-18:35"

REFERENT_CATEGORY_KEYWORDS: Dict[str, Dict[str, List[str]]] = {
    "China": {
        "Leadership": ["xi", "jinping", "beijing", "cpc", "ccp", "prc", "communist party"],
        "Economy": ["yuan", "renminbi", "economy", "trade", "bri", "belt and road", "huawei", "tiktok", "alibaba"],
        "Security": ["taiwan", "south china sea", "pla", "xinjiang", "sanction", "military"],
        "Culture": ["culture", "confucius", "cinema", "music", "sport", "olympic"],
    },
    "USA": {
        "Leadership": ["biden", "trump", "white house", "washington", "congress", "senate"],
        "Economy": ["dollar", "federal reserve", "treasury", "wall street", "economy", "tariff", "trade"],
        "Security": ["pentagon", "nato", "military", "sanction", "defense", "state department"],
        "Culture": ["american culture", "hollywood", "silicon valley", "music", "sport"],
    },
    "Russia": {
        "Leadership": ["putin", "kremlin", "moscow", "lavrov", "medvedev"],
        "Economy": ["ruble", "economy", "energy", "gazprom", "rosneft", "sanction"],
        "Security": ["military", "ukraine war", "csto", "eaeu", "defense"],
        "Culture": ["russian culture", "orthodox", "cinema", "sport"],
    },
}


SOURCE_ALIASES = {
    "antara": "Antara",
    "astro": "Astro Awani",
    "awani": "Astro Awani",
    "bernama": "Bernama",
    "kompas": "Kompas Indonesia",
    "tempo": "Tempo",
    "jakarta_post": "The Jakarta Post",
    "jakartapost": "The Jakarta Post",
    "the_star": "The Star",
    "thestar": "The Star",
    "the_edge": "The Edge Malaysia",
    "edge": "The Edge Malaysia",
}

COUNTRY_HINTS = {
    "usa": ["usa", "united states", "america", "american", "washington", "u.s"],
    "russia": ["russia", "russian", "rusia", "moscow", "kremlin", "putin"],
    "china": ["china", "chinese", "cina", "tiongkok", "beijing", "xi jinping"],
}


def guess_source(filename: str) -> str:
    f = filename.casefold()
    for key, source in SOURCE_ALIASES.items():
        if key in f:
            return source
    return "Unknown"


def source_from_raw(raw: str) -> str:
    m = re.search(r"^\s*Source:\s*(.+?)\s*$", raw, flags=re.IGNORECASE | re.MULTILINE)
    if m and m.group(1).strip():
        return m.group(1).strip()
    return ""


def media_country_from_raw(raw: str) -> str:
    m = re.search(r"^\s*MediaCountry:\s*(.+?)\s*$", raw, flags=re.IGNORECASE | re.MULTILINE)
    if m and m.group(1).strip():
        return m.group(1).strip()
    return ""


def guess_country(filename: str, text: str) -> str:
    s = (filename + "\n" + text[:4000]).casefold()
    scores = {}
    for c, hints in COUNTRY_HINTS.items():
        scores[c] = sum(1 for h in hints if h in s)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "usa"


def guess_year(filename: str, text: str) -> int:
    for src in (filename, text[:500]):
        m = re.search(r"\b(20\d{2})\b", src)
        if m:
            y = int(m.group(1))
            if 2000 <= y <= 2100:
                return y
    return 2026


def extract_title_and_body(raw: str) -> Tuple[str, str]:
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return "Untitled", ""

    title = lines[0]
    if title.casefold().startswith("title:"):
        title = title.split(":", 1)[1].strip() or "Untitled"

    body = raw
    # Strip common metadata header blocks if present
    body = re.sub(r"^\s*Title:\s*.*$", "", body, flags=re.IGNORECASE | re.MULTILINE)
    body = re.sub(r"^\s*URL:\s*.*$", "", body, flags=re.IGNORECASE | re.MULTILINE)
    body = re.sub(r"^\s*Date:\s*.*$", "", body, flags=re.IGNORECASE | re.MULTILINE)
    return title, body.strip()


def decode_text_bytes(data: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


def extract_raw_by_extension(name: str, data: bytes) -> str:
    low = name.casefold()
    if low.endswith((".txt", ".md", ".text")):
        return decode_text_bytes(data)
    if low.endswith(".docx"):
        doc = Document(io.BytesIO(data))
        return "\n".join(p.text for p in doc.paragraphs if p.text)
    if low.endswith(".pdf"):
        pdf = PdfReader(io.BytesIO(data))
        return "\n".join((p.extract_text() or "") for p in pdf.pages)
    return ""


def _pick_col(columns: List[str], candidates: List[str]) -> Optional[str]:
    low = {c.casefold(): c for c in columns}
    for cand in candidates:
        if cand.casefold() in low:
            return low[cand.casefold()]
    for c in columns:
        cl = c.casefold()
        if any(k in cl for k in candidates):
            return c
    return None


def extract_rows_from_table_bytes(name: str, data: bytes) -> List[Tuple[str, str]]:
    ext = Path(name).suffix.casefold()
    try:
        if ext == ".csv":
            try:
                df = pd.read_csv(io.BytesIO(data))
            except Exception:
                df = pd.read_csv(io.BytesIO(data), sep=None, engine="python")
        elif ext in {".xlsx", ".xls"}:
            df = pd.read_excel(io.BytesIO(data))
        elif ext == ".json":
            obj = json.loads(decode_text_bytes(data))
            if isinstance(obj, list):
                df = pd.DataFrame(obj)
            elif isinstance(obj, dict):
                df = pd.DataFrame(obj.get("rows", obj))
            else:
                return []
        else:
            return []
    except Exception:
        return []

    if df.empty:
        return []
    df = df.fillna("")
    cols = [str(c) for c in df.columns.tolist()]
    text_col = _pick_col(cols, ["text", "content", "body", "article", "full_text", "текст", "материал"])
    if not text_col:
        return []
    title_col = _pick_col(cols, ["title", "headline", "заголовок"])
    source_col = _pick_col(cols, ["source", "outlet_name", "media", "publisher", "издание", "источник"])
    date_col = _pick_col(cols, ["date", "published_at", "datetime", "дата", "year", "год"])
    media_country_col = _pick_col(cols, ["media_country", "region", "страна_сми", "country"])

    out: List[Tuple[str, str]] = []
    for i, row in df.iterrows():
        text = str(row.get(text_col, "")).strip()
        if not text:
            continue
        title = str(row.get(title_col, "")).strip() if title_col else f"Row {i+1}"
        source = str(row.get(source_col, "")).strip() if source_col else "Table upload"
        date = str(row.get(date_col, "")).strip() if date_col else ""
        media_country = str(row.get(media_country_col, "")).strip() if media_country_col else ""
        blob = (
            f"Title: {title or f'Row {i+1}'}\n"
            f"Date: {date}\n"
            f"Source: {source}\n"
            f"MediaCountry: {media_country}\n\n"
            f"{text}"
        )
        out.append((f"{name}__row_{i+1:06d}.txt", blob))
    return out


def extract_file_items_by_extension(name: str, data: bytes) -> List[Tuple[str, str]]:
    low = name.casefold()
    if low.endswith((".csv", ".xlsx", ".xls", ".json")):
        return extract_rows_from_table_bytes(name, data)
    raw = extract_raw_by_extension(name, data)
    if not raw.strip():
        return []
    return [(name, raw)]


def read_zip_corpus_files(zip_bytes: bytes) -> List[Tuple[str, str]]:
    out = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = info.filename
            if not name.casefold().endswith((".txt", ".md", ".text", ".docx", ".pdf", ".csv", ".xlsx", ".xls", ".json")):
                continue
            out.extend(extract_file_items_by_extension(name, zf.read(info)))
    return out


def read_uploaded_corpus_files(files) -> List[Tuple[str, str]]:
    out = []
    for f in files or []:
        name = f.name
        out.extend(extract_file_items_by_extension(name, f.getvalue()))
    return out


def build_docs(file_items: List[Tuple[str, str]], min_year: int, max_year: int, use_lemma: bool) -> List[core.Doc]:
    docs = []
    for filename, raw in file_items:
        title, body = extract_title_and_body(raw)
        if not body.strip():
            continue

        year = guess_year(filename, raw)
        if year < min_year or year > max_year:
            continue

        source = source_from_raw(raw) or guess_source(filename)
        country = guess_country(filename, raw)

        body_clean = core.strip_boilerplate(body)
        raw_toks = core.tokenize(body_clean)
        lang = core.detect_language(raw_toks, source)
        toks = core.preprocess_tokens(raw_toks, use_lemma=use_lemma)
        if not toks:
            continue

        docs.append(
            core.Doc(
                source=source,
                region="malaysia" if source in {"Astro Awani", "Bernama", "The Star", "The Edge Malaysia"} else "indonesia",
                year=year,
                primary_country=country,
                language=lang,
                title=title,
                text=body_clean,
                tokens=toks,
            )
        )
    return docs


def map_source_to_media_country(source: str) -> str:
    if source in {"Astro Awani", "Bernama", "The Star", "The Edge Malaysia"}:
        return "Malaysia"
    if source in {"Antara", "Kompas Indonesia", "Tempo", "The Jakarta Post"}:
        return "Indonesia"
    return "Unknown"


def build_referent_input_df(file_items: List[Tuple[str, str]], min_year: int, max_year: int) -> pd.DataFrame:
    rows = []
    for i, (filename, raw) in enumerate(file_items, start=1):
        title, body = extract_title_and_body(raw)
        if not body.strip():
            continue
        year = guess_year(filename, raw)
        if year < min_year or year > max_year:
            continue
        source = source_from_raw(raw) or guess_source(filename)
        media_country = media_country_from_raw(raw) or map_source_to_media_country(source)
        date_guess = str(year)
        m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", raw[:500])
        if m:
            date_guess = m.group(1)
        rows.append(
            {
                "doc_id": f"doc_{i:06d}",
                "media_country": media_country,
                "outlet_name": source,
                "date": date_guess,
                "title": title,
                "text": core.strip_boilerplate(body),
                "language": "",
            }
        )
    return pd.DataFrame(rows)


def _empirical_level(percentile: float) -> str:
    p = float(percentile)
    if p <= 20:
        return "very_low"
    if p <= 40:
        return "low"
    if p <= 60:
        return "medium"
    if p <= 80:
        return "elevated"
    if p <= 95:
        return "high"
    return "extreme"


def _assign_percentiles(df: pd.DataFrame, col: str, out_col: str, basis_mode: str) -> pd.DataFrame:
    out = df.copy()
    if col not in out.columns:
        out[out_col] = 0.0
        return out
    s = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    if basis_mode == "full corpus":
        out[out_col] = s.rank(method="average", pct=True) * 100.0
        return out
    if basis_mode == "selected media_country":
        out[out_col] = out.groupby("media_country")[col].rank(method="average", pct=True) * 100.0
        return out
    if basis_mode == "selected ref_country":
        out[out_col] = out.groupby("ref_country")[col].rank(method="average", pct=True) * 100.0
        return out
    # media_country × ref_country
    out[out_col] = out.groupby(["media_country", "ref_country"])[col].rank(method="average", pct=True) * 100.0
    return out


def _distribution_stats(df: pd.DataFrame, col: str, basis_mode: str) -> pd.DataFrame:
    if col not in df.columns or df.empty:
        return pd.DataFrame([{"basis": basis_mode, "count": 0}])
    gkeys: List[str] = []
    if basis_mode == "selected media_country":
        gkeys = ["media_country"]
    elif basis_mode == "selected ref_country":
        gkeys = ["ref_country"]
    elif basis_mode == "media_country × ref_country":
        gkeys = ["media_country", "ref_country"]

    rows = []
    if not gkeys:
        groups = [("full corpus", df)]
    else:
        groups = []
        for keys, part in df.groupby(gkeys, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            label = " | ".join(f"{k}={v}" for k, v in zip(gkeys, keys))
            groups.append((label, part))

    for label, part in groups:
        s = pd.to_numeric(part[col], errors="coerce").dropna()
        if s.empty:
            rows.append({"basis": label, "count": 0})
            continue
        rows.append(
            {
                "basis": label,
                "min": float(s.min()),
                "max": float(s.max()),
                "mean": float(s.mean()),
                "median": float(s.median()),
                "std": float(s.std(ddof=0)),
                "p10": float(s.quantile(0.10)),
                "p25": float(s.quantile(0.25)),
                "p50": float(s.quantile(0.50)),
                "p75": float(s.quantile(0.75)),
                "p90": float(s.quantile(0.90)),
                "p95": float(s.quantile(0.95)),
                "p99": float(s.quantile(0.99)),
                "count": int(len(s)),
            }
        )
    return pd.DataFrame(rows)


def _read_calibration_df(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        return pd.DataFrame()
    if df.empty:
        return df
    df = df.fillna("")
    if "text" not in df.columns:
        return pd.DataFrame()
    if "calibration_type" not in df.columns:
        df["calibration_type"] = "ordinary_political_news"
    if "calibration_id" not in df.columns:
        df["calibration_id"] = [f"cal_{i+1:06d}" for i in range(len(df))]
    return df


def _compute_calibration_report(cal_df: pd.DataFrame, dict_dir: Path, ip_formula_mode: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if cal_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    ideol_markers = referent_core.load_ideological_markers(dict_dir)
    emot_markers = referent_core.load_emotional_markers(dict_dir)
    metaphors = referent_core.load_metaphor_candidates(dict_dir)

    rows = []
    for _, r in cal_df.iterrows():
        txt = str(r.get("text", ""))
        n_content = referent_core.compute_n_content(txt)
        n_ideol, _ = referent_core.count_marker_hits(txt, ideol_markers)
        n_w, _ = referent_core.count_marker_hits(txt, emot_markers.get("weak", []))
        n_m, _ = referent_core.count_marker_hits(txt, emot_markers.get("medium", []))
        n_s, _ = referent_core.count_marker_hits(txt, emot_markers.get("strong", []))
        n_met, _ = referent_core.count_marker_hits(txt, metaphors)
        den = max(float(n_content), 1.0)
        idi = max(0.0, min(1.0, n_ideol / den))
        emi = max(0.0, min(1.0, ((n_w / 3.0) + (2.0 * n_m / 3.0) + n_s) / den))
        mti = max(0.0, min(1.0, n_met / den))
        pos_lex = len(re.findall(r"\b(partner|cooperation|support|stability|development|trust|constructive)\b", txt, flags=re.IGNORECASE))
        neg_lex = len(re.findall(r"\b(threat|aggression|pressure|crisis|interference|coercion|conflict)\b", txt, flags=re.IGNORECASE))
        evi_raw = int(max(-10, min(10, pos_lex - neg_lex)))
        evi_norm = evi_raw / 5.0
        energy = idi + emi + mti
        if ip_formula_mode.startswith("updated"):
            ip_i = evi_norm * (1.0 + energy)
        else:
            ip_i = evi_norm * energy
        rows.append(
            {
                "calibration_id": str(r.get("calibration_id", "")),
                "calibration_type": str(r.get("calibration_type", "ordinary_political_news")),
                "IDI_raw": idi,
                "EMI_raw": emi,
                "MTI_raw": mti,
                "EVI_raw": evi_raw,
                "EVI_norm": evi_norm,
                "IP_i": ip_i,
                "IP_abs_i": abs(ip_i),
            }
        )
    detailed = pd.DataFrame(rows)
    report = (
        detailed.groupby("calibration_type", as_index=False)
        .agg(
            count=("calibration_id", "count"),
            mean_IDI=("IDI_raw", "mean"),
            mean_EMI=("EMI_raw", "mean"),
            mean_MTI=("MTI_raw", "mean"),
            mean_EVI_raw=("EVI_raw", "mean"),
            mean_IP_abs=("IP_abs_i", "mean"),
            p75_IDI=("IDI_raw", lambda x: float(pd.Series(x).quantile(0.75))),
            p75_EMI=("EMI_raw", lambda x: float(pd.Series(x).quantile(0.75))),
            p75_MTI=("MTI_raw", lambda x: float(pd.Series(x).quantile(0.75))),
            p75_IP_abs=("IP_abs_i", lambda x: float(pd.Series(x).quantile(0.75))),
        )
        .sort_values("count", ascending=False)
    )
    return detailed, report


def run_referent_analysis(
    input_df: pd.DataFrame,
    out_dir: Path,
    evi_mode: str,
    exclude_technical_mentions: bool,
    evi_manual_path: Optional[Path] = None,
    metaphor_review_path: Optional[Path] = None,
    calibration_path: Optional[Path] = None,
    ip_formula_mode: str = "updated",
    aggregation_mode: str = "weighted by S_r",
    percentile_basis: str = "full corpus",
):
    if referent_core is None:
        raise RuntimeError("referent analyzer module is unavailable")

    dict_dir = out_dir / "referent_dicts"
    referent_core.ensure_default_dictionaries(dict_dir)
    docs = referent_core.ensure_required_fields(input_df)
    ref_keywords = referent_core.load_ref_keywords(dict_dir)
    ref_patterns = referent_core.compile_keyword_patterns(ref_keywords)
    contexts = referent_core.extract_context_rows(docs, ref_patterns)
    if contexts.empty:
        raise RuntimeError("Не удалось извлечь референтные контексты (China/USA/Russia).")

    scored = referent_core.apply_metrics(
        contexts=contexts,
        dict_dir=dict_dir,
        evi_mode=evi_mode,
        evi_manual_path=evi_manual_path,
        metaphor_review_path=metaphor_review_path,
    )
    scored = referent_core.add_multicountry_flags(scored)
    ref_countries_allowed = set(getattr(referent_core, "REF_COUNTRIES", ["China", "USA", "Russia"]))
    evi_coarse_allowed = set(getattr(referent_core, "EVI_COARSE_ALLOWED", {-2, -1, 0, 1, 2}))
    scored = scored[scored["ref_country"].isin(ref_countries_allowed)].copy()
    # Backward compatibility with older referent modules / column schemas.
    if "EVI" not in scored.columns:
        scored["EVI"] = 0
    if "EVI_raw" not in scored.columns:
        scored["EVI_raw"] = pd.to_numeric(scored["EVI"], errors="coerce").fillna(0).astype(int) * 5
    if "EVI_norm" not in scored.columns:
        scored["EVI_norm"] = pd.to_numeric(scored["EVI_raw"], errors="coerce").fillna(0.0) / 5.0
    if "IP" not in scored.columns:
        scored["IP"] = 0.0
    for col in ["IDI", "EMI", "MTI", "N_content", "referent_salience"]:
        if col not in scored.columns:
            scored[col] = 0.0 if col != "N_content" else 0
    scored.loc[(scored["N_content"] <= 0), ["IDI", "EMI", "MTI", "IP"]] = 0.0
    scored.loc[(~scored["EVI"].isin(evi_coarse_allowed)), "EVI"] = 0
    scored.loc[(~scored["EVI_raw"].between(-10, 10)), ["EVI_raw", "EVI_norm", "IP"]] = [0, 0.0, 0.0]
    scored.loc[(scored["referent_salience"] == 0), "IP"] = 0.0
    scored.loc[(scored["EVI_raw"] == 0), "IP"] = 0.0
    for col in ["IDI", "EMI", "MTI"]:
        scored[col] = scored[col].clip(lower=0.0, upper=1.0)
    scored["EVI_norm"] = scored["EVI_raw"] / 5.0
    scored["Discursive_energy"] = scored["IDI"] + scored["EMI"] + scored["MTI"]
    if str(ip_formula_mode).startswith("legacy"):
        scored["IP_i"] = scored["EVI_norm"] * scored["Discursive_energy"]
        formula_ver = "legacy_energy_times_evi_norm"
    else:
        scored["IP_i"] = scored["EVI_norm"] * (1.0 + scored["Discursive_energy"])
        formula_ver = "updated_evi_norm_times_1_plus_energy"
    scored.loc[scored["EVI_norm"] == 0.0, "IP_i"] = 0.0
    scored["IP_abs_i"] = scored["IP_i"].abs()
    scored["IP_context"] = scored["IP_i"]
    scored["IP_context_abs"] = scored["IP_abs_i"]
    scored["IP"] = scored["IP_i"]
    scored["IP_formula_version"] = formula_ver
    scored["IP_old_context"] = scored["EVI_norm"] * scored["Discursive_energy"]

    # Raw and percent aliases
    scored["IDI_raw"] = scored["IDI"]
    scored["EMI_raw"] = scored["EMI"]
    scored["MTI_raw"] = scored["MTI"]
    scored["IDI_r"] = scored["IDI_raw"]
    scored["EMI_r"] = scored["EMI_raw"]
    scored["MTI_r"] = scored["MTI_raw"]
    scored["EVI_raw_r"] = scored["EVI_raw"]
    scored["EVI_norm_r"] = scored["EVI_norm"]
    scored["S_r"] = scored["referent_salience"]
    scored["IP_abs_i"] = scored["IP_abs_i"]
    scored["IDI_percent_value"] = scored["IDI_raw"] * 100.0
    scored["EMI_percent_value"] = scored["EMI_raw"] * 100.0
    scored["MTI_percent_value"] = scored["MTI_raw"] * 100.0
    scored["EVI_explanation"] = scored.get("evi_explanation", "")

    # Aggregation weights
    if str(aggregation_mode).startswith("weighted"):
        scored["aggregation_weight"] = pd.to_numeric(scored["referent_salience"], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    else:
        scored["aggregation_weight"] = 1.0
    if exclude_technical_mentions and "is_technical_mention" in scored.columns:
        scored.loc[scored["is_technical_mention"] == True, "aggregation_weight"] = 0.0
    if str(aggregation_mode).startswith("unweighted"):
        scored.loc[pd.to_numeric(scored["N_content"], errors="coerce").fillna(0) <= 0, "aggregation_weight"] = 0.0

    # Percentiles
    scored = _assign_percentiles(scored, "IDI_raw", "IDI_percentile", percentile_basis)
    scored = _assign_percentiles(scored, "EMI_raw", "EMI_percentile", percentile_basis)
    scored = _assign_percentiles(scored, "MTI_raw", "MTI_percentile", percentile_basis)
    scored = _assign_percentiles(scored, "IP_i", "IP_percentile", percentile_basis)
    scored = _assign_percentiles(scored, "IP_abs_i", "IP_abs_percentile", percentile_basis)
    scored["IDI_level_empirical"] = scored["IDI_percentile"].map(_empirical_level)
    scored["EMI_level_empirical"] = scored["EMI_percentile"].map(_empirical_level)
    scored["MTI_level_empirical"] = scored["MTI_percentile"].map(_empirical_level)
    scored["IP_level_empirical"] = scored["IP_percentile"].map(_empirical_level)
    scored["IP_abs_level_empirical"] = scored["IP_abs_percentile"].map(_empirical_level)

    # Round storage precision (do not visually collapse later)
    for c in ["IDI_raw", "EMI_raw", "MTI_raw", "IP_i", "IP_abs_i", "IP_context", "IP_context_abs", "EVI_norm", "Discursive_energy"]:
        scored[c] = pd.to_numeric(scored[c], errors="coerce").fillna(0.0).round(8)
    for c in ["IDI_percent_value", "EMI_percent_value", "MTI_percent_value"]:
        scored[c] = pd.to_numeric(scored[c], errors="coerce").fillna(0.0).round(6)

    # Aggregations
    def _agg_table(keys: List[str]) -> pd.DataFrame:
        rows = []
        for vals, g in scored.groupby(keys, dropna=False):
            if not isinstance(vals, tuple):
                vals = (vals,)
            row = {k: v for k, v in zip(keys, vals)}
            total = len(g)
            arts = int(g["doc_id"].nunique()) if "doc_id" in g.columns else 0
            technical_count = int(g["is_technical_mention"].fillna(False).astype(bool).sum()) if "is_technical_mention" in g.columns else 0
            gv = g[g["aggregation_weight"] > 0].copy()
            if gv.empty or float(gv["aggregation_weight"].sum()) == 0.0:
                row.update(
                    {
                        "number_of_contexts": int(total),
                        "number_of_articles": arts,
                        "contexts_analyzed": 0,
                        "contexts_excluded": int(total),
                        "mean_IDI_raw": 0.0,
                        "mean_IDI_percent_value": 0.0,
                        "mean_IDI_percentile": 0.0,
                        "mean_EMI_raw": 0.0,
                        "mean_EMI_percent_value": 0.0,
                        "mean_EMI_percentile": 0.0,
                        "mean_MTI_raw": 0.0,
                        "mean_MTI_percent_value": 0.0,
                        "mean_MTI_percentile": 0.0,
                        "mean_EVI_raw": 0.0,
                        "mean_EVI_norm": 0.0,
                        "IP_final": 0.0,
                        "IP_abs_final": 0.0,
                        "mean_IP_percentile": 0.0,
                        "mean_IP_abs_percentile": 0.0,
                        "positive_context_share": 0.0,
                        "negative_context_share": 0.0,
                        "neutral_context_share": 0.0,
                        "central_context_share": 0.0,
                        "technical_mentions_count": technical_count,
                        "technical_mentions_excluded": technical_count if exclude_technical_mentions else 0,
                        "warning": "No content-relevant referent contexts",
                    }
                )
            else:
                w = gv["aggregation_weight"]
                ip = gv["IP_i"]
                row.update(
                    {
                        "number_of_contexts": int(total),
                        "number_of_articles": arts,
                        "contexts_analyzed": int(len(gv)),
                        "contexts_excluded": int((g["aggregation_weight"] == 0).sum()),
                        "mean_IDI_raw": float(gv["IDI_raw"].mean()),
                        "mean_IDI_percent_value": float(gv["IDI_percent_value"].mean()),
                        "mean_IDI_percentile": float(gv["IDI_percentile"].mean()),
                        "mean_EMI_raw": float(gv["EMI_raw"].mean()),
                        "mean_EMI_percent_value": float(gv["EMI_percent_value"].mean()),
                        "mean_EMI_percentile": float(gv["EMI_percentile"].mean()),
                        "mean_MTI_raw": float(gv["MTI_raw"].mean()),
                        "mean_MTI_percent_value": float(gv["MTI_percent_value"].mean()),
                        "mean_MTI_percentile": float(gv["MTI_percentile"].mean()),
                        "mean_EVI_raw": float(gv["EVI_raw"].mean()),
                        "mean_EVI_norm": float(gv["EVI_norm"].mean()),
                        "IP_final": float((ip * w).sum() / w.sum()),
                        "IP_abs_final": float((gv["IP_abs_i"] * w).sum() / w.sum()),
                        "mean_IP_percentile": float(gv["IP_percentile"].mean()),
                        "mean_IP_abs_percentile": float(gv["IP_abs_percentile"].mean()),
                        "positive_context_share": float((gv["EVI_raw"] > 0).sum() / len(gv)),
                        "negative_context_share": float((gv["EVI_raw"] < 0).sum() / len(gv)),
                        "neutral_context_share": float((gv["EVI_raw"] == 0).sum() / len(gv)),
                        "central_context_share": float((gv["referent_salience"] == 1.0).sum() / len(gv)),
                        "technical_mentions_count": technical_count,
                        "technical_mentions_excluded": technical_count if exclude_technical_mentions else 0,
                        "warning": "",
                    }
                )
            rows.append(row)
        return pd.DataFrame(rows)

    by_article = _agg_table(["doc_id", "ref_country", "media_country", "outlet_name"])
    by_outlet = _agg_table(["outlet_name", "media_country", "ref_country"])
    by_media_ref = _agg_table(["media_country", "ref_country"])
    matrix = by_media_ref.copy()

    flagged = referent_core.build_flagged_cases(scored) if hasattr(referent_core, "build_flagged_cases") else pd.DataFrame()
    if not scored.empty:
        if scored["aggregation_weight"].sum() == 0:
            flagged = pd.concat([flagged, pd.DataFrame([{"flag_case_type": "zero_salience_all_contexts"}])], ignore_index=True)
        outlier_mask = (
            (pd.to_numeric(scored["IDI_percentile"], errors="coerce").fillna(0) > 99)
            | (pd.to_numeric(scored["EMI_percentile"], errors="coerce").fillna(0) > 99)
            | (pd.to_numeric(scored["MTI_percentile"], errors="coerce").fillna(0) > 99)
            | (pd.to_numeric(scored["IP_abs_percentile"], errors="coerce").fillna(0) > 99)
        )
        if outlier_mask.any():
            extra = scored[outlier_mask].copy()
            extra["flag_case_type"] = "suspicious_percentile_outliers"
            flagged = pd.concat([flagged, extra], ignore_index=True)

    # Exports
    out_dir.mkdir(parents=True, exist_ok=True)
    scored.to_csv(out_dir / "contexts_full.csv", index=False)
    by_article.to_csv(out_dir / "aggregated_by_article.csv", index=False)
    by_outlet.to_csv(out_dir / "aggregated_by_outlet.csv", index=False)
    by_media_ref.to_csv(out_dir / "aggregated_by_media_country_and_ref_country.csv", index=False)
    flagged.to_csv(out_dir / "flagged_cases.csv", index=False)

    dist_idi = _distribution_stats(scored, "IDI_raw", percentile_basis)
    dist_emi = _distribution_stats(scored, "EMI_raw", percentile_basis)
    dist_mti = _distribution_stats(scored, "MTI_raw", percentile_basis)
    dist_ip = _distribution_stats(scored, "IP_i", percentile_basis)
    dist_ip_abs = _distribution_stats(scored, "IP_abs_i", percentile_basis)

    calibration_detailed = pd.DataFrame()
    calibration_report = pd.DataFrame()
    if calibration_path and calibration_path.exists():
        cal_df = _read_calibration_df(calibration_path)
        calibration_detailed, calibration_report = _compute_calibration_report(cal_df, dict_dir, ip_formula_mode)
        calibration_detailed.to_csv(out_dir / "calibration_detailed.csv", index=False)

    with pd.ExcelWriter(out_dir / "distribution_stats.xlsx", engine="openpyxl") as xw:
        dist_idi.to_excel(xw, index=False, sheet_name="IDI_distribution")
        dist_emi.to_excel(xw, index=False, sheet_name="EMI_distribution")
        dist_mti.to_excel(xw, index=False, sheet_name="MTI_distribution")
        dist_ip.to_excel(xw, index=False, sheet_name="IP_distribution")
        dist_ip_abs.to_excel(xw, index=False, sheet_name="IP_abs_distribution")

    with pd.ExcelWriter(out_dir / "summary_matrix.xlsx", engine="openpyxl") as xw:
        matrix.to_excel(xw, index=False, sheet_name="summary_matrix")
        by_media_ref.to_excel(xw, index=False, sheet_name="long_table")
        scored.to_excel(xw, index=False, sheet_name="contexts_full")
        flagged.to_excel(xw, index=False, sheet_name="flagged_cases")
        if not calibration_report.empty:
            calibration_report.to_excel(xw, index=False, sheet_name="calibration_report")

    if not calibration_report.empty:
        calibration_report.to_excel(out_dir / "calibration_report.xlsx", index=False)

    return {
        "docs": len(docs),
        "contexts": len(scored),
        "flagged": len(flagged),
        "calibration_types": int(calibration_report["calibration_type"].nunique()) if not calibration_report.empty else 0,
    }


def _split_marker_cell(cell: str) -> List[str]:
    if not isinstance(cell, str) or not cell.strip():
        return []
    return [x.strip() for x in cell.split(";") if x.strip()]


def _assign_keyword_category(ref_country: str, matched_keywords: str) -> str:
    cats = REFERENT_CATEGORY_KEYWORDS.get(ref_country, {})
    kws = [k.casefold() for k in _split_marker_cell(matched_keywords)]
    if not kws:
        return "other"
    matched = []
    for cat, hints in cats.items():
        hh = [h.casefold() for h in hints]
        if any(any(h in kw for h in hh) for kw in kws):
            matched.append(cat)
    return "; ".join(matched) if matched else "other"


def _build_referent_view_df(contexts_df: pd.DataFrame) -> pd.DataFrame:
    df = contexts_df.copy()
    if "matched_keywords" not in df.columns:
        df["matched_keywords"] = ""
    df["keyword_category"] = df.apply(
        lambda r: _assign_keyword_category(str(r.get("ref_country", "")), str(r.get("matched_keywords", ""))),
        axis=1,
    )
    return df


def _dominant_discrete_evi(values: pd.Series) -> int:
    allowed = {-2, -1, 0, 1, 2}
    vals = []
    for v in values.dropna().tolist():
        try:
            iv = int(round(float(v)))
        except Exception:
            continue
        if iv in allowed:
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


def _render_proof_contexts(df: pd.DataFrame, marker_col: str, title: str, limit: int = 6) -> None:
    st.markdown(f"**{title}**")
    if marker_col not in df.columns:
        st.caption("Для этой версии данных колонка маркеров отсутствует.")
        return
    tmp = df.copy()
    tmp["_marker_list"] = tmp[marker_col].fillna("").astype(str).map(_split_marker_cell)
    tmp = tmp[tmp["_marker_list"].map(len) > 0]
    if tmp.empty:
        st.caption("Маркеры не найдены в текущей выборке.")
        return
    tmp = tmp.head(limit)
    for _, r in tmp.iterrows():
        terms = r["_marker_list"]
        meta = f"{r.get('context_id','')} | {r.get('outlet_name','')} | {r.get('date','')} | {r.get('keyword_category','')}"
        st.caption(meta)
        st.markdown(
            f"<div style='padding:10px;border:1px solid #334155;border-radius:8px;line-height:1.6'>{_highlight_terms_html(str(r.get('context_text','')), terms)}</div>",
            unsafe_allow_html=True,
        )


def _sign_label(v: float) -> str:
    if v > 0:
        return "положительный"
    if v < 0:
        return "отрицательный"
    return "нейтральный"


def _evi_mode_ru(mode: str) -> str:
    m = (mode or "").strip().lower()
    mapping = {
        "fine": "Точный режим: оценка от -10 до +10 (самый детальный).",
        "coarse": "Крупный режим: оценка только -2, -1, 0, +1, +2.",
        "suggested": "Автоподсказка: система предлагает оценку по правилам.",
        "manual": "Ручная разметка: оценка берется из вашего CSV.",
    }
    return mapping.get(m, m)


def _collect_terms(series: pd.Series) -> List[str]:
    terms: List[str] = []
    for val in series.fillna("").astype(str).tolist():
        for t in _split_marker_cell(val):
            if t:
                terms.append(t.casefold())
    return terms


def _dominant_frames_and_strategies(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    terms = []
    for col in ["found_ideol_markers", "evi_pos_markers", "evi_neg_markers", "found_metaphor_markers"]:
        if col in df.columns:
            terms.extend(_collect_terms(df[col]))
    bag = pd.Series(terms).value_counts() if terms else pd.Series(dtype=int)

    frame_rules = {
        "Суверенитет и легитимность": ["sovereignty", "суверенитет", "legitimate", "responsible actor", "defender"],
        "Угроза и секьюритизация": ["threat", "aggression", "security", "угроза", "агресс", "конфликт"],
        "Партнерство и развитие": ["cooperation", "development", "partnership", "growth", "cooperat", "развитие"],
        "Геополитическое соперничество": ["hegemony", "authoritarian", "санкц", "sanction", "nato", "military"],
    }
    strategy_rules = {
        "Легитимация": ["legitimate", "responsible", "stability", "protects", "defends", "cooperation"],
        "Делегитимация": ["violates", "aggressor", "threat", "authoritarian", "диктат", "угроза"],
        "Секьюритизация": ["security", "threat", "military", "defense", "sanction", "конфликт"],
        "Эмоционализация": ["fear", "anger", "catastrophe", "heroic", "panic", "outrage"],
        "Метафоризация": ["battle", "frontline", "wave", "chessboard", "storm", "path"],
    }

    frame_rows = []
    for label, keys in frame_rules.items():
        score = 0
        examples = []
        for term, cnt in bag.items():
            if any(k in term for k in keys):
                score += int(cnt)
                if len(examples) < 5:
                    examples.append(term)
        if score > 0:
            frame_rows.append({"frame": label, "score": score, "examples": ", ".join(examples)})
    frame_df = pd.DataFrame(frame_rows).sort_values("score", ascending=False) if frame_rows else pd.DataFrame(columns=["frame", "score", "examples"])

    strategy_rows = []
    for label, keys in strategy_rules.items():
        score = 0
        examples = []
        for term, cnt in bag.items():
            if any(k in term for k in keys):
                score += int(cnt)
                if len(examples) < 5:
                    examples.append(term)
        if score > 0:
            strategy_rows.append({"strategy": label, "score": score, "examples": ", ".join(examples)})
    strategy_df = pd.DataFrame(strategy_rows).sort_values("score", ascending=False) if strategy_rows else pd.DataFrame(columns=["strategy", "score", "examples"])

    return frame_df, strategy_df


def show_referent_dashboard(
    out_dir: Path,
    default_ref: str,
    default_category: str,
    evi_mode: str,
    exclude_technical_mentions: bool,
    show_evi_rubric_details: bool,
    show_salience_diagnostics: bool,
    display_precision: int = 6,
    show_percent_values: bool = True,
    show_empirical_percentiles: bool = True,
    percentile_basis: str = "full corpus",
    use_calibration_anchors: bool = False,
) -> None:
    p_ctx = out_dir / "contexts_full.csv"
    if not p_ctx.exists():
        st.error("Файл contexts_full.csv не найден.")
        return
    df_all = pd.read_csv(p_ctx)
    if df_all.empty:
        st.warning("Нет контекстов для отображения.")
        return
    df_all = _build_referent_view_df(df_all)

    refs = [r for r in ["China", "USA", "Russia"] if r in set(df_all["ref_country"].astype(str))]
    if not refs:
        st.warning("В данных нет референтов China/USA/Russia.")
        return
    if default_ref not in refs:
        default_ref = refs[0]
    ref_country = st.selectbox("Референт анализа", options=refs, index=refs.index(default_ref), key="ref_country_main")

    df_ref = df_all[df_all["ref_country"] == ref_country].copy()
    categories = sorted({c for val in df_ref["keyword_category"].dropna().astype(str) for c in [x.strip() for x in val.split(";")] if c})
    categories = ["all"] + categories
    cat_index = categories.index(default_category) if default_category in categories else 0
    category = st.selectbox("Категория ключевых слов", options=categories, index=cat_index, key="ref_cat_main")
    if category != "all":
        df_ref = df_ref[df_ref["keyword_category"].astype(str).str.contains(rf"(^|;\s*){re.escape(category)}($|;)", regex=True)]

    if df_ref.empty:
        st.warning("После фильтрации по референту/категории не осталось контекстов.")
        return

    # Compatibility layer: some cloud runs may have reduced schemas from older referent modules.
    numeric_defaults = {
        "IDI": 0.0,
        "EMI": 0.0,
        "MTI": 0.0,
        "N_content": 0,
        "N_ideol": 0,
        "N_e_w": 0,
        "N_e_m": 0,
        "N_e_s": 0,
        "N_met": 0,
        "referent_salience": 1.0,
        "EVI": 0,
        "IP": 0.0,
    }
    text_defaults = {
        "context_id": "",
        "outlet_name": "Unknown",
        "date": "",
        "matched_keywords": "",
        "context_text": "",
        "found_ideol_markers": "",
        "found_emotional_markers": "",
        "found_metaphor_markers": "",
        "evi_pos_markers": "",
        "evi_neg_markers": "",
        "positive_evidence_terms": "",
        "negative_evidence_terms": "",
        "evi_explanation": "",
        "notes": "",
    }
    for c, d in numeric_defaults.items():
        if c not in df_ref.columns:
            df_ref[c] = d
    for c, d in text_defaults.items():
        if c not in df_ref.columns:
            df_ref[c] = d
    if "positive_score" not in df_ref.columns:
        df_ref["positive_score"] = 0
    if "negative_score" not in df_ref.columns:
        df_ref["negative_score"] = 0
    if "EVI_raw" not in df_ref.columns:
        df_ref["EVI_raw"] = pd.to_numeric(df_ref["EVI"], errors="coerce").fillna(0).astype(int) * 5
    if "EVI_norm" not in df_ref.columns:
        df_ref["EVI_norm"] = pd.to_numeric(df_ref["EVI_raw"], errors="coerce").fillna(0.0) / 5.0

    st.subheader(f"Анализ референта: {ref_country}")
    fmt_raw = f".{int(display_precision)}f"
    fmt_pct = ".4f"
    fmt_ip = ".6f"
    c1, c2, c3 = st.columns(3)
    c1.metric("Контексты", int(len(df_ref)))
    c2.metric("Статьи", int(df_ref["doc_id"].nunique()))
    c3.metric("Источники", int(df_ref["outlet_name"].nunique()))

    if hasattr(referent_core, "compute_context_ip"):
        df_ref = referent_core.compute_context_ip(df_ref)
    else:
        df_ref["discursive_energy"] = df_ref["IDI"] + df_ref["EMI"] + df_ref["MTI"]
        df_ref["IP_context"] = df_ref["EVI_norm"] * (1.0 + df_ref["discursive_energy"])
        df_ref["IP_context_abs"] = df_ref["IP_context"].abs()
        df_ref["IP_old_context"] = df_ref["discursive_energy"] * df_ref["EVI_norm"]
        df_ref["aggregation_weight"] = df_ref["referent_salience"].clip(lower=0.0, upper=1.0)
    if hasattr(referent_core, "weighted_aggregate_ip"):
        agg = referent_core.weighted_aggregate_ip(df_ref)
    else:
        valid_tmp = df_ref[df_ref["aggregation_weight"] > 0].copy()
        if valid_tmp.empty:
            agg = {"IP_final": 0.0, "IP_abs_final": 0.0, "mean_IP_unweighted": 0.0, "contexts_analyzed": 0, "contexts_excluded": int(len(df_ref)), "warning": "No substantive referent contexts after salience filtering"}
        else:
            w = valid_tmp["aggregation_weight"]
            ip = valid_tmp["IP_context"]
            agg = {"IP_final": float((ip * w).sum() / w.sum()), "IP_abs_final": float((ip.abs() * w).sum() / w.sum()), "mean_IP_unweighted": float(ip.mean()), "contexts_analyzed": int(len(valid_tmp)), "contexts_excluded": int((df_ref["aggregation_weight"] == 0).sum()), "warning": None}
    valid = df_ref[df_ref["aggregation_weight"] > 0].copy()
    metric_df = valid.copy()
    metric_scope = "взвешенные контексты (S_r > 0)"
    if metric_df.empty:
        if exclude_technical_mentions and "is_technical_mention" in df_ref.columns:
            non_tech = df_ref[df_ref["is_technical_mention"] != True].copy()
            if not non_tech.empty:
                metric_df = non_tech
                metric_scope = "нетехнические контексты (fallback)"
        if metric_df.empty:
            metric_df = df_ref.copy()
            metric_scope = "все контексты (fallback)"

    n_content_sum = max(float(metric_df["N_content"].sum()), 1.0)
    n_ideol_sum = float(metric_df["N_ideol"].sum())
    weighted_emotion_sum = float(((metric_df["N_e_w"] / 3.0) + (2.0 * metric_df["N_e_m"] / 3.0) + metric_df["N_e_s"]).sum())
    n_met_sum = float(metric_df["N_met"].sum())

    idi = float(n_ideol_sum / n_content_sum)
    emi = float(weighted_emotion_sum / n_content_sum)
    mti = float(n_met_sum / n_content_sum)
    evi_raw = float(metric_df["EVI_raw"].mean()) if "EVI_raw" in metric_df.columns and not metric_df.empty else 0.0
    evi_norm = float(metric_df["EVI_norm"].mean()) if "EVI_norm" in metric_df.columns and not metric_df.empty else 0.0
    evi_coarse = int(_dominant_discrete_evi(metric_df["EVI"])) if "EVI" in metric_df.columns and not metric_df.empty else 0
    ip_formula = float(agg["IP_final"])
    ip_abs_formula = float(agg["IP_abs_final"])
    contexts_analyzed = int(agg["contexts_analyzed"])
    contexts_excluded = int(agg["contexts_excluded"])
    technical_mentions_excluded = int((df_ref["aggregation_weight"] == 0).sum())
    warning_msg = agg["warning"]
    if metric_scope != "взвешенные контексты (S_r > 0)":
        st.warning(
            f"Для описательных индикаторов использован набор: {metric_scope}. "
            "Это предотвращает ложные нули, когда взвешенная выборка пуста."
        )

    def density_level(v: float) -> str:
        if v < 0.03:
            return "низкий"
        if v < 0.08:
            return "умеренный"
        if v < 0.15:
            return "высокий"
        return "очень высокий"

    def evi_level(v: float) -> str:
        if v <= -1.5:
            return "резко негативный"
        if v < -0.5:
            return "негативный"
        if v < 0.5:
            return "нейтральный"
        if v < 1.5:
            return "позитивный"
        return "резко позитивный"

    def ip_level(v: float) -> str:
        av = abs(v)
        if av < 0.5:
            return "слабое воздействие"
        if av < 1.5:
            return "умеренное воздействие"
        if av < 3.0:
            return "заметное воздействие"
        return "сильное воздействие"

    zero_evi_share = float((metric_df["EVI_raw"] == 0).mean()) if not metric_df.empty else 1.0
    zero_weight_share = float((df_ref["aggregation_weight"] == 0).mean()) if not df_ref.empty else 1.0

    st.markdown("### Индикаторы и доказательства")
    st.caption(
        f"Режим EVI: `{evi_mode}` — {_evi_mode_ru(evi_mode)} "
        f"Технические упоминания {'исключены' if exclude_technical_mentions else 'учтены'} в итоговых цифрах. "
        f"База процентилей: {percentile_basis}."
    )
    tabs = st.tabs(
        [
            "1) IDI",
            "2) EMI",
            "3) MTI",
            "4) EVI",
            "5) IP",
        ]
    )

    with tabs[0]:
        st.markdown("**Идеологизированность (IDI)**")
        st.caption("Доля идеологических маркеров среди знаменательных слов контекста.")
        st.metric("IDI_raw", format(idi, fmt_raw))
        if show_percent_values:
            st.metric("IDI_percent_value", f"{idi*100:{fmt_pct}}%")
        if show_empirical_percentiles and "IDI_percentile" in metric_df.columns:
            st.metric("IDI_percentile", f"{float(metric_df['IDI_percentile'].mean()):.1f}")
        st.info(f"Уровень: {density_level(idi)}. Чем выше IDI, тем сильнее текст рамочно направляет интерпретацию.")
        with st.expander("Формула и объяснение", expanded=False):
            st.code("IDI = N_ideol / N_content", language="text")
            st.code(f"IDI = {int(n_ideol_sum)} / {int(n_content_sum)} = {idi:.3f}", language="text")
            st.write("`N_ideol` — число идеологических маркеров, `N_content` — знаменательные слова.")
        cc1, cc2 = st.columns(2)
        with cc1:
            st.plotly_chart(px.histogram(df_ref, x="IDI", nbins=20, template="plotly_dark", title="Распределение IDI"), use_container_width=True)
        with cc2:
            idi_by_outlet = df_ref.groupby("outlet_name", as_index=False)["IDI"].mean().sort_values("IDI", ascending=False)
            st.plotly_chart(px.bar(idi_by_outlet, x="outlet_name", y="IDI", template="plotly_dark", title="IDI по источникам"), use_container_width=True)
        with st.expander("Примеры из корпуса (развернуть)", expanded=False):
            _render_proof_contexts(df_ref, "found_ideol_markers", "Контексты с идеологическими маркерами")

    with tabs[1]:
        st.markdown("**Эмоциональность (EMI)**")
        st.caption("Взвешенная доля эмоциональных маркеров (слабые/средние/сильные).")
        st.metric("EMI_raw", format(emi, fmt_raw))
        if show_percent_values:
            st.metric("EMI_percent_value", f"{emi*100:{fmt_pct}}%")
        if show_empirical_percentiles and "EMI_percentile" in metric_df.columns:
            st.metric("EMI_percentile", f"{float(metric_df['EMI_percentile'].mean()):.1f}")
        st.info(f"Уровень: {density_level(emi)}. Чем выше EMI, тем сильнее эмоциональное давление текста.")
        with st.expander("Формула и объяснение", expanded=False):
            st.code("EMI = (1/3*N_e_w + 2/3*N_e_m + 1*N_e_s) / N_content", language="text")
            st.code(f"EMI = {weighted_emotion_sum:.2f} / {int(n_content_sum)} = {emi:.3f}", language="text")
            st.write("Сильные маркеры вносят наибольший вклад, слабые — наименьший.")
        cc1, cc2 = st.columns(2)
        with cc1:
            st.plotly_chart(px.histogram(df_ref, x="EMI", nbins=20, template="plotly_dark", title="Распределение EMI"), use_container_width=True)
        with cc2:
            emo_break = pd.DataFrame(
                [{"layer": "weak", "count": int(df_ref["N_e_w"].sum())}, {"layer": "medium", "count": int(df_ref["N_e_m"].sum())}, {"layer": "strong", "count": int(df_ref["N_e_s"].sum())}]
            )
            st.plotly_chart(px.bar(emo_break, x="layer", y="count", template="plotly_dark", title="Структура эмоциональных маркеров"), use_container_width=True)
        with st.expander("Примеры из корпуса (развернуть)", expanded=False):
            _render_proof_contexts(df_ref, "found_emotional_markers", "Контексты с эмоциональными маркерами")

    with tabs[2]:
        st.markdown("**Метафоричность (MTI)**")
        st.caption("Доля метафорических единиц среди знаменательных слов.")
        st.metric("MTI_raw", format(mti, fmt_raw))
        if show_percent_values:
            st.metric("MTI_percent_value", f"{mti*100:{fmt_pct}}%")
        if show_empirical_percentiles and "MTI_percentile" in metric_df.columns:
            st.metric("MTI_percentile", f"{float(metric_df['MTI_percentile'].mean()):.1f}")
        st.info(f"Уровень: {density_level(mti)}. Чем выше MTI, тем сильнее образная подача политической реальности.")
        with st.expander("Формула и объяснение", expanded=False):
            st.code("MTI = N_met / N_content", language="text")
            st.code(f"MTI = {int(n_met_sum)} / {int(n_content_sum)} = {mti:.3f}", language="text")
            st.write("Метафоры создают когнитивные рамки восприятия политических событий.")
        cc1, cc2 = st.columns(2)
        with cc1:
            st.plotly_chart(px.histogram(df_ref, x="MTI", nbins=20, template="plotly_dark", title="Распределение MTI"), use_container_width=True)
        with cc2:
            mti_year = df_ref.groupby("date", as_index=False)["MTI"].mean().head(60)
            st.plotly_chart(px.line(mti_year, x="date", y="MTI", template="plotly_dark", title="Динамика MTI (по дате)"), use_container_width=True)
        with st.expander("Примеры из корпуса (развернуть)", expanded=False):
            _render_proof_contexts(df_ref, "found_metaphor_markers", "Контексты с метафорическими маркерами")

    with tabs[3]:
        st.markdown("**Оценочный вектор (EVI_r)**")
        st.caption("EVI_raw от -10 до +10, нормирование: EVI_norm = EVI_raw / 5.")
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("EVI_raw (mean)", f"{evi_raw:.1f}")
        col_b.metric("EVI_norm (mean)", f"{evi_norm:.4f}")
        col_c.metric("EVI coarse", f"{evi_coarse}")
        st.info(f"Интерпретация: {evi_level(evi_norm)} отношение к референту.")
        with st.expander("Формула и объяснение", expanded=False):
            st.code("EVI_raw = P_r - N_r", language="text")
            st.code("EVI_norm = EVI_raw / 5", language="text")
            st.code(f"EVI_raw(mean) = {evi_raw:.3f}; EVI_norm(mean) = {evi_norm:.3f}", language="text")
            st.write("`P_r` — позитивные сигналы, `N_r` — негативные сигналы относительно выбранного референта.")
        cc1, cc2 = st.columns(2)
        with cc1:
            evi_dist = df_ref.groupby("EVI_raw", as_index=False).size().rename(columns={"size": "contexts"}).sort_values("EVI_raw")
            st.plotly_chart(px.bar(evi_dist, x="EVI_raw", y="contexts", template="plotly_dark", title="Распределение EVI_raw"), use_container_width=True)
        with cc2:
            score_df = df_ref[["positive_score", "negative_score"]].mean().reset_index()
            score_df.columns = ["component", "mean_score"]
            st.plotly_chart(px.bar(score_df, x="component", y="mean_score", template="plotly_dark", title="Средний вклад P_r и N_r"), use_container_width=True)
        with st.expander("Примеры из корпуса и пояснения (развернуть)", expanded=False):
            cols = [
                c
                for c in [
                    "context_id",
                    "matched_keywords",
                    "positive_score",
                    "negative_score",
                    "EVI_raw",
                    "EVI_norm",
                    "evi_explanation",
                    "positive_evidence_terms",
                    "negative_evidence_terms",
                    "notes",
                ]
                if c in df_ref.columns
            ]
            st.dataframe(df_ref[cols].head(40), use_container_width=True)
            if "evi_pos_markers" in df_ref.columns and "evi_neg_markers" in df_ref.columns:
                evi_proof = df_ref.copy()
                evi_proof["evi_markers"] = (
                    evi_proof["matched_keywords"].fillna("").astype(str)
                    + "; "
                    + evi_proof["evi_pos_markers"].fillna("").astype(str)
                    + "; "
                    + evi_proof["evi_neg_markers"].fillna("").astype(str)
                )
                _render_proof_contexts(evi_proof, "evi_markers", "Контексты с оценочными маркерами")
            if show_evi_rubric_details:
                rubric_cols = [c for c in ["context_id", "evi_evidence", "evi_pos_hits", "evi_neg_hits"] if c in df_ref.columns]
                if rubric_cols:
                    st.dataframe(df_ref[rubric_cols].head(30), use_container_width=True)

    with tabs[4]:
        st.markdown("**Воздействующий потенциал (IP_r)**")
        st.caption("Главный итог: показывает направление (плюс/минус) и силу влияния медиаобраза страны.")
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Итоговый индекс имиджа (IP_final)", format(ip_formula, fmt_ip))
        col_b.metric("Сила воздействия без знака (IP_abs_final)", format(ip_abs_formula, fmt_ip))
        col_c.metric("Контексты в расчете", f"{contexts_analyzed}")
        if show_empirical_percentiles and "IP_percentile" in metric_df.columns and "IP_abs_percentile" in metric_df.columns:
            p1, p2 = st.columns(2)
            p1.metric("IP_percentile", f"{float(metric_df['IP_percentile'].mean()):.1f}")
            p2.metric("IP_abs_percentile", f"{float(metric_df['IP_abs_percentile'].mean()):.1f}")
        st.metric("Итоговый знак имиджа", _sign_label(ip_formula))
        st.info(
            f"Как читать: сейчас это **{ip_level(ip_formula)}**. "
            f"`IP_final` отвечает за направление (положительный/отрицательный), "
            f"`IP_abs_final` — за силу воздействия (насколько ярко выражен образ)."
        )
        if abs(ip_formula) < 1e-12:
            reasons = []
            if zero_evi_share > 0.8:
                reasons.append("у большинства контекстов оценка EVI близка к нейтральной (EVI_raw = 0)")
            if zero_weight_share > 0.8:
                reasons.append("большая доля контекстов исключена весом S_r (технические/фоновые упоминания)")
            if contexts_analyzed == 0:
                reasons.append("после фильтрации не осталось контекстов для агрегирования")
            if not reasons:
                reasons.append("положительные и отрицательные контексты взаимно компенсировались")
            st.warning("Почему IP=0: " + "; ".join(reasons) + ".")
        with st.expander("Пояснение простыми словами", expanded=True):
            st.markdown(
                "- `Итоговый индекс имиджа (IP_final)`: главный результат. "
                "Плюс = скорее позитивный образ, минус = скорее негативный.\n"
                "- `Сила воздействия без знака (IP_abs_final)`: насколько сильное воздействие, "
                "даже если в корпусе есть и плюс, и минус.\n"
                "- `Контексты в расчете`: сколько контекстов реально вошло в итог после фильтров.\n"
                "- `Итоговый знак имиджа`: быстрый ответ, какой образ доминирует."
            )
        with st.expander("Формула и объяснение", expanded=False):
            st.code("IP_i = EVI_norm_i × (1 + IDI_i + EMI_i + MTI_i)", language="text")
            st.code("IP_final = Σ(S_i × IP_i) / ΣS_i", language="text")
            st.write(
                "`S_i` — вес значимости контекста для образа страны. "
                "Технические упоминания (например, просто локация в подписи) дают нулевой вес и не искажают итог."
            )
            st.write(f"contexts_analyzed={contexts_analyzed}, contexts_excluded={contexts_excluded}, technical_mentions_excluded={technical_mentions_excluded}")
            if warning_msg:
                st.warning(str(warning_msg))
        cc1, cc2 = st.columns(2)
        with cc1:
            st.plotly_chart(px.histogram(df_ref, x="IP_context", nbins=20, template="plotly_dark", title="Распределение IP_context"), use_container_width=True)
        with cc2:
            ip_year = df_ref.groupby("date", as_index=False)["IP_context"].mean().head(60)
            st.plotly_chart(px.line(ip_year, x="date", y="IP_context", template="plotly_dark", title="Динамика IP_context"), use_container_width=True)
        with st.expander("Примеры из корпуса (развернуть)", expanded=False):
            _render_proof_contexts(df_ref, "matched_keywords", "Контексты для расчета IP")

    if show_salience_diagnostics:
        with st.expander("Диагностика референтной значимости (S_r)", expanded=False):
            sal_cols = [c for c in ["context_id", "referent_salience", "salience_label", "is_technical_mention", "technical_mention_reason", "salience_explanation"] if c in df_ref.columns]
            if sal_cols:
                st.dataframe(df_ref[sal_cols].head(40), use_container_width=True)

    st.markdown("### Анализ по категориям ключевых слов")
    cat_rows = []
    for _, r in df_ref.iterrows():
        cats = [x.strip() for x in str(r.get("keyword_category", "other")).split(";") if x.strip()]
        if not cats:
            cats = ["other"]
        for cat in cats:
            cat_rows.append(
                {
                    "category": cat,
                    "IDI": r["IDI"],
                    "EMI": r["EMI"],
                    "MTI": r["MTI"],
                    "EVI_raw": r.get("EVI_raw", 0),
                    "EVI_norm": r.get("EVI_norm", 0),
                    "S_r": r.get("referent_salience", 1),
                    "IP_context": r.get("IP_context", 0),
                    "IP_context_abs": r.get("IP_context_abs", 0),
                    "aggregation_weight": r.get("aggregation_weight", 0),
                }
            )
    cat_df = pd.DataFrame(cat_rows)
    if not cat_df.empty:
        cat_agg = (
            cat_df.groupby("category", as_index=False)
            .agg(
                contexts=("category", "count"),
                IDI=("IDI", "mean"),
                EMI=("EMI", "mean"),
                MTI=("MTI", "mean"),
                EVI_raw=("EVI_raw", "mean"),
                EVI_norm=("EVI_norm", "mean"),
                S_r=("S_r", "mean"),
                IP_context=("IP_context", "mean"),
                IP_context_abs=("IP_context_abs", "mean"),
                aggregation_weight=("aggregation_weight", "mean"),
            )
            .sort_values("contexts", ascending=False)
        )
        st.dataframe(cat_agg, use_container_width=True)
        fig_cat = px.bar(cat_agg, x="category", y="IP_context", color="contexts", template="plotly_dark", title="IP_context по категориям ключевых слов")
        st.plotly_chart(fig_cat, use_container_width=True)

    st.markdown("### Итоговая интерпретация")
    st.info(
        f"Формируемый имидж референта: **{_sign_label(ip_formula)}** "
        f"(IP_final={ip_formula:.4f}, IP_abs_final={ip_abs_formula:.4f})."
    )
    with st.expander("Формулы и как читать итог", expanded=False):
        st.code("IDI = N_ideol / N_content", language="text")
        st.code("EMI = (1/3*N_e_w + 2/3*N_e_m + N_e_s) / N_content", language="text")
        st.code("MTI = N_met / N_content", language="text")
        st.code("EVI_raw = P_r - N_r;  EVI_norm = EVI_raw / 5", language="text")
        st.code("IP_i = EVI_norm_i × (1 + IDI_i + EMI_i + MTI_i)", language="text")
        st.code("IP_final = Σ(S_i × IP_i) / ΣS_i", language="text")
        st.write("Плюс IP = позитивный образ, минус IP = негативный образ, модуль IP = сила воздействия.")

    frame_df, strategy_df = _dominant_frames_and_strategies(df_ref)
    st.markdown("### Доминирующие фреймы и дискурсивные стратегии")
    f1, f2 = st.columns(2)
    with f1:
        st.markdown("**Фреймы**")
        if frame_df.empty:
            st.caption("Выраженные фреймы не выявлены.")
        else:
            st.dataframe(frame_df.head(8), use_container_width=True)
            st.plotly_chart(px.bar(frame_df.head(8), x="frame", y="score", template="plotly_dark", title="Топ фреймов"), use_container_width=True)
    with f2:
        st.markdown("**Стратегии**")
        if strategy_df.empty:
            st.caption("Выраженные стратегии не выявлены.")
        else:
            st.dataframe(strategy_df.head(8), use_container_width=True)
            st.plotly_chart(px.bar(strategy_df.head(8), x="strategy", y="score", template="plotly_dark", title="Топ стратегий"), use_container_width=True)

    if use_calibration_anchors and (out_dir / "calibration_report.xlsx").exists():
        st.markdown("### Calibration Anchors")
        cal = pd.read_excel(out_dir / "calibration_report.xlsx")
        st.dataframe(cal, use_container_width=True)
        if not cal.empty and "mean_IP_abs" in cal.columns:
            base = float(cal["mean_IP_abs"].min()) if float(cal["mean_IP_abs"].min()) > 0 else 1e-9
            ratio = float(ip_abs_formula / base)
            st.info(f"Сила текущего подкорпуса относительно минимального calibration-baseline: x{ratio:.2f}.")

    st.markdown("### Контексты и объяснения")
    ctx_cols = [
        c
        for c in [
            "context_id",
            "doc_id",
            "ref_country",
            "matched_keywords",
            "referent_salience",
            "salience_label",
            "positive_score",
            "negative_score",
            "EVI_raw",
            "EVI_norm",
            "discursive_energy",
            "IP_context",
            "IP_context_abs",
            "aggregation_weight",
            "IP_formula_version",
            "evi_explanation",
        ]
        if c in df_ref.columns
    ]
    st.dataframe(df_ref[ctx_cols].head(100), use_container_width=True)
    st.markdown("**Highlighted contexts (пруфы по каждому контексту)**")
    for _, r in df_ref.head(20).iterrows():
        terms = _split_marker_cell(str(r.get("matched_keywords", "")))
        terms += _split_marker_cell(str(r.get("positive_evidence_terms", "")))
        terms += _split_marker_cell(str(r.get("negative_evidence_terms", "")))
        terms = [t for t in sorted(set(terms)) if t]
        st.caption(
            f"{r.get('context_id','')} | ref={r.get('ref_country','')} | "
            f"S_r={r.get('referent_salience','')} | EVI_raw={r.get('EVI_raw','')} | IP_i={r.get('IP_context','')}"
        )
        st.markdown(
            f"<div style='padding:10px;border:1px solid #334155;border-radius:8px;line-height:1.6'>{_highlight_terms_html(str(r.get('context_text','')), terms)}</div>",
            unsafe_allow_html=True,
        )


def zip_dir_bytes(dir_path: Path) -> bytes:
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        for fp in sorted(dir_path.rglob("*")):
            if fp.is_file():
                zf.write(fp, arcname=fp.relative_to(dir_path).as_posix())
    mem.seek(0)
    return mem.read()


def read_csv_preview(path: Path, limit: int = 20) -> List[List[str]]:
    rows = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            rows.append(row)
            if i >= limit:
                break
    return rows


def show_charts(out_dir: Path) -> None:
    st.subheader("Диаграммы")
    p_source = out_dir / "stage1_profile_source.csv"
    p_country = out_dir / "stage1_profile_country.csv"
    p_year = out_dir / "stage1_profile_year.csv"
    p_pp = out_dir / "stage7_persuasion_summary_country_year.csv"

    c1, c2 = st.columns(2)
    if p_source.exists():
        df = pd.read_csv(p_source).sort_values("doc_count", ascending=False).set_index("source")
        with c1:
            st.markdown("**Распределение по источникам**")
            st.bar_chart(df["doc_count"])
    if p_country.exists():
        df = pd.read_csv(p_country).sort_values("doc_count", ascending=False).set_index("country")
        with c2:
            st.markdown("**Распределение по странам**")
            st.bar_chart(df["doc_count"])

    if p_year.exists():
        df = pd.read_csv(p_year).sort_values("year").set_index("year")
        st.markdown("**Динамика по годам**")
        st.line_chart(df["doc_count"])

    if p_pp.exists():
        df = pd.read_csv(p_pp).sort_values(["country", "year"])
        st.markdown("**Воздействующий потенциал (IP) по годам**")
        value_col = "avg_IP" if "avg_IP" in df.columns else "avg_PP_weighted"
        pivot_pp = df.pivot(index="year", columns="country", values=value_col).fillna(0.0)
        st.line_chart(pivot_pp)
        st.markdown("**Индексы IDI / EMI / EVI / MTI (средние по странам)**")
        idx = df.groupby("country")[["avg_IDI", "avg_EMI", "avg_EVI", "avg_MTI"]].mean().reset_index().set_index("country")
        st.bar_chart(idx)


def _get_lexicon(name: str, fallback: dict) -> dict:
    return getattr(core, name, fallback)


def build_five_indicator_df(docs: List[core.Doc]) -> pd.DataFrame:
    ideology_fallback = {
        "ideol": {"sovereignty", "kedaulatan", "pancasila", "unity", "stability", "national", "суверенитет", "единство", "стабильность"},
        "prec": {"washington", "beijing", "moscow", "putin", "biden", "trump", "xi", "сша", "россия", "китай", "кпк"},
        "slog": {"national", "interest", "stability", "security", "unity", "национальный", "интерес", "безопасность"},
        "dich": {"we", "they", "our", "their", "us", "them", "kita", "mereka", "мы", "они", "наш", "их"},
    }
    emotion_fallback = {
        "weak": {"concern", "worry", "uncertain", "khawatir", "cemas", "тревога", "сомнение"},
        "medium": {"fear", "anger", "pride", "hope", "trust", "marah", "bangga", "страх", "гнев", "гордость", "надежда"},
        "strong": {"panic", "threat", "catastrophe", "shock", "outrage", "ancaman", "krisis", "паника", "угроза", "катастрофа"},
    }
    eval_fallback = {
        "rational": {"strategic", "important", "legal", "effective", "necessary", "penting", "стратегический", "важный", "законный"},
        "emotional": {"outrageous", "heroic", "shameful", "brutal", "berbahaya", "ужасный", "героический", "агрессивный"},
        "explicit": {"must", "should", "need", "harus", "wajib", "perlu", "должен", "должны", "нужно"},
        "implicit": {"allegedly", "claimed", "reportedly", "so-called", "seolah", "якобы", "будто"},
    }
    metaphor_fallback = {
        "weak": {"wave", "path", "bridge", "shield", "gelombang", "jembatan", "волна", "путь", "мост"},
        "medium": {"battle", "arena", "storm", "engine", "medan", "badai", "битва", "арена", "шторм"},
        "strong": {"chess", "wounded", "organism", "frontline", "perang", "шахмат", "ранен", "фронт"},
    }

    ideology = _get_lexicon("IDEOLOGY_MARKERS", ideology_fallback)
    emotion = _get_lexicon("EMOTION_MARKERS", emotion_fallback)
    evaluation = _get_lexicon("EVALUATION_MARKERS", eval_fallback)
    metaphor = _get_lexicon("METAPHOR_MARKERS", metaphor_fallback)

    referent_aliases = {
        "usa": {
            "usa", "us", "u.s", "u.s.", "united", "states", "america", "american", "washington",
            "white", "house", "pentagon", "state", "department", "congress", "senate", "biden", "trump",
            "сша", "соединенный", "штат", "америка", "американский", "вашингтон", "белый", "дом", "пентагон",
        },
        "russia": {
            "russia", "russian", "rusia", "moscow", "kremlin", "putin", "lavrov",
            "россия", "российский", "москва", "кремль", "путин", "лавров",
        },
        "china": {
            "china", "chinese", "cina", "tiongkok", "beijing", "xi", "jinping", "ccp", "prc",
            "belt", "road", "yuan", "pla", "xinjiang", "taiwan", "hong", "kong",
            "китай", "китайский", "пекин", "кпк", "юань", "ся", "цзиньпин", "пояс", "путь",
            "тайвань", "гонконг", "синьцзян",
        },
    }
    sentence_splitter = re.compile(r"(?<=[\.\!\?])\s+")

    def content_len(tokens: List[str]) -> int:
        return sum(1 for t in tokens if core.is_content(t))

    rows = []
    for d in docs:
        toks = d.tokens
        if not toks:
            continue
        c = {}
        for t in toks:
            c[t] = c.get(t, 0) + 1
        W = max(content_len(toks), 1)

        # IDI share in [0,1]
        n_ideol = sum(c.get(t, 0) for t in ideology["ideol"]) + sum(c.get(t, 0) for t in ideology["prec"]) + sum(c.get(t, 0) for t in ideology["slog"]) + sum(c.get(t, 0) for t in ideology["dich"])
        IDI_share = min(max(n_ideol / W, 0.0), 1.0)
        IDI = IDI_share

        # EMI share in [0,1]
        e_w = sum(c.get(t, 0) for t in emotion["weak"])
        e_m = sum(c.get(t, 0) for t in emotion["medium"])
        e_s = sum(c.get(t, 0) for t in emotion["strong"])
        EMI_share = min(max(((e_w / 3.0) + (2.0 * e_m / 3.0) + e_s) / W, 0.0), 1.0)
        EMI = EMI_share

        # MTI share in [0,1]
        n_met = sum(c.get(t, 0) for t in metaphor["weak"]) + sum(c.get(t, 0) for t in metaphor["medium"]) + sum(c.get(t, 0) for t in metaphor["strong"])
        MTI_share = min(max(n_met / W, 0.0), 1.0)
        MTI = MTI_share

        # EVI (-2..2) on referent-focused expanded context
        aliases = referent_aliases.get(d.primary_country, set())
        sents = [s.strip() for s in sentence_splitter.split(d.text) if s.strip()]
        selected_tokens = []
        for i, s in enumerate(sents):
            stoks = core.preprocess_tokens(core.tokenize(s), use_lemma=True)
            if any(tok in aliases for tok in stoks):
                lo = max(0, i - 1)
                hi = min(len(sents), i + 2)
                for j in range(lo, hi):
                    selected_tokens.extend(core.preprocess_tokens(core.tokenize(sents[j]), use_lemma=True))
        context_toks = selected_tokens if selected_tokens else toks
        pos_lex = getattr(core, "SENT_POS_BY_LANG", {}).get(d.language, getattr(core, "SENT_POS", set()))
        neg_lex = getattr(core, "SENT_NEG_BY_LANG", {}).get(d.language, getattr(core, "SENT_NEG", set()))
        ref_w = max(content_len(context_toks), 1)
        score = (sum(1 for t in context_toks if t in pos_lex) - sum(1 for t in context_toks if t in neg_lex)) / ref_w
        if score <= -0.02:
            EVI = -2
        elif score < -0.005:
            EVI = -1
        elif score < 0.005:
            EVI = 0
        elif score < 0.02:
            EVI = 1
        else:
            EVI = 2

        # IP in [-6, +6]
        IP = (IDI + EMI + MTI) * EVI
        IP = max(min(IP, 6.0), -6.0)

        rows.append(
            {
                "source": d.source,
                "country": d.primary_country,
                "year": d.year,
                "IDI": IDI,
                "EMI": EMI,
                "EVI": EVI,
                "MTI": MTI,
                "IDI_share": IDI_share,
                "EMI_share": EMI_share,
                "MTI_share": MTI_share,
                "IP": IP,
                "PP": IP,  # backward-compat alias for older UI blocks
            }
        )
    return pd.DataFrame(rows)


def _collect_marker_hits_for_doc(d: core.Doc) -> dict:
    ideology_fallback = {
        "ideol": {"sovereignty", "kedaulatan", "pancasila", "unity", "stability", "national", "суверенитет", "единство", "стабильность"},
        "prec": {"washington", "beijing", "moscow", "putin", "biden", "trump", "xi", "сша", "россия", "китай", "кпк"},
        "slog": {"national", "interest", "stability", "security", "unity", "национальный", "интерес", "безопасность"},
        "dich": {"we", "they", "our", "their", "us", "them", "kita", "mereka", "мы", "они", "наш", "их"},
    }
    emotion_fallback = {
        "weak": {"concern", "worry", "uncertain", "khawatir", "cemas", "тревога", "сомнение"},
        "medium": {"fear", "anger", "pride", "hope", "trust", "marah", "bangga", "страх", "гнев", "гордость", "надежда"},
        "strong": {"panic", "threat", "catastrophe", "shock", "outrage", "ancaman", "krisis", "паника", "угроза", "катастрофа"},
    }
    eval_fallback = {
        "rational": {"strategic", "important", "legal", "effective", "necessary", "penting", "стратегический", "важный", "законный"},
        "emotional": {"outrageous", "heroic", "shameful", "brutal", "berbahaya", "ужасный", "героический", "агрессивный"},
        "explicit": {"must", "should", "need", "harus", "wajib", "perlu", "должен", "должны", "нужно"},
        "implicit": {"allegedly", "claimed", "reportedly", "so-called", "seolah", "якобы", "будто"},
    }
    metaphor_fallback = {
        "weak": {"wave", "path", "bridge", "shield", "gelombang", "jembatan", "волна", "путь", "мост"},
        "medium": {"battle", "arena", "storm", "engine", "medan", "badai", "битва", "арена", "шторм"},
        "strong": {"chess", "wounded", "organism", "frontline", "perang", "шахмат", "ранен", "фронт"},
    }
    ideology = _get_lexicon("IDEOLOGY_MARKERS", ideology_fallback)
    emotion = _get_lexicon("EMOTION_MARKERS", emotion_fallback)
    evaluation = _get_lexicon("EVALUATION_MARKERS", eval_fallback)
    metaphor = _get_lexicon("METAPHOR_MARKERS", metaphor_fallback)

    cnt = {}
    for t in d.tokens:
        cnt[t] = cnt.get(t, 0) + 1

    def hits(words: set[str]) -> dict:
        return {w: cnt[w] for w in words if w in cnt}

    return {
        "IDI": {
            "ideol": hits(set(ideology["ideol"])),
            "prec": hits(set(ideology["prec"])),
            "slog": hits(set(ideology["slog"])),
            "dich": hits(set(ideology["dich"])),
        },
        "EMI": {
            "weak": hits(set(emotion["weak"])),
            "medium": hits(set(emotion["medium"])),
            "strong": hits(set(emotion["strong"])),
        },
        "EVI": {
            "rational": hits(set(evaluation["rational"])),
            "emotional": hits(set(evaluation["emotional"])),
            "explicit": hits(set(evaluation["explicit"])),
            "implicit": hits(set(evaluation["implicit"])),
        },
        "MTI": {
            "weak": hits(set(metaphor["weak"])),
            "medium": hits(set(metaphor["medium"])),
            "strong": hits(set(metaphor["strong"])),
        },
    }


def _highlight_terms_html(text: str, terms: List[str]) -> str:
    safe = html.escape(text)
    for t in sorted(set(terms), key=len, reverse=True):
        if len(t.strip()) < 2:
            continue
        p = re.compile(rf"(?i)\b({re.escape(t)})\b")
        safe = p.sub(r"<mark style='background:#ffd166;color:#111;padding:1px 3px;border-radius:3px;'>\1</mark>", safe)
    return safe.replace("\n", "<br>")


def _extract_context_blocks(text: str, terms: List[str], max_blocks: int = 15) -> List[dict]:
    if not text.strip() or not terms:
        return []
    sentence_splitter = re.compile(r"(?<=[\.\!\?])\s+")
    sents = [s.strip() for s in sentence_splitter.split(text) if s.strip()]
    if not sents:
        return []
    pats = [re.compile(rf"(?i)\b{re.escape(t)}\b") for t in sorted(set(terms), key=len, reverse=True) if t.strip()]
    blocks = []
    seen = set()
    for i, s in enumerate(sents):
        matched = [t for t, p in zip(sorted(set(terms), key=len, reverse=True), pats) if p.search(s)]
        if not matched:
            continue
        key = (i, tuple(sorted(matched)))
        if key in seen:
            continue
        seen.add(key)
        prev_sent = sents[i - 1] if i - 1 >= 0 else ""
        next_sent = sents[i + 1] if i + 1 < len(sents) else ""
        expanded = " ".join([x for x in [prev_sent, s, next_sent] if x]).strip()
        blocks.append({"hit_sentence": s, "expanded_context": expanded, "markers": ", ".join(sorted(set(matched)))})
        if len(blocks) >= max_blocks:
            break
    return blocks


def show_five_indicator_charts(docs: List[core.Doc], selected_indicator: str) -> None:
    df = build_five_indicator_df(docs)
    if df.empty:
        return

    def lvl(v: float, bounds: tuple[float, float, float, float]) -> str:
        b1, b2, b3, b4 = bounds
        if v < b1:
            return "очень низкий"
        if v < b2:
            return "низкий"
        if v < b3:
            return "средний"
        if v < b4:
            return "высокий"
        return "очень высокий"

    def pp_lvl(v: float) -> str:
        a = abs(v)
        if a < 0.5:
            return "очень низкий"
        if a < 1.5:
            return "низкий"
        if a < 3.0:
            return "средний"
        if a < 4.5:
            return "высокий"
        return "очень высокий"

    st.subheader("5 обязательных индикаторов")
    avg = df[["IDI", "EMI", "EVI", "MTI", "IP"]].mean()

    indicator_specs = [
        {
            "code": "IDI",
            "label": "Идеологичность (IDI)",
            "value": float(avg["IDI"]),
            "bounds": (0.03, 0.06, 0.11, 0.16),
            "max_scale": 1.0,
            "about": "Показывает, насколько текст насыщен идеологическими рамками: «свои/чужие», суверенитет, ценностные формулы.",
            "meaning_low": "Низкий IDI: текст больше информирует, чем идеологически направляет.",
            "meaning_high": "Высокий IDI: текст заметно формирует «правильную» интерпретацию через ценностные маркеры.",
            "scale": "Шкала (доля 0..1): 0.00–0.02 низкий, 0.03–0.05 сниженный, 0.06–0.10 средний, 0.11–0.15 высокий, >0.15 очень высокий.",
        },
        {
            "code": "EMI",
            "label": "Эмоциональность (EMI)",
            "value": float(avg["EMI"]),
            "bounds": (0.03, 0.06, 0.11, 0.16),
            "max_scale": 1.0,
            "about": "Показывает силу эмоционального давления (взвешенно: слабые=1/3, средние=2/3, сильные=1.0).",
            "meaning_low": "Низкий EMI: текст подан более нейтрально и рационально.",
            "meaning_high": "Высокий EMI: текст активно воздействует на эмоции аудитории.",
            "scale": "Шкала (доля 0..1): 0.00–0.02 низкий, 0.03–0.05 сниженный, 0.06–0.10 средний, 0.11–0.15 высокий, >0.15 очень высокий.",
        },
        {
            "code": "EVI",
            "label": "Оценка объекта (EVI)",
            "value": float(avg["EVI"]),
            "bounds": (-1.5, -0.5, 0.5, 1.5),
            "max_scale": 2.0,
            "about": "Показывает, насколько явно и интенсивно объект описывается как «хороший/плохой».",
            "meaning_low": "Низкий EVI: мало явных оценок, тон ближе к констатации.",
            "meaning_high": "Высокий EVI: оценки систематичны и задают нужное отношение к объекту.",
            "scale": "Шкала дискретная: -2 (резко негативно), -1 (негативно), 0 (нейтрально), +1 (позитивно), +2 (резко позитивно).",
        },
        {
            "code": "MTI",
            "label": "Метафоричность (MTI)",
            "value": float(avg["MTI"]),
            "bounds": (0.03, 0.06, 0.11, 0.16),
            "max_scale": 1.0,
            "about": "Показывает, насколько активно используются образные модели (метафоры) для объяснения политики.",
            "meaning_low": "Низкий MTI: текст говорит преимущественно буквально, без сильной образности.",
            "meaning_high": "Высокий MTI: метафоры заметно направляют восприятие и упрощают сложные темы.",
            "scale": "Шкала (доля 0..1): 0.00–0.02 низкий, 0.03–0.05 сниженный, 0.06–0.10 средний, 0.11–0.15 высокий, >0.15 очень высокий.",
        },
        {
            "code": "IP",
            "label": "Воздействующий потенциал (IP)",
            "value": float(avg["IP"]),
            "bounds": (-3.0, -0.5, 0.5, 3.0),
            "max_scale": 6.0,
            "about": "Итоговый индекс воздействия: IP = (IDI + EMI + MTI) × EVI.",
            "meaning_low": "Низкий IP: текст слабо влияет на установки читателя.",
            "meaning_high": "Высокий IP: текст вероятно формирует устойчивое отношение к теме/стране.",
            "scale": "Диапазон: от -6 до +6. Знак показывает направление (минус/плюс), модуль — силу воздействия.",
        },
    ]

    selected_spec = next((x for x in indicator_specs if x["code"] == selected_indicator), indicator_specs[0])
    value = selected_spec["value"]
    bounds = selected_spec["bounds"]
    level = pp_lvl(value) if selected_spec["code"] == "IP" else lvl(value, bounds)
    if selected_spec["code"] == "EVI":
        normalized = abs(value) / selected_spec["max_scale"]
    elif selected_spec["code"] == "IP":
        normalized = abs(value) / selected_spec["max_scale"]
    else:
        normalized = value / selected_spec["max_scale"]
    normalized = max(0.0, min(normalized, 1.0))
    st.markdown(f"### {selected_spec['label']}")
    if selected_spec["code"] == "EVI":
        st.metric(selected_spec["code"], f"{int(round(value))}")
    else:
        st.metric(selected_spec["code"], f"{value:.3f}")
    st.progress(normalized)
    st.caption(selected_spec["about"])
    st.markdown(f"- {selected_spec['scale']}\n- {selected_spec['meaning_low']}\n- {selected_spec['meaning_high']}")
    st.info(f"Частный случай (по корпусу): текущее значение `{value:.3f}` — это **{level} уровень** по данному индикатору.")

    st.markdown("### Маркеры в анализируемом тексте")
    doc_labels = [f"{i+1}. {d.source} | {d.year} | {d.primary_country} | {d.title[:55]}" for i, d in enumerate(docs)]
    chosen = st.selectbox("Выберите текст для детального разбора", options=doc_labels, index=0)
    idx = doc_labels.index(chosen)
    dsel = docs[idx]
    df_doc = build_five_indicator_df([dsel])
    if not df_doc.empty:
        v = float(df_doc.iloc[0]["IP" if selected_spec["code"] == "IP" else selected_spec["code"]])
        l = pp_lvl(v) if selected_spec["code"] == "IP" else lvl(v, selected_spec["bounds"])
        shown_v = str(int(round(v))) if selected_spec["code"] == "EVI" else f"{v:.3f}"
        st.success(f"Текущий случай: `{selected_spec['code']}={shown_v}` ({l} уровень).")

    if selected_spec["code"] == "IP":
        st.markdown("**Прозрачная формула интегративного индикатора**")
        st.code("IP = (IDI + EMI + MTI) × EVI", language="text")
        if not df_doc.empty:
            row0 = df_doc.iloc[0]
            st.code(
                f"IP = ({row0['IDI']:.3f} + {row0['EMI']:.3f} + {row0['MTI']:.3f}) × {int(round(row0['EVI']))} = {row0['IP']:.3f}",
                language="text",
            )

    marker_hits = _collect_marker_hits_for_doc(dsel)
    marker_terms = []
    marker_rows = []
    if selected_spec["code"] in marker_hits:
        for cat, m in marker_hits[selected_spec["code"]].items():
            for term, n in sorted(m.items(), key=lambda x: x[1], reverse=True):
                marker_terms.append(term)
                marker_rows.append({"category": cat, "marker": term, "count": n})
    if marker_rows:
        st.dataframe(pd.DataFrame(marker_rows), use_container_width=True)
        st.markdown("**Подсветка маркеров в тексте**")
        st.markdown(
            f"<div style='padding:12px;border:1px solid #3a3a3a;border-radius:8px;max-height:320px;overflow:auto;line-height:1.6'>{_highlight_terms_html(dsel.text, marker_terms)}</div>",
            unsafe_allow_html=True,
        )
        st.markdown("**Контекстные блоки (предложение с маркером ±1 предложение)**")
        blocks = _extract_context_blocks(dsel.text, marker_terms, max_blocks=12)
        if blocks:
            for i, b in enumerate(blocks, start=1):
                st.markdown(f"{i}. Маркеры: `{b['markers']}`")
                st.markdown(
                    f"<div style='padding:10px;border:1px solid #334155;border-radius:8px;line-height:1.6'>{_highlight_terms_html(b['expanded_context'], marker_terms)}</div>",
                    unsafe_allow_html=True,
                )
        else:
            st.caption("Для выбранного индикатора контекстные блоки в этом тексте не найдены.")
    else:
        st.warning("Для этого индикатора в выбранном тексте маркеры не найдены.")

    col_name = "IP" if selected_indicator == "IP" else selected_indicator
    title_name = "IP" if selected_indicator == "IP" else selected_indicator

    st.markdown(f"### Графики индикатора {title_name}")
    c1, c2 = st.columns(2)

    # 1) Распределение выбранного индикатора по документам
    with c1:
        fig_hist = px.histogram(
            df,
            x=col_name,
            nbins=20,
            template="plotly_dark",
            title=f"Распределение {title_name} по документам",
        )
        fig_hist.update_layout(height=360, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_hist, use_container_width=True)

    # 2) Среднее по странам
    with c2:
        by_country = df.groupby("country", as_index=False)[col_name].mean().sort_values(col_name, ascending=False)
        fig_country = px.bar(
            by_country,
            x="country",
            y=col_name,
            template="plotly_dark",
            title=f"Средний {title_name} по странам",
        )
        fig_country.update_layout(height=360, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_country, use_container_width=True)

    # 3) Динамика по годам
    by_year = df.groupby(["year", "country"], as_index=False)[col_name].mean()
    fig_line = px.line(
        by_year,
        x="year",
        y=col_name,
        color="country",
        markers=True,
        template="plotly_dark",
        title=f"Динамика {title_name} по годам",
    )
    fig_line.update_layout(height=360, margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig_line, use_container_width=True)

    # 4) Специальный график для дискретной шкалы EVI
    if selected_indicator == "EVI":
        evi_dist = (
            df.groupby("EVI", as_index=False)
            .size()
            .rename(columns={"size": "doc_count"})
            .sort_values("EVI")
        )
        fig_evi = px.bar(
            evi_dist,
            x="EVI",
            y="doc_count",
            template="plotly_dark",
            title="Распределение оценок EVI (-2..2)",
        )
        fig_evi.update_layout(height=320, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_evi, use_container_width=True)


def run_analysis(docs: List[core.Doc], out_dir: Path, top_n: int, kwic_window: int, kwic_max: int, colloc_window: int, colloc_min: int, top_n_logodds: int, dedup: bool, near_dup_jaccard: float, near_dup_hamming: int):
    dedup_stats = {
        "total_docs_before_dedup": len(docs),
        "exact_duplicates_removed": 0,
        "near_duplicates_removed": 0,
        "total_docs_after_dedup": len(docs),
    }

    if dedup:
        docs, dedup_stats = core.deduplicate_docs(
            docs,
            near_dup_jaccard=near_dup_jaccard,
            simhash_hamming=near_dup_hamming,
        )

    core.write_rows(out_dir / "stage1_dedup_stats.csv", ["metric", "value"], [[k, v] for k, v in dedup_stats.items()])

    if not docs:
        raise RuntimeError("После фильтрации/дедупликации не осталось документов для анализа.")

    core.stage1_corpus_profile(docs, out_dir)
    core.stage2_quantitative(docs, out_dir, top_n=top_n, kwic_window=kwic_window, kwic_max=kwic_max, colloc_window=colloc_window, colloc_min=colloc_min)
    core.stage3_qualitative(docs, out_dir)
    core.stage4_prognostic(docs, out_dir)
    core.stage5_representativeness(docs, out_dir)
    core.stage6_significance(docs, out_dir, top_n_logodds=top_n_logodds)
    # Backward compatibility: some deployed repos may still use an older analyzer core.
    if hasattr(core, "stage7_persuasion_indicator_model"):
        core.stage7_persuasion_indicator_model(docs, out_dir)

    return dedup_stats, len(docs), docs


def main() -> None:
    st.set_page_config(page_title="Mediatext analyzator", layout="wide")
    st.title("Mediatext analyzator")
    st.caption(f"Индикаторная модель лингвопрагматического анализа воздействующего потенциала политического медиатекста. Build: {APP_BUILD}")

    with st.sidebar:
        st.header("Параметры")
        min_year = st.number_input("Минимальный год", min_value=2000, max_value=2100, value=2022)
        max_year = st.number_input("Максимальный год", min_value=2000, max_value=2100, value=2026)
        analysis_mode = st.selectbox(
            "Режим анализа",
            options=["Стандартный (5 индикаторов)", "Расширенный (корпусный)", "Референтный (China/USA/Russia)"],
            index=0,
        )
        referent_evi_mode = "suggested"
        referent_target = "China"
        referent_category_default = "all"
        indicator_tab = "IDI"

        dedup = True
        use_lemma = True
        near_dup_jaccard = 0.92
        near_dup_hamming = 3
        top_n = 250
        kwic_window = 7
        kwic_max = 12000
        colloc_window = 5
        colloc_min = 5
        top_n_logodds = 120

        if analysis_mode == "Референтный (China/USA/Russia)":
            referent_evi_mode = st.selectbox(
                "EVI режим (референтный анализ)",
                options=["fine", "coarse", "suggested", "manual"],
                index=0,
                help="fine: EVI_raw -10..10; coarse: legacy -2..2 с маппингом; suggested: авто-rubric; manual: из evi_manual.csv",
            )
            referent_target = st.selectbox(
                "Целевой референт (витрина)",
                options=["China", "USA", "Russia"],
                index=0,
                help="Контексты извлекаются по всем референтам, здесь выбирается фокус на странице.",
            )
            referent_category_default = st.selectbox(
                "Категория ключевых слов",
                options=["all", "Leadership", "Economy", "Security", "Culture", "other"],
                index=0,
            )
            exclude_technical_mentions = st.toggle(
                "Исключать технические упоминания из итогов",
                value=True,
                help="Если включено, контексты с S_r=0 не входят в итоговые агрегированные метрики.",
            )
            show_evi_rubric_details = st.toggle("Показывать детали расчета EVI", value=False)
            show_salience_diagnostics = st.toggle("Показывать диагностику значимости (S_r)", value=True)
            display_precision = st.selectbox("Display precision", options=[4, 6, 8], index=1)
            show_percent_values = st.toggle("Show percent values", value=True)
            show_empirical_percentiles = st.toggle("Show empirical percentiles", value=True)
            percentile_basis = st.selectbox(
                "Percentile basis",
                options=["full corpus", "selected media_country", "selected ref_country", "media_country × ref_country"],
                index=0,
            )
            ip_formula_mode = st.selectbox(
                "IP formula mode",
                options=["updated: EVI_norm * (1 + IDI + EMI + MTI)", "legacy: (IDI + EMI + MTI) * EVI_norm"],
                index=0,
            )
            aggregation_mode = st.selectbox(
                "Aggregation mode",
                options=["weighted by S_r", "unweighted valid contexts"],
                index=0,
            )
            use_calibration_anchors = st.toggle("Use calibration anchors in interpretation", value=False)
            st.caption("Для manual режима загрузите evi_manual.csv ниже в основном окне.")
        else:
            exclude_technical_mentions = True
            show_evi_rubric_details = False
            show_salience_diagnostics = False
            display_precision = 6
            show_percent_values = True
            show_empirical_percentiles = True
            percentile_basis = "full corpus"
            ip_formula_mode = "updated: EVI_norm * (1 + IDI + EMI + MTI)"
            aggregation_mode = "weighted by S_r"
            use_calibration_anchors = False
            indicator_tab = st.selectbox(
                "Вкладка индикатора",
                options=["IDI", "EMI", "EVI", "MTI", "IP"],
                index=0,
                help="Выберите индикатор для подробного разбора и подсветки маркеров в тексте.",
            )
            dedup = st.checkbox("Dedup (exact + near)", value=True)
            use_lemma = st.checkbox("Лемматизация (легкая)", value=True)

            with st.expander("Расширенные настройки", expanded=False):
                near_dup_jaccard = st.slider("Near-dup Jaccard", min_value=0.80, max_value=0.99, value=0.92, step=0.01)
                near_dup_hamming = st.slider("Near-dup SimHash Hamming", min_value=1, max_value=8, value=3)
                top_n = st.number_input("Top-N частот/коллокаций", min_value=50, max_value=1000, value=250)
                kwic_window = st.number_input("KWIC окно", min_value=3, max_value=20, value=7)
                kwic_max = st.number_input("KWIC максимум строк", min_value=500, max_value=50000, value=12000)
                colloc_window = st.number_input("Collocation окно", min_value=2, max_value=15, value=5)
                colloc_min = st.number_input("Collocation min cooc", min_value=2, max_value=100, value=5)
                top_n_logodds = st.number_input("Top log-odds токенов", min_value=30, max_value=500, value=120)

    col1, col2 = st.columns(2)
    with col1:
        zip_upload = st.file_uploader("ZIP с корпусом (.zip)", type=["zip"], accept_multiple_files=False)
    with col2:
        txt_uploads = st.file_uploader(
            "Или отдельные файлы (.txt/.md/.docx/.pdf/.csv/.xlsx/.json)",
            type=["txt", "md", "text", "docx", "pdf", "csv", "xlsx", "xls", "json"],
            accept_multiple_files=True,
        )

    referent_evi_manual_upload = None
    referent_metaphor_review_upload = None
    referent_calibration_upload = None
    if analysis_mode == "Референтный (China/USA/Russia)":
        st.markdown("### Доп. файлы для референтного режима")
        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            referent_evi_manual_upload = st.file_uploader(
                "evi_manual.csv (manual: context_id, ref_country, EVI_raw, referent_salience, evi_explanation)",
                type=["csv"],
                accept_multiple_files=False,
                key="evi_manual_csv",
            )
        with rc2:
            referent_metaphor_review_upload = st.file_uploader(
                "metaphor_review.csv (опционально)",
                type=["csv"],
                accept_multiple_files=False,
                key="metaphor_review_csv",
            )
        with rc3:
            referent_calibration_upload = st.file_uploader(
                "Calibration texts (.csv/.xlsx, optional)",
                type=["csv", "xlsx", "xls"],
                accept_multiple_files=False,
                key="calibration_upload",
            )

    st.markdown("### Или вставьте текст вручную")
    manual_text = st.text_area("Текст для анализа", height=180, placeholder="Вставьте сюда любой медиатекст...")
    m1, m2, m3 = st.columns(3)
    with m1:
        manual_title = st.text_input("Заголовок (опционально)", value="Manual input")
    with m2:
        manual_source = st.text_input("Источник (опционально)", value="Manual")
    with m3:
        manual_year = st.number_input("Год ручного текста", min_value=2000, max_value=2100, value=2026)

    run_btn = st.button("Запустить анализ", type="primary")

    if run_btn:
        prog_box = st.empty()
        prog_bar = st.progress(0, text="Подготовка анализа: 0%")
        prog_box.markdown("**Статус анализа корпуса:** 0%")
        file_items = []
        if zip_upload is not None:
            file_items.extend(read_zip_corpus_files(zip_upload.getvalue()))
        file_items.extend(read_uploaded_corpus_files(txt_uploads))
        if manual_text.strip():
            manual_blob = f"Title: {manual_title}\nDate: {int(manual_year)}\nSource: {manual_source}\n\n{manual_text.strip()}"
            file_items.append((f"manual_{int(manual_year)}.txt", manual_blob))

        # deduplicate same filename+content across inputs
        uniq = {}
        for n, t in file_items:
            content_md5 = hashlib.md5(t.encode("utf-8", errors="ignore")).hexdigest()
            fingerprint = f"{n}|{len(t)}|{content_md5}"
            uniq[fingerprint] = (n, t)
        file_items = list(uniq.values())
        prog_bar.progress(20, text="Файлы загружены и подготовлены: 20%")
        prog_box.markdown("**Статус анализа корпуса:** 20%")

        if not file_items:
            st.error("Не найдено входных текстов. Загрузите ZIP/файлы или вставьте текст вручную.")
            return

        with tempfile.TemporaryDirectory(prefix="sea_media_analysis_") as tmp:
            out_dir = Path(tmp) / "analysis_output"
            out_dir.mkdir(parents=True, exist_ok=True)
            prog_bar.progress(35, text="Предобработка корпуса: 35%")
            prog_box.markdown("**Статус анализа корпуса:** 35%")

            if analysis_mode == "Референтный (China/USA/Russia)":
                if referent_core is None:
                    st.error("Референтный модуль не найден. Добавьте media_analyzer_referent.py в корень проекта.")
                    return
                input_df = build_referent_input_df(file_items, int(min_year), int(max_year))
                if input_df.empty:
                    st.error("После фильтрации по годам не осталось документов для референтного анализа.")
                    return
                evi_manual_path = None
                metaphor_review_path = None
                if referent_evi_manual_upload is not None:
                    evi_manual_path = out_dir / "evi_manual.csv"
                    evi_manual_path.write_bytes(referent_evi_manual_upload.getvalue())
                if referent_metaphor_review_upload is not None:
                    metaphor_review_path = out_dir / "metaphor_review.csv"
                    metaphor_review_path.write_bytes(referent_metaphor_review_upload.getvalue())
                calibration_path = None
                if referent_calibration_upload is not None:
                    calibration_path = out_dir / referent_calibration_upload.name
                    calibration_path.write_bytes(referent_calibration_upload.getvalue())
                try:
                    prog_bar.progress(55, text="Референтный анализ выполняется: 55%")
                    prog_box.markdown("**Статус анализа корпуса:** 55%")
                    stats = run_referent_analysis(
                        input_df=input_df,
                        out_dir=out_dir,
                        evi_mode=referent_evi_mode,
                        exclude_technical_mentions=exclude_technical_mentions,
                        evi_manual_path=evi_manual_path,
                        metaphor_review_path=metaphor_review_path,
                        calibration_path=calibration_path,
                        ip_formula_mode=ip_formula_mode,
                        aggregation_mode=aggregation_mode,
                        percentile_basis=percentile_basis,
                    )
                except Exception as e:
                    st.exception(e)
                    return

                st.success(f"Готово. Документов: {stats['docs']}, контекстов: {stats['contexts']}, flagged: {stats['flagged']}")
                prog_bar.progress(85, text="Формирование визуализаций: 85%")
                prog_box.markdown("**Статус анализа корпуса:** 85%")
                show_referent_dashboard(
                    out_dir=out_dir,
                    default_ref=referent_target,
                    default_category=referent_category_default,
                    evi_mode=referent_evi_mode,
                    exclude_technical_mentions=exclude_technical_mentions,
                    show_evi_rubric_details=show_evi_rubric_details,
                    show_salience_diagnostics=show_salience_diagnostics,
                    display_precision=int(display_precision),
                    show_percent_values=show_percent_values,
                    show_empirical_percentiles=show_empirical_percentiles,
                    percentile_basis=percentile_basis,
                    use_calibration_anchors=use_calibration_anchors,
                )

                with st.expander("Технические таблицы (референтный режим)"):
                    for preview_name in [
                        "contexts_full.csv",
                        "aggregated_by_article.csv",
                        "aggregated_by_outlet.csv",
                        "aggregated_by_media_country_and_ref_country.csv",
                        "flagged_cases.csv",
                        "calibration_detailed.csv",
                    ]:
                        p = out_dir / preview_name
                        if p.exists():
                            st.markdown(f"**{preview_name}**")
                            st.dataframe(pd.read_csv(p).head(20), use_container_width=True)
                    p_dist = out_dir / "distribution_stats.xlsx"
                    if p_dist.exists():
                        st.markdown("**distribution_stats.xlsx**")
                        st.caption("Содержит распределения IDI/EMI/MTI/IP/IP_abs по выбранной базе процентилей.")
            else:
                docs = build_docs(file_items, int(min_year), int(max_year), use_lemma=use_lemma)
                if not docs:
                    st.error("После предобработки не осталось документов в указанном диапазоне лет.")
                    return

                try:
                    prog_bar.progress(55, text="Корпусный анализ выполняется: 55%")
                    prog_box.markdown("**Статус анализа корпуса:** 55%")
                    dedup_stats, analyzed_docs, analyzed_doc_objs = run_analysis(
                        docs=docs,
                        out_dir=out_dir,
                        top_n=int(top_n),
                        kwic_window=int(kwic_window),
                        kwic_max=int(kwic_max),
                        colloc_window=int(colloc_window),
                        colloc_min=int(colloc_min),
                        top_n_logodds=int(top_n_logodds),
                        dedup=dedup,
                        near_dup_jaccard=float(near_dup_jaccard),
                        near_dup_hamming=int(near_dup_hamming),
                    )
                except Exception as e:
                    st.exception(e)
                    return

                st.success(f"Готово. Проанализировано документов: {analyzed_docs}")
                prog_bar.progress(85, text="Формирование визуализаций: 85%")
                prog_box.markdown("**Статус анализа корпуса:** 85%")
                if analysis_mode == "Расширенный (корпусный)":
                    st.json(dedup_stats)

                show_five_indicator_charts(analyzed_doc_objs, selected_indicator=indicator_tab)

                if analysis_mode == "Расширенный (корпусный)":
                    st.subheader("Дополнительные исследовательские таблицы")
                    for preview_name in [
                        "stage1_profile_source.csv",
                        "stage1_profile_country.csv",
                        "stage1_profile_year.csv",
                        "stage1_profile_language.csv",
                        "stage5_representativeness_country_total.csv",
                        "stage6_significance_pairwise.csv",
                        "stage7_persuasion_summary_country_year.csv",
                        "stage7_persuasion_summary_source.csv",
                    ]:
                        p = out_dir / preview_name
                        if p.exists():
                            st.markdown(f"**{preview_name}**")
                            rows = read_csv_preview(p, limit=15)
                            st.dataframe(rows)
                    show_charts(out_dir)

                if analysis_mode == "Расширенный (корпусный)":
                    with st.expander("DEBUG"):
                        st.write({"build": APP_BUILD, "docs_for_indicator_charts": len(analyzed_doc_objs)})

            out_zip = zip_dir_bytes(out_dir)
            prog_bar.progress(100, text="Анализ завершён: 100%")
            prog_box.markdown("**Статус анализа корпуса:** 100%")
            st.download_button(
                label="Скачать результаты анализа (ZIP)",
                data=out_zip,
                file_name="mediatext_analyzator_output.zip",
                mime="application/zip",
            )


if __name__ == "__main__":
    main()
