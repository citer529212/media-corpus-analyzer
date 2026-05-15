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
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from docx import Document
from pypdf import PdfReader
try:
    from corpus_analyzer_webapp.formula_traces import build_context_formula_traces, traces_to_dataframe
except Exception:
    from formula_traces import build_context_formula_traces, traces_to_dataframe

# Reuse your strict analyzer core
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import corpus_analysis_strict_method as core
try:
    import media_analyzer_referent as referent_core
except Exception as _referent_import_exc:
    referent_core = None
try:
    from calibration.calibration_builder import CalibrationBuilder
    from calibration.calibration_lexicon_expander import apply_verified_terms_to_lexicons
    from calibration.calibration_metrics import add_percentiles as calibration_add_percentiles
    from calibration.calibration_ui import render_main_tabs as render_calibration_panel
    from calibration.calibration_ui import render_sidebar_controls as render_calibration_sidebar
    CALIBRATION_IMPORT_ERROR = ""
except Exception as _calibration_import_exc:
    CalibrationBuilder = None
    apply_verified_terms_to_lexicons = None
    calibration_add_percentiles = None
    render_calibration_panel = None
    render_calibration_sidebar = None
    CALIBRATION_IMPORT_ERROR = f"Calibration modules import failed: {_calibration_import_exc}"

APP_BUILD = "2026-05-10-21:06"
APP_DIR = Path(__file__).resolve().parent
DEFAULT_CALIBRATION_TEXTS_PATH = APP_DIR / "default_calibration_texts.csv"
DEFAULT_CALIBRATION_CONTEXTS_PATH = APP_DIR / "default_calibration_contexts.csv"
DEFAULT_CALIBRATION_DISTRIBUTIONS_PATH = APP_DIR / "default_calibration_distributions.csv"
DEFAULT_CALIBRATION_REPORT_PATH = APP_DIR / "default_calibration_report.md"
DEFAULT_CALIBRATION_CANDIDATES_PATH = APP_DIR / "default_candidate_terms.csv"
DEFAULT_CALIBRATION_FLAGS_PATH = APP_DIR / "default_calibration_quality_flags.csv"
DEFAULT_CALIBRATION_VERIFIED_PATH = APP_DIR / "default_verified_terms.csv"
DEFAULT_CALIBRATION_REJECTED_PATH = APP_DIR / "default_rejected_terms.csv"
DEFAULT_CALIBRATION_CHANGELOG_PATH = APP_DIR / "default_dictionary_change_log.csv"

PROGRESS_STAGES = {
    "init": (3, "Инициализация анализа: проверка входных данных, параметров и выбранных референтов."),
    "load": (8, "Загрузка корпуса: чтение файлов, проверка колонок, извлечение метаданных."),
    "clean": (13, "Предобработка: очистка текста, удаление пустых записей, нормализация пробелов и служебных символов."),
    "segment": (20, "Сегментация: разбиение текстов на предложения."),
    "find_refs": (30, "Поиск референтов: обнаружение упоминаний China / USA / Russia и связанных номинаций."),
    "extract_ctx": (40, "Извлечение контекстов: формирование расширенных фрагментов вокруг референта."),
    "salience": (48, "Проверка значимости: отделение технических, фоновых и центральных упоминаний."),
    "ling": (56, "Лингвистическая обработка: токенизация, лемматизация, подсчет знаменательных слов."),
    "idi": (64, "Расчет IDI: идеологические маркеры и идеологическая плотность."),
    "emi": (72, "Расчет EMI: эмоциональные маркеры и учет интенсивности."),
    "mti": (80, "Расчет MTI: метафорические маркеры и метафорическая плотность."),
    "evi": (88, "Расчет EVI: позитивный/негативный оценочный потенциал от -10 до +10."),
    "ip": (93, "Расчет IP: воздействующий потенциал каждого контекста."),
    "calib": (96, "Калибровка: сопоставление с calibration corpus и расчет процентилей."),
    "agg": (98, "Сводные результаты: агрегация по статьям, изданиям, странам СМИ и референтам."),
    "final": (100, "Готово: формирование таблиц, графиков, пояснений и файлов экспорта."),
}

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

# Fast prefilter terms for demo runtime stability (used before heavy referent pipeline).
QUICK_REF_TERMS: Dict[str, List[str]] = {
    "China": [
        "china", "chinese", "prc", "beijing", "xi jinping", "ccp", "cpc",
        "tiongkok", "cina", "taiwan strait", "belt and road", "huawei",
    ],
    "USA": [
        "usa", "u.s.", "us ", "united states", "america", "american",
        "washington", "white house", "pentagon", "congress", "biden", "trump",
        "amerika serikat", "amerika syarikat",
    ],
    "Russia": [
        "russia", "russian", "russian federation", "moscow", "kremlin",
        "putin", "lavrov", "medvedev", "ruble", "ukraine war",
        "rossiya", "россия", "rusia",
    ],
}

# Streamlit demo/beta 1.1 feature flags (stability-first)
ENABLE_ADVANCED_CALIBRATION_UI = False
ENABLE_LEGACY_STAGE_CHARTS = False
ENABLE_LEGACY_FIVE_INDICATOR_VIEW = False
ENABLE_DEBUG_RAW_TABLES = False
ENABLE_HEAVY_EXPORTS_IN_UI = False
ENABLE_EXPERIMENTAL_MINI_REFERENT_CHARTS = False


def _quick_prefilter_by_referent(input_df: pd.DataFrame, ref_country: str) -> pd.DataFrame:
    if input_df.empty or "text" not in input_df.columns:
        return input_df
    terms = QUICK_REF_TERMS.get(str(ref_country), [])
    if not terms:
        return input_df
    pattern = re.compile("|".join(re.escape(t.strip()) for t in terms if t.strip()), re.IGNORECASE)
    mask = input_df["text"].astype(str).str.contains(pattern, na=False)
    return input_df[mask].copy()


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

_EXCEL_ILLEGAL_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
_EXCEL_MAX_CELL_LEN = 32767
_FORMULA_TRACES_MAX_CONTEXTS = 6000
_XLSX_CONTEXTS_MAX_ROWS = 50000
_XLSX_MARKER_TRACES_MAX_ROWS = 120000


def _excel_safe_text(value: object) -> object:
    if value is None or not isinstance(value, str):
        return value
    cleaned = _EXCEL_ILLEGAL_RE.sub("", value)
    # Excel/OpenXML cell text hard limit
    if len(cleaned) > _EXCEL_MAX_CELL_LEN:
        cleaned = cleaned[:_EXCEL_MAX_CELL_LEN]
    return cleaned


def _excel_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    obj_cols = [c for c in out.columns if out[c].dtype == "object"]
    for c in obj_cols:
        out[c] = out[c].map(_excel_safe_text)
    return out


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


def _interpret_evi(evi: float) -> str:
    v = float(evi)
    if v <= -10:
        return "Предельно негативная репрезентация."
    if v <= -8:
        return "Очень сильная делегитимация."
    if v <= -6:
        return "Выраженно негативная оценка."
    if v <= -4:
        return "Умеренно негативная оценка."
    if v < 0:
        return "Слабая негативная окраска."
    if v == 0:
        return "Нейтральная/информационная подача."
    if v <= 3:
        return "Слабая позитивная окраска."
    if v <= 5:
        return "Умеренно позитивная оценка."
    if v <= 7:
        return "Выраженно позитивная оценка."
    if v <= 9:
        return "Очень сильная легитимация."
    return "Предельно позитивная репрезентация."


def _interpret_ip(ip_final: float, ip_abs_final: float, near_zero_threshold: float = 0.05) -> str:
    ip_final = float(ip_final)
    ip_abs_final = float(ip_abs_final)
    if abs(ip_final) < near_zero_threshold and ip_abs_final < near_zero_threshold:
        return "Образ преимущественно нейтрален или слабо оценочно оформлен."
    if abs(ip_final) < near_zero_threshold and ip_abs_final >= near_zero_threshold:
        return "Среднее направление близко к нейтральному, но есть поляризация оценок."
    direction = "Преобладает позитивная направленность образа." if ip_final > 0 else "Преобладает негативная направленность образа."
    return f"{direction} Сила воздействия определяется по |IP|."


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
    try:
        if ext == ".csv":
            df = pd.read_csv(path)
        elif ext in {".xlsx", ".xls"}:
            df = pd.read_excel(path)
        else:
            return pd.DataFrame()
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception:
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


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _mirror_default_calibration_assets(calibration_dir: Path) -> None:
    calibration_dir.mkdir(parents=True, exist_ok=True)
    to_copy = [
        (DEFAULT_CALIBRATION_TEXTS_PATH, calibration_dir / "calibration_texts.csv"),
        (DEFAULT_CALIBRATION_CONTEXTS_PATH, calibration_dir / "calibration_contexts.csv"),
        (DEFAULT_CALIBRATION_DISTRIBUTIONS_PATH, calibration_dir / "calibration_distributions.csv"),
        (DEFAULT_CALIBRATION_REPORT_PATH, calibration_dir / "calibration_report.md"),
        (DEFAULT_CALIBRATION_CANDIDATES_PATH, calibration_dir / "candidate_terms.csv"),
        (DEFAULT_CALIBRATION_FLAGS_PATH, calibration_dir / "calibration_quality_flags.csv"),
        (DEFAULT_CALIBRATION_VERIFIED_PATH, calibration_dir / "verified_terms.csv"),
        (DEFAULT_CALIBRATION_REJECTED_PATH, calibration_dir / "rejected_terms.csv"),
        (DEFAULT_CALIBRATION_CHANGELOG_PATH, calibration_dir / "dictionary_change_log.csv"),
    ]
    for src, dst in to_copy:
        if src.exists() and not dst.exists():
            try:
                dst.write_bytes(src.read_bytes())
            except Exception:
                # Keep app resilient in read-only / transient FS modes.
                pass


def _source_has_data(src: object, base_dir: Path) -> bool:
    mode = str(getattr(src, "mode", "")).strip().lower()
    if mode == "local":
        raw_path = str(getattr(src, "path", "") or "").strip()
        if not raw_path:
            return False
        p = Path(raw_path)
        if not p.is_absolute():
            p = (base_dir / p).resolve()
        if p.is_file():
            return True
        if not p.exists() or not p.is_dir():
            return False
        for ext in ("*.txt", "*.md", "*.docx", "*.pdf", "*.csv", "*.xlsx", "*.xls", "*.json"):
            if any(p.rglob(ext)):
                return True
        return False
    if mode == "url_list":
        raw = str(getattr(src, "url_csv_path", "") or "").strip()
        if not raw:
            return False
        p = Path(raw)
        if not p.is_absolute():
            p = (base_dir / p).resolve()
        return p.exists() and p.is_file()
    if mode == "rss":
        return bool(str(getattr(src, "rss_url", "") or "").strip())
    return False


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
        evi_norm = evi_raw / 10.0
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
    calibration_texts_df: Optional[pd.DataFrame] = None,
    calibration_contexts_df: Optional[pd.DataFrame] = None,
    calibration_filter: str = "full_calibration_corpus",
    use_empirical_percentile_interpretation: bool = True,
    lexicon_version: str = "default",
    target_ref_country: Optional[str] = None,
):
    if referent_core is None:
        raise RuntimeError("referent analyzer module is unavailable")
    if str(evi_mode).strip().lower() == "calibration-assisted":
        evi_mode = "suggested"

    dict_dir = out_dir / "referent_dicts"
    referent_core.ensure_default_dictionaries(dict_dir)
    docs = referent_core.ensure_required_fields(input_df)
    ref_keywords = referent_core.load_ref_keywords(dict_dir)
    ref_patterns = referent_core.compile_keyword_patterns(ref_keywords)
    contexts = referent_core.extract_context_rows(docs, ref_patterns)
    if target_ref_country and "ref_country" in contexts.columns:
        contexts = contexts[contexts["ref_country"].astype(str) == str(target_ref_country)].copy()
    if contexts.empty:
        raise RuntimeError("Не удалось извлечь референтные контексты (China/USA/Russia).")

    scored_payload = referent_core.apply_metrics(
        contexts=contexts,
        dict_dir=dict_dir,
        evi_mode=evi_mode,
        evi_manual_path=evi_manual_path,
        metaphor_review_path=metaphor_review_path,
        return_traces=True,
    )
    if isinstance(scored_payload, tuple):
        scored, marker_traces = scored_payload
    else:
        scored = scored_payload
        marker_traces = pd.DataFrame()
    scored = referent_core.add_multicountry_flags(scored)
    ref_countries_allowed = set(getattr(referent_core, "REF_COUNTRIES", ["China", "USA", "Russia"]))
    scored = scored[scored["ref_country"].isin(ref_countries_allowed)].copy()
    if target_ref_country:
        scored = scored[scored["ref_country"].astype(str) == str(target_ref_country)].copy()
    if scored.empty:
        raise RuntimeError("После фильтрации не осталось валидных референтных контекстов.")
    # Backward compatibility with older referent modules / column schemas.
    if "EVI" not in scored.columns:
        scored["EVI"] = 0.0
    if "EVI_raw" not in scored.columns:
        scored["EVI_raw"] = pd.to_numeric(scored["EVI"], errors="coerce").fillna(0.0)
    if "EVI_norm" not in scored.columns:
        scored["EVI_norm"] = pd.to_numeric(scored["EVI_raw"], errors="coerce").fillna(0.0) / 10.0
    if "IP" not in scored.columns:
        scored["IP"] = 0.0
    for col in ["IDI", "EMI", "MTI", "N_content", "referent_salience"]:
        if col not in scored.columns:
            scored[col] = 0.0 if col != "N_content" else 0
    legacy_norm_mask = (
        (pd.to_numeric(scored["EVI_norm"], errors="coerce").fillna(0.0) - (pd.to_numeric(scored["EVI_raw"], errors="coerce").fillna(0.0) / 5.0)).abs()
        < 1e-9
    ) & (
        (pd.to_numeric(scored["EVI_norm"], errors="coerce").fillna(0.0) - (pd.to_numeric(scored["EVI_raw"], errors="coerce").fillna(0.0) / 10.0)).abs()
        > 1e-9
    )
    if legacy_norm_mask.any():
        scored["migration_warning"] = "Detected legacy EVI_norm. Recalculated using EVI_norm = EVI / 10."
    else:
        scored["migration_warning"] = ""
    scored.loc[(scored["N_content"] <= 0), ["IDI", "EMI", "MTI", "IP"]] = 0.0
    scored["EVI"] = pd.to_numeric(scored["EVI"], errors="coerce").fillna(0.0)
    scored.loc[(~scored["EVI_raw"].between(-10, 10)), ["EVI_raw", "EVI_norm", "IP"]] = [0, 0.0, 0.0]
    scored.loc[(scored["referent_salience"] == 0), "IP"] = 0.0
    scored.loc[(scored["EVI_raw"] == 0), "IP"] = 0.0
    for col in ["IDI", "EMI", "MTI"]:
        scored[col] = scored[col].clip(lower=0.0, upper=1.0)
    scored["EVI_norm"] = scored["EVI_raw"] / 10.0
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
    scored["IP_abs_context"] = scored["IP_abs_i"]
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
    scored["IDI_percent"] = scored["IDI_percent_value"]
    scored["EMI_percent"] = scored["EMI_percent_value"]
    scored["MTI_percent"] = scored["MTI_percent_value"]
    scored["EVI_explanation"] = scored.get("evi_explanation", "")
    scored["lexicon_version_used"] = str(lexicon_version)
    scored["EVI_P_score"] = pd.to_numeric(scored.get("positive_score", 0), errors="coerce").fillna(0)
    scored["EVI_N_score"] = pd.to_numeric(scored.get("negative_score", 0), errors="coerce").fillna(0)

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
    baseline_df = None
    if calibration_contexts_df is not None and not calibration_contexts_df.empty:
        baseline_df = calibration_contexts_df.copy()
    elif calibration_texts_df is not None and not calibration_texts_df.empty:
        baseline_df = calibration_texts_df.copy()

    if (
        use_empirical_percentile_interpretation
        and calibration_add_percentiles is not None
        and baseline_df is not None
        and not baseline_df.empty
    ):
        base = baseline_df.copy()
        if calibration_filter == "neutral_news_only":
            base = base[base.get("calibration_type", "") == "neutral_news"]
        elif calibration_filter == "political_news_only":
            base = base[base.get("calibration_type", "") == "standard_political_news"]
        elif calibration_filter == "same_language_only" and "language" in scored.columns and "language" in base.columns:
            lang_mode = str(scored["language"].mode().iloc[0]) if not scored["language"].dropna().empty else ""
            base = base[base["language"] == lang_mode]
        elif calibration_filter == "same_ref_country_only" and "ref_country" in scored.columns and "ref_country" in base.columns:
            refs = set(scored["ref_country"].dropna().astype(str).tolist())
            base = base[base["ref_country"].astype(str).isin(refs)]
        if base.empty:
            base = baseline_df.copy()

        # Require indicator columns in baseline, otherwise fallback to corpus percentile ranking.
        needed = {"IDI_raw", "EMI_raw", "MTI_raw", "IP_context", "IP_abs_context"}
        if not needed.issubset(set(base.columns)):
            base = pd.DataFrame()

        if not base.empty:
            scored = calibration_add_percentiles(scored, base)
        else:
            scored = _assign_percentiles(scored, "IDI_raw", "IDI_percentile", percentile_basis)
            scored = _assign_percentiles(scored, "EMI_raw", "EMI_percentile", percentile_basis)
            scored = _assign_percentiles(scored, "MTI_raw", "MTI_percentile", percentile_basis)
            scored = _assign_percentiles(scored, "IP_i", "IP_percentile", percentile_basis)
            scored = _assign_percentiles(scored, "IP_abs_i", "IP_abs_percentile", percentile_basis)
            scored["IDI_empirical_level"] = scored["IDI_percentile"].map(_empirical_level)
            scored["EMI_empirical_level"] = scored["EMI_percentile"].map(_empirical_level)
            scored["MTI_empirical_level"] = scored["MTI_percentile"].map(_empirical_level)
            scored["IP_empirical_level"] = scored["IP_percentile"].map(_empirical_level)
            scored["IP_abs_empirical_level"] = scored["IP_abs_percentile"].map(_empirical_level)
        # alias naming used in dashboard
        rename_map = {
            "IDI_percentile": "IDI_percentile",
            "EMI_percentile": "EMI_percentile",
            "MTI_percentile": "MTI_percentile",
            "IP_percentile": "IP_percentile",
            "IP_abs_percentile": "IP_abs_percentile",
            "IDI_empirical_level": "IDI_empirical_level",
            "EMI_empirical_level": "EMI_empirical_level",
            "MTI_empirical_level": "MTI_empirical_level",
            "IP_empirical_level": "IP_empirical_level",
            "IP_abs_empirical_level": "IP_abs_empirical_level",
        }
        for k, v in rename_map.items():
            if k in scored.columns and v not in scored.columns:
                scored[v] = scored[k]
    else:
        scored = _assign_percentiles(scored, "IDI_raw", "IDI_percentile", percentile_basis)
        scored = _assign_percentiles(scored, "EMI_raw", "EMI_percentile", percentile_basis)
        scored = _assign_percentiles(scored, "MTI_raw", "MTI_percentile", percentile_basis)
        scored = _assign_percentiles(scored, "IP_i", "IP_percentile", percentile_basis)
        scored = _assign_percentiles(scored, "IP_abs_i", "IP_abs_percentile", percentile_basis)
        scored["IDI_empirical_level"] = scored["IDI_percentile"].map(_empirical_level)
        scored["EMI_empirical_level"] = scored["EMI_percentile"].map(_empirical_level)
        scored["MTI_empirical_level"] = scored["MTI_percentile"].map(_empirical_level)
        scored["IP_empirical_level"] = scored["IP_percentile"].map(_empirical_level)
        scored["IP_abs_empirical_level"] = scored["IP_abs_percentile"].map(_empirical_level)

    # Backward-compatible level aliases
    if "IDI_level_empirical" not in scored.columns and "IDI_empirical_level" in scored.columns:
        scored["IDI_level_empirical"] = scored["IDI_empirical_level"]
    if "EMI_level_empirical" not in scored.columns and "EMI_empirical_level" in scored.columns:
        scored["EMI_level_empirical"] = scored["EMI_empirical_level"]
    if "MTI_level_empirical" not in scored.columns and "MTI_empirical_level" in scored.columns:
        scored["MTI_level_empirical"] = scored["MTI_empirical_level"]
    if "IP_level_empirical" not in scored.columns and "IP_empirical_level" in scored.columns:
        scored["IP_level_empirical"] = scored["IP_empirical_level"]
    if "IP_abs_level_empirical" not in scored.columns and "IP_abs_empirical_level" in scored.columns:
        scored["IP_abs_level_empirical"] = scored["IP_abs_empirical_level"]

    scored["percentile_explanation"] = scored.apply(
        lambda r: (
            f"IDI pctl={float(r.get('IDI_percentile', 0)):.1f}, "
            f"EMI pctl={float(r.get('EMI_percentile', 0)):.1f}, "
            f"MTI pctl={float(r.get('MTI_percentile', 0)):.1f}, "
            f"IP_abs pctl={float(r.get('IP_abs_percentile', 0)):.1f}"
        ),
        axis=1,
    )
    scored["interpretation_summary"] = scored.apply(
        lambda r: (
            f"EVI={float(r.get('EVI_raw', 0)):.1f}, "
            f"IP_i={float(r.get('IP_i', 0)):.6f}, "
            f"S_r={float(r.get('referent_salience', 0)):.2f}"
        ),
        axis=1,
    )

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
    # Required summary aliases for calibrated reporting
    if "IP_final" in matrix.columns:
        matrix["mean_IP"] = matrix["IP_final"]
    if "IP_abs_final" in matrix.columns:
        matrix["mean_abs_IP"] = matrix["IP_abs_final"]
    matrix["mean_EVI"] = matrix.get("mean_EVI_raw", 0.0)
    matrix["interpretation_summary"] = matrix.apply(
        lambda r: (
            f"IDI pctl={float(r.get('mean_IDI_percentile', 0.0)):.1f}; "
            f"EMI pctl={float(r.get('mean_EMI_percentile', 0.0)):.1f}; "
            f"MTI pctl={float(r.get('mean_MTI_percentile', 0.0)):.1f}; "
            f"IP_abs pctl={float(r.get('mean_IP_abs_percentile', 0.0)):.1f}"
        ),
        axis=1,
    )

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
    all_traces = []
    trace_id_by_context: Dict[str, str] = {}
    for idx, (_, row) in enumerate(scored.iterrows()):
        if idx >= _FORMULA_TRACES_MAX_CONTEXTS:
            break
        traces = build_context_formula_traces(row)
        if traces:
            trace_id_by_context[str(row.get("context_id", ""))] = traces[-1].trace_id
            all_traces.extend(traces)
    traces_df = traces_to_dataframe(all_traces)
    if "context_id" in scored.columns:
        scored["formula_trace_id"] = scored["context_id"].astype(str).map(trace_id_by_context).fillna("")

    scored.to_csv(out_dir / "contexts_full.csv", index=False)
    by_article.to_csv(out_dir / "aggregated_by_article.csv", index=False)
    by_outlet.to_csv(out_dir / "aggregated_by_outlet.csv", index=False)
    by_media_ref.to_csv(out_dir / "aggregated_by_media_country_and_ref_country.csv", index=False)
    flagged.to_csv(out_dir / "flagged_cases.csv", index=False)
    if marker_traces is None or marker_traces.empty:
        marker_traces = pd.DataFrame(
            columns=[
                "marker_id", "context_id", "ref_country", "indicator", "term_found", "lemma", "dictionary_source",
                "category", "semantic_zone_or_model", "intensity_or_strength", "matched_span", "context_text",
                "is_context_dependent", "verification_status", "inclusion_reason", "exclusion_reason",
            ]
        )
    marker_traces.to_csv(out_dir / "marker_traces.csv", index=False)
    marker_traces.to_json(out_dir / "marker_traces.json", orient="records", force_ascii=False, indent=2)
    if ENABLE_HEAVY_EXPORTS_IN_UI:
        marker_traces_xlsx = marker_traces.head(_XLSX_MARKER_TRACES_MAX_ROWS)
        _excel_safe_df(marker_traces_xlsx).to_excel(out_dir / "marker_traces.xlsx", index=False)
        if len(marker_traces) > _XLSX_MARKER_TRACES_MAX_ROWS:
            (out_dir / "marker_traces.xlsx.note.txt").write_text(
                f"marker_traces.xlsx truncated to {_XLSX_MARKER_TRACES_MAX_ROWS} rows for performance. "
                f"Full data is in marker_traces.csv/json ({len(marker_traces)} rows).",
                encoding="utf-8",
            )

    # QA: each counted marker should have at least one trace per context.
    if not scored.empty:
        counted = (
            pd.to_numeric(scored.get("N_ideol", 0), errors="coerce").fillna(0)
            + pd.to_numeric(scored.get("N_e_w", 0), errors="coerce").fillna(0)
            + pd.to_numeric(scored.get("N_e_m", 0), errors="coerce").fillna(0)
            + pd.to_numeric(scored.get("N_e_s", 0), errors="coerce").fillna(0)
            + pd.to_numeric(scored.get("N_met", 0), errors="coerce").fillna(0)
        )
        if not marker_traces.empty:
            trace_counts = marker_traces.groupby("context_id").size().to_dict()
        else:
            trace_counts = {}
        missing_trace_mask = []
        for i, ctx_id in enumerate(scored["context_id"].astype(str).tolist()):
            c = float(counted.iloc[i]) if i < len(counted) else 0.0
            t = int(trace_counts.get(ctx_id, 0))
            missing_trace_mask.append(c > 0 and t == 0)
        if any(missing_trace_mask):
            bad = scored[pd.Series(missing_trace_mask, index=scored.index)].copy()
            bad["flag_case_type"] = "missing_marker_trace_for_counted_marker"
            flagged = pd.concat([flagged, bad], ignore_index=True)

    # Lexicon quality and workflow traces.
    if hasattr(referent_core, "validate_lexicons"):
        qdf, qmd = referent_core.validate_lexicons(dict_dir)
        qdf.to_csv(out_dir / "lexicon_quality_report.csv", index=False)
        (out_dir / "lexicon_quality_report.md").write_text(qmd, encoding="utf-8")
        chg = dict_dir / "dictionary_change_log.csv"
        if chg.exists():
            try:
                pd.read_csv(chg).to_csv(out_dir / "dictionary_change_log.csv", index=False)
            except Exception:
                (out_dir / "dictionary_change_log.csv").write_text(chg.read_text(encoding="utf-8"), encoding="utf-8")
        else:
            pd.DataFrame(columns=["timestamp", "action", "term", "lemma", "dictionary", "category", "status", "details"]).to_csv(
                out_dir / "dictionary_change_log.csv", index=False
            )
    else:
        qdf = pd.DataFrame()
        qdf.to_csv(out_dir / "lexicon_quality_report.csv", index=False)
        (out_dir / "lexicon_quality_report.md").write_text("# lexicon_quality_report\n\n- unavailable in current core\n", encoding="utf-8")
    if not traces_df.empty:
        traces_df.to_json(out_dir / "formula_traces.json", orient="records", force_ascii=False, indent=2)
        if ENABLE_HEAVY_EXPORTS_IN_UI:
            _excel_safe_df(traces_df).to_excel(out_dir / "formula_traces.xlsx", index=False)
    if len(scored) > _FORMULA_TRACES_MAX_CONTEXTS:
        (out_dir / "formula_traces.note.txt").write_text(
            f"Formula traces generated for first {_FORMULA_TRACES_MAX_CONTEXTS} contexts "
            f"out of {len(scored)} for performance. Full numeric outputs remain in contexts_full.csv.",
            encoding="utf-8",
        )

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

    if ENABLE_HEAVY_EXPORTS_IN_UI:
        with pd.ExcelWriter(out_dir / "distribution_stats.xlsx", engine="openpyxl") as xw:
            _excel_safe_df(dist_idi).to_excel(xw, index=False, sheet_name="IDI_distribution")
            _excel_safe_df(dist_emi).to_excel(xw, index=False, sheet_name="EMI_distribution")
            _excel_safe_df(dist_mti).to_excel(xw, index=False, sheet_name="MTI_distribution")
            _excel_safe_df(dist_ip).to_excel(xw, index=False, sheet_name="IP_distribution")
            _excel_safe_df(dist_ip_abs).to_excel(xw, index=False, sheet_name="IP_abs_distribution")

        scored_xlsx = scored.head(_XLSX_CONTEXTS_MAX_ROWS)
        marker_traces_sheet = marker_traces.head(_XLSX_MARKER_TRACES_MAX_ROWS)
        with pd.ExcelWriter(out_dir / "summary_matrix.xlsx", engine="openpyxl") as xw:
            _excel_safe_df(matrix).to_excel(xw, index=False, sheet_name="summary_matrix")
            _excel_safe_df(by_media_ref).to_excel(xw, index=False, sheet_name="long_table")
            _excel_safe_df(scored_xlsx).to_excel(xw, index=False, sheet_name="contexts_full")
            _excel_safe_df(flagged).to_excel(xw, index=False, sheet_name="flagged_cases")
            _excel_safe_df(marker_traces_sheet).to_excel(xw, index=False, sheet_name="marker_traces")
            if 'qdf' in locals() and not qdf.empty:
                _excel_safe_df(qdf).to_excel(xw, index=False, sheet_name="lexicon_quality")
            if not calibration_report.empty:
                _excel_safe_df(calibration_report).to_excel(xw, index=False, sheet_name="calibration_report")
        if len(scored) > _XLSX_CONTEXTS_MAX_ROWS:
            (out_dir / "summary_matrix.xlsx.note.txt").write_text(
                f"Sheet contexts_full truncated to {_XLSX_CONTEXTS_MAX_ROWS} rows for performance. "
                f"Full data is in contexts_full.csv ({len(scored)} rows).",
                encoding="utf-8",
            )

        if not calibration_report.empty:
            _excel_safe_df(calibration_report).to_excel(out_dir / "calibration_report.xlsx", index=False)

    report_lines = [
        "# analysis_report",
        "",
        "## Corpus overview",
        f"- documents: {len(docs)}",
        f"- contexts: {len(scored)}",
        f"- technical_excluded: {int((scored['aggregation_weight'] == 0).sum()) if 'aggregation_weight' in scored.columns else 0}",
        "",
        "## Reference country overview",
    ]
    for ref in ["China", "USA", "Russia"]:
        part = scored[scored["ref_country"] == ref]
        if part.empty:
            continue
        report_lines.append(
            f"- {ref}: contexts={len(part)}, mean_EVI={float(part['EVI_raw'].mean()):.2f}, mean_IP={float(part['IP_i'].mean()):.6f}"
        )
    report_lines.extend(
        [
            "",
            "## Main formulas",
            "- IDI = N_ideol / N_content",
            "- EMI = (1/3*N_e_w + 2/3*N_e_m + N_e_s) / N_content",
            "- MTI = N_met / N_content",
            "- EVI = P_r - N_r",
            "- EVI_norm = EVI / 10",
            "- IP_i = EVI_norm × (1 + IDI + EMI + MTI)",
            "- IP_final = Σ(S_i × IP_i) / ΣS_i",
            "",
            "## Marker basis and lexicon methodology",
            "- NLP libraries are used only for technical preprocessing (sentence split, tokenization, normalization).",
            "- Scientific marker classification is defined by lexicons and rubric rules.",
            "- Marker traces are exported to marker_traces.csv/json/xlsx for verification.",
            "",
        ]
    )
    if not traces_df.empty:
        report_lines.append("## Formula examples")
        for _, tr in traces_df.head(12).iterrows():
            report_lines.append(f"- [{tr['context_id']}] {tr['formula_name']}: {tr['formula_substitution']}")
    (out_dir / "analysis_report.md").write_text("\n".join(report_lines), encoding="utf-8")

    return {
        "docs": len(docs),
        "contexts": len(scored),
        "flagged": len(flagged),
        "technical_excluded": int((scored["aggregation_weight"] == 0).sum()) if "aggregation_weight" in scored.columns else 0,
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


def _safe_read_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path).fillna("")
        if path.suffix.lower() in {".xlsx", ".xls"}:
            return pd.read_excel(path).fillna("")
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame()


def _render_marker_base_panel(dict_dir: Path) -> None:
    st.markdown("### Маркерная база")
    st.caption("NLP-библиотеки используются только для технической обработки текста. Научная классификация маркеров задается словарями, рубрикаторами и контекстными правилами.")
    tabs = st.tabs(
        [
            "1. Что такое маркер?",
            "2. Словари или библиотеки?",
            "3. IDI markers",
            "4. EMI markers",
            "5. MTI markers",
            "6. EVI rubric",
            "7. S_r patterns",
            "8. Candidate terms",
            "9. Verified terms",
            "10. Change log",
        ]
    )
    with tabs[0]:
        st.info("Маркер — это языковая единица/формула, которая в конкретном референтном контексте участвует в формировании имиджа государства.")
    with tabs[1]:
        st.write("Словари = научная модель; NLP-библиотеки = только технический слой (tokenize, sentence split, POS/лемматизация).")
    with tabs[2]:
        st.dataframe(_safe_read_table(dict_dir / "ideological_markers.csv").head(300), use_container_width=True)
    with tabs[3]:
        st.dataframe(_safe_read_table(dict_dir / "emotional_markers.csv").head(300), use_container_width=True)
    with tabs[4]:
        st.dataframe(_safe_read_table(dict_dir / "metaphor_markers.csv").head(300), use_container_width=True)
    with tabs[5]:
        st.dataframe(_safe_read_table(dict_dir / "evi_lexicon.csv").head(300), use_container_width=True)
        st.dataframe(_safe_read_table(dict_dir / "actor_actions.csv").head(200), use_container_width=True)
        st.dataframe(_safe_read_table(dict_dir / "consequence_markers.csv").head(200), use_container_width=True)
        st.dataframe(_safe_read_table(dict_dir / "ideological_frames.csv").head(200), use_container_width=True)
    with tabs[6]:
        st.dataframe(_safe_read_table(dict_dir / "salience_patterns.csv").head(300), use_container_width=True)
        st.dataframe(_safe_read_table(dict_dir / "technical_mention_patterns.csv").head(300), use_container_width=True)
    with tabs[7]:
        st.dataframe(_safe_read_table(dict_dir / "candidate_terms.csv").head(500), use_container_width=True)
    with tabs[8]:
        st.dataframe(_safe_read_table(dict_dir / "verified_terms.csv").head(500), use_container_width=True)
        st.dataframe(_safe_read_table(dict_dir / "rejected_terms.csv").head(500), use_container_width=True)
    with tabs[9]:
        st.dataframe(_safe_read_table(dict_dir / "dictionary_change_log.csv").head(500), use_container_width=True)


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
        "fine": "Точный режим: оценка от -10 до +10.",
        "suggested": "Автоподсказка: система предлагает оценку по правилам.",
        "manual": "Ручная разметка: оценка берется из вашего CSV.",
        "calibration-assisted": "Калибровочный режим: оценка с опорой на калибровочный корпус.",
        "coarse": "Legacy режим (скрыт): укрупненная оценка -2..+2.",
    }
    return mapping.get(m, m)


def _percentile_basis_ru_to_internal(label: str) -> str:
    m = {
        "Весь корпус": "full corpus",
        "По стране медиа": "selected media_country",
        "По референту": "selected ref_country",
        "Страна медиа × референт": "media_country × ref_country",
    }
    return m.get(label, "full corpus")


def _percentile_basis_internal_to_ru(value: str) -> str:
    m = {
        "full corpus": "весь корпус",
        "selected media_country": "по стране медиа",
        "selected ref_country": "по референту",
        "media_country × ref_country": "страна медиа × референт",
    }
    return m.get((value or "").strip(), str(value))


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
    mini_referents_raw: str = "",
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
    categories = sorted({c for val in df_ref.get("keyword_category", pd.Series(dtype=str)).dropna().astype(str) for c in [x.strip() for x in val.split(";")] if c})
    categories = ["all"] + categories
    cat_index = categories.index(default_category) if default_category in categories else 0
    category = st.selectbox("Категория ключевых слов", options=categories, index=cat_index, key="ref_cat_main")
    if category != "all":
        df_ref = df_ref[df_ref["keyword_category"].astype(str).str.contains(rf"(^|;\s*){re.escape(category)}($|;)", regex=True)]

    if df_ref.empty:
        st.warning("После фильтрации по референту/категории не осталось контекстов.")
        return

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
        "EVI": 0.0,
        "EVI_raw": 0.0,
        "EVI_norm": 0.0,
        "IP": 0.0,
    }
    text_defaults = {
        "context_id": "",
        "outlet_name": "Unknown",
        "date": "",
        "matched_keywords": "",
        "context_text": "",
        "evi_explanation": "",
        "positive_evidence_terms": "",
        "negative_evidence_terms": "",
    }
    for c, d in numeric_defaults.items():
        if c not in df_ref.columns:
            df_ref[c] = d
    for c, d in text_defaults.items():
        if c not in df_ref.columns:
            df_ref[c] = d

    if "positive_score" not in df_ref.columns:
        df_ref["positive_score"] = 0.0
    if "negative_score" not in df_ref.columns:
        df_ref["negative_score"] = 0.0

    df_ref["EVI_raw"] = pd.to_numeric(df_ref.get("EVI_raw", df_ref.get("EVI", 0.0)), errors="coerce").fillna(0.0)
    df_ref["EVI"] = pd.to_numeric(df_ref.get("EVI", df_ref["EVI_raw"]), errors="coerce").fillna(0.0)
    df_ref["EVI_norm"] = pd.to_numeric(df_ref.get("EVI_norm", df_ref["EVI_raw"] / 10.0), errors="coerce").fillna(0.0)

    if hasattr(referent_core, "compute_context_ip"):
        df_ref = referent_core.compute_context_ip(df_ref)
    else:
        df_ref["discursive_energy"] = df_ref["IDI"] + df_ref["EMI"] + df_ref["MTI"]
        df_ref["IP_context"] = df_ref["EVI_norm"] * (1.0 + df_ref["discursive_energy"])
        df_ref["IP_context_abs"] = df_ref["IP_context"].abs()
        df_ref["aggregation_weight"] = df_ref["referent_salience"].clip(lower=0.0, upper=1.0)

    for alias_col, src_col in [("IP_i", "IP_context"), ("IP_abs_i", "IP_context_abs")]:
        if alias_col not in df_ref.columns:
            df_ref[alias_col] = pd.to_numeric(df_ref.get(src_col, 0.0), errors="coerce").fillna(0.0)

    if "aggregation_weight" not in df_ref.columns:
        df_ref["aggregation_weight"] = pd.to_numeric(df_ref.get("referent_salience", 1.0), errors="coerce").fillna(1.0).clip(0.0, 1.0)

    if exclude_technical_mentions and "is_technical_mention" in df_ref.columns:
        df_ref.loc[df_ref["is_technical_mention"] == True, "aggregation_weight"] = 0.0

    def _bundle(g: pd.DataFrame) -> Dict[str, float]:
        if g.empty:
            return {
                "contexts": 0,
                "articles": 0,
                "n_content": 0.0,
                "n_ideol": 0.0,
                "n_met": 0.0,
                "n_ew": 0.0,
                "n_em": 0.0,
                "n_es": 0.0,
                "weighted_emotion_sum": 0.0,
                "idi": 0.0,
                "emi": 0.0,
                "mti": 0.0,
                "evi": 0.0,
                "evi_norm": 0.0,
                "ip_final": 0.0,
                "ip_abs": 0.0,
                "w_num": 0.0,
                "w_den": 0.0,
                "idi_pct": 0.0,
                "emi_pct": 0.0,
                "mti_pct": 0.0,
                "ip_pct": 0.0,
                "ip_abs_pct": 0.0,
                "tech_excluded": 0,
                "analyzed": 0,
            }

        valid = g[g["aggregation_weight"] > 0].copy()
        metric_df = valid if not valid.empty else g.copy()

        n_content = max(float(pd.to_numeric(metric_df.get("N_content", 0), errors="coerce").fillna(0).sum()), 1.0)
        n_ideol = float(pd.to_numeric(metric_df.get("N_ideol", 0), errors="coerce").fillna(0).sum())
        n_met = float(pd.to_numeric(metric_df.get("N_met", 0), errors="coerce").fillna(0).sum())
        n_ew = pd.to_numeric(metric_df.get("N_e_w", 0), errors="coerce").fillna(0)
        n_em = pd.to_numeric(metric_df.get("N_e_m", 0), errors="coerce").fillna(0)
        n_es = pd.to_numeric(metric_df.get("N_e_s", 0), errors="coerce").fillna(0)
        w_emotion = float(((n_ew / 3.0) + (2.0 * n_em / 3.0) + n_es).sum())

        idi = float(n_ideol / n_content)
        emi = float(w_emotion / n_content)
        mti = float(n_met / n_content)
        evi = float(pd.to_numeric(metric_df.get("EVI_raw", 0), errors="coerce").fillna(0).mean())
        evi_norm = float(pd.to_numeric(metric_df.get("EVI_norm", 0), errors="coerce").fillna(0).mean())

        if not valid.empty and float(valid["aggregation_weight"].sum()) > 0:
            w = pd.to_numeric(valid["aggregation_weight"], errors="coerce").fillna(0)
            ip = pd.to_numeric(valid.get("IP_i", valid.get("IP_context", 0)), errors="coerce").fillna(0)
            ip_abs = pd.to_numeric(valid.get("IP_abs_i", valid.get("IP_context_abs", 0)), errors="coerce").fillna(0)
            num = float((w * ip).sum())
            den = float(w.sum())
            num_abs = float((w * ip_abs).sum())
            ip_final = float(num / den) if den > 0 else 0.0
            ip_abs_final = float(num_abs / den) if den > 0 else 0.0
            analyzed = int(len(valid))
        else:
            num = den = num_abs = 0.0
            ip_final = 0.0
            ip_abs_final = 0.0
            analyzed = 0

        def _m(col: str) -> float:
            if col not in metric_df.columns:
                return 0.0
            return float(pd.to_numeric(metric_df[col], errors="coerce").fillna(0).mean())

        return {
            "contexts": int(len(g)),
            "articles": int(g["doc_id"].nunique()) if "doc_id" in g.columns else 0,
            "n_content": n_content,
            "n_ideol": n_ideol,
            "n_met": n_met,
            "n_ew": float(n_ew.sum()),
            "n_em": float(n_em.sum()),
            "n_es": float(n_es.sum()),
            "weighted_emotion_sum": w_emotion,
            "idi": idi,
            "emi": emi,
            "mti": mti,
            "evi": evi,
            "evi_norm": evi_norm,
            "ip_final": ip_final,
            "ip_abs": ip_abs_final,
            "w_num": num,
            "w_den": den,
            "w_num_abs": num_abs,
            "idi_pct": _m("IDI_percentile"),
            "emi_pct": _m("EMI_percentile"),
            "mti_pct": _m("MTI_percentile"),
            "ip_pct": _m("IP_percentile"),
            "ip_abs_pct": _m("IP_abs_percentile"),
            "tech_excluded": int((pd.to_numeric(g["aggregation_weight"], errors="coerce").fillna(0) == 0).sum()),
            "analyzed": analyzed,
        }

    total = _bundle(df_ref)

    st.subheader(f"Референтный анализ: {ref_country}")
    st.caption(
        f"Калибровка работает в backend. Режим: `{evi_mode}`; база процентилей: `{_percentile_basis_internal_to_ru(percentile_basis)}`."
    )

    fmt_raw = f".{int(display_precision)}f"

    st.markdown("### 1) Формулы модели")
    st.code(
        "IDI = N_ideol / N_content\n"
        "EMI = (1/3*N_e_w + 2/3*N_e_m + N_e_s) / N_content\n"
        "MTI = N_met / N_content\n"
        "EVI = P_r - N_r; EVI_norm = EVI / 10\n"
        "IP_i = EVI_norm_i * (1 + IDI_i + EMI_i + MTI_i)\n"
        "IP_final = Σ(S_i * IP_i) / ΣS_i",
        language="text",
    )

    context_formula = total["evi_norm"] * (1.0 + total["idi"] + total["emi"] + total["mti"])
    st.code(
        "Подстановка по вашему корпусу:\n"
        f"IDI = N_ideol / N_content = {total['n_ideol']:.0f} / {total['n_content']:.0f} = {total['idi']:{fmt_raw}}\n"
        f"EMI = (1/3*N_e_w + 2/3*N_e_m + N_e_s) / N_content\n"
        f"    = (1/3*{total['n_ew']:.0f} + 2/3*{total['n_em']:.0f} + {total['n_es']:.0f}) / {total['n_content']:.0f}\n"
        f"    = {total['weighted_emotion_sum']:.4f} / {total['n_content']:.0f} = {total['emi']:{fmt_raw}}\n"
        f"MTI = N_met / N_content = {total['n_met']:.0f} / {total['n_content']:.0f} = {total['mti']:{fmt_raw}}\n"
        f"EVI = {total['evi']:.2f}; EVI_norm = EVI/10 = {total['evi_norm']:.4f}\n"
        f"IP_context(mean) = {total['evi_norm']:.4f} * (1 + {total['idi']:{fmt_raw}} + {total['emi']:{fmt_raw}} + {total['mti']:{fmt_raw}}) = {context_formula:.6f}\n"
        f"IP_final = Σ(S_i*IP_i)/ΣS_i = {total['w_num']:.6f} / {total['w_den']:.6f} = {total['ip_final']:.6f}",
        language="text",
    )
    st.caption(
        "Что это значит: IDI/EMI/MTI показывают плотность маркеров в тексте; "
        "EVI задает направление оценки (плюс/минус), а IP_final — итоговый вектор имиджа с учетом веса контекстов."
    )

    st.markdown("### 2) Реальные числа анализа")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Контексты", total["contexts"])
    c2.metric("Статьи", total["articles"])
    c3.metric("Техн. исключено", total["tech_excluded"])
    c4.metric("Знак имиджа", _sign_label(total["ip_final"]))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("IDI", f"{total['idi']:{fmt_raw}}")
    c6.metric("EMI", f"{total['emi']:{fmt_raw}}")
    c7.metric("MTI", f"{total['mti']:{fmt_raw}}")
    c8.metric("EVI", f"{total['evi']:.2f}")

    c9, c10, c11 = st.columns(3)
    c9.metric("IP_final", f"{total['ip_final']:.6f}")
    c10.metric("IP_abs_final", f"{total['ip_abs']:.6f}")
    c11.metric("Контексты в расчете", total["analyzed"])

    st.info(_interpret_ip(total["ip_final"], total["ip_abs"]))
    st.caption(
        "Коротко: если IP_final > 0, в корпусе доминирует положительный образ референта; "
        "если < 0 — отрицательный. IP_abs_final показывает силу воздействия вне зависимости от знака."
    )

    st.markdown("### 3) Шкалы 0–100 (эмпирические процентили)")
    evi_scale = max(0.0, min(100.0, (total["evi_norm"] + 1.0) * 50.0))
    p_df = pd.DataFrame(
        [
            {"indicator": "IDI", "percentile_0_100": total["idi_pct"]},
            {"indicator": "EMI", "percentile_0_100": total["emi_pct"]},
            {"indicator": "MTI", "percentile_0_100": total["mti_pct"]},
            {"indicator": "IP", "percentile_0_100": total["ip_pct"]},
            {"indicator": "IP_abs", "percentile_0_100": total["ip_abs_pct"]},
            {"indicator": "EVI_norm_scale", "percentile_0_100": evi_scale},
        ]
    )
    p_df["percentile_0_100"] = pd.to_numeric(p_df["percentile_0_100"], errors="coerce").fillna(0.0).clip(0.0, 100.0)
    st.dataframe(p_df, use_container_width=True)
    st.plotly_chart(
        px.bar(
            p_df,
            x="indicator",
            y="percentile_0_100",
            text=p_df["percentile_0_100"].map(lambda v: f"{v:.1f}"),
            range_y=[0, 100],
            template="plotly_dark",
            title="Положение индикаторов на шкале 0–100",
        ),
        use_container_width=True,
    )

    st.markdown("### 4) Мини-референты внутри выбранного референта")
    mini_terms_raw = [x.strip() for x in re.split(r"[,\n;]+", str(mini_referents_raw)) if x.strip()]
    # Deduplicate while preserving order to keep selectbox stable.
    seen_terms = set()
    mini_terms = []
    for t in mini_terms_raw:
        k = t.casefold()
        if k in seen_terms:
            continue
        seen_terms.add(k)
        mini_terms.append(t)
    if not mini_terms:
        st.caption("Добавьте мини-референты в левом меню, например: китайская экономика, си цзиньпин, пекин.")
    else:
        rows = []
        ctx_text = df_ref["context_text"].fillna("").astype(str)
        kw_text = df_ref["matched_keywords"].fillna("").astype(str)
        for term in mini_terms:
            try:
                # regex=False is much more stable for arbitrary user-entered mini-referents.
                mask = ctx_text.str.contains(term, case=False, regex=False) | kw_text.str.contains(term, case=False, regex=False)
            except Exception:
                mask = pd.Series([False] * len(df_ref), index=df_ref.index)
            sub = df_ref[mask].copy()
            b = _bundle(sub)
            rows.append(
                {
                    "mini_referent": term,
                    "contexts": b["contexts"],
                    "articles": b["articles"],
                    "IDI": b["idi"],
                    "EMI": b["emi"],
                    "MTI": b["mti"],
                    "EVI": b["evi"],
                    "IP_final": b["ip_final"],
                    "IP_abs": b["ip_abs"],
                    "IDI_pctl": b["idi_pct"],
                    "EMI_pctl": b["emi_pct"],
                    "MTI_pctl": b["mti_pct"],
                    "IP_pctl": b["ip_pct"],
                }
            )

        mini_df = pd.DataFrame(rows)
        st.dataframe(mini_df, use_container_width=True)

        if not mini_df.empty:
            st.plotly_chart(
                px.bar(
                    mini_df,
                    x="mini_referent",
                    y="IP_final",
                    color="contexts",
                    template="plotly_dark",
                    title="IP_final по мини-референтам",
                ),
                use_container_width=True,
            )
            pcols = ["mini_referent", "IDI_pctl", "EMI_pctl", "MTI_pctl", "IP_pctl"]
            m2 = mini_df[pcols].melt(id_vars=["mini_referent"], var_name="indicator", value_name="percentile")
            st.plotly_chart(
                px.line(
                    m2,
                    x="mini_referent",
                    y="percentile",
                    color="indicator",
                    markers=True,
                    template="plotly_dark",
                    title="Процентили 0–100 по мини-референтам",
                ),
                use_container_width=True,
            )

            term_options = mini_df["mini_referent"].tolist()
            term_pick = st.selectbox(
                "Показать контексты по мини-референту",
                options=term_options,
                index=0,
                key="mini_ref_pick",
            )
            try:
                mask = ctx_text.str.contains(term_pick, case=False, regex=False) | kw_text.str.contains(term_pick, case=False, regex=False)
            except Exception:
                mask = pd.Series([False] * len(df_ref), index=df_ref.index)
            sub_ctx = df_ref[mask].copy()
            sub_ctx["context_preview"] = sub_ctx["context_text"].fillna("").astype(str).str.slice(0, 500)
            cols = [
                c
                for c in [
                    "context_id",
                    "outlet_name",
                    "date",
                    "matched_keywords",
                    "EVI",
                    "EVI_norm",
                    "IP_i",
                    "context_preview",
                ]
                if c in sub_ctx.columns
            ]
            st.caption("Легкий режим показа контекстов (таблица), чтобы не перегружать страницу.")
            show_n = min(200, len(sub_ctx))
            st.dataframe(sub_ctx[cols].head(show_n), use_container_width=True)
            max_export_rows = 5000
            export_df = sub_ctx[cols].head(max_export_rows).copy()
            if len(sub_ctx) > max_export_rows:
                st.warning(
                    f"Для стабильности экспорт ограничен первыми {max_export_rows} строками "
                    f"(всего найдено: {len(sub_ctx)})."
                )
            csv_data = export_df.to_csv(index=False).encode("utf-8")
            safe_term = re.sub(r"[^a-zA-Z0-9_-]+", "_", term_pick).strip("_") or "mini_referent"
            st.download_button(
                label=f"Скачать контексты мини-референта: {term_pick}",
                data=csv_data,
                file_name=f"mini_referent_{safe_term}.csv",
                mime="text/csv",
            )

    st.markdown("### 5) Контексты и маркерные обоснования")
    view_cols = [c for c in ["context_id", "outlet_name", "date", "matched_keywords", "IDI", "EMI", "MTI", "EVI_raw", "referent_salience", "IP_i"] if c in df_ref.columns]
    st.dataframe(df_ref[view_cols].head(200), use_container_width=True)
    context_ids = df_ref["context_id"].astype(str).tolist()
    if context_ids:
        pick_ctx = st.selectbox("Выберите context_id для детального маркерного просмотра", context_ids, index=0, key="ctx_marker_pick")
        r = df_ref[df_ref["context_id"].astype(str) == str(pick_ctx)].head(1)
        if not r.empty:
            rr = r.iloc[0]
            st.caption(str(rr.get("context_text", ""))[:1500])
            mtp = out_dir / "marker_traces.csv"
            ctx_traces = pd.DataFrame()
            if mtp.exists():
                try:
                    ctx_traces = pd.read_csv(mtp).fillna("")
                    ctx_traces = ctx_traces[ctx_traces["context_id"].astype(str) == str(pick_ctx)]
                except Exception:
                    ctx_traces = pd.DataFrame()

            def _clean_text_value(v: object) -> str:
                if v is None:
                    return ""
                sv = str(v).strip()
                if sv == "" or sv.casefold() in {"nan", "none", "null"}:
                    return ""
                return sv

            def _terms_from_traces(indicator: str) -> str:
                if ctx_traces.empty or "indicator" not in ctx_traces.columns:
                    return ""
                sub = ctx_traces[ctx_traces["indicator"].astype(str).str.casefold() == indicator.casefold()]
                if sub.empty:
                    return ""
                terms = []
                if "term_found" in sub.columns:
                    terms = [str(x).strip() for x in sub["term_found"].tolist() if str(x).strip()]
                terms = sorted(set(terms))
                return ", ".join(terms)

            idi_txt = _clean_text_value(rr.get("found_ideol_markers", ""))
            emi_txt = _clean_text_value(rr.get("found_emotional_markers", ""))
            mti_txt = _clean_text_value(rr.get("found_metaphor_markers", ""))
            if not idi_txt:
                idi_txt = _terms_from_traces("IDI")
            if not emi_txt:
                emi_txt = _terms_from_traces("EMI")
            if not mti_txt:
                mti_txt = _terms_from_traces("MTI")

            with st.expander("Найденные IDI-маркеры", expanded=False):
                st.write(idi_txt or "Маркеры не найдены в этом контексте.")
            with st.expander("Найденные EMI-маркеры", expanded=False):
                st.write(emi_txt or "Маркеры не найдены в этом контексте.")
            with st.expander("Найденные MTI-маркеры", expanded=False):
                st.write(mti_txt or "Маркеры не найдены в этом контексте.")
            with st.expander("Обоснование EVI", expanded=False):
                evi_expl = _clean_text_value(rr.get("evi_explanation", ""))
                pos_terms = _clean_text_value(rr.get("positive_evidence_terms", ""))
                neg_terms = _clean_text_value(rr.get("negative_evidence_terms", ""))
                evi_ev = _clean_text_value(rr.get("evi_evidence", "{}")) or "{}"
                st.write(evi_expl or "Явной оценочной аргументации в этом контексте не обнаружено.")
                st.write(f"Positive terms: {pos_terms or '—'}")
                st.write(f"Negative terms: {neg_terms or '—'}")
                st.write(f"EVI evidence JSON: {evi_ev}")
            with st.expander("Обоснование S_r", expanded=False):
                s_label = _clean_text_value(rr.get("salience_label", ""))
                s_exp = _clean_text_value(rr.get("salience_explanation", "")) or _clean_text_value(rr.get("technical_mention_reason", ""))
                st.write(f"S_r = {rr.get('referent_salience', 0)}; label = {s_label or '—'}")
                st.write(s_exp or "Подробное обоснование значимости не указано.")
            with st.expander("Все marker traces (этот context_id)", expanded=False):
                if not ctx_traces.empty:
                    st.dataframe(ctx_traces, use_container_width=True)
                else:
                    st.caption("Файл marker_traces.csv не найден.")

    marker_dict_dir = out_dir / "referent_dicts"
    if not marker_dict_dir.exists():
        marker_dict_dir = ROOT_DIR / "lexicons"
    _render_marker_base_panel(marker_dict_dir)



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


@st.cache_data(show_spinner=False)
def _cached_read_csv(path_str: str) -> pd.DataFrame:
    return _safe_read_csv(Path(path_str))


def _load_result_tables(out_dir: Path) -> Dict[str, pd.DataFrame]:
    return {
        "contexts": _cached_read_csv(str(out_dir / "contexts_full.csv")),
        "marker_traces": _cached_read_csv(str(out_dir / "marker_traces.csv")),
        "formula_traces": _cached_read_csv(str(out_dir / "formula_traces.csv")),
        "summary_media_ref": _cached_read_csv(str(out_dir / "aggregated_by_media_country_and_ref_country.csv")),
        "flagged": _cached_read_csv(str(out_dir / "flagged_cases.csv")),
    }


def _compute_ref_summary(df_all: pd.DataFrame, ref_country: str, exclude_technical_mentions: bool) -> Dict[str, float]:
    df = df_all.copy()
    if "ref_country" in df.columns:
        df = df[df["ref_country"].astype(str) == str(ref_country)].copy()
    if df.empty:
        return {
            "contexts_with_ref": 0,
            "documents_with_ref": 0,
            "central_contexts": 0,
            "technical_excluded": 0,
            "contexts_analyzed": 0,
            "sum_salience": 0.0,
            "IDI": 0.0,
            "EMI": 0.0,
            "MTI": 0.0,
            "EVI": 0.0,
            "EVI_norm": 0.0,
            "IP_final": 0.0,
            "IP_abs_final": 0.0,
            "n_ideol": 0.0,
            "n_content": 0.0,
            "n_ew": 0.0,
            "n_em": 0.0,
            "n_es": 0.0,
            "n_met": 0.0,
            "weighted_emo_sum": 0.0,
            "ip_num": 0.0,
            "ip_den": 0.0,
        }

    for col in ["IDI", "EMI", "MTI", "EVI_raw", "EVI_norm", "IP_i", "referent_salience", "N_content", "N_ideol", "N_e_w", "N_e_m", "N_e_s", "N_met"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    if "IP_i" not in df.columns or (df["IP_i"] == 0).all():
        df["IP_i"] = df["EVI_norm"] * (1.0 + df["IDI"] + df["EMI"] + df["MTI"])
    df["IP_abs_i"] = df["IP_i"].abs()

    df["aggregation_weight"] = df["referent_salience"].clip(lower=0.0, upper=1.0)
    if exclude_technical_mentions and "is_technical_mention" in df.columns:
        tech_mask = df["is_technical_mention"].fillna(False).astype(bool)
        df.loc[tech_mask, "aggregation_weight"] = 0.0

    valid = df[df["aggregation_weight"] > 0].copy()
    metric_df = valid if not valid.empty else df

    n_content = float(metric_df["N_content"].sum())
    safe_n_content = max(n_content, 1.0)
    n_ideol = float(metric_df["N_ideol"].sum())
    n_ew = float(metric_df["N_e_w"].sum())
    n_em = float(metric_df["N_e_m"].sum())
    n_es = float(metric_df["N_e_s"].sum())
    n_met = float(metric_df["N_met"].sum())
    weighted_emo = float((metric_df["N_e_w"] / 3.0 + (2.0 * metric_df["N_e_m"] / 3.0) + metric_df["N_e_s"]).sum())

    idi = float(n_ideol / safe_n_content)
    emi = float(weighted_emo / safe_n_content)
    mti = float(n_met / safe_n_content)
    evi = float(metric_df["EVI_raw"].mean()) if not metric_df.empty else 0.0
    evi_norm = float(metric_df["EVI_norm"].mean()) if not metric_df.empty else 0.0

    w = valid["aggregation_weight"] if not valid.empty else pd.Series(dtype=float)
    ip_num = float((valid["IP_i"] * w).sum()) if not valid.empty else 0.0
    ip_abs_num = float((valid["IP_abs_i"] * w).sum()) if not valid.empty else 0.0
    ip_den = float(w.sum()) if not valid.empty else 0.0
    ip_final = float(ip_num / ip_den) if ip_den > 0 else 0.0
    ip_abs_final = float(ip_abs_num / ip_den) if ip_den > 0 else 0.0

    return {
        "contexts_with_ref": int(len(df)),
        "documents_with_ref": int(df["doc_id"].nunique()) if "doc_id" in df.columns else 0,
        "central_contexts": int((df["referent_salience"] == 1.0).sum()),
        "technical_excluded": int((df["aggregation_weight"] == 0).sum()),
        "contexts_analyzed": int(len(valid)),
        "sum_salience": ip_den,
        "IDI": idi,
        "EMI": emi,
        "MTI": mti,
        "EVI": evi,
        "EVI_norm": evi_norm,
        "IP_final": ip_final,
        "IP_abs_final": ip_abs_final,
        "n_ideol": n_ideol,
        "n_content": n_content,
        "n_ew": n_ew,
        "n_em": n_em,
        "n_es": n_es,
        "n_met": n_met,
        "weighted_emo_sum": weighted_emo,
        "ip_num": ip_num,
        "ip_den": ip_den,
    }


def _diagnostics_lite(df_ref: pd.DataFrame, summary: Dict[str, float], out_dir: Path) -> pd.DataFrame:
    rows = []
    contexts = int(summary.get("contexts_with_ref", 0))
    technical = int(summary.get("technical_excluded", 0))

    rows.append({"check": "contexts_found", "status": "ok" if contexts > 0 else "warn", "details": f"contexts={contexts}"})
    rows.append(
        {
            "check": "technical_mentions_detected",
            "status": "warn" if (contexts > 500 and technical == 0) else "ok",
            "details": f"technical_excluded={technical}, contexts={contexts}",
        }
    )
    cal_present = (out_dir / "calibration" / "calibration_contexts.csv").exists() or DEFAULT_CALIBRATION_CONTEXTS_PATH.exists()
    rows.append({"check": "calibration_present", "status": "ok" if cal_present else "warn", "details": "default/local calibration file"})
    rows.append(
        {
            "check": "formula_traces_present",
            "status": "ok" if (out_dir / "formula_traces.json").exists() else "warn",
            "details": "formula_traces.json",
        }
    )

    if df_ref.empty:
        rows.append({"check": "evi_range", "status": "warn", "details": "no rows"})
        rows.append({"check": "evi_norm_match", "status": "warn", "details": "no rows"})
        rows.append({"check": "ip_formula_match", "status": "warn", "details": "no rows"})
    else:
        evi_bad = int((~pd.to_numeric(df_ref.get("EVI_raw", 0), errors="coerce").fillna(0).between(-10, 10)).sum())
        rows.append({"check": "evi_range", "status": "ok" if evi_bad == 0 else "warn", "details": f"bad_rows={evi_bad}"})
        evi_norm = pd.to_numeric(df_ref.get("EVI_norm", 0), errors="coerce").fillna(0)
        evi_raw = pd.to_numeric(df_ref.get("EVI_raw", 0), errors="coerce").fillna(0)
        norm_mis = int((evi_norm - (evi_raw / 10.0)).abs().gt(1e-6).sum())
        rows.append({"check": "evi_norm_match", "status": "ok" if norm_mis == 0 else "warn", "details": f"mismatch_rows={norm_mis}"})
        idi = pd.to_numeric(df_ref.get("IDI", 0), errors="coerce").fillna(0)
        emi = pd.to_numeric(df_ref.get("EMI", 0), errors="coerce").fillna(0)
        mti = pd.to_numeric(df_ref.get("MTI", 0), errors="coerce").fillna(0)
        ipi = pd.to_numeric(df_ref.get("IP_i", 0), errors="coerce").fillna(0)
        ip_should = evi_norm * (1.0 + idi + emi + mti)
        ip_mis = int((ipi - ip_should).abs().gt(1e-6).sum())
        rows.append({"check": "ip_formula_match", "status": "ok" if ip_mis == 0 else "warn", "details": f"mismatch_rows={ip_mis}"})
    rows.append(
        {
            "check": "sum_salience_nonzero",
            "status": "ok" if float(summary.get("sum_salience", 0.0)) > 0 else "warn",
            "details": f"sum_S={float(summary.get('sum_salience', 0.0)):.6f}",
        }
    )
    return pd.DataFrame(rows)


def main() -> None:
    st.set_page_config(page_title="media analizator 1.1", layout="wide")
    st.title("media analizator 1.1")
    st.caption(f"Легкая Streamlit demo/beta-версия референтного анализа (China/USA/Russia). Build: {APP_BUILD}")

    if "analysis_out_dir" not in st.session_state:
        st.session_state["analysis_out_dir"] = ""
    if "analysis_stats" not in st.session_state:
        st.session_state["analysis_stats"] = {}
    if "analysis_zip" not in st.session_state:
        st.session_state["analysis_zip"] = b""
    if "analysis_error" not in st.session_state:
        st.session_state["analysis_error"] = ""

    with st.sidebar:
        st.header("Параметры")
        min_year = st.number_input("Минимальный год", min_value=2000, max_value=2100, value=2022)
        max_year = st.number_input("Максимальный год", min_value=2000, max_value=2100, value=2026)
        referent_target = st.selectbox("Целевой референт", options=["China", "USA", "Russia"], index=0)
        exclude_technical_mentions = st.toggle("Исключать технические упоминания", value=True)
        mini_referents_raw = st.text_area("Мини-референты (через запятую/новую строку)", value="", height=100)
        with st.expander("Точность и отображение", expanded=False):
            display_precision = st.selectbox("Точность raw-значений", options=[4, 6, 8], index=1)
            show_percent_values = st.toggle("Показывать значения в %", value=True)
            show_empirical_percentiles = st.toggle("Показывать процентили 0–100", value=True)

        if st.button("Очистить кэш / сбросить анализ"):
            st.cache_data.clear()
            st.cache_resource.clear()
            for k in ["analysis_out_dir", "analysis_stats", "analysis_zip", "analysis_error"]:
                st.session_state[k] = "" if k in {"analysis_out_dir", "analysis_error"} else (b"" if k == "analysis_zip" else {})
            st.success("Кэш очищен, состояние анализа сброшено.")

    tab_analysis, tab_results, tab_contexts, tab_diag, tab_export = st.tabs(
        ["Анализ", "Результаты", "Контексты", "Диагностика", "Экспорт"]
    )

    with tab_analysis:
        with st.form("analysis_form"):
            c1, c2 = st.columns(2)
            with c1:
                zip_upload = st.file_uploader("ZIP с корпусом (.zip)", type=["zip"], accept_multiple_files=False)
            with c2:
                txt_uploads = st.file_uploader(
                    "Или отдельные файлы (.txt/.md/.docx/.pdf/.csv/.xlsx/.json)",
                    type=["txt", "md", "text", "docx", "pdf", "csv", "xlsx", "xls", "json"],
                    accept_multiple_files=True,
                )
            st.markdown("### Или вставьте текст вручную")
            manual_text = st.text_area("Текст для анализа", height=160)
            m1, m2, m3 = st.columns(3)
            with m1:
                manual_title = st.text_input("Заголовок (опционально)", value="Manual input")
            with m2:
                manual_source = st.text_input("Источник (опционально)", value="Manual")
            with m3:
                manual_year = st.number_input("Год ручного текста", min_value=2000, max_value=2100, value=2026)
            run_btn = st.form_submit_button("Запустить анализ", type="primary")

        if run_btn:
            if referent_core is None:
                st.error("Референтный backend недоступен (media_analyzer_referent.py).")
            else:
                prog_box = st.empty()
                prog_bar = st.progress(0, text="Инициализация анализа...")
                progress_state = {"docs": 0, "contexts": 0, "technical": 0, "ref": referent_target}

                def set_stage(stage_key: str, **kwargs) -> None:
                    if stage_key not in PROGRESS_STAGES:
                        return
                    progress_state.update(kwargs)
                    pct, text = PROGRESS_STAGES[stage_key]
                    details = (
                        f"Документы: {int(progress_state.get('docs', 0))} | "
                        f"Контексты: {int(progress_state.get('contexts', 0))} | "
                        f"Technical excluded: {int(progress_state.get('technical', 0))} | "
                        f"Ref: {progress_state.get('ref', 'all')}"
                    )
                    prog_bar.progress(int(pct), text=text)
                    prog_box.markdown(f"**Статус анализа корпуса:** {pct}%  \n{text}  \n{details}")

                set_stage("init")
                file_items: List[Tuple[str, str]] = []
                set_stage("load")
                if zip_upload is not None:
                    file_items.extend(read_zip_corpus_files(zip_upload.getvalue()))
                file_items.extend(read_uploaded_corpus_files(txt_uploads))
                if manual_text.strip():
                    manual_blob = f"Title: {manual_title}\nDate: {int(manual_year)}\nSource: {manual_source}\n\n{manual_text.strip()}"
                    file_items.append((f"manual_{int(manual_year)}.txt", manual_blob))

                uniq = {}
                for n, t in file_items:
                    content_md5 = hashlib.md5(t.encode("utf-8", errors="ignore")).hexdigest()
                    uniq[f"{n}|{len(t)}|{content_md5}"] = (n, t)
                file_items = list(uniq.values())
                set_stage("clean", docs=len(file_items), ref=referent_target)
                if not file_items:
                    st.warning("Не найдено входных текстов. Загрузите ZIP/файлы или вставьте текст вручную.")
                else:
                    input_df = build_referent_input_df(file_items, int(min_year), int(max_year))
                    if input_df.empty:
                        st.warning("После фильтрации по годам не осталось документов.")
                    else:
                        prefiltered_df = _quick_prefilter_by_referent(input_df, referent_target)
                        if prefiltered_df.empty:
                            set_stage("find_refs", docs=len(input_df), contexts=0, ref=referent_target)
                            st.warning(
                                "Для выбранного референта не найдено ни одного кандидата на уровне быстрых ключевых совпадений. "
                                "Анализ остановлен до этапа IP, чтобы избежать зависания."
                            )
                            st.session_state["analysis_error"] = ""
                            return

                        out_root = Path(tempfile.mkdtemp(prefix="sea_media_analysis_"))
                        out_dir = out_root / "analysis_output"
                        out_dir.mkdir(parents=True, exist_ok=True)
                        calibration_dir = out_dir / "calibration"
                        calibration_dir.mkdir(parents=True, exist_ok=True)
                        _mirror_default_calibration_assets(calibration_dir)
                        calibration_texts_df = _safe_read_csv(DEFAULT_CALIBRATION_TEXTS_PATH) if DEFAULT_CALIBRATION_TEXTS_PATH.exists() else pd.DataFrame()
                        calibration_contexts_df = _safe_read_csv(DEFAULT_CALIBRATION_CONTEXTS_PATH) if DEFAULT_CALIBRATION_CONTEXTS_PATH.exists() else pd.DataFrame()

                        set_stage("segment", docs=len(prefiltered_df), ref=referent_target)
                        set_stage("find_refs", docs=len(prefiltered_df), ref=referent_target)
                        set_stage("extract_ctx", docs=len(prefiltered_df), ref=referent_target)
                        set_stage("salience", docs=len(prefiltered_df), ref=referent_target)
                        set_stage("ling", docs=len(prefiltered_df), ref=referent_target)
                        set_stage("idi", docs=len(prefiltered_df), ref=referent_target)
                        set_stage("emi", docs=len(prefiltered_df), ref=referent_target)
                        set_stage("mti", docs=len(prefiltered_df), ref=referent_target)
                        set_stage("evi", docs=len(prefiltered_df), ref=referent_target)
                        set_stage("ip", docs=len(prefiltered_df), ref=referent_target)

                        try:
                            stats = run_referent_analysis(
                                input_df=prefiltered_df,
                                out_dir=out_dir,
                                evi_mode="suggested",
                                exclude_technical_mentions=exclude_technical_mentions,
                                calibration_path=DEFAULT_CALIBRATION_TEXTS_PATH if DEFAULT_CALIBRATION_TEXTS_PATH.exists() else None,
                                ip_formula_mode="updated: EVI_norm * (1 + IDI + EMI + MTI)",
                                aggregation_mode="weighted by S_r",
                                percentile_basis="full corpus",
                                calibration_texts_df=calibration_texts_df if not calibration_texts_df.empty else None,
                                calibration_contexts_df=calibration_contexts_df if not calibration_contexts_df.empty else None,
                                calibration_filter="full_calibration_corpus",
                                use_empirical_percentile_interpretation=True,
                                lexicon_version=str(st.session_state.get("lexicon_version", "default")),
                                target_ref_country=referent_target,
                            )
                        except Exception as e:
                            st.session_state["analysis_error"] = str(e)
                            st.error(str(e))
                        else:
                            st.session_state["analysis_out_dir"] = str(out_dir)
                            st.session_state["analysis_stats"] = stats
                            st.session_state["analysis_zip"] = zip_dir_bytes(out_dir)
                            st.session_state["analysis_error"] = ""
                            set_stage("calib", docs=stats["docs"], contexts=stats["contexts"], technical=stats.get("technical_excluded", 0), ref=referent_target)
                            set_stage("agg", docs=stats["docs"], contexts=stats["contexts"], technical=stats.get("technical_excluded", 0), ref=referent_target)
                            set_stage("final", docs=stats["docs"], contexts=stats["contexts"], technical=stats.get("technical_excluded", 0), ref=referent_target)
                            st.success(f"Готово. Документов: {stats['docs']}, контекстов: {stats['contexts']}, flagged: {stats['flagged']}")

        if not ENABLE_ADVANCED_CALIBRATION_UI:
            st.caption("Расширенный calibration UI отключён в Streamlit demo 1.1 и развивается в локальной версии.")

    out_dir_str = str(st.session_state.get("analysis_out_dir", "") or "")
    out_dir = Path(out_dir_str) if out_dir_str else None
    tables = _load_result_tables(out_dir) if out_dir and out_dir.exists() else {
        "contexts": pd.DataFrame(), "marker_traces": pd.DataFrame(), "formula_traces": pd.DataFrame(), "summary_media_ref": pd.DataFrame(), "flagged": pd.DataFrame()
    }
    df_all = tables["contexts"].copy()

    with tab_results:
        if df_all.empty:
            st.info("Сначала запустите анализ во вкладке «Анализ».")
        else:
            refs = [r for r in ["China", "USA", "Russia"] if r in set(df_all.get("ref_country", pd.Series(dtype=str)).astype(str))]
            ref_pick = st.selectbox("Референт", options=refs if refs else ["China", "USA", "Russia"], index=0, key="res_ref_pick")
            summary = _compute_ref_summary(df_all, ref_pick, exclude_technical_mentions)
            fmt_raw = f".{int(display_precision)}f"
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Документов с референтом", summary["documents_with_ref"])
            c2.metric("Контекстов с референтом", summary["contexts_with_ref"])
            c3.metric("Центральных контекстов", summary["central_contexts"])
            c4.metric("Technical excluded", summary["technical_excluded"])
            c5.metric("IP_final", f"{summary['IP_final']:.6f}")
            c6, c7, c8, c9, c10 = st.columns(5)
            c6.metric("IDI", f"{summary['IDI']:{fmt_raw}}")
            c7.metric("EMI", f"{summary['EMI']:{fmt_raw}}")
            c8.metric("MTI", f"{summary['MTI']:{fmt_raw}}")
            c9.metric("EVI", f"{summary['EVI']:.2f}")
            c10.metric("EVI_norm", f"{summary['EVI_norm']:.4f}")
            c11, c12 = st.columns(2)
            c11.metric("IP_abs_final", f"{summary['IP_abs_final']:.6f}")
            c12.metric("Контексты в расчете", summary["contexts_analyzed"])
            st.info(_interpret_ip(summary["IP_final"], summary["IP_abs_final"]))

            st.markdown("#### Что означают показатели простыми словами")
            sign_label = "положительный" if summary["IP_final"] > 0 else ("отрицательный" if summary["IP_final"] < 0 else "нейтральный")
            st.markdown(
                "- `IDI`: доля идеологических маркеров в контекстах референта.\n"
                "- `EMI`: доля эмоциональных маркеров (с учетом силы эмоции).\n"
                "- `MTI`: доля метафорических маркеров.\n"
                "- `EVI`: итоговая оценка образа референта по шкале от -10 до +10.\n"
                "- `EVI_norm`: нормировка `EVI` в диапазон от -1 до +1.\n"
                "- `IP_final`: общий вектор воздействия образа (знак = направление).\n"
                "- `IP_abs_final`: сила воздействия без учета знака (насколько образ вообще «сильный»).\n"
                f"- По текущему расчету: образ `{ref_pick}` сейчас **{sign_label}**."
            )

            with st.expander("Пояснение формул для не-специалиста", expanded=False):
                st.markdown(
                    "1. Сначала считаем, сколько в текстах идеологических, эмоциональных и метафорических маркеров.\n"
                    "2. Затем считаем оценку образа (`EVI`): позитивные признаки минус негативные.\n"
                    "3. Нормируем оценку (`EVI_norm = EVI/10`), чтобы получить удобный диапазон от -1 до +1.\n"
                    "4. Для каждого контекста считаем воздействие: `IP_i = EVI_norm × (1 + IDI + EMI + MTI)`.\n"
                    "5. В финале усредняем `IP_i` с весами `S_r` (важность упоминания референта):\n"
                    "`IP_final = Σ(S_i × IP_i) / ΣS_i`."
                )

            st.markdown("#### Формулы и числовая подстановка")
            context_formula = summary["EVI_norm"] * (1.0 + summary["IDI"] + summary["EMI"] + summary["MTI"])
            st.code(
                "IDI = N_ideol / N_content\n"
                f"IDI = {summary['n_ideol']:.0f} / {max(summary['n_content'], 1.0):.0f} = {summary['IDI']:{fmt_raw}}\n\n"
                "EMI = (1/3*N_e_w + 2/3*N_e_m + N_e_s) / N_content\n"
                f"EMI = (1/3*{summary['n_ew']:.0f} + 2/3*{summary['n_em']:.0f} + {summary['n_es']:.0f}) / {max(summary['n_content'], 1.0):.0f} = {summary['EMI']:{fmt_raw}}\n\n"
                "MTI = N_met / N_content\n"
                f"MTI = {summary['n_met']:.0f} / {max(summary['n_content'], 1.0):.0f} = {summary['MTI']:{fmt_raw}}\n\n"
                "EVI = P_r - N_r\n"
                f"EVI = {summary['EVI']:.2f}; EVI_norm = EVI / 10 = {summary['EVI_norm']:.4f}\n\n"
                "IP_i = EVI_norm_i * (1 + IDI_i + EMI_i + MTI_i)\n"
                f"IP_context(mean) = {summary['EVI_norm']:.4f} * (1 + {summary['IDI']:{fmt_raw}} + {summary['EMI']:{fmt_raw}} + {summary['MTI']:{fmt_raw}}) = {context_formula:.6f}\n"
                "IP_final = Σ(S_i * IP_i) / ΣS_i\n"
                f"IP_final = {summary['ip_num']:.6f} / {summary['ip_den']:.6f} = {summary['IP_final']:.6f}\n"
                "IP_abs_final = Σ(S_i * |IP_i|) / ΣS_i",
                language="text",
            )
            if show_percent_values:
                st.caption(
                    f"IDI={summary['IDI']*100:.4f}% | EMI={summary['EMI']*100:.4f}% | MTI={summary['MTI']*100:.4f}%"
                )
            if show_empirical_percentiles and "IP_percentile" in df_all.columns:
                df_ref = df_all[df_all["ref_country"].astype(str) == str(ref_pick)].copy()
                ip_pct = float(pd.to_numeric(df_ref.get("IP_percentile", 0), errors="coerce").fillna(0).mean()) if not df_ref.empty else 0.0
                st.caption(f"Эмпирический процентиль IP (0–100): {ip_pct:.1f}")

    with tab_contexts:
        if df_all.empty:
            st.info("Нет contexts_full.csv. Запустите анализ.")
        else:
            df_view = _build_referent_view_df(df_all.copy())
            if "ref_country" in df_view.columns:
                df_view = df_view[df_view["ref_country"].astype(str) == str(st.session_state.get("res_ref_pick", referent_target))]
            st.dataframe(df_view.head(200), use_container_width=True)
            if not df_view.empty and "context_id" in df_view.columns:
                ctx_ids = df_view["context_id"].astype(str).tolist()
                ctx_pick = st.selectbox("Выберите context_id", options=ctx_ids, index=0)
                row = df_view[df_view["context_id"].astype(str) == str(ctx_pick)].head(1)
                if not row.empty:
                    r = row.iloc[0]
                    st.markdown(f"**context_id:** `{r.get('context_id','')}`")
                    st.caption(f"source/outlet/date: {r.get('source','')} / {r.get('outlet_name','')} / {r.get('date','')}")
                    st.caption(f"ref_country: {r.get('ref_country','')}")
                    st.write(str(r.get("context_text", "")))
                    st.markdown(
                        f"- IDI={float(pd.to_numeric(r.get('IDI',0), errors='coerce')):.6f}\n"
                        f"- EMI={float(pd.to_numeric(r.get('EMI',0), errors='coerce')):.6f}\n"
                        f"- MTI={float(pd.to_numeric(r.get('MTI',0), errors='coerce')):.6f}\n"
                        f"- EVI={float(pd.to_numeric(r.get('EVI_raw',0), errors='coerce')):.2f}\n"
                        f"- EVI_norm={float(pd.to_numeric(r.get('EVI_norm',0), errors='coerce')):.4f}\n"
                        f"- S_r={float(pd.to_numeric(r.get('referent_salience',0), errors='coerce')):.2f}\n"
                        f"- IP_i={float(pd.to_numeric(r.get('IP_i',0), errors='coerce')):.6f}"
                    )
                    st.caption(f"EVI explanation: {r.get('evi_explanation','')}")
                    st.caption(f"S_r explanation: {r.get('salience_explanation','')}")
                    mt = tables["marker_traces"]
                    if mt.empty:
                        st.caption("marker_traces.csv не найден или пуст.")
                    else:
                        mt_ctx = mt[mt.get("context_id", "").astype(str) == str(ctx_pick)].copy()
                        if mt_ctx.empty:
                            st.caption("Для выбранного context_id marker traces не найдены.")
                        else:
                            st.dataframe(mt_ctx.head(200), use_container_width=True)

    with tab_diag:
        if df_all.empty:
            st.info("Диагностика будет доступна после анализа.")
        else:
            ref_pick = str(st.session_state.get("res_ref_pick", referent_target))
            summary = _compute_ref_summary(df_all, ref_pick, exclude_technical_mentions)
            df_ref = df_all[df_all["ref_country"].astype(str) == ref_pick].copy() if "ref_country" in df_all.columns else pd.DataFrame()
            diag = _diagnostics_lite(df_ref, summary, out_dir) if out_dir else pd.DataFrame(columns=["check", "status", "details"])
            st.dataframe(diag, use_container_width=True)
            if (diag["status"] == "warn").any():
                st.warning("Есть предупреждения диагностики. Это не падение, но требует проверки корпуса/словарей.")

    with tab_export:
        if not out_dir or not out_dir.exists():
            st.info("Сначала выполните анализ.")
        else:
            if st.session_state.get("analysis_zip", b""):
                st.download_button(
                    label="Скачать результаты анализа (ZIP)",
                    data=st.session_state["analysis_zip"],
                    file_name="media_analizator_1_1_output.zip",
                    mime="application/zip",
                )
            else:
                st.caption("ZIP ещё не сформирован.")
            if not ENABLE_HEAVY_EXPORTS_IN_UI:
                st.caption("Тяжёлые Excel/расширенные экспортные блоки отключены в Streamlit demo 1.1 для стабильности.")


if __name__ == "__main__":
    main()
