#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import hashlib
import io
import re
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import List, Tuple

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

APP_BUILD = "2026-05-08-14:15"


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


def read_zip_corpus_files(zip_bytes: bytes) -> List[Tuple[str, str]]:
    out = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = info.filename
            if not name.casefold().endswith((".txt", ".md", ".text", ".docx", ".pdf")):
                continue
            raw = extract_raw_by_extension(name, zf.read(info))
            if not raw.strip():
                continue
            out.append((name, raw))
    return out


def read_uploaded_corpus_files(files) -> List[Tuple[str, str]]:
    out = []
    for f in files or []:
        name = f.name
        raw = extract_raw_by_extension(name, f.getvalue())
        if not raw.strip():
            continue
        out.append((name, raw))
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
        media_country = map_source_to_media_country(source)
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


def run_referent_analysis(input_df: pd.DataFrame, out_dir: Path, evi_mode: str):
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
        evi_manual_path=None,
        metaphor_review_path=None,
    )
    scored = referent_core.add_multicountry_flags(scored)
    scored = scored[scored["ref_country"].isin(referent_core.REF_COUNTRIES)].copy()
    scored.loc[(scored["N_content"] <= 0), ["IDI", "EMI", "MTI", "IP"]] = 0.0
    scored.loc[(~scored["EVI"].isin(referent_core.EVI_ALLOWED)), "EVI"] = 0
    scored.loc[(scored["EVI"] == 0), "IP"] = 0.0
    for col in ["IDI", "EMI", "MTI"]:
        scored[col] = scored[col].clip(lower=0.0, upper=1.0)
    scored["IP"] = scored["IP"].clip(lower=-6.0, upper=6.0)

    by_article, by_outlet, by_media_ref, matrix = referent_core.aggregate_outputs(scored)
    flagged = referent_core.build_flagged_cases(scored)
    referent_core.save_outputs(scored, by_article, by_outlet, by_media_ref, matrix, flagged, out_dir)

    return {
        "docs": len(docs),
        "contexts": len(scored),
        "flagged": len(flagged),
    }


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
        referent_evi_mode = st.selectbox(
            "EVI режим (референтный анализ)",
            options=["suggested", "manual"],
            index=0,
            help="suggested: авто-подсказка EVI; manual: без файла разметки EVI по умолчанию 0.",
        )
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
            "Или отдельные файлы (.txt/.md/.docx/.pdf)",
            type=["txt", "md", "text", "docx", "pdf"],
            accept_multiple_files=True,
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

        if not file_items:
            st.error("Не найдено входных текстов. Загрузите ZIP/файлы или вставьте текст вручную.")
            return

        with tempfile.TemporaryDirectory(prefix="sea_media_analysis_") as tmp:
            out_dir = Path(tmp) / "analysis_output"
            out_dir.mkdir(parents=True, exist_ok=True)

            if analysis_mode == "Референтный (China/USA/Russia)":
                if referent_core is None:
                    st.error("Референтный модуль не найден. Добавьте media_analyzer_referent.py в корень проекта.")
                    return
                input_df = build_referent_input_df(file_items, int(min_year), int(max_year))
                if input_df.empty:
                    st.error("После фильтрации по годам не осталось документов для референтного анализа.")
                    return
                try:
                    stats = run_referent_analysis(input_df=input_df, out_dir=out_dir, evi_mode=referent_evi_mode)
                except Exception as e:
                    st.exception(e)
                    return

                st.success(f"Готово. Документов: {stats['docs']}, контекстов: {stats['contexts']}, flagged: {stats['flagged']}")
                st.subheader("Референтные результаты")
                for preview_name in [
                    "contexts_full.csv",
                    "aggregated_by_article.csv",
                    "aggregated_by_outlet.csv",
                    "aggregated_by_media_country_and_ref_country.csv",
                    "flagged_cases.csv",
                ]:
                    p = out_dir / preview_name
                    if p.exists():
                        st.markdown(f"**{preview_name}**")
                        st.dataframe(pd.read_csv(p).head(20), use_container_width=True)

                p_agg = out_dir / "aggregated_by_media_country_and_ref_country.csv"
                if p_agg.exists():
                    agg = pd.read_csv(p_agg)
                    c1, c2 = st.columns(2)
                    with c1:
                        fig = px.bar(
                            agg,
                            x="ref_country",
                            y="IP",
                            color="media_country",
                            barmode="group",
                            title="Average IP by media country and referent",
                            template="plotly_dark",
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    with c2:
                        fig2 = px.bar(
                            agg,
                            x="ref_country",
                            y="number_of_contexts",
                            color="media_country",
                            barmode="group",
                            title="Number of contexts",
                            template="plotly_dark",
                        )
                        st.plotly_chart(fig2, use_container_width=True)
            else:
                docs = build_docs(file_items, int(min_year), int(max_year), use_lemma=use_lemma)
                if not docs:
                    st.error("После предобработки не осталось документов в указанном диапазоне лет.")
                    return

                try:
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
            st.download_button(
                label="Скачать результаты анализа (ZIP)",
                data=out_zip,
                file_name="mediatext_analyzator_output.zip",
                mime="application/zip",
            )


if __name__ == "__main__":
    main()
