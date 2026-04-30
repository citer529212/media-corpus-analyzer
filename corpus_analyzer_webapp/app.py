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
APP_BUILD = "2026-04-20-10:35"


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
        st.markdown("**Персуазивный потенциал (PP_weighted) по годам**")
        pivot_pp = df.pivot(index="year", columns="country", values="avg_PP_weighted").fillna(0.0)
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

    rows = []
    for d in docs:
        toks = d.tokens
        if not toks:
            continue
        c = {}
        for t in toks:
            c[t] = c.get(t, 0) + 1
        W = len(toks)

        ideol = sum(c.get(t, 0) for t in ideology["ideol"])
        prec = sum(c.get(t, 0) for t in ideology["prec"])
        slog = sum(c.get(t, 0) for t in ideology["slog"])
        dich = sum(c.get(t, 0) for t in ideology["dich"])
        n_ideol = ideol + prec + slog + dich
        IDI = (n_ideol * 100.0) / W

        e_w = sum(c.get(t, 0) for t in emotion["weak"])
        e_m = sum(c.get(t, 0) for t in emotion["medium"])
        e_s = sum(c.get(t, 0) for t in emotion["strong"])
        EMI = ((1 * e_w + 2 * e_m + 3 * e_s) * 100.0) / W

        R = sum(c.get(t, 0) for t in evaluation["rational"])
        E = sum(c.get(t, 0) for t in evaluation["emotional"])
        Imp = sum(c.get(t, 0) for t in evaluation["implicit"])
        Exp = sum(c.get(t, 0) for t in evaluation["explicit"])
        n_eval = R + E + Imp + Exp
        EDI = (n_eval * 100.0) / W if n_eval else 0.0
        EII = ((1 * R + 3 * E) / n_eval) if n_eval else 0.0
        ELFI = ((1 * Imp + 3 * Exp) / n_eval) if n_eval else 0.0
        EVI = 0.5 * EDI + 0.25 * EII + 0.25 * ELFI

        M_w = sum(c.get(t, 0) for t in metaphor["weak"])
        M_m = sum(c.get(t, 0) for t in metaphor["medium"])
        M_s = sum(c.get(t, 0) for t in metaphor["strong"])
        n_met = M_w + M_m + M_s
        text_low = d.text.casefold()
        dir_hits = len(re.findall(r"\b(as|like|seperti|bagai|ibarat|laksana|как)\b", text_low))
        Dir = min(dir_hits, n_met)
        Ind = max(n_met - Dir, 0)
        MDI = (n_met * 100.0) / W if n_met else 0.0
        MII = ((1 * M_w + 2 * M_m + 3 * M_s) / n_met) if n_met else 0.0
        MLFI = ((1 * Ind + 3 * Dir) / n_met) if n_met else 0.0
        MTI = 0.5 * MDI + 0.25 * MII + 0.25 * MLFI

        IInorm = min(IDI / 8.0, 1.0)
        EInorm = min(EMI / 8.0, 1.0)
        EVInorm = min(EVI / 8.0, 1.0)
        MInorm = min(MTI / 4.0, 1.0)
        PP = 0.30 * IInorm + 0.25 * EInorm + 0.25 * EVInorm + 0.20 * MInorm

        rows.append(
            {
                "source": d.source,
                "country": d.primary_country,
                "year": d.year,
                "IDI": IDI,
                "EMI": EMI,
                "EVI": EVI,
                "MTI": MTI,
                "PP": PP,
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
        if v <= 0.20:
            return "очень низкий"
        if v <= 0.40:
            return "низкий"
        if v <= 0.60:
            return "средний"
        if v <= 0.80:
            return "высокий"
        return "очень высокий"

    st.subheader("5 обязательных индикаторов")
    avg = df[["IDI", "EMI", "EVI", "MTI", "PP"]].mean()

    indicator_specs = [
        {
            "code": "IDI",
            "label": "Идеологичность (IDI)",
            "value": float(avg["IDI"]),
            "bounds": (2.0, 4.0, 6.0, 8.0),
            "max_scale": 10.0,
            "about": "Показывает, насколько текст насыщен идеологическими рамками: «свои/чужие», суверенитет, ценностные формулы.",
            "meaning_low": "Низкий IDI: текст больше информирует, чем идеологически направляет.",
            "meaning_high": "Высокий IDI: текст заметно формирует «правильную» интерпретацию через ценностные маркеры.",
            "scale": "Шкала: <2 очень низкий, 2–4 низкий, 4–6 средний, 6–8 высокий, >8 очень высокий.",
        },
        {
            "code": "EMI",
            "label": "Эмоциональность (EMI)",
            "value": float(avg["EMI"]),
            "bounds": (2.0, 4.0, 6.0, 8.0),
            "max_scale": 10.0,
            "about": "Показывает силу эмоционального давления (страх, тревога, гордость, возмущение и т.п.).",
            "meaning_low": "Низкий EMI: текст подан более нейтрально и рационально.",
            "meaning_high": "Высокий EMI: текст активно воздействует на эмоции аудитории.",
            "scale": "Шкала: <2 очень низкий, 2–4 низкий, 4–6 средний, 6–8 высокий, >8 очень высокий.",
        },
        {
            "code": "EVI",
            "label": "Оценка объекта (EVI)",
            "value": float(avg["EVI"]),
            "bounds": (2.0, 3.0, 4.0, 5.0),
            "max_scale": 6.0,
            "about": "Показывает, насколько явно и интенсивно объект описывается как «хороший/плохой».",
            "meaning_low": "Низкий EVI: мало явных оценок, тон ближе к констатации.",
            "meaning_high": "Высокий EVI: оценки систематичны и задают нужное отношение к объекту.",
            "scale": "Шкала: <2 очень низкий, 2–3 низкий, 3–4 средний, 4–5 высокий, >5 очень высокий.",
        },
        {
            "code": "MTI",
            "label": "Метафоричность (MTI)",
            "value": float(avg["MTI"]),
            "bounds": (1.25, 2.0, 2.75, 3.5),
            "max_scale": 4.5,
            "about": "Показывает, насколько активно используются образные модели (метафоры) для объяснения политики.",
            "meaning_low": "Низкий MTI: текст говорит преимущественно буквально, без сильной образности.",
            "meaning_high": "Высокий MTI: метафоры заметно направляют восприятие и упрощают сложные темы.",
            "scale": "Шкала: <1.25 очень низкий, 1.25–2 низкий, 2–2.75 средний, 2.75–3.5 высокий, >3.5 очень высокий.",
        },
        {
            "code": "IP",
            "label": "Воздействующий потенциал (IP)",
            "value": float(avg["PP"]),
            "bounds": (0.20, 0.40, 0.60, 0.80),
            "max_scale": 1.0,
            "about": "Итоговый индекс воздействия текста: объединяет идеологичность, эмоции, оценочность и метафоричность.",
            "meaning_low": "Низкий IP: текст слабо влияет на установки читателя.",
            "meaning_high": "Высокий IP: текст вероятно формирует устойчивое отношение к теме/стране.",
            "scale": "Шкала: 0–0.20 очень низкий, 0.21–0.40 низкий, 0.41–0.60 средний, 0.61–0.80 высокий, 0.81–1.00 очень высокий.",
        },
    ]

    selected_spec = next((x for x in indicator_specs if x["code"] == selected_indicator), indicator_specs[0])
    value = selected_spec["value"]
    bounds = selected_spec["bounds"]
    level = pp_lvl(value) if selected_spec["code"] == "IP" else lvl(value, bounds)
    normalized = max(0.0, min(value / selected_spec["max_scale"], 1.0))
    st.markdown(f"### {selected_spec['label']}")
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
        v = float(df_doc.iloc[0]["PP" if selected_spec["code"] == "IP" else selected_spec["code"]])
        l = pp_lvl(v) if selected_spec["code"] == "IP" else lvl(v, selected_spec["bounds"])
        st.success(f"Текущий случай: `{selected_spec['code']}={v:.3f}` ({l} уровень).")

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
    else:
        st.warning("Для этого индикатора в выбранном тексте маркеры не найдены.")

    col_name = "PP" if selected_indicator == "IP" else selected_indicator
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
    st.caption(f"Индикаторная модель лингвопрагматического анализа персуазивного потенциала политического медиатекста. Build: {APP_BUILD}")

    with st.sidebar:
        st.header("Параметры")
        min_year = st.number_input("Минимальный год", min_value=2000, max_value=2100, value=2022)
        max_year = st.number_input("Максимальный год", min_value=2000, max_value=2100, value=2026)
        analysis_mode = st.selectbox(
            "Режим анализа",
            options=["Стандартный (5 индикаторов)", "Расширенный (корпусный)"],
            index=0,
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

        docs = build_docs(file_items, int(min_year), int(max_year), use_lemma=use_lemma)
        if not docs:
            st.error("После предобработки не осталось документов в указанном диапазоне лет.")
            return

        with tempfile.TemporaryDirectory(prefix="sea_media_analysis_") as tmp:
            out_dir = Path(tmp) / "analysis_output"
            out_dir.mkdir(parents=True, exist_ok=True)

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
