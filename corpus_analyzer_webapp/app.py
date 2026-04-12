#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import io
import re
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import List, Tuple

import streamlit as st

# Reuse your strict analyzer core
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import corpus_analysis_strict_method as core


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


def read_zip_txt_files(zip_bytes: bytes) -> List[Tuple[str, str]]:
    out = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = info.filename
            if not name.casefold().endswith((".txt", ".md", ".text")):
                continue
            raw = zf.read(info).decode("utf-8", errors="ignore")
            out.append((name, raw))
    return out


def read_uploaded_txt_files(files) -> List[Tuple[str, str]]:
    out = []
    for f in files or []:
        name = f.name
        raw = f.getvalue().decode("utf-8", errors="ignore")
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

        source = guess_source(filename)
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

    return dedup_stats, len(docs)


def main() -> None:
    st.set_page_config(page_title="SEA Media Corpus Analyzer", layout="wide")
    st.title("SEA Media Corpus Analyzer")
    st.caption("Загрузите ZIP с корпусом или отдельные тексты. На выходе получите полный пакет CSV-результатов анализа.")

    with st.sidebar:
        st.header("Параметры")
        min_year = st.number_input("Минимальный год", min_value=2000, max_value=2100, value=2022)
        max_year = st.number_input("Максимальный год", min_value=2000, max_value=2100, value=2026)
        use_lemma = st.checkbox("Лемматизация (легкая)", value=True)
        dedup = st.checkbox("Dedup (exact + near)", value=True)
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
        txt_uploads = st.file_uploader("Или отдельные тексты (.txt/.md)", type=["txt", "md", "text"], accept_multiple_files=True)

    run_btn = st.button("Запустить анализ", type="primary")

    if run_btn:
        file_items = []
        if zip_upload is not None:
            file_items.extend(read_zip_txt_files(zip_upload.getvalue()))
        file_items.extend(read_uploaded_txt_files(txt_uploads))

        # deduplicate same filename+content across inputs
        uniq = {}
        for n, t in file_items:
            content_md5 = hashlib.md5(t.encode("utf-8", errors="ignore")).hexdigest()
            fingerprint = f"{n}|{len(t)}|{content_md5}"
            uniq[fingerprint] = (n, t)
        file_items = list(uniq.values())

        if not file_items:
            st.error("Не найдено входных текстов. Загрузите ZIP или отдельные .txt/.md файлы.")
            return

        docs = build_docs(file_items, int(min_year), int(max_year), use_lemma=use_lemma)
        if not docs:
            st.error("После предобработки не осталось документов в указанном диапазоне лет.")
            return

        with tempfile.TemporaryDirectory(prefix="sea_media_analysis_") as tmp:
            out_dir = Path(tmp) / "analysis_output"
            out_dir.mkdir(parents=True, exist_ok=True)

            try:
                dedup_stats, analyzed_docs = run_analysis(
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
            st.json(dedup_stats)

            # Quick preview tables
            for preview_name in [
                "stage1_profile_source.csv",
                "stage1_profile_country.csv",
                "stage1_profile_year.csv",
                "stage1_profile_language.csv",
                "stage5_representativeness_country_total.csv",
                "stage6_significance_pairwise.csv",
            ]:
                p = out_dir / preview_name
                if p.exists():
                    st.subheader(preview_name)
                    rows = read_csv_preview(p, limit=15)
                    st.dataframe(rows)

            out_zip = zip_dir_bytes(out_dir)
            st.download_button(
                label="Скачать результаты анализа (ZIP)",
                data=out_zip,
                file_name="sea_media_analysis_output.zip",
                mime="application/zip",
            )


if __name__ == "__main__":
    main()
