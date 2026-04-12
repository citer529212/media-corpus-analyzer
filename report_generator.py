#!/usr/bin/env python3
"""Generate dissertation-ready markdown report from strict stage analysis outputs."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def as_int(v: str) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0


def as_float(v: str) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def md_table(headers: List[str], rows: List[List[str]]) -> str:
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("|" + "|".join(["---"] * len(headers)) + "|")
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)


def pct(part: int, total: int) -> str:
    if total <= 0:
        return "0.00%"
    return f"{(part / total) * 100:.2f}%"


def top_keywords_by_country(rows: List[Dict[str, str]], per_country: int = 12) -> Dict[str, List[Tuple[str, int, float]]]:
    out: Dict[str, List[Tuple[str, int, float]]] = defaultdict(list)
    for r in rows:
        c = r.get("country", "")
        tok = r.get("token", "")
        freq = as_int(r.get("sub_freq", "0"))
        g2 = as_float(r.get("llr_g2", "0"))
        out[c].append((tok, freq, g2))
    for c in list(out.keys()):
        out[c].sort(key=lambda x: x[2], reverse=True)
        out[c] = out[c][:per_country]
    return out


def aggregate_sentiment(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, float]]:
    agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"docs": 0.0, "weighted_sent": 0.0, "pos": 0.0, "neg": 0.0, "neu": 0.0})
    for r in rows:
        c = r.get("country", "")
        docs = as_int(r.get("doc_count", "0"))
        avg = as_float(r.get("avg_sent_score", "0"))
        agg[c]["docs"] += docs
        agg[c]["weighted_sent"] += avg * docs
        agg[c]["pos"] += as_int(r.get("positive_docs", "0"))
        agg[c]["neg"] += as_int(r.get("negative_docs", "0"))
        agg[c]["neu"] += as_int(r.get("neutral_docs", "0"))
    for c in list(agg.keys()):
        docs = agg[c]["docs"]
        agg[c]["avg_sent"] = (agg[c]["weighted_sent"] / docs) if docs else 0.0
    return agg


def aggregate_frames(rows: List[Dict[str, str]]) -> Dict[str, List[Tuple[str, int]]]:
    acc: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in rows:
        c = r.get("country", "")
        frame = r.get("frame", "")
        n = as_int(r.get("count", "0"))
        acc[c][frame] += n
    out: Dict[str, List[Tuple[str, int]]] = {}
    for c, d in acc.items():
        arr = sorted(d.items(), key=lambda kv: kv[1], reverse=True)
        out[c] = arr
    return out


def aggregate_persuasion(rows: List[Dict[str, str]]) -> Dict[str, List[Tuple[str, int]]]:
    # aggregate by country only for concise report
    acc: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in rows:
        c = r.get("country", "")
        marker = r.get("marker_group", "")
        n = as_int(r.get("count", "0"))
        acc[c][marker] += n
    out: Dict[str, List[Tuple[str, int]]] = {}
    for c, d in acc.items():
        out[c] = sorted(d.items(), key=lambda kv: kv[1], reverse=True)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Generate markdown report from strict analysis stage CSVs")
    p.add_argument("--analysis-dir", default="output_country_discourse_raw_analysis_strict")
    p.add_argument("--output", default="output_country_discourse_raw_analysis_strict/report_strict_method.md")
    args = p.parse_args()

    analysis_dir = Path(args.analysis_dir)
    out_path = Path(args.output)

    s1_source = read_csv(analysis_dir / "stage1_profile_source.csv")
    s1_country = read_csv(analysis_dir / "stage1_profile_country.csv")
    s1_year = read_csv(analysis_dir / "stage1_profile_year.csv")
    s1_lang = []
    s1_dedup = []
    p_lang = analysis_dir / "stage1_profile_language.csv"
    p_dedup = analysis_dir / "stage1_dedup_stats.csv"
    if p_lang.exists():
        s1_lang = read_csv(p_lang)
    if p_dedup.exists():
        s1_dedup = read_csv(p_dedup)

    s2_keywords = read_csv(analysis_dir / "stage2_keywords.csv")
    s2_colloc = read_csv(analysis_dir / "stage2_collocations.csv")

    s3_sent = read_csv(analysis_dir / "stage3_sentiment_summary_country_year.csv")
    s3_frames = read_csv(analysis_dir / "stage3_frame_summary_country_year.csv")
    s3_persu = read_csv(analysis_dir / "stage3_persuasion_markers_summary.csv")

    s4_prog = read_csv(analysis_dir / "stage4_prognostic_trends.csv")
    s6_sig = []
    s6_logodds = []
    p6a = analysis_dir / "stage6_significance_pairwise.csv"
    p6b = analysis_dir / "stage6_logodds_tokens.csv"
    if p6a.exists():
        s6_sig = read_csv(p6a)
    if p6b.exists():
        s6_logodds = read_csv(p6b)
    s5_country_total = []
    s5_country_year = []
    s5_source_country = []
    p5a = analysis_dir / "stage5_representativeness_country_total.csv"
    p5b = analysis_dir / "stage5_representativeness_country_year.csv"
    p5c = analysis_dir / "stage5_source_country_balance.csv"
    if p5a.exists():
        s5_country_total = read_csv(p5a)
    if p5b.exists():
        s5_country_year = read_csv(p5b)
    if p5c.exists():
        s5_source_country = read_csv(p5c)

    total_docs = sum(as_int(r.get("doc_count", "0")) for r in s1_source)

    lines: List[str] = []
    lines.append("# Отчет по корпусному анализу (строгая поэтапная методика)")
    lines.append("")
    lines.append(f"Анализировано документов: **{total_docs}**")
    lines.append("")
    lines.append("## Этап 1. Профиль корпуса")

    # Source table
    source_rows = []
    for r in sorted(s1_source, key=lambda x: as_int(x.get("doc_count", "0")), reverse=True):
        n = as_int(r.get("doc_count", "0"))
        source_rows.append([r.get("source", ""), str(n), pct(n, total_docs)])
    lines.append(md_table(["Источник", "Документов", "Доля"], source_rows))
    lines.append("")

    country_rows = []
    for r in sorted(s1_country, key=lambda x: as_int(x.get("doc_count", "0")), reverse=True):
        n = as_int(r.get("doc_count", "0"))
        country_rows.append([r.get("country", ""), str(n), pct(n, total_docs)])
    lines.append(md_table(["Страна (primary_country)", "Документов", "Доля"], country_rows))
    lines.append("")

    year_rows = []
    for r in sorted(s1_year, key=lambda x: as_int(x.get("year", "0"))):
        n = as_int(r.get("doc_count", "0"))
        year_rows.append([r.get("year", ""), str(n), pct(n, total_docs)])
    lines.append(md_table(["Год", "Документов", "Доля"], year_rows))
    lines.append("")
    if s1_lang:
        lang_rows = []
        for r in sorted(s1_lang, key=lambda x: as_int(x.get("doc_count", "0")), reverse=True):
            n = as_int(r.get("doc_count", "0"))
            lang_rows.append([r.get("language", ""), str(n), pct(n, total_docs)])
        lines.append(md_table(["Язык", "Документов", "Доля"], lang_rows))
        lines.append("")
    if s1_dedup:
        dedup_rows = [[r.get("metric", ""), r.get("value", "")] for r in s1_dedup]
        lines.append(md_table(["Dedup metric", "Value"], dedup_rows))
        lines.append("")

    lines.append("## Этап 2. Количественный корпусный анализ")

    kw = top_keywords_by_country(s2_keywords, per_country=10)
    for country in sorted(kw.keys()):
        lines.append(f"### Ключевые слова: {country}")
        rows = [[tok, str(freq), f"{g2:.2f}"] for tok, freq, g2 in kw[country]]
        lines.append(md_table(["Токен", "Частота в субкорпусе", "LLR G2"], rows))
        lines.append("")

    coll_rows = []
    for r in s2_colloc[:25]:
        coll_rows.append([
            r.get("anchor_country", ""),
            r.get("collocate", ""),
            r.get("cooc_freq", ""),
            r.get("mi", ""),
            r.get("t_score", ""),
            r.get("llr_g2", ""),
        ])
    lines.append("### Топ коллокаций (первые 25 строк)")
    lines.append(md_table(["Якорь", "Коллокат", "Cooc", "MI", "t-score", "LLR"], coll_rows))
    lines.append("")

    lines.append("## Этап 3. Качественный лингвопрагматический слой")

    sent = aggregate_sentiment(s3_sent)
    sent_rows = []
    for c in sorted(sent.keys()):
        d = sent[c]
        docs = int(d["docs"])
        sent_rows.append([
            c,
            str(docs),
            f"{d['avg_sent']:.6f}",
            str(int(d["pos"])),
            str(int(d["neg"])),
            str(int(d["neu"])),
        ])
    lines.append("### Сводка тональности по странам")
    lines.append(md_table(["Страна", "Документов", "Средний sent_score", "Positive", "Negative", "Neutral"], sent_rows))
    lines.append("")

    frames = aggregate_frames(s3_frames)
    lines.append("### Доминирующие фреймы по странам (топ-5)")
    for c in sorted(frames.keys()):
        rows = [[fr, str(n)] for fr, n in frames[c][:5]]
        lines.append(f"#### {c}")
        lines.append(md_table(["Фрейм", "Count"], rows))
        lines.append("")

    persu = aggregate_persuasion(s3_persu)
    lines.append("### Маркеры персуазии по странам")
    for c in sorted(persu.keys()):
        rows = [[m, str(n)] for m, n in persu[c]]
        lines.append(f"#### {c}")
        lines.append(md_table(["Группа маркеров", "Count"], rows))
        lines.append("")

    lines.append("## Этап 4. Прогностический уровень")
    prog_rows = []
    for r in sorted(s4_prog, key=lambda x: x.get("country", "")):
        prog_rows.append([
            r.get("country", ""),
            r.get("volume_slope_per_year", ""),
            r.get("sentiment_slope_per_year", ""),
            f"{r.get('from_year', '')}-{r.get('to_year', '')}",
        ])
    lines.append(md_table(["Страна", "Наклон объема/год", "Наклон тональности/год", "Интервал"], prog_rows))
    lines.append("")

    lines.append("## Интерпретационные выводы")
    lines.append("1. Объемная репрезентативность неоднородна по источникам; это требует двойного чтения результатов (raw volume и нормированные сравнения).")
    lines.append("2. Лексико-коллокационные профили субкорпусов по странам статистически различимы (LLR/MI/t-score).")
    lines.append("3. Фреймовые и персуазивные паттерны могут использоваться как операциональные признаки имиджа страны в медиа.")
    lines.append("4. Прогностические наклоны дают индикативную динамику, но требуют верификации на расширенном временном окне и ручной экспертной разметке.")
    lines.append("")

    if s5_country_total:
        lines.append("## Этап 5. Индикаторы репрезентативности")
        rows = []
        for r in sorted(s5_country_total, key=lambda x: x.get("country", "")):
            rows.append([
                r.get("country", ""),
                r.get("doc_count", ""),
                r.get("active_sources", ""),
                r.get("hhi_concentration", ""),
                r.get("shannon_entropy", ""),
                r.get("effective_sources", ""),
            ])
        lines.append(md_table(["Страна", "Документов", "Активных источников", "HHI", "Shannon H", "Effective Sources"], rows))
        lines.append("")

    if s5_country_year:
        lines.append("### Динамика концентрации по годам (country-year)")
        rows = []
        for r in sorted(s5_country_year, key=lambda x: (x.get("country", ""), as_int(x.get("year", "0"))))[:30]:
            rows.append([
                r.get("country", ""),
                r.get("year", ""),
                r.get("doc_count", ""),
                r.get("coverage_ratio", ""),
                r.get("hhi_concentration", ""),
                r.get("effective_sources", ""),
                r.get("max_source_share", ""),
            ])
        lines.append(md_table(["Страна", "Год", "Док-тов", "Coverage", "HHI", "Eff.Sources", "Max source share"], rows))
        lines.append("")

    if s5_source_country:
        lines.append("### Баланс стран внутри источников (первые 24 строки)")
        rows = []
        for r in sorted(s5_source_country, key=lambda x: (x.get("source", ""), -as_int(x.get("doc_count", "0"))))[:24]:
            rows.append([
                r.get("source", ""),
                r.get("country", ""),
                r.get("doc_count", ""),
                r.get("share_within_source", ""),
                r.get("share_within_country", ""),
            ])
        lines.append(md_table(["Источник", "Страна", "Док-тов", "Доля в источнике", "Доля в стране"], rows))
        lines.append("")

    if s6_sig:
        lines.append("## Этап 6. Тесты статистической значимости")
        sig_rows = []
        sig_interp = []
        for r in s6_sig:
            pval = as_float(r.get("p_value", "1"))
            sig = "yes" if pval < 0.05 else "no"
            sig_rows.append([
                r.get("country_a", ""),
                r.get("country_b", ""),
                r.get("test_target", ""),
                r.get("chi2", ""),
                r.get("df", ""),
                r.get("p_value", ""),
                r.get("cramers_v", ""),
                r.get("effect_size_label", ""),
                sig,
            ])
            sig_interp.append((r.get("country_a", ""), r.get("country_b", ""), r.get("test_target", ""), pval, r.get("effect_size_label", "")))
        lines.append(md_table(["Country A", "Country B", "Target", "Chi2", "df", "p-value", "Cramer's V", "Effect", "p<0.05"], sig_rows))
        lines.append("")
        lines.append("### Краткая интерпретация значимости")
        for a, b, tgt, pval, eff in sig_interp:
            mark = "значимо" if pval < 0.05 else "не значимо"
            lines.append(f"- {a} vs {b}, {tgt}: {mark}; сила эффекта: {eff}.")
        lines.append("")

    if s6_logodds:
        lines.append("### Лексические контрасты (log-odds z-score, первые 36)")
        rows = []
        for r in s6_logodds[:36]:
            rows.append([
                r.get("country_a", ""),
                r.get("country_b", ""),
                r.get("token", ""),
                r.get("z_score", ""),
                r.get("favored_country", ""),
            ])
        lines.append(md_table(["A", "B", "Token", "z", "Favored"], rows))
        lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"Report generated: {out_path.resolve()}")


if __name__ == "__main__":
    main()
