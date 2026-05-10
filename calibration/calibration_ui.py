from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None
from calibration.calibration_lexicon_expander import approve_candidate, reject_candidate


def render_sidebar_controls() -> Dict[str, object]:
    if st is None:
        return {}

    st.markdown("### Calibration corpus")
    files = st.file_uploader(
        "Upload local calibration files",
        type=["txt", "md", "docx", "pdf", "csv", "xlsx", "xls", "json"],
        accept_multiple_files=True,
        key="cal_files_upload",
    )
    meta_csv = st.file_uploader("Upload calibration metadata CSV", type=["csv"], key="cal_meta_csv")
    sources_yaml = st.file_uploader("Upload calibration_sources.yaml", type=["yaml", "yml"], key="cal_sources_yaml_upload")

    b_build = st.button("Build calibration corpus", key="cal_btn_build")
    b_recalc = st.button("Recalculate distributions", key="cal_btn_recalc")
    b_extract = st.button("Extract dictionary candidates", key="cal_btn_extract")
    b_reload = st.button("Reload verified lexicons", key="cal_btn_reload")
    baseline = st.selectbox(
        "Select baseline",
        options=[
            "full_calibration_corpus",
            "neutral_news_only",
            "political_news_only",
            "same_language_only",
            "same_ref_country_only",
            "custom",
        ],
        index=0,
        key="cal_baseline_mode",
    )
    interpretation_mode = st.selectbox(
        "Interpretation mode",
        options=["use_empirical_percentiles", "use_calibration_baseline", "use_theoretical_interpretation"],
        index=0,
        key="cal_interpret_mode",
    )

    use_pct = st.toggle("Use calibration percentiles", value=True, key="cal_use_percentiles")
    show_candidates = st.toggle("Show dictionary candidates", value=True, key="cal_show_candidates")
    show_flags = st.toggle("Show quality flags", value=True, key="cal_show_flags")

    return {
        "files_upload": files,
        "meta_csv_upload": meta_csv,
        "sources_yaml_upload": sources_yaml,
        "build_btn": b_build,
        "recalc_btn": b_recalc,
        "extract_btn": b_extract,
        "reload_btn": b_reload,
        "baseline": baseline,
        "interpretation_mode": interpretation_mode,
        "use_percentiles": use_pct,
        "show_candidates": show_candidates,
        "show_flags": show_flags,
    }


def render_main_tabs(calibration_dir: Path, lexicons_dir: Path | None = None) -> None:
    if st is None:
        return
    st.markdown("## Calibration and Dictionary Expansion")

    tabs = st.tabs([
        "1. Calibration overview",
        "2. Texts",
        "3. Contexts",
        "4. Distributions",
        "5. Percentiles",
        "6. Dictionary candidates",
        "7. Verified lexicons",
        "8. Quality flags",
        "9. Report",
    ])

    p_texts = calibration_dir / "calibration_texts.csv"
    p_ctx = calibration_dir / "calibration_contexts.csv"
    p_dist = calibration_dir / "calibration_distributions.csv"
    p_cand = calibration_dir / "candidate_terms.csv"
    p_ver = calibration_dir / "verified_terms.csv"
    p_flags = calibration_dir / "calibration_quality_flags.csv"
    p_report = calibration_dir / "calibration_report.md"

    texts = pd.read_csv(p_texts) if p_texts.exists() else pd.DataFrame()
    ctx = pd.read_csv(p_ctx) if p_ctx.exists() else pd.DataFrame()
    dist = pd.read_csv(p_dist) if p_dist.exists() else pd.DataFrame()
    cand = pd.read_csv(p_cand) if p_cand.exists() else pd.DataFrame()
    ver = pd.read_csv(p_ver) if p_ver.exists() else pd.DataFrame()
    flags = pd.read_csv(p_flags) if p_flags.exists() else pd.DataFrame()

    with tabs[0]:
        c1, c2, c3 = st.columns(3)
        c1.metric("texts", int(len(texts)))
        c2.metric("contexts", int(len(ctx)))
        c3.metric("candidates", int(len(cand)))
        if "calibration_type" in texts.columns:
            st.bar_chart(texts["calibration_type"].value_counts())

    with tabs[1]:
        st.dataframe(texts, use_container_width=True)

    with tabs[2]:
        st.dataframe(ctx, use_container_width=True)

    with tabs[3]:
        st.dataframe(dist, use_container_width=True)
        for metric in ["IDI_raw", "EMI_raw", "MTI_raw", "IP_abs_context"]:
            if metric in ctx.columns:
                with st.expander(f"histogram {metric}", expanded=False):
                    st.bar_chart(ctx[metric])

    with tabs[4]:
        if not dist.empty:
            show = dist[[c for c in ["group_scope", "group_value", "metric", "p75", "p90", "p95", "p99"] if c in dist.columns]]
            st.dataframe(show, use_container_width=True)

    with tabs[5]:
        st.dataframe(cand, use_container_width=True)
        if lexicons_dir is not None and not cand.empty:
            st.markdown("#### Manual candidate review")
            term = st.selectbox("Candidate term", options=sorted(set(cand["candidate_term"].astype(str).tolist())), key="cand_term_pick")
            dict_opts = sorted(set(cand[cand["candidate_term"].astype(str) == term]["proposed_dictionary"].astype(str).tolist()))
            dictionary = st.selectbox("Target dictionary", options=dict_opts, key="cand_dict_pick")
            note = st.text_input("Note", value="", key="cand_note")
            c1, c2 = st.columns(2)
            if c1.button("Approve", key="cand_approve"):
                approve_candidate(lexicons_dir, term, dictionary, reason="streamlit_approve", note=note)
                st.success("Approved and logged.")
            if c2.button("Reject", key="cand_reject"):
                reject_candidate(lexicons_dir, term, dictionary, reason="streamlit_reject", note=note)
                st.warning("Rejected and logged.")

    with tabs[6]:
        st.dataframe(ver, use_container_width=True)

    with tabs[7]:
        st.dataframe(flags, use_container_width=True)

    with tabs[8]:
        if p_report.exists():
            st.markdown(p_report.read_text(encoding="utf-8"))
        else:
            st.info("Report not found yet")
