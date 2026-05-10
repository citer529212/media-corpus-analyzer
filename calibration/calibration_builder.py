from __future__ import annotations

import io
import json
import re
import time
import urllib.parse
import urllib.request
import urllib.robotparser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

import media_analyzer_referent as referent_core
from calibration.calibration_export import export_all
from calibration.calibration_interpreter import interpret_indicator
from calibration.calibration_lexicon_expander import extract_candidate_terms, ensure_lexicon_files
from calibration.calibration_metrics import add_percentiles, build_distributions
from calibration.calibration_quality import build_quality_flags
from calibration.calibration_schema import CalibrationRunArtifacts, CalibrationSource

try:
    from docx import Document
except Exception:  # pragma: no cover
    Document = None

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover
    PdfReader = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decode(data: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


def _read_table_bytes(name: str, data: bytes) -> pd.DataFrame:
    ext = Path(name).suffix.lower()
    if ext == ".csv":
        try:
            return pd.read_csv(io.BytesIO(data))
        except Exception:
            return pd.read_csv(io.BytesIO(data), sep=None, engine="python")
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(io.BytesIO(data))
    if ext == ".json":
        obj = json.loads(_decode(data))
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        if isinstance(obj, dict):
            return pd.DataFrame(obj.get("rows", obj))
    return pd.DataFrame()


def _parse_sources_yaml(path: Path) -> List[CalibrationSource]:
    raw = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(raw) or {}
    except Exception:
        # simple fallback parser
        data = {"sources": []}
        cur: Dict[str, str] | None = None
        for line in raw.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s == "sources:":
                continue
            if s.startswith("- "):
                if cur:
                    data["sources"].append(cur)
                cur = {}
                s = s[2:].strip()
                if ":" in s:
                    k, v = s.split(":", 1)
                    cur[k.strip()] = v.strip().strip("'\"")
                continue
            if cur is not None and ":" in s:
                k, v = s.split(":", 1)
                cur[k.strip()] = v.strip().strip("'\"")
        if cur:
            data["sources"].append(cur)
    return [CalibrationSource.from_dict(x) for x in data.get("sources", [])]


class CalibrationBuilder:
    def __init__(
        self,
        dict_dir: Path,
        lexicons_dir: Path,
        user_agent: str = "CalibrationBuilder/1.0 (+research)",
        default_delay: float = 1.0,
    ) -> None:
        self.dict_dir = Path(dict_dir)
        self.lexicons_dir = Path(lexicons_dir)
        self.user_agent = user_agent
        self.default_delay = max(0.2, float(default_delay))

        referent_core.ensure_default_dictionaries(self.dict_dir)
        ensure_lexicon_files(self.lexicons_dir)

    @staticmethod
    def load_sources(path: Path) -> List[CalibrationSource]:
        return _parse_sources_yaml(path)

    def _robots_allowed(self, url: str) -> bool:
        parts = urllib.parse.urlparse(url)
        robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
        rp = urllib.robotparser.RobotFileParser()
        try:
            rp.set_url(robots_url)
            rp.read()
            return bool(rp.can_fetch(self.user_agent, url))
        except Exception:
            return False

    def _fetch_url(self, url: str, delay: float) -> Tuple[str, str, str]:
        if not self._robots_allowed(url):
            return "fetch_failed", "", "blocked_by_robots"

        req = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
                ctype = str(resp.headers.get("Content-Type", ""))
        except Exception as e:  # pragma: no cover
            return "fetch_failed", "", f"network_error:{type(e).__name__}"
        finally:
            time.sleep(delay)

        text = _decode(raw)
        low = text.lower()
        if any(x in low for x in ["subscribe", "paywall", "premium"]):
            return "fetch_failed", "", "paywall_detected"
        if "html" in ctype.lower() or "<html" in low:
            text = re.sub(r"<script.*?>.*?</script>", " ", text, flags=re.IGNORECASE | re.DOTALL)
            text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
            paras = re.findall(r"<p[^>]*>(.*?)</p>", text, flags=re.IGNORECASE | re.DOTALL)
            text = "\n".join(paras) if paras else re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
        return "ok", text, ""

    def _load_local_items(self, src: CalibrationSource, base_dir: Path) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not src.path:
            return rows
        p = Path(src.path)
        if not p.is_absolute():
            p = (base_dir / p).resolve()
        files = [p] if p.is_file() else sorted([x for x in p.rglob("*") if x.is_file()])
        src_tag = re.sub(r"[^a-z0-9]+", "_", src.source_name.lower()).strip("_") or "source"

        for i, fp in enumerate(files, start=1):
            ext = fp.suffix.lower()
            texts: List[Tuple[str, str]] = []
            if ext in {".txt", ".md", ".text"}:
                texts = [(fp.stem, fp.read_text(encoding="utf-8", errors="ignore"))]
            elif ext == ".docx" and Document is not None:
                doc = Document(str(fp))
                texts = [(fp.stem, "\n".join(p.text for p in doc.paragraphs if p.text))]
            elif ext == ".pdf" and PdfReader is not None:
                pdf = PdfReader(str(fp))
                texts = [(fp.stem, "\n".join((pg.extract_text() or "") for pg in pdf.pages))]
            elif ext in {".csv", ".xlsx", ".xls", ".json"}:
                df = _read_table_bytes(fp.name, fp.read_bytes()).fillna("")
                if not df.empty:
                    text_col = None
                    for c in df.columns:
                        if str(c).lower() in {"text", "content", "body"}:
                            text_col = c
                            break
                    if text_col is not None:
                        for j, r in df.iterrows():
                            txt = str(r.get(text_col, ""))
                            if txt.strip():
                                texts.append((f"{fp.stem}_{j+1}", txt))
            for title, txt in texts:
                txt = str(txt).strip()
                if not txt:
                    continue
                rid = len(rows) + 1
                rows.append(
                    {
                        "calibration_id": f"cal_local_{src_tag}_{i:06d}_{rid:06d}",
                        "doc_id": f"cal_doc_{src_tag}_{i:06d}_{rid:06d}",
                        "source_name": src.source_name,
                        "source_url": "",
                        "fetch_mode": "local",
                        "fetch_status": "ok",
                        "publication_date": "",
                        "collected_at": _now(),
                        "title": title,
                        "author": "",
                        "outlet": src.source_name,
                        "language": src.language,
                        "country_context": "",
                        "calibration_type": src.calibration_type,
                        "expected_indicator_focus": src.expected_indicator_focus,
                        "ref_country": "",
                        "text": txt,
                        "text_length_words": len(txt.split()),
                        "notes": src.notes,
                        "anchor_status": "none",
                        "quality_flags": "",
                    }
                )
        return rows

    def _load_url_items(self, src: CalibrationSource, base_dir: Path) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not src.url_csv_path:
            return rows
        p = Path(src.url_csv_path)
        if not p.is_absolute():
            p = (base_dir / p).resolve()
        df = pd.read_csv(p).fillna("")
        required = ["url", "source_name", "calibration_type", "expected_indicator_focus", "language", "ref_country", "notes"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"URL CSV missing columns: {missing}")
        for i, r in df.iterrows():
            url = str(r.get("url", "")).strip()
            if not url:
                continue
            status, text, reason = self._fetch_url(url, src.rate_limit_seconds)
            rows.append(
                {
                    "calibration_id": f"cal_url_{i+1:06d}",
                    "doc_id": f"cal_doc_url_{i+1:06d}",
                    "source_name": str(r.get("source_name", src.source_name)),
                    "source_url": url,
                    "fetch_mode": "url_list",
                    "fetch_status": status,
                    "publication_date": "",
                    "collected_at": _now(),
                    "title": "",
                    "author": "",
                    "outlet": str(r.get("source_name", src.source_name)),
                    "language": str(r.get("language", src.language)),
                    "country_context": "",
                    "calibration_type": str(r.get("calibration_type", src.calibration_type)),
                    "expected_indicator_focus": str(r.get("expected_indicator_focus", src.expected_indicator_focus)),
                    "ref_country": str(r.get("ref_country", "")),
                    "text": text,
                    "text_length_words": len(text.split()),
                    "notes": str(r.get("notes", reason)),
                    "anchor_status": "none",
                    "quality_flags": "",
                }
            )
        return rows

    def _load_rss_items(self, src: CalibrationSource) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not src.rss_url:
            return rows
        req = urllib.request.Request(src.rss_url, headers={"User-Agent": self.user_agent})
        with urllib.request.urlopen(req, timeout=20) as resp:
            xml = _decode(resp.read())
        time.sleep(src.rate_limit_seconds)

        for i, chunk in enumerate(re.findall(r"<item>(.*?)</item>", xml, flags=re.DOTALL | re.IGNORECASE), start=1):
            title_m = re.search(r"<title>(.*?)</title>", chunk, flags=re.DOTALL | re.IGNORECASE)
            link_m = re.search(r"<link>(.*?)</link>", chunk, flags=re.DOTALL | re.IGNORECASE)
            date_m = re.search(r"<pubDate>(.*?)</pubDate>", chunk, flags=re.DOTALL | re.IGNORECASE)
            url = (link_m.group(1).strip() if link_m else "")
            status, text, reason = self._fetch_url(url, src.rate_limit_seconds)
            rows.append(
                {
                    "calibration_id": f"cal_rss_{i:06d}",
                    "doc_id": f"cal_doc_rss_{i:06d}",
                    "source_name": src.source_name,
                    "source_url": url,
                    "fetch_mode": "rss",
                    "fetch_status": status,
                    "publication_date": date_m.group(1).strip() if date_m else "",
                    "collected_at": _now(),
                    "title": (title_m.group(1).strip() if title_m else ""),
                    "author": "",
                    "outlet": src.source_name,
                    "language": src.language,
                    "country_context": "",
                    "calibration_type": src.calibration_type,
                    "expected_indicator_focus": src.expected_indicator_focus,
                    "ref_country": "",
                    "text": text,
                    "text_length_words": len(text.split()),
                    "notes": reason,
                    "anchor_status": "none",
                    "quality_flags": "",
                }
            )
        return rows

    @staticmethod
    def _manual_items(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for i, r in enumerate(rows, start=1):
            txt = str(r.get("text", "")).strip()
            if not txt:
                continue
            out.append(
                {
                    "calibration_id": str(r.get("calibration_id", f"cal_manual_{i:06d}")),
                    "doc_id": f"cal_doc_manual_{i:06d}",
                    "source_name": str(r.get("source_name", "manual")),
                    "source_url": str(r.get("source_url", "")),
                    "fetch_mode": "manual",
                    "fetch_status": "ok",
                    "publication_date": str(r.get("publication_date", "")),
                    "collected_at": _now(),
                    "title": str(r.get("title", f"manual_{i}")),
                    "author": str(r.get("author", "")),
                    "outlet": str(r.get("outlet", "manual")),
                    "language": str(r.get("language", "en")),
                    "country_context": str(r.get("country_context", "")),
                    "calibration_type": str(r.get("calibration_type", "standard_political_news")),
                    "expected_indicator_focus": str(r.get("expected_indicator_focus", "mixed")),
                    "ref_country": str(r.get("ref_country", "")),
                    "text": txt,
                    "text_length_words": len(txt.split()),
                    "notes": str(r.get("notes", "")),
                    "anchor_status": str(r.get("anchor_status", "none")),
                    "quality_flags": "",
                }
            )
        return out

    def collect_texts(
        self,
        sources: List[CalibrationSource],
        base_dir: Path,
        manual_rows: Optional[Iterable[Dict[str, Any]]] = None,
    ) -> pd.DataFrame:
        all_rows: List[Dict[str, Any]] = []
        for src in sources:
            if not src.enabled:
                continue
            if src.mode == "local":
                all_rows.extend(self._load_local_items(src, base_dir))
            elif src.mode == "url_list":
                all_rows.extend(self._load_url_items(src, base_dir))
            elif src.mode == "rss":
                all_rows.extend(self._load_rss_items(src))
        if manual_rows:
            all_rows.extend(self._manual_items(manual_rows))
        if not all_rows:
            return pd.DataFrame(columns=[
                "calibration_id","source_name","source_url","fetch_mode","fetch_status","publication_date","collected_at","title","author","outlet","language","country_context","calibration_type","expected_indicator_focus","ref_country","text","text_length_words","notes","anchor_status","quality_flags",
            ])
        df = pd.DataFrame(all_rows).fillna("")
        # dedup by id and content
        df = df.drop_duplicates(subset=["calibration_id", "text"], keep="first").reset_index(drop=True)
        return df

    def build_contexts(self, texts_df: pd.DataFrame) -> pd.DataFrame:
        if texts_df.empty:
            return pd.DataFrame()

        docs = pd.DataFrame(
            {
                "doc_id": texts_df["doc_id"],
                "media_country": texts_df.get("country_context", ""),
                "outlet_name": texts_df.get("outlet", texts_df.get("source_name", "")),
                "date": texts_df.get("publication_date", ""),
                "title": texts_df.get("title", ""),
                "text": texts_df.get("text", ""),
                "language": texts_df.get("language", ""),
            }
        )
        docs = referent_core.ensure_required_fields(docs)

        ref_keywords = referent_core.load_ref_keywords(self.dict_dir)
        ref_patterns = referent_core.compile_keyword_patterns(ref_keywords)
        contexts = referent_core.extract_context_rows(docs, ref_patterns)

        # fallback: if no contexts, keep empty with ids for diagnostics
        if contexts.empty:
            return pd.DataFrame(columns=[
                "context_id","calibration_id","source_name","title","calibration_type","expected_indicator_focus","ref_country","matched_keywords","context_text","previous_sentence","target_sentence","next_sentence","N_content","IDI_raw","IDI_percent","EMI_raw","EMI_percent","MTI_raw","MTI_percent","EVI_raw","EVI_norm","S_r","salience_label","IP_context","IP_abs_context","quality_flags",
            ])

        scored = referent_core.apply_metrics(
            contexts=contexts,
            dict_dir=self.dict_dir,
            evi_mode="fine",
            evi_manual_path=None,
            metaphor_review_path=None,
        )

        meta_cols = [
            "doc_id", "calibration_id", "source_name", "title", "calibration_type", "expected_indicator_focus", "language", "notes"
        ]
        meta = texts_df[meta_cols].drop_duplicates(subset=["doc_id"]).copy()
        out = scored.merge(meta, on="doc_id", how="left")

        out["IDI_raw"] = pd.to_numeric(out.get("IDI", 0.0), errors="coerce").fillna(0.0)
        out["EMI_raw"] = pd.to_numeric(out.get("EMI", 0.0), errors="coerce").fillna(0.0)
        out["MTI_raw"] = pd.to_numeric(out.get("MTI", 0.0), errors="coerce").fillna(0.0)
        out["IDI_percent"] = out["IDI_raw"] * 100.0
        out["EMI_percent"] = out["EMI_raw"] * 100.0
        out["MTI_percent"] = out["MTI_raw"] * 100.0
        out["S_r"] = pd.to_numeric(out.get("referent_salience", 0.0), errors="coerce").fillna(0.0)
        out["IP_abs_context"] = pd.to_numeric(out.get("IP_context_abs", 0.0), errors="coerce").fillna(0.0)
        out["quality_flags"] = ""

        need = [
            "context_id", "calibration_id", "source_name", "title", "calibration_type", "expected_indicator_focus", "ref_country", "matched_keywords",
            "context_text", "previous_sentence", "target_sentence", "next_sentence", "N_content", "IDI_raw", "IDI_percent", "EMI_raw", "EMI_percent",
            "MTI_raw", "MTI_percent", "EVI_raw", "EVI_norm", "S_r", "salience_label", "IP_context", "IP_abs_context", "quality_flags", "language", "notes",
        ]
        for c in need:
            if c not in out.columns:
                out[c] = "" if c in {"title", "source_name", "calibration_type", "expected_indicator_focus", "matched_keywords", "context_text", "previous_sentence", "target_sentence", "next_sentence", "quality_flags", "language", "notes", "salience_label", "ref_country", "calibration_id"} else 0.0
        return out[need]

    @staticmethod
    def _assign_anchor_status(contexts_df: pd.DataFrame) -> pd.DataFrame:
        out = contexts_df.copy()
        if out.empty:
            out["anchor_status"] = "none"
            return out
        vals = pd.to_numeric(out["IP_abs_context"], errors="coerce").fillna(0.0).tolist()
        from calibration.calibration_metrics import get_percentile_rank

        def status(v: float) -> str:
            p = get_percentile_rank(v, vals)
            if p >= 95:
                return "extreme_anchor"
            if p >= 85:
                return "upper_anchor"
            if 45 <= p <= 55:
                return "middle_anchor"
            if p <= 15:
                return "lower_anchor"
            return "none"

        out["anchor_status"] = pd.to_numeric(out["IP_abs_context"], errors="coerce").fillna(0.0).apply(status)
        return out

    def build_report(self, texts_df: pd.DataFrame, contexts_df: pd.DataFrame, dists_df: pd.DataFrame, candidates_df: pd.DataFrame, flags_df: pd.DataFrame) -> str:
        lines = [
            "# Calibration Report",
            "",
            "## 1. Corpus size and category distribution",
            f"- Total texts: **{len(texts_df)}**",
            f"- Total referent contexts: **{len(contexts_df)}**",
            "",
            "## 2. Text counts by calibration_type",
        ]
        if "calibration_type" in texts_df.columns:
            vc = texts_df["calibration_type"].value_counts(dropna=False)
            for k, v in vc.items():
                lines.append(f"- {k}: {int(v)}")

        lines.append("\n## 3. Language distribution")
        if "language" in texts_df.columns:
            for k, v in texts_df["language"].value_counts(dropna=False).items():
                lines.append(f"- {k}: {int(v)}")

        lines.append("\n## 4. Ref_country distribution")
        if "ref_country" in contexts_df.columns and not contexts_df.empty:
            for k, v in contexts_df["ref_country"].value_counts(dropna=False).items():
                lines.append(f"- {k}: {int(v)}")

        lines.append("\n## 5. Indicator distributions")
        for metric in ["IDI_raw", "EMI_raw", "MTI_raw", "IP_abs_context"]:
            if metric in contexts_df.columns and not contexts_df.empty:
                med = float(pd.to_numeric(contexts_df[metric], errors="coerce").median())
                lines.append(f"- median {metric}: `{med:.6f}`")

        lines.append("\n## 6. Percentile thresholds")
        if not dists_df.empty:
            full = dists_df[(dists_df["group_scope"] == "full_calibration_corpus") & (dists_df["group_value"] == "all")]
            for metric in ["IDI_raw", "EMI_raw", "MTI_raw", "IP_abs_context"]:
                m = full[full["metric"] == metric]
                if not m.empty:
                    r = m.iloc[0]
                    lines.append(f"- {metric}: p75={float(r['p75']):.6f}, p90={float(r['p90']):.6f}, p95={float(r['p95']):.6f}")

        lines.append("\n## 7. Anchor texts")
        if "anchor_status" in texts_df.columns:
            ext = texts_df[texts_df["anchor_status"].isin(["upper_anchor", "extreme_anchor"])].head(10)
            if ext.empty:
                lines.append("- no anchors yet")
            else:
                for _, r in ext.iterrows():
                    lines.append(f"- {r.get('calibration_id','')} | {r.get('calibration_type','')} | {r.get('anchor_status','')}")

        lines.append("\n## 8. Dictionary expansion summary")
        lines.append(f"- candidate terms extracted: **{len(candidates_df)}**")

        lines.append("\n## 9. Quality flags")
        lines.append(f"- total flags: **{len(flags_df)}**")
        if not flags_df.empty:
            for k, v in flags_df["issue_type"].value_counts().items():
                lines.append(f"- {k}: {int(v)}")

        lines.append("\n## 10. Recommended next steps")
        lines.append("- Reach 100+ texts in core categories for dissertation-grade stability.")
        lines.append("- Approve/reject candidate terms and reload lexicons.")
        lines.append("- Recompute baseline and compare indicator deltas.")

        if not contexts_df.empty:
            # requested style example
            neu = contexts_df[contexts_df["calibration_type"] == "neutral_news"] if "calibration_type" in contexts_df.columns else pd.DataFrame()
            cri = contexts_df[contexts_df["calibration_type"] == "crisis_report"] if "calibration_type" in contexts_df.columns else pd.DataFrame()
            if not neu.empty:
                neu_med = float(pd.to_numeric(neu["EMI_raw"], errors="coerce").median())
                lines.append(f"\nCurrent neutral_news subset has median EMI_raw = {neu_med:.4f}.")
            if not cri.empty:
                cri_med = float(pd.to_numeric(cri["EMI_raw"], errors="coerce").median())
                lines.append(f"Current crisis_report subset has median EMI_raw = {cri_med:.4f}.")

        return "\n".join(lines)

    def run(
        self,
        sources: List[CalibrationSource],
        base_dir: Path,
        output_dir: Path,
        manual_rows: Optional[Iterable[Dict[str, Any]]] = None,
    ) -> CalibrationRunArtifacts:
        texts_df = self.collect_texts(sources=sources, base_dir=base_dir, manual_rows=manual_rows)

        contexts_df = self.build_contexts(texts_df)
        if not contexts_df.empty:
            contexts_df = self._assign_anchor_status(contexts_df)
            contexts_df = add_percentiles(contexts_df, contexts_df)
            for metric, pcol, lcol in [
                ("IDI_raw", "IDI_percentile", "IDI_empirical_level"),
                ("EMI_raw", "EMI_percentile", "EMI_empirical_level"),
                ("MTI_raw", "MTI_percentile", "MTI_empirical_level"),
                ("IP_context", "IP_percentile", "IP_empirical_level"),
                ("IP_abs_context", "IP_abs_percentile", "IP_abs_empirical_level"),
            ]:
                contexts_df[f"{metric}_interpretation"] = contexts_df.apply(
                    lambda r: interpret_indicator(metric, float(r.get(metric, 0.0)), float(r.get(pcol, 0.0)), str(r.get(lcol, "very_low")), str(r.get("calibration_type", "")), language="en"),
                    axis=1,
                )

        # update text-level ref_country/anchor/quality summary
        if not contexts_df.empty:
            grp = contexts_df.groupby("calibration_id", as_index=False).agg(
                ref_country=("ref_country", lambda x: ";".join(sorted(set(pd.Series(x).astype(str).tolist())))),
                anchor_status=("anchor_status", lambda x: pd.Series(x).mode().iloc[0] if not pd.Series(x).mode().empty else "none"),
            )
            texts_df = texts_df.merge(grp, on="calibration_id", how="left", suffixes=("", "_ctx"))
            texts_df["ref_country"] = texts_df["ref_country_ctx"].where(texts_df["ref_country_ctx"].notna() & (texts_df["ref_country_ctx"].astype(str) != ""), texts_df["ref_country"])
            texts_df["anchor_status"] = texts_df["anchor_status_ctx"].where(texts_df["anchor_status_ctx"].notna() & (texts_df["anchor_status_ctx"].astype(str) != ""), texts_df["anchor_status"])
            texts_df = texts_df.drop(columns=[c for c in ["ref_country_ctx", "anchor_status_ctx"] if c in texts_df.columns])

        dists_df = build_distributions(contexts_df)
        candidates_df = extract_candidate_terms(contexts_df)

        # persist candidate terms into lexicons workspace
        (self.lexicons_dir / "candidate_terms.csv").parent.mkdir(parents=True, exist_ok=True)
        candidates_df.to_csv(self.lexicons_dir / "candidate_terms.csv", index=False)

        flags_df = build_quality_flags(texts_df=texts_df, contexts_df=contexts_df)
        if not flags_df.empty:
            fgrp = flags_df.groupby("calibration_id")["issue_type"].apply(lambda x: ";".join(sorted(set(pd.Series(x).astype(str).tolist())))).reset_index(name="quality_flags")
            texts_df = texts_df.merge(fgrp, on="calibration_id", how="left", suffixes=("", "_new"))
            texts_df["quality_flags"] = texts_df["quality_flags_new"].where(texts_df["quality_flags_new"].notna(), texts_df.get("quality_flags", ""))
            texts_df = texts_df.drop(columns=[c for c in ["quality_flags_new"] if c in texts_df.columns])

        verified_df = pd.read_csv(self.lexicons_dir / "verified_terms.csv") if (self.lexicons_dir / "verified_terms.csv").exists() else pd.DataFrame()
        rejected_df = pd.read_csv(self.lexicons_dir / "rejected_terms.csv") if (self.lexicons_dir / "rejected_terms.csv").exists() else pd.DataFrame()
        change_log_df = pd.read_csv(self.lexicons_dir / "dictionary_change_log.csv") if (self.lexicons_dir / "dictionary_change_log.csv").exists() else pd.DataFrame()

        report_md = self.build_report(texts_df, contexts_df, dists_df, candidates_df, flags_df)

        export_all(
            out_dir=output_dir,
            texts_df=texts_df,
            contexts_df=contexts_df,
            distributions_df=dists_df,
            candidate_df=candidates_df,
            verified_df=verified_df,
            rejected_df=rejected_df,
            change_log_df=change_log_df,
            quality_flags_df=flags_df,
            report_md=report_md,
        )

        return CalibrationRunArtifacts(
            texts_df=texts_df,
            contexts_df=contexts_df,
            distributions_df=dists_df,
            quality_flags_df=flags_df,
            candidate_terms_df=candidates_df,
            report_markdown=report_md,
            metadata={
                "total_texts": int(len(texts_df)),
                "total_contexts": int(len(contexts_df)),
                "meets_min_50": bool(len(texts_df) >= 50),
            },
        )
