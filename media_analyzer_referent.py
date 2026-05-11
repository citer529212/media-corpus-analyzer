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
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
from analysis_core import preprocessing as prep
from analysis_core.lexicon_expansion import (
    approve_candidate as lex_approve_candidate,
    ensure_lexicon_workflow_files,
    extract_candidate_terms as lex_extract_candidate_terms,
    mark_context_dependent as lex_mark_context_dependent,
    reject_candidate as lex_reject_candidate,
    score_candidate_term as lex_score_candidate_term,
    suggest_dictionary as lex_suggest_dictionary,
    write_dictionary_change_log as lex_write_dictionary_change_log,
)


REQUIRED_FIELDS = ["doc_id", "media_country", "outlet_name", "date", "title", "text"]
REF_COUNTRIES = ["China", "USA", "Russia"]
EVI_COARSE_ALLOWED = {-2, -1, 0, 1, 2}  # legacy
SALIENCE_ALLOWED = {0.0, 0.25, 0.5, 1.0}

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

RUBRIC_POS_LEX = {
    "partner", "cooperation", "support", "stability", "opportunity", "contribution", "leadership", "development",
    "trust", "constructive", "benefit", "growth", "peace", "responsible", "legitimate",
    "kemitraan", "kerja sama", "kerjasama", "dukungan", "sokongan", "stabilitas", "kestabilan", "peluang",
    "kontribusi", "kepemimpinan", "kepimpinan", "pembangunan", "kepercayaan", "konstruktif", "membina",
    "manfaat", "faedah", "pertumbuhan", "damai", "aman", "bertanggung jawab", "bertanggungjawab", "sah",
    "партнер", "партнерство", "сотрудничество", "поддержка", "стабильность", "возможность", "вклад", "лидерство",
    "развитие", "доверие", "конструктивный", "выгода", "рост", "мир", "ответственный", "легитимный",
}
RUBRIC_NEG_LEX = {
    "threat", "aggression", "pressure", "crisis", "interference", "destabilize", "risk", "failure", "repression",
    "coercion", "conflict", "instability", "violates", "authoritarian", "hegemony",
    "ancaman", "agresi", "tekanan", "krisis", "campur tangan", "risiko", "kegagalan", "represi", "paksaan",
    "konflik", "ketidakstabilan", "pelanggaran", "otoriter", "hegemoni", "ugutan",
    "угроза", "агрессия", "давление", "кризис", "вмешательство", "дестабилизация", "риск", "провал",
    "репрессии", "принуждение", "конфликт", "нестабильность", "нарушает", "авторитарный", "гегемония",
}
RUBRIC_POS_ACTION = {
    "supports", "cooperates", "contributes", "stabilizes", "protects", "invests", "develops",
    "mendukung", "menyokong", "bekerja sama", "berkerjasama", "berkolaborasi", "berkontribusi",
    "menstabilkan", "melindungi", "berinvestasi", "mengembangkan",
    "поддерживает", "сотрудничает", "вкладывает", "инвестирует", "развивает", "стабилизирует", "защищает",
}
RUBRIC_NEG_ACTION = {
    "threatens", "pressures", "interferes", "attacks", "destabilizes", "violates", "controls",
    "mengancam", "menekan", "campur tangan", "menyerang", "mendestabilisasi", "melanggar", "mengontrol",
    "mengawal", "угрожает", "давит", "вмешивается", "атакует", "дестабилизирует", "нарушает", "контролирует",
}
RUBRIC_POS_CONSEQUENCE = {
    "development", "stability", "security", "growth", "peace", "partnership", "opportunities",
    "pembangunan", "stabilitas", "kestabilan", "keamanan", "pertumbuhan", "damai", "kemitraan", "peluang",
    "развитие", "стабильность", "безопасность", "рост", "мир", "партнерство", "возможности",
}
RUBRIC_NEG_CONSEQUENCE = {
    "instability", "tension", "conflict", "crisis", "dependence", "sovereignty risk",
    "ketidakstabilan", "ketegangan", "konflik", "krisis", "ketergantungan", "risiko kedaulatan",
    "нестабильность", "напряженность", "конфликт", "кризис", "зависимость", "риск суверенитету", "риск суверенитета",
}
RUBRIC_POS_FRAME = {
    "legitimate partner", "responsible actor", "defender of sovereignty", "stabilizing power",
    "mitra yang sah", "aktor bertanggung jawab", "pembela kedaulatan", "kekuatan penstabil",
    "легитимный партнер", "ответственный актор", "защитник суверенитета", "стабилизирующая сила",
}
RUBRIC_NEG_FRAME = {
    "hegemon", "aggressor", "authoritarian regime", "violator of international law", "destabilizing force",
    "hegemoni", "agresor", "rezim otoriter", "pelanggar hukum internasional", "kekuatan destabilisasi",
    "гегемон", "агрессор", "авторитарный режим", "нарушитель международного права", "дестабилизирующая сила",
}
AUTHORITY_CUES = {
    "according to", "official", "minister", "president", "government", "spokesperson", "analyst", "diplomat",
    "menurut", "pemerintah", "kerajaan", "menteri", "jurucakap", "analis", "diplomat",
    "президент", "министр", "правительство", "по словам", "заявил", "сообщил", "дипломат",
}
NEUTRAL_ACTION_CUES = {
    "said", "stated", "announced", "met", "meeting", "discussed", "agreed", "visited", "reported",
    "mengatakan", "menyatakan", "bertemu", "membahas", "setuju", "melaporkan",
    "заявил", "сообщил", "обсудил", "встретился", "провел встречу",
}

COARSE_TO_RAW = {-2: -10, -1: -5, 0: 0, 1: 5, 2: 10}

LEXICON_SCHEMAS: Dict[str, List[str]] = {
    "ideological_markers.csv": [
        "term", "lemma", "language", "category", "semantic_zone", "polarity_hint", "strength_hint",
        "context_dependent", "examples", "exclude_patterns", "source", "verified",
    ],
    "emotional_markers.csv": [
        "term", "lemma", "language", "emotion_type", "intensity_level", "weight", "polarity_hint",
        "context_dependent", "examples", "exclude_patterns", "source", "verified",
    ],
    "metaphor_markers.csv": [
        "term", "lemma", "language", "metaphor_model", "source_domain", "target_domain", "conventionality",
        "default_strength", "context_dependent", "examples", "exclude_patterns", "source", "verified",
    ],
    "evi_lexicon.csv": [
        "term", "lemma", "language", "evaluation_type", "polarity", "strength", "category", "context_dependent",
        "examples", "exclude_patterns", "source", "verified",
    ],
    "actor_actions.csv": [
        "verb_or_phrase", "lemma", "language", "action_polarity", "action_strength", "typical_subject",
        "typical_object", "examples", "verified",
    ],
    "consequence_markers.csv": [
        "term_or_phrase", "language", "consequence_polarity", "consequence_domain", "strength", "examples", "verified",
    ],
    "ideological_frames.csv": [
        "frame_name", "frame_type", "polarity", "keywords", "examples", "verified",
    ],
    "salience_patterns.csv": [
        "pattern", "pattern_type", "salience_value", "examples", "verified",
    ],
    "technical_mention_patterns.csv": [
        "pattern", "technical_type", "salience_value", "examples", "verified",
    ],
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

DEFAULT_IDEO_MARKERS = [
    # EN
    "democracy", "sovereignty", "freedom", "authoritarianism", "communism", "imperialism", "colonialism",
    "neocolonialism", "multipolarity", "rules-based order", "regime", "dictatorship", "hegemony",
    "communist party", "liberal order", "authoritarian state", "violates international law",
    "defends sovereignty", "threatens regional stability", "protects national interests",
    # RU
    "демократия", "суверенитет", "свобода", "авторитаризм", "коммунизм", "империализм", "колониализм",
    "неоколониализм", "многополярность", "порядок, основанный на правилах", "режим", "диктатура", "гегемония",
    "коммунистическая партия", "либеральный порядок", "авторитарное государство",
    "нарушает международное право", "защищает суверенитет", "угрожает региональной стабильности",
    "защищает национальные интересы",
    # ID / MS
    "demokrasi", "kedaulatan", "kebebasan", "otoritarianisme", "komunisme", "imperialisme", "kolonialisme",
    "neokolonialisme", "multipolaritas", "aturan berbasis tatanan", "rezim", "kediktatoran", "hegemoni",
    "partai komunis", "tatanan liberal", "negara otoriter", "melanggar hukum internasional",
    "membela kedaulatan", "mengancam stabilitas regional", "melindungi kepentingan nasional",
]

DEFAULT_EMOT_ROWS = [
    # weak
    ("concern", "weak"), ("problem", "weak"), ("support", "weak"), ("challenge", "weak"), ("tension", "weak"),
    ("kebimbangan", "weak"), ("keprihatinan", "weak"), ("masalah", "weak"), ("dukungan", "weak"), ("sokongan", "weak"),
    ("cabaran", "weak"), ("tantangan", "weak"), ("ketegangan", "weak"),
    ("озабоченность", "weak"), ("проблема", "weak"), ("поддержка", "weak"), ("вызов", "weak"), ("напряжение", "weak"),
    # medium
    ("threat", "medium"), ("anger", "medium"), ("fear", "medium"), ("pressure", "medium"), ("conflict", "medium"), ("criticism", "medium"),
    ("ancaman", "medium"), ("kemarahan", "medium"), ("ketakutan", "medium"), ("tekanan", "medium"), ("konflik", "medium"), ("kritik", "medium"),
    ("угроза", "medium"), ("гнев", "medium"), ("страх", "medium"), ("давление", "medium"), ("конфликт", "medium"), ("критика", "medium"),
    # strong
    ("catastrophe", "strong"), ("betrayal", "strong"), ("aggression", "strong"), ("triumph", "strong"), ("heroic", "strong"), ("disaster", "strong"), ("collapse", "strong"),
    ("bencana", "strong"), ("pengkhianatan", "strong"), ("agresi", "strong"), ("kejayaan", "strong"), ("heroik", "strong"), ("runtuh", "strong"),
    ("катастрофа", "strong"), ("предательство", "strong"), ("агрессия", "strong"), ("триумф", "strong"), ("героический", "strong"), ("бедствие", "strong"), ("коллапс", "strong"),
]

DEFAULT_META_ROWS = [
    ("battle", "war"), ("fight", "war"), ("attack", "war"), ("defense", "war"), ("frontline", "war"),
    ("pertempuran", "war"), ("bertarung", "war"), ("serangan", "war"), ("pertahanan", "war"), ("garis depan", "war"),
    ("битва", "war"), ("борьба", "war"), ("атака", "war"), ("оборона", "war"), ("фронт", "war"),
    ("healthy economy", "organism"), ("sick system", "organism"), ("recovery", "organism"), ("virus", "organism"),
    ("ekonomi sehat", "organism"), ("sistem sakit", "organism"), ("pemulihan", "organism"), ("восстановление", "organism"),
    ("growth path", "movement"), ("wave", "movement"), ("flow", "movement"), ("collapse", "movement"),
    ("jalur pertumbuhan", "movement"), ("gelombang", "movement"), ("aliran", "movement"), ("runtuh", "movement"),
    ("player", "game"), ("move", "game"), ("strategy", "game"), ("chessboard", "game"),
    ("pemain", "game"), ("langkah", "game"), ("strategi", "game"), ("papan catur", "game"),
    ("wants", "personification"), ("fears", "personification"), ("pressures", "personification"),
    ("ingin", "personification"), ("takut", "personification"), ("menekan", "personification"),
    ("хочет", "personification"), ("боится", "personification"), ("давит", "personification"),
]


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for raw in values:
        v = str(raw).strip()
        if not v:
            continue
        k = v.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(v)
    return out


def _read_terms_column(df: pd.DataFrame, candidates: List[str]) -> List[str]:
    for c in candidates:
        if c in df.columns:
            return _dedupe_keep_order(df[c].fillna("").astype(str).tolist())
    return []


def _project_lexicons_dir() -> Path:
    return Path(__file__).resolve().parent / "lexicons"


def _ensure_table_columns(path: Path, columns: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        pd.DataFrame(columns=columns).to_csv(path, index=False)
        return
    try:
        df = pd.read_csv(path).fillna("")
    except Exception:
        pd.DataFrame(columns=columns).to_csv(path, index=False)
        return
    changed = False
    for c in columns:
        if c not in df.columns:
            df[c] = ""
            changed = True
    if changed:
        df = df[columns + [c for c in df.columns if c not in columns]]
        df.to_csv(path, index=False)


def ensure_project_lexicon_schema(lex_dir: Optional[Path] = None) -> Path:
    base = lex_dir or _project_lexicons_dir()
    base.mkdir(parents=True, exist_ok=True)
    for name, cols in LEXICON_SCHEMAS.items():
        _ensure_table_columns(base / name, cols)
    meta = base / "lexicon_metadata.json"
    if not meta.exists():
        meta.write_text(
            json.dumps(
                {
                    "version": "1.0.0",
                    "description": "Marker dictionaries and rubric rules for referent-focused discourse analysis.",
                    "languages": ["en", "id", "ms", "ru", "mixed"],
                    "referents": REF_COUNTRIES,
                    "note": "NLP libraries are preprocessing tools; scientific marker categories come from lexicons.",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    ensure_lexicon_workflow_files(base)
    return base


def _load_project_lexicon_overrides() -> Dict[str, object]:
    out: Dict[str, object] = {
        "ideological": [],
        "emotional": {"weak": [], "medium": [], "strong": []},
        "metaphor": [],
        "rubric_pos_lex": [],
        "rubric_neg_lex": [],
        "rubric_pos_action": [],
        "rubric_neg_action": [],
        "rubric_pos_consequence": [],
        "rubric_neg_consequence": [],
        "technical_patterns": [],
    }
    lex_dir = ensure_project_lexicon_schema(_project_lexicons_dir())
    if not lex_dir.exists():
        return out

    p_ideo = lex_dir / "ideological_markers.csv"
    if p_ideo.exists():
        try:
            df = pd.read_csv(p_ideo).fillna("")
            out["ideological"] = _read_terms_column(df, ["marker", "term", "lemma"])
        except Exception:
            pass

    p_emot = lex_dir / "emotional_markers.csv"
    if p_emot.exists():
        try:
            df = pd.read_csv(p_emot).fillna("")
            term_col = "marker" if "marker" in df.columns else ("term" if "term" in df.columns else None)
            int_col = "intensity" if "intensity" in df.columns else ("intensity_level" if "intensity_level" in df.columns else None)
            if term_col and int_col:
                emo = {"weak": [], "medium": [], "strong": []}
                for _, r in df.iterrows():
                    term = str(r.get(term_col, "")).strip()
                    intensity = str(r.get(int_col, "")).strip().lower()
                    if not term:
                        continue
                    if intensity in {"weak", "low"}:
                        emo["weak"].append(term)
                    elif intensity in {"medium", "mid"}:
                        emo["medium"].append(term)
                    elif intensity in {"strong", "high"}:
                        emo["strong"].append(term)
                out["emotional"] = {k: _dedupe_keep_order(v) for k, v in emo.items()}
        except Exception:
            pass

    p_meta = lex_dir / "metaphor_markers.csv"
    if p_meta.exists():
        try:
            df = pd.read_csv(p_meta).fillna("")
            out["metaphor"] = _read_terms_column(df, ["marker", "term", "lemma"])
        except Exception:
            pass

    p_evi = lex_dir / "evi_lexicon.csv"
    if p_evi.exists():
        try:
            df = pd.read_csv(p_evi).fillna("")
            term_col = "term" if "term" in df.columns else ("marker" if "marker" in df.columns else None)
            pol_col = "polarity" if "polarity" in df.columns else ("polarity_hint" if "polarity_hint" in df.columns else None)
            cat_col = "category" if "category" in df.columns else None
            if term_col and pol_col:
                pos, neg = [], []
                for _, r in df.iterrows():
                    term = str(r.get(term_col, "")).strip()
                    pol = str(r.get(pol_col, "")).strip().lower()
                    cat = str(r.get(cat_col, "")).strip().lower() if cat_col else ""
                    if not term:
                        continue
                    if pol in {"positive", "pos", "+"}:
                        if cat in {"action", "actor_action"}:
                            out["rubric_pos_action"].append(term)
                        elif cat in {"consequence"}:
                            out["rubric_pos_consequence"].append(term)
                        else:
                            pos.append(term)
                    elif pol in {"negative", "neg", "-"}:
                        if cat in {"action", "actor_action"}:
                            out["rubric_neg_action"].append(term)
                        elif cat in {"consequence"}:
                            out["rubric_neg_consequence"].append(term)
                        else:
                            neg.append(term)
                out["rubric_pos_lex"] = _dedupe_keep_order(pos)
                out["rubric_neg_lex"] = _dedupe_keep_order(neg)
        except Exception:
            pass

    p_actions = lex_dir / "actor_actions.csv"
    if p_actions.exists():
        try:
            df = pd.read_csv(p_actions).fillna("")
            term_col = "verb_or_phrase" if "verb_or_phrase" in df.columns else "term"
            pol_col = "action_polarity" if "action_polarity" in df.columns else "polarity"
            for _, r in df.iterrows():
                term = str(r.get(term_col, "")).strip()
                pol = str(r.get(pol_col, "")).strip().lower()
                if not term:
                    continue
                if pol in {"positive", "pos", "+"}:
                    out["rubric_pos_action"].append(term)
                elif pol in {"negative", "neg", "-"}:
                    out["rubric_neg_action"].append(term)
        except Exception:
            pass

    p_cons = lex_dir / "consequence_markers.csv"
    if p_cons.exists():
        try:
            df = pd.read_csv(p_cons).fillna("")
            term_col = "term_or_phrase" if "term_or_phrase" in df.columns else "term"
            pol_col = "consequence_polarity" if "consequence_polarity" in df.columns else "polarity"
            for _, r in df.iterrows():
                term = str(r.get(term_col, "")).strip()
                pol = str(r.get(pol_col, "")).strip().lower()
                if not term:
                    continue
                if pol in {"positive", "pos", "+"}:
                    out["rubric_pos_consequence"].append(term)
                elif pol in {"negative", "neg", "-"}:
                    out["rubric_neg_consequence"].append(term)
        except Exception:
            pass

    p_tech = lex_dir / "technical_mention_patterns.csv"
    if p_tech.exists():
        try:
            df = pd.read_csv(p_tech).fillna("")
            out["technical_patterns"] = _read_terms_column(df, ["pattern"])
        except Exception:
            pass

    # Final normalize
    out["ideological"] = _dedupe_keep_order(out["ideological"])  # type: ignore[index]
    out["metaphor"] = _dedupe_keep_order(out["metaphor"])  # type: ignore[index]
    for k in ["rubric_pos_lex", "rubric_neg_lex", "rubric_pos_action", "rubric_neg_action", "rubric_pos_consequence", "rubric_neg_consequence", "technical_patterns"]:
        out[k] = _dedupe_keep_order(out[k])  # type: ignore[index]
    emo_obj = out["emotional"]  # type: ignore[assignment]
    if isinstance(emo_obj, dict):
        out["emotional"] = {x: _dedupe_keep_order(emo_obj.get(x, [])) for x in ["weak", "medium", "strong"]}
    return out


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


@dataclass
class MarkerTrace:
    marker_id: str
    context_id: str
    ref_country: str
    indicator: str
    term_found: str
    lemma: str
    dictionary_source: str
    category: str
    semantic_zone_or_model: str
    intensity_or_strength: str
    matched_span: str
    context_text: str
    is_context_dependent: bool
    verification_status: str
    inclusion_reason: str
    exclusion_reason: str

    def to_dict(self) -> Dict[str, object]:
        return self.__dict__.copy()


def normalize_token(s: str) -> str:
    return s.casefold().strip("-'_ ")


def tokenize(text: str) -> List[str]:
    return prep.tokenize(text)


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
    project_lex = ensure_project_lexicon_schema(_project_lexicons_dir())
    ensure_project_lexicon_schema(dict_dir)
    lex_overrides = _load_project_lexicon_overrides()

    # Mirror full schema tables into working dictionary dir when empty.
    for fname in LEXICON_SCHEMAS.keys():
        src = project_lex / fname
        dst = dict_dir / fname
        if src.exists() and (not dst.exists() or dst.stat().st_size <= 8):
            dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    # Backward compatibility aliases.
    p_meta = dict_dir / "metaphor_markers.csv"
    p_meta_legacy = dict_dir / "metaphor_candidates.csv"
    if p_meta.exists() and not p_meta_legacy.exists():
        p_meta_legacy.write_text(p_meta.read_text(encoding="utf-8"), encoding="utf-8")

    # Ensure essential defaults are present in working dicts.
    p_ideo = dict_dir / "ideological_markers.csv"
    d_ideo = pd.read_csv(p_ideo).fillna("")
    col_ideo = "term" if "term" in d_ideo.columns else ("marker" if "marker" in d_ideo.columns else None)
    if col_ideo is None:
        d_ideo["term"] = ""
        col_ideo = "term"
    known = set(d_ideo[col_ideo].astype(str).str.casefold().tolist())
    for t in DEFAULT_IDEO_MARKERS + list(lex_overrides.get("ideological", [])):
        if str(t).casefold() in known:
            continue
        row = {c: "" for c in d_ideo.columns}
        row[col_ideo] = t
        if "lemma" in row:
            row["lemma"] = t
        if "verified" in row:
            row["verified"] = True
        if "source" in row:
            row["source"] = "seed"
        d_ideo = pd.concat([d_ideo, pd.DataFrame([row])], ignore_index=True)
        known.add(str(t).casefold())
    d_ideo.to_csv(p_ideo, index=False)

    p_emot = dict_dir / "emotional_markers.csv"
    d_emot = pd.read_csv(p_emot).fillna("")
    term_col = "term" if "term" in d_emot.columns else ("marker" if "marker" in d_emot.columns else None)
    int_col = "intensity_level" if "intensity_level" in d_emot.columns else ("intensity" if "intensity" in d_emot.columns else None)
    if term_col is None:
        d_emot["term"] = ""
        term_col = "term"
    if int_col is None:
        d_emot["intensity_level"] = ""
        int_col = "intensity_level"
    seen = set((str(r.get(term_col, "")).casefold(), str(r.get(int_col, "")).casefold()) for _, r in d_emot.iterrows())
    for term, intensity in DEFAULT_EMOT_ROWS:
        key = (str(term).casefold(), str(intensity).casefold())
        if key in seen:
            continue
        row = {c: "" for c in d_emot.columns}
        row[term_col] = term
        row[int_col] = intensity
        if "weight" in row:
            row["weight"] = 1.0 if intensity == "strong" else (2.0 / 3.0 if intensity == "medium" else 1.0 / 3.0)
        if "lemma" in row:
            row["lemma"] = term
        if "verified" in row:
            row["verified"] = True
        if "source" in row:
            row["source"] = "seed"
        d_emot = pd.concat([d_emot, pd.DataFrame([row])], ignore_index=True)
        seen.add(key)
    d_emot.to_csv(p_emot, index=False)

    d_meta = pd.read_csv(p_meta).fillna("")
    tcol = "term" if "term" in d_meta.columns else ("marker" if "marker" in d_meta.columns else None)
    if tcol is None:
        d_meta["term"] = ""
        tcol = "term"
    known_meta = set(d_meta[tcol].astype(str).str.casefold().tolist())
    for t, mmodel in DEFAULT_META_ROWS:
        if str(t).casefold() in known_meta:
            continue
        row = {c: "" for c in d_meta.columns}
        row[tcol] = t
        if "lemma" in row:
            row["lemma"] = t
        if "metaphor_model" in row:
            row["metaphor_model"] = mmodel
        if "context_dependent" in row:
            row["context_dependent"] = True
        if "verified" in row:
            row["verified"] = "context_dependent"
        if "source" in row:
            row["source"] = "seed"
        d_meta = pd.concat([d_meta, pd.DataFrame([row])], ignore_index=True)
        known_meta.add(str(t).casefold())
    d_meta.to_csv(p_meta, index=False)
    d_meta.to_csv(p_meta_legacy, index=False)

    ensure_lexicon_workflow_files(dict_dir)


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
    return _read_terms_column(df, ["term", "marker", "lemma"])


def load_emotional_markers(dict_dir: Path) -> Dict[str, List[str]]:
    df = pd.read_csv(dict_dir / "emotional_markers.csv").fillna("")
    out = {"weak": [], "medium": [], "strong": []}
    term_col = "term" if "term" in df.columns else ("marker" if "marker" in df.columns else None)
    int_col = "intensity_level" if "intensity_level" in df.columns else ("intensity" if "intensity" in df.columns else None)
    if not term_col or not int_col:
        return out
    for _, r in df.iterrows():
        marker = str(r.get(term_col, "")).strip()
        intensity = str(r.get(int_col, "")).strip().lower()
        if marker and intensity in out:
            out[intensity].append(marker)
    for k in out:
        out[k] = _dedupe_keep_order(out[k])
    return out


def load_metaphor_candidates(dict_dir: Path) -> List[str]:
    p_main = dict_dir / "metaphor_markers.csv"
    p_legacy = dict_dir / "metaphor_candidates.csv"
    p = p_main if p_main.exists() else p_legacy
    if not p.exists():
        return []
    df = pd.read_csv(p).fillna("")
    return _read_terms_column(df, ["term", "marker", "lemma"])


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
    return prep.sentence_split(text)


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
    return int(prep.count_content_words(text))


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def coarse_from_raw(evi_raw: int) -> int:
    if evi_raw <= -8:
        return -2
    if evi_raw <= -3:
        return -1
    if evi_raw < 3:
        return 0
    if evi_raw < 8:
        return 1
    return 2


def score_by_terms(text: str, terms: Iterable[str], max_points: int) -> Tuple[int, List[str], int]:
    total, found = count_marker_hits(text, terms)
    points = int(min(max_points, len(found)))
    return points, found, total


def detect_technical_mention(context_text: str, ref_country: str, extra_patterns: Optional[List[str]] = None) -> Tuple[bool, str]:
    txt = context_text.casefold().strip()
    loc = {"USA": "washington", "China": "beijing", "Russia": "moscow"}.get(ref_country, "")
    if not txt:
        return True, "Empty context."
    byline_patterns = [
        rf"^reporting by .+ in {re.escape(loc)}\.?$",
        rf"^by .+ in {re.escape(loc)}\.?$",
        rf"^reuters in {re.escape(loc)}\.?$",
        rf"^associated press in {re.escape(loc)}\.?$",
        rf"^reporting from {re.escape(loc)}\.?$",
        rf"^laporan oleh .+ di {re.escape(loc)}\.?$",
        rf"^dilaporkan dari {re.escape(loc)}\.?$",
        rf"^из {re.escape(loc)}\.?$",
        rf"^в {re.escape(loc)}\.?$",
    ]
    if extra_patterns:
        byline_patterns.extend([p for p in extra_patterns if p])
    for p in byline_patterns:
        if re.match(p, txt):
            return True, f"{loc.title()} appears only in byline/dateline."
    if loc and re.search(rf"\b(in|from|di|dari|в|из)\s+{re.escape(loc)}\b", txt):
        has_predicate = bool(re.search(r"\b(support|threat|pressure|cooperat|invest|develop|aggress|violat|stabil|interfer)\w*\b", txt))
        if not has_predicate and len(tokenize(txt)) <= 14:
            return True, f"{loc.title()} used as geographic location only."
    return False, ""


def calc_referent_salience(
    ref_country: str,
    context_text: str,
    title: str,
    target_sentence: str,
    positive_score: int,
    negative_score: int,
    matched_keywords: str,
    technical_patterns: Optional[List[str]] = None,
) -> Tuple[float, str, bool, str, str]:
    technical, reason = detect_technical_mention(context_text, ref_country, extra_patterns=technical_patterns)
    if technical:
        return 0.0, "technical", True, reason, "Technical mention only; excluded from image formation."

    eval_present = (positive_score + negative_score) > 0
    target_txt = target_sentence.casefold()
    title_txt = title.casefold()
    action_present = any(k in target_txt for k in (RUBRIC_POS_ACTION | RUBRIC_NEG_ACTION | NEUTRAL_ACTION_CUES))
    ref_in_title = any(k.casefold() in title_txt for k in [x.strip() for x in matched_keywords.split(";") if x.strip()])
    match_count = len([x for x in matched_keywords.split(";") if x.strip()])

    if not eval_present and not action_present:
        return 0.25, "background", False, "", "Referent mentioned as background reference without evaluative role."
    if action_present and not eval_present:
        return 0.5, "secondary_actor", False, "", "Referent participates in event, but evaluation focus is weak."
    if eval_present and (ref_in_title or match_count >= 2):
        return 1.0, "central_actor", False, "", "Referent is central actor and explicit object of evaluation."
    if eval_present:
        return 1.0, "central_actor", False, "", "Referent is explicitly evaluated in the context."
    return 0.5, "secondary_actor", False, "", "Referent has partial discourse role."


def calc_evi_rubric(
    ref_country: str,
    row: pd.Series,
    rubric_pos_lex: Optional[Set[str]] = None,
    rubric_neg_lex: Optional[Set[str]] = None,
    rubric_pos_action: Optional[Set[str]] = None,
    rubric_neg_action: Optional[Set[str]] = None,
    rubric_pos_consequence: Optional[Set[str]] = None,
    rubric_neg_consequence: Optional[Set[str]] = None,
    rubric_pos_frame: Optional[Set[str]] = None,
    rubric_neg_frame: Optional[Set[str]] = None,
) -> dict:
    rubric_pos_lex = rubric_pos_lex or set(RUBRIC_POS_LEX)
    rubric_neg_lex = rubric_neg_lex or set(RUBRIC_NEG_LEX)
    rubric_pos_action = rubric_pos_action or set(RUBRIC_POS_ACTION)
    rubric_neg_action = rubric_neg_action or set(RUBRIC_NEG_ACTION)
    rubric_pos_consequence = rubric_pos_consequence or set(RUBRIC_POS_CONSEQUENCE)
    rubric_neg_consequence = rubric_neg_consequence or set(RUBRIC_NEG_CONSEQUENCE)
    rubric_pos_frame = rubric_pos_frame or set(RUBRIC_POS_FRAME)
    rubric_neg_frame = rubric_neg_frame or set(RUBRIC_NEG_FRAME)

    context_text = str(row.get("context_text", ""))
    target_sentence = str(row.get("target_sentence", "")) or context_text
    title = str(row.get("title", ""))
    eval_text = " ".join([title, target_sentence]).strip()
    full_text = context_text

    p1, p1_terms, p1_hits = score_by_terms(eval_text, rubric_pos_lex, 3)
    n1, n1_terms, n1_hits = score_by_terms(eval_text, rubric_neg_lex, 3)

    p2, p2_terms, p2_hits = score_by_terms(eval_text, rubric_pos_action, 2)
    n2, n2_terms, n2_hits = score_by_terms(eval_text, rubric_neg_action, 2)

    p3, p3_terms, p3_hits = score_by_terms(eval_text, rubric_pos_consequence, 2)
    n3, n3_terms, n3_hits = score_by_terms(eval_text, rubric_neg_consequence, 2)

    p4, p4_terms, p4_hits = score_by_terms(eval_text, rubric_pos_frame, 2)
    n4, n4_terms, n4_hits = score_by_terms(eval_text, rubric_neg_frame, 2)

    has_authority = any(cue in full_text.casefold() for cue in AUTHORITY_CUES)
    pos_repeat = (p1_hits + p2_hits + p3_hits + p4_hits) >= 2
    neg_repeat = (n1_hits + n2_hits + n3_hits + n4_hits) >= 2
    title_casefold = title.casefold()
    ref_in_title = ref_country.casefold() in title_casefold or any(k in title_casefold for k in ["washington", "beijing", "moscow"])
    p5 = 1 if (p1 + p2 + p3 + p4) > 0 and (ref_in_title or pos_repeat or has_authority) else 0
    n5 = 1 if (n1 + n2 + n3 + n4) > 0 and (ref_in_title or neg_repeat or has_authority) else 0

    positive_score = int(clamp(p1 + p2 + p3 + p4 + p5, 0, 10))
    negative_score = int(clamp(n1 + n2 + n3 + n4 + n5, 0, 10))
    evi_raw = int(clamp(positive_score - negative_score, -10, 10))
    evi_norm = float(evi_raw / 10.0)

    pos_terms = sorted(set(p1_terms + p2_terms + p3_terms + p4_terms))
    neg_terms = sorted(set(n1_terms + n2_terms + n3_terms + n4_terms))
    evi_evidence = {
        "criterion_1_lex": {"positive": p1, "negative": n1},
        "criterion_2_action": {"positive": p2, "negative": n2},
        "criterion_3_consequence": {"positive": p3, "negative": n3},
        "criterion_4_frame": {"positive": p4, "negative": n4},
        "criterion_5_salience": {"positive": p5, "negative": n5},
    }
    if evi_raw > 0:
        pol = "positive"
    elif evi_raw < 0:
        pol = "negative"
    else:
        pol = "neutral"
    evi_expl = (
        f"{ref_country} receives a {pol} score: P={positive_score}, N={negative_score}, EVI={evi_raw}. "
        "Score is derived from lexical, action, consequence, ideological and prominence criteria."
    )
    return {
        "positive_score": positive_score,
        "negative_score": negative_score,
        "evi_raw": evi_raw,
        "evi_norm": evi_norm,
        "evi_evidence": json.dumps(evi_evidence, ensure_ascii=False),
        "evi_explanation": evi_expl,
        "positive_evidence_terms": "; ".join(pos_terms),
        "negative_evidence_terms": "; ".join(neg_terms),
        "evi_pos_hits": int(p1_hits + p2_hits + p3_hits + p4_hits),
        "evi_neg_hits": int(n1_hits + n2_hits + n3_hits + n4_hits),
    }


def suggest_evi_coarse(
    context_text: str,
    ref_keywords: List[str],
    rubric_pos_lex: Optional[Set[str]] = None,
    rubric_neg_lex: Optional[Set[str]] = None,
    rubric_pos_action: Optional[Set[str]] = None,
    rubric_neg_action: Optional[Set[str]] = None,
    rubric_pos_consequence: Optional[Set[str]] = None,
    rubric_neg_consequence: Optional[Set[str]] = None,
    rubric_pos_frame: Optional[Set[str]] = None,
    rubric_neg_frame: Optional[Set[str]] = None,
) -> Tuple[int, str]:
    rubric_pos_lex = rubric_pos_lex or set(RUBRIC_POS_LEX)
    rubric_neg_lex = rubric_neg_lex or set(RUBRIC_NEG_LEX)
    rubric_pos_action = rubric_pos_action or set(RUBRIC_POS_ACTION)
    rubric_neg_action = rubric_neg_action or set(RUBRIC_NEG_ACTION)
    rubric_pos_consequence = rubric_pos_consequence or set(RUBRIC_POS_CONSEQUENCE)
    rubric_neg_consequence = rubric_neg_consequence or set(RUBRIC_NEG_CONSEQUENCE)
    rubric_pos_frame = rubric_pos_frame or set(RUBRIC_POS_FRAME)
    rubric_neg_frame = rubric_neg_frame or set(RUBRIC_NEG_FRAME)

    ref_hit = False
    for kw in ref_keywords:
        if re.search(rf"\b{re.escape(kw)}\b", context_text, flags=re.IGNORECASE):
            ref_hit = True
            break
    if not ref_hit:
        return 0, "No clear referent cue in context."

    pos_hits, _ = count_marker_hits(context_text, rubric_pos_lex | rubric_pos_action | rubric_pos_consequence | rubric_pos_frame)
    neg_hits, _ = count_marker_hits(context_text, rubric_neg_lex | rubric_neg_action | rubric_neg_consequence | rubric_neg_frame)
    score = pos_hits - neg_hits
    if score <= -3:
        return -2, f"Strong negative cues: pos={pos_hits}, neg={neg_hits}"
    if score < 0:
        return -1, f"Moderate negative cues: pos={pos_hits}, neg={neg_hits}"
    if score == 0:
        return 0, "Balanced or informational context."
    if score < 3:
        return 1, f"Moderate positive cues: pos={pos_hits}, neg={neg_hits}"
    return 2, f"Strong positive cues: pos={pos_hits}, neg={neg_hits}"


def _truthy(v: object) -> bool:
    s = str(v).strip().casefold()
    return s in {"1", "true", "yes", "y", "verified"}


def _first_matched_span(text: str, term: str) -> str:
    try:
        m = re.search(rf"\b{re.escape(term)}\b", text, flags=re.IGNORECASE)
        if not m:
            return ""
        lo = max(0, m.start() - 25)
        hi = min(len(text), m.end() + 25)
        return text[lo:hi]
    except Exception:
        return ""


def _term_meta_map(path: Path, term_candidates: List[str], fields: List[str]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    if not path.exists():
        return out
    try:
        df = pd.read_csv(path).fillna("")
    except Exception:
        return out
    tcol = None
    for c in term_candidates:
        if c in df.columns:
            tcol = c
            break
    if not tcol:
        return out
    for _, r in df.iterrows():
        t = str(r.get(tcol, "")).strip()
        if not t:
            continue
        out[t.casefold()] = {f: str(r.get(f, "")).strip() for f in fields}
    return out


def _build_marker_trace(
    context_id: str,
    ref_country: str,
    indicator: str,
    term: str,
    context_text: str,
    dictionary_source: str,
    meta: Optional[Dict[str, str]] = None,
    inclusion_reason: str = "",
    exclusion_reason: str = "",
) -> MarkerTrace:
    meta = meta or {}
    return MarkerTrace(
        marker_id=f"m_{context_id}_{uuid.uuid4().hex[:10]}",
        context_id=context_id,
        ref_country=ref_country,
        indicator=indicator,
        term_found=term,
        lemma=meta.get("lemma", term),
        dictionary_source=dictionary_source,
        category=meta.get("category", ""),
        semantic_zone_or_model=meta.get("semantic_zone", meta.get("metaphor_model", meta.get("emotion_type", ""))),
        intensity_or_strength=meta.get("intensity_level", meta.get("strength", meta.get("default_strength", ""))),
        matched_span=_first_matched_span(context_text, term),
        context_text=context_text,
        is_context_dependent=_truthy(meta.get("context_dependent", "")),
        verification_status=str(meta.get("verified", "")),
        inclusion_reason=inclusion_reason,
        exclusion_reason=exclusion_reason,
    )


def apply_metrics(
    contexts: pd.DataFrame,
    dict_dir: Path,
    evi_mode: str,
    evi_manual_path: Path | None,
    metaphor_review_path: Path | None,
    return_traces: bool = False,
) -> pd.DataFrame | Tuple[pd.DataFrame, pd.DataFrame]:
    ideol_markers = load_ideological_markers(dict_dir)
    emot_markers = load_emotional_markers(dict_dir)
    metaphors = load_metaphor_candidates(dict_dir)
    ref_kw = load_ref_keywords(dict_dir)
    lex_overrides = _load_project_lexicon_overrides()
    ideol_markers = _dedupe_keep_order(ideol_markers + list(lex_overrides.get("ideological", [])))
    metaphors = _dedupe_keep_order(metaphors + list(lex_overrides.get("metaphor", [])))
    emo_override = lex_overrides.get("emotional", {})
    if isinstance(emo_override, dict):
        for k in ["weak", "medium", "strong"]:
            emot_markers[k] = _dedupe_keep_order(emot_markers.get(k, []) + list(emo_override.get(k, [])))

    rubric_pos_lex = set(RUBRIC_POS_LEX) | set(lex_overrides.get("rubric_pos_lex", []))
    rubric_neg_lex = set(RUBRIC_NEG_LEX) | set(lex_overrides.get("rubric_neg_lex", []))
    rubric_pos_action = set(RUBRIC_POS_ACTION) | set(lex_overrides.get("rubric_pos_action", []))
    rubric_neg_action = set(RUBRIC_NEG_ACTION) | set(lex_overrides.get("rubric_neg_action", []))
    rubric_pos_consequence = set(RUBRIC_POS_CONSEQUENCE) | set(lex_overrides.get("rubric_pos_consequence", []))
    rubric_neg_consequence = set(RUBRIC_NEG_CONSEQUENCE) | set(lex_overrides.get("rubric_neg_consequence", []))
    rubric_pos_frame = set(RUBRIC_POS_FRAME)
    rubric_neg_frame = set(RUBRIC_NEG_FRAME)

    ideol_meta = _term_meta_map(
        dict_dir / "ideological_markers.csv",
        ["term", "marker", "lemma"],
        ["lemma", "category", "semantic_zone", "strength_hint", "context_dependent", "verified"],
    )
    emo_meta = _term_meta_map(
        dict_dir / "emotional_markers.csv",
        ["term", "marker", "lemma"],
        ["lemma", "emotion_type", "intensity_level", "weight", "context_dependent", "verified"],
    )
    meta_meta = _term_meta_map(
        dict_dir / "metaphor_markers.csv",
        ["term", "marker", "lemma"],
        ["lemma", "metaphor_model", "default_strength", "context_dependent", "verified"],
    )
    evi_meta = _term_meta_map(
        dict_dir / "evi_lexicon.csv",
        ["term", "marker", "lemma"],
        ["lemma", "category", "strength", "context_dependent", "verified", "polarity"],
    )

    manual_evi = {}
    if evi_manual_path and evi_manual_path.exists():
        mdf = pd.read_csv(evi_manual_path).fillna("")
        for _, r in mdf.iterrows():
            key = (str(r.get("context_id", "")), str(r.get("ref_country", "")))
            try:
                raw_val = int(float(r.get("EVI_raw", r.get("EVI", 0))))
            except Exception:
                continue
            sal_raw = r.get("referent_salience", "")
            sal_val = None
            if str(sal_raw).strip() != "":
                try:
                    sv = float(sal_raw)
                    if sv in SALIENCE_ALLOWED:
                        sal_val = sv
                except Exception:
                    sal_val = None
            expl = str(r.get("evi_explanation", r.get("explanation", ""))).strip()
            if -10 <= raw_val <= 10:
                manual_evi[key] = {
                    "evi_raw": raw_val,
                    "salience": sal_val,
                    "evi_explanation": expl if expl else "Manual EVI annotation",
                }

    metaphor_review = {}
    if metaphor_review_path and metaphor_review_path.exists():
        rdf = pd.read_csv(metaphor_review_path).fillna("")
        for _, r in rdf.iterrows():
            key = (str(r.get("context_id", "")), str(r.get("ref_country", "")), str(r.get("marker", "")).strip())
            is_met = str(r.get("is_metaphor", "")).strip().lower() in {"1", "true", "yes", "y"}
            metaphor_review[key] = is_met

    out_rows = []
    marker_traces: List[MarkerTrace] = []
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
        context_marker_ids: List[str] = []

        for t in found_ideol:
            tr = _build_marker_trace(
                context_id=context_id,
                ref_country=ref_country,
                indicator="IDI",
                term=t,
                context_text=ctx,
                dictionary_source="ideological_markers.csv",
                meta=ideol_meta.get(t.casefold(), {}),
                inclusion_reason="Matched ideological marker in referent-bound context.",
            )
            marker_traces.append(tr)
            context_marker_ids.append(tr.marker_id)

        for t in found_w:
            tr = _build_marker_trace(
                context_id=context_id,
                ref_country=ref_country,
                indicator="EMI",
                term=t,
                context_text=ctx,
                dictionary_source="emotional_markers.csv",
                meta=emo_meta.get(t.casefold(), {}),
                inclusion_reason="Matched weak emotional marker (weight 1/3).",
            )
            marker_traces.append(tr)
            context_marker_ids.append(tr.marker_id)
        for t in found_m:
            tr = _build_marker_trace(
                context_id=context_id,
                ref_country=ref_country,
                indicator="EMI",
                term=t,
                context_text=ctx,
                dictionary_source="emotional_markers.csv",
                meta=emo_meta.get(t.casefold(), {}),
                inclusion_reason="Matched medium emotional marker (weight 2/3).",
            )
            marker_traces.append(tr)
            context_marker_ids.append(tr.marker_id)
        for t in found_s:
            tr = _build_marker_trace(
                context_id=context_id,
                ref_country=ref_country,
                indicator="EMI",
                term=t,
                context_text=ctx,
                dictionary_source="emotional_markers.csv",
                meta=emo_meta.get(t.casefold(), {}),
                inclusion_reason="Matched strong emotional marker (weight 1).",
            )
            marker_traces.append(tr)
            context_marker_ids.append(tr.marker_id)
        # Semi-automatic metaphor handling
        n_met = 0
        if metaphor_review:
            for m in found_met_candidates:
                if metaphor_review.get((context_id, ref_country, m), False):
                    n_met += 1
                    tr = _build_marker_trace(
                        context_id=context_id,
                        ref_country=ref_country,
                        indicator="MTI",
                        term=m,
                        context_text=ctx,
                        dictionary_source="metaphor_markers.csv",
                        meta=meta_meta.get(m.casefold(), {}),
                        inclusion_reason="Confirmed metaphor in semi-automatic review.",
                    )
                    marker_traces.append(tr)
                    context_marker_ids.append(tr.marker_id)
                else:
                    tr = _build_marker_trace(
                        context_id=context_id,
                        ref_country=ref_country,
                        indicator="MTI",
                        term=m,
                        context_text=ctx,
                        dictionary_source="metaphor_markers.csv",
                        meta=meta_meta.get(m.casefold(), {}),
                        inclusion_reason="Candidate found.",
                        exclusion_reason="Rejected in metaphor_review.csv",
                    )
                    marker_traces.append(tr)
                    context_marker_ids.append(tr.marker_id)
            if found_met_candidates and n_met == 0:
                notes.append("metaphor_candidates_present_but_not_confirmed")
        else:
            n_met = n_met_candidates
            if n_met > 0:
                notes.append("metaphor_needs_manual_verification")
            for m in found_met_candidates:
                tr = _build_marker_trace(
                    context_id=context_id,
                    ref_country=ref_country,
                    indicator="MTI",
                    term=m,
                    context_text=ctx,
                    dictionary_source="metaphor_markers.csv",
                    meta=meta_meta.get(m.casefold(), {}),
                    inclusion_reason="Auto-included metaphor candidate; manual confirmation recommended.",
                )
                marker_traces.append(tr)
                context_marker_ids.append(tr.marker_id)

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
                rubric = calc_evi_rubric(
                    ref_country,
                    row,
                    rubric_pos_lex=rubric_pos_lex,
                    rubric_neg_lex=rubric_neg_lex,
                    rubric_pos_action=rubric_pos_action,
                    rubric_neg_action=rubric_neg_action,
                    rubric_pos_consequence=rubric_pos_consequence,
                    rubric_neg_consequence=rubric_neg_consequence,
                    rubric_pos_frame=rubric_pos_frame,
                    rubric_neg_frame=rubric_neg_frame,
                )
                rubric["evi_raw"] = int(manual_evi[key]["evi_raw"])
                rubric["evi_norm"] = float(rubric["evi_raw"] / 10.0)
                rubric["evi_explanation"] = manual_evi[key]["evi_explanation"]
                manual_sal = manual_evi[key]["salience"]
            else:
                rubric = {
                    "positive_score": 0,
                    "negative_score": 0,
                    "evi_raw": 0,
                    "evi_norm": 0.0,
                    "evi_evidence": "{}",
                    "evi_explanation": "Manual EVI missing -> default 0",
                    "positive_evidence_terms": "",
                    "negative_evidence_terms": "",
                    "evi_pos_hits": 0,
                    "evi_neg_hits": 0,
                }
                manual_sal = None
                notes.append("manual_evi_missing")
        elif evi_mode == "coarse":
            coarse_evi, coarse_expl = suggest_evi_coarse(
                str(row.get("target_sentence", ctx)),
                ref_kw[ref_country],
                rubric_pos_lex=rubric_pos_lex,
                rubric_neg_lex=rubric_neg_lex,
                rubric_pos_action=rubric_pos_action,
                rubric_neg_action=rubric_neg_action,
                rubric_pos_consequence=rubric_pos_consequence,
                rubric_neg_consequence=rubric_neg_consequence,
                rubric_pos_frame=rubric_pos_frame,
                rubric_neg_frame=rubric_neg_frame,
            )
            coarse_evi = int(clamp(coarse_evi, -2, 2))
            rubric = calc_evi_rubric(
                ref_country,
                row,
                rubric_pos_lex=rubric_pos_lex,
                rubric_neg_lex=rubric_neg_lex,
                rubric_pos_action=rubric_pos_action,
                rubric_neg_action=rubric_neg_action,
                rubric_pos_consequence=rubric_pos_consequence,
                rubric_neg_consequence=rubric_neg_consequence,
                rubric_pos_frame=rubric_pos_frame,
                rubric_neg_frame=rubric_neg_frame,
            )
            rubric["evi_raw"] = int(COARSE_TO_RAW.get(coarse_evi, 0))
            rubric["evi_norm"] = float(rubric["evi_raw"] / 10.0)
            rubric["evi_explanation"] = f"Coarse mode (legacy): {coarse_expl}; mapped to EVI={rubric['evi_raw']}"
            manual_sal = None
        else:
            rubric = calc_evi_rubric(
                ref_country,
                row,
                rubric_pos_lex=rubric_pos_lex,
                rubric_neg_lex=rubric_neg_lex,
                rubric_pos_action=rubric_pos_action,
                rubric_neg_action=rubric_neg_action,
                rubric_pos_consequence=rubric_pos_consequence,
                rubric_neg_consequence=rubric_neg_consequence,
                rubric_pos_frame=rubric_pos_frame,
                rubric_neg_frame=rubric_neg_frame,
            )
            if evi_mode == "suggested":
                rubric["evi_explanation"] = "Suggested mode: " + rubric["evi_explanation"]
            manual_sal = None

        salience, sal_label, is_technical, technical_reason, sal_expl = calc_referent_salience(
            ref_country=ref_country,
            context_text=ctx,
            title=str(row.get("title", "")),
            target_sentence=str(row.get("target_sentence", "")),
            positive_score=int(rubric["positive_score"]),
            negative_score=int(rubric["negative_score"]),
            matched_keywords=str(row.get("matched_keywords", "")),
            technical_patterns=list(lex_overrides.get("technical_patterns", [])),
        )
        if manual_sal is not None:
            salience = manual_sal
            is_technical = salience == 0.0
            sal_label = "technical" if is_technical else sal_label
            sal_expl = "Manual salience annotation."
            technical_reason = "Manual salience annotation." if is_technical else ""

        # EVI evidence markers traces
        for t in [x.strip() for x in str(rubric.get("positive_evidence_terms", "")).split(";") if x.strip()]:
            tr = _build_marker_trace(
                context_id=context_id,
                ref_country=ref_country,
                indicator="EVI",
                term=t,
                context_text=ctx,
                dictionary_source="evi_lexicon.csv",
                meta=evi_meta.get(t.casefold(), {}),
                inclusion_reason="Term contributes to positive EVI score.",
            )
            marker_traces.append(tr)
            context_marker_ids.append(tr.marker_id)
        for t in [x.strip() for x in str(rubric.get("negative_evidence_terms", "")).split(";") if x.strip()]:
            tr = _build_marker_trace(
                context_id=context_id,
                ref_country=ref_country,
                indicator="EVI",
                term=t,
                context_text=ctx,
                dictionary_source="evi_lexicon.csv",
                meta=evi_meta.get(t.casefold(), {}),
                inclusion_reason="Term contributes to negative EVI score.",
            )
            marker_traces.append(tr)
            context_marker_ids.append(tr.marker_id)

        if bool(salience == 0.0):
            tr = _build_marker_trace(
                context_id=context_id,
                ref_country=ref_country,
                indicator="S_r",
                term=str(technical_reason or "technical_mention"),
                context_text=ctx,
                dictionary_source="technical_mention_patterns.csv",
                meta={},
                inclusion_reason="Context classified as technical mention.",
            )
            marker_traces.append(tr)
            context_marker_ids.append(tr.marker_id)

        evi_raw = int(clamp(int(rubric["evi_raw"]), -10, 10))
        evi_norm = float(evi_raw / 10.0)
        evi = float(evi_raw)
        discursive_energy = float(idi + emi + mti)
        ip_context = float(evi_norm * (1.0 + discursive_energy))
        if evi_norm == 0.0:
            ip_context = 0.0
        ip_old_context = float(discursive_energy * evi_norm)
        aggregation_weight = float(clamp(salience, 0.0, 1.0))

        if (emi >= 0.12) and evi_raw == 0:
            notes.append("high_emi_neutral_evi")
        if (idi >= 0.12) and evi_raw == 0:
            notes.append("high_idi_neutral_evi")
        if n_content < 8:
            notes.append("low_n_content")
        if int(rubric["positive_score"]) > 0 and int(rubric["negative_score"]) > 0:
            notes.append("ambivalent_context")
        if abs(evi_raw) >= 7 and not (rubric["positive_evidence_terms"] or rubric["negative_evidence_terms"]):
            notes.append("high_evi_low_evidence")
        if salience == 0.0 and not is_technical:
            notes.append("salience_zero_but_not_technical")

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
                "EVI": float(evi),
                "EVI_raw": int(evi_raw),
                "EVI_norm": round(evi_norm, 6),
                "positive_score": int(rubric["positive_score"]),
                "negative_score": int(rubric["negative_score"]),
                "referent_salience": float(salience),
                "salience_label": sal_label,
                "is_technical_mention": bool(salience == 0.0),
                "salience_explanation": sal_expl,
                "technical_mention_reason": technical_reason,
                "discursive_energy": round(discursive_energy, 6),
                "IP_context": round(ip_context, 6),
                "IP_context_abs": round(abs(ip_context), 6),
                "IP_old_context": round(ip_old_context, 6),
                "aggregation_weight": round(aggregation_weight, 6),
                "IP_formula_version": "EVI_norm_times_1_plus_energy_weighted_by_salience",
                "IP": round(ip_context, 6),  # backward-compat alias
                "found_ideol_markers": "; ".join(found_ideol),
                "found_emotional_markers": "; ".join(sorted(set(found_w + found_m + found_s))),
                "found_metaphor_markers": "; ".join(found_met_candidates),
                "evi_pos_hits": int(rubric["evi_pos_hits"]),
                "evi_neg_hits": int(rubric["evi_neg_hits"]),
                "evi_score_raw": int(rubric["positive_score"] - rubric["negative_score"]),
                "evi_pos_markers": str(rubric["positive_evidence_terms"]),
                "evi_neg_markers": str(rubric["negative_evidence_terms"]),
                "evi_evidence": str(rubric["evi_evidence"]),
                "evi_explanation": str(rubric["evi_explanation"]),
                "positive_evidence_terms": str(rubric["positive_evidence_terms"]),
                "negative_evidence_terms": str(rubric["negative_evidence_terms"]),
                "explanation": str(rubric["evi_explanation"]),  # backward-compat
                "notes": "; ".join(sorted(set(notes))),
                "marker_trace_ids": "; ".join(context_marker_ids),
                "marker_counts_by_indicator": json.dumps(
                    {
                        "IDI": len(found_ideol),
                        "EMI_weak": len(found_w),
                        "EMI_medium": len(found_m),
                        "EMI_strong": len(found_s),
                        "MTI": len(found_met_candidates),
                        "EVI_pos_terms": len([x for x in str(rubric["positive_evidence_terms"]).split(";") if x.strip()]),
                        "EVI_neg_terms": len([x for x in str(rubric["negative_evidence_terms"]).split(";") if x.strip()]),
                        "S_r": 1 if salience == 0.0 else 0,
                    },
                    ensure_ascii=False,
                ),
            }
        )
        out_rows.append(out)
    out_df = pd.DataFrame(out_rows)
    out_df = compute_context_ip(out_df)
    if "IP_context" in out_df.columns:
        out_df["IP"] = out_df["IP_context"]
    if return_traces:
        traces_df = pd.DataFrame([t.to_dict() for t in marker_traces])
        return out_df, traces_df
    return out_df


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
        if -10 <= iv <= 10:
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


def compute_context_ip(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    col_map_candidates = {
        "IDI": ["IDI_i", "IDI_r", "IDI"],
        "EMI": ["EMI_i", "EMI_r", "EMI"],
        "MTI": ["MTI_i", "MTI_r", "MTI"],
        "EVI_norm": ["EVI_norm_i", "EVI_norm_r", "EVI_norm"],
        "S": ["S_i", "S_r", "referent_salience", "salience"],
    }

    def find_col(names: List[str]) -> str:
        for name in names:
            if name in df.columns:
                return name
        raise KeyError(f"Missing required column; expected one of: {names}")

    idi_col = find_col(col_map_candidates["IDI"])
    emi_col = find_col(col_map_candidates["EMI"])
    mti_col = find_col(col_map_candidates["MTI"])
    evi_col = find_col(col_map_candidates["EVI_norm"])
    s_col = find_col(col_map_candidates["S"])

    for col in [idi_col, emi_col, mti_col, evi_col, s_col]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["discursive_energy"] = df[idi_col] + df[emi_col] + df[mti_col]
    df["IP_context"] = df[evi_col] * (1.0 + df["discursive_energy"])
    df.loc[df[evi_col] == 0.0, "IP_context"] = 0.0
    df["IP_context_abs"] = df["IP_context"].abs()
    df["IP_old_context"] = df["discursive_energy"] * df[evi_col]
    df["aggregation_weight"] = df[s_col].clip(lower=0.0, upper=1.0)
    df.loc[df["aggregation_weight"] == 0.0, "IP_context"] = df.loc[df["aggregation_weight"] == 0.0, "IP_context"].fillna(0.0)
    df["IP_formula_version"] = "EVI_norm_times_1_plus_energy_weighted_by_salience"
    return df


def weighted_aggregate_ip(df: pd.DataFrame) -> dict:
    df = compute_context_ip(df)
    valid = df[df["aggregation_weight"] > 0].copy()
    if valid.empty or float(valid["aggregation_weight"].sum()) == 0.0:
        return {
            "IP_final": 0.0,
            "IP_abs_final": 0.0,
            "mean_IP_unweighted": 0.0,
            "contexts_analyzed": 0,
            "contexts_excluded": int(len(df)),
            "warning": "No substantive referent contexts after salience filtering",
        }
    w = valid["aggregation_weight"]
    ip = valid["IP_context"]
    return {
        "IP_final": float((ip * w).sum() / w.sum()),
        "IP_abs_final": float((ip.abs() * w).sum() / w.sum()),
        "mean_IP_unweighted": float(ip.mean()),
        "contexts_analyzed": int(len(valid)),
        "contexts_excluded": int((df["aggregation_weight"] == 0).sum()),
        "warning": None,
    }


def aggregate_outputs(df: pd.DataFrame, exclude_technical_mentions: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_all = compute_context_ip(df.copy())
    if exclude_technical_mentions and "is_technical_mention" in df_all.columns:
        df_all["aggregation_weight"] = df_all["aggregation_weight"].where(df_all["is_technical_mention"] != True, 0.0)
        technical_excluded_global = int(df_all["is_technical_mention"].fillna(False).astype(bool).sum())
    else:
        technical_excluded_global = 0

    def aggregate_group(keys: List[str]) -> pd.DataFrame:
        grouped = []
        all_keys = df_all[keys].drop_duplicates()
        for _, key_row in all_keys.iterrows():
            mask_all = pd.Series([True] * len(df_all))
            key_vals = []
            for k in keys:
                kv = key_row[k]
                key_vals.append(kv)
                mask_all = mask_all & (df_all[k] == kv)
            g_all = df_all[mask_all]
            key_vals = tuple(key_vals)
            row = {k: v for k, v in zip(keys, key_vals)}
            total_contexts = int(len(g_all))
            technical_count = int(g_all["is_technical_mention"].fillna(False).astype(bool).sum()) if not g_all.empty else 0
            agg = weighted_aggregate_ip(g_all)
            valid = g_all[g_all["aggregation_weight"] > 0].copy()
            excluded_contexts = int(agg["contexts_excluded"])

            if valid.empty:
                row.update(
                    {
                        "mean_IDI": 0.0,
                        "mean_EMI": 0.0,
                        "mean_MTI": 0.0,
                        "mean_EVI_raw": 0.0,
                        "mean_EVI_norm": 0.0,
                        "mean_IP": 0.0,
                        "mean_abs_IP": 0.0,
                        "mean_IP_unweighted": 0.0,
                        "positive_context_share": 0.0,
                        "negative_context_share": 0.0,
                        "neutral_context_share": 0.0,
                        "central_context_share": 0.0,
                        "background_context_share": 0.0,
                        "mean_referent_salience": 0.0,
                        "contexts_analyzed": int(agg["contexts_analyzed"]),
                        "contexts_excluded": int(agg["contexts_excluded"]),
                        "warning": str(agg["warning"] or ""),
                        "technical_mentions_count": technical_count,
                        "technical_mentions_excluded": technical_count if exclude_technical_mentions else 0,
                        "number_of_contexts": total_contexts,
                        "IP_final": 0.0,
                        "IP_abs_final": 0.0,
                    }
                )
                grouped.append(row)
                continue

            pos_share = float((valid["EVI_raw"] > 0).sum() / len(valid))
            neg_share = float((valid["EVI_raw"] < 0).sum() / len(valid))
            neu_share = float((valid["EVI_raw"] == 0).sum() / len(valid))
            central_share = float((valid["referent_salience"] == 1.0).sum() / len(valid))
            background_share = float((valid["referent_salience"] == 0.25).sum() / len(valid))
            row.update(
                {
                    "mean_IDI": float(valid["IDI"].mean()),
                    "mean_EMI": float(valid["EMI"].mean()),
                    "mean_MTI": float(valid["MTI"].mean()),
                    "mean_EVI_raw": float(valid["EVI_raw"].mean()),
                    "mean_EVI_norm": float(valid["EVI_norm"].mean()),
                    "EVI": int(dominant_evi(valid["EVI"])),
                    "mean_IP": float(agg["IP_final"]),
                    "mean_abs_IP": float(agg["IP_abs_final"]),
                    "mean_IP_unweighted": float(agg["mean_IP_unweighted"]),
                    "positive_context_share": pos_share,
                    "negative_context_share": neg_share,
                    "neutral_context_share": neu_share,
                    "central_context_share": central_share,
                    "background_context_share": background_share,
                    "mean_referent_salience": float(valid["referent_salience"].mean()),
                    "contexts_analyzed": int(agg["contexts_analyzed"]),
                    "contexts_excluded": int(agg["contexts_excluded"]),
                    "warning": str(agg["warning"] or ""),
                    "technical_mentions_count": technical_count,
                    "technical_mentions_excluded": technical_count if exclude_technical_mentions else 0,
                    "EVI": int(dominant_evi(valid["EVI"])),
                    "IP": float(agg["IP_final"]),  # backward-compat alias
                    "IP_final": float(agg["IP_final"]),
                    "IP_abs_final": float(agg["IP_abs_final"]),
                    "number_of_contexts": total_contexts,
                }
            )
            grouped.append(row)
        return pd.DataFrame(grouped)

    by_article = aggregate_group(["doc_id", "ref_country", "media_country", "outlet_name"])
    by_outlet = aggregate_group(["outlet_name", "media_country", "ref_country"])
    by_media_ref = aggregate_group(["media_country", "ref_country"])

    df_effective = df_all[df_all["aggregation_weight"] > 0].copy()
    art_counts = (
        df_effective.groupby(["media_country", "ref_country"])["doc_id"]
        .nunique()
        .reset_index(name="number_of_articles")
    )
    by_media_ref = by_media_ref.merge(art_counts, on=["media_country", "ref_country"], how="left")
    by_media_ref["number_of_articles"] = by_media_ref["number_of_articles"].fillna(0).astype(int)
    by_outlet_articles = df_effective.groupby(["outlet_name", "media_country", "ref_country"])["doc_id"].nunique().reset_index(name="number_of_articles")
    by_outlet = by_outlet.merge(by_outlet_articles, on=["outlet_name", "media_country", "ref_country"], how="left")
    by_outlet["number_of_articles"] = by_outlet["number_of_articles"].fillna(0).astype(int)
    by_article["number_of_articles"] = 1

    return by_article, by_outlet, by_media_ref, build_summary_matrix(by_media_ref)


def build_summary_matrix(by_media_ref: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "media_country",
        "ref_country",
        "mean_IDI",
        "mean_EMI",
        "mean_MTI",
        "mean_EVI_raw",
        "mean_EVI_norm",
        "IP_final",
        "mean_abs_IP",
        "mean_IP_unweighted",
        "positive_context_share",
        "negative_context_share",
        "neutral_context_share",
        "central_context_share",
        "background_context_share",
        "technical_mentions_excluded",
        "contexts_analyzed",
        "contexts_excluded",
        "warning",
        "number_of_contexts",
        "number_of_articles",
    ]
    out = by_media_ref.copy()
    if "IP_final" not in out.columns and "mean_IP" in out.columns:
        out["IP_final"] = out["mean_IP"]
    for c in cols:
        if c not in out.columns:
            out[c] = 0
    return out[cols].sort_values(["media_country", "ref_country"]).reset_index(drop=True)


def build_flagged_cases(df: pd.DataFrame) -> pd.DataFrame:
    flagged = []
    for _, r in df.iterrows():
        reasons = set()
        notes = str(r.get("notes", ""))
        if notes:
            reasons.update([x.strip() for x in notes.split(";") if x.strip()])
        if bool(r.get("is_technical_mention", False)):
            reasons.add("technical_mentions_detected")
        if bool(r.get("multi_country_context", False)):
            reasons.add("multi_country_contexts")
        if float(r.get("IDI", 0)) >= 0.12 and int(r.get("EVI_raw", 0)) == 0:
            reasons.add("high_IDI_neutral_EVI")
        if float(r.get("EMI", 0)) >= 0.12 and int(r.get("EVI_raw", 0)) == 0:
            reasons.add("high_EMI_neutral_EVI")
        if int(r.get("positive_score", 0)) > 0 and int(r.get("negative_score", 0)) > 0:
            reasons.add("ambivalent_contexts")
        if abs(int(r.get("EVI_raw", 0))) >= 7 and not (str(r.get("positive_evidence_terms", "")).strip() or str(r.get("negative_evidence_terms", "")).strip()):
            reasons.add("high_EVI_low_evidence")
        if int(r.get("N_content", 0)) < 8:
            reasons.add("low_N_content")
        if float(r.get("IP", 0)) < -6 or float(r.get("IP", 0)) > 6:
            reasons.add("ip_out_of_expected_range")
        if not (-10 <= int(r.get("EVI_raw", 0)) <= 10):
            reasons.add("invalid_evi_raw")
        if abs(float(r.get("EVI_norm", 0)) - float(int(r.get("EVI_raw", 0)) / 10.0)) > 1e-9:
            reasons.add("invalid_evi_norm")
        sal = float(r.get("referent_salience", -1))
        if not (0.0 <= sal <= 1.0):
            reasons.add("invalid_referent_salience")
        if sal not in SALIENCE_ALLOWED:
            reasons.add("invalid_referent_salience_set")
        if sal == 0.0 and not bool(r.get("is_technical_mention", False)):
            reasons.add("salience_zero_not_technical")
        if sal == 0.0 and float(r.get("aggregation_weight", -1)) != 0.0:
            reasons.add("salience_zero_weight_mismatch")
        if float(r.get("EVI_norm", 0.0)) == 0.0 and abs(float(r.get("IP_context", 0.0))) > 1e-9:
            reasons.add("evi_zero_ip_nonzero")
        if reasons:
            base = r.to_dict()
            for rr in sorted(reasons):
                out = dict(base)
                out["flag_case_type"] = rr
                flagged.append(out)
    return pd.DataFrame(flagged)


def validate_lexicons(dict_dir: Path) -> Tuple[pd.DataFrame, str]:
    ensure_project_lexicon_schema(dict_dir)
    issues: List[Dict[str, object]] = []
    for fname, cols in LEXICON_SCHEMAS.items():
        path = dict_dir / fname
        try:
            df = pd.read_csv(path).fillna("")
        except Exception as exc:
            issues.append({"file": fname, "level": "error", "issue": "read_error", "details": str(exc)})
            continue
        for c in cols:
            if c not in df.columns:
                issues.append({"file": fname, "level": "error", "issue": "missing_column", "details": c})
        if "verified" in df.columns and not df.empty:
            allowed_verified = {"true", "false", "context_dependent"}
            for i, v in enumerate(df["verified"].tolist()):
                sv = str(v).strip().casefold()
                if sv in {"1", "yes"}:
                    sv = "true"
                elif sv in {"0", "no"}:
                    sv = "false"
                elif sv == "":
                    issues.append({
                        "file": fname,
                        "level": "warning",
                        "issue": "empty_verified",
                        "details": f"row {i+1}: empty verified value",
                    })
                    continue
                if sv not in allowed_verified:
                    issues.append({
                        "file": fname,
                        "level": "error",
                        "issue": "invalid_verified_value",
                        "details": f"row {i+1}: {v}",
                    })
        if fname == "emotional_markers.csv" and not df.empty:
            for i, r in df.iterrows():
                lvl = str(r.get("intensity_level", "")).strip().lower()
                w = str(r.get("weight", "")).strip()
                if lvl not in {"weak", "medium", "strong"}:
                    issues.append({"file": fname, "level": "error", "issue": "invalid_intensity_level", "details": f"row {i+1}: {lvl}"})
                try:
                    wf = float(w) if w != "" else 0.0
                except Exception:
                    wf = -1.0
                expected = {"weak": 1.0 / 3.0, "medium": 2.0 / 3.0, "strong": 1.0}.get(lvl, None)
                if expected is not None and abs(wf - expected) > 1e-3:
                    issues.append({"file": fname, "level": "warning", "issue": "weight_mismatch", "details": f"row {i+1}: {wf} != {expected:.6f}"})
        if fname == "technical_mention_patterns.csv" and not df.empty:
            bad = df[pd.to_numeric(df.get("salience_value", 0), errors="coerce").fillna(-1) != 0]
            if not bad.empty:
                issues.append({"file": fname, "level": "error", "issue": "technical_salience_must_be_zero", "details": f"rows={len(bad)}"})

    report_df = pd.DataFrame(issues, columns=["file", "level", "issue", "details"])
    md_lines = ["# lexicon_quality_report", ""]
    if report_df.empty:
        md_lines.append("- status: OK")
        md_lines.append("- notes: no schema violations detected.")
    else:
        md_lines.append(f"- total_issues: {len(report_df)}")
        for _, r in report_df.iterrows():
            md_lines.append(f"- [{r['level']}] {r['file']}: {r['issue']} ({r['details']})")
    return report_df, "\n".join(md_lines)


def extract_candidate_terms(calibration_contexts: pd.DataFrame, lex_dir: Path) -> pd.DataFrame:
    return lex_extract_candidate_terms(calibration_contexts, lex_dir)


def score_candidate_term(term: str, frequency: int = 1) -> float:
    return lex_score_candidate_term(term, frequency)


def suggest_dictionary(term: str) -> str:
    return lex_suggest_dictionary(term)


def approve_candidate(term_id: str, lex_dir: Path) -> Optional[Dict[str, object]]:
    return lex_approve_candidate(term_id, lex_dir)


def reject_candidate(term_id: str, lex_dir: Path, reason: str = "not_relevant") -> Optional[Dict[str, object]]:
    return lex_reject_candidate(term_id, lex_dir, reason=reason)


def mark_context_dependent(term_id: str, lex_dir: Path) -> None:
    lex_mark_context_dependent(term_id, lex_dir)


def write_dictionary_change_log(lex_dir: Path, action: str, term: str, lemma: str = "", dictionary: str = "", category: str = "", status: str = "", details: str = "") -> None:
    lex_write_dictionary_change_log(lex_dir, action, term, lemma=lemma, dictionary=dictionary, category=category, status=status, details=details)


def save_outputs(
    contexts_full: pd.DataFrame,
    by_article: pd.DataFrame,
    by_outlet: pd.DataFrame,
    by_media_ref: pd.DataFrame,
    summary_matrix: pd.DataFrame,
    flagged: pd.DataFrame,
    output_dir: Path,
    marker_traces: Optional[pd.DataFrame] = None,
    lexicon_quality_report: Optional[pd.DataFrame] = None,
    lexicon_quality_md: str = "",
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    contexts_full.to_csv(output_dir / "contexts_full.csv", index=False)
    by_article.to_csv(output_dir / "aggregated_by_article.csv", index=False)
    by_outlet.to_csv(output_dir / "aggregated_by_outlet.csv", index=False)
    by_media_ref.to_csv(output_dir / "aggregated_by_media_country_and_ref_country.csv", index=False)
    flagged.to_csv(output_dir / "flagged_cases.csv", index=False)
    if marker_traces is not None and not marker_traces.empty:
        marker_traces.to_csv(output_dir / "marker_traces.csv", index=False)
        marker_traces.to_json(output_dir / "marker_traces.json", orient="records", force_ascii=False, indent=2)
        marker_traces.to_excel(output_dir / "marker_traces.xlsx", index=False)
    if lexicon_quality_report is not None:
        lexicon_quality_report.to_csv(output_dir / "lexicon_quality_report.csv", index=False)
        (output_dir / "lexicon_quality_report.md").write_text(lexicon_quality_md or "# lexicon_quality_report\n", encoding="utf-8")
    # Persist dictionary workflow log if available near lexicons.
    # best effort: infer from cwd-level lexicons
    for cand in [Path("lexicons"), output_dir / "referent_dicts"]:
        ch = cand / "dictionary_change_log.csv"
        if ch.exists():
            try:
                pd.read_csv(ch).to_csv(output_dir / "dictionary_change_log.csv", index=False)
            except Exception:
                (output_dir / "dictionary_change_log.csv").write_text(ch.read_text(encoding="utf-8"), encoding="utf-8")
            break

    with pd.ExcelWriter(output_dir / "summary_matrix.xlsx", engine="openpyxl") as xw:
        summary_matrix.to_excel(xw, index=False, sheet_name="summary_matrix")
        by_media_ref.to_excel(xw, index=False, sheet_name="long_table")
        contexts_full.to_excel(xw, index=False, sheet_name="contexts_full")
        flagged.to_excel(xw, index=False, sheet_name="flagged_cases")
        if marker_traces is not None and not marker_traces.empty:
            marker_traces.to_excel(xw, index=False, sheet_name="marker_traces")
        if lexicon_quality_report is not None and not lexicon_quality_report.empty:
            lexicon_quality_report.to_excel(xw, index=False, sheet_name="lexicon_quality")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Referent-context analyzer (China/USA/Russia)")
    p.add_argument("--input", required=True, help="Input CSV/XLSX/JSON path")
    p.add_argument("--output-dir", required=True, help="Output directory")
    p.add_argument("--dict-dir", default="referent_dicts", help="Directory for editable marker dictionaries")
    p.add_argument(
        "--evi-mode",
        default="fine",
        choices=["coarse", "fine", "suggested", "manual"],
        help="fine/suggested/manual use EVI scale -10..10; coarse is legacy fallback.",
    )
    p.add_argument(
        "--evi-manual",
        default="",
        help="Optional CSV with context_id,ref_country,EVI_raw,referent_salience,evi_explanation",
    )
    p.add_argument("--metaphor-review", default="", help="Optional CSV with context_id,ref_country,marker,is_metaphor")
    p.add_argument(
        "--exclude-technical-mentions",
        default="true",
        choices=["true", "false"],
        help="When true, technical mentions (salience=0) are excluded from aggregation.",
    )
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

    scored_payload = apply_metrics(
        contexts=contexts,
        dict_dir=dict_dir,
        evi_mode=args.evi_mode,
        evi_manual_path=Path(args.evi_manual) if args.evi_manual else None,
        metaphor_review_path=Path(args.metaphor_review) if args.metaphor_review else None,
        return_traces=True,
    )
    if isinstance(scored_payload, tuple):
        scored, marker_traces = scored_payload
    else:
        scored = scored_payload
        marker_traces = pd.DataFrame()
    scored = add_multicountry_flags(scored)
    scored = scored[scored["ref_country"].isin(REF_COUNTRIES)].copy()

    # Mandatory QA constraints
    scored.loc[(scored["N_content"] <= 0), ["IDI", "EMI", "MTI", "IP"]] = 0.0
    scored.loc[(~scored["EVI_raw"].between(-10, 10)), "EVI"] = 0
    scored.loc[(~scored["EVI_raw"].between(-10, 10)), ["EVI_raw", "EVI_norm", "IP"]] = [0, 0.0, 0.0]
    scored.loc[(~scored["referent_salience"].between(0.0, 1.0)), ["referent_salience", "IP"]] = [0.0, 0.0]
    scored.loc[(scored["referent_salience"] == 0), "is_technical_mention"] = True
    scored.loc[(scored["referent_salience"] == 0), ["IP", "aggregation_weight"]] = [0.0, 0.0]
    scored.loc[(scored["EVI_raw"] == 0), "IP"] = 0.0
    for c in ["IDI", "EMI", "MTI"]:
        scored[c] = scored[c].clip(lower=0.0, upper=1.0)
    scored["EVI_norm"] = scored["EVI_raw"] / 10.0
    scored = compute_context_ip(scored)
    scored["IP"] = scored["IP_context"]

    exclude_technical = str(args.exclude_technical_mentions).lower() == "true"
    by_article, by_outlet, by_media_ref, matrix = aggregate_outputs(scored, exclude_technical_mentions=exclude_technical)
    flagged = build_flagged_cases(scored)
    qdf, qmd = validate_lexicons(dict_dir)
    save_outputs(
        scored,
        by_article,
        by_outlet,
        by_media_ref,
        matrix,
        flagged,
        output_dir,
        marker_traces=marker_traces,
        lexicon_quality_report=qdf,
        lexicon_quality_md=qmd,
    )

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
