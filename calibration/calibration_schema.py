from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Literal, Optional

CalibrationMode = Literal["rss", "url_list", "local", "manual"]
CalibrationType = Literal[
    "neutral_news",
    "standard_political_news",
    "analytical_article",
    "crisis_report",
    "opinion_editorial",
    "ideological_mobilization",
    "propaganda_like_text",
    "highly_emotional_text",
    "highly_metaphorical_text",
]
ExpectedIndicatorFocus = Literal["IDI", "EMI", "MTI", "EVI", "IP", "neutral_baseline", "mixed"]
AnchorStatus = Literal["none", "lower_anchor", "middle_anchor", "upper_anchor", "extreme_anchor"]


@dataclass
class CalibrationSource:
    source_name: str
    mode: CalibrationMode
    language: str = "en"
    calibration_type: str = "standard_political_news"
    expected_indicator_focus: str = "mixed"
    path: Optional[str] = None
    rss_url: Optional[str] = None
    url_csv_path: Optional[str] = None
    enabled: bool = True
    rate_limit_seconds: float = 1.0
    notes: str = ""

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "CalibrationSource":
        return cls(
            source_name=str(payload.get("source_name", "Unnamed source")),
            mode=str(payload.get("mode", "local")),
            language=str(payload.get("language", "en")),
            calibration_type=str(payload.get("calibration_type", "standard_political_news")),
            expected_indicator_focus=str(payload.get("expected_indicator_focus", "mixed")),
            path=payload.get("path"),
            rss_url=payload.get("rss_url"),
            url_csv_path=payload.get("url_csv_path"),
            enabled=bool(payload.get("enabled", True)),
            rate_limit_seconds=float(payload.get("rate_limit_seconds", 1.0)),
            notes=str(payload.get("notes", "")),
        )


@dataclass
class CalibrationRecord:
    calibration_id: str
    source_name: str
    source_url: str
    fetch_mode: str
    fetch_status: str
    publication_date: str
    collected_at: str
    title: str
    author: str
    outlet: str
    language: str
    country_context: str
    calibration_type: str
    expected_indicator_focus: str
    ref_country: str
    text: str
    text_length_words: int
    notes: str = ""
    anchor_status: str = "none"
    quality_flags: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CalibrationContextRecord:
    context_id: str
    calibration_id: str
    source_name: str
    title: str
    calibration_type: str
    expected_indicator_focus: str
    ref_country: str
    matched_keywords: str
    context_text: str
    previous_sentence: str
    target_sentence: str
    next_sentence: str
    N_content: int
    IDI_raw: float
    IDI_percent: float
    EMI_raw: float
    EMI_percent: float
    MTI_raw: float
    MTI_percent: float
    EVI_raw: float
    EVI_norm: float
    S_r: float
    salience_label: str
    IP_context: float
    IP_abs_context: float
    quality_flags: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CalibrationRunArtifacts:
    texts_df: Any
    contexts_df: Any
    distributions_df: Any
    quality_flags_df: Any
    candidate_terms_df: Any
    report_markdown: str
    metadata: Dict[str, Any] = field(default_factory=dict)
