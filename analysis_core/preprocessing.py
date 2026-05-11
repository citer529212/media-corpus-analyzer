from __future__ import annotations

import re
from typing import Iterable, List, Sequence, Tuple

TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁёІіЇїЄє'\-]+")
SENT_SPLIT_RE = re.compile(r"(?<=[\.\!\?])\s+")

# Minimal cross-lingual functional set (EN, ID/MS, RU)
STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "at", "by", "from", "is", "are", "was",
    "were", "be", "been", "being", "as", "that", "this", "these", "those", "it", "its", "their", "his", "her", "they",
    "them", "he", "she", "we", "you", "i", "not", "but", "if", "then", "than", "into", "about", "after", "before",
    "during", "up", "down", "out", "over", "have", "has", "had", "do", "does", "did", "can", "could", "will", "would",
    "may", "might", "must", "should", "am",
    "dan", "atau", "yang", "di", "ke", "dari", "untuk", "dengan", "pada", "adalah", "itu", "ini", "dalam", "oleh",
    "sebagai", "juga", "akan", "telah", "lebih", "karena", "serta", "tidak", "bagi", "antara", "agar", "namun", "ia",
    "mereka", "kami", "kita", "saya", "anda", "usai", "jadi", "sebut", "kata", "menurut", "tersebut", "hingga", "kepada",
    "dapat", "boleh", "perlu", "harus", "masih", "atas", "setelah", "sebelum", "ketika", "bukan",
    "и", "а", "но", "или", "в", "во", "на", "по", "с", "со", "к", "ко", "из", "за", "для", "о", "об", "от", "до",
    "это", "как", "что", "бы", "же", "ли", "не", "ни", "он", "она", "они", "мы", "вы", "я", "его", "ее", "их",
}


def normalize_token(s: str) -> str:
    return s.casefold().strip("-'_ ")


def sentence_split(text: str, language: str | None = None) -> List[str]:
    if not text or not str(text).strip():
        return []
    return [s.strip() for s in SENT_SPLIT_RE.split(str(text)) if s.strip()]


def tokenize(text: str, language: str | None = None) -> List[str]:
    return [normalize_token(t) for t in TOKEN_RE.findall(str(text)) if normalize_token(t)]


def lemmatize(tokens: Sequence[str], language: str | None = None) -> List[str]:
    # Lightweight deployment-safe fallback: return normalized tokens.
    return [normalize_token(t) for t in tokens if normalize_token(t)]


def pos_tag(tokens: Sequence[str], language: str | None = None) -> List[Tuple[str, str]]:
    # Heuristic POS tags to avoid heavyweight runtime deps in Streamlit Cloud.
    out: List[Tuple[str, str]] = []
    for t in tokens:
        tok = normalize_token(t)
        if not tok:
            continue
        if tok in STOPWORDS:
            out.append((tok, "FUNC"))
            continue
        if tok.endswith(("ly", "но", "ly", "nya")):
            out.append((tok, "ADV"))
        elif tok.endswith(("ing", "ed", "ize", "ise", "kan", "ть", "ти", "лся", "лась")):
            out.append((tok, "VERB"))
        elif tok.endswith(("ous", "ive", "al", "ic", "ный", "ская", "ской", "ший", "est")):
            out.append((tok, "ADJ"))
        else:
            out.append((tok, "NOUN"))
    return out


def count_content_words(context: str | Iterable[str]) -> int:
    if isinstance(context, str):
        toks = tokenize(context)
    else:
        toks = [normalize_token(t) for t in context if normalize_token(str(t))]
    tagged = pos_tag(toks)
    return sum(1 for tok, pos in tagged if pos in {"NOUN", "VERB", "ADJ", "ADV"} and tok not in STOPWORDS and len(tok) >= 2)
