#!/usr/bin/env python3
"""Stage-based corpus analyzer aligned with dissertation methodology.

Method stages:
1) Corpus loading and preprocessing
2) Quantitative corpus analysis (frequency, KWIC, collocations, keywords)
3) Qualitative linguopragmatic proxies (sentiment, framing, persuasion tactics)
4) Prognostic layer (time trends)
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import re
from itertools import combinations
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Dict, Iterable, List, Tuple

TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁёІіЇїЄє'\-]+")
US_RE = re.compile(r"\bu\.\s*s\.?\s*a?\.?\b", flags=re.IGNORECASE)

STOPWORDS = {
    # English
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "at", "by", "from", "is", "are", "was", "were",
    "be", "been", "being", "as", "that", "this", "these", "those", "it", "its", "their", "his", "her", "they", "them", "he", "she",
    "we", "you", "i", "not", "but", "if", "then", "than", "into", "about", "after", "before", "during", "up", "down", "out", "over",
    "have", "has", "had", "do", "does", "did", "can", "could", "will", "would", "may", "might", "must", "should",
    # Indonesian/Malay
    "dan", "atau", "yang", "di", "ke", "dari", "untuk", "dengan", "pada", "adalah", "itu", "ini", "dalam", "oleh", "sebagai", "juga",
    "akan", "telah", "lebih", "karena", "serta", "tidak", "bagi", "antara", "agar", "namun", "ia", "mereka", "kami", "kita", "saya",
    "anda", "usai", "jadi", "sebut", "kata", "menurut", "tersebut", "hingga", "kepada", "dapat", "boleh", "perlu", "harus", "masih",
    "dalam", "atas", "setelah", "sebelum", "ketika", "bukan", "akan", "telah", "sebagai", "namun", "yakni", "untuk",
    # boilerplate/newsroom frequent
    "reuters", "afp", "ap", "kompas", "tempo", "antara", "bernama", "thejakartapost", "thestar",
    "share", "facebook", "twitter", "whatsapp", "telegram", "instagram", "youtube", "follow", "subscribe", "channel",
    "comment", "comments", "video", "photo", "breaking", "news", "read", "also", "more",
    "our", "please", "email", "valid", "click", "log", "sign", "newsletter",
}

COUNTRY_TERMS = {
    "usa": [
        "usa", "us", "u.s", "united", "states", "america", "american", "washington", "amerika", "serikat", "syarikat",
        "white", "house", "pentagon", "biden", "trump", "congress", "senate", "сша", "америка", "вашингтон",
    ],
    "russia": [
        "russia", "russian", "rusia", "moscow", "kremlin", "putin", "lavrov", "россия", "москва", "кремль", "путин",
    ],
    "china": [
        "china", "chinese", "cina", "tiongkok", "beijing", "xi", "jinping", "ccp", "cpc", "prc",
        "yuan", "taiwan", "xinjiang", "китай", "кпк", "пекин", "юань",
    ],
}

SOURCE_LANG_DEFAULT = {
    "Antara": "id",
    "Kompas Indonesia": "id",
    "Tempo": "id",
    "The Jakarta Post": "en",
    "Astro Awani": "ms",
    "Bernama": "ms",
    "The Star": "en",
    "The Edge Malaysia": "en",
}

LANG_HINTS = {
    "en": {"the", "and", "of", "to", "in", "for", "with", "on", "is", "are", "was", "were", "said", "government"},
    "id": {"dan", "yang", "di", "ke", "dari", "untuk", "dengan", "adalah", "tidak", "akan", "menurut", "pemerintah"},
    "ms": {"dan", "yang", "di", "ke", "dari", "untuk", "dengan", "adalah", "tidak", "akan", "kerajaan", "menteri", "syarikat"},
    "ru": {"и", "в", "на", "с", "по", "для", "что", "это", "как", "был", "были", "заявил", "правительство", "президент"},
}

FRAME_LEXICONS = {
    "security_threat": {"threat", "risk", "attack", "war", "conflict", "ancaman", "risiko", "serangan", "perang", "konflik"},
    "economy_trade": {"trade", "tariff", "market", "investment", "economy", "dagang", "tarif", "pasar", "investasi", "ekonomi"},
    "diplomacy_partnership": {"cooperation", "partnership", "dialogue", "agreement", "kerja", "sama", "kemitraan", "dialog", "kesepakatan"},
    "governance_values": {"democracy", "sovereignty", "stability", "law", "demokrasi", "kedaulatan", "stabilitas", "hukum"},
}

SENT_POS = {
    "cooperation", "partnership", "stability", "growth", "peace", "agreement", "support", "progress", "benefit",
    "kerja", "sama", "stabil", "pertumbuhan", "damai", "kesepakatan", "dukungan", "kemajuan", "manfaat",
}

SENT_NEG = {
    "threat", "crisis", "conflict", "war", "attack", "sanction", "decline", "risk", "tension", "failure", "violence",
    "ancaman", "krisis", "konflik", "perang", "serangan", "sanksi", "penurunan", "risiko", "ketegangan", "gagal", "kekerasan",
}

SENT_POS_BY_LANG = {
    "en": {"cooperation", "partnership", "stability", "growth", "peace", "agreement", "support", "progress", "benefit"},
    "id": {"kerja", "sama", "stabil", "pertumbuhan", "damai", "kesepakatan", "dukungan", "kemajuan", "manfaat"},
    "ms": {"kerja", "sama", "stabil", "pertumbuhan", "damai", "persetujuan", "sokongan", "kemajuan", "manfaat"},
    "ru": {"сотрудничество", "партнерство", "стабильность", "рост", "мир", "соглашение", "поддержка", "прогресс", "выгода"},
}

SENT_NEG_BY_LANG = {
    "en": {"threat", "crisis", "conflict", "war", "attack", "sanction", "decline", "risk", "tension", "failure", "violence"},
    "id": {"ancaman", "krisis", "konflik", "perang", "serangan", "sanksi", "penurunan", "risiko", "ketegangan", "gagal", "kekerasan"},
    "ms": {"ancaman", "krisis", "konflik", "perang", "serangan", "sekatan", "penurunan", "risiko", "ketegangan", "gagal", "keganasan"},
    "ru": {"угроза", "кризис", "конфликт", "война", "атака", "санкции", "спад", "риск", "напряженность", "провал", "насилие"},
}

FRAME_LEXICONS_BY_LANG = {
    "en": {
        "security_threat": {"threat", "risk", "attack", "war", "conflict"},
        "economy_trade": {"trade", "tariff", "market", "investment", "economy"},
        "diplomacy_partnership": {"cooperation", "partnership", "dialogue", "agreement"},
        "governance_values": {"democracy", "sovereignty", "stability", "law"},
    },
    "id": {
        "security_threat": {"ancaman", "risiko", "serangan", "perang", "konflik"},
        "economy_trade": {"dagang", "tarif", "pasar", "investasi", "ekonomi"},
        "diplomacy_partnership": {"kerja", "sama", "kemitraan", "dialog", "kesepakatan"},
        "governance_values": {"demokrasi", "kedaulatan", "stabilitas", "hukum"},
    },
    "ms": {
        "security_threat": {"ancaman", "risiko", "serangan", "perang", "konflik"},
        "economy_trade": {"dagangan", "tarif", "pasaran", "pelaburan", "ekonomi"},
        "diplomacy_partnership": {"kerjasama", "kemitraan", "dialog", "persetujuan", "kerja", "sama"},
        "governance_values": {"demokrasi", "kedaulatan", "kestabilan", "undang"},
    },
    "ru": {
        "security_threat": {"угроза", "риск", "атака", "война", "конфликт"},
        "economy_trade": {"торговля", "тариф", "рынок", "инвестиции", "экономика"},
        "diplomacy_partnership": {"сотрудничество", "партнерство", "диалог", "соглашение"},
        "governance_values": {"демократия", "суверенитет", "стабильность", "закон"},
    },
}

PERSUASION_MARKERS = {
    "modality_obligation": ["must", "should", "need", "harus", "wajib", "perlu"],
    "modality_possibility": ["can", "could", "may", "might", "dapat", "bisa", "boleh"],
    "authority_reference": ["official", "minister", "president", "government", "pejabat", "menteri", "presiden", "pemerintah"],
    "evaluation_positive": ["strategic", "important", "vital", "strong", "strategis", "penting", "kuat"],
    "evaluation_negative": ["dangerous", "illegal", "aggressive", "berbahaya", "ilegal", "agresif"],
}

PERSUASION_MARKERS_BY_LANG = {
    "en": {
        "modality_obligation": ["must", "should", "need"],
        "modality_possibility": ["can", "could", "may", "might"],
        "authority_reference": ["official", "minister", "president", "government"],
        "evaluation_positive": ["strategic", "important", "vital", "strong"],
        "evaluation_negative": ["dangerous", "illegal", "aggressive"],
    },
    "id": {
        "modality_obligation": ["harus", "wajib", "perlu"],
        "modality_possibility": ["dapat", "bisa", "boleh"],
        "authority_reference": ["pejabat", "menteri", "presiden", "pemerintah"],
        "evaluation_positive": ["strategis", "penting", "kuat"],
        "evaluation_negative": ["berbahaya", "ilegal", "agresif"],
    },
    "ms": {
        "modality_obligation": ["mesti", "harus", "wajib", "perlu"],
        "modality_possibility": ["boleh", "dapat", "mungkin"],
        "authority_reference": ["pegawai", "menteri", "presiden", "kerajaan"],
        "evaluation_positive": ["strategik", "penting", "kuat"],
        "evaluation_negative": ["berbahaya", "haram", "agresif"],
    },
    "ru": {
        "modality_obligation": ["должен", "должны", "нужно", "необходимо", "обязан"],
        "modality_possibility": ["может", "могут", "возможно", "способен"],
        "authority_reference": ["официальный", "министр", "президент", "правительство", "власти"],
        "evaluation_positive": ["стратегический", "важный", "сильный", "конструктивный"],
        "evaluation_negative": ["опасный", "незаконный", "агрессивный", "деструктивный"],
    },
}

# Indicator model (based on provided methodology docs)
IDEOLOGY_MARKERS = {
    "ideol": {
        "sovereignty", "kedaulatan", "суверенитет", "national", "nasional", "bangsa",
        "identity", "identitas", "pancasila", "bhinneka", "unity", "persatuan",
        "stability", "stabilitas", "islamic", "islam", "development", "pembangunan",
    },
    "prec": {
        "pancasila", "bhinneka", "nato", "asean", "kremlin", "white", "house",
        "washington", "beijing", "moscow", "putin", "biden", "trump", "xi", "jinping",
    },
    "slog": {
        "for", "nation", "national", "interest", "demi", "bangsa", "negara", "unity",
        "persatuan", "stability", "keamanan", "security", "kestabilan",
    },
    "dich": {
        "we", "they", "our", "their", "us", "them", "kita", "kami", "mereka", "мы", "они",
        "ourselves", "themselves", "sendiri",
    },
}

EMOTION_MARKERS = {
    "weak": {
        "concern", "worry", "uncertain", "harap", "khawatir", "cemas", "risau", "тревога", "сомнение",
    },
    "medium": {
        "fear", "anger", "pride", "hope", "trust", "marah", "bangga", "percaya", "надежда", "гнев",
    },
    "strong": {
        "panic", "threat", "catastrophe", "shock", "outrage", "terror", "ancaman", "krisis", "ужас", "катастрофа",
    },
}

EVALUATION_MARKERS = {
    "rational": {
        "strategic", "important", "legal", "effective", "necessary", "stabil", "penting", "sah",
        "эффективный", "рациональный", "стратегический",
    },
    "emotional": {
        "outrageous", "heroic", "shameful", "brutal", "berbahaya", "agresif", "kejam", "ужасный", "героический",
    },
    "explicit": {
        "must", "should", "need", "harus", "wajib", "perlu", "clearly", "obviously", "очевидно", "должен",
    },
    "implicit": {
        "allegedly", "claimed", "reportedly", "so-called", "seolah", "katanya", "будто", "якобы",
    },
}

METAPHOR_MARKERS = {
    "weak": {"wave", "path", "bridge", "shield", "gelombang", "jembatan", "мост", "волна"},
    "medium": {"battle", "arena", "storm", "engine", "medan", "badai", "арена", "буря"},
    "strong": {"chess", "wounded", "organism", "frontline", "perang", "раненый", "шахмат"},
}


def level_5(v: float, cutoffs: Tuple[float, float, float, float]) -> Tuple[int, str]:
    c1, c2, c3, c4 = cutoffs
    if v < c1:
        return 1, "very_low"
    if v < c2:
        return 2, "low"
    if v < c3:
        return 3, "medium"
    if v < c4:
        return 4, "high"
    return 5, "very_high"


@dataclass
class Doc:
    source: str
    region: str
    year: int
    primary_country: str
    language: str
    title: str
    text: str
    tokens: List[str]


def normalize_token(token: str) -> str:
    return token.casefold().strip("-'_")


def tokenize(text: str) -> List[str]:
    text = US_RE.sub(" usa ", text)
    return [normalize_token(tok) for tok in TOKEN_RE.findall(text) if normalize_token(tok)]


def is_content(tok: str) -> bool:
    if not tok or tok in STOPWORDS:
        return False
    if tok.isdigit() or len(tok) < 2:
        return False
    return True


def simple_lemmatize(tok: str) -> str:
    # Lightweight fallback lemmatization/stemming rules (keeps pipeline dependency-free)
    for suf in ("ing", "ed", "ly", "es"):
        if tok.endswith(suf) and len(tok) > len(suf) + 2:
            return tok[: -len(suf)]
    for suf in ("nya", "lah", "kah", "pun", "ku", "mu", "an", "kan"):
        if tok.endswith(suf) and len(tok) > len(suf) + 2:
            return tok[: -len(suf)]
    return tok


def preprocess_tokens(tokens: List[str], use_lemma: bool) -> List[str]:
    out = []
    for tok in tokens:
        if tok in STOPWORDS:
            continue
        t = simple_lemmatize(tok) if use_lemma else tok
        if is_content(t):
            out.append(t)
    return out


def strip_boilerplate(text: str) -> str:
    lines = text.splitlines()
    cleaned = []
    skip_patterns = (
        "read also",
        "baca juga",
        "follow us",
        "ikuti kami",
        "whatsapp channel",
        "join our",
        "share this",
        "comments",
        "please enter valid email",
        "log in",
        "sign up",
    )
    for line in lines:
        low = line.casefold().strip()
        if not low:
            cleaned.append(line)
            continue
        if any(p in low for p in skip_patterns):
            continue
        cleaned.append(line)
    return "\n".join(cleaned)


def detect_language(raw_tokens: List[str], source: str) -> str:
    scores = {}
    tset = set(raw_tokens)
    for lang, hints in LANG_HINTS.items():
        scores[lang] = sum(1 for h in hints if h in tset)
    best_lang = max(scores, key=scores.get)
    if scores[best_lang] >= 2:
        return best_lang
    return SOURCE_LANG_DEFAULT.get(source, "mixed")


def simhash(tokens: List[str], bits: int = 64) -> int:
    vec = [0] * bits
    counts = Counter(tokens)
    for tok, w in counts.items():
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest()[:16], 16)
        for i in range(bits):
            bit = (h >> i) & 1
            vec[i] += w if bit else -w
    out = 0
    for i, v in enumerate(vec):
        if v >= 0:
            out |= (1 << i)
    return out


def hamming_distance(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def read_docs(metadata_csv: Path, min_year: int, max_year: int, use_lemma: bool) -> List[Doc]:
    docs: List[Doc] = []
    with metadata_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                year = int(row.get("year", "0"))
            except ValueError:
                continue
            if year < min_year or year > max_year:
                continue

            fp = Path(row.get("file_path", ""))
            if not fp.exists():
                continue

            raw = fp.read_text(encoding="utf-8", errors="ignore")
            parts = raw.split("\n\n", 1)
            body = parts[1] if len(parts) == 2 else raw

            body = strip_boilerplate(body)
            raw_toks = tokenize(body)
            language = detect_language(raw_toks, row.get("source", ""))
            toks = preprocess_tokens(raw_toks, use_lemma)
            if not toks:
                continue

            docs.append(
                Doc(
                    source=row.get("source", ""),
                    region=row.get("region", ""),
                    year=year,
                    primary_country=row.get("primary_country", ""),
                    language=language,
                    title=row.get("title", ""),
                    text=body,
                    tokens=toks,
                )
            )
    return docs


def deduplicate_docs(docs: List[Doc], near_dup_jaccard: float, simhash_hamming: int) -> Tuple[List[Doc], Dict[str, int]]:
    kept: List[Doc] = []
    exact_seen = set()
    total = len(docs)
    exact_removed = 0
    near_removed = 0

    # LSH buckets by 4 bands of simhash
    band_map = defaultdict(list)  # key -> kept index
    kept_hashes = []
    kept_sets = []
    kept_lens = []

    for d in docs:
        norm = " ".join(d.tokens)
        hexact = hashlib.md5(norm.encode("utf-8")).hexdigest()
        if hexact in exact_seen:
            exact_removed += 1
            continue
        exact_seen.add(hexact)

        sh = simhash(d.tokens)
        tok_set = set(d.tokens)
        dlen = len(d.tokens)
        candidate_ids = set()
        for b in range(4):
            key = (b, (sh >> (b * 16)) & 0xFFFF)
            for idx in band_map.get(key, []):
                candidate_ids.add(idx)

        is_dup = False
        for idx in candidate_ids:
            if hamming_distance(sh, kept_hashes[idx]) > simhash_hamming:
                continue
            if min(dlen, kept_lens[idx]) / max(dlen, kept_lens[idx]) < 0.85:
                continue
            inter = len(tok_set & kept_sets[idx])
            union = len(tok_set | kept_sets[idx]) or 1
            jac = inter / union
            if jac >= near_dup_jaccard:
                is_dup = True
                break

        if is_dup:
            near_removed += 1
            continue

        idx_new = len(kept)
        kept.append(d)
        kept_hashes.append(sh)
        kept_sets.append(tok_set)
        kept_lens.append(dlen)
        for b in range(4):
            key = (b, (sh >> (b * 16)) & 0xFFFF)
            band_map[key].append(idx_new)

    stats = {
        "total_docs_before_dedup": total,
        "exact_duplicates_removed": exact_removed,
        "near_duplicates_removed": near_removed,
        "total_docs_after_dedup": len(kept),
    }
    return kept, stats


def write_rows(path: Path, header: List[str], rows: Iterable[List[object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def stage1_corpus_profile(docs: List[Doc], out: Path) -> None:
    by_source = Counter(d.source for d in docs)
    by_country = Counter(d.primary_country for d in docs)
    by_year = Counter(d.year for d in docs)
    by_language = Counter(d.language for d in docs)
    by_scy = Counter((d.source, d.primary_country, d.year) for d in docs)

    write_rows(out / "stage1_profile_source.csv", ["source", "doc_count"], [[k, v] for k, v in sorted(by_source.items())])
    write_rows(out / "stage1_profile_country.csv", ["country", "doc_count"], [[k, v] for k, v in sorted(by_country.items())])
    write_rows(out / "stage1_profile_year.csv", ["year", "doc_count"], [[k, v] for k, v in sorted(by_year.items())])
    write_rows(out / "stage1_profile_language.csv", ["language", "doc_count"], [[k, v] for k, v in sorted(by_language.items())])
    write_rows(
        out / "stage1_profile_source_country_year.csv",
        ["source", "country", "year", "doc_count"],
        [[s, c, y, n] for (s, c, y), n in sorted(by_scy.items())],
    )


def stage2_quantitative(docs: List[Doc], out: Path, top_n: int, kwic_window: int, kwic_max: int, colloc_window: int, colloc_min: int) -> None:
    all_freq = Counter()
    by_country = defaultdict(Counter)
    by_source = defaultdict(Counter)
    doc_freq = Counter()

    for d in docs:
        all_freq.update(d.tokens)
        by_country[d.primary_country].update(d.tokens)
        by_source[d.source].update(d.tokens)
        doc_freq.update(set(d.tokens))

    write_rows(out / "stage2_freq_overall.csv", ["token", "freq"], [[t, f] for t, f in all_freq.most_common(top_n)])

    rows_country = []
    for c, cnt in sorted(by_country.items()):
        for t, f in cnt.most_common(top_n):
            rows_country.append([c, t, f])
    write_rows(out / "stage2_freq_by_country.csv", ["country", "token", "freq"], rows_country)

    rows_source = []
    for s, cnt in sorted(by_source.items()):
        for t, f in cnt.most_common(min(300, top_n)):
            rows_source.append([s, t, f])
    write_rows(out / "stage2_freq_by_source.csv", ["source", "token", "freq"], rows_source)

    # KWIC
    query_terms = {t for terms in COUNTRY_TERMS.values() for t in terms}
    kwic_rows = []
    for d in docs:
        toks = d.tokens
        for i, tok in enumerate(toks):
            if tok not in query_terms:
                continue
            left = " ".join(toks[max(0, i - kwic_window):i])
            right = " ".join(toks[i + 1:i + 1 + kwic_window])
            kwic_rows.append([d.source, d.year, d.primary_country, tok, left, tok, right])
            if len(kwic_rows) >= kwic_max:
                break
        if len(kwic_rows) >= kwic_max:
            break
    write_rows(out / "stage2_kwic.csv", ["source", "year", "primary_country", "query_term", "left_context", "keyword", "right_context"], kwic_rows)

    # Expanded sentence-level referent contexts: previous + hit sentence + next
    sent_splitter = re.compile(r"(?<=[\.\!\?])\s+")
    sent_rows = []
    max_sent_rows = max(kwic_max, 5000)
    for d in docs:
        sents = [s.strip() for s in sent_splitter.split(d.text) if s.strip()]
        if not sents:
            continue
        for i, s in enumerate(sents):
            stoks = preprocess_tokens(tokenize(s), use_lemma=True)
            if not stoks:
                continue
            hit_country = ""
            hit_term = ""
            for country, anchors in COUNTRY_TERMS.items():
                aset = set(anchors)
                found = next((t for t in stoks if t in aset), "")
                if found:
                    hit_country = country
                    hit_term = found
                    break
            if not hit_term:
                continue
            prev_sent = sents[i - 1] if i - 1 >= 0 else ""
            next_sent = sents[i + 1] if i + 1 < len(sents) else ""
            expanded = " ".join([x for x in [prev_sent, s, next_sent] if x]).strip()
            sent_rows.append([
                d.source, d.year, d.primary_country, hit_country, hit_term,
                prev_sent, s, next_sent, expanded,
            ])
            if len(sent_rows) >= max_sent_rows:
                break
        if len(sent_rows) >= max_sent_rows:
            break
    write_rows(
        out / "stage2_referent_context_sentences.csv",
        ["source", "year", "primary_country", "anchor_country", "anchor_term", "prev_sent", "hit_sent", "next_sent", "expanded_context"],
        sent_rows,
    )

    # Collocations with MI/t-score/LLR for country anchors
    total_n = sum(all_freq.values())
    coll_rows = []

    def llr(a: int, b: int, c: int, d: int) -> float:
        def xlogx(x: float) -> float:
            return x * math.log(x) if x > 0 else 0.0
        g2 = 2.0 * (xlogx(a) + xlogx(b) + xlogx(c) + xlogx(d) - xlogx(a + b) - xlogx(c + d) - xlogx(a + c) - xlogx(b + d) + xlogx(a + b + c + d))
        return max(g2, 0.0)

    for country, anchors in COUNTRY_TERMS.items():
        aset = set(anchors)
        cooc = Counter()
        anchor_n = 0
        for d in docs:
            toks = d.tokens
            for i, tok in enumerate(toks):
                if tok not in aset:
                    continue
                anchor_n += 1
                l = max(0, i - colloc_window)
                r = min(len(toks), i + colloc_window + 1)
                for j in range(l, r):
                    if j == i:
                        continue
                    w = toks[j]
                    if w in aset or not is_content(w):
                        continue
                    if doc_freq[w] / max(len(docs), 1) > 0.4:
                        continue
                    cooc[w] += 1

        scored = []
        for w, c_xy in cooc.items():
            if c_xy < colloc_min:
                continue
            c_x = anchor_n
            c_y = all_freq[w]
            expected = (c_x * c_y / total_n) if total_n else 0.0
            mi = math.log2((c_xy * total_n) / (c_x * c_y)) if c_xy > 0 and c_x > 0 and c_y > 0 else 0.0
            t_score = (c_xy - expected) / math.sqrt(c_xy) if c_xy > 0 else 0.0
            a = c_xy
            b = max(c_x - c_xy, 0)
            c = max(c_y - c_xy, 0)
            d = max(total_n - a - b - c, 0)
            g2 = llr(a, b, c, d)
            scored.append((w, c_xy, mi, t_score, g2))

        scored.sort(key=lambda x: (x[4], x[1]), reverse=True)
        for w, c_xy, mi, t, g2 in scored[:top_n]:
            coll_rows.append([country, w, c_xy, round(mi, 5), round(t, 5), round(g2, 5)])

    write_rows(out / "stage2_collocations.csv", ["anchor_country", "collocate", "cooc_freq", "mi", "t_score", "llr_g2"], coll_rows)

    # Keyword analysis (subcorpus vs rest; LLR)
    key_rows = []
    total_tokens = sum(all_freq.values())
    for country, cnt in by_country.items():
        sub_total = sum(cnt.values())
        rest_total = total_tokens - sub_total
        if sub_total <= 0 or rest_total <= 0:
            continue
        for tok, a in cnt.items():
            if a < 5:
                continue
            b = sub_total - a
            c = all_freq[tok] - a
            d = rest_total - c
            if c < 0 or d < 0:
                continue
            g2 = llr(a, b, c, d)
            # keyness direction
            sub_rate = a / sub_total
            rest_rate = c / rest_total if rest_total else 0.0
            if sub_rate <= rest_rate:
                continue
            key_rows.append([country, tok, a, round(sub_rate, 8), round(rest_rate, 8), round(g2, 5)])

    key_rows.sort(key=lambda x: x[5], reverse=True)
    write_rows(out / "stage2_keywords.csv", ["country", "token", "sub_freq", "sub_rate", "rest_rate", "llr_g2"], key_rows[:3000])


def stage3_qualitative(docs: List[Doc], out: Path) -> None:
    # sentiment + framing + persuasion markers
    sent_doc_rows = []
    frame_rows = []
    persu_rows = []

    sent_agg = defaultdict(lambda: {"n": 0, "sum": 0.0, "pos": 0, "neg": 0, "neu": 0})
    frame_agg = defaultdict(Counter)
    persu_agg = defaultdict(Counter)

    for d in docs:
        toks = d.tokens
        pos_lex = SENT_POS_BY_LANG.get(d.language, SENT_POS)
        neg_lex = SENT_NEG_BY_LANG.get(d.language, SENT_NEG)
        pos = sum(1 for t in toks if t in pos_lex)
        neg = sum(1 for t in toks if t in neg_lex)
        score = (pos - neg) / max(len(toks), 1)
        label = "neutral"
        if score > 0.001:
            label = "positive"
        elif score < -0.001:
            label = "negative"

        sent_doc_rows.append([d.source, d.year, d.primary_country, round(score, 6), label, pos, neg, len(toks)])
        skey = (d.primary_country, d.year)
        sent_agg[skey]["n"] += 1
        sent_agg[skey]["sum"] += score
        sent_agg[skey][label[:3]] += 1

        frame_lex = FRAME_LEXICONS_BY_LANG.get(d.language, FRAME_LEXICONS)
        for frame_name, lex in frame_lex.items():
            c = sum(1 for t in toks if t in lex)
            if c > 0:
                frame_rows.append([d.source, d.year, d.primary_country, frame_name, c])
                frame_agg[(d.primary_country, d.year)][frame_name] += c

        marker_lex = PERSUASION_MARKERS_BY_LANG.get(d.language, PERSUASION_MARKERS)
        for group, terms in marker_lex.items():
            tset = set(terms)
            c = sum(1 for t in toks if t in tset)
            if c > 0:
                persu_rows.append([d.source, d.year, d.primary_country, group, c])
                persu_agg[(d.source, d.primary_country, d.year)][group] += c

    write_rows(out / "stage3_sentiment_doc_level.csv", ["source", "year", "country", "sent_score", "label", "pos_hits", "neg_hits", "token_count"], sent_doc_rows)

    sent_sum_rows = []
    for (country, year), agg in sorted(sent_agg.items()):
        avg = agg["sum"] / agg["n"] if agg["n"] else 0.0
        sent_sum_rows.append([country, year, agg["n"], round(avg, 6), agg["pos"], agg["neg"], agg["neu"]])
    write_rows(out / "stage3_sentiment_summary_country_year.csv", ["country", "year", "doc_count", "avg_sent_score", "positive_docs", "negative_docs", "neutral_docs"], sent_sum_rows)

    frame_sum_rows = []
    for (country, year), counter in sorted(frame_agg.items()):
        for frame_name, c in counter.items():
            frame_sum_rows.append([country, year, frame_name, c])
    write_rows(out / "stage3_frame_summary_country_year.csv", ["country", "year", "frame", "count"], frame_sum_rows)

    persu_sum_rows = []
    for (source, country, year), counter in sorted(persu_agg.items()):
        for group, c in counter.items():
            persu_sum_rows.append([source, country, year, group, c])
    write_rows(out / "stage3_persuasion_markers_summary.csv", ["source", "country", "year", "marker_group", "count"], persu_sum_rows)


def stage4_prognostic(docs: List[Doc], out: Path) -> None:
    # Time trend proxies: volume slope and sentiment slope by country across years
    years = sorted(set(d.year for d in docs))
    by_country_year_volume = Counter((d.primary_country, d.year) for d in docs)

    # sentiment per doc for slope
    sent_by_country_year = defaultdict(list)
    for d in docs:
        toks = d.tokens
        pos_lex = SENT_POS_BY_LANG.get(d.language, SENT_POS)
        neg_lex = SENT_NEG_BY_LANG.get(d.language, SENT_NEG)
        pos = sum(1 for t in toks if t in pos_lex)
        neg = sum(1 for t in toks if t in neg_lex)
        score = (pos - neg) / max(len(toks), 1)
        sent_by_country_year[(d.primary_country, d.year)].append(score)

    def linear_slope(xs: List[float], ys: List[float]) -> float:
        n = len(xs)
        if n < 2:
            return 0.0
        mx = sum(xs) / n
        my = sum(ys) / n
        num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        den = sum((x - mx) ** 2 for x in xs)
        return num / den if den else 0.0

    rows = []
    for country in sorted({d.primary_country for d in docs}):
        xs = []
        volume_ys = []
        sent_ys = []
        for y in years:
            xs.append(float(y))
            volume_ys.append(float(by_country_year_volume.get((country, y), 0)))
            vals = sent_by_country_year.get((country, y), [])
            sent_ys.append(mean(vals) if vals else 0.0)

        vol_slope = linear_slope(xs, volume_ys)
        sent_slope = linear_slope(xs, sent_ys)
        rows.append([country, round(vol_slope, 6), round(sent_slope, 6), min(years), max(years)])

    write_rows(out / "stage4_prognostic_trends.csv", ["country", "volume_slope_per_year", "sentiment_slope_per_year", "from_year", "to_year"], rows)


def stage5_representativeness(docs: List[Doc], out: Path) -> None:
    all_sources = sorted({d.source for d in docs})
    source_n = len(all_sources)

    by_cy_source = defaultdict(Counter)
    by_source_country = Counter((d.source, d.primary_country) for d in docs)
    by_country = Counter(d.primary_country for d in docs)
    by_source = Counter(d.source for d in docs)

    for d in docs:
        by_cy_source[(d.primary_country, d.year)][d.source] += 1

    # Country-year representativeness (concentration/diversity)
    cy_rows = []
    for (country, year), cnt in sorted(by_cy_source.items()):
        total = sum(cnt.values())
        if total <= 0:
            continue
        ps = [v / total for v in cnt.values()]
        hhi = sum(p * p for p in ps)
        entropy = -sum(p * math.log(p) for p in ps if p > 0)
        effective_sources = math.exp(entropy) if entropy > 0 else 1.0
        active_sources = len(cnt)
        max_share = max(ps)
        min_share_active = min(ps) if ps else 0.0
        imbalance_ratio = (max_share / min_share_active) if min_share_active > 0 else 0.0
        cy_rows.append([
            country,
            year,
            total,
            active_sources,
            source_n,
            round(active_sources / source_n if source_n else 0.0, 6),
            round(hhi, 6),
            round(entropy, 6),
            round(effective_sources, 6),
            round(max_share, 6),
            round(min_share_active, 6),
            round(imbalance_ratio, 6),
        ])

    write_rows(
        out / "stage5_representativeness_country_year.csv",
        [
            "country",
            "year",
            "doc_count",
            "active_sources",
            "total_sources",
            "coverage_ratio",
            "hhi_concentration",
            "shannon_entropy",
            "effective_sources",
            "max_source_share",
            "min_active_source_share",
            "imbalance_ratio_max_to_min",
        ],
        cy_rows,
    )

    # Country totals: source concentration and cross-country balance inside each source
    country_rows = []
    for country in sorted(by_country.keys()):
        total = by_country[country]
        src_counts = [by_source_country[(s, country)] for s in all_sources if by_source_country[(s, country)] > 0]
        if not src_counts:
            continue
        ps = [c / total for c in src_counts]
        hhi = sum(p * p for p in ps)
        entropy = -sum(p * math.log(p) for p in ps if p > 0)
        effective_sources = math.exp(entropy) if entropy > 0 else 1.0
        country_rows.append([
            country,
            total,
            len(src_counts),
            round(hhi, 6),
            round(entropy, 6),
            round(effective_sources, 6),
        ])

    write_rows(
        out / "stage5_representativeness_country_total.csv",
        ["country", "doc_count", "active_sources", "hhi_concentration", "shannon_entropy", "effective_sources"],
        country_rows,
    )

    sc_rows = []
    for (source, country), n in sorted(by_source_country.items()):
        source_total = by_source[source]
        country_total = by_country[country]
        sc_rows.append([
            source,
            country,
            n,
            round(n / source_total if source_total else 0.0, 6),
            round(n / country_total if country_total else 0.0, 6),
        ])
    write_rows(
        out / "stage5_source_country_balance.csv",
        ["source", "country", "doc_count", "share_within_source", "share_within_country"],
        sc_rows,
    )


def _chi2_stat(contingency: List[List[float]]) -> Tuple[float, int, float]:
    r = len(contingency)
    c = len(contingency[0]) if r else 0
    if r < 2 or c < 2:
        return 0.0, 0, 0.0
    row_sums = [sum(row) for row in contingency]
    col_sums = [sum(contingency[i][j] for i in range(r)) for j in range(c)]
    n = sum(row_sums)
    if n <= 0:
        return 0.0, 0, 0.0
    chi2 = 0.0
    for i in range(r):
        for j in range(c):
            exp = row_sums[i] * col_sums[j] / n
            if exp > 0:
                chi2 += (contingency[i][j] - exp) ** 2 / exp
    df = (r - 1) * (c - 1)
    v = math.sqrt(chi2 / (n * min(r - 1, c - 1))) if min(r - 1, c - 1) > 0 else 0.0
    return chi2, df, v


def _gammainc_lower_reg(a: float, x: float) -> float:
    # Regularized lower incomplete gamma P(a, x) using series / continued fraction
    if a <= 0:
        return 0.0
    if x <= 0:
        return 0.0
    gln = math.lgamma(a)
    eps = 1e-12
    itmax = 200

    if x < a + 1.0:
        ap = a
        summ = 1.0 / a
        delt = summ
        for _ in range(itmax):
            ap += 1.0
            delt *= x / ap
            summ += delt
            if abs(delt) < abs(summ) * eps:
                break
        return max(0.0, min(1.0, summ * math.exp(-x + a * math.log(x) - gln)))

    # Q(a,x) via continued fraction, then P = 1 - Q
    b = x + 1.0 - a
    c = 1.0 / 1e-30
    d = 1.0 / b
    h = d
    for i in range(1, itmax + 1):
        an = -i * (i - a)
        b += 2.0
        d = an * d + b
        if abs(d) < 1e-30:
            d = 1e-30
        c = b + an / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        delt = d * c
        h *= delt
        if abs(delt - 1.0) < eps:
            break
    q = math.exp(-x + a * math.log(x) - gln) * h
    p = 1.0 - q
    return max(0.0, min(1.0, p))


def chi2_p_value(chi2: float, df: int) -> float:
    if chi2 < 0 or df <= 0:
        return 1.0
    # Right-tail probability for chi-square(df): p = Q(df/2, chi2/2) = 1 - P(...)
    p_lower = _gammainc_lower_reg(df / 2.0, chi2 / 2.0)
    return max(0.0, min(1.0, 1.0 - p_lower))


def cramers_v_label(v: float) -> str:
    if v < 0.1:
        return "negligible"
    if v < 0.3:
        return "small"
    if v < 0.5:
        return "moderate"
    return "strong"


def stage6_significance(docs: List[Doc], out: Path, top_n_logodds: int) -> None:
    countries = sorted({d.primary_country for d in docs})
    if len(countries) < 2:
        return

    by_country_tokens = defaultdict(Counter)
    by_country_frames = defaultdict(Counter)
    by_country_sent_labels = defaultdict(Counter)

    for d in docs:
        by_country_tokens[d.primary_country].update(d.tokens)
        frame_lex = FRAME_LEXICONS_BY_LANG.get(d.language, FRAME_LEXICONS)
        for frame_name, lex in frame_lex.items():
            c = sum(1 for t in d.tokens if t in lex)
            if c > 0:
                by_country_frames[d.primary_country][frame_name] += c

        pos_lex = SENT_POS_BY_LANG.get(d.language, SENT_POS)
        neg_lex = SENT_NEG_BY_LANG.get(d.language, SENT_NEG)
        pos = sum(1 for t in d.tokens if t in pos_lex)
        neg = sum(1 for t in d.tokens if t in neg_lex)
        score = (pos - neg) / max(len(d.tokens), 1)
        label = "neutral"
        if score > 0.001:
            label = "positive"
        elif score < -0.001:
            label = "negative"
        by_country_sent_labels[d.primary_country][label] += 1

    # Pairwise chi-square on frames/sentiment labels
    sig_rows = []
    frame_names = sorted({f for c in countries for f in by_country_frames[c].keys()})
    sent_labels = ["positive", "negative", "neutral"]

    for c1, c2 in combinations(countries, 2):
        frame_cont = [
            [by_country_frames[c1].get(f, 0) for f in frame_names],
            [by_country_frames[c2].get(f, 0) for f in frame_names],
        ]
        chi2_f, df_f, v_f = _chi2_stat(frame_cont)
        p_f = chi2_p_value(chi2_f, df_f)
        sig_rows.append([c1, c2, "frame_distribution", round(chi2_f, 6), df_f, f"{p_f:.8f}", round(v_f, 6), cramers_v_label(v_f)])

        sent_cont = [
            [by_country_sent_labels[c1].get(s, 0) for s in sent_labels],
            [by_country_sent_labels[c2].get(s, 0) for s in sent_labels],
        ]
        chi2_s, df_s, v_s = _chi2_stat(sent_cont)
        p_s = chi2_p_value(chi2_s, df_s)
        sig_rows.append([c1, c2, "sentiment_distribution", round(chi2_s, 6), df_s, f"{p_s:.8f}", round(v_s, 6), cramers_v_label(v_s)])

    write_rows(
        out / "stage6_significance_pairwise.csv",
        ["country_a", "country_b", "test_target", "chi2", "df", "p_value", "cramers_v", "effect_size_label"],
        sig_rows,
    )

    # Weighted log-odds (z-score proxy) for lexical contrasts
    logodds_rows = []
    for c1, c2 in combinations(countries, 2):
        cnt1 = by_country_tokens[c1]
        cnt2 = by_country_tokens[c2]
        n1 = sum(cnt1.values())
        n2 = sum(cnt2.values())
        vocab = set(cnt1) | set(cnt2)
        scored = []
        for tok in vocab:
            a = cnt1.get(tok, 0)
            b = cnt2.get(tok, 0)
            if a + b < 20:
                continue
            lor = math.log((a + 0.5) / (max(n1 - a, 0) + 0.5)) - math.log((b + 0.5) / (max(n2 - b, 0) + 0.5))
            se = math.sqrt(1.0 / (a + 0.5) + 1.0 / (b + 0.5))
            z = lor / se if se > 0 else 0.0
            scored.append((tok, a, b, z))
        scored.sort(key=lambda x: abs(x[3]), reverse=True)
        for tok, a, b, z in scored[:top_n_logodds]:
            favored = c1 if z > 0 else c2
            logodds_rows.append([c1, c2, tok, a, b, round(z, 6), favored])

    write_rows(
        out / "stage6_logodds_tokens.csv",
        ["country_a", "country_b", "token", "freq_a", "freq_b", "z_score", "favored_country"],
        logodds_rows,
    )


def stage7_persuasion_indicator_model(docs: List[Doc], out: Path) -> None:
    rows_doc = []
    agg_country_year = defaultdict(lambda: {"n": 0, "IDI": 0.0, "EMI": 0.0, "EVI": 0.0, "MTI": 0.0, "IP": 0.0})
    agg_source = defaultdict(lambda: {"n": 0, "IDI": 0.0, "EMI": 0.0, "EVI": 0.0, "MTI": 0.0, "IP": 0.0})

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
        return sum(1 for t in tokens if is_content(t))

    def sentence_tokens(text: str) -> List[List[str]]:
        sents = [s.strip() for s in sentence_splitter.split(text) if s.strip()]
        out_s = []
        for s in sents:
            stoks = preprocess_tokens(tokenize(s), use_lemma=True)
            if stoks:
                out_s.append(stoks)
        return out_s

    for d in docs:
        toks = d.tokens
        W = max(content_len(toks), 1)
        tset = Counter(toks)
        ideol = sum(tset[t] for t in IDEOLOGY_MARKERS["ideol"] if t in tset)
        prec = sum(tset[t] for t in IDEOLOGY_MARKERS["prec"] if t in tset)
        slog = sum(tset[t] for t in IDEOLOGY_MARKERS["slog"] if t in tset)
        dich = sum(tset[t] for t in IDEOLOGY_MARKERS["dich"] if t in tset)
        n_ideol = ideol + prec + slog + dich
        IDI_share = min(max(n_ideol / W, 0.0), 1.0)
        IDI = IDI_share

        e_w = sum(tset[t] for t in EMOTION_MARKERS["weak"] if t in tset)
        e_m = sum(tset[t] for t in EMOTION_MARKERS["medium"] if t in tset)
        e_s = sum(tset[t] for t in EMOTION_MARKERS["strong"] if t in tset)
        # EMI = (1/3*weak + 2/3*medium + 1*strong) / N_content
        EMI_share = min(max(((e_w / 3.0) + (2.0 * e_m / 3.0) + e_s) / W, 0.0), 1.0)
        EMI = EMI_share

        M_w = sum(tset[t] for t in METAPHOR_MARKERS["weak"] if t in tset)
        M_m = sum(tset[t] for t in METAPHOR_MARKERS["medium"] if t in tset)
        M_s = sum(tset[t] for t in METAPHOR_MARKERS["strong"] if t in tset)
        n_met = M_w + M_m + M_s
        MTI_share = min(max(n_met / W, 0.0), 1.0)
        MTI = MTI_share

        # Referent-oriented discrete EVI: -2..2 based on expanded contexts (sent-1/sent/sent+1)
        aliases = referent_aliases.get(d.primary_country, set())
        sent_toks = sentence_tokens(d.text)
        selected = []
        for i, st in enumerate(sent_toks):
            if any(tok in aliases for tok in st):
                lo = max(0, i - 1)
                hi = min(len(sent_toks), i + 2)
                for j in range(lo, hi):
                    selected.extend(sent_toks[j])
        context_toks = selected if selected else toks
        pos_lex = SENT_POS_BY_LANG.get(d.language, SENT_POS)
        neg_lex = SENT_NEG_BY_LANG.get(d.language, SENT_NEG)
        pos = sum(1 for t in context_toks if t in pos_lex)
        neg = sum(1 for t in context_toks if t in neg_lex)
        ref_w = max(content_len(context_toks), 1)
        score = (pos - neg) / ref_w
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

        IP = (IDI + EMI + MTI) * EVI
        IP = max(min(IP, 6.0), -6.0)

        # Backward-compat aliases
        PP_equal = IP
        PP_weighted = IP

        rows_doc.append([
            d.source, d.year, d.primary_country, d.language, W,
            ideol, prec, slog, dich, round(IDI, 6), "", "",
            e_w, e_m, e_s, round(EMI, 6), "", "",
            len(context_toks), pos, neg, 0, 0, 0, 0, 0, EVI, "", "",
            n_met, M_w, M_m, M_s, 0, 0, 0, 0, 0, round(MTI, 6), "", "",
            round(IDI_share, 6), round(EMI_share, 6), round(MTI_share, 6),
            round(IP, 6), round(PP_equal, 6), round(PP_weighted, 6),
        ])

        cy = (d.primary_country, d.year)
        agg_country_year[cy]["n"] += 1
        agg_source[d.source]["n"] += 1
        for key, val in [("IDI", IDI), ("EMI", EMI), ("EVI", EVI), ("MTI", MTI), ("IP", IP)]:
            agg_country_year[cy][key] += val
            agg_source[d.source][key] += val

    write_rows(
        out / "stage7_persuasion_doc_indices.csv",
        [
            "source", "year", "country", "language", "token_count",
            "ideol", "prec", "slog", "dich", "IDI", "IDI_level_num", "IDI_level",
            "e_w", "e_m", "e_s", "EMI", "EMI_level_num", "EMI_level",
            "n_eval", "R", "E", "Imp", "Exp", "EDI", "EII", "ELFI", "EVI", "EVI_level_num", "EVI_level",
            "n_met", "M_w", "M_m", "M_s", "Ind", "Dir", "MDI", "MII", "MLFI", "MTI", "MTI_level_num", "MTI_level",
            "IDI_share", "EMI_share", "MTI_share",
            "IP", "PP_equal", "PP_weighted",
        ],
        rows_doc,
    )

    rows_cy = []
    for (country, year), a in sorted(agg_country_year.items()):
        n = a["n"]
        rows_cy.append([
            country, year, n,
            round(a["IDI"] / n, 6), round(a["EMI"] / n, 6), round(a["EVI"] / n, 6), round(a["MTI"] / n, 6),
            round(a["IDI"] / n, 6), round(a["EMI"] / n, 6), round(a["MTI"] / n, 6),
            round(a["IP"] / n, 6), round(a["IP"] / n, 6), round(a["IP"] / n, 6),
        ])
    write_rows(
        out / "stage7_persuasion_summary_country_year.csv",
        [
            "country", "year", "doc_count",
            "avg_IDI", "avg_EMI", "avg_EVI", "avg_MTI",
            "avg_IDI_share", "avg_EMI_share", "avg_MTI_share",
            "avg_IP", "avg_PP_equal", "avg_PP_weighted",
        ],
        rows_cy,
    )

    rows_source = []
    for source, a in sorted(agg_source.items()):
        n = a["n"]
        rows_source.append([
            source, n,
            round(a["IDI"] / n, 6), round(a["EMI"] / n, 6), round(a["EVI"] / n, 6), round(a["MTI"] / n, 6),
            round(a["IDI"] / n, 6), round(a["EMI"] / n, 6), round(a["MTI"] / n, 6),
            round(a["IP"] / n, 6), round(a["IP"] / n, 6), round(a["IP"] / n, 6),
        ])
    write_rows(
        out / "stage7_persuasion_summary_source.csv",
        [
            "source", "doc_count",
            "avg_IDI", "avg_EMI", "avg_EVI", "avg_MTI",
            "avg_IDI_share", "avg_EMI_share", "avg_MTI_share",
            "avg_IP", "avg_PP_equal", "avg_PP_weighted",
        ],
        rows_source,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mediatext analyzator: strict stage-based linguopragmatic corpus analyzer")
    p.add_argument("--input-metadata", default="output_country_discourse_raw/metadata.csv")
    p.add_argument("--output-dir", default="output_country_discourse_raw_analysis_strict")
    p.add_argument("--min-year", type=int, default=2022)
    p.add_argument("--max-year", type=int, default=2026)
    p.add_argument("--top-n", type=int, default=250)
    p.add_argument("--kwic-window", type=int, default=7)
    p.add_argument("--kwic-max-rows", type=int, default=12000)
    p.add_argument("--colloc-window", type=int, default=5)
    p.add_argument("--colloc-min-cooc", type=int, default=5)
    p.add_argument("--no-lemma", action="store_true", help="Disable lightweight fallback lemmatization")
    p.add_argument("--disable-dedup", action="store_true", help="Disable exact/near duplicate filtering")
    p.add_argument("--near-dup-jaccard", type=float, default=0.92)
    p.add_argument("--near-dup-simhash-hamming", type=int, default=3)
    p.add_argument("--top-n-logodds", type=int, default=120)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    meta = Path(args.input_metadata)
    if not meta.exists():
        raise FileNotFoundError(f"metadata not found: {meta}")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    docs = read_docs(meta, args.min_year, args.max_year, use_lemma=not args.no_lemma)
    if not docs:
        raise RuntimeError("No documents in selected year range")
    dedup_stats = {
        "total_docs_before_dedup": len(docs),
        "exact_duplicates_removed": 0,
        "near_duplicates_removed": 0,
        "total_docs_after_dedup": len(docs),
    }
    if not args.disable_dedup:
        docs, dedup_stats = deduplicate_docs(
            docs,
            near_dup_jaccard=args.near_dup_jaccard,
            simhash_hamming=args.near_dup_simhash_hamming,
        )
        if not docs:
            raise RuntimeError("No documents after de-duplication")
    write_rows(
        out / "stage1_dedup_stats.csv",
        ["metric", "value"],
        [[k, v] for k, v in dedup_stats.items()],
    )

    stage1_corpus_profile(docs, out)
    stage2_quantitative(
        docs,
        out,
        top_n=args.top_n,
        kwic_window=args.kwic_window,
        kwic_max=args.kwic_max_rows,
        colloc_window=args.colloc_window,
        colloc_min=args.colloc_min_cooc,
    )
    stage3_qualitative(docs, out)
    stage4_prognostic(docs, out)
    stage5_representativeness(docs, out)
    stage6_significance(docs, out, top_n_logodds=args.top_n_logodds)
    stage7_persuasion_indicator_model(docs, out)

    print("=" * 80)
    print("STRICT ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"Documents analyzed: {len(docs)}")
    print(f"Output: {out.resolve()}")
    print("Stage 1: stage1_profile_*.csv")
    print("Stage 2: stage2_freq_*.csv, stage2_kwic.csv, stage2_collocations.csv, stage2_keywords.csv")
    print("Stage 3: stage3_sentiment_*.csv, stage3_frame_summary_country_year.csv, stage3_persuasion_markers_summary.csv")
    print("Stage 4: stage4_prognostic_trends.csv")
    print("Stage 5: stage5_representativeness_country_year.csv, stage5_representativeness_country_total.csv, stage5_source_country_balance.csv")
    print("Stage 6: stage6_significance_pairwise.csv, stage6_logodds_tokens.csv")
    print("Stage 7: stage7_persuasion_doc_indices.csv, stage7_persuasion_summary_country_year.csv, stage7_persuasion_summary_source.csv")


if __name__ == "__main__":
    main()
