#!/usr/bin/env python3
"""Multi-country political media parser for Malaysia and Indonesia sources.

Targets discourse around USA, Russia, and China with configurable date window.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

DEFAULT_OUTPUT_DIR = "output_country_discourse"
DEFAULT_MIN_YEAR = 2025
DEFAULT_MAX_YEAR = 2026
DEFAULT_MAX_PAGES = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


@dataclass
class SourceConfig:
    name: str
    region: str
    domain: str
    search_template: str
    article_link_selector: str
    article_url_contains: Sequence[str]
    title_selectors: Sequence[str]
    content_selectors: Sequence[str]
    date_selectors: Sequence[str]
    search_mode: str = "html"
    queryly_key: Optional[str] = None
    article_url_regex: Optional[str] = None
    algolia_app_id: Optional[str] = None
    algolia_api_key: Optional[str] = None
    algolia_index: Optional[str] = None


COUNTRY_PROFILES: Dict[str, Dict[str, Sequence[str]]] = {
    "usa": {
        "search_terms": [
            "USA",
            "United States",
            "Amerika Serikat",
            "Amerika Syarikat",
            "Washington",
            "Biden",
            "Trump",
            "White House",
            "Pentagon",
            "US Congress",
            "US economy",
            "American sanctions",
        ],
        "regex_i": [
            r"\bUnited States\b",
            r"\bU\.?S\.?A\.?\b",
            r"\bAmerika Serikat\b",
            r"\bAmerika Syarikat\b",
            r"\bAmerican\b",
            r"\bWashington\b",
            r"\bGedung Putih\b",
            r"\bWhite House\b",
            r"\bPentagon\b",
            r"\bUS Congress\b",
            r"\bState Department\b",
            r"\bFederal Reserve\b",
            r"\bWall Street\b",
        ],
        "regex_cs": [
            r"\bUS\b",
            r"\bAS\b",
        ],
    },
    "russia": {
        "search_terms": [
            "Russia",
            "Rusia",
            "Moscow",
            "Kremlin",
            "Putin",
            "Lavrov",
            "Russian economy",
            "Russian military",
            "Moscow sanctions",
        ],
        "regex_i": [
            r"\bRussia\b",
            r"\bRusia\b",
            r"\bRussian\b",
            r"\bMoscow\b",
            r"\bKremlin\b",
            r"\bPutin\b",
            r"\bLavrov\b",
            r"\bRussian economy\b",
            r"\bRussian military\b",
            r"\bEurasian\b",
        ],
        "regex_cs": [],
    },
    "china": {
        "search_terms": [
            "China",
            "Cina",
            "Tiongkok",
            "Beijing",
            "Xi Jinping",
            "CCP",
            "CPC",
            "Belt and Road",
            "Yuan",
            "Chinese economy",
            "Chinese culture",
            "Taiwan Strait",
        ],
        "regex_i": [
            r"\bChina\b",
            r"\bChinese\b",
            r"\bCina\b",
            r"\bTiongkok\b",
            r"\bBeijing\b",
            r"\bXi Jinping\b",
            r"\bCCP\b",
            r"\bCPC\b",
            r"\bCommunist Party of China\b",
            r"\bBelt and Road\b",
            r"\bBRI\b",
            r"\bYuan\b",
            r"\bRenminbi\b",
            r"\bTaiwan Strait\b",
            r"\bSouth China Sea\b",
        ],
        "regex_cs": [],
    },
}

THEME_PROFILES: Dict[str, Sequence[str]] = {
    "security": [
        r"\bwar\b", r"\bmilitary\b", r"\bdefense\b", r"\bsecurity\b", r"\bthreat\b", r"\bmissile\b",
        r"\bperang\b", r"\bmiliter\b", r"\bpertahanan\b", r"\bkeamanan\b", r"\bancaman\b",
    ],
    "economy": [
        r"\beconomy\b", r"\btrade\b", r"\btariff\b", r"\binvestment\b", r"\bmarket\b", r"\binflation\b", r"\bcurrency\b",
        r"\bekonomi\b", r"\bperdagangan\b", r"\btarif\b", r"\binvestasi\b", r"\bpasar\b",
    ],
    "diplomacy": [
        r"\bdiplomacy\b", r"\bdiplomatic\b", r"\bmeeting\b", r"\bminister\b", r"\bsummit\b", r"\bagreement\b",
        r"\bdiplomasi\b", r"\bpertemuan\b", r"\bmenteri\b", r"\bktt\b", r"\bkesepakatan\b",
    ],
    "culture": [
        r"\bculture\b", r"\bcultural\b", r"\bartist\b", r"\bmusic\b", r"\bfilm\b", r"\bsport\b", r"\bsports\b",
        r"\bbudaya\b", r"\bseniman\b", r"\bmusik\b", r"\bfilm\b", r"\bolahraga\b",
    ],
}


def _text_or_none(node) -> Optional[str]:
    if not node:
        return None
    for attr in ("content", "datetime"):
        attr_val = node.get(attr)
        if attr_val:
            return str(attr_val).strip()
    text = node.get_text("\n", strip=True)
    return text if text else None


def _find_first(soup: BeautifulSoup, selectors: Sequence[str]):
    for css in selectors:
        node = soup.select_one(css)
        if node:
            return node
    return None


def _sanitize_filename(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"\s+", "_", value)
    return re.sub(r"[^a-z0-9_\-]+", "", value) or "keyword"


def _extract_year(pub_date: str, url: str = "") -> Optional[int]:
    source = (pub_date or "").strip()
    if source and source != "no_date":
        year_match = re.search(r"(19|20)\d{2}", source)
        if year_match:
            return int(year_match.group(0))
    if url:
        year_from_url = re.search(r"/((19|20)\d{2})/\d{2}/\d{2}/", url)
        if year_from_url:
            return int(year_from_url.group(1))
    return None


def _extract_text_from_next_data_for_edge(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    node = soup.find("script", id="__NEXT_DATA__")
    if not node:
        return None

    try:
        payload = json.loads(node.get_text())
    except Exception:
        return None

    data = payload.get("props", {}).get("pageProps", {}).get("data", {})
    if not isinstance(data, dict):
        return None

    title = (data.get("title") or "").strip()
    summary = (data.get("summary") or "").strip()
    content_html = data.get("content") or ""
    content_text = BeautifulSoup(content_html, "html.parser").get_text("\n", strip=True) if content_html else ""
    content = content_text if content_text else summary
    if not title or not content:
        return None

    created = data.get("created")
    pub_date = "no_date"
    if isinstance(created, (int, float)):
        pub_date = datetime.fromtimestamp(created / 1000, tz=timezone.utc).isoformat()

    return {"title": title, "date": pub_date, "content": content}


def _extract_paragraph_fallback(soup: BeautifulSoup) -> str:
    texts: List[str] = []
    boilerplate = (
        "please update your browser",
        "can't find what you're looking for",
        "by registering, you agree",
        "privacy policy",
        "iklan",
    )

    for p in soup.select("article p, div p"):
        line = _text_or_none(p)
        if not line:
            continue
        low = line.casefold()
        if any(bp in low for bp in boilerplate):
            continue
        if len(line) < 40:
            continue
        texts.append(line)

    unique_lines = list(dict.fromkeys(texts))
    if len(unique_lines) < 3:
        return ""
    return "\n".join(unique_lines)


def parse_article(url: str, config: SourceConfig, timeout: int = 30, max_retries: int = 3) -> Optional[Dict[str, Any]]:
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "html.parser")

            title_node = _find_first(soup, config.title_selectors)
            content_node = _find_first(soup, config.content_selectors)
            date_node = _find_first(soup, config.date_selectors)

            title = _text_or_none(title_node) if title_node else None
            content = _text_or_none(content_node) if content_node else None
            pub_date = _text_or_none(date_node) if date_node else None

            if config.name == "The Edge Malaysia" and (not title or not content):
                edge_data = _extract_text_from_next_data_for_edge(soup)
                if edge_data:
                    title = edge_data["title"]
                    content = edge_data["content"]
                    pub_date = edge_data["date"]

            if not title:
                og_title = soup.select_one("meta[property='og:title']")
                title = _text_or_none(og_title) if og_title else None

            if not pub_date:
                pub_meta = soup.select_one("meta[property='article:published_time'], meta[name='pubdate']")
                pub_date = _text_or_none(pub_meta) if pub_meta else None

            if not content:
                content = _extract_paragraph_fallback(soup)

            if not title or not content:
                return None

            pub_date = pub_date or "no_date"
            year = _extract_year(pub_date, url=url)

            text = (
                f"Source: {config.name}\n"
                f"Region: {config.region}\n"
                f"Title: {title}\n"
                f"URL: {url}\n"
                f"Date: {pub_date}\n"
                f"Year: {year if year is not None else 'no_year'}\n\n"
                f"{content}\n"
            )
            return {
                "text": text,
                "title": title,
                "date": pub_date,
                "year": year,
                "content": content,
            }
        except requests.exceptions.Timeout:
            print(f"    Timeout {attempt + 1}/{max_retries}: {url}")
            if attempt < max_retries - 1:
                time.sleep(3)
        except requests.RequestException as exc:
            print(f"    Request error: {exc}")
            return None
        except Exception as exc:
            print(f"    Parse error: {exc}")
            return None

    return None


def _build_country_regexes() -> Dict[str, Dict[str, List[re.Pattern[str]]]]:
    out: Dict[str, Dict[str, List[re.Pattern[str]]]] = {}
    for country, profile in COUNTRY_PROFILES.items():
        out[country] = {
            "i": [re.compile(p, re.IGNORECASE) for p in profile["regex_i"]],
            "cs": [re.compile(p) for p in profile["regex_cs"]],
        }
    return out


def _build_theme_regexes() -> Dict[str, List[re.Pattern[str]]]:
    out: Dict[str, List[re.Pattern[str]]] = {}
    for theme, pats in THEME_PROFILES.items():
        out[theme] = [re.compile(p, re.IGNORECASE) for p in pats]
    return out


def detect_country_scores(title: str, content: str, compiled: Dict[str, Dict[str, List[re.Pattern[str]]]]) -> Dict[str, int]:
    sample = f"{title}\n{content[:2500]}"
    scores: Dict[str, int] = {}
    for country, pack in compiled.items():
        score = 0
        for pat in pack["i"]:
            score += len(pat.findall(sample))
        for pat in pack["cs"]:
            score += len(pat.findall(sample))
        if score > 0:
            scores[country] = score
    return scores


def detect_theme_scores(title: str, content: str, compiled: Dict[str, List[re.Pattern[str]]]) -> Dict[str, int]:
    sample = f"{title}\n{content[:4000]}"
    scores: Dict[str, int] = {}
    for theme, patterns in compiled.items():
        score = 0
        for pat in patterns:
            score += len(pat.findall(sample))
        if score > 0:
            scores[theme] = score
    return scores


def build_search_url(config: SourceConfig, keyword: str, page: int) -> str:
    return config.search_template.format(q=quote_plus(keyword), page=page)


def _extract_links_from_queryly(config: SourceConfig, keyword: str, page: int) -> List[str]:
    if not config.queryly_key:
        return []

    batch_size = 20
    endindex = (page - 1) * batch_size
    params = {
        "queryly_key": config.queryly_key,
        "query": keyword,
        "endindex": endindex,
        "batchsize": batch_size,
    }
    resp = requests.get("https://api.queryly.com/json.aspx", params=params, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        return []

    try:
        data = resp.json()
    except Exception:
        return []

    links: List[str] = []
    for item in data.get("items", []):
        link = (item.get("link") or "").strip()
        if link and link.startswith("http") and config.domain in urlparse(link).netloc:
            links.append(link)
    return list(dict.fromkeys(links))


def _extract_links_from_algolia_tempo(config: SourceConfig, keyword: str, page: int) -> List[str]:
    if not config.algolia_app_id or not config.algolia_api_key or not config.algolia_index:
        return []

    url = f"https://{config.algolia_app_id}-dsn.algolia.net/1/indexes/{config.algolia_index}/query"
    headers = {
        "X-Algolia-API-Key": config.algolia_api_key,
        "X-Algolia-Application-Id": config.algolia_app_id,
        "X-Algolia-Agent": "Algolia for JavaScript (4.24.0); Browser (lite)",
        "Content-Type": "application/json",
        "User-Agent": HEADERS["User-Agent"],
        "Origin": "https://www.tempo.co",
        "Referer": f"https://www.tempo.co/search?q={quote_plus(keyword)}",
    }
    payload = {
        "query": keyword,
        "page": max(page - 1, 0),
        "hitsPerPage": 20,
    }

    try:
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception:
        return []

    links: List[str] = []
    for hit in data.get("hits", []):
        canonical = str(hit.get("canonical_url") or "").strip()
        if not canonical:
            continue
        link = canonical if canonical.startswith("http") else f"https://www.tempo.co/{canonical.lstrip('/')}"
        if config.domain in urlparse(link).netloc:
            links.append(link)
    return list(dict.fromkeys(links))


def extract_article_links(soup: BeautifulSoup, config: SourceConfig) -> List[str]:
    links: List[str] = []
    seen = set()

    candidates = soup.select(config.article_link_selector) if config.article_link_selector else []
    if not candidates:
        candidates = soup.select("a[href]")

    for a_tag in candidates:
        href = (a_tag.get("href") or "").strip()
        if not href:
            continue

        href = urljoin(f"https://{config.domain}", href)
        if not href.startswith("http"):
            continue

        parsed = urlparse(href)
        if config.domain not in parsed.netloc:
            continue

        if config.article_url_contains and not any(token in href for token in config.article_url_contains):
            continue

        if config.article_url_regex and not re.search(config.article_url_regex, href):
            continue

        if href not in seen:
            seen.add(href)
            links.append(href)

    return links


def get_search_links(config: SourceConfig, keyword: str, page: int) -> List[str]:
    if config.search_mode == "queryly":
        return _extract_links_from_queryly(config, keyword, page)
    if config.search_mode == "algolia_tempo":
        return _extract_links_from_algolia_tempo(config, keyword, page)

    search_url = build_search_url(config, keyword, page)
    resp = requests.get(search_url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    return extract_article_links(soup, config)


def source_configs() -> List[SourceConfig]:
    return [
        SourceConfig(
            name="Astro Awani",
            region="Malaysia",
            domain="astroawani.com",
            search_template="https://www.astroawani.com/search?q={q}&page={page}",
            article_link_selector="a[href]",
            article_url_contains=("/berita-", "/news/", "/video/"),
            article_url_regex=r"-\d{4,}$",
            title_selectors=("h1.title", "h1.entry-title", "h1"),
            content_selectors=("div.field-items", "div.node__content", "div.article-body", "article"),
            date_selectors=("time", "span.date-display-single", "div.date", "meta[property='article:published_time']"),
        ),
        SourceConfig(
            name="Bernama",
            region="Malaysia",
            domain="bernama.com",
            search_template="https://www.bernama.com/en/search.php?cat1=&terms={q}&page={page}",
            article_link_selector="a[href]",
            article_url_contains=("/news.php?id=",),
            article_url_regex=r"news\.php\?id=\d+",
            title_selectors=("h1", "div.news-title h1", "div.title h1"),
            content_selectors=("div.news-content", "div.text-justify", "article", "div.entry-content"),
            date_selectors=("time", "div.news-date", "span.date", "meta[property='article:published_time']"),
        ),
        SourceConfig(
            name="The Star",
            region="Malaysia",
            domain="thestar.com.my",
            search_template="https://www.thestar.com.my/search?query={q}&pgno={page}",
            article_link_selector="a[href]",
            article_url_contains=("/news/", "/business/", "/sport/", "/aseanplus/", "/opinion/", "/lifestyle/", "/tech/"),
            article_url_regex=r"/\d{4}/\d{2}/\d{2}/",
            title_selectors=("h1", "h1.headline", "h1.article-title"),
            content_selectors=("div.story-body", "div.article-body", "article", "div#story-body"),
            date_selectors=("time", "p.timestamp", "div.date", "meta[property='article:published_time']"),
            search_mode="queryly",
            queryly_key="6ddd278bf17648ac",
        ),
        SourceConfig(
            name="The Edge Malaysia",
            region="Malaysia",
            domain="theedgemalaysia.com",
            search_template="https://theedgemalaysia.com/news-search-results/?keywords={q}&page={page}",
            article_link_selector="a[href]",
            article_url_contains=("/node/",),
            article_url_regex=r"/node/\d+",
            title_selectors=("h1", "h1.page-title", "h1.article-title"),
            content_selectors=("div.field-name-body", "div.article-content", "article", "div.content"),
            date_selectors=("time", "div.published-date", "span.date", "meta[property='article:published_time']"),
        ),
        SourceConfig(
            name="Kompas Indonesia",
            region="Indonesia",
            domain="kompas.com",
            search_template="https://search.kompas.com/search/?q={q}&page={page}",
            article_link_selector="a.article__link, a[href]",
            article_url_contains=("/read/",),
            article_url_regex=r"/read/\d{4}/",
            title_selectors=("h1.read__title", "h1"),
            content_selectors=("div.read__content", "article"),
            date_selectors=("div.read__time", "time", "meta[property='article:published_time']"),
        ),
        SourceConfig(
            name="Tempo",
            region="Indonesia",
            domain="tempo.co",
            search_template="https://www.tempo.co/search?q={q}&page={page}",
            article_link_selector="a[href]",
            article_url_contains=("/tempo.co/", "/"),
            article_url_regex=r"-\d{6,8}$",
            title_selectors=("h1", "h1.detail-title", "h1.title"),
            content_selectors=("div.detail-konten", "div.article-content", "article", "div.read-content"),
            date_selectors=("time", "p.date", "div.date", "meta[property='article:published_time']"),
            search_mode="algolia_tempo",
            algolia_app_id="U2CIAZRCAD",
            algolia_api_key="a74cdcfcc2c69b5dabb4d13c4ce52788",
            algolia_index="production_articles",
        ),
        SourceConfig(
            name="The Jakarta Post",
            region="Indonesia",
            domain="thejakartapost.com",
            search_template="https://www.thejakartapost.com/search?q={q}&page={page}",
            article_link_selector="a[href]",
            article_url_contains=("/202",),
            article_url_regex=r"/\d{4}/\d{2}/\d{2}/",
            title_selectors=("h1", "h1.title", "h1.headline"),
            content_selectors=("div.col-detail-content", "div.article-content", "article", "div.content"),
            date_selectors=("time", "div.article-date", "span.date", "meta[property='article:published_time']"),
        ),
        SourceConfig(
            name="Antara",
            region="Indonesia",
            domain="antaranews.com",
            search_template="https://www.antaranews.com/search?q={q}&page={page}",
            article_link_selector="a[href]",
            article_url_contains=("/berita/",),
            article_url_regex=r"/berita/\d+/",
            title_selectors=("h1.post-title", "h1", "h1.entry-title"),
            content_selectors=("div.post-content", "div#main-container", "article", "div.article-content"),
            date_selectors=("time", "span.post-date", "div.date", "meta[property='article:published_time']"),
        ),
    ]


def _should_keep_by_cap(
    cap: int,
    counter: Dict[Tuple[str, str, int], int],
    source_name: str,
    primary_country: str,
    year: int,
) -> bool:
    if cap <= 0:
        return True
    key = (source_name, primary_country, year)
    return counter.get(key, 0) < cap


def run_collection(
    sources: Sequence[SourceConfig],
    output_dir: Path,
    min_year: int,
    max_year: int,
    max_pages: int,
    cap_per_source_country_year: int,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    compiled = _build_country_regexes()
    theme_compiled = _build_theme_regexes()
    metadata_rows: List[Dict[str, Any]] = []

    # volumetric stats
    source_country_year_counts: Dict[Tuple[str, str, int], int] = {}
    source_theme_year_counts: Dict[Tuple[str, str, int], int] = {}
    theme_country_counts: Dict[Tuple[str, str], int] = {}
    source_total_saved: Dict[str, int] = {}
    cap_counter: Dict[Tuple[str, str, int], int] = {}

    print("=" * 90)
    print("POLITICAL DISCOURSE PARSER: USA + RUSSIA + CHINA")
    print("=" * 90)
    print(f"Date window: {min_year}-{max_year}")
    print(f"Output: {output_dir.resolve()}\n")

    for source in sources:
        print("=" * 90)
        print(f"SOURCE: {source.name} ({source.region})")
        print("=" * 90)

        source_dir = output_dir / source.name.lower().replace(" ", "_")
        source_dir.mkdir(parents=True, exist_ok=True)

        seen_links = set()
        source_total_saved[source.name] = 0

        for query_country, profile in COUNTRY_PROFILES.items():
            terms = profile["search_terms"]
            print(f"\n[COUNTRY QUERY: {query_country.upper()}]")

            for term in terms:
                print(f"  - Term: '{term}'")
                saved_for_term = 0

                for page in range(1, max_pages + 1):
                    try:
                        links = get_search_links(source, term, page)
                    except Exception as exc:
                        print(f"    Page {page}: search error: {exc}")
                        break

                    if not links:
                        break

                    for idx, link in enumerate(links, start=1):
                        if link in seen_links:
                            continue

                        article = parse_article(link, source)
                        if not article:
                            continue

                        year = article["year"]
                        if year is None or year < min_year or year > max_year:
                            continue

                        scores = detect_country_scores(article["title"], article["content"], compiled)
                        if not scores:
                            continue
                        theme_scores = detect_theme_scores(article["title"], article["content"], theme_compiled)
                        primary_theme = max(theme_scores.items(), key=lambda kv: kv[1])[0] if theme_scores else "general"
                        thematic_subcorpora = sorted(theme_scores.keys()) if theme_scores else ["general"]

                        primary_country = max(scores.items(), key=lambda kv: kv[1])[0]
                        if not _should_keep_by_cap(cap_per_source_country_year, cap_counter, source.name, primary_country, year):
                            continue

                        seen_links.add(link)
                        saved_for_term += 1
                        source_total_saved[source.name] += 1

                        cap_key = (source.name, primary_country, year)
                        cap_counter[cap_key] = cap_counter.get(cap_key, 0) + 1

                        for c in scores.keys():
                            stats_key = (source.name, c, year)
                            source_country_year_counts[stats_key] = source_country_year_counts.get(stats_key, 0) + 1
                        for theme in thematic_subcorpora:
                            source_theme_key = (source.name, theme, year)
                            source_theme_year_counts[source_theme_key] = source_theme_year_counts.get(source_theme_key, 0) + 1
                            theme_country_key = (theme, primary_country)
                            theme_country_counts[theme_country_key] = theme_country_counts.get(theme_country_key, 0) + 1

                        file_name = (
                            f"{source.name.lower().replace(' ', '_')}_"
                            f"{primary_country}_{primary_theme}_{year}_{source_total_saved[source.name]:05d}.txt"
                        )
                        file_path = source_dir / file_name
                        file_path.write_text(article["text"], encoding="utf-8")

                        metadata_rows.append(
                            {
                                "source": source.name,
                                "region": source.region,
                                "url": link,
                                "date": article["date"],
                                "year": year,
                                "title": article["title"],
                                "query_country": query_country,
                                "query_term": term,
                                "primary_country": primary_country,
                                "country_scores": json.dumps(scores, ensure_ascii=False),
                                "countries_detected": ";".join(sorted(scores.keys())),
                                "primary_subcorpus": primary_theme,
                                "subcorpora_detected": ";".join(thematic_subcorpora),
                                "theme_scores": json.dumps(theme_scores, ensure_ascii=False),
                                "file_path": str(file_path.resolve()),
                            }
                        )

                        if source_total_saved[source.name] % 50 == 0:
                            print(f"    Saved so far: {source_total_saved[source.name]}")

                        time.sleep(0.6)

                    time.sleep(1.0)

                print(f"    Saved for term '{term}': {saved_for_term}")

    metadata_path = output_dir / "metadata.csv"
    with metadata_path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "source",
            "region",
            "url",
            "date",
            "year",
            "title",
            "query_country",
            "query_term",
            "primary_country",
            "country_scores",
            "countries_detected",
            "primary_subcorpus",
            "subcorpora_detected",
            "theme_scores",
            "file_path",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(metadata_rows)

    summary_path = output_dir / "summary_source_country_year.csv"
    with summary_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["source", "country", "year", "count"])
        for (source_name, country, year), count in sorted(source_country_year_counts.items()):
            writer.writerow([source_name, country, year, count])

    summary_source_country: Dict[Tuple[str, str], int] = {}
    for (source_name, country, _year), count in source_country_year_counts.items():
        key = (source_name, country)
        summary_source_country[key] = summary_source_country.get(key, 0) + count

    summary_source_country_path = output_dir / "summary_source_country.csv"
    with summary_source_country_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["source", "country", "count"])
        for (source_name, country), count in sorted(summary_source_country.items()):
            writer.writerow([source_name, country, count])

    summary_theme_year_path = output_dir / "summary_source_subcorpus_year.csv"
    with summary_theme_year_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["source", "subcorpus", "year", "count"])
        for (source_name, theme, year), count in sorted(source_theme_year_counts.items()):
            writer.writerow([source_name, theme, year, count])

    summary_subcorpus_country_path = output_dir / "summary_subcorpus_country.csv"
    with summary_subcorpus_country_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["subcorpus", "country", "count"])
        for (theme, country), count in sorted(theme_country_counts.items()):
            writer.writerow([theme, country, count])

    print("\n" + "=" * 90)
    print("FINAL SUMMARY")
    print("=" * 90)
    for source_name in sorted(source_total_saved.keys()):
        print(f"{source_name}: {source_total_saved[source_name]} saved articles")

    print("-" * 90)
    print(f"TOTAL SAVED: {sum(source_total_saved.values())}")
    print(f"Metadata CSV: {metadata_path.resolve()}")
    print(f"Summary CSV: {summary_path.resolve()}")
    print(f"Summary Source-Country CSV: {summary_source_country_path.resolve()}")
    print(f"Summary Source-Subcorpus-Year CSV: {summary_theme_year_path.resolve()}")
    print(f"Summary Subcorpus-Country CSV: {summary_subcorpus_country_path.resolve()}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect discourse on USA/Russia/China from MY+ID media")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory")
    parser.add_argument("--min-year", type=int, default=DEFAULT_MIN_YEAR, help="Minimum publication year")
    parser.add_argument("--max-year", type=int, default=DEFAULT_MAX_YEAR, help="Maximum publication year")
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES, help="Max search pages per term")
    parser.add_argument(
        "--cap-per-source-country-year",
        type=int,
        default=0,
        help="Optional cap for balancing per (source,country,year). 0 = no cap",
    )
    parser.add_argument(
        "--start-2022",
        action="store_true",
        help="Shortcut to set min-year=2022 for wider representativity",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    min_year = 2022 if args.start_2022 else args.min_year
    max_year = args.max_year
    if min_year > max_year:
        raise ValueError("min_year cannot be greater than max_year")

    run_collection(
        sources=source_configs(),
        output_dir=Path(args.output_dir),
        min_year=min_year,
        max_year=max_year,
        max_pages=args.max_pages,
        cap_per_source_country_year=max(0, args.cap_per_source_country_year),
    )


if __name__ == "__main__":
    main()
