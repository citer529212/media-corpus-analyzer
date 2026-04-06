#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path

HEAD_RE = re.compile(r"^([a-z][a-z0-9'\-]{1,})(?:\s+[ivx]{1,4})?(?:\s+[12])?\s+(.+)$", re.I)


def clean(text: str) -> str:
    text = text.replace("\u00ad", "")
    text = text.replace("|", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip(" -\t")


def looks_like_translation(text: str) -> bool:
    text = clean(text)
    if len(text) < 2:
        return False
    if re.search(r"[;,:~()]", text):
        return True
    if " " in text:
        return True
    if re.search(r"[A-ZА-ЯЁ]", text):
        return True
    return True


def parse_sidecar(content: str):
    pages = content.split("\f")
    entries = []

    for page_idx, page in enumerate(pages, start=1):
        lines = [clean(x) for x in page.splitlines()]
        lines = [x for x in lines if x]

        current = None

        def flush_current():
            nonlocal current
            if not current:
                return
            current["title"] = clean(current["title"]).lower()
            current["body"] = clean(current["body"])
            if (
                current["title"]
                and 2 <= len(current["title"]) <= 24
                and looks_like_translation(current["body"])
            ):
                entries.append(current)
            current = None

        for line in lines:
            if line.startswith("[OCR skipped on page"):
                continue

            m = HEAD_RE.match(line)
            if m and looks_like_translation(m.group(2)):
                flush_current()
                current = {
                    "title": m.group(1),
                    "body": m.group(2),
                    "page": page_idx,
                }
                continue

            if current:
                # Join wrapped translation lines.
                if len(line) <= 220 and (
                    looks_like_translation(line) or line.startswith("(") or line.startswith("~")
                ):
                    current["body"] += " " + line
                    continue

                # Next potential entry without cyr text (OCR gap) -> stop the previous one.
                if re.match(r"^[a-z][a-z0-9'\-]{1,}$", line, re.I):
                    flush_current()
                    continue

        flush_current()

    # Deduplicate keeping first page.
    dedup = {}
    for entry in entries:
        key = (entry["title"], entry["body"])
        if key not in dedup:
            dedup[key] = entry
        else:
            dedup[key]["page"] = min(dedup[key]["page"], entry["page"])

    final_entries = []
    for idx, item in enumerate(sorted(dedup.values(), key=lambda x: (x["title"], x["page"]))):
        final_entries.append(
            {
                "id": f"lex-{idx}",
                "type": "entry",
                "title": item["title"],
                "body": item["body"],
                "page": item["page"],
            }
        )

    return final_entries


def main():
    parser = argparse.ArgumentParser(description="Build dictionary JSON from OCR sidecar text")
    parser.add_argument("--sidecar", required=True, help="Path to sidecar .txt from ocrmypdf")
    parser.add_argument("--output", required=True, help="Output JSON path")
    args = parser.parse_args()

    sidecar_path = Path(args.sidecar)
    output_path = Path(args.output)

    content = sidecar_path.read_text(encoding="utf-8", errors="ignore")
    entries = parse_sidecar(content)

    payload = {
        "version": "1",
        "source": sidecar_path.name,
        "entries": entries,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"Built {len(entries)} entries -> {output_path}")


if __name__ == "__main__":
    main()
