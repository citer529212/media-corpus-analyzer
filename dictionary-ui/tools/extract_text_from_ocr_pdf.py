#!/usr/bin/env python3
import argparse
import re
import subprocess
import tempfile
from pathlib import Path

OBJ_RE = re.compile(rb"(?m)^(\d+) 0 obj\s*\n")
REF_TOUNICODE_RE = re.compile(rb"/ToUnicode\s+(\d+)\s+0\s+R")
PAGE_RE = re.compile(rb"/Type\s*/Page\b")
OCR_FORM_RE = re.compile(rb"/OCR-[^\s/]+\s+(\d+)\s+0\s+R")
BFC_RE = re.compile(r"<([0-9A-Fa-f]+)>\s+<([0-9A-Fa-f]+)>")


def parse_objects(blob: bytes):
    starts = list(OBJ_RE.finditer(blob))
    objects = {}
    order = []
    for i, m in enumerate(starts):
        obj_id = int(m.group(1))
        s = m.end()
        e = starts[i + 1].start() if i + 1 < len(starts) else len(blob)
        chunk = blob[s:e]
        j = chunk.find(b"endobj")
        if j != -1:
            chunk = chunk[:j]
        objects[obj_id] = chunk
        order.append(obj_id)
    return objects, order


def get_stream(obj_chunk: bytes):
    i = obj_chunk.find(b"stream\n")
    sep = b"stream\n"
    if i == -1:
        i = obj_chunk.find(b"stream\r\n")
        sep = b"stream\r\n"
    if i == -1:
        return None
    s = i + len(sep)
    e = obj_chunk.find(b"endstream", s)
    if e == -1:
        return None
    return obj_chunk[s:e]


def parse_cmap(stream_bytes: bytes):
    cmap = {}
    text = stream_bytes.decode("latin1", errors="ignore")
    for line in text.splitlines():
        m = BFC_RE.search(line)
        if not m:
            continue
        cid = int(m.group(1), 16)
        raw = bytes.fromhex(m.group(2))
        try:
            val = raw.decode("utf-16-be", errors="ignore")
        except Exception:
            val = ""
        cmap[cid] = val
    return cmap


def parse_literal(buf: bytes, i: int):
    assert buf[i] == 40
    i += 1
    out = bytearray()
    depth = 1
    while i < len(buf):
        c = buf[i]
        if c == 92:
            i += 1
            if i >= len(buf):
                break
            c2 = buf[i]
            if c2 in b"nrtbf":
                out.extend({110: b"\n", 114: b"\r", 116: b"\t", 98: b"\b", 102: b"\f"}[c2])
            elif c2 in b"()\\":
                out.append(c2)
            elif 48 <= c2 <= 55:
                octs = [c2]
                i += 1
                for _ in range(2):
                    if i < len(buf) and 48 <= buf[i] <= 55:
                        octs.append(buf[i])
                        i += 1
                    else:
                        break
                i -= 1
                out.append(int(bytes(octs), 8))
            else:
                out.append(c2)
        elif c == 40:
            depth += 1
            out.append(c)
        elif c == 41:
            depth -= 1
            if depth == 0:
                return bytes(out), i + 1
            out.append(c)
        else:
            out.append(c)
        i += 1
    return bytes(out), i


def decode_bytes(bs: bytes, cmap: dict[int, str]):
    if not bs:
        return ""
    if len(bs) % 2 == 0:
        out = []
        for j in range(0, len(bs), 2):
            cid = (bs[j] << 8) | bs[j + 1]
            out.append(cmap.get(cid, ""))
        text = "".join(out)
        if text.strip():
            return text
    return "".join(cmap.get(b, "") for b in bs)


def extract_stream_text(stream: bytes, cmap: dict[int, str]):
    if not stream:
        return ""
    i = 0
    chunks = []
    while i < len(stream):
        if stream[i] == 40:
            s, ni = parse_literal(stream, i)
            j = ni
            while j < len(stream) and stream[j] in b" \t\r\n":
                j += 1
            if stream[j:j + 2] == b"Tj":
                chunks.append(decode_bytes(s, cmap))
            i = ni
            continue

        if stream[i] == 91:
            j = i + 1
            arr = []
            while j < len(stream) and stream[j] != 93:
                if stream[j] == 40:
                    s, nj = parse_literal(stream, j)
                    arr.append(decode_bytes(s, cmap))
                    j = nj
                else:
                    j += 1
            k = j + 1
            while k < len(stream) and stream[k] in b" \t\r\n":
                k += 1
            if stream[k:k + 2] == b"TJ":
                chunks.append("".join(arr))
            i = j + 1
            continue

        i += 1

    text = "\n".join(x.strip() for x in chunks if x and x.strip())
    text = re.sub(r"[ \t]+", " ", text).strip()
    return text


def extract_pages(input_pdf: Path, output_txt: Path, start: int, end: int, chunk_size: int):
    pages_out = []

    for chunk_start in range(start, end + 1, chunk_size):
        chunk_end = min(end, chunk_start + chunk_size - 1)
        with tempfile.TemporaryDirectory() as td:
            qdf_path = Path(td) / "chunk_qdf.pdf"
            cmd = [
                "qpdf",
                str(input_pdf),
                "--pages",
                str(input_pdf),
                f"{chunk_start}-{chunk_end}",
                "--",
                "--stream-data=uncompress",
                "--qdf",
                str(qdf_path),
            ]
            subprocess.run(cmd, check=True)
            blob = qdf_path.read_bytes()

        objects, order = parse_objects(blob)

        cmap = {}
        to_unicode_ids = set()
        for obj_id in order:
            chunk = objects[obj_id]
            for m in REF_TOUNICODE_RE.finditer(chunk):
                to_unicode_ids.add(int(m.group(1)))

        for tid in sorted(to_unicode_ids):
            if tid not in objects:
                continue
            stream = get_stream(objects[tid])
            if not stream:
                continue
            cmap.update(parse_cmap(stream))

        page_obj_ids = [oid for oid in order if PAGE_RE.search(objects[oid])]
        for oid in page_obj_ids:
            page_obj = objects[oid]
            mo = OCR_FORM_RE.search(page_obj)
            if not mo:
                pages_out.append("")
                continue
            text_obj_id = int(mo.group(1))
            stream = get_stream(objects.get(text_obj_id, b""))
            pages_out.append(extract_stream_text(stream or b"", cmap))

        print(f"Processed pages {chunk_start}-{chunk_end}")

    output_txt.parent.mkdir(parents=True, exist_ok=True)
    output_txt.write_text("\f".join(pages_out), encoding="utf-8")
    print(f"Saved extracted text to {output_txt}")


def main():
    parser = argparse.ArgumentParser(description="Extract text layer from OCR PDF using qpdf")
    parser.add_argument("--input", required=True, help="Input OCR PDF path")
    parser.add_argument("--output", required=True, help="Output TXT path")
    parser.add_argument("--start", type=int, default=1, help="Start page (1-based)")
    parser.add_argument("--end", type=int, default=1031, help="End page (1-based)")
    parser.add_argument("--chunk-size", type=int, default=80, help="Pages per qpdf chunk")
    args = parser.parse_args()

    extract_pages(
        input_pdf=Path(args.input),
        output_txt=Path(args.output),
        start=args.start,
        end=args.end,
        chunk_size=args.chunk_size,
    )


if __name__ == "__main__":
    main()
