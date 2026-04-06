#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <input-pdf> <output-sidecar-txt> [tmp-output-pdf]"
  exit 1
fi

INPUT_PDF="$1"
SIDECAR_TXT="$2"
TMP_OUT="${3:-/tmp/dictionary_sidecar_tmp.pdf}"

ocrmypdf --redo-ocr --sidecar "$SIDECAR_TXT" "$INPUT_PDF" "$TMP_OUT"

echo "Sidecar saved to: $SIDECAR_TXT"
echo "Temporary OCR PDF: $TMP_OUT"
