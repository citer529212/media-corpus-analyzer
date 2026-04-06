import * as pdfjsLib from "https://cdn.jsdelivr.net/npm/pdfjs-dist@4.5.136/build/pdf.min.mjs";

pdfjsLib.GlobalWorkerOptions.workerSrc =
  "https://cdn.jsdelivr.net/npm/pdfjs-dist@4.5.136/build/pdf.worker.min.mjs";

const ui = {
  fileInput: document.getElementById("pdfFile"),
  fileMeta: document.getElementById("fileMeta"),
  searchInput: document.getElementById("searchInput"),
  searchButton: document.getElementById("searchButton"),
  searchModeSwitch: document.getElementById("searchModeSwitch"),
  searchHistory: document.getElementById("searchHistory"),
  answerCard: document.getElementById("answerCard"),
  answerTitle: document.getElementById("answerTitle"),
  answerBody: document.getElementById("answerBody"),
  answerMeta: document.getElementById("answerMeta"),
  resultCount: document.getElementById("resultCount"),
  resultList: document.getElementById("resultList"),
  statusText: document.getElementById("statusText"),
  progressBar: document.getElementById("progressBar"),
  pdfCanvas: document.getElementById("pdfCanvas"),
  pageLabel: document.getElementById("pageLabel"),
  prevPage: document.getElementById("prevPage"),
  nextPage: document.getElementById("nextPage"),
  zoomOut: document.getElementById("zoomOut"),
  zoomIn: document.getElementById("zoomIn"),
  zoomValue: document.getElementById("zoomValue"),
};

const SEARCH_HISTORY_KEY = "dictionary-shell:search-history:v2";
const INDEX_VERSION = "v4-columns-context";
const MAX_RESULTS = 300;
const MAX_HISTORY_ITEMS = 8;

const state = {
  pdfDoc: null,
  entries: [],
  lines: [],
  pageTexts: [],
  results: [],
  currentPage: 1,
  zoom: 1,
  renderToken: 0,
  selectedResultId: null,
  bestAnswer: null,
  searchMode: "entries",
  history: [],
  activeQuery: "",
};

ui.fileInput.addEventListener("change", async (event) => {
  const [file] = event.target.files || [];
  if (!file) {
    return;
  }

  await loadPdfFile(file);
});

ui.searchButton.addEventListener("click", runSearch);

ui.searchInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    runSearch();
  }
});

let inputDebounceId = null;
ui.searchInput.addEventListener("input", () => {
  clearTimeout(inputDebounceId);
  inputDebounceId = setTimeout(runSearch, 120);
});

ui.searchModeSwitch.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-mode]");
  if (!button || button.disabled) {
    return;
  }

  state.searchMode = button.dataset.mode;
  updateModeButtons();
  runSearch();
});

ui.searchHistory.addEventListener("click", (event) => {
  const chip = event.target.closest("button[data-query]");
  if (!chip || chip.disabled) {
    return;
  }

  ui.searchInput.value = chip.dataset.query || "";
  runSearch();
});

ui.prevPage.addEventListener("click", () => {
  if (!state.pdfDoc || state.currentPage <= 1) {
    return;
  }

  state.currentPage -= 1;
  void renderCurrentPage();
});

ui.nextPage.addEventListener("click", () => {
  if (!state.pdfDoc || state.currentPage >= state.pdfDoc.numPages) {
    return;
  }

  state.currentPage += 1;
  void renderCurrentPage();
});

ui.zoomIn.addEventListener("click", () => {
  if (!state.pdfDoc) {
    return;
  }

  state.zoom = Math.min(state.zoom + 0.15, 2.4);
  updateZoomLabel();
  void renderCurrentPage();
});

ui.zoomOut.addEventListener("click", () => {
  if (!state.pdfDoc) {
    return;
  }

  state.zoom = Math.max(state.zoom - 0.15, 0.55);
  updateZoomLabel();
  void renderCurrentPage();
});

function setStatus(message) {
  ui.statusText.textContent = message;
}

function setProgress(percent) {
  ui.progressBar.style.width = `${Math.max(0, Math.min(100, percent))}%`;
}

function setSearchAvailability(available) {
  ui.searchInput.disabled = !available;
  ui.searchButton.disabled = !available;

  ui.searchModeSwitch
    .querySelectorAll("button")
    .forEach((button) => (button.disabled = !available));

  ui.searchHistory
    .querySelectorAll("button")
    .forEach((button) => (button.disabled = !available));
}

function updateModeButtons() {
  ui.searchModeSwitch.querySelectorAll("button[data-mode]").forEach((button) => {
    button.classList.toggle("active", button.dataset.mode === state.searchMode);
  });
}

function updateViewerControls() {
  const hasPdf = Boolean(state.pdfDoc);
  ui.prevPage.disabled = !hasPdf || state.currentPage <= 1;
  ui.nextPage.disabled = !hasPdf || state.currentPage >= state.pdfDoc.numPages;
  ui.zoomOut.disabled = !hasPdf;
  ui.zoomIn.disabled = !hasPdf;
}

function updateZoomLabel() {
  ui.zoomValue.textContent = `${Math.round(state.zoom * 100)}%`;
}

function formatSize(bytes) {
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }

  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function normalizeText(text) {
  return text
    .toLowerCase()
    .replace(/[ё]/g, "е")
    .replace(/[’`´]/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeHeadwordLoose(text) {
  return normalizeText(text)
    .replace(/[^a-z0-9\s-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanupLine(text) {
  return text.replace(/\s+/g, " ").trim();
}

function buildCacheKey(file) {
  return `dictionary-shell:${INDEX_VERSION}:${file.name}:${file.size}:${file.lastModified}`;
}

function loadHistory() {
  try {
    const parsed = JSON.parse(localStorage.getItem(SEARCH_HISTORY_KEY) || "[]");
    if (Array.isArray(parsed)) {
      state.history = parsed.filter((item) => typeof item === "string");
    }
  } catch {
    state.history = [];
  }

  renderHistory();
}

function persistHistory() {
  localStorage.setItem(SEARCH_HISTORY_KEY, JSON.stringify(state.history));
}

function pushHistory(query) {
  if (!query || query.length < 2) {
    return;
  }

  state.history = [query, ...state.history.filter((item) => item !== query)].slice(
    0,
    MAX_HISTORY_ITEMS
  );

  persistHistory();
  renderHistory();
}

function renderHistory() {
  ui.searchHistory.innerHTML = "";

  state.history.forEach((query) => {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "history-chip";
    chip.dataset.query = query;
    chip.textContent = query;
    chip.disabled = !state.pdfDoc;
    ui.searchHistory.append(chip);
  });
}

function escapeHtml(text) {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeRegExp(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildBigrams(text) {
  const value = normalizeText(text).replace(/\s+/g, "");
  if (value.length < 2) {
    return new Set([value]);
  }

  const grams = new Set();
  for (let i = 0; i < value.length - 1; i += 1) {
    grams.add(value.slice(i, i + 2));
  }
  return grams;
}

function jaccardScore(a, b) {
  if (!a.size || !b.size) {
    return 0;
  }

  let inter = 0;
  a.forEach((token) => {
    if (b.has(token)) {
      inter += 1;
    }
  });

  return inter / (a.size + b.size - inter);
}

function highlightText(text, query) {
  const safe = escapeHtml(text);
  if (!query) {
    return safe;
  }

  const pattern = escapeRegExp(query);
  if (!pattern) {
    return safe;
  }

  return safe.replace(new RegExp(pattern, "gi"), (match) => `<mark>${match}</mark>`);
}

function looksLikeHeadword(text) {
  const value = text.trim();
  if (!value || value.length < 2 || value.length > 72) {
    return false;
  }

  if (!/[a-z]/i.test(value) || /[а-яё]/i.test(value)) {
    return false;
  }

  return value.split(/\s+/).length <= 6;
}

function cleanHeadword(text) {
  return text
    .replace(/^[^a-z]+/i, "")
    .replace(/[^a-z\-'.()\s]/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractEntry(line, page, localLineIndex) {
  const normalized = cleanupLine(line);
  if (normalized.length < 4) {
    return null;
  }

  const candidates = [
    normalized.split(/\s+[—–-]\s+/),
    normalized.split(/\s{2,}/),
    normalized.split(/\s+:\s+/),
  ];

  for (const split of candidates) {
    if (split.length < 2) {
      continue;
    }

    const left = cleanHeadword(split[0]);
    const right = split.slice(1).join(" ").trim();

    if (looksLikeHeadword(left) && /[а-яё]/i.test(right)) {
      return {
        id: `entry-${page}-${localLineIndex}-${left.slice(0, 8)}`,
        type: "entry",
        title: left,
        body: right,
        page,
      };
    }
  }

  const firstCyr = normalized.search(/[а-яё]/i);
  if (firstCyr > 1) {
    const left = cleanHeadword(normalized.slice(0, firstCyr));
    const right = normalized.slice(firstCyr).trim();
    if (looksLikeHeadword(left) && right.length > 1) {
      return {
        id: `entry-${page}-${localLineIndex}-c`,
        type: "entry",
        title: left,
        body: right,
        page,
      };
    }
  }

  return null;
}

function looksLikeTranslationLine(text) {
  const line = cleanupLine(text);
  if (!line || line.length < 2) {
    return false;
  }
  return /[а-яё]/i.test(line);
}

function extractEntryWithContext(lines, index, page) {
  const line = cleanupLine(lines[index] || "");
  if (!line) {
    return null;
  }

  // Case 1: headword-only line, translation in the next line(s).
  if (looksLikeHeadword(line)) {
    const next = cleanupLine(lines[index + 1] || "");
    const next2 = cleanupLine(lines[index + 2] || "");

    if (looksLikeTranslationLine(next)) {
      return {
        id: `entry-${page}-${index}-ctx1`,
        type: "entry",
        title: cleanHeadword(line),
        body: next,
        page,
      };
    }

    if (looksLikeTranslationLine(next2)) {
      return {
        id: `entry-${page}-${index}-ctx2`,
        type: "entry",
        title: cleanHeadword(line),
        body: next2,
        page,
      };
    }
  }

  // Case 2: first token is headword, translation moved to the next line.
  const firstToken = cleanHeadword(line.split(/\s+/)[0] || "");
  const next = cleanupLine(lines[index + 1] || "");
  if (looksLikeHeadword(firstToken) && looksLikeTranslationLine(next)) {
    const normalizedLine = normalizeText(line);
    if (!/[а-яё]/i.test(line) && normalizedLine.length <= 38) {
      return {
        id: `entry-${page}-${index}-ctx3`,
        type: "entry",
        title: firstToken,
        body: next,
        page,
      };
    }
  }

  return null;
}

function splitIntoColumns(textItems, pageWidth) {
  if (textItems.length < 80) {
    return [textItems];
  }

  const xs = textItems
    .map((item) => item.transform?.[4] || 0)
    .filter((x) => Number.isFinite(x));

  if (xs.length < 80) {
    return [textItems];
  }

  let c1 = Math.min(...xs);
  let c2 = Math.max(...xs);
  if (Math.abs(c2 - c1) < pageWidth * 0.28) {
    return [textItems];
  }

  for (let i = 0; i < 8; i += 1) {
    const g1 = [];
    const g2 = [];
    xs.forEach((x) => {
      if (Math.abs(x - c1) <= Math.abs(x - c2)) {
        g1.push(x);
      } else {
        g2.push(x);
      }
    });
    if (!g1.length || !g2.length) {
      return [textItems];
    }
    c1 = g1.reduce((acc, x) => acc + x, 0) / g1.length;
    c2 = g2.reduce((acc, x) => acc + x, 0) / g2.length;
  }

  if (Math.abs(c2 - c1) < pageWidth * 0.28) {
    return [textItems];
  }

  const left = [];
  const right = [];
  textItems.forEach((item) => {
    const x = item.transform?.[4] || 0;
    if (Math.abs(x - c1) <= Math.abs(x - c2)) {
      left.push(item);
    } else {
      right.push(item);
    }
  });

  if (left.length < 20 || right.length < 20) {
    return [textItems];
  }

  return c1 <= c2 ? [left, right] : [right, left];
}

function groupLinesSingleColumn(textItems) {
  const buckets = new Map();

  for (const item of textItems) {
    if (!item.str) {
      continue;
    }

    const text = cleanupLine(item.str);
    if (!text) {
      continue;
    }

    const x = item.transform[4] || 0;
    const y = Math.round((item.transform[5] || 0) * 2) / 2;

    if (!buckets.has(y)) {
      buckets.set(y, []);
    }

    buckets.get(y).push({ x, text });
  }

  const lines = [];

  for (const y of [...buckets.keys()].sort((a, b) => b - a)) {
    const joined = buckets
      .get(y)
      .sort((a, b) => a.x - b.x)
      .map((part) => part.text)
      .join(" ")
      .replace(/\s+/g, " ")
      .trim();

    if (joined) {
      lines.push(joined);
    }
  }

  return lines;
}

function groupLines(textItems, pageWidth) {
  const columns = splitIntoColumns(textItems, pageWidth);
  const lines = [];

  columns.forEach((columnItems) => {
    lines.push(...groupLinesSingleColumn(columnItems));
  });

  return lines;
}

function deduplicateEntries(entries) {
  const seen = new Set();
  const result = [];

  entries.forEach((entry) => {
    const key = `${normalizeText(entry.title)}|${normalizeText(entry.body)}|${entry.page}`;
    if (seen.has(key)) {
      return;
    }

    seen.add(key);
    result.push(entry);
  });

  return result;
}

function hydrateEntries(entries) {
  entries.forEach((entry) => {
    entry._normTitle = normalizeText(entry.title);
    entry._normBody = normalizeText(entry.body);
    entry._grams = buildBigrams(entry._normTitle);
  });
}

async function buildTextIndex(cacheKey) {
  const cached = localStorage.getItem(cacheKey);
  if (cached) {
    try {
      const parsed = JSON.parse(cached);
      if (
        Array.isArray(parsed.entries) &&
        Array.isArray(parsed.lines) &&
        Array.isArray(parsed.pageTexts)
      ) {
        state.entries = parsed.entries;
        state.lines = parsed.lines;
        state.pageTexts = parsed.pageTexts;
        hydrateEntries(state.entries);
        setStatus(
          `Загружен индекс: ${state.entries.length} статей, ${state.lines.length} текстовых строк.`
        );
        setProgress(100);
        setSearchAvailability(true);
        runSearch();
        return;
      }
    } catch {
      localStorage.removeItem(cacheKey);
    }
  }

  state.entries = [];
  state.lines = [];
  state.pageTexts = [];

  const pageCount = state.pdfDoc.numPages;

  for (let pageNum = 1; pageNum <= pageCount; pageNum += 1) {
    const page = await state.pdfDoc.getPage(pageNum);
    const content = await page.getTextContent();
    const viewport = page.getViewport({ scale: 1 });
    const lines = groupLines(content.items, viewport.width);

    state.pageTexts.push(lines.join("\n"));

    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index];
      state.lines.push({
        id: `line-${pageNum}-${index}`,
        type: "fulltext",
        title: `Фрагмент текста`,
        body: line,
        page: pageNum,
      });

      let entry = extractEntry(line, pageNum, index);
      if (!entry) {
        entry = extractEntryWithContext(lines, index, pageNum);
      }
      if (entry) {
        state.entries.push(entry);
      }
    }

    const percent = Math.round((pageNum / pageCount) * 100);
    setProgress(percent);
    setStatus(`Индексация PDF: ${pageNum}/${pageCount} страниц...`);
  }

  state.entries = deduplicateEntries(state.entries);
  hydrateEntries(state.entries);

  const payload = JSON.stringify({
    entries: state.entries,
    lines: state.lines,
    pageTexts: state.pageTexts,
  });

  if (payload.length < 7_000_000) {
    localStorage.setItem(cacheKey, payload);
  }

  setSearchAvailability(true);
  setStatus(
    `Готово: ${state.entries.length} статей, ${state.lines.length} OCR-строк доступны для поиска.`
  );

  runSearch();
}

function rankMatch(haystack, query) {
  if (!haystack || !query) {
    return 99;
  }

  if (haystack === query) {
    return 0;
  }

  if (haystack.startsWith(query)) {
    return 1;
  }

  if (new RegExp(`(^|\\s)${escapeRegExp(query)}`).test(haystack)) {
    return 2;
  }

  if (haystack.includes(query)) {
    return 3;
  }

  return 99;
}

function searchEntries(query) {
  if (!query) {
    return state.entries.slice(0, MAX_RESULTS).map((entry) => ({
      ...entry,
      _rank: 10,
    }));
  }

  const q = normalizeText(query);
  const qGrams = buildBigrams(q);
  const queryPattern = new RegExp(`\\b${escapeRegExp(q)}\\b`);
  const queryWords = q.split(/\s+/).filter(Boolean);
  const isSingleWordQuery = queryWords.length === 1;

  return state.entries
    .map((entry) => {
      const title = entry._normTitle;
      const body = entry._normBody;
      const titleWords = (title || "").split(/\s+/).filter(Boolean);
      const titleWordCount = titleWords.length || 1;
      const lenDelta = Math.abs((title || "").length - q.length);
      let score = 99;

      if (title === q) {
        score = 0;
      } else if (queryPattern.test(title)) {
        score = 0.2 + lenDelta / 50;
      } else if (title.startsWith(q)) {
        // Prefix matches are useful, but shorter/closer words should rank above derivatives.
        score = 0.9 + lenDelta / 20;
      } else if (title.includes(q)) {
        score = 1.8 + lenDelta / 18;
      } else if (body.includes(q)) {
        score = 3.9;
      }

      const fuzzy = jaccardScore(entry._grams, qGrams);
      if (score >= 99 && fuzzy >= 0.65) {
        score = 5.4 + (1 - fuzzy) * 2;
      }

      // For one-word queries, prefer one-word headwords over phrases.
      if (isSingleWordQuery && titleWordCount > 1 && score < 99) {
        score += 1.3 + Math.min(1.2, (titleWordCount - 1) * 0.45);
      }

      return { score, lenDelta, titleWordCount, entry };
    })
    .filter((row) => row.score < 99)
    .sort(
      (a, b) =>
        a.score - b.score ||
        a.titleWordCount - b.titleWordCount ||
        a.lenDelta - b.lenDelta ||
        a.entry.title.length - b.entry.title.length ||
        a.entry.page - b.entry.page
    )
    .slice(0, MAX_RESULTS)
    .map((row) => ({
      ...row.entry,
      _rank: row.score,
      _lenDelta: row.lenDelta,
      _wordCount: row.titleWordCount,
      _exactTitle: normalizeText(row.entry.title) === q,
    }));
}

function groupEntryResults(rows, query = "") {
  const groups = new Map();
  const qNorm = normalizeText(query);

  rows.forEach((row) => {
    const key = row._normTitle || normalizeText(row.title);
    const rowDelta = Math.abs((key || "").length - qNorm.length);

    if (!groups.has(key)) {
      groups.set(key, {
        id: `group-${key}`,
        type: "entry",
        title: row.title,
        body: row.body,
        page: row.page,
        _rank: row._rank || 10,
        _lenDelta: Number.isFinite(row._lenDelta) ? row._lenDelta : rowDelta,
        _wordCount: Number.isFinite(row._wordCount) ? row._wordCount : key.split(/\s+/).length,
        _exactTitle: Boolean(row._exactTitle),
      });
      return;
    }

    const current = groups.get(key);
    if (!current.body.includes(row.body) && current.body.length < 280) {
      current.body = `${current.body}; ${row.body}`.slice(0, 340);
    }
    current.page = Math.min(current.page, row.page);
    current._rank = Math.min(current._rank, row._rank || 10);
    current._lenDelta = Math.min(
      current._lenDelta,
      Number.isFinite(row._lenDelta) ? row._lenDelta : rowDelta
    );
    current._wordCount = Math.min(
      current._wordCount,
      Number.isFinite(row._wordCount) ? row._wordCount : key.split(/\s+/).length
    );
    current._exactTitle = current._exactTitle || Boolean(row._exactTitle);
  });

  return [...groups.values()]
    .sort(
      (a, b) =>
        Number(b._exactTitle) - Number(a._exactTitle) ||
        a._rank - b._rank ||
        a._wordCount - b._wordCount ||
        a._lenDelta - b._lenDelta ||
        a.page - b.page
    )
    .slice(0, MAX_RESULTS);
}

function searchFulltext(query) {
  if (!query) {
    return state.lines.slice(0, MAX_RESULTS).map((row) => ({ ...row, _rank: 10 }));
  }

  const q = normalizeText(query);

  return state.lines
    .map((row) => {
      const hay = normalizeText(row.body);
      const score = rankMatch(hay, q);
      return { score, row };
    })
    .filter((item) => item.score < 99)
    .sort((a, b) => a.score - b.score || a.row.page - b.row.page)
    .slice(0, MAX_RESULTS)
    .map((item) => ({ ...item.row, _rank: item.score }));
}

function mergeResults(query) {
  const groupedEntries = groupEntryResults(searchEntries(query), query);

  if (state.searchMode === "entries") {
    return groupedEntries;
  }

  if (state.searchMode === "fulltext") {
    return searchFulltext(query);
  }

  const fulltext = searchFulltext(query);
  return [...groupedEntries.slice(0, 180), ...fulltext.slice(0, 120)].slice(0, MAX_RESULTS);
}

function computeBestAnswer(query) {
  if (!query) {
    return null;
  }

  const qNorm = normalizeText(query);
  const qWords = qNorm.split(/\s+/).filter(Boolean);
  const groupedEntries = groupEntryResults(searchEntries(query), query);

  if (groupedEntries.length) {
    if (qWords.length === 1) {
      const exactSingle = groupedEntries.find((row) => {
        if (row._wordCount !== 1) {
          return false;
        }
        return normalizeHeadwordLoose(row.title) === qNorm;
      });
      if (exactSingle) {
        return exactSingle;
      }

      const nearSingle = groupedEntries.find((row) => {
        if (row._wordCount !== 1) {
          return false;
        }
        const head = normalizeHeadwordLoose(row.title);
        return head.startsWith(qNorm) && Math.abs(head.length - qNorm.length) <= 2;
      });
      if (nearSingle) {
        return nearSingle;
      }

      // Avoid returning phrase-based "best answer" for one-word query.
      return null;
    }

    return groupedEntries[0];
  }

  return searchFulltext(query)[0] || null;
}

function renderBestAnswer() {
  const query = state.activeQuery;

  if (!query) {
    ui.answerTitle.textContent = "Введите слово для поиска";
    ui.answerBody.textContent =
      "Начните печатать: сначала показывается самый точный словарный перевод.";
    ui.answerMeta.innerHTML = "";
    return;
  }

  const hit = state.bestAnswer;

  if (!hit) {
    ui.answerTitle.textContent = `Ничего не найдено для “${query}”`;
    ui.answerBody.textContent =
      "Попробуйте похожее написание или переключите режим на “Весь текст”.";
    ui.answerMeta.innerHTML = "";
    return;
  }

  ui.answerTitle.innerHTML = highlightText(hit.title, query);
  ui.answerBody.innerHTML = highlightText(hit.body, query);
  ui.answerMeta.innerHTML = "";

  const typeChip = document.createElement("span");
  typeChip.className = "answer-chip";
  typeChip.textContent =
    hit.type === "entry" ? "словарная статья (приоритет)" : "найдено в полном тексте";
  ui.answerMeta.append(typeChip);

  if (hit.type === "entry" && !hit._exactTitle) {
    const nearMeta = document.createElement("span");
    nearMeta.textContent = "точного заголовка нет, показан ближайший вариант";
    ui.answerMeta.append(nearMeta);
  }

  const pageMeta = document.createElement("span");
  pageMeta.textContent = `страница ${hit.page}`;
  ui.answerMeta.append(pageMeta);
}

function renderResults() {
  ui.resultList.innerHTML = "";

  if (!state.results.length) {
    const emptyItem = document.createElement("li");
    emptyItem.className = "result-item";
    emptyItem.textContent = "Совпадений пока нет.";
    ui.resultList.append(emptyItem);
    ui.resultCount.textContent = "0";
    return;
  }

  const query = state.activeQuery;
  ui.resultCount.textContent = String(state.results.length);

  for (const result of state.results) {
    const item = document.createElement("li");
    item.className = "result-item";
    if (result.id === state.selectedResultId) {
      item.classList.add("active");
    }

    const title = document.createElement("p");
    title.className = "result-headword";
    title.innerHTML = highlightText(result.title, query);

    const desc = document.createElement("p");
    desc.className = "result-translation";
    desc.innerHTML = highlightText(result.body, query);

    const type = document.createElement("span");
    type.className = `result-type ${result.type === "fulltext" ? "fulltext" : ""}`;
    type.textContent = result.type === "fulltext" ? "весь текст" : "словарная статья";

    const page = document.createElement("span");
    page.className = "result-page";
    page.textContent = `Страница ${result.page}`;

    item.append(title, desc, type, page);

    item.addEventListener("click", () => {
      state.selectedResultId = result.id;
      state.currentPage = result.page;
      renderResults();
      void renderCurrentPage();
    });

    ui.resultList.append(item);
  }
}

function runSearch() {
  if (!state.pdfDoc) {
    renderBestAnswer();
    return;
  }

  const query = ui.searchInput.value.trim();
  state.activeQuery = query;
  state.bestAnswer = computeBestAnswer(query);

  state.results = mergeResults(query);

  if (!query && state.searchMode === "entries") {
    state.results = groupEntryResults(state.entries.slice(0, MAX_RESULTS), "");
  }

  if (!query && state.searchMode === "fulltext") {
    state.results = state.lines.slice(0, MAX_RESULTS);
  }

  if (!query && state.searchMode === "all") {
    state.results = [
      ...groupEntryResults(state.entries.slice(0, 120), ""),
      ...state.lines.slice(0, 80),
    ];
  }

  if (query) {
    pushHistory(query);
  }

  if (state.bestAnswer && query) {
    state.selectedResultId = state.bestAnswer.id;
    state.currentPage = state.bestAnswer.page;
    void renderCurrentPage();
  }

  renderBestAnswer();
  renderResults();
}

async function renderCurrentPage() {
  if (!state.pdfDoc) {
    return;
  }

  state.currentPage = Math.max(1, Math.min(state.currentPage, state.pdfDoc.numPages));
  updateViewerControls();

  const token = ++state.renderToken;
  const page = await state.pdfDoc.getPage(state.currentPage);
  const viewport = page.getViewport({ scale: state.zoom });
  const canvas = ui.pdfCanvas;
  const context = canvas.getContext("2d");
  const outputScale = window.devicePixelRatio || 1;

  canvas.width = Math.floor(viewport.width * outputScale);
  canvas.height = Math.floor(viewport.height * outputScale);
  canvas.style.width = `${Math.floor(viewport.width)}px`;
  canvas.style.height = `${Math.floor(viewport.height)}px`;

  context.setTransform(outputScale, 0, 0, outputScale, 0, 0);
  context.clearRect(0, 0, canvas.width, canvas.height);

  await page.render({ canvasContext: context, viewport }).promise;

  if (token !== state.renderToken) {
    return;
  }

  ui.pageLabel.textContent = `Страница ${state.currentPage}/${state.pdfDoc.numPages}`;
}

async function loadPdfFile(file) {
  setSearchAvailability(false);
  state.entries = [];
  state.lines = [];
  state.pageTexts = [];
  state.results = [];
  state.selectedResultId = null;
  state.bestAnswer = null;
  state.currentPage = 1;
  state.zoom = 1;
  state.activeQuery = "";

  updateZoomLabel();
  ui.resultList.innerHTML = "";
  ui.resultCount.textContent = "0";

  ui.fileMeta.textContent = `${file.name} (${formatSize(file.size)})`;

  setStatus("Загрузка PDF...");
  setProgress(8);

  const bytes = await file.arrayBuffer();
  const loadingTask = pdfjsLib.getDocument({ data: bytes });

  state.pdfDoc = await loadingTask.promise;

  setStatus(`PDF загружен: ${state.pdfDoc.numPages} страниц.`);
  setProgress(14);

  await renderCurrentPage();
  updateViewerControls();

  const cacheKey = buildCacheKey(file);
  await buildTextIndex(cacheKey);
  renderHistory();
}

loadHistory();
setSearchAvailability(false);
updateModeButtons();
updateViewerControls();
updateZoomLabel();
setProgress(0);
renderBestAnswer();
