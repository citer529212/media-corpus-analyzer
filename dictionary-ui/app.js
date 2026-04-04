import * as pdfjsLib from "https://cdn.jsdelivr.net/npm/pdfjs-dist@4.5.136/build/pdf.min.mjs";

pdfjsLib.GlobalWorkerOptions.workerSrc =
  "https://cdn.jsdelivr.net/npm/pdfjs-dist@4.5.136/build/pdf.worker.min.mjs";

const ui = {
  fileInput: document.getElementById("pdfFile"),
  fileMeta: document.getElementById("fileMeta"),
  searchInput: document.getElementById("searchInput"),
  searchButton: document.getElementById("searchButton"),
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

const state = {
  pdfDoc: null,
  entries: [],
  pageTexts: [],
  results: [],
  currentPage: 1,
  zoom: 1,
  renderToken: 0,
  selectedResultId: null,
};

const TEXT_INDEX_MIN_ENTRIES = 15;
const MAX_RESULTS = 200;

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
  inputDebounceId = setTimeout(runSearch, 130);
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
    .replace(/\s+/g, " ")
    .trim();
}

function cleanupLine(text) {
  return text.replace(/\s+/g, " ").trim();
}

function buildCacheKey(file) {
  return `dictionary-shell:${file.name}:${file.size}:${file.lastModified}`;
}

function looksLikeHeadword(text) {
  const value = text.trim();
  if (!value || value.length < 2 || value.length > 64) {
    return false;
  }

  if (!/[a-z]/i.test(value)) {
    return false;
  }

  if (/[а-яё]/i.test(value)) {
    return false;
  }

  const words = value.split(/\s+/);
  return words.length <= 5;
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

  const splitByDash = normalized.split(/\s+[—–-]\s+/);
  if (splitByDash.length >= 2) {
    const left = cleanHeadword(splitByDash[0]);
    const right = splitByDash.slice(1).join(" - ").trim();
    if (looksLikeHeadword(left) && /[а-яё]/i.test(right)) {
      return {
        id: `${page}-${localLineIndex}-d`,
        headword: left,
        translation: right,
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
        id: `${page}-${localLineIndex}-c`,
        headword: left,
        translation: right,
        page,
      };
    }
  }

  const splitByWideSpaces = normalized.split(/\s{2,}/);
  if (splitByWideSpaces.length >= 2) {
    const left = cleanHeadword(splitByWideSpaces[0]);
    const right = splitByWideSpaces.slice(1).join(" ").trim();
    if (looksLikeHeadword(left) && /[а-яё]/i.test(right)) {
      return {
        id: `${page}-${localLineIndex}-s`,
        headword: left,
        translation: right,
        page,
      };
    }
  }

  return null;
}

function groupLines(textItems) {
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

async function buildTextIndex(cacheKey) {
  const cached = localStorage.getItem(cacheKey);
  if (cached) {
    try {
      const parsed = JSON.parse(cached);
      if (Array.isArray(parsed.entries) && Array.isArray(parsed.pageTexts)) {
        state.entries = parsed.entries;
        state.pageTexts = parsed.pageTexts;
        setStatus(`Загружен кэш индекса: ${state.entries.length} статей.`);
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
  state.pageTexts = [];

  const pageCount = state.pdfDoc.numPages;

  for (let pageNum = 1; pageNum <= pageCount; pageNum += 1) {
    const page = await state.pdfDoc.getPage(pageNum);
    const content = await page.getTextContent();
    const lines = groupLines(content.items);

    state.pageTexts.push(lines.join("\n"));

    lines.forEach((line, index) => {
      const entry = extractEntry(line, pageNum, index);
      if (entry) {
        state.entries.push(entry);
      }
    });

    const percent = Math.round((pageNum / pageCount) * 100);
    setProgress(percent);
    setStatus(`Индексация PDF: ${pageNum}/${pageCount} страниц...`);
  }

  const payload = JSON.stringify({
    entries: state.entries,
    pageTexts: state.pageTexts,
  });

  if (payload.length < 4_000_000) {
    localStorage.setItem(cacheKey, payload);
  }

  setSearchAvailability(true);

  if (state.entries.length >= TEXT_INDEX_MIN_ENTRIES) {
    setStatus(`Готово: проиндексировано ${state.entries.length} статей.`);
  } else {
    setStatus(
      "Текстовых словарных статей почти нет. Доступен fallback-поиск по страницам."
    );
  }

  runSearch();
}

function searchEntries(query) {
  if (!query) {
    return state.entries.slice(0, MAX_RESULTS);
  }

  const normalized = normalizeText(query);

  const scored = state.entries
    .map((entry) => {
      const headword = normalizeText(entry.headword);
      const translation = normalizeText(entry.translation);

      let score = 9;
      if (headword === normalized) {
        score = 0;
      } else if (headword.startsWith(normalized)) {
        score = 1;
      } else if (headword.includes(normalized)) {
        score = 2;
      } else if (translation.includes(normalized)) {
        score = 3;
      }

      return { score, entry };
    })
    .filter((row) => row.score < 9)
    .sort((a, b) => a.score - b.score || a.entry.headword.localeCompare(b.entry.headword))
    .slice(0, MAX_RESULTS)
    .map((row) => row.entry);

  return scored;
}

function fallbackSearch(query) {
  const normalized = normalizeText(query);

  if (!normalized) {
    return [];
  }

  const rows = [];

  state.pageTexts.forEach((pageText, index) => {
    const haystack = normalizeText(pageText);
    const at = haystack.indexOf(normalized);
    if (at === -1) {
      return;
    }

    const source = pageText.replace(/\s+/g, " ");
    const start = Math.max(0, at - 60);
    const end = Math.min(source.length, at + normalized.length + 120);

    rows.push({
      id: `fallback-${index + 1}`,
      headword: `Фрагмент страницы ${index + 1}`,
      translation: source.slice(start, end),
      page: index + 1,
    });
  });

  return rows.slice(0, MAX_RESULTS);
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

  ui.resultCount.textContent = String(state.results.length);

  for (const entry of state.results) {
    const item = document.createElement("li");
    item.className = "result-item";
    if (entry.id === state.selectedResultId) {
      item.classList.add("active");
    }

    const title = document.createElement("p");
    title.className = "result-headword";
    title.textContent = entry.headword;

    const desc = document.createElement("p");
    desc.className = "result-translation";
    desc.textContent = entry.translation;

    const page = document.createElement("span");
    page.className = "result-page";
    page.textContent = `Страница ${entry.page}`;

    item.append(title, desc, page);

    item.addEventListener("click", () => {
      state.selectedResultId = entry.id;
      state.currentPage = entry.page;
      renderResults();
      void renderCurrentPage();
    });

    ui.resultList.append(item);
  }
}

function runSearch() {
  if (!state.pdfDoc) {
    return;
  }

  const query = ui.searchInput.value.trim();

  if (state.entries.length >= TEXT_INDEX_MIN_ENTRIES) {
    state.results = searchEntries(query);
  } else {
    state.results = fallbackSearch(query);
  }

  if (!query && state.entries.length >= TEXT_INDEX_MIN_ENTRIES) {
    state.results = state.entries.slice(0, MAX_RESULTS);
  }

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
  state.pageTexts = [];
  state.results = [];
  state.selectedResultId = null;
  state.currentPage = 1;
  state.zoom = 1;

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
}

setSearchAvailability(false);
updateViewerControls();
updateZoomLabel();
setProgress(0);
