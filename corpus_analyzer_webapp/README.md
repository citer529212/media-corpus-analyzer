# Mediatext analyzator (Web)

Веб-приложение для загрузки медиакорпуса (ZIP папкой или отдельные `.txt/.md/.docx/.pdf/.csv/.xlsx/.json`), запуска Mediatext analyzator и скачивания всех результатов в одном ZIP.

## Возможности
- Загрузка корпуса:
  - `ZIP` архив (внутри любые подпапки с `.txt/.md/.docx/.pdf`)
  - или по одному/нескольким файлам (`txt/md/docx/pdf`)
  - или ручная вставка текста в интерфейсе
- Диапазон годов `min/max`
- `Dedup` (exact + near duplicates)
- Полный пайплайн анализа:
  - Stage 1: профиль корпуса
  - Stage 2: частоты, KWIC, коллокации, keywords
  - Stage 3: тональность, фреймы, персуазивные маркеры
  - Stage 4: тренды
  - Stage 5: репрезентативность
  - Stage 6: значимость (chi2, p-value, Cramer's V, effect label, log-odds)
  - Stage 7: индикаторная модель персуазивного потенциала (IDI, EMI, EVI, MTI, PP)
- Скачивание результата как `ZIP`
- Диаграммы в интерфейсе (распределения, динамика по годам, IP и базовые индексы)
- Эмпирическая нормализация:
  - `raw` плотности (`IDI_raw/EMI_raw/MTI_raw`)
  - процентные значения (`*100`)
  - процентильные ранги по выбранной базе (`full corpus / media / referent / media×referent`)
- Calibration texts (reference anchors) для эмпирических baseline-сравнений

## Локальный запуск
Из корня проекта:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run corpus_analyzer_webapp/app.py
```

## Деплой через GitHub (Streamlit Community Cloud)
1. Запушить проект в GitHub.
2. На [streamlit.io/cloud](https://streamlit.io/cloud) нажать **New app**.
3. Выбрать репозиторий и ветку.
4. Указать `Main file path`: `corpus_analyzer_webapp/app.py`.
5. Используется единый `requirements.txt` в корне репозитория.

## Важные примечания
- Источник/страна/год определяются эвристически из имени файла и текста.
- Для максимальной точности лучше именовать файлы с метками, например:
  - `the_star_usa_2025_001.txt`
  - `antara_china_2024_123.txt`
- Приложение рассчитано на большие корпуса, но лимит зависит от ресурсов хостинга.

## Empirical normalization and updated Impact Potential
- Почему raw-значения малы:
  - `IDI_raw`, `EMI_raw`, `MTI_raw` — это плотности (доли), поэтому значения `0.001–0.02` нормальны для новостного дискурса.
- Как читать индексы:
  - `raw`: плотность в долях (точность до 6–8 знаков);
  - `percent value`: `raw * 100` (наглядный процент);
  - `percentile rank`: позиция контекста в распределении корпуса (0–100).
- Почему percentiles лучше теоретического min/max:
  - percentiles дают эмпирическую относительную позицию внутри реального корпуса, а не абстрактную шкалу.
- EVI как базовый оценочный вектор:
  - `EVI_raw ∈ [-10; +10]`, `EVI_norm = EVI_raw / 5`.
- IDI/EMI/MTI как усилители:
  - `Discursive_energy = IDI + EMI + MTI`.
- Формула IP на уровне контекста:
  - `IP_i = EVI_norm_i × (1 + IDI_i + EMI_i + MTI_i)`.
- Агрегация:
  - `S_r` используется как вес контекста, а не как множитель средних;
  - `IP_final = Σ(S_i × IP_i) / ΣS_i`;
  - `IP_abs_final = Σ(S_i × |IP_i|) / ΣS_i`.
- Разница между `IP_final` и `IP_abs_final`:
  - `IP_final` = направление образа (плюс/минус),
  - `IP_abs_final` = сила воздействия без знака.
- Calibration texts:
  - в UI используется термин **Calibration texts / Reference anchors** (не “precedent texts”);
  - позволяют сравнить текущий подкорпус с эмпирическими baseline-типами (`neutral_news`, `crisis_text` и др.).
- Экспорт для диссертации:
  - `contexts_full.csv/xlsx` (контекстные показатели + evidence),
  - `distribution_stats.xlsx` (распределения и квантили),
  - `summary_matrix.xlsx` (сводная матрица media_country × ref_country),
  - `calibration_report.xlsx` (если загружены calibration texts).
