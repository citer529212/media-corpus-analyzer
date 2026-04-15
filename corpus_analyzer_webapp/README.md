# Mediatext analyzator (Web)

Веб-приложение для загрузки медиакорпуса (ZIP папкой или отдельные `.txt/.md`), запуска Mediatext analyzator и скачивания всех результатов в одном ZIP.

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
- Диаграммы в интерфейсе (распределения, динамика по годам, PP и базовые индексы)

## Локальный запуск
Из корня проекта:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r corpus_analyzer_webapp/requirements.txt
streamlit run corpus_analyzer_webapp/app.py
```

## Деплой через GitHub (Streamlit Community Cloud)
1. Запушить проект в GitHub.
2. На [streamlit.io/cloud](https://streamlit.io/cloud) нажать **New app**.
3. Выбрать репозиторий и ветку.
4. Указать `Main file path`: `corpus_analyzer_webapp/app.py`.
5. В `Advanced settings` указать зависимости из `corpus_analyzer_webapp/requirements.txt` (или оставить авто-детект).

## Важные примечания
- Источник/страна/год определяются эвристически из имени файла и текста.
- Для максимальной точности лучше именовать файлы с метками, например:
  - `the_star_usa_2025_001.txt`
  - `antara_china_2024_123.txt`
- Приложение рассчитано на большие корпуса, но лимит зависит от ресурсов хостинга.
