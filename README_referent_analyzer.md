# Mediatext Referent Analyzer

Этот модуль реализует лингвопрагматический анализ по методологии диссертации:

1. Общий корпус используется только как исходная база.
2. Расчеты выполняются по подкорпусам контекстов, привязанных к стране-референту (`China`, `USA`, `Russia`).
3. Один текст может анализироваться несколько раз: отдельно для каждого референта.
4. `EVI` считается только по отношению к конкретной стране-референту, а не как общий sentiment текста.
5. Итоговый `IP` прозрачен и проверяем: `IP = (IDI + EMI + MTI) * EVI`.

## Формулы

- `IDI = N_ideol / N_content`
- `EMI = (1/3 * N_e_w + 2/3 * N_e_m + 1 * N_e_s) / N_content`
- `MTI = N_met / N_content`
- `EVI ∈ {-2, -1, 0, +1, +2}`
- `IP = (IDI + EMI + MTI) * EVI`, диапазон `[-6, +6]`

## Почему EVI не общий sentiment

Один и тот же контекст может по-разному оценивать разные страны. Поэтому при нескольких референтах в одном контексте создаются отдельные строки анализа, и `EVI/IP` считаются отдельно по каждой стране.

## Единица анализа

Расширенный контекст:
- предыдущее предложение
- предложение с ключом референта
- последующее предложение

Пересекающиеся окна объединяются, чтобы избежать дублей.

## Вход

Поддерживается `CSV / XLSX / JSON` с обязательными полями:
- `doc_id`
- `media_country` (`Malaysia` / `Indonesia`)
- `outlet_name`
- `date`
- `title`
- `text`

Дополнительные поля сохраняются в исходной таблице и не мешают анализу.

## Редактируемые словари

При первом запуске создаются:
- `referent_dicts/ref_keywords.csv`
- `referent_dicts/ideological_markers.csv`
- `referent_dicts/emotional_markers.csv`
- `referent_dicts/metaphor_candidates.csv`

Можно редактировать вручную перед повторным запуском.

## Ручная верификация

- `EVI` (основной режим) берется из `evi_manual.csv` (`context_id,ref_country,EVI,explanation`).
- Для метафор можно подать `metaphor_review.csv` (`context_id,ref_country,marker,is_metaphor`).

## Выходные файлы

1. `contexts_full.csv`
2. `aggregated_by_article.csv`
3. `aggregated_by_outlet.csv`
4. `aggregated_by_media_country_and_ref_country.csv`
5. `summary_matrix.xlsx`
6. `flagged_cases.csv`

## Запуск

```bash
python3 media_analyzer_referent.py \
  --input "/path/to/input.csv" \
  --output-dir "/path/to/output" \
  --dict-dir "referent_dicts" \
  --evi-mode manual \
  --evi-manual "/path/to/evi_manual.csv" \
  --metaphor-review "/path/to/metaphor_review.csv"
```

Если пока нет ручной разметки:

```bash
python3 media_analyzer_referent.py \
  --input "/path/to/input.csv" \
  --output-dir "/path/to/output" \
  --dict-dir "referent_dicts" \
  --evi-mode suggested
```

