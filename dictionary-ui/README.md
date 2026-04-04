# Малайско-русский словарь (веб-оболочка)

Веб-интерфейс для электронного словаря на основе PDF:
- загрузка PDF словаря через браузер
- построение индекса по текстовому слою (если он есть)
- поиск по малайским заголовкам и русским переводам
- просмотр страницы, на которой найдена словарная статья

## Локальный запуск

```bash
cd "dictionary-ui"
python3 -m http.server 8080
```

Откройте [http://localhost:8080](http://localhost:8080), затем выберите PDF-файл.

## Особенности текущего PDF

Файл `Большой малайско-русский словарь.pdf` очень тяжелый (сотни МБ) и похож на скан.
Если текстовый слой есть, поиск заработает сразу.
Если это только изображения страниц, нужен отдельный OCR-этап для полноценного поиска.

## Публикация на GitHub

В репозитории уже добавлен workflow для GitHub Pages:
`.github/workflows/deploy-dictionary-ui.yml`

После пуша в GitHub сайт будет публиковаться автоматически из папки `dictionary-ui`.

### Минимальные шаги

1. Создайте репозиторий на GitHub (пустой, без README).
2. В текущем проекте выполните:

```bash
git add dictionary-ui .github/workflows/deploy-dictionary-ui.yml
git commit -m "Add Malay-RU dictionary web shell"
git branch -M main
git remote add origin <ВАШ_HTTPS_ИЛИ_SSH_URL>
git push -u origin main
```

3. На GitHub зайдите в `Settings -> Pages` и убедитесь, что источник настроен на `GitHub Actions`.
