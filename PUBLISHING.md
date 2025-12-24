# Publishing Checklist (Chrome Web Store)

## Required files
- `manifest.json` (MV3) with service worker `background.js`
- Icons: `icons/icon48.png`, `icons/icon128.png`
- Privacy policy: link to `PRIVACY.md`
- Screenshots: at least 2 (1280x800 or 640x400) of popup and options

## Listing text (draft)
- Short description: "Автокликер для кнопок \"В работу\" на панели exotic.company с уведомлениями и статистикой."
- Full description:
  - Автоматический клик по кнопкам "В работу" на странице выплат `exotic.company/operator/payout_transaction`
  - Статистика (всего, сегодня, сессия, точность) и уведомления о кликах
  - Настройки задержек, целевых текстов, паттернов поиска
  - Работает только на целевом домене, данные хранятся локально

## Permission declaration
- `storage`: сохраняет настройки и статистику локально
- `activeTab`, `scripting`: инжект скрипта на целевой странице для клика
- `notifications`: локальные уведомления о кликах/статусе
- `alarms`: ежедневный сброс суточной статистики
- `host_permissions`: ограничение действия расширения страницей выплат exotic.company

## Packaging steps
1) Снять билд: собрать ZIP из содержимого папки (файлы + `icons/` в корне, без вложенной папки-обёртки).
2) Проверить распакованное в `chrome://extensions` → "Загрузить распакованное".
3) Если без ошибок, загрузить ZIP в CWS dev console.
4) Добавить ссылку на `PRIVACY.md` как политику конфиденциальности.
5) Приложить скриншоты и иконку (128x128), заполнить описание.

## Notes
- Укажите, что расширение работает только на `exotic.company`.
- Нет внешних сетевых запросов в коде, данные не уходят наружу.
