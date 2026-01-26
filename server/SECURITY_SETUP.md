# ========================================
# 🔐 НАСТРОЙКА БЕЗОПАСНОСТИ
# Exotic Operator Assistant
# ========================================

## Вариант 1: Через Cloudflare Dashboard (РЕКОМЕНДУЕТСЯ)

### Шаг 1: Откройте Cloudflare Dashboard
1. Перейдите на https://dash.cloudflare.com
2. Войдите под mabastik@gmail.com
3. Слева выберите **Workers & Pages**
4. Нажмите на **exotic-telegram**

### Шаг 2: Добавьте секреты
1. Перейдите во вкладку **Settings**
2. Найдите раздел **Variables and Secrets**
3. В разделе **Secrets** нажмите **Add**
4. Добавьте следующие секреты:

| Имя переменной | Значение |
|----------------|----------|
| `ADMIN_SECRET_PHRASE` | `ihRHgjVKTGkySq3WEmUTFdxR` |
| `TELEGRAM_WEBHOOK_SECRET` | `X4VuF24vyDRh6KuwpU7bwCjtCiL5FcJF` |
| `ADMIN_TOKEN` | `tyxktbcCh66TRuJLragzS9YCSYxZi68C` |

### Шаг 3: Загрузите обновлённый код
1. Во вкладке **Workers & Pages** → **exotic-telegram**
2. Нажмите **Quick Edit** или загрузите файл `worker.js`
3. Скопируйте содержимое файла `server/worker.js` и вставьте
4. Нажмите **Save and Deploy**

### Шаг 4: Переустановите Webhook
После деплоя выполните в PowerShell:
```powershell
Invoke-RestMethod -Uri 'https://exotic-telegram.mabastik.workers.dev/api/set-webhook' -Headers @{Authorization='Bearer tyxktbcCh66TRuJLragzS9YCSYxZi68C'}
```

---

## Вариант 2: Через Wrangler CLI

### Шаг 1: Авторизуйтесь
```powershell
npx wrangler login
```
Подтвердите в браузере.

### Шаг 2: Запустите скрипт настройки
```powershell
cd server
.\setup-secrets.ps1
```

---

## 🔑 Как использовать секреты

### ADMIN_SECRET_PHRASE
Секретная фраза для входа в админ-панель бота.
Отправьте боту @clickuved_bot сообщение:
```
ihRHgjVKTGkySq3WEmUTFdxR
```

### ADMIN_TOKEN
Используется для защищённых API эндпоинтов:
- `/api/set-webhook`
- `/api/webhook-info`
- `/api/test-log`
- `/api/metrics`

Пример запроса:
```powershell
Invoke-RestMethod -Uri 'https://exotic-telegram.mabastik.workers.dev/api/webhook-info' -Headers @{Authorization='Bearer tyxktbcCh66TRuJLragzS9YCSYxZi68C'}
```

### TELEGRAM_WEBHOOK_SECRET
Автоматически проверяется при получении webhook от Telegram.
После добавления секрета, переустановите webhook.

---

## ⚠️ ВАЖНО

1. **Удалите файл SECRETS.txt после настройки!**
2. Не коммитьте секреты в git
3. Храните резервную копию секретов в безопасном месте
