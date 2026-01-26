# ========================================
# Exotic Operator Assistant - Security Setup
# ========================================
# Запустите этот скрипт после авторизации в Wrangler
# 
# Сначала выполните:
#   npx wrangler login
# 
# Затем запустите этот скрипт:
#   .\setup-secrets.ps1
# ========================================

Write-Host "🔐 Настройка безопасности Exotic Operator Assistant" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Генерируем случайные секреты
$adminSecretPhrase = -join ((65..90) + (97..122) + (48..57) | Get-Random -Count 24 | ForEach-Object {[char]$_})
$webhookSecret = -join ((65..90) + (97..122) + (48..57) | Get-Random -Count 32 | ForEach-Object {[char]$_})
$adminToken = -join ((65..90) + (97..122) + (48..57) | Get-Random -Count 32 | ForEach-Object {[char]$_})

Write-Host "📝 Сгенерированные секреты:" -ForegroundColor Yellow
Write-Host "   ADMIN_SECRET_PHRASE: $adminSecretPhrase" -ForegroundColor Green
Write-Host "   TELEGRAM_WEBHOOK_SECRET: $webhookSecret" -ForegroundColor Green
Write-Host "   ADMIN_TOKEN: $adminToken" -ForegroundColor Green
Write-Host ""

# Сохраняем секреты в файл (для справки)
$secretsFile = "SECRETS.txt"
@"
# ========================================
# СЕКРЕТЫ - ХРАНИТЕ В БЕЗОПАСНОМ МЕСТЕ!
# Сгенерировано: $(Get-Date)
# ========================================

ADMIN_SECRET_PHRASE=$adminSecretPhrase
TELEGRAM_WEBHOOK_SECRET=$webhookSecret
ADMIN_TOKEN=$adminToken

# Используйте ADMIN_SECRET_PHRASE как секретную фразу для входа в админ-панель бота
# Используйте ADMIN_TOKEN в заголовке Authorization: Bearer <ADMIN_TOKEN> для API
"@ | Out-File -FilePath $secretsFile -Encoding UTF8

Write-Host "💾 Секреты сохранены в $secretsFile" -ForegroundColor Yellow
Write-Host ""

# 2. Устанавливаем секреты в Cloudflare
Write-Host "🚀 Устанавливаю секреты в Cloudflare Workers..." -ForegroundColor Cyan

Write-Host "   [1/3] ADMIN_SECRET_PHRASE..." -ForegroundColor White
$adminSecretPhrase | npx wrangler secret put ADMIN_SECRET_PHRASE

Write-Host "   [2/3] TELEGRAM_WEBHOOK_SECRET..." -ForegroundColor White
$webhookSecret | npx wrangler secret put TELEGRAM_WEBHOOK_SECRET

Write-Host "   [3/3] ADMIN_TOKEN..." -ForegroundColor White
$adminToken | npx wrangler secret put ADMIN_TOKEN

Write-Host ""
Write-Host "✅ Секреты установлены!" -ForegroundColor Green
Write-Host ""

# 3. Деплоим worker
Write-Host "🚀 Деплою worker..." -ForegroundColor Cyan
npx wrangler deploy

Write-Host ""
Write-Host "✅ Готово! Теперь переустановите webhook:" -ForegroundColor Green
Write-Host ""
Write-Host "   curl -X GET 'https://exotic-telegram.mabastik.workers.dev/api/set-webhook' -H 'Authorization: Bearer $adminToken'" -ForegroundColor Yellow
Write-Host ""
Write-Host "Или в PowerShell:" -ForegroundColor White
Write-Host "   Invoke-RestMethod -Uri 'https://exotic-telegram.mabastik.workers.dev/api/set-webhook' -Headers @{Authorization='Bearer $adminToken'}" -ForegroundColor Yellow
