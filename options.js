/**
 * Exotic Assistant - Options Controller
 * Минималистичная страница настроек
 */

// Безопасное определение API для всех браузеров
// PC/Mac: Chrome, Firefox, Edge, Opera, Brave, Arc
// Android: Kiwi, Mises, Samsung Internet, Firefox
// iPhone/iPad: Orion Browser
var api = null;
try {
    if (typeof browser !== 'undefined' && browser && browser.runtime) {
        api = browser;
    } else if (typeof chrome !== 'undefined' && chrome && chrome.runtime) {
        api = chrome;
    }
} catch (e) {
    if (typeof chrome !== 'undefined') api = chrome;
}
const SERVER_URL = 'https://exotic-telegram.mabastik.workers.dev';

// ========== КОНСТАНТЫ ==========
const CONNECTION_CHECK_INTERVAL = 2000; // 2 секунды для fallback polling
const CONNECTION_CHECK_FAST = 500; // Быстрая проверка для первых попыток
const MAX_CONNECTION_ATTEMPTS = 90; // 3 минуты (2с * 90)
const COPY_HINT_DURATION = 2000; // 2 секунды
const REQUEST_TIMEOUT = 5000; // 5 секунд
const STATUS_CACHE_TTL = 60000; // Кэш статуса 1 минуту
// SSE отключен - Cloudflare Workers не поддерживают SSE
const SSE_SUPPORTED = false;

class OptionsController {
    constructor() {
        this.deviceId = null;
        this.eventSource = null; // SSE соединение
        this.init();
    }

    async init() {
        // Сначала загружаем deviceId - он нужен для остальных операций
        await this.loadDeviceId();
        
        // Привязываем события сразу, не ждём загрузки данных
        this.bindEvents();
        
        // Параллельно загружаем все данные для ускорения
        await Promise.all([
            this.loadSettings(),
            this.checkTelegramConnection().then(() => this.loadLicense()),
            this.loadStats()
        ]);
    }

    async loadDeviceId() {
        try {
            const result = await api.storage.local.get(['state']);
            this.deviceId = result.state?.settings?.deviceId;
            
            // Если deviceId отсутствует, он будет создан в background.js
            if (!this.deviceId) {
                // Генерируем временный ID для текущей сессии
                // Проверяем поддержку crypto.getRandomValues (может отсутствовать на мобильных)
                if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
                    const array = new Uint8Array(16);
                    crypto.getRandomValues(array);
                    this.deviceId = Array.from(array, byte => byte.toString(16).padStart(2, '0')).join('');
                } else {
                    // Fallback: используем Math.random
                    this.deviceId = 'dev_' + Date.now().toString(36) + '_' + Math.random().toString(36).substring(2, 15);
                }
            }
        } catch (e) {
            console.log('[Exotic] loadDeviceId error:', e.message);
            this.deviceId = 'dev_' + Date.now().toString(36) + '_' + Math.random().toString(36).substring(2, 15);
        }
    }

    // ============ Settings ============
    async loadSettings() {
        const result = await api.storage.local.get(['state']);
        const settings = result.state?.settings || {};
        
        document.getElementById('soundEnabled').checked = settings.soundEnabled !== false;
        document.getElementById('notificationsEnabled').checked = settings.notifications !== false;
        document.getElementById('autoRefreshEnabled').checked = settings.autoRefreshEnabled !== false;
    }

    async saveSettings() {
        const soundEnabled = document.getElementById('soundEnabled').checked;
        const notificationsEnabled = document.getElementById('notificationsEnabled').checked;
        const autoRefreshEnabled = document.getElementById('autoRefreshEnabled').checked;
        
        // Отправляем в background.js
        await this.sendMessage({
            action: 'updateSettings',
            settings: {
                soundEnabled,
                notifications: notificationsEnabled,
                autoRefreshEnabled
            }
        });
    }

    // ============ Events ============
    bindEvents() {
        document.getElementById('soundEnabled').addEventListener('change', () => this.saveSettings());
        document.getElementById('notificationsEnabled').addEventListener('change', () => this.saveSettings());
        document.getElementById('autoRefreshEnabled').addEventListener('change', () => this.saveSettings());
        document.getElementById('generateCodeBtn').addEventListener('click', () => this.generateCode());
        document.getElementById('newCodeBtn').addEventListener('click', () => this.generateCode(true));
        document.getElementById('disconnectBtn').addEventListener('click', () => this.disconnectTelegram());
        document.getElementById('resetStatsBtn').addEventListener('click', () => this.resetStats());
        
        // Копирование кода по клику
        document.getElementById('connectCode').addEventListener('click', () => this.copyCode());
        
        // Кнопки проверки и теста
        document.getElementById('verifyBtn').addEventListener('click', () => this.verifyConnection());
        
        document.getElementById('testNotifyBtn').addEventListener('click', () => this.testNotification());
        
        // Устройства
        document.getElementById('refreshDevicesBtn').addEventListener('click', () => this.loadDevices());
    }

    // ============ Copy Code ============
    async copyCode() {
        const codeEl = document.getElementById('connectCode');
        const code = codeEl.textContent;
        
        if (code === '--------' || code === 'Ошибка') return;
        
        try {
            await navigator.clipboard.writeText(code);
            this.showCopyHint();
        } catch (err) {
            // Fallback для старых браузеров
            const textArea = document.createElement('textarea');
            textArea.value = code;
            document.body.appendChild(textArea);
            textArea.select();
            document.execCommand('copy');
            document.body.removeChild(textArea);
            this.showCopyHint();
        }
    }

    showCopyHint() {
        const hint = document.getElementById('copyHint');
        hint.classList.add('show');
        setTimeout(() => hint.classList.remove('show'), COPY_HINT_DURATION);
    }

    // ============ Telegram ============
    async checkTelegramConnection(forceRefresh = false) {
        const result = await api.storage.local.get(['state']);
        const settings = result.state?.settings || {};
        
        // Если есть сохранённые данные - проверяем
        if (settings.telegramUserId) {
            // ОПТИМИЗАЦИЯ: Сначала показываем кэшированные данные
            if (settings.telegramConnected && settings.telegramUsername && !forceRefresh) {
                this.showConnected(settings.telegramUsername);
                
                // Проверяем в фоне, не блокируя UI
                this.validateConnectionInBackground(settings.telegramUserId);
                return;
            }
            
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
                
                const response = await fetch(`${SERVER_URL}/api/status`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ oderId: settings.telegramUserId }),
                    signal: controller.signal
                });
                
                clearTimeout(timeoutId);
                const data = await response.json();
                
                if (data.connected) {
                    // Обновляем локальные данные
                    await this.sendMessage({
                        action: 'updateSettings',
                        settings: {
                            telegramConnected: true,
                            telegramUsername: data.username,
                            lastStatusCheck: Date.now()
                        }
                    });
                    this.showConnected(data.username || data.telegramId);
                    return;
                }
            } catch (error) {
                console.error('Ошибка проверки подключения:', error);
                // При ошибке сети показываем кэшированные данные если есть
                if (settings.telegramConnected && settings.telegramUsername) {
                    this.showConnected(settings.telegramUsername);
                    return;
                }
            }
            
            // Сервер не подтвердил - сбрасываем локальные данные
            await this.sendMessage({
                action: 'updateSettings',
                settings: {
                    telegramConnected: false,
                    telegramUsername: null
                }
            });
        }
        
        this.showNotConnected();
    }
    
    // Фоновая валидация без блокировки UI
    async validateConnectionInBackground(oderId) {
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
            
            const response = await fetch(`${SERVER_URL}/api/status`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ oderId }),
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            const data = await response.json();
            
            if (!data.connected) {
                // Привязка исчезла на сервере - обновляем UI
                await this.sendMessage({
                    action: 'updateSettings',
                    settings: {
                        telegramConnected: false,
                        telegramUsername: null
                    }
                });
                this.showNotConnected();
            } else {
                // Обновляем время последней проверки
                await this.sendMessage({
                    action: 'updateSettings',
                    settings: { lastStatusCheck: Date.now() }
                });
            }
        } catch (error) {
            // Игнорируем ошибки фоновой проверки
            console.log('Background validation error:', error.message);
        }
    }

    showConnected(username) {
        document.getElementById('telegramNotConnected').classList.add('hidden');
        document.getElementById('telegramConnected').classList.remove('hidden');
        document.getElementById('devicesSection').classList.remove('hidden');
        const display = String(username).startsWith('@') ? username : `@${username}`;
        document.getElementById('connectedUsername').textContent = display;
        document.getElementById('verifyStatus').textContent = '';
        
        // Загружаем устройства
        this.loadDevices();
    }

    showNotConnected() {
        document.getElementById('telegramNotConnected').classList.remove('hidden');
        document.getElementById('telegramConnected').classList.add('hidden');
        document.getElementById('devicesSection').classList.add('hidden');
    }

    // Проверка привязки на сервере
    async verifyConnection() {
        const btn = document.getElementById('verifyBtn');
        const statusEl = document.getElementById('verifyStatus');
        
        btn.disabled = true;
        btn.textContent = '⏳ Проверка...';
        statusEl.textContent = '';
        
        try {
            const result = await api.storage.local.get(['state']);
            const settings = result.state?.settings || {};
            
            if (!settings.telegramUserId) {
                throw new Error('Нет ID устройства');
            }
            
            const response = await fetch(`${SERVER_URL}/api/status`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ oderId: settings.telegramUserId })
            });
            
            const data = await response.json();
            
            if (data.connected) {
                statusEl.textContent = `✅ Привязка подтверждена (TG: ${data.telegramId || 'ID скрыт'})`;
                statusEl.style.color = 'var(--success)';
                document.getElementById('connectedUsername').textContent = data.username ? `@${data.username}` : `ID: ${data.telegramId}`;
            } else {
                statusEl.textContent = '❌ Привязка не найдена на сервере';
                statusEl.style.color = 'var(--danger)';
            }
        } catch (error) {
            console.error('Ошибка проверки:', error);
            statusEl.textContent = '⚠️ Ошибка: ' + error.message;
            statusEl.style.color = 'var(--warning)';
        }
        
        btn.disabled = false;
        btn.textContent = '🔍 Проверить';
    }

    // Тест уведомлений в Telegram
    async testNotification() {
        const btn = document.getElementById('testNotifyBtn');
        const statusEl = document.getElementById('verifyStatus');
        
        btn.disabled = true;
        btn.textContent = '⏳ Отправка...';
        
        try {
            const result = await api.storage.local.get(['state']);
            const settings = result.state?.settings || {};
            
            if (!settings.telegramUserId) {
                throw new Error('Telegram не подключён');
            }
            
            const response = await fetch(`${SERVER_URL}/api/notify`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ 
                    oderId: settings.telegramUserId,
                    message: `🔔 <b>Тестовое уведомление</b>\n\nЕсли вы видите это сообщение, значит уведомления работают корректно!\n\n⏰ ${new Date().toLocaleString('ru-RU')}`
                })
            });
            
            const data = await response.json();
            
            if (data.success) {
                statusEl.textContent = '✅ Тестовое уведомление отправлено!';
                statusEl.style.color = 'var(--success)';
            } else {
                throw new Error(data.error || 'Ошибка отправки');
            }
        } catch (error) {
            console.error('Ошибка теста:', error);
            statusEl.textContent = '❌ ' + (error.message || 'Ошибка отправки');
            statusEl.style.color = 'var(--danger)';
        }
        
        btn.disabled = false;
        btn.textContent = '🔔 Тест';
    }

    async generateCode(isNewCode = false) {
        const btn = document.getElementById('generateCodeBtn');
        const newCodeBtn = document.getElementById('newCodeBtn');
        const codeEl = document.getElementById('connectCode');
        const openBotBtn = document.getElementById('openBotBtn');
        const statusEl = document.getElementById('connectionStatus');
        const deviceNameInput = document.getElementById('deviceNameInput');
        
        btn.disabled = true;
        if (isNewCode) {
            newCodeBtn.disabled = true;
            newCodeBtn.textContent = '⏳';
        } else {
            btn.textContent = 'Генерация...';
        }
        statusEl.textContent = '';
        
        try {
            // ВАЖНО: Сбрасываем локальное состояние подключения
            // Это нужно чтобы при перепривязке к другому аккаунту
            // не показывалось старое подключение
            await this.sendMessage({
                action: 'updateSettings',
                settings: {
                    telegramConnected: false,
                    telegramUsername: null
                }
            });
            
            // Показываем форму подключения
            this.showNotConnected();
            
            // Генерируем oderId (уникальный ID устройства/расширения)
            const oderId = this.deviceId;
            
            // Получаем название устройства
            const deviceName = deviceNameInput.value.trim() || this.getDefaultDeviceName();
            
            // Сохраняем название устройства
            await this.sendMessage({
                action: 'updateSettings',
                settings: { deviceName }
            });
            
            // Запрашиваем код с сервера
            const response = await fetch(`${SERVER_URL}/api/generate-code`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ oderId, deviceId: this.deviceId, deviceName })
            });
            
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            codeEl.textContent = data.code;
            codeEl.classList.add('active');
            codeEl.style.cursor = 'pointer';
            
            // Автоматически копируем код
            try {
                await navigator.clipboard.writeText(data.code);
                this.showCopyHint();
            } catch (e) {
                // Игнорируем ошибку копирования
            }
            
            // Показываем кнопку "Открыть бота"
            openBotBtn.style.display = 'inline-flex';
            openBotBtn.href = data.link || `https://t.me/clickuved_bot?start=${data.code}`;
            
            // Показываем кнопку смены кода
            newCodeBtn.classList.remove('hidden');
            newCodeBtn.disabled = false;
            newCodeBtn.textContent = '🔄';
            
            // Сохраняем oderId и код для проверки статуса
            this.currentCode = data.code; // Сохраняем код для SSE
            await this.sendMessage({
                action: 'updateSettings',
                settings: { telegramUserId: oderId, deviceId: this.deviceId }
            });
            
            btn.textContent = '✓ Код получен';
            statusEl.textContent = '⚡ Ожидание подключения...';
            statusEl.style.color = 'var(--warning)';
            
            // Начинаем проверку подключения (с SSE если поддерживается)
            this.startConnectionCheck(oderId, data.code);
            
        } catch (error) {
            console.error('Ошибка генерации кода:', error);
            codeEl.textContent = 'Ошибка';
            btn.textContent = 'Повторить';
            btn.disabled = false;
            newCodeBtn.disabled = false;
            newCodeBtn.textContent = '🔄';
            statusEl.textContent = '❌ ' + error.message;
            statusEl.style.color = 'var(--danger)';
        }
    }

    // ========== REAL-TIME ПОДКЛЮЧЕНИЕ (SSE) ==========
    // Использует Server-Sent Events для мгновенного оповещения
    startConnectionCheck(oderId, code) {
        const statusEl = document.getElementById('connectionStatus');
        
        // Останавливаем предыдущие проверки
        this.stopConnectionCheck();
        
        // Пробуем SSE если поддерживается
        if (SSE_SUPPORTED && code) {
            console.log('[Exotic] Используем SSE для подключения');
            this.startSSEConnection(oderId, code, statusEl);
        } else {
            console.log('[Exotic] SSE недоступен, используем polling');
            this.startPollingConnection(oderId, statusEl);
        }
    }
    
    // SSE соединение - мгновенное оповещение
    startSSEConnection(oderId, code, statusEl) {
        const sseUrl = `${SERVER_URL}/api/connect/stream?code=${encodeURIComponent(code)}&oderId=${encodeURIComponent(oderId)}`;
        
        statusEl.textContent = '⚡ Ожидание подключения...';
        statusEl.style.color = 'var(--warning)';
        
        this.eventSource = new EventSource(sseUrl);
        
        // Событие успешного подключения
        this.eventSource.addEventListener('connected', async (event) => {
            console.log('[Exotic] SSE: connected event received');
            
            try {
                const data = JSON.parse(event.data);
                
                // Сохраняем данные подключения
                await this.sendMessage({
                    action: 'updateSettings',
                    settings: {
                        telegramConnected: true,
                        telegramUserId: oderId,
                        telegramUsername: data.username,
                        lastStatusCheck: Date.now()
                    }
                });
                
                statusEl.textContent = '✅ Подключено мгновенно!';
                statusEl.style.color = 'var(--success)';
                
                this.showConnected(data.username || data.telegramId);
                
                // ПРИНУДИТЕЛЬНО обновляем лицензию в background.js
                await this.sendMessage({ action: 'forceCheckLicense' });
                
                // Небольшая задержка для синхронизации
                await new Promise(r => setTimeout(r, 500));
                
                await this.loadLicense();
                this.resetCodeUI();
                
            } catch (error) {
                console.error('[Exotic] SSE parse error:', error);
            }
            
            this.stopConnectionCheck();
        });
        
        // Таймаут
        this.eventSource.addEventListener('timeout', () => {
            console.log('[Exotic] SSE: timeout');
            statusEl.textContent = '❌ Время истекло. Получите новый код.';
            statusEl.style.color = 'var(--danger)';
            this.resetCodeUI();
            this.stopConnectionCheck();
        });
        
        // Ошибка SSE - переключаемся на polling
        this.eventSource.onerror = (error) => {
            console.log('[Exotic] SSE error, switching to polling:', error);
            this.stopConnectionCheck();
            this.startPollingConnection(oderId, statusEl);
        };
        
        // Таймаут для SSE - если за 30 сек нет ответа, переключаемся на polling
        this.sseTimeout = setTimeout(() => {
            if (this.eventSource && this.eventSource.readyState !== EventSource.CLOSED) {
                console.log('[Exotic] SSE timeout, switching to polling');
                this.stopConnectionCheck();
                this.startPollingConnection(oderId, statusEl);
            }
        }, 30000);
    }
    
    // Fallback: polling (старый метод)
    startPollingConnection(oderId, statusEl) {
        let attempts = 0;
        const maxAttempts = MAX_CONNECTION_ATTEMPTS;
        
        const check = async () => {
            attempts++;
            
            // Первые 20 проверок быстрее (500мс), потом 2с
            const isEarlyCheck = attempts <= 20;
            const interval = isEarlyCheck ? CONNECTION_CHECK_FAST : CONNECTION_CHECK_INTERVAL;
            
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
                
                const response = await fetch(`${SERVER_URL}/api/status`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ oderId }),
                    signal: controller.signal
                });
                
                clearTimeout(timeoutId);
                const data = await response.json();
                
                if (data.connected) {
                    // Сохраняем данные подключения
                    await this.sendMessage({
                        action: 'updateSettings',
                        settings: {
                            telegramConnected: true,
                            telegramUserId: oderId,
                            telegramUsername: data.username,
                            lastStatusCheck: Date.now()
                        }
                    });
                    
                    statusEl.textContent = '✅ Подключено!';
                    statusEl.style.color = 'var(--success)';
                    
                    this.showConnected(data.username || data.telegramId);
                    
                    // ПРИНУДИТЕЛЬНО обновляем лицензию в background.js
                    await this.sendMessage({ action: 'forceCheckLicense' });
                    
                    // Небольшая задержка для синхронизации
                    await new Promise(r => setTimeout(r, 500));
                    
                    await this.loadLicense();
                    this.resetCodeUI();
                    return;
                }
                
                // Обновляем статус
                if (isEarlyCheck) {
                    statusEl.textContent = `⏳ Подключение...`;
                } else {
                    const elapsed = Math.ceil(attempts * 2 / 60);
                    statusEl.textContent = `⏳ Ожидание... (${elapsed} мин)`;
                }
                
            } catch (error) {
                if (!isEarlyCheck) {
                    console.error('Ошибка проверки:', error);
                    statusEl.textContent = '⚠️ Повтор проверки...';
                    statusEl.style.color = 'var(--warning)';
                }
            }
            
            if (attempts < maxAttempts) {
                this.checkInterval = setTimeout(check, interval);
            } else {
                statusEl.textContent = '❌ Время истекло. Получите новый код.';
                statusEl.style.color = 'var(--danger)';
                this.resetCodeUI();
            }
        };
        
        // Первая проверка сразу
        this.checkInterval = setTimeout(check, 100);
    }
    
    // Остановка всех проверок
    stopConnectionCheck() {
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
        }
        if (this.checkInterval) {
            clearTimeout(this.checkInterval);
            this.checkInterval = null;
        }
        if (this.sseTimeout) {
            clearTimeout(this.sseTimeout);
            this.sseTimeout = null;
        }
    }

    resetCodeUI() {
        document.getElementById('generateCodeBtn').textContent = 'Получить код';
        document.getElementById('generateCodeBtn').disabled = false;
        document.getElementById('newCodeBtn').classList.add('hidden');
        document.getElementById('connectCode').textContent = '--------';
        document.getElementById('connectCode').classList.remove('active');
        document.getElementById('openBotBtn').style.display = 'none';
        this.currentCode = null; // Очищаем сохранённый код
    }

    async disconnectTelegram() {
        // Останавливаем все проверки подключения (включая SSE)
        this.stopConnectionCheck();
        
        // Очищаем настройки через background.js
        await this.sendMessage({
            action: 'updateSettings',
            settings: {
                telegramConnected: false,
                telegramUserId: null,
                telegramUsername: null
            }
        });
        
        this.showNotConnected();
        await this.loadLicense();
    }

    // ============ License ============
    async loadLicense() {
        const statusEl = document.getElementById('licenseStatus');
        const detailsEl = document.getElementById('licenseDetails');
        const badgeEl = document.getElementById('licenseBadgeOptions');
        
        // Получаем лицензию через background.js
        const response = await this.sendMessage({ action: 'getLicense' });
        const license = response?.license;
        
        if (!license || license.error === 'NOT_CONNECTED') {
            statusEl.textContent = 'Не подключено';
            detailsEl.textContent = 'Подключите Telegram для активации';
            badgeEl.textContent = '—';
            badgeEl.className = 'license-badge';
            return;
        }
        
        if (!license.valid) {
            // Проверяем специфические ошибки
            if (license.error === 'MAX_DEVICES') {
                statusEl.textContent = 'Лимит устройств';
                detailsEl.textContent = `Максимум ${license.maxDevices || 2} устр. Удалите старое в боте.`;
                badgeEl.textContent = '⚠️';
                badgeEl.className = 'license-badge';
            } else if (license.error === 'NO_LICENSE') {
                statusEl.textContent = 'Нет лицензии';
                detailsEl.textContent = 'Обратитесь к @YaMob для покупки';
                badgeEl.textContent = '—';
                badgeEl.className = 'license-badge';
            } else {
                statusEl.textContent = 'Доступ истёк';
                detailsEl.textContent = 'Обратитесь к @YaMob для продления';
                badgeEl.textContent = '—';
                badgeEl.className = 'license-badge';
            }
            return;
        }
        
        const daysLeft = license.daysLeft || 0;
        const hoursLeft = license.expiresAt ? Math.max(0, Math.ceil((license.expiresAt - Date.now()) / (1000 * 60 * 60))) : 0;
        
        if (license.type === 'trial') {
            statusEl.textContent = 'Пробный период';
            detailsEl.textContent = `Осталось ${hoursLeft} ${this.pluralize(hoursLeft, 'час', 'часа', 'часов')}`;
            badgeEl.textContent = 'TRIAL';
            badgeEl.className = 'license-badge trial';
        } else {
            statusEl.textContent = 'Лицензия активна';
            detailsEl.textContent = `Осталось ${daysLeft} ${this.pluralize(daysLeft, 'день', 'дня', 'дней')}`;
            badgeEl.textContent = 'PRO';
            badgeEl.className = 'license-badge pro';
        }
    }

    pluralize(n, one, two, five) {
        const mod10 = n % 10;
        const mod100 = n % 100;
        
        if (mod100 >= 11 && mod100 <= 19) return five;
        if (mod10 === 1) return one;
        if (mod10 >= 2 && mod10 <= 4) return two;
        return five;
    }

    // ============ Stats ============
    async loadStats() {
        const response = await this.sendMessage({ action: 'getState' });
        const stats = response?.stats || {};
        
        document.getElementById('statTotal').textContent = stats.totalClicks || 0;
        document.getElementById('statToday').textContent = stats.todayClicks || 0;
        document.getElementById('statSession').textContent = stats.sessionClicks || 0;
    }

    async resetStats() {
        if (!confirm('Сбросить всю статистику?')) return;
        
        await this.sendMessage({ action: 'resetStats' });
        await this.loadStats();
    }

    // ============ Devices ============
    getDefaultDeviceName() {
        // Определяем платформу
        const ua = navigator.userAgent;
        let platform = 'Устройство';
        
        if (ua.includes('Windows')) platform = 'Windows';
        else if (ua.includes('Mac')) platform = 'Mac';
        else if (ua.includes('Linux')) platform = 'Linux';
        else if (ua.includes('Android')) platform = 'Android';
        else if (ua.includes('iPhone') || ua.includes('iPad')) platform = 'iOS';
        
        // Определяем браузер
        let browser = '';
        if (ua.includes('Chrome') && !ua.includes('Edg')) browser = 'Chrome';
        else if (ua.includes('Firefox')) browser = 'Firefox';
        else if (ua.includes('Edg')) browser = 'Edge';
        else if (ua.includes('Safari') && !ua.includes('Chrome')) browser = 'Safari';
        
        return browser ? `${platform} ${browser}` : platform;
    }

    async loadDevices() {
        const listEl = document.getElementById('devicesList');
        const countEl = document.getElementById('devicesCount');
        const hintEl = document.getElementById('devicesHint');
        
        listEl.innerHTML = '<div class="device-item"><div class="device-info"><span class="device-name">⏳ Загрузка...</span></div></div>';
        
        try {
            const result = await api.storage.local.get(['state']);
            const settings = result.state?.settings || {};
            
            if (!settings.telegramUserId) {
                listEl.innerHTML = '<div class="device-item"><div class="device-info"><span class="device-name">Не подключено</span></div></div>';
                return;
            }
            
            const response = await fetch(`${SERVER_URL}/api/devices/list`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ oderId: settings.telegramUserId })
            });
            
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            // Сохраняем CSRF токен для критических операций
            this.csrfToken = data.csrfToken;
            
            countEl.textContent = `${data.devices.length}/${data.maxDevices}`;
            
            if (data.devices.length === 0) {
                listEl.innerHTML = '<div class="device-item"><div class="device-info"><span class="device-name">Нет устройств</span></div></div>';
                hintEl.classList.add('hidden');
                return;
            }
            
            // Показываем подсказку если лимит исчерпан
            if (data.devices.length >= data.maxDevices) {
                hintEl.classList.remove('hidden');
            } else {
                hintEl.classList.add('hidden');
            }
            
            listEl.innerHTML = data.devices.map(device => this.renderDeviceItem(device, settings.telegramUserId)).join('');
            
            // Привязываем события
            listEl.querySelectorAll('.device-btn.rename').forEach(btn => {
                btn.addEventListener('click', () => this.renameDevice(btn.dataset.id, btn.dataset.name));
            });
            
            listEl.querySelectorAll('.device-btn.delete').forEach(btn => {
                btn.addEventListener('click', () => this.removeDevice(btn.dataset.id, btn.dataset.name, btn.dataset.current === 'true'));
            });
            
        } catch (error) {
            console.error('Load devices error:', error);
            listEl.innerHTML = '<div class="device-item"><div class="device-info"><span class="device-name">❌ Ошибка загрузки</span></div></div>';
        }
    }

    renderDeviceItem(device, currentOderId) {
        const lastSeen = device.lastSeen ? this.formatTimeAgo(device.lastSeen) : 'никогда';
        const isCurrent = device.isCurrent;
        
        return `
            <div class="device-item ${isCurrent ? 'current' : ''}">
                <div class="device-info">
                    <span class="device-name">
                        ${this.escapeHtml(device.name)}
                        ${isCurrent ? '<span class="current-badge">Это устр.</span>' : ''}
                    </span>
                    <span class="device-meta">Активность: ${lastSeen}</span>
                </div>
                <div class="device-actions">
                    <button class="device-btn rename" data-id="${device.id}" data-name="${this.escapeHtml(device.name)}" title="Переименовать">✏️</button>
                    <button class="device-btn delete" data-id="${device.id}" data-name="${this.escapeHtml(device.name)}" data-current="${isCurrent}" title="Удалить">🗑️</button>
                </div>
            </div>
        `;
    }

    formatTimeAgo(timestamp) {
        const diff = Date.now() - timestamp;
        const minutes = Math.floor(diff / 60000);
        const hours = Math.floor(diff / 3600000);
        const days = Math.floor(diff / 86400000);
        
        if (minutes < 1) return 'только что';
        if (minutes < 60) return `${minutes} ${this.pluralize(minutes, 'минуту', 'минуты', 'минут')} назад`;
        if (hours < 24) return `${hours} ${this.pluralize(hours, 'час', 'часа', 'часов')} назад`;
        return `${days} ${this.pluralize(days, 'день', 'дня', 'дней')} назад`;
    }

    escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    async renameDevice(deviceId, currentName) {
        const newName = prompt('Новое название устройства:', currentName);
        if (!newName || newName === currentName) return;
        
        if (newName.length > 30) {
            alert('Название слишком длинное (максимум 30 символов)');
            return;
        }
        
        try {
            const result = await api.storage.local.get(['state']);
            const settings = result.state?.settings || {};
            
            const response = await fetch(`${SERVER_URL}/api/devices/rename`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    oderId: settings.telegramUserId,
                    deviceIdToRename: deviceId,
                    newName
                })
            });
            
            const data = await response.json();
            
            if (data.error) {
                if (data.error === 'NAME_ALREADY_EXISTS') {
                    alert('Устройство с таким именем уже существует');
                } else {
                    alert('Ошибка: ' + data.error);
                }
                return;
            }
            
            await this.loadDevices();
            
        } catch (error) {
            console.error('Rename device error:', error);
            alert('Ошибка переименования');
        }
    }

    async removeDevice(deviceId, deviceName, isCurrent) {
        const confirmMsg = isCurrent 
            ? `Удалить текущее устройство "${deviceName}"?\n\nВам придётся подключиться заново.`
            : `Удалить устройство "${deviceName}"?`;
        
        if (!confirm(confirmMsg)) return;
        
        try {
            const result = await api.storage.local.get(['state']);
            const settings = result.state?.settings || {};
            
            const response = await fetch(`${SERVER_URL}/api/devices/remove`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    oderId: settings.telegramUserId,
                    deviceIdToRemove: deviceId,
                    csrfToken: this.csrfToken // CSRF защита
                })
            });
            
            const data = await response.json();
            
            if (data.error) {
                alert('Ошибка: ' + data.error);
                return;
            }
            
            if (isCurrent) {
                // Если удалили текущее устройство - разлогиниваем
                await this.disconnectTelegram();
            } else {
                await this.loadDevices();
            }
            
        } catch (error) {
            console.error('Remove device error:', error);
            alert('Ошибка удаления');
        }
    }

    // ============ Messaging ============
    async sendMessage(message) {
        return new Promise((resolve) => {
            try {
                api.runtime.sendMessage(message, (response) => {
                    if (api.runtime.lastError) {
                        console.log('Message error:', api.runtime.lastError.message);
                        resolve({});
                        return;
                    }
                    resolve(response || {});
                });
            } catch (e) {
                console.log('SendMessage error:', e);
                resolve({});
            }
        });
    }
}

// Инициализация
document.addEventListener('DOMContentLoaded', () => {
    new OptionsController();
});
