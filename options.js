/**
 * Exotic Assistant - Options Controller
 * Минималистичная страница настроек
 */

const api = globalThis.browser ?? chrome;
const SERVER_URL = 'https://exotic-telegram.mabastik.workers.dev';

// ========== КОНСТАНТЫ ==========
const CONNECTION_CHECK_INTERVAL = 2000; // 2 секунды (было 5)
const CONNECTION_CHECK_FAST = 500; // Быстрая проверка для первых попыток
const MAX_CONNECTION_ATTEMPTS = 90; // 3 минуты (2с * 90)
const COPY_HINT_DURATION = 2000; // 2 секунды
const REQUEST_TIMEOUT = 5000; // 5 секунд (было 10)
const STATUS_CACHE_TTL = 60000; // Кэш статуса 1 минуту

class OptionsController {
    constructor() {
        this.deviceId = null;
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
        const result = await api.storage.local.get(['state']);
        this.deviceId = result.state?.settings?.deviceId;
        
        // Если deviceId отсутствует, он будет создан в background.js
        if (!this.deviceId) {
            // Генерируем временный ID для текущей сессии
            const array = new Uint8Array(16);
            crypto.getRandomValues(array);
            this.deviceId = Array.from(array, byte => byte.toString(16).padStart(2, '0')).join('');
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
        const display = String(username).startsWith('@') ? username : `@${username}`;
        document.getElementById('connectedUsername').textContent = display;
        document.getElementById('verifyStatus').textContent = '';
    }

    showNotConnected() {
        document.getElementById('telegramNotConnected').classList.remove('hidden');
        document.getElementById('telegramConnected').classList.add('hidden');
    }

    // Проверка привязки на сервере
    async verifyConnection() {
        const btn = document.getElementById('verifyBtn');
        const statusEl = document.getElementById('verifyStatus');
        
        btn.disabled = true;
        btn.textContent = '⏳ Проверка...';
        statusEl.textContent = '';
        
        try {
            const result = await chrome.storage.local.get(['state']);
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
                    transaction: {
                        transactionId: 'TEST-' + Date.now(),
                        amount: '1 000 ₽',
                        method: 'Тестовая заявка',
                        created: new Date().toLocaleString('ru-RU'),
                        requisites: '0000 0000 0000 0000',
                        bank: 'Тест банк'
                    }
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
            
            // Запрашиваем код с сервера
            const response = await fetch(`${SERVER_URL}/api/generate-code`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ oderId, deviceId: this.deviceId })
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
            
            // Сохраняем oderId для проверки статуса
            await this.sendMessage({
                action: 'updateSettings',
                settings: { telegramUserId: oderId, deviceId: this.deviceId }
            });
            
            btn.textContent = '✓ Код получен';
            statusEl.textContent = '⏳ Ожидание подключения...';
            statusEl.style.color = 'var(--warning)';
            
            // Начинаем проверку подключения
            this.startConnectionCheck(oderId);
            
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

    startConnectionCheck(oderId) {
        let attempts = 0;
        const maxAttempts = MAX_CONNECTION_ATTEMPTS;
        const statusEl = document.getElementById('connectionStatus');
        
        // Останавливаем предыдущую проверку если есть
        if (this.checkInterval) {
            clearTimeout(this.checkInterval);
        }
        
        const check = async () => {
            attempts++;
            
            // ОПТИМИЗАЦИЯ: Первые 10 проверок быстрее (500мс), потом 2с
            const isEarlyCheck = attempts <= 10;
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
                    // Сохраняем данные подключения через background.js
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
                // Не показываем ошибку на ранних проверках
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
        
        // ОПТИМИЗАЦИЯ: Первая проверка почти сразу (300мс)
        this.checkInterval = setTimeout(check, 300);
    }

    resetCodeUI() {
        document.getElementById('generateCodeBtn').textContent = 'Получить код';
        document.getElementById('generateCodeBtn').disabled = false;
        document.getElementById('newCodeBtn').classList.add('hidden');
        document.getElementById('connectCode').textContent = '--------';
        document.getElementById('connectCode').classList.remove('active');
        document.getElementById('openBotBtn').style.display = 'none';
    }

    async disconnectTelegram() {
        // Останавливаем проверку подключения
        if (this.checkInterval) {
            clearTimeout(this.checkInterval);
            this.checkInterval = null;
        }
        
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
            statusEl.textContent = 'Доступ истёк';
            detailsEl.textContent = 'Обратитесь к @YaMob для продления';
            badgeEl.textContent = '—';
            badgeEl.className = 'license-badge';
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
