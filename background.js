const api = globalThis.browser ?? chrome;

// ========== КОНСТАНТЫ (из constants.js) ==========
const TELEGRAM_SERVER = 'https://exotic-telegram.mabastik.workers.dev';
const LICENSE_CHECK_INTERVAL = 5 * 60 * 1000; // 5 минут
const TAB_MONITOR_INTERVAL = 5000;
const KEEP_ALIVE_INTERVAL_MINUTES = 0.4; // ~25 секунд
const AUTO_SAVE_DELAY = 300; // мс
const MAX_NOTIFICATIONS = 100;
const STATS_PERIODS = [7, 30, 90];
const MAX_STATS_DAYS = 90;

// ========== RATE LIMITER ==========
const RATE_LIMIT = {
  requests: 60,      // Максимум запросов
  windowMs: 60000,   // За минуту
  requestLog: []     // Лог запросов
};

function checkRateLimit() {
  const now = Date.now();
  // Очищаем старые записи
  RATE_LIMIT.requestLog = RATE_LIMIT.requestLog.filter(t => now - t < RATE_LIMIT.windowMs);
  
  if (RATE_LIMIT.requestLog.length >= RATE_LIMIT.requests) {
    console.warn('Rate limit exceeded');
    return false;
  }
  
  RATE_LIMIT.requestLog.push(now);
  return true;
}

// ========== RETRY LOGIC ==========
const RETRY_CONFIG = {
  maxAttempts: 3,
  baseDelay: 1000,
  maxDelay: 30000,
  backoffMultiplier: 2
};

async function fetchWithRetry(url, options = {}, attempt = 1) {
  try {
    // Проверяем rate limit
    if (!checkRateLimit()) {
      throw new Error('Rate limit exceeded');
    }
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000); // Уменьшили с 10с до 5с
    
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    
    clearTimeout(timeoutId);
    
    if (!response.ok && response.status >= 500 && attempt < RETRY_CONFIG.maxAttempts) {
      throw new Error(`Server error: ${response.status}`);
    }
    
    return response;
  } catch (error) {
    if (attempt >= RETRY_CONFIG.maxAttempts) {
      throw error;
    }
    
    // Exponential backoff - уменьшили начальную задержку
    const delay = Math.min(
      500 * Math.pow(RETRY_CONFIG.backoffMultiplier, attempt - 1), // Было 1000
      RETRY_CONFIG.maxDelay
    );
    
    // Retry with backoff
    await new Promise(resolve => setTimeout(resolve, delay));
    
    return fetchWithRetry(url, options, attempt + 1);
  }
}

// ========== НАЧАЛЬНОЕ СОСТОЯНИЕ ==========
const DEFAULT_STATE = {
  enabled: false,
  license: {
    valid: false,
    type: null,
    expiresAt: null,
    daysLeft: 0,
    lastCheck: 0,
    error: null
  },
  stats: {
    totalClicks: 0,
    todayClicks: 0,
    todayStart: Date.now(),
    sessionClicks: 0,
    lastClickTime: null,
    history: { days: {} },
    efficiency: {
      successful: 0,
      failed: 0,
      accuracy: 100
    }
  },
  notifications: [],
  settings: {
    notifications: true,
    clickNotifications: true,
    minDelay: 5000,
    maxDelay: 60000,
    checkInterval: 2000,
    targetTexts: ['В работу', 'Взять в работу', 'Принять в работу', 'Take to work'],
    statsPeriodDays: 30,
    statsTimezoneOffset: -new Date().getTimezoneOffset(),
    storeHistory: true,
    compactNotifications: false,
    telegramEnabled: true,
    telegramUserId: null,
    telegramConnected: false,
    soundEnabled: true,
    soundVolume: 0.5,
    autoRefreshEnabled: true,
    lastSync: null
  }
};

// Интервал синхронизации с ботом (15 минут)
const SYNC_INTERVAL = 15 * 60 * 1000;

class BackgroundService {
  constructor() {
    this.state = JSON.parse(JSON.stringify(DEFAULT_STATE));
    this.targetTabId = null;
    this.saveTimer = null;
    this.init();
  }

  async init() {
    // Загружаем состояние и сразу запускаем некритические процессы
    await this.loadState();
    
    // Эти операции не зависят от сети - выполняем сразу
    this.setupAlarms();
    this.setupListeners();
    this.startTabMonitor();
    this.updateBadge();
    
    // Генерация deviceId не зависит от сети
    await this.generateDeviceId();
    
    // Сетевые операции - выполняем параллельно
    // Если есть кэшированная лицензия - используем её, обновляем в фоне
    const hasValidCache = this.state.license.lastCheck && 
                          (Date.now() - this.state.license.lastCheck < 5 * 60 * 1000);
    
    if (hasValidCache) {
      // Кэш свежий - обновляем в фоне без блокировки
      this.checkLicense();
      this.syncWithBot();
    } else {
      // Нужно обновить - делаем параллельно
      await Promise.all([
        this.checkLicense(),
        this.syncWithBot()
      ]);
    }
    
    // Запускаем мониторы
    this.startLicenseMonitor();
    this.startSyncMonitor();
  }

  // Синхронизация настроек с ботом
  async syncWithBot() {
    const { telegramUserId, telegramConnected } = this.state.settings;
    
    if (!telegramUserId || !telegramConnected) {
      return { synced: false, reason: 'NOT_CONNECTED' };
    }

    try {
      const response = await fetchWithRetry(`${TELEGRAM_SERVER}/api/sync/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ oderId: telegramUserId })
      });

      const data = await response.json();
      
      if (data.connected) {
        // Обновляем данные лицензии
        if (data.license) {
          this.state.license = {
            valid: data.license.valid,
            type: data.license.type,
            typeName: data.license.typeName,
            expiresAt: data.license.expiresAt,
            daysLeft: data.license.daysLeft,
            lastCheck: Date.now(),
            error: null
          };
        }
        
        // Синхронизируем настройки уведомлений (из бота в расширение)
        if (data.settings) {
          this.state.settings.telegramEnabled = data.settings.clicks !== false;
        }
        
        this.state.settings.lastSync = Date.now();
        this.state.settings.serverVersion = data.version;
        
        this.scheduleSave();
        
        // Synced successfully
        return { synced: true, data };
      }
      
      return { synced: false, reason: 'NOT_FOUND' };
    } catch (error) {
      console.error('Sync error:', error.message);
      return { synced: false, reason: error.message };
    }
  }

  // Отправить настройки расширения на сервер
  async pushSettingsToServer() {
    const { telegramUserId, telegramConnected } = this.state.settings;
    
    if (!telegramUserId || !telegramConnected) {
      return false;
    }

    try {
      const response = await fetchWithRetry(`${TELEGRAM_SERVER}/api/settings/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          oderId: telegramUserId,
          settings: {
            clickNotifications: this.state.settings.clickNotifications,
            soundEnabled: this.state.settings.soundEnabled,
            extensionVersion: api.runtime.getManifest().version
          }
        })
      });

      const data = await response.json();
      return data.ok;
    } catch (error) {
      console.error('Push settings error:', error.message);
      return false;
    }
  }

  // Периодическая синхронизация
  startSyncMonitor() {
    setInterval(() => {
      this.syncWithBot();
    }, SYNC_INTERVAL);
  }

  async generateDeviceId() {
    if (!this.state.settings.deviceId) {
      const array = new Uint8Array(16);
      crypto.getRandomValues(array);
      const deviceId = Array.from(array, byte => byte.toString(16).padStart(2, '0')).join('');
      this.state.settings.deviceId = deviceId;
      await this.saveState();
    }
  }

  async checkLicense() {
    const { telegramUserId, deviceId } = this.state.settings;
    
    if (!telegramUserId) {
      this.state.license = {
        valid: false,
        type: null,
        expiresAt: null,
        daysLeft: 0,
        lastCheck: Date.now(),
        error: 'NOT_CONNECTED'
      };
      return false;
    }

    try {
      const response = await fetchWithRetry(`${TELEGRAM_SERVER}/api/license/check`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          oderId: telegramUserId,
          deviceId: deviceId
        })
      });

      const data = await response.json();
      
      this.state.license = {
        valid: data.valid === true,
        type: data.type || null,
        expiresAt: data.expiresAt || null,
        daysLeft: data.daysLeft || 0,
        lastCheck: Date.now(),
        error: data.valid ? null : data.error
      };

      if (!data.valid && this.state.enabled) {
        this.state.enabled = false;
        await this.sendTabNotification('Лицензия неактивна', 'Проверьте статус лицензии в настройках Exotic Assistant.');
      }

      this.scheduleSave();
      return data.valid;
    } catch (error) {
      console.error('License check error:', error.message);
      // Не сбрасываем статус лицензии при сетевой ошибке
      return this.state.license.valid;
    }
  }

  startLicenseMonitor() {
    setInterval(() => {
      this.checkLicense();
    }, LICENSE_CHECK_INTERVAL);
  }

  async loadState() {
    const data = await api.storage.local.get(['state']);
    if (data.state) {
      const loaded = data.state;
      this.state = {
        ...this.state,
        ...loaded,
        stats: { ...this.state.stats, ...(loaded.stats || {}) },
        settings: { ...this.state.settings, ...(loaded.settings || {}) },
        notifications: loaded.notifications || this.state.notifications
      };
      this.normalizeHistory();
    }
  }

  async saveState() {
    await api.storage.local.set({ state: this.state });
  }

  scheduleSave(delay = AUTO_SAVE_DELAY) {
    if (this.saveTimer) return;
    this.saveTimer = setTimeout(async () => {
      this.saveTimer = null;
      await this.saveState();
    }, delay);
  }

  setupAlarms() {
    api.alarms.create('dailyReset', {
      when: this.getNextMidnight(),
      periodInMinutes: 1440
    });
    
    // KeepAlive alarm - пробуждает Service Worker каждые 25 секунд
    api.alarms.create('keepAlive', {
      periodInMinutes: KEEP_ALIVE_INTERVAL_MINUTES
    });
    
    // License check alarm
    api.alarms.create('licenseCheck', {
      periodInMinutes: LICENSE_CHECK_INTERVAL / 60000
    });
  }

  getNextMidnight() {
    const now = new Date();
    const midnight = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
    return midnight.getTime();
  }

  getDateKey(date = new Date()) {
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const d = String(date.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }

  getUTCDateKey(date = new Date()) {
    const y = date.getUTCFullYear();
    const m = String(date.getUTCMonth() + 1).padStart(2, '0');
    const d = String(date.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }

  setupListeners() {
    api.alarms.onAlarm.addListener((alarm) => {
      if (alarm.name === 'dailyReset') {
        this.resetDailyStats();
      } else if (alarm.name === 'keepAlive') {
        this.keepAlive();
      } else if (alarm.name === 'licenseCheck') {
        this.checkLicense();
      }
    });

    api.runtime.onMessage.addListener((message, sender, sendResponse) => {
      this.handleMessage(message, sender, sendResponse);
      return true;
    });

    api.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
      // Поддержка всех доменов exotic.company
      if (changeInfo.status === 'complete' && tab?.url?.includes('exotic.company')) {
        this.targetTabId = tabId;
      }
    });

    api.tabs.onRemoved.addListener((tabId) => {
      if (this.targetTabId === tabId) {
        this.targetTabId = null;
      }
    });
  }

  async startTabMonitor() {
    setInterval(async () => {
      if (this.targetTabId) {
        try {
          await api.tabs.get(this.targetTabId);
        } catch (error) {
          this.targetTabId = null;
        }
      }
      
      if (!this.targetTabId) {
        await this.findTargetTab();
      }
    }, TAB_MONITOR_INTERVAL);
  }

  async findTargetTab() {
    try {
      // Ищем на всех доменах exotic.company
      const tabs = await api.tabs.query({ 
        url: '*://*.exotic.company/*' 
      });
      
      if (tabs.length > 0) {
        this.targetTabId = tabs[0].id;
      }
    } catch (error) {
      
    }
  }

  async handleMessage(message, sender, sendResponse) {
    try {
      switch (message.action) {
        case 'updateStats':
          await this.updateStats(message.data);
          if (message.data.clickTime && this.state.settings.clickNotifications) {
            await this.sendClickNotification(message.data.clickTime);
          }
          // Воспроизводим звук при клике
          if (this.state.settings.soundEnabled) {
            await this.playSound('click');
          }
          this.updateBadge();
          sendResponse({ success: true });
          break;
          
        case 'getStats':
          sendResponse({ stats: this.state.stats });
          break;
          
        case 'toggle':
          if (message.enabled) {
            const licenseValid = await this.checkLicense();
            if (!licenseValid) {
              sendResponse({ 
                success: false, 
                error: 'LICENSE_REQUIRED',
                license: this.state.license
              });
              break;
            }
          }
          
          this.state.enabled = message.enabled;
          if (!this.state.enabled) {
            this.state.stats.sessionClicks = 0;
            this.state.stats.lastClickTime = null;
          }
          await this.saveState();
          this.updateBadge();
          
          // ВАЖНО: Отправляем команду toggle во ВСЕ вкладки exotic.company
          await this.broadcastToggle(this.state.enabled);
          
          if (this.state.enabled && this.state.settings.notifications) {
            await this.sendNotification('Автокликер включен', 'Автоматический кликер активирован');
          }
          
          // Звук при включении/выключении
          if (this.state.settings.soundEnabled) {
            await this.playSound(this.state.enabled ? 'enable' : 'disable');
          }
          
          sendResponse({ success: true, license: this.state.license });
          break;
          
        case 'getState':
          sendResponse({ 
            enabled: this.state.enabled,
            stats: this.state.stats,
            statsSummary: this.getStatsSummary(this.state.settings.statsPeriodDays, this.state.settings.statsTimezoneOffset),
            settings: this.state.settings,
            license: this.state.license
          });
          break;
          
        case 'getLicense':
          await this.checkLicense();
          sendResponse({ license: this.state.license });
          break;
          
        case 'refreshLicense':
          await this.checkLicense();
          sendResponse({ license: this.state.license });
          break;
          
        case 'syncWithBot':
          const syncResult = await this.syncWithBot();
          sendResponse(syncResult);
          break;
          
        case 'pushSettings':
          const pushResult = await this.pushSettingsToServer();
          sendResponse({ success: pushResult });
          break;
          
        case 'updateSettings':
          this.state.settings = { ...this.state.settings, ...message.settings };

          if (typeof this.state.settings.statsPeriodDays !== 'number' || !STATS_PERIODS.includes(this.state.settings.statsPeriodDays)) {
            this.state.settings.statsPeriodDays = 30;
          }

          const maxOffset = 14 * 60;
          if (typeof this.state.settings.statsTimezoneOffset !== 'number') {
            this.state.settings.statsTimezoneOffset = -new Date().getTimezoneOffset();
          } else {
            this.state.settings.statsTimezoneOffset = Math.max(-maxOffset, Math.min(maxOffset, this.state.settings.statsTimezoneOffset));
          }

          if (this.state.settings.storeHistory === false) {
            this.state.stats.history = { days: {} };
          }

          if (message.settings?.resetStats) {
            await this.resetAllStats();
          } else {
            await this.saveState();
          }
          
          // Синхронизируем изменённые настройки на сервер
          if (this.state.settings.telegramConnected) {
            this.pushSettingsToServer();
          }
          
          sendResponse({ success: true });
          break;
        
        case 'resetStats':
          await this.resetAllStats();
          sendResponse({ success: true });
          break;

        case 'getNotifications':
          sendResponse({ notifications: this.getRecentNotifications(message.limit) });
          break;

        case 'markNotificationRead':
          await this.markNotification(message.id, true);
          sendResponse({ success: true });
          break;

        case 'clearNotifications':
          await this.clearNotifications();
          sendResponse({ success: true });
          break;

        case 'shouldRefresh':
          // Content script спрашивает нужно ли обновить страницу
          const shouldRefresh = this.state.enabled && this.shouldAutoRefresh(message.reason);
          if (shouldRefresh) {
            this.addNotification('🔄 Авто-обновление', `Причина: ${message.reason}`, 'info');
          }
          sendResponse({ shouldRefresh });
          break;

        case 'pageRefreshing':
          // Content script сообщает что обновляет страницу
          this.addNotification('🔄 Страница обновлена', message.reason, 'info');
          sendResponse({ success: true });
          break;
          
        default:
          sendResponse({ success: false, error: 'Unknown action' });
      }
    } catch (error) {
      
      sendResponse({ success: false, error: error.message });
    }
  }

  shouldAutoRefresh(reason) {
    // Разрешаем авто-обновление только если кликер включен
    if (!this.state.enabled) return false;
    
    // Проверяем настройку авто-обновления
    if (this.state.settings.autoRefreshEnabled === false) return false;
    
    return true;
  }

  async updateStats(data) {
    const todayKey = this.getDateKey();
    const lastUpdateKey = this.getDateKey(new Date(this.state.stats.todayStart));
    
    if (todayKey !== lastUpdateKey) {
      await this.resetDailyStats();
    }

    if (data.clicks) {
      this.state.stats.totalClicks += data.clicks;
      this.state.stats.todayClicks += data.clicks;
      this.state.stats.sessionClicks += data.clicks;
      this.state.stats.lastClickTime = Date.now();

      if (this.state.settings.telegramEnabled) {
        this.sendTelegramNotification(data.clicks, data.transaction);
      }

      if (this.state.settings.storeHistory !== false) {
        const now = new Date();
        const dateKey = this.getUTCDateKey(now);
        const hour = now.getUTCHours();
        const dayEntry = this.ensureHistoryEntry(dateKey);
        if (!Array.isArray(dayEntry.perHour)) {
          dayEntry.perHour = Array(24).fill(0);
        }
        dayEntry.perHour[hour] = (dayEntry.perHour[hour] || 0) + data.clicks;
        dayEntry.total = dayEntry.perHour.reduce((sum, val) => sum + val, 0);
        dayEntry.lastClickTime = Date.now();
        this.pruneHistory();
      }
    }

    if (data.successful !== undefined) {
      this.state.stats.efficiency.successful += data.successful;
      this.state.stats.efficiency.failed += data.failed || 0;
      this.state.stats.efficiency.accuracy = 
        (this.state.stats.efficiency.successful / 
         (this.state.stats.efficiency.successful + this.state.stats.efficiency.failed) * 100) || 100;
    }

    this.scheduleSave();
  }

  async sendTelegramNotification(clicks = 1, transaction = null) {
    const { telegramUserId, telegramConnected, telegramEnabled } = this.state.settings;
    
    if (!telegramUserId || !telegramConnected || !telegramEnabled) {
      return;
    }
    
    try {
      const response = await fetchWithRetry(`${TELEGRAM_SERVER}/api/notify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          oderId: telegramUserId,
          transaction: transaction
        })
      });

      const data = await response.json();
      
      if (data.error === 'User not connected') {
        this.state.settings.telegramConnected = false;
        this.scheduleSave();
      }
    } catch (error) {
      console.error('[BG] Telegram notification error:', error.message);
    }
  }

  async resetDailyStats() {
    this.state.stats.todayClicks = 0;
    this.state.stats.todayStart = Date.now();
    this.state.stats.efficiency.successful = 0;
    this.state.stats.efficiency.failed = 0;
    this.state.stats.efficiency.accuracy = 100;
    await this.saveState();
  }

  normalizeHistory() {
    if (!this.state.stats.history) {
      this.state.stats.history = { days: {} };
    }
    const days = this.state.stats.history.days || {};
    Object.keys(days).forEach(key => {
      const entry = days[key] || {};
      if (!Array.isArray(entry.perHour) || entry.perHour.length !== 24) {
        entry.perHour = Array.from({ length: 24 }, (_, idx) => entry.perHour?.[idx] || 0);
      }
      // Миграция: clicks -> total
      if (entry.clicks !== undefined && entry.total === undefined) {
        entry.total = entry.clicks;
        delete entry.clicks;
      }
      if (typeof entry.total !== 'number') {
        entry.total = entry.perHour.reduce((sum, val) => sum + (val || 0), 0);
      }
      days[key] = entry;
    });
    this.state.stats.history.days = days;
    this.pruneHistory();
  }

  ensureHistoryEntry(dateKey) {
    if (!this.state.stats.history) {
      this.state.stats.history = { days: {} };
    }
    if (!this.state.stats.history.days[dateKey]) {
      this.state.stats.history.days[dateKey] = {
        total: 0,
        perHour: Array(24).fill(0),
        lastClickTime: null,
        dateKey
      };
    }
    const entry = this.state.stats.history.days[dateKey];
    if (!Array.isArray(entry.perHour) || entry.perHour.length !== 24) {
      entry.perHour = Array.from({ length: 24 }, (_, idx) => entry.perHour?.[idx] || 0);
    }
    // Миграция: если есть clicks, переносим в total
    if (entry.clicks !== undefined && entry.total === undefined) {
      entry.total = entry.clicks;
      delete entry.clicks;
    }
    return entry;
  }

  pruneHistory(maxDays = MAX_STATS_DAYS) {
    const days = this.state.stats.history?.days || {};
    const keys = Object.keys(days).sort();
    while (keys.length > maxDays) {
      const oldest = keys.shift();
      delete days[oldest];
    }
    this.state.stats.history.days = days;
  }

  getStatsSummary(periodDays = 30, tzOffsetMinutes = this.state.settings.statsTimezoneOffset || 0) {
    const days = this.state.stats.history?.days || {};
    const maxWindow = MAX_STATS_DAYS;
    const todayUTC = new Date();
    const calendar = [];
    const totals = { 7: 0, 30: 0, 90: 0 };
    const hourlyByPeriod = {
      7: Array(24).fill(0),
      30: Array(24).fill(0),
      90: Array(24).fill(0)
    };
    const bestDayByPeriod = {
      7: { date: null, clicks: 0 },
      30: { date: null, clicks: 0 },
      90: { date: null, clicks: 0 }
    };

    const shiftHour = (hour, offset) => {
      const totalMinutes = hour * 60 + offset;
      const dayDelta = Math.floor(totalMinutes / 1440);
      const normalized = ((totalMinutes % 1440) + 1440) % 1440;
      const newHour = Math.floor(normalized / 60);
      return { newHour, dayDelta };
    };

    const addToMap = (map, dateKey, hour, clicks) => {
      if (!map[dateKey]) {
        map[dateKey] = Array(24).fill(0);
      }
      map[dateKey][hour] = (map[dateKey][hour] || 0) + clicks;
    };

    const shiftedPerDay = {};
    Object.entries(days).forEach(([key, entry]) => {
      if (!entry?.perHour) return;
      const [y, m, d] = key.split('-').map(Number);
      const baseDate = Date.UTC(y, m - 1, d);
      entry.perHour.forEach((val, hourIdx) => {
        if (!val) return;
        const { newHour, dayDelta } = shiftHour(hourIdx, tzOffsetMinutes);
        const targetDate = new Date(baseDate + dayDelta * 86400000);
        const targetKey = this.getUTCDateKey(targetDate);
        addToMap(shiftedPerDay, targetKey, newHour, val);
      });
    });

    for (let i = 0; i < maxWindow; i++) {
      const d = new Date(todayUTC);
      d.setUTCDate(todayUTC.getUTCDate() - i);
      const key = this.getUTCDateKey(d);
      const perHour = shiftedPerDay[key] || Array(24).fill(0);
      const clicks = perHour.reduce((s, v) => s + (v || 0), 0);

      calendar.push({ date: key, clicks });
      if (i < 7) {
        totals[7] += clicks;
        if (clicks > bestDayByPeriod[7].clicks) bestDayByPeriod[7] = { date: key, clicks };
        perHour.forEach((v, idx) => hourlyByPeriod[7][idx] += v || 0);
      }
      if (i < 30) {
        totals[30] += clicks;
        if (clicks > bestDayByPeriod[30].clicks) bestDayByPeriod[30] = { date: key, clicks };
        perHour.forEach((v, idx) => hourlyByPeriod[30][idx] += v || 0);
      }
      if (i < 90) {
        totals[90] += clicks;
        if (clicks > bestDayByPeriod[90].clicks) bestDayByPeriod[90] = { date: key, clicks };
        perHour.forEach((v, idx) => hourlyByPeriod[90][idx] += v || 0);
      }
    }

    const period = STATS_PERIODS.includes(periodDays) ? periodDays : 30;
    const hourlyTotals = hourlyByPeriod[period];
    const bestHourData = hourlyTotals.reduce(
      (best, val, idx) => (val > best.val ? { val, idx } : best),
      { val: 0, idx: null }
    );

    return {
      calendar: calendar.reverse(),
      periodTotals: { 7: totals[7], 30: totals[30], 90: totals[90] },
      hourlyTotals,
      hourlyTotalsAll: hourlyByPeriod,
      bestHour: {
        hour: bestHourData.idx,
        clicks: bestHourData.val
      },
      bestDay: bestDayByPeriod[period]
    };
  }

  async resetAllStats() {
    this.state.stats = JSON.parse(JSON.stringify(DEFAULT_STATE.stats));
    this.state.stats.todayStart = Date.now();
    await this.saveState();
  }

  async sendClickNotification(clickTime) {
    const time = new Date(clickTime).toLocaleTimeString('ru-RU', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });

    await this.sendNotification(
      'Клик выполнен',
      `Время: ${time}\nВсего сегодня: ${this.state.stats.todayClicks}`
    );
  }

  async sendNotification(title, message) {
    const notification = {
      id: `exotic_${Date.now()}`,
      title,
      message,
      timestamp: Date.now(),
      read: false
    };
    let shown = false;
    if (api.notifications?.create) {
      try {
        await api.notifications.create(notification.id, {
          type: 'basic',
          iconUrl: 'icons/icon128.png',
          title: title,
          message: message,
          priority: 1,
          requireInteraction: false
        });
        shown = true;
      } catch (err) {
        
      }
    }
    if (!shown) {
      await this.sendTabNotification(title, message);
    }
    this.state.notifications.push(notification);
    if (this.state.notifications.length > MAX_NOTIFICATIONS) {
      this.state.notifications.splice(0, this.state.notifications.length - MAX_NOTIFICATIONS);
    }
    await this.saveState();
  }

  async sendTabNotification(title, message) {
    try {
      const tabs = await api.tabs.query({ url: '*://*.exotic.company/*' });
      const targets = [];
      if (this.targetTabId) targets.push(this.targetTabId);
      tabs.forEach(t => targets.push(t.id));
      const unique = [...new Set(targets)];
      for (const id of unique) {
        try {
          await api.tabs.sendMessage(id, {
            action: 'localNotify',
            title,
            message
          });
        } catch (_) {
        }
      }
    } catch (err) {
      
    }
  }

  getRecentNotifications(limit = 20) {
    return [...(this.state.notifications || [])]
      .sort((a, b) => b.timestamp - a.timestamp)
      .slice(0, limit);
  }

  async markNotification(id, read = true) {
    const target = this.state.notifications.find(n => n.id === id);
    if (target) {
      target.read = read;
      await this.saveState();
    }
  }

  async clearNotifications() {
    this.state.notifications = [];
    await this.saveState();
  }

  // ========== BROADCAST TOGGLE ==========
  
  async broadcastToggle(enabled) {
    try {
      const tabs = await api.tabs.query({ 
        url: '*://*.exotic.company/*' 
      });
      
      for (const tab of tabs) {
        try {
          await api.tabs.sendMessage(tab.id, {
            action: 'toggle',
            enabled: enabled
          });
          // Toggle sent successfully
        } catch (e) {
          // Вкладка может быть не готова
        }
      }
    } catch (err) {
      // Broadcast error - ignore silently
    }
  }

  // ========== KEEPALIVE МЕХАНИЗМ ==========

  async keepAlive() {
    // Проверяем статус целевой вкладки
    if (this.targetTabId) {
      try {
        await api.tabs.get(this.targetTabId);
      } catch (e) {
        this.targetTabId = null;
        await this.findTargetTab();
      }
    }
    
    // Пинг активных вкладок для поддержания соединения
    try {
      const tabs = await api.tabs.query({ 
        url: '*://*.exotic.company/*' 
      });
      
      for (const tab of tabs) {
        try {
          await api.tabs.sendMessage(tab.id, { action: 'ping' });
        } catch (e) {
          // Вкладка может быть не готова
        }
      }
    } catch (e) {
      // Игнорируем ошибки
    }
    
    // Обновляем badge
    this.updateBadge();
  }

  // ========== ИНДИКАТОР НА ИКОНКЕ (BADGE) ==========
  
  updateBadge() {
    try {
      const { enabled, license, stats } = this.state;
      
      let badgeText = '';
      let badgeColor = '#888888'; // Серый - неактивен
      
      if (!license.valid) {
        // Нет лицензии - красный
        badgeText = '!';
        badgeColor = '#FF0000';
      } else if (enabled) {
        // Активен - зеленый с количеством кликов за сессию
        badgeText = stats.sessionClicks > 0 ? String(stats.sessionClicks) : 'ON';
        badgeColor = '#00CC00';
        
        // Если кликов много, сокращаем
        if (stats.sessionClicks > 99) {
          badgeText = '99+';
        }
      } else {
        // Лицензия есть, но выключен - оранжевый
        badgeText = 'OFF';
        badgeColor = '#FF9900';
      }
      
      api.action.setBadgeText({ text: badgeText });
      api.action.setBadgeBackgroundColor({ color: badgeColor });
      
      // Tooltip с дополнительной информацией
      let title = 'Exotic Operator Assistant\n';
      if (!license.valid) {
        title += '⚠️ Лицензия неактивна';
      } else if (enabled) {
        title += `✅ Активен | Сегодня: ${stats.todayClicks} кликов`;
      } else {
        title += '⏸️ Приостановлен';
      }
      
      api.action.setTitle({ title });
      
    } catch (err) {
      console.error('Badge update error:', err);
    }
  }

  // ========== ЗВУКОВЫЕ УВЕДОМЛЕНИЯ ==========
  
  async playSound(type = 'click') {
    try {
      // В Service Worker нельзя воспроизводить звук напрямую,
      // отправляем команду в content script или popup
      const tabs = await api.tabs.query({ 
        url: '*://*.exotic.company/*' 
      });
      
      for (const tab of tabs) {
        try {
          await api.tabs.sendMessage(tab.id, {
            action: 'playSound',
            type: type,
            volume: this.state.settings.soundVolume || 0.5
          });
        } catch (e) {
          // Вкладка может быть недоступна
        }
      }
    } catch (err) {
      console.error('Play sound error:', err);
    }
  }
}

const backgroundService = new BackgroundService();