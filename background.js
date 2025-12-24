const api = globalThis.browser ?? chrome;

class BackgroundService {
  TELEGRAM_SERVER = 'https://exotic-telegram.mabastik.workers.dev';
  
  LICENSE_CHECK_INTERVAL = 5 * 60 * 1000;

  constructor() {
    this.state = {
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
        history: {
          days: {}
        },
        efficiency: {
          successful: 0,
          failed: 0,
          accuracy: 100
        }
      },
      patterns: [],
      notifications: [],
      settings: {
        notifications: true,
        clickNotifications: true,
        detectionWarnings: true,
        stealthMode: true,
        minDelay: 5000,
        maxDelay: 60000,
        checkInterval: 2000,
        targetTexts: ['В работу', 'Взять в работу', 'Принять в работу', 'Take to work'],
        telemetryEnabled: false,
        statsPeriodDays: 30,
        statsTimezoneOffset: -new Date().getTimezoneOffset(),
        storeHistory: true,
        compactNotifications: false,
        telegramEnabled: true,
        telegramUserId: null,
        telegramConnected: false
      }
    };
    
    this.targetTabId = null;
    this.init();
  }

  async init() {
    await this.loadState();
    await this.generateDeviceId();
    await this.checkLicense();
    this.setupAlarms();
    this.setupListeners();
    this.startTabMonitor();
    this.startLicenseMonitor();
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
      const response = await fetch(`${this.TELEGRAM_SERVER}/api/license/check`, {
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
      console.error('License check error:', error);
      return this.state.license.valid;
    }
  }

  startLicenseMonitor() {
    setInterval(() => {
      this.checkLicense();
    }, this.LICENSE_CHECK_INTERVAL);
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
        patterns: loaded.patterns || this.state.patterns,
        notifications: loaded.notifications || this.state.notifications
      };
      this.normalizeHistory();
    }
  }

  async saveState() {
    await api.storage.local.set({ state: this.state });
  }

  scheduleSave(delay = 300) {
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
      }
    });

    api.runtime.onMessage.addListener((message, sender, sendResponse) => {
      this.handleMessage(message, sender, sendResponse);
      return true;
    });

    api.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
      if (changeInfo.status === 'complete' && tab.url?.includes('exotic.company/operator/payout_transaction')) {
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
    }, 5000);
  }

  async findTargetTab() {
    try {
      const tabs = await api.tabs.query({ 
        url: 'https://exotic.company/operator/payout_transaction*' 
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
          await this.sendTelemetry({
            event: 'click',
            clickTime: message.data.clickTime,
            successful: message.data.successful
          });
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
          
          if (this.state.enabled && this.state.settings.notifications) {
            await this.sendNotification('Автокликер включен', 'Автоматический кликер активирован');
          }

          await this.sendTelemetry({
            event: 'toggle',
            enabled: this.state.enabled
          });
          
          sendResponse({ success: true, license: this.state.license });
          break;
          
        case 'getState':
          sendResponse({ 
            enabled: this.state.enabled,
            stats: this.state.stats,
            statsSummary: this.getStatsSummary(this.state.settings.statsPeriodDays, this.state.settings.statsTimezoneOffset),
            settings: this.state.settings,
            patterns: this.state.patterns,
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
          
        case 'updateSettings':
          this.state.settings = { ...this.state.settings, ...message.settings };

          if (typeof this.state.settings.statsPeriodDays !== 'number' || ![7, 30, 90].includes(this.state.settings.statsPeriodDays)) {
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
          await this.sendTelemetry({ event: 'settings', settings: this.sanitizeSettingsForTelemetry(this.state.settings) });
          sendResponse({ success: true });
          break;
        
        case 'resetStats':
          await this.resetAllStats();
          sendResponse({ success: true });
          break;

        case 'savePattern':
          await this.savePattern(message.pattern);
          sendResponse({ success: true, patterns: this.state.patterns });
          break;

        case 'deletePattern':
          await this.deletePattern(message.id);
          sendResponse({ success: true, patterns: this.state.patterns });
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
          
        default:
          sendResponse({ success: false, error: 'Unknown action' });
      }
    } catch (error) {
      
      sendResponse({ success: false, error: error.message });
    }
  }

  async updateStats(data) {
    const today = new Date().toDateString();
    const lastUpdate = new Date(this.state.stats.todayStart).toDateString();
    
    if (today !== lastUpdate) {
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
        dayEntry.clicks += data.clicks;
        if (!Array.isArray(dayEntry.perHour)) {
          dayEntry.perHour = Array(24).fill(0);
        }
        dayEntry.perHour[hour] = (dayEntry.perHour[hour] || 0) + data.clicks;
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

  TELEGRAM_SERVER = 'https://exotic-telegram.mabastik.workers.dev';

  async sendTelegramNotification(clicks = 1, transaction = null) {
    const { telegramUserId, telegramConnected, telegramEnabled } = this.state.settings;
    
    
    
    if (!telegramUserId || !telegramConnected) {
      
      return;
    }

    try {
      
      const response = await fetch(`${this.TELEGRAM_SERVER}/api/notify`, {
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
      if (typeof entry.clicks !== 'number') entry.clicks = Number(entry.clicks) || 0;
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
        clicks: 0,
        perHour: Array(24).fill(0),
        lastClickTime: null,
        dateKey
      };
    }
    const entry = this.state.stats.history.days[dateKey];
    if (!Array.isArray(entry.perHour) || entry.perHour.length !== 24) {
      entry.perHour = Array.from({ length: 24 }, (_, idx) => entry.perHour?.[idx] || 0);
    }
    return entry;
  }

  pruneHistory(maxDays = 90) {
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
    const maxWindow = 90;
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

    const period = [7, 30, 90].includes(periodDays) ? periodDays : 30;
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
    this.state.stats = {
      totalClicks: 0,
      todayClicks: 0,
      todayStart: Date.now(),
      sessionClicks: 0,
      lastClickTime: null,
      history: {
        days: {}
      },
      efficiency: {
        successful: 0,
        failed: 0,
        accuracy: 100
      }
    };
    await this.saveState();
  }

  async savePattern(pattern) {
    if (!pattern) return;
    const withId = pattern.id ? pattern : { ...pattern, id: `pattern_${Date.now()}` };
    const existingIndex = this.state.patterns.findIndex(p => p.id === withId.id);
    if (existingIndex >= 0) {
      this.state.patterns[existingIndex] = withId;
    } else {
      this.state.patterns.push({ ...withId, createdAt: withId.createdAt || Date.now() });
    }
    await this.saveState();
  }

  async deletePattern(id) {
    if (!id) return;
    this.state.patterns = this.state.patterns.filter(p => p.id !== id);
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
    if (this.state.notifications.length > 100) {
      this.state.notifications.splice(0, this.state.notifications.length - 100);
    }
    await this.saveState();
  }

  async sendTabNotification(title, message) {
    try {
      const tabs = await api.tabs.query({ url: 'https://exotic.company/operator/payout_transaction*' });
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

  sanitizeSettingsForTelemetry(settings) {
    const {
      notifications,
      clickNotifications,
      detectionWarnings,
      stealthMode,
      minDelay,
      maxDelay,
      checkInterval,
      telemetryEnabled,
      statsPeriodDays,
      statsTimezoneOffset,
      storeHistory,
      compactNotifications
    } = settings || {};
    return {
      notifications,
      clickNotifications,
      detectionWarnings,
      stealthMode,
      minDelay,
      maxDelay,
      checkInterval,
      telemetryEnabled,
      statsPeriodDays,
      statsTimezoneOffset,
      storeHistory,
      compactNotifications
    };
  }

  async sendTelemetry(payload) {
    try {
      if (!this.state.settings.telemetryEnabled) return;
      const url = this.state.settings.telemetryEndpoint;
      if (!url) return;
      const body = {
        ...payload,
        ts: Date.now(),
        version: api.runtime?.getManifest ? api.runtime.getManifest().version : undefined,
        platform: navigator?.userAgent || 'unknown'
      };
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 4000);
      await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal
      }).catch(() => {});
      clearTimeout(timeout);
    } catch (err) {
      
    }
  }
}

const backgroundService = new BackgroundService();