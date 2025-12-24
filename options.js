const api = globalThis.browser ?? chrome;


class OptionsController {
  constructor() {
    this.state = {
      settings: {},
      patterns: [],
      stats: {},
      statsSummary: {},
      notifications: [],
      enabled: false
    };
    this._eventRemovers = [];
    this._toastContainer = null;
    this.init();
  }

  async init() {
    await this.loadData();
    this.removeEventListeners();
    this.setupTabs();
    this.setupEventListeners();
    this.renderTimezoneOptions();
    this.updateStats();
    this.renderCalendar();
    this.renderHourlyChart();
    this.renderActivityChart();
    this.loadLicenseStatus();
  }

  removeEventListeners() {
    if (this._eventRemovers && this._eventRemovers.length) {
      this._eventRemovers.forEach(fn => { try { fn(); } catch (_) {} });
      this._eventRemovers = [];
    }
  }

  async loadData() {
    try {
      const response = await this.sendMessage({ action: 'getState' });
      if (response) {
        this.state.settings = response.settings || {};
        this.state.patterns = response.patterns || [];
        this.state.stats = response.stats || {};
        this.state.statsSummary = response.statsSummary || {};
        this.state.enabled = response.enabled || false;
        const notificationsResponse = await this.sendMessage({ action: 'getNotifications', limit: 20 });
        this.state.notifications = notificationsResponse?.notifications || [];
        this.applySettings();
      }
    } catch (error) {}
  }

  applySettings() {
    const settings = this.state.settings;
    
    if (!this._domCache) {
      this._domCache = {
        enabled: document.getElementById('enabled'),
        clickInterval: document.getElementById('clickInterval'),
        intervalValue: document.getElementById('intervalValue'),
        randomDelay: document.getElementById('randomDelay'),
        randomValue: document.getElementById('randomValue'),
        buttonSelector: document.getElementById('buttonSelector'),
        targetUrl: document.getElementById('targetUrl'),
        autoStart: document.getElementById('autoStart'),
        scanInterval: document.getElementById('scanInterval'),
        scanValue: document.getElementById('scanValue'),
        dailyLimit: document.getElementById('dailyLimit'),
        storeHistory: document.getElementById('storeHistory'),
        statsPeriodDays: document.getElementById('statsPeriodDays'),
        statsTimezoneOffset: document.getElementById('statsTimezoneOffset'),
        notifications: document.getElementById('notifications'),
        compactNotifications: document.getElementById('compactNotifications'),
        notificationSound: document.getElementById('notificationSound'),
        achievementNotifications: document.getElementById('achievementNotifications'),
        telemetryEnabled: document.getElementById('telemetryEnabled'),
        telegramEnabled: document.getElementById('telegramEnabled'),
        telegramBotToken: document.getElementById('telegramBotToken'),
        telegramChatId: document.getElementById('telegramChatId')
      };
    }
    const c = this._domCache;
    
    if (c.enabled) c.enabled.checked = this.state.enabled;
    if (c.clickInterval && c.intervalValue) {
      // Храним в секундах, отображаем с 1 знаком после запятой
      const interval = (settings.clickInterval ? settings.clickInterval / 1000 : 2.0);
      c.clickInterval.value = interval;
      c.intervalValue.textContent = interval.toFixed(1);
    }
    if (c.randomDelay && c.randomValue) {
      // Храним в секундах, отображаем с 1 знаком после запятой
      const delay = (settings.randomDelay ? settings.randomDelay / 1000 : 0.5);
      c.randomDelay.value = delay;
      c.randomValue.textContent = delay.toFixed(1);
    }
    if (c.buttonSelector) {
      c.buttonSelector.value = settings.buttonSelector || 'button[class*="workButton"]';
    }
    if (c.targetUrl) {
      c.targetUrl.value = settings.targetUrl || 'https://exotic.company/operator/payout_transaction';
    }
    if (c.autoStart) c.autoStart.checked = settings.autoStart !== false;
    if (c.scanInterval && c.scanValue) {
      const scan = settings.scanInterval || 1000;
      c.scanInterval.value = scan;
      c.scanValue.textContent = scan;
    }
    if (c.dailyLimit) c.dailyLimit.value = settings.dailyLimit || 0;
    
    if (c.storeHistory) c.storeHistory.checked = settings.storeHistory !== false;
    if (c.statsPeriodDays) c.statsPeriodDays.value = settings.statsPeriodDays || 30;
    if (c.statsTimezoneOffset) {
      this.renderTimezoneOptions();
      c.statsTimezoneOffset.value = String(settings.statsTimezoneOffset ?? -new Date().getTimezoneOffset());
    }
    
    if (c.notifications) c.notifications.checked = settings.notifications !== false;
    if (c.compactNotifications) c.compactNotifications.checked = settings.compactNotifications === true;
    if (c.notificationSound) c.notificationSound.checked = settings.notificationSound !== false;
    if (c.achievementNotifications) c.achievementNotifications.checked = settings.achievementNotifications !== false;
    if (c.telemetryEnabled) c.telemetryEnabled.checked = settings.telemetryEnabled === true;
    
    if (c.telegramEnabled) c.telegramEnabled.checked = settings.telegramEnabled === true;
    if (c.telegramBotToken) c.telegramBotToken.value = settings.telegramBotToken || '';
    if (c.telegramChatId) c.telegramChatId.value = settings.telegramChatId || '';
  }

  setupTabs() {
    if (this._tabHandlers) {
      this._tabHandlers.forEach(fn => fn());
    }
    this._tabHandlers = [];
    const tabs = document.querySelectorAll('.tab-btn');
    const contents = document.querySelectorAll('.tab-content');
    tabs.forEach(tab => {
      const handler = () => {
        const tabId = tab.dataset.tab;
        tabs.forEach(t => t.classList.remove('active'));
        contents.forEach(c => c.classList.remove('active'));
        tab.classList.add('active');
        const show = document.getElementById(tabId);
        if (show) show.classList.add('active');
      };
      tab.addEventListener('click', handler);
      this._tabHandlers.push(() => tab.removeEventListener('click', handler));
    });
    
    let anyActive = false;
    contents.forEach(c => {
      if (c.classList.contains('active')) {
        if (!anyActive) {
          anyActive = true;
        } else {
          c.classList.remove('active');
        }
      }
    });
    if (!anyActive && contents.length) contents[0].classList.add('active');
  }

  setupEventListeners() {
    const add = (el, event, handler) => {
      if (el) {
        el.addEventListener(event, handler);
        this._eventRemovers.push(() => el.removeEventListener(event, handler));
      }
    };
    
    const enabled = document.getElementById('enabled');
    if (enabled) {
      const handler = async (e) => {
        await this.sendMessage({ action: 'toggle', enabled: e.target.checked });
        this.state.enabled = e.target.checked;
      };
      add(enabled, 'change', handler);
    }

    const clickInterval = document.getElementById('clickInterval');
    const intervalValue = document.getElementById('intervalValue');
    if (clickInterval && intervalValue) {
      add(clickInterval, 'input', (e) => {
        intervalValue.textContent = Number(e.target.value).toFixed(1);
      });
      add(clickInterval, 'change', async (e) => {
        await this.updateSettingPatch({ clickInterval: Math.round(Number(e.target.value) * 1000) });
      });
    }

    const randomDelay = document.getElementById('randomDelay');
    const randomValue = document.getElementById('randomValue');
    if (randomDelay && randomValue) {
      add(randomDelay, 'input', (e) => {
        randomValue.textContent = Number(e.target.value).toFixed(1);
      });
      add(randomDelay, 'change', async (e) => {
        await this.updateSettingPatch({ randomDelay: Math.round(Number(e.target.value) * 1000) });
      });
    }

    const buttonSelector = document.getElementById('buttonSelector');
    if (buttonSelector) {
      add(buttonSelector, 'change', async (e) => {
        await this.updateSettingPatch({ buttonSelector: e.target.value });
      });
    }

    const targetUrl = document.getElementById('targetUrl');
    if (targetUrl) {
      add(targetUrl, 'change', async (e) => {
        await this.updateSettingPatch({ targetUrl: e.target.value });
      });
    }

    const autoStart = document.getElementById('autoStart');
    if (autoStart) {
      add(autoStart, 'change', async (e) => {
        await this.updateSettingPatch({ autoStart: e.target.checked });
      });
    }

    const scanInterval = document.getElementById('scanInterval');
    const scanValue = document.getElementById('scanValue');
    if (scanInterval && scanValue) {
      add(scanInterval, 'input', (e) => {
        scanValue.textContent = e.target.value;
      });
      add(scanInterval, 'change', async (e) => {
        await this.updateSettingPatch({ scanInterval: Number(e.target.value) });
      });
    }

    const dailyLimit = document.getElementById('dailyLimit');
    if (dailyLimit) {
      add(dailyLimit, 'change', async (e) => {
        await this.updateSettingPatch({ dailyLimit: Number(e.target.value) });
      });
    }

    
    const storeHistory = document.getElementById('storeHistory');
    if (storeHistory) {
      add(storeHistory, 'change', async (e) => {
        await this.updateSettingPatch({ storeHistory: e.target.checked });
      });
    }

    const statsPeriodDays = document.getElementById('statsPeriodDays');
    if (statsPeriodDays) {
      add(statsPeriodDays, 'change', async (e) => {
        await this.updateSettingPatch({ statsPeriodDays: Number(e.target.value) });
      });
    }

    const statsTimezoneOffset = document.getElementById('statsTimezoneOffset');
    if (statsTimezoneOffset) {
      add(statsTimezoneOffset, 'change', async (e) => {
        await this.updateSettingPatch({ statsTimezoneOffset: Number(e.target.value) });
      });
    }

    const resetStats = document.getElementById('resetStats');
    if (resetStats) {
      add(resetStats, 'click', () => this.clearStats());
    }

    
    const notifications = document.getElementById('notifications');
    if (notifications) {
      add(notifications, 'change', async (e) => {
        await this.updateSettingPatch({ notifications: e.target.checked });
      });
    }

    const compactNotifications = document.getElementById('compactNotifications');
    if (compactNotifications) {
      add(compactNotifications, 'change', async (e) => {
        await this.updateSettingPatch({ compactNotifications: e.target.checked });
      });
    }

    const notificationSound = document.getElementById('notificationSound');
    if (notificationSound) {
      add(notificationSound, 'change', async (e) => {
        await this.updateSettingPatch({ notificationSound: e.target.checked });
      });
    }

    const achievementNotifications = document.getElementById('achievementNotifications');
    if (achievementNotifications) {
      add(achievementNotifications, 'change', async (e) => {
        await this.updateSettingPatch({ achievementNotifications: e.target.checked });
      });
    }

    const telemetryEnabled = document.getElementById('telemetryEnabled');
    if (telemetryEnabled) {
      add(telemetryEnabled, 'change', async (e) => {
        await this.updateSettingPatch({ telemetryEnabled: e.target.checked });
      });
    }

    
    const telegramEnabled = document.getElementById('telegramEnabled');
    if (telegramEnabled) {
      add(telegramEnabled, 'change', async (e) => {
        await this.updateSettingPatch({ telegramEnabled: e.target.checked });
      });
    }

    
    const connectTelegram = document.getElementById('connectTelegram');
    if (connectTelegram) {
      add(connectTelegram, 'click', () => this.connectTelegram());
    }

    const checkConnection = document.getElementById('checkConnection');
    if (checkConnection) {
      add(checkConnection, 'click', () => this.checkTelegramConnection());
    }

    const disconnectTelegram = document.getElementById('disconnectTelegram');
    if (disconnectTelegram) {
      add(disconnectTelegram, 'click', () => this.disconnectTelegram());
    }

    const testNotification = document.getElementById('testNotification');
    if (testNotification) {
      add(testNotification, 'click', () => this.testTelegramNotification());
    }

    
    this.checkTelegramStatus();

    
    const supportBtn = document.getElementById('supportBtn');
    if (supportBtn) {
      add(supportBtn, 'click', () => {
        window.open('https://t.me/Asadov_p2p', '_blank');
      });
    }

    
    const exportJson = document.getElementById('exportJson');
    if (exportJson) {
      add(exportJson, 'click', () => this.exportData('json'));
    }

    const exportCsv = document.getElementById('exportCsv');
    if (exportCsv) {
      add(exportCsv, 'click', () => this.exportData('csv'));
    }

    
    const chartPeriod = document.getElementById('chartPeriod');
    if (chartPeriod) {
      add(chartPeriod, 'change', () => {
        this.renderActivityChart();
      });
    }

    
    const resizeHandler = () => this.renderActivityChart();
    window.addEventListener('resize', resizeHandler);
    this._eventRemovers.push(() => window.removeEventListener('resize', resizeHandler));
  }

  renderStatus() {
    const badge = document.getElementById('statusIndicator');
    if (!badge) return;
    const on = this.state.enabled;
    badge.textContent = on ? 'Включен' : 'Выключен';
    badge.classList.toggle('on', on);
  }

  renderCalendar() {
    const grid = document.getElementById('calendarGrid');
    if (!grid) return;
    const calendar = this.state.statsSummary?.calendar || [];
    
    if (this._lastCalendar && JSON.stringify(this._lastCalendar) === JSON.stringify(calendar)) return;
    this._lastCalendar = JSON.parse(JSON.stringify(calendar));
    
    
    const days = [];
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    
    const startDate = new Date(today);
    startDate.setDate(startDate.getDate() - 34);
    while (startDate.getDay() !== 1) {
      startDate.setDate(startDate.getDate() - 1);
    }
    const clicksMap = {};
    calendar.forEach(day => {
      if (day.date) clicksMap[day.date] = day.clicks || 0;
    });
    const current = new Date(startDate);
    while (current <= today) {
      const dateStr = current.toISOString().split('T')[0];
      days.push({
        date: new Date(current),
        dateStr,
        clicks: clicksMap[dateStr] || 0
      });
      current.setDate(current.getDate() + 1);
    }
    while (days.length % 7 !== 0) {
      days.push({ date: null, dateStr: null, clicks: 0, empty: true });
    }
    const maxClicks = Math.max(...days.map(d => d.clicks), 1);
    grid.innerHTML = days.map(day => {
      if (day.empty) {
        return `<div class="calendar-day empty"></div>`;
      }
      const intensity = day.clicks / maxClicks;
      const opacity = day.clicks > 0 ? Math.max(0.2, Math.min(1, intensity)) : 0.05;
      const dayNum = day.date ? day.date.getDate() : '';
      const tooltip = day.dateStr ? `${dayNum}: ${day.clicks} кликов` : '';
      return `<div class="calendar-day" style="opacity: ${opacity}" data-tooltip="${tooltip}"></div>`;
    }).join('');
  }

  renderHourlyChart() {
    const container = document.getElementById('hourlyChart');
    if (!container) return;
    const hourly = this.state.statsSummary?.hourlyTotals || new Array(24).fill(0);
    if (this._lastHourly && JSON.stringify(this._lastHourly) === JSON.stringify(hourly)) return;
    this._lastHourly = JSON.parse(JSON.stringify(hourly));
    const max = Math.max(...hourly, 1);
    container.innerHTML = hourly.map((clicks, hour) => {
      const height = Math.max(4, (clicks / max) * 100);
      const tooltip = `${String(hour).padStart(2, '0')}:00 — ${clicks} кликов`;
      return `<div class="hourly-bar" style="height: ${height}%" data-tooltip="${tooltip}"></div>`;
    }).join('');
  }

  renderActivityChart() {
    const canvas = document.getElementById('activityChart');
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    const container = canvas.parentElement;
    
    
    const dpr = window.devicePixelRatio || 1;
    const rect = container.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);
    
    const width = rect.width;
    const height = rect.height;
    const padding = { top: 20, right: 20, bottom: 30, left: 45 };
    
    
    const calendar = this.state.statsSummary?.calendar || [];
    const period = parseInt(document.getElementById('chartPeriod')?.value || 30);
    
    
    const today = new Date();
    const data = [];
    
    for (let i = period - 1; i >= 0; i--) {
      const date = new Date(today);
      date.setDate(date.getDate() - i);
      const dateStr = date.toISOString().split('T')[0];
      const dayData = calendar.find(d => d.date === dateStr);
      data.push({
        date: date,
        clicks: dayData?.clicks || 0
      });
    }
    
    
    ctx.clearRect(0, 0, width, height);
    
    if (data.length === 0) {
      ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-tertiary').trim() || '#666';
      ctx.font = '14px -apple-system, BlinkMacSystemFont, sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('Нет данных', width / 2, height / 2);
      return;
    }
    
    const maxClicks = Math.max(...data.map(d => d.clicks), 10);
    const chartWidth = width - padding.left - padding.right;
    const chartHeight = height - padding.top - padding.bottom;
    
    
    const primaryColor = getComputedStyle(document.documentElement).getPropertyValue('--color-primary').trim() || '#0A84FF';
    const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-tertiary').trim() || '#666';
    const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border-subtle').trim() || '#333';
    
    
    ctx.strokeStyle = gridColor;
    ctx.lineWidth = 1;
    const gridLines = 4;
    for (let i = 0; i <= gridLines; i++) {
      const y = padding.top + (chartHeight / gridLines) * i;
      ctx.beginPath();
      ctx.moveTo(padding.left, y);
      ctx.lineTo(width - padding.right, y);
      ctx.stroke();
      
      const value = Math.round(maxClicks - (maxClicks / gridLines) * i);
      ctx.fillStyle = textColor;
      ctx.font = '11px -apple-system, BlinkMacSystemFont, sans-serif';
      ctx.textAlign = 'right';
      ctx.fillText(value, padding.left - 8, y + 4);
    }
  }

  async editPattern(id) {
    const pattern = this.state.patterns.find(p => p.id === id);
    if (!pattern) return;
    
    const newText = prompt('Введите новый текст для поиска:', pattern.text || '');
    if (newText !== null) {
      pattern.text = newText;
      
      await this.sendMessage({
        action: 'savePattern',
        pattern: pattern
      });
      
      await this.loadData();
      this.showToast('Паттерн изменен');
    }
  }

  async deletePattern(id) {
    if (!confirm('Удалить этот паттерн?')) return;
    
    await this.sendMessage({
      action: 'deletePattern',
      id: id
    });
    
    await this.loadData();
    this.showToast('Паттерн удален');
  }

  updateStats() {
    const stats = this.state.stats || {};
    const efficiency = stats.efficiency || {};
    const summary = this.state.statsSummary || {};
    
    
    const statsTotal = document.getElementById('statsTotal');
    const statsToday = document.getElementById('statsToday');
    const statsAccuracy = document.getElementById('statsAccuracy');
    const statsSession = document.getElementById('statsSession');

    if (statsTotal) statsTotal.textContent = this.formatNumber(stats.totalClicks || 0);
    if (statsToday) statsToday.textContent = this.formatNumber(stats.todayClicks || 0);
    if (statsAccuracy) statsAccuracy.textContent = `${(efficiency.accuracy || 100).toFixed(0)}%`;
    if (statsSession) statsSession.textContent = this.formatNumber(stats.sessionClicks || 0);

    
    const stats7Days = document.getElementById('stats7Days');
    const stats30Days = document.getElementById('stats30Days');
    const stats90Days = document.getElementById('stats90Days');
    const avg7 = document.getElementById('avg7');
    const avg30 = document.getElementById('avg30');
    const avg90 = document.getElementById('avg90');

    const p7 = summary.periodTotals?.[7] || 0;
    const p30 = summary.periodTotals?.[30] || 0;
    const p90 = summary.periodTotals?.[90] || 0;

    if (stats7Days) stats7Days.textContent = this.formatNumber(p7);
    if (stats30Days) stats30Days.textContent = this.formatNumber(p30);
    if (stats90Days) stats90Days.textContent = this.formatNumber(p90);
    
    if (avg7) avg7.textContent = p7 > 0 ? Math.round(p7 / 7) : 0;
    if (avg30) avg30.textContent = p30 > 0 ? Math.round(p30 / 30) : 0;
    if (avg90) avg90.textContent = p90 > 0 ? Math.round(p90 / 90) : 0;

    
    const statsBestHour = document.getElementById('statsBestHour');
    const statsBestDay = document.getElementById('statsBestDay');
    const statsBestDayClicks = document.getElementById('statsBestDayClicks');
    const statsLastClick = document.getElementById('statsLastClick');

    const bestHour = summary.bestHour || {};
    const bestHourLabel = bestHour.hour !== null && bestHour.hour !== undefined
      ? `${String(bestHour.hour).padStart(2, '0')}:00`
      : '—';
    if (statsBestHour) statsBestHour.textContent = bestHourLabel;

    const bestDay = summary.bestDay || {};
    if (statsBestDay) {
      if (bestDay.date && bestDay.clicks > 0) {
        const d = new Date(bestDay.date);
        const days = ['Вс', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб'];
        statsBestDay.textContent = `${days[d.getDay()]} ${d.getDate()}.${d.getMonth()+1}`;
      } else {
        statsBestDay.textContent = '—';
      }
    }
    
    if (statsBestDayClicks) {
      statsBestDayClicks.textContent = this.formatNumber(bestDay.clicks || 0);
    }

    if (statsLastClick) {
      if (stats.lastClickTime) {
        const d = new Date(stats.lastClickTime);
        statsLastClick.textContent = d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
      } else {
        statsLastClick.textContent = '—';
      }
    }
  }

  async saveSettings() {
    const settings = {
      stealthMode: document.getElementById('stealthMode').checked,
      notifications: document.getElementById('pushNotifications').checked,
      clickNotifications: document.getElementById('clickNotifications').checked,
      detectionWarnings: document.getElementById('detectionWarnings').checked,
      telemetryEnabled: document.getElementById('telemetryEnabled').checked,
      minDelay: parseInt(document.getElementById('minDelay').value),
      maxDelay: parseInt(document.getElementById('maxDelay').value),
      checkInterval: parseInt(document.getElementById('checkInterval').value),
      targetTexts: document.getElementById('targetTexts').value
        .split('\n')
        .map(text => text.trim())
        .filter(text => text.length > 0)
    };
    
    await this.sendMessage({
      action: 'updateSettings',
      settings: settings
    });

    
    if (settings.notifications || settings.clickNotifications) {
      if (typeof Notification !== 'undefined' && Notification.permission === 'default') {
        try { await Notification.requestPermission(); } catch (_) {}
      }
    }
    
    this.showToast('Настройки сохранены');
  }

  async exportData(format) {
    const data = {
      stats: this.state.stats,
      statsSummary: this.state.statsSummary,
      patterns: this.state.patterns,
      settings: this.state.settings,
      exportDate: new Date().toISOString(),
      version: '3.2'
    };
    
    let content, filename, type;
    
    switch (format) {
      case 'csv':
        content = this.convertToCSV(data);
        filename = `exotic_stats_${Date.now()}.csv`;
        type = 'text/csv';
        break;
      case 'json':
        content = JSON.stringify(data, null, 2);
        filename = `exotic_stats_${Date.now()}.json`;
        type = 'application/json';
        break;
      default:
        return;
    }
    
    this.downloadFile(content, filename, type);
    this.showToast(`Данные экспортированы в ${format.toUpperCase()}`);
  }

  convertToCSV(data) {
    const headers = ['Дата', 'Всего кликов', 'Кликов сегодня', 'Точность', 'В сессии', 'За 7 дней', 'За 30 дней'];
    const date = new Date().toLocaleDateString('ru-RU');
    
    const rows = [[
      date,
      data.stats.totalClicks || 0,
      data.stats.todayClicks || 0,
      `${(data.stats.efficiency?.accuracy || 100).toFixed(1)}%`,
      data.stats.sessionClicks || 0,
      data.statsSummary?.periodTotals?.[7] || 0,
      data.statsSummary?.periodTotals?.[30] || 0
    ]];
    
    return [
      headers.join(','),
      ...rows.map(row => row.join(','))
    ].join('\n');
  }

  downloadFile(content, filename, type) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  
  TELEGRAM_SERVER = 'https://exotic-telegram.mabastik.workers.dev';

  async connectTelegram() {
    const connectBtn = document.getElementById('connectTelegram');
    const linkBox = document.getElementById('telegramLinkBox');
    const telegramLink = document.getElementById('telegramLink');
    
    if (!connectBtn || !linkBox || !telegramLink) return;

    connectBtn.disabled = true;
    connectBtn.textContent = 'Загрузка...';

    try {
      
      let userId = this.state.settings.telegramUserId;
      if (!userId) {
        userId = 'user_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
        await this.updateSettingPatch({ telegramUserId: userId });
      }

      const response = await fetch(`${this.TELEGRAM_SERVER}/api/generate-code`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ oderId: userId })
      });

      const data = await response.json();

      if (data.success && data.link) {
        telegramLink.href = data.link;
        telegramLink.textContent = '🔗 Открыть бота в Telegram';
        linkBox.style.display = 'block';
        this.showToast('Ссылка получена! Нажмите на неё для подключения', 'success');
      } else {
        throw new Error(data.error || 'Не удалось получить код');
      }
    } catch (error) {
      console.error('Connect error:', error);
      this.showToast('❌ Ошибка подключения к серверу', 'error');
    } finally {
      connectBtn.disabled = false;
      connectBtn.innerHTML = `
        <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
          <path d="M16 8A8 8 0 1 1 0 8a8 8 0 0 1 16 0zM8.287 5.906c-.778.324-2.334.994-4.666 2.01-.378.15-.577.298-.595.442-.03.243.275.339.69.47l.175.055c.408.133.958.288 1.243.294.26.006.549-.1.868-.32 2.179-1.471 3.304-2.214 3.374-2.23.05-.012.12-.026.166.016.047.041.042.12.037.141-.03.129-1.227 1.241-1.846 1.817-.193.18-.33.307-.358.336a8.154 8.154 0 0 1-.188.186c-.38.366-.664.64.015 1.088.327.216.589.393.85.571.284.194.568.387.936.629.093.06.183.125.27.187.331.236.63.448.997.414.214-.02.435-.22.547-.82.265-1.417.786-4.486.906-5.751a1.426 1.426 0 0 0-.013-.315.337.337 0 0 0-.114-.217.526.526 0 0 0-.31-.093c-.3.005-.763.166-2.984 1.09z"/>
        </svg>
        Подключить
      `;
    }
  }

  async checkTelegramConnection() {
    const checkBtn = document.getElementById('checkConnection');
    if (checkBtn) {
      checkBtn.disabled = true;
      checkBtn.textContent = 'Проверка...';
    }

    try {
      const userId = this.state.settings.telegramUserId;
      if (!userId) {
        this.showToast('Сначала нажмите "Подключить"', 'error');
        return;
      }

      const response = await fetch(`${this.TELEGRAM_SERVER}/api/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ oderId: userId })
      });

      const data = await response.json();

      if (data.connected) {
        await this.updateSettingPatch({ telegramConnected: true });
        this.updateTelegramUI(true, data.username);
        this.showToast('✅ Telegram успешно подключен!', 'success');
        
        
        const linkBox = document.getElementById('telegramLinkBox');
        if (linkBox) linkBox.style.display = 'none';
        
        
        await this.loadLicenseStatus();
      } else {
        this.showToast('Telegram ещё не подключен. Нажмите на ссылку выше.', 'warning');
      }
    } catch (error) {
      console.error('Check connection error:', error);
      this.showToast('❌ Ошибка проверки подключения', 'error');
    } finally {
      if (checkBtn) {
        checkBtn.disabled = false;
        checkBtn.textContent = 'Проверить подключение';
      }
    }
  }

  async checkTelegramStatus() {
    const userId = this.state.settings.telegramUserId;
    if (!userId) return;

    try {
      const response = await fetch(`${this.TELEGRAM_SERVER}/api/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ oderId: userId })
      });

      const data = await response.json();
      this.updateTelegramUI(data.connected, data.username);
    } catch (error) {
      
    }
  }

  updateTelegramUI(connected, username = null) {
    const statusIndicator = document.querySelector('#telegramStatus .status-indicator');
    const statusText = document.getElementById('telegramStatusText');
    const connectRow = document.querySelector('.telegram-connect-row');
    const disconnectRow = document.getElementById('telegramDisconnectRow');
    const testRow = document.getElementById('testNotificationRow');

    if (connected) {
      if (statusIndicator) {
        statusIndicator.classList.remove('status-disconnected');
        statusIndicator.classList.add('status-connected');
      }
      if (statusText) {
        statusText.textContent = username ? `Подключено: @${username}` : 'Подключено';
      }
      if (connectRow) connectRow.style.display = 'none';
      if (disconnectRow) disconnectRow.style.display = 'flex';
      if (testRow) testRow.style.display = 'flex';
    } else {
      if (statusIndicator) {
        statusIndicator.classList.remove('status-connected');
        statusIndicator.classList.add('status-disconnected');
      }
      if (statusText) {
        statusText.textContent = 'Не подключено';
      }
      if (connectRow) connectRow.style.display = 'flex';
      if (disconnectRow) disconnectRow.style.display = 'none';
      if (testRow) testRow.style.display = 'none';
    }
  }

  async disconnectTelegram() {
    if (!confirm('Отключить Telegram уведомления?')) return;

    try {
      await this.updateSettingPatch({ 
        telegramConnected: false,
        telegramUserId: null 
      });
      this.updateTelegramUI(false);
      this.showToast('Telegram отключен', 'success');
    } catch (error) {
      console.error('Disconnect error:', error);
      this.showToast('❌ Ошибка отключения', 'error');
    }
  }

  async testTelegramNotification() {
    const testBtn = document.getElementById('testNotification');
    if (testBtn) {
      testBtn.disabled = true;
      testBtn.textContent = 'Отправка...';
    }

    try {
      const userId = this.state.settings.telegramUserId;
      if (!userId) {
        this.showToast('Сначала подключите Telegram', 'error');
        return;
      }

      
      const testTransaction = {
        transactionId: 'TEST-' + Date.now(),
        amount: '10 000 ₽',
        method: 'Банковская карта',
        created: new Date().toLocaleString('ru-RU', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' }),
        requisites: '2202 2083 **** 6158',
        bank: 'Sberbank (тест)'
      };

      const response = await fetch(`${this.TELEGRAM_SERVER}/api/notify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          oderId: userId,
          transaction: testTransaction
        })
      });

      const data = await response.json();
      

      if (data.success) {
        this.showToast('✅ Тестовое уведомление отправлено!', 'success');
      } else if (data.code === 'NOT_CONNECTED') {
        this.showToast('❌ Telegram не подключен. Переподключитесь.', 'error');
        this.updateTelegramUI(false);
      } else if (data.code === 'NO_LICENSE') {
        this.showToast('❌ Нет активной лицензии', 'error');
        this.loadLicenseStatus();
      } else {
        this.showToast(`❌ Ошибка: ${data.error || 'Неизвестная ошибка'}`, 'error');
      }
    } catch (error) {
      console.error('Test notification error:', error);
      this.showToast('❌ Ошибка отправки', 'error');
    } finally {
      if (testBtn) {
        testBtn.disabled = false;
        testBtn.innerHTML = `
          <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
            <path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/>
            <path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/>
          </svg>
          Тест
        `;
      }
    }
  }

  // ==================== License Methods ====================
    

  async loadLicenseStatus() {
    const licenseStatusBox = document.getElementById('licenseStatusBox');
    const licenseCard = document.getElementById('licenseCard');
    const licenseNotConnected = document.getElementById('licenseNotConnected');

    
    if (licenseStatusBox) licenseStatusBox.style.display = 'block';
    if (licenseCard) licenseCard.style.display = 'none';
    if (licenseNotConnected) licenseNotConnected.style.display = 'none';

    try {
      const response = await this.sendMessage({ action: 'getLicense' });
      const license = response?.license;

      
      if (licenseStatusBox) licenseStatusBox.style.display = 'none';

      if (!license || license.error === 'NOT_CONNECTED') {
        
        if (licenseNotConnected) licenseNotConnected.style.display = 'block';
        return;
      }

      
      if (licenseCard) licenseCard.style.display = 'block';
      this.updateLicenseUI(license);
    } catch (error) {
      console.error('Load license error:', error);
      if (licenseStatusBox) {
        licenseStatusBox.innerHTML = `
          <div style="color: var(--color-danger);">
            ❌ Ошибка загрузки статуса лицензии
          </div>
        `;
      }
    }
  }

  updateLicenseUI(license) {
    const badge = document.getElementById('licenseBadge');
    const icon = document.getElementById('licenseStatusIcon');
    const statusText = document.getElementById('licenseStatusText');
    const expiresText = document.getElementById('licenseExpiresText');
    const daysLeftText = document.getElementById('licenseDaysLeft');
    const actionsDiv = document.getElementById('licenseActions');

    
    const isValid = license.valid === true;
    const isTrial = license.type === 'trial';
    const daysLeft = license.daysLeft || 0;

    
    if (badge) {
      if (isValid) {
        if (isTrial) {
          badge.textContent = '🎁 Пробный период';
          badge.className = 'license-badge trial';
        } else {
          badge.textContent = '⭐ Подписка';
          badge.className = 'license-badge active';
        }
      } else {
        badge.textContent = '❌ Истекла';
        badge.className = 'license-badge expired';
      }
    }

    
    if (icon) {
      icon.textContent = isValid ? '✅' : '❌';
    }

    
    if (statusText) {
      if (isValid) {
        statusText.textContent = 'Активна';
        statusText.className = 'license-value success';
      } else {
        statusText.textContent = 'Истекла';
        statusText.className = 'license-value danger';
      }
    }

    
    if (expiresText && license.expiresAt) {
      const date = new Date(license.expiresAt);
      expiresText.textContent = date.toLocaleDateString('ru-RU', {
        day: 'numeric',
        month: 'long',
        year: 'numeric'
      });
    }

    
    if (daysLeftText) {
      if (isValid) {
        if (daysLeft <= 1) {
          daysLeftText.textContent = 'Менее 1 дня';
          daysLeftText.className = 'license-value danger';
        } else if (daysLeft <= 3) {
          daysLeftText.textContent = `${daysLeft} дн.`;
          daysLeftText.className = 'license-value warning';
        } else {
          daysLeftText.textContent = `${daysLeft} дн.`;
          daysLeftText.className = 'license-value success';
        }
      } else {
        daysLeftText.textContent = '—';
        daysLeftText.className = 'license-value';
      }
    }

    
    if (actionsDiv) {
      let actionsHtml = '';

      if (!isValid || daysLeft <= 3) {
        actionsHtml += `
          <a href="https://t.me/mabastik" target="_blank" class="btn-upgrade">
            ⭐ Продлить подписку
          </a>
        `;
      }

      actionsHtml += `
        <button class="btn-refresh-license" id="refreshLicense">
          🔄 Обновить
        </button>
      `;

      actionsDiv.innerHTML = actionsHtml;

      
      const refreshBtn = document.getElementById('refreshLicense');
      if (refreshBtn) {
        refreshBtn.addEventListener('click', () => this.refreshLicense());
      }
    }
  }

  async refreshLicense() {
    const refreshBtn = document.getElementById('refreshLicense');
    if (refreshBtn) {
      refreshBtn.disabled = true;
      refreshBtn.textContent = '⏳ Проверка...';
    }

    try {
      const response = await this.sendMessage({ action: 'refreshLicense' });
      if (response?.license) {
        this.updateLicenseUI(response.license);
        this.showToast('Статус лицензии обновлён', 'success');
      }
    } catch (error) {
      console.error('Refresh license error:', error);
      this.showToast('❌ Ошибка обновления', 'error');
    } finally {
      if (refreshBtn) {
        refreshBtn.disabled = false;
        refreshBtn.textContent = '🔄 Обновить';
      }
    }
  }

  async clearStats() {
    if (!confirm('Очистить всю статистику? Это действие нельзя отменить.')) return;
    
    await this.sendMessage({
      action: 'updateSettings',
      settings: {
        resetStats: true
      }
    });
    
    await this.loadData();
    this.updateStats();
    this.showToast('Статистика очищена');
  }

  async sendMessage(message) {
    return new Promise((resolve) => {
      api.runtime.sendMessage(message, (response) => {
        if (api.runtime.lastError) {
          
          resolve(null);
        } else {
          resolve(response);
        }
      });
    });
  }

  formatNumber(num) {
    if (num === null || num === undefined || Number.isNaN(num)) return '0';
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'k';
    return num.toString();
  }

  showToast(message) {
    if (!this._toastContainer) {
      this._toastContainer = document.createElement('div');
      this._toastContainer.className = 'toast-container';
      this._toastContainer.style.cssText = 'position: fixed; top: 20px; right: 20px; z-index: 10000; pointer-events: none;';
      document.body.appendChild(this._toastContainer);
    }
    
    this._toastContainer.innerHTML = '';
    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.textContent = message;
    toast.style.cssText = `
      background: var(--surface);
      color: var(--text-primary);
      padding: 12px 20px;
      border-radius: var(--radius-md);
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
      border: 1px solid var(--border);
      font-weight: 500;
      transform: translateX(100px);
      opacity: 0;
      transition: all 0.3s ease;
      margin-bottom: 8px;
      pointer-events: auto;
    `;
    this._toastContainer.appendChild(toast);
    setTimeout(() => {
      toast.style.transform = 'translateX(0)';
      toast.style.opacity = '1';
    }, 10);
    setTimeout(() => {
      toast.style.transform = 'translateX(100px)';
      toast.style.opacity = '0';
      setTimeout(() => {
        if (toast.parentNode) toast.parentNode.removeChild(toast);
      }, 300);
    }, 3000);
  }

  renderTimezoneOptions() {
    const select = document.getElementById('statsTimezoneOffset');
    if (!select) return;
    const offsets = [-720, -600, -480, -420, -360, -300, -240, -180, -120, -60, 0, 60, 120, 180, 240, 300, 330, 345, 360, 390, 420, 480, 540, 570, 600, 660, 720];
    const current = String(this.state.settings.statsTimezoneOffset ?? -new Date().getTimezoneOffset());
    select.innerHTML = offsets.map(mins => {
      const sign = mins >= 0 ? '+' : '-';
      const abs = Math.abs(mins);
      const hh = String(Math.floor(abs / 60)).padStart(2, '0');
      const mm = String(abs % 60).padStart(2, '0');
      const label = `UTC${sign}${hh}:${mm}`;
      return `<option value="${mins}" ${String(mins) === current ? 'selected' : ''}>${label}</option>`;
    }).join('');
  }

  async updateSettingPatch(patch) {
    await this.sendMessage({ action: 'updateSettings', settings: patch });
    
    await Promise.all([
      this.loadData(),
      (async () => { this.updateStats(); })()
    ]);
  }
}

const optionsController = new OptionsController();