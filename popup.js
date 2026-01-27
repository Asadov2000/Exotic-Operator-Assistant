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

// ========== КОНСТАНТЫ ==========
const AUTO_REFRESH_INTERVAL = 5000; // 5 секунд (оптимизация)
const MAX_ACTIVITY_ITEMS = 10;
const ANIMATION_DURATION = 300; // мс

class PopupController {
  constructor() {
    this.elements = this.cacheElements();
    this.state = {
      enabled: false,
      stats: {},
      license: null,
      activity: []
    };
    this.autoRefreshId = null;
    this.init();
  }

  cacheElements() {
    return {
      toggleSwitch: document.getElementById('toggleSwitch'),
      toggleCard: document.getElementById('toggleCard'),
      statusDot: document.getElementById('statusDot'),
      statusText: document.getElementById('statusText'),
      toggleHint: document.getElementById('toggleHint'),
      todayClicks: document.getElementById('todayClicks'),
      sessionClicks: document.getElementById('sessionClicks'),
      totalClicks: document.getElementById('totalClicks'),
      avgClicks: document.getElementById('avgClicks'),
      bestDay: document.getElementById('bestDay'),
      activeDays: document.getElementById('activeDays'),
      licenseBadge: document.getElementById('licenseBadge'),
      licenseCard: document.getElementById('licenseCard'),
      licenseIcon: document.getElementById('licenseIcon'),
      licenseType: document.getElementById('licenseType'),
      licenseExpires: document.getElementById('licenseExpires'),
      activityList: document.getElementById('activityList'),
      connectionStatus: document.getElementById('connectionStatus'),
      refreshBtn: document.getElementById('refreshBtn'),
      clearActivityBtn: document.getElementById('clearActivityBtn'),
      settingsBtn: document.getElementById('settingsBtn'),
      chartBars: document.getElementById('chartBars'),
      efficiency: document.getElementById('efficiency')
    };
  }

  async init() {
    await this.loadState();
    this.setupEventListeners();
    this.updateUI();
    this.startAutoRefresh();
    this.checkTargetTab();
    await this.checkFirstRun();
  }

  async checkFirstRun() {
    try {
      const result = await api.storage.local.get('firstRunShown');
      if (!result.firstRunShown && !this.state.license?.valid) {
        await api.storage.local.set({ firstRunShown: true });
        this.addActivity('👋 Добро пожаловать! Подключите Telegram', 'info');
        setTimeout(() => {
          if (confirm('Подключите Telegram для получения бесплатного пробного периода.\n\nОткрыть настройки?')) {
            this.openSettings();
          }
        }, 1000);
      }
    } catch (e) {
      console.log('First run check error:', e);
    }
  }

  async loadState() {
    try {
      const response = await this.sendMessage({ action: 'getState' });
      if (response) {
        this.state.enabled = response.enabled || false;
        this.state.stats = response.stats || {};
        this.state.license = response.license || null;
        this.elements.toggleSwitch.checked = this.state.enabled;
      }
    } catch (error) {
      console.log('Load state error:', error);
    }
  }

  setupEventListeners() {
    this.elements.toggleSwitch.addEventListener('change', () => this.toggleClicker());
    this.elements.refreshBtn.addEventListener('click', () => this.refresh());
    this.elements.clearActivityBtn?.addEventListener('click', () => this.clearActivity());
    this.elements.settingsBtn.addEventListener('click', () => this.openSettings());
    this.elements.licenseCard?.addEventListener('click', () => this.openSettings());
  }

  async clearActivity() {
    this.state.activity = [];
    this.renderActivity();
    await this.sendMessage({ action: 'clearNotifications' });
    this.addActivity('🧹 История очищена', 'info');
  }

  async toggleClicker() {
    const wantEnabled = this.elements.toggleSwitch.checked;
    
    try {
      const response = await this.sendMessage({
        action: 'toggle',
        enabled: wantEnabled
      });

      if (!response) {
        throw new Error('Нет ответа от сервиса');
      }

      if (response.error === 'LICENSE_REQUIRED') {
        this.elements.toggleSwitch.checked = false;
        this.state.enabled = false;
        
        let message = '🔐 Требуется лицензия';
        if (response.license?.error === 'NOT_CONNECTED') {
          message = '🔗 Подключите Telegram';
        } else if (response.license?.error === 'EXPIRED') {
          message = '⏰ Лицензия истекла';
        }
        
        this.addActivity(message, 'error');
      } else {
        this.state.enabled = wantEnabled;
        this.addActivity(
          this.state.enabled ? '✅ Автокликер включен' : '⏸️ Автокликер выключен',
          this.state.enabled ? 'success' : 'info'
        );
      }
      
      this.updateUI();
    } catch (error) {
      this.elements.toggleSwitch.checked = !wantEnabled;
      this.addActivity('❌ Ошибка переключения', 'error');
    }
  }

  updateUI() {
    this.updateToggleState();
    this.updateStats();
    this.updateLicense();
  }

  updateToggleState() {
    const { enabled } = this.state;
    
    this.elements.toggleCard.classList.toggle('active', enabled);
    this.elements.statusDot.classList.toggle('active', enabled);
    this.elements.statusText.textContent = enabled ? 'Активен' : 'Выключен';
    this.elements.toggleHint.textContent = enabled ? 'Мониторинг запущен' : 'Нажмите для запуска';
  }

  updateStats() {
    const { stats } = this.state;
    
    this.elements.todayClicks.textContent = this.formatNumber(stats.todayClicks || 0);
    this.elements.sessionClicks.textContent = this.formatNumber(stats.sessionClicks || 0);
    this.elements.totalClicks.textContent = this.formatNumber(stats.totalClicks || 0);
    
    // Расширенная статистика
    this.updateExtendedStats();
    
    // Обновляем эффективность
    if (stats.efficiency && this.elements.efficiency) {
      const accuracy = Math.round(stats.efficiency.accuracy || 100);
      this.elements.efficiency.textContent = `${accuracy}%`;
      this.elements.efficiency.className = 'chart-efficiency';
      if (accuracy < 70) {
        this.elements.efficiency.classList.add('danger');
      } else if (accuracy < 90) {
        this.elements.efficiency.classList.add('warning');
      }
    }
    
    // Рисуем график
    this.renderChart();
  }

  updateExtendedStats() {
    const { stats } = this.state;
    const history = stats.history?.days || {};
    const days = Object.keys(history);
    
    // Количество активных дней
    const activeDays = days.length;
    if (this.elements.activeDays) {
      this.elements.activeDays.textContent = activeDays;
    }
    
    // Среднее кликов в день
    if (this.elements.avgClicks) {
      if (activeDays > 0) {
        const totalFromHistory = days.reduce((sum, day) => sum + (history[day].total || 0), 0);
        const avg = Math.round(totalFromHistory / activeDays);
        this.elements.avgClicks.textContent = this.formatNumber(avg);
      } else {
        this.elements.avgClicks.textContent = '0';
      }
    }
    
    // Лучший день (рекорд)
    if (this.elements.bestDay) {
      if (activeDays > 0) {
        const best = Math.max(...days.map(day => history[day].total || 0));
        this.elements.bestDay.textContent = this.formatNumber(best);
        if (stats.todayClicks >= best && best > 0) {
          this.elements.bestDay.classList.add('success');
        } else {
          this.elements.bestDay.classList.remove('success');
        }
      } else {
        this.elements.bestDay.textContent = '0';
      }
    }
  }

  renderChart() {
    const container = this.elements.chartBars;
    if (!container) return;
    
    const { stats } = this.state;
    const history = stats.history?.days || {};
    
    // Получаем данные за последние 24 часа
    const now = new Date();
    const currentHour = now.getUTCHours();
    const todayKey = this.getUTCDateKey(now);
    const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const yesterdayKey = this.getUTCDateKey(yesterday);
    
    const todayData = history[todayKey]?.perHour || [];
    const yesterdayData = history[yesterdayKey]?.perHour || [];
    
    // Собираем данные за 24 часа (от текущего часа назад)
    const hours = [];
    for (let i = 23; i >= 0; i--) {
      const hourIndex = (currentHour - i + 24) % 24;
      const isYesterday = (currentHour - i) < 0;
      const data = isYesterday ? yesterdayData : todayData;
      hours.push({
        value: data[hourIndex] || 0,
        isCurrent: i === 0,
        hour: hourIndex
      });
    }
    
    // Находим максимум для масштабирования
    const maxValue = Math.max(1, ...hours.map(h => h.value));
    
    // Рисуем бары
    container.innerHTML = hours.map((h, idx) => {
      const height = Math.max(2, (h.value / maxValue) * 100);
      const classes = ['chart-bar'];
      if (h.isCurrent) classes.push('current');
      if (h.value === 0) classes.push('empty');
      const safeValue = parseInt(h.value, 10) || 0;
      const hourStr = h.hour.toString().padStart(2, '0') + ':00';
      return `<div class="${classes.join(' ')}" style="height: ${height}%" title="${hourStr} — ${safeValue} кликов"></div>`;
    }).join('');
    
    // Добавляем метки времени
    this.renderChartLabels(hours);
  }

  renderChartLabels(hours) {
    // Находим контейнер меток или создаём его
    let labelsContainer = document.getElementById('chart-labels');
    if (!labelsContainer) {
      labelsContainer = document.createElement('div');
      labelsContainer.id = 'chart-labels';
      labelsContainer.className = 'chart-labels';
      // Вставляем после контейнера с барами
      if (this.elements.chartBars && this.elements.chartBars.parentNode) {
        this.elements.chartBars.parentNode.appendChild(labelsContainer);
      }
    }
    
    // Показываем метки каждые 6 часов (4 метки)
    const labels = [];
    for (let i = 0; i < 24; i += 6) {
      if (hours[i]) {
        const hourStr = hours[i].hour.toString().padStart(2, '0');
        labels.push(`<span class="chart-label" style="left: ${(i / 24) * 100}%">${hourStr}:00</span>`);
      }
    }
    // Добавляем текущий час справа
    if (hours.length > 0) {
      const currentHourStr = hours[hours.length - 1].hour.toString().padStart(2, '0');
      labels.push(`<span class="chart-label current-label" style="left: calc(100% - 2px)">${currentHourStr}:00</span>`);
    }
    
    labelsContainer.innerHTML = labels.join('');
  }

  getUTCDateKey(date) {
    return date.toISOString().slice(0, 10);
  }

  updateLicense() {
    const { license } = this.state;
    const badge = this.elements.licenseBadge;
    const card = this.elements.licenseCard;
    const icon = this.elements.licenseIcon;
    const type = this.elements.licenseType;
    const expires = this.elements.licenseExpires;

    badge.className = 'header-badge';
    card.className = 'license-card';

    if (!license || !license.valid) {
      badge.textContent = '—';
      icon.textContent = '🔒';
      type.textContent = 'Нет лицензии';
      expires.textContent = 'Подключите Telegram для активации';
      card.classList.add('error');
      return;
    }

    const daysLeft = license.daysLeft || 0;
    const hoursLeft = Math.max(0, Math.ceil((license.expiresAt - Date.now()) / (60 * 60 * 1000)));

    if (license.type === 'trial') {
      badge.textContent = 'Trial';
      badge.classList.add('trial');
      icon.textContent = '🎁';
      type.textContent = 'Пробный период';
      expires.textContent = `Осталось ${hoursLeft} ч.`;
      
      if (hoursLeft < 6) card.classList.add('warning');
    } else if (license.type === 'level1') {
      badge.textContent = 'L1';
      badge.classList.add('pro');
      icon.textContent = '⭐';
      type.textContent = 'Уровень 1';
      expires.textContent = daysLeft > 0 ? `Осталось ${daysLeft} дн.` : 'Активна';
      
      if (daysLeft < 3 && daysLeft > 0) card.classList.add('warning');
    } else if (license.type === 'level2') {
      badge.textContent = 'L2';
      badge.classList.add('pro');
      icon.textContent = '⭐⭐';
      type.textContent = 'Уровень 2';
      expires.textContent = daysLeft > 0 ? `Осталось ${daysLeft} дн.` : 'Активна';
      
      if (daysLeft < 3 && daysLeft > 0) card.classList.add('warning');
    } else if (license.type === 'level3') {
      badge.textContent = 'L3';
      badge.classList.add('premium');
      icon.textContent = '⭐⭐⭐';
      type.textContent = 'Уровень 3 (Максимум)';
      expires.textContent = daysLeft > 0 ? `Осталось ${daysLeft} дн.` : 'Активна';
      
      if (daysLeft < 3 && daysLeft > 0) card.classList.add('warning');
    } else {
      // Для старых или неизвестных типов
      badge.textContent = 'Pro';
      badge.classList.add('pro');
      icon.textContent = '⭐';
      type.textContent = license.typeName || 'Подписка';
      expires.textContent = daysLeft > 0 ? `Осталось ${daysLeft} дн.` : 'Активна';
      
      if (daysLeft < 3 && daysLeft > 0) card.classList.add('warning');
    }
  }

  addActivity(text, type = 'info') {
    const time = new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    this.state.activity.unshift({ text, type, time });
    if (this.state.activity.length > MAX_ACTIVITY_ITEMS) this.state.activity.pop();
    this.renderActivity();
  }

  // Экранирование HTML для защиты от XSS
  escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  renderActivity() {
    const { activity } = this.state;
    const container = this.elements.activityList;

    if (activity.length === 0) {
      container.innerHTML = '<div class="activity-empty"><span>Ожидание...</span></div>';
      return;
    }

    container.innerHTML = activity.map(item => `
      <div class="activity-item fade-in">
        <div class="activity-icon ${this.escapeHtml(item.type)}">${this.getActivityIcon(item.type)}</div>
        <div class="activity-content">
          <div class="activity-text">${this.escapeHtml(item.text)}</div>
          <div class="activity-time">${this.escapeHtml(item.time)}</div>
        </div>
      </div>
    `).join('');
  }

  getActivityIcon(type) {
    switch (type) {
      case 'success': return '✓';
      case 'error': return '✕';
      case 'info': return 'ℹ';
      default: return '•';
    }
  }

  async checkTargetTab() {
    const status = this.elements.connectionStatus;
    const text = status.querySelector('.connection-text');

    try {
      const tabs = await api.tabs.query({ url: '*://*.exotic.company/*' });
      
      if (tabs.length > 0) {
        status.classList.add('connected');
        status.classList.remove('disconnected');
        text.textContent = 'Страница найдена';
      } else {
        status.classList.remove('connected');
        status.classList.add('disconnected');
        text.textContent = 'Откройте exotic.company';
      }
    } catch (e) {
      status.classList.remove('connected', 'disconnected');
      text.textContent = 'Проверка...';
    }
  }

  refresh() {
    this.loadState().then(() => {
      this.updateUI();
      this.checkTargetTab();
    });
    
    this.elements.refreshBtn.style.transform = 'rotate(360deg)';
    setTimeout(() => this.elements.refreshBtn.style.transform = '', ANIMATION_DURATION);
  }

  startAutoRefresh() {
    // Очищаем предыдущий интервал если есть
    if (this.autoRefreshId) {
      clearInterval(this.autoRefreshId);
    }
    this.autoRefreshId = setInterval(() => {
      this.loadState().then(() => this.updateUI());
      this.checkTargetTab();
    }, AUTO_REFRESH_INTERVAL);
    
    // Очищаем при закрытии popup
    window.addEventListener('unload', () => {
      if (this.autoRefreshId) {
        clearInterval(this.autoRefreshId);
      }
    }, { once: true });
  }

  openSettings() {
    if (api.runtime.openOptionsPage) {
      api.runtime.openOptionsPage();
    } else {
      window.open('options.html');
    }
  }

  formatNumber(num) {
    if (num >= 1000) return (num / 1000).toFixed(1) + 'k';
    return String(num);
  }

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

document.addEventListener('DOMContentLoaded', () => new PopupController());
