const api = globalThis.browser ?? chrome;

class PopupController {

  updateUI() {
    this.updateStatus();
    this.updateStats();
    this.updateProgress();
    this.updateActivity();
    this.updateLicenseInfo();
    this.updateTargetInfo();
  }

  openSettings() {
    // Открытие страницы настроек (options.html)
    if (chrome.runtime.openOptionsPage) {
      chrome.runtime.openOptionsPage();
    } else {
      window.open('options.html');
    }
  }

  refreshData() {
    // Принудительно обновить состояние
    this.loadState();
    this.updateUI();
    this.checkTargetTab();
  }

    async ensureNotificationPermission() {
      if (!('Notification' in window)) return;
      if (Notification.permission === 'default') {
        try {
          await Notification.requestPermission();
        } catch (e) {
          // Игнорируем ошибку
        }
      }
    }
  constructor() {
    this.elements = {
      toggleSwitch: document.getElementById('toggleSwitch'),
      statusText: document.getElementById('statusText'),
      statusIndicator: document.getElementById('statusIndicator'),
      totalClicks: document.getElementById('totalClicks'),
      todayClicks: document.getElementById('todayClicks'),
      accuracy: document.getElementById('accuracy'),
      sessionClicks: document.getElementById('sessionClicks'),
      progressPercent: document.getElementById('progressPercent'),
      progressFill: document.getElementById('progressFill'),
      activityList: document.getElementById('activityList'),
      targetDot: document.getElementById('targetDot'),
      targetText: document.getElementById('targetText'),
       // pageUrl: document.getElementById('pageUrl'),
      refreshBtn: document.getElementById('refreshBtn'),
      settingsBtn: document.getElementById('settingsBtn')
    };

    this.state = {
      enabled: false,
      stats: {},
      activity: [],
      targetActive: false,
      lastUpdate: null
    };

    this.init();
  }

  async init() {
    await this.loadState();
    await this.ensureNotificationPermission();
    this.setupEventListeners();
    this.updateUI();
    this.startAutoRefresh();
    this.checkTargetTab();
  }

  async loadState() {
    try {
      const response = await this.sendMessage({ action: 'getState' });
      
      if (response) {
        this.state.enabled = response.enabled || false;
        this.state.stats = response.stats || {};
        this.state.license = response.license || null;
        
        this.elements.toggleSwitch.checked = this.state.enabled;
        this.updateLicenseInfo();
      }
    } catch (error) {
      console.log('Не удалось загрузить состояние:', error);
    }
  }

  setupEventListeners() {
    this.elements.toggleSwitch.addEventListener('change', () => this.toggleClicker());
    this.elements.refreshBtn.addEventListener('click', () => this.refreshData());
    this.elements.settingsBtn.addEventListener('click', () => this.openSettings());
  }

  async toggleClicker() {
    const wantEnabled = this.elements.toggleSwitch.checked;
    try {
      const response = await this.sendMessage({
        action: 'toggle',
        enabled: wantEnabled
      });
      // Проверяем ошибку лицензии
      if (response.error === 'LICENSE_REQUIRED') {
        this.elements.toggleSwitch.checked = false;
        this.state.enabled = false;
        // Показываем уведомление о лицензии
        const license = response.license;
        let message = '🔐 Требуется лицензия';
        if (license?.error === 'NOT_CONNECTED') {
          message = '🔗 Подключите Telegram в настройках для активации';
        } else if (license?.error === 'EXPIRED') {
          message = '⏰ Лицензия истекла. Продлите подписку.';
        } else if (license?.error === 'NO_LICENSE') {
          message = '🎁 Подключите Telegram для получения пробного периода';
        }
        this.addActivity(message, 'error');
        this.updateUI();
      } else {
        this.state.enabled = wantEnabled;
        this.addActivity(
          this.state.enabled ? 'Автокликер включен' : 'Автокликер выключен',
          'system'
        );
        this.updateUI();
      }
    } catch (error) {
      this.elements.toggleSwitch.checked = !wantEnabled;
    }
  }

  updateStatus() {
    if (this.state.enabled) {
      this.elements.statusText.textContent = 'Включен';
      this.elements.statusIndicator.classList.add('active');
    } else {
      this.elements.statusText.textContent = 'Выключен';
      this.elements.statusIndicator.classList.remove('active');
    }
  }

  updateStats() {
    const stats = this.state.stats || {};
    const efficiency = stats.efficiency || {};
    
    this.elements.totalClicks.textContent = this.formatNumber(stats.totalClicks || 0);
    this.elements.todayClicks.textContent = this.formatNumber(stats.todayClicks || 0);
    this.elements.sessionClicks.textContent = this.formatNumber(stats.sessionClicks || 0);
    this.elements.accuracy.textContent = `${(efficiency.accuracy || 100).toFixed(1)}%`;
  }

  updateProgress() {
    const todayClicks = this.state.stats.todayClicks || 0;
    const goal = Math.max(todayClicks, 50);
    const percent = Math.min((todayClicks / goal) * 100, 100);
    
    this.elements.progressPercent.textContent = `${Math.round(percent)}%`;
    this.elements.progressFill.style.width = `${percent}%`;
  }

  updateActivity() {
    const container = this.elements.activityList;
    
    if (this.state.activity.length === 0) {
      container.innerHTML = `
        <div class="activity-item">
          <div class="activity-icon">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 0C3.58 0 0 3.58 0 8s3.58 8 8 8 8-3.58 8-8-3.58-8-8-8zm0 14.5c-3.59 0-6.5-2.91-6.5-6.5S4.41 1.5 8 1.5s6.5 2.91 6.5 6.5-2.91 6.5-6.5 6.5zm.75-10.25h-1.5v4.5l3.75 2.25.75-1.23-3-1.77V4.25z"/>
            </svg>
          </div>
          <div class="activity-content">
            <div class="activity-text">Ожидание активности...</div>
            <div class="activity-time">--:--</div>
          </div>
        </div>
      `;
      return;
    }
    
    const recentActivity = this.state.activity.slice(0, 3);
    
    container.innerHTML = recentActivity.map(activity => `
      <div class="activity-item">
        <div class="activity-icon">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
            <path d="M8 16A8 8 0 1 1 8 0a8 8 0 0 1 0 16zm.93-9.412-2.29.287-.082.38.45.083c.294.07.352.176.288.469l-.738 3.468c-.194.897.105 1.319.808 1.319.545 0 1.178-.252 1.465-.598l.088-.416c-.2.176-.492.246-.686.246-.275 0-.375-.193-.304-.533L8.93 6.588zM8 5.5a1 1 0 1 0 0-2 1 1 0 0 0 0 2z"/>
          </svg>
        </div>
        <div class="activity-content">
          <div class="activity-text">${activity.text}</div>
          <div class="activity-time">${activity.time}</div>
        </div>
      </div>
    `).join('');
  }

  updateTargetInfo() {
    if (this.state.targetActive) {
      this.elements.targetDot.classList.add('active');
      this.elements.targetText.textContent = 'Страница активна';
        if (this.elements.pageUrl) {
          this.elements.pageUrl.style.display = 'none';
        }
    } else {
      this.elements.targetDot.classList.remove('active');
      this.elements.targetText.textContent = 'Страница не обнаружена';
        if (this.elements.pageUrl) {
          this.elements.pageUrl.style.display = 'none';
        }
    }
  }

  async checkTargetTab() {
    try {
      const urls = [
        'https://exotic.company/operator/payout_transaction*',
        'https://gate-1.exotic.company/operator/payout_transaction*',
        'https://gate-2.exotic.company/operator/payout_transaction*'
      ];
      let found = false;
      for (const url of urls) {
        const tabs = await api.tabs.query({ url });
        if (tabs.length > 0) {
          found = true;
          break;
        }
      }
      this.state.targetActive = found;
      this.updateTargetInfo();
    } catch (error) {
      this.state.targetActive = false;
    }
  }

  addActivity(text, type = 'info') {
    const now = new Date();
    const time = now.toLocaleTimeString('ru-RU', {
      hour: '2-digit',
      minute: '2-digit'
    });
    
    this.state.activity.unshift({
      text: text,
      time: time,
      type: type,
      timestamp: now.getTime()
    });
    
    if (this.state.activity.length > 10) {
      this.state.activity = this.state.activity.slice(0, 10);
    }
    
    this.updateActivity();
  }

  getActivityIcon(type) {
    const icons = {
      'click': '🖱️',
      'system': '⚙️',
      'error': '⚠️',
      'success': '✅',
      'info': 'ℹ️'
    };
    
    return icons[type] || '📝';
  }

  startAutoRefresh() {
    this._autoRefreshInterval = setInterval(async () => {
      await this.loadState();
      this.updateUI();
      await this.checkTargetTab();
    }, 3000);
    window.addEventListener('unload', () => clearInterval(this._autoRefreshInterval), { once: true });
  }

  async sendMessage(message) {
    return new Promise((resolve) => {
      api.runtime.sendMessage(message, (response) => {
        if (api.runtime.lastError) {
          console.log('Ошибка отправки сообщения:', api.runtime.lastError);
          resolve(null);
        } else {
          resolve(response);
        }
      });
    });
  }

  formatNumber(num) {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'k';
    return num.toString();
  }

  updateLicenseInfo() {
    const infoLine = document.getElementById('licenseInfoLine');
    const iconEl = document.getElementById('licenseIconSmall');
    const textEl = document.getElementById('licenseTextSmall');
    const daysEl = document.getElementById('licenseDaysSmall');

    if (!infoLine) return;

    const license = this.state.license;

    // Если нет данных о лицензии или не подключен
    if (!license || license.error === 'NOT_CONNECTED') {
      infoLine.style.display = 'flex';
      infoLine.className = 'license-info-line expired';
      iconEl.textContent = '🔗';
      textEl.textContent = 'Подключите Telegram';
      daysEl.textContent = '→ Настройки';
      daysEl.className = 'license-days-small';
      return;
    }

    infoLine.style.display = 'flex';

    const isValid = license.valid === true;
    const isTrial = license.type === 'trial';
    const daysLeft = license.daysLeft || 0;

    if (isValid) {
      if (isTrial) {
        iconEl.textContent = '🎁';
        textEl.textContent = 'Пробный период';
        infoLine.className = 'license-info-line warning';
      } else {
        iconEl.textContent = '⭐';
        textEl.textContent = 'Подписка активна';
        infoLine.className = 'license-info-line active';
      }

      if (daysLeft <= 1) {
        daysEl.textContent = '< 1 дн.';
        daysEl.className = 'license-days-small danger';
      } else if (daysLeft <= 3) {
        daysEl.textContent = `${daysLeft} дн.`;
        daysEl.className = 'license-days-small warning';
      } else {
        daysEl.textContent = `${daysLeft} дн.`;
        daysEl.className = 'license-days-small success';
      }
    } else {
      iconEl.textContent = '❌';
      textEl.textContent = 'Лицензия истекла';
      daysEl.textContent = 'Продлить';
      daysEl.className = 'license-days-small danger';
      infoLine.className = 'license-info-line expired';
    }
  }
}

new PopupController();