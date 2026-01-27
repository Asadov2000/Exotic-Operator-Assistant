// Безопасное определение API для всех браузеров
// PC/Mac: Chrome, Firefox, Edge, Opera, Brave, Arc
// Android: Kiwi, Mises, Samsung Internet, Firefox
// iPhone/iPad: Orion Browser (единственный с поддержкой Chrome-расширений)
var api = null;
try {
  // Firefox/Orion используют 'browser', Chrome-based - 'chrome'
  if (typeof browser !== 'undefined' && browser && browser.runtime) {
    api = browser;
  } else if (typeof chrome !== 'undefined' && chrome && chrome.runtime) {
    api = chrome;
  }
} catch (e) {
  // Fallback для старых браузеров
  if (typeof chrome !== 'undefined') api = chrome;
}

// === КОНФИГУРАЦИЯ ===
const DEBUG = false;
const VERSION = '5.0';

// Временные константы (оптимизированы для снижения нагрузки)
const CHECK_INTERVAL = 750; // мс (было 500) - баланс между скоростью и нагрузкой
const OBSERVER_DEBOUNCE = 150; // мс (было 100)
const MIN_CLICK_DELAY = 100; // мс (было 50)
const MAX_CLICK_DELAY = 200; // мс (было 150)
const MAX_RETRIES = 5; // Увеличено для 100% надёжности
const RETRY_DELAY = 150; // мс - быстрее между попытками
const HEALTH_CHECK_INTERVAL = 60 * 1000; // 60 сек (было 30) - реже проверка
const MAX_INACTIVITY = 5 * 60 * 1000; // 5 минут
const MAX_CONSECUTIVE_ERRORS = 10;
const MIN_PAGE_AGE = 30 * 1000; // 30 сек
const TOAST_DURATION = 2500; // мс
const MAX_CLICKED_IDS = 500; // (было 1000) - меньше хранимых ID
const BATCH_PROCESS_LIMIT = 10; // Обрабатывать максимум 10 кнопок за раз
const IDLE_SLOWDOWN_MULTIPLIER = 1.5; // Меньшее замедление при отсутствии активности
const CLICK_VERIFICATION_DELAY = 100; // Задержка перед проверкой результата клика

// Тексты кнопок по умолчанию
const DEFAULT_TARGET_TEXTS = ['В работу', 'Взять в работу', 'Принять в работу', 'Take to work'];

const log = (...args) => DEBUG && console.log('[Exotic]', ...args);
const logAlways = (...args) => console.log('[Exotic]', ...args);

class ExoticAutoclicker {
  constructor() {
    logAlways('🚀 ========================================');
    logAlways(`🚀 Exotic Autoclicker v${VERSION} загружается...`);
    logAlways('🚀 URL:', window.location.href);
    logAlways('🚀 DEBUG:', DEBUG ? 'ВКЛ' : 'ВЫКЛ');
    logAlways('🚀 ========================================');
    
    this.state = {
      enabled: false,
      running: false,
      // WeakSet может не поддерживаться на старых мобильных браузерах
      clickedElements: typeof WeakSet !== 'undefined' ? new WeakSet() : new Set(),
      clickedIds: new Set(),
      observer: null,
      monitorId: null,
      healthMonitorId: null,
      destroyed: false,
      notificationPermissionRequested: false,
      stats: {
        found: 0,
        clicked: 0,
        failed: 0,
        retries: 0
      },
      lastActivity: Date.now(),
      lastCheck: Date.now(),
      consecutiveErrors: 0,
      pageLoadTime: Date.now()
    };

    this.config = {
      targetTexts: [...DEFAULT_TARGET_TEXTS],
      checkInterval: CHECK_INTERVAL,
      minDelay: MIN_CLICK_DELAY,
      maxDelay: MAX_CLICK_DELAY,
      maxRetries: MAX_RETRIES,
      retryDelay: RETRY_DELAY,
      autoRefresh: {
        enabled: true,
        maxInactivity: MAX_INACTIVITY,
        maxErrors: MAX_CONSECUTIVE_ERRORS,
        minPageAge: MIN_PAGE_AGE,
        checkInterval: HEALTH_CHECK_INTERVAL
      }
    };

    this.setupListeners();
    this.setupObserver();
    this.startMonitor();
    this.loadConfig();
    this.startHealthMonitor();
    this.startRateCalculator(); // Калькулятор курса
    
    // Обработка выгрузки страницы
    window.addEventListener('beforeunload', () => this.destroy());
    
    logAlways('✅ Autoclicker инициализирован!');
    logAlways('⏳ Ожидание включения...');
  }
  
  // ========== ОЧИСТКА РЕСУРСОВ ==========
  
  destroy() {
    this.state.destroyed = true;
    this.state.enabled = false;
    
    // Очищаем все интервалы
    if (this.state.monitorId) {
      clearInterval(this.state.monitorId);
      this.state.monitorId = null;
    }
    if (this.state.healthMonitorId) {
      clearInterval(this.state.healthMonitorId);
      this.state.healthMonitorId = null;
    }
    if (this.rateCalculatorId) {
      clearInterval(this.rateCalculatorId);
      this.rateCalculatorId = null;
    }
    
    // Отключаем observer
    if (this.state.observer) {
      this.state.observer.disconnect();
      this.state.observer = null;
    }
    
    // Удаляем UI элементы
    const rateDisplay = document.getElementById('exotic-rate-display');
    if (rateDisplay) rateDisplay.remove();
    
    const toast = document.querySelector('.exotic-toast');
    if (toast) toast.remove();
    
    logAlways('🛑 Autoclicker остановлен и очищен');
  }

  // ========== КАЛЬКУЛЯТОР КУРСА USDT ==========
  
  startRateCalculator() {
    // Создаём плавающий элемент для отображения курса
    this.createRateDisplay();
    // Проверяем каждые 1000мс (оптимизировано для снижения нагрузки)
    this.rateCalculatorId = setInterval(() => this.calculateAndShowRate(), 1000);
    logAlways('💱 Калькулятор курса запущен');
  }
  
  createRateDisplay() {
    // Удаляем старый если есть
    const existing = document.getElementById('exotic-rate-display');
    if (existing) existing.remove();
    
    const display = document.createElement('div');
    display.id = 'exotic-rate-display';
    display.style.cssText = `
      position: fixed;
      top: 80px;
      right: 20px;
      z-index: 2147483647;
      padding: 14px 16px;
      background: linear-gradient(135deg, #c0392b 0%, #922b21 100%);
      color: white;
      border-radius: 14px;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      font-size: 14px;
      box-shadow: 0 4px 24px rgba(192, 57, 43, 0.5);
      display: none;
      min-width: 200px;
      max-width: 280px;
      border: 1px solid rgba(255,255,255,0.15);
      backdrop-filter: blur(10px);
      pointer-events: auto;
    `;
    display.innerHTML = `
      <div style="font-size: 11px; opacity: 0.9; margin-bottom: 6px; display: flex; align-items: center; gap: 6px;">
        <span>📊 Себестоимость</span>
        <span id="exotic-toggle-calc" style="cursor: pointer; font-size: 13px; opacity: 0.8; margin-left: auto;" title="Калькулятор прибыли">⚙️</span>
      </div>
      <div id="exotic-rate-value" style="font-size: 24px; font-weight: 700; letter-spacing: -0.5px;">—</div>
      <div id="exotic-rate-details" style="font-size: 10px; opacity: 0.75; margin-top: 4px;"></div>
      
      <!-- Калькулятор прибыли (скрыт по умолчанию) -->
      <div id="exotic-profit-calc" style="display: none; margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.2);">
        <div style="font-size: 11px; opacity: 0.9; margin-bottom: 8px;">💰 Калькулятор прибыли</div>
        
        <div style="display: flex; gap: 8px; margin-bottom: 8px;">
          <div style="flex: 1;">
            <label style="font-size: 9px; opacity: 0.7; display: block; margin-bottom: 3px;">Курс продажи ₽</label>
            <div id="exotic-sell-rate" style="
              width: 100%;
              padding: 8px 10px;
              border: none;
              border-radius: 8px;
              background: rgba(255,255,255,0.15);
              color: white;
              font-size: 14px;
              font-weight: 600;
              box-sizing: border-box;
              cursor: pointer;
              min-height: 20px;
              user-select: none;
            " title="Нажмите чтобы ввести">—</div>
          </div>
          <div style="flex: 1;">
            <label style="font-size: 9px; opacity: 0.7; display: block; margin-bottom: 3px;">Комиссия %</label>
            <div id="exotic-commission" style="
              width: 100%;
              padding: 8px 10px;
              border: none;
              border-radius: 8px;
              background: rgba(255,255,255,0.15);
              color: white;
              font-size: 14px;
              font-weight: 600;
              box-sizing: border-box;
              cursor: pointer;
              min-height: 20px;
              user-select: none;
            " title="Нажмите чтобы ввести">1</div>
          </div>
        </div>
        
        <div id="exotic-profit-result" style="
          background: rgba(0,0,0,0.2);
          border-radius: 10px;
          padding: 10px 12px;
          margin-top: 8px;
        ">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
            <span style="font-size: 10px; opacity: 0.8;">Выручка:</span>
            <span id="exotic-revenue" style="font-size: 13px; font-weight: 600;">— ₽</span>
          </div>
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
            <span style="font-size: 10px; opacity: 0.8;">Комиссия:</span>
            <span id="exotic-commission-amount" style="font-size: 13px; font-weight: 600; color: #ff9999;">— ₽</span>
          </div>
          <div style="height: 1px; background: rgba(255,255,255,0.2); margin: 8px 0;"></div>
          <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="font-size: 11px; font-weight: 600;">Чистая прибыль:</span>
            <span id="exotic-net-profit" style="font-size: 18px; font-weight: 700; color: #7dff7d;">— ₽</span>
          </div>
          <div id="exotic-profit-percent" style="font-size: 10px; opacity: 0.8; text-align: right; margin-top: 2px;"></div>
        </div>
      </div>
    `;
    document.body.appendChild(display);
    this.rateDisplay = display;
    
    // Сохраняем текущие значения для калькулятора
    this.currentSumUsdt = 0;
    this.currentSumRub = 0;
    
    // Обработчик кнопки toggle
    const toggleBtn = document.getElementById('exotic-toggle-calc');
    if (toggleBtn) {
      toggleBtn.addEventListener('click', () => {
        const calc = document.getElementById('exotic-profit-calc');
        const sellRateEl = document.getElementById('exotic-sell-rate');
        if (calc) {
          const isHidden = calc.style.display === 'none';
          calc.style.display = isHidden ? 'block' : 'none';
          toggleBtn.textContent = isHidden ? '✖️' : '⚙️';
          if (isHidden) {
            this.calculateProfit();
          } else {
            // При сворачивании сбрасываем курс продажи
            this.sellRateValue = 0;
            if (sellRateEl) sellRateEl.textContent = '—';
          }
        }
      });
    }
    
    // Обработчики ввода через prompt()
    const sellRateEl = document.getElementById('exotic-sell-rate');
    const commissionEl = document.getElementById('exotic-commission');
    
    // Сохраняем значения
    this.sellRateValue = 0;
    this.commissionValue = parseFloat(localStorage.getItem('exotic_commission')) || 1;
    
    // Отображаем сохранённую комиссию
    if (commissionEl) {
      commissionEl.textContent = this.commissionValue || '1';
    }
    
    // Клик на курс продажи
    if (sellRateEl) {
      sellRateEl.addEventListener('click', () => {
        const current = this.sellRateValue || '';
        const input = prompt('Введите курс продажи ₽:', current);
        if (input !== null) {
          const val = parseFloat(input.replace(',', '.')) || 0;
          this.sellRateValue = val;
          sellRateEl.textContent = val > 0 ? val : '—';
          this.calculateProfit();
        }
      });
    }
    
    // Клик на комиссию
    if (commissionEl) {
      commissionEl.addEventListener('click', () => {
        const current = this.commissionValue || '';
        const input = prompt('Введите комиссию %:', current);
        if (input !== null) {
          const val = parseFloat(input.replace(',', '.')) || 0;
          this.commissionValue = val;
          commissionEl.textContent = val > 0 ? val : '0';
          localStorage.setItem('exotic_commission', val);
          this.calculateProfit();
        }
      });
    }
  }
  
  calculateProfit() {
    const revenueEl = document.getElementById('exotic-revenue');
    const commissionAmountEl = document.getElementById('exotic-commission-amount');
    const netProfitEl = document.getElementById('exotic-net-profit');
    const profitPercentEl = document.getElementById('exotic-profit-percent');
    
    if (!this.currentSumUsdt) return;
    
    const sellRate = this.sellRateValue || 0;
    const commissionPercent = this.commissionValue || 0;
    
    if (sellRate <= 0) {
      if (revenueEl) revenueEl.textContent = '— ₽';
      if (commissionAmountEl) commissionAmountEl.textContent = '— ₽';
      if (netProfitEl) netProfitEl.textContent = '— ₽';
      if (profitPercentEl) profitPercentEl.textContent = '';
      return;
    }
    
    // Расчёты
    const revenue = this.currentSumUsdt * sellRate;
    const commissionAmount = revenue * (commissionPercent / 100);
    const netProfit = revenue - commissionAmount - this.currentSumRub;
    const profitPercent = this.currentSumRub > 0 ? ((netProfit / this.currentSumRub) * 100) : 0;
    
    // Отображаем
    if (revenueEl) revenueEl.textContent = `${revenue.toLocaleString('ru-RU', {maximumFractionDigits: 2})} ₽`;
    if (commissionAmountEl) commissionAmountEl.textContent = `-${commissionAmount.toLocaleString('ru-RU', {maximumFractionDigits: 2})} ₽`;
    
    if (netProfitEl) {
      const isPositive = netProfit >= 0;
      netProfitEl.textContent = `${isPositive ? '+' : ''}${netProfit.toLocaleString('ru-RU', {maximumFractionDigits: 2})} ₽`;
      netProfitEl.style.color = isPositive ? '#7dff7d' : '#ff7d7d';
    }
    
    if (profitPercentEl) {
      const isPositive = profitPercent >= 0;
      profitPercentEl.textContent = `${isPositive ? '+' : ''}${profitPercent.toFixed(2)}% от себестоимости`;
      profitPercentEl.style.color = isPositive ? 'rgba(125, 255, 125, 0.8)' : 'rgba(255, 125, 125, 0.8)';
    }
  }
  
  calculateAndShowRate() {
    try {
      // Получаем весь текст страницы
      const pageText = document.body.innerText || '';
      
      // Проверяем есть ли нужные данные на странице
      if (!pageText.includes('USDT') || !pageText.includes('₽')) {
        this.hideRateDisplay();
        return;
      }
      
      // Ищем значения
      let sumRub = null;
      let sumUsdt = null;
      
      // Метод 1: Ищем "Сумма:" + число + ₽ (НЕ "Сумма зачисляемая")
      // Регулярка для: "Сумма:\n3 300.00 ₽" или "Сумма: 3300 ₽"
      const rubMatches = pageText.match(/Сумма:[\s\n]*([\d\s,.]+)\s*₽/g);
      if (rubMatches) {
        for (const match of rubMatches) {
          // Пропускаем если это "Сумма зачисляемая"
          if (match.includes('зачисляемая')) continue;
          const num = this.parseNumber(match);
          if (num && num > 100) {
            sumRub = num;
            break;
          }
        }
      }
      
      // Метод 2: Ищем "Сумма зачисляемая команде:" + число + USDT
      const usdtMatch = pageText.match(/зачисляемая[^:]*:[\s\n]*([\d\s,.]+)\s*USDT/i);
      if (usdtMatch) {
        sumUsdt = this.parseNumber(usdtMatch[1]);
      }
      
      // Альтернатива: ищем любое число перед USDT (если выше не нашли)
      if (!sumUsdt) {
        const allUsdtMatches = pageText.match(/([\d,.]+)\s*USDT/gi);
        if (allUsdtMatches && allUsdtMatches.length > 0) {
          // Берём первое совпадение
          sumUsdt = this.parseNumber(allUsdtMatches[0]);
        }
      }
      
      // Если нашли оба - показываем
      if (sumRub && sumUsdt && sumUsdt > 0) {
        const rate = sumRub / sumUsdt;
        this.showRateDisplay(rate, sumRub, sumUsdt);
      } else {
        this.hideRateDisplay();
      }
    } catch (e) {
      logAlways('❌ Ошибка расчёта:', e.message);
    }
  }
  
  showRateDisplay(rate, sumRub, sumUsdt) {
    if (!this.rateDisplay) this.createRateDisplay();
    
    // Сохраняем значения для калькулятора
    this.currentSumRub = sumRub;
    this.currentSumUsdt = sumUsdt;
    
    const valueEl = document.getElementById('exotic-rate-value');
    const detailsEl = document.getElementById('exotic-rate-details');
    
    if (valueEl) {
      valueEl.textContent = `${rate.toFixed(2)} ₽`;
    }
    if (detailsEl) {
      detailsEl.textContent = `${sumRub.toLocaleString('ru-RU')} ₽ ÷ ${sumUsdt.toFixed(4)} USDT`;
    }
    
    this.rateDisplay.style.display = 'block';
    
    // Обновляем калькулятор если он открыт
    const calcVisible = document.getElementById('exotic-profit-calc');
    if (calcVisible && calcVisible.style.display !== 'none') {
      this.calculateProfit();
    }
  }
  
  hideRateDisplay() {
    if (this.rateDisplay) {
      this.rateDisplay.style.display = 'none';
      
      // Сбрасываем курс продажи и закрываем калькулятор
      this.sellRateValue = 0;
      const sellRateEl = document.getElementById('exotic-sell-rate');
      if (sellRateEl) sellRateEl.textContent = '—';
      
      const calc = document.getElementById('exotic-profit-calc');
      const toggleBtn = document.getElementById('exotic-toggle-calc');
      if (calc) calc.style.display = 'none';
      if (toggleBtn) toggleBtn.textContent = '⚙️';
    }
  }
  
  parseNumber(str) {
    if (!str) return null;
    // Убираем пробелы, заменяем запятую на точку, убираем валюту
    const cleaned = String(str).replace(/\s/g, '').replace(',', '.').replace(/[₽USDT]/gi, '');
    // Извлекаем число
    const match = cleaned.match(/([\d.]+)/);
    if (match) {
      const num = parseFloat(match[1]);
      return isNaN(num) || num < 0 ? null : num;
    }
    return null;
  }
  
  insertCalculatedRate(modal, rate, sumRub, sumUsdt) {
    // Теперь используем плавающий элемент вместо вставки в модал
    this.showRateDisplay(rate, sumRub, sumUsdt);
  }

  // ========== ОЧИСТКА ПАМЯТИ ==========
  
  cleanupClickedIds() {
    if (this.state.clickedIds.size > MAX_CLICKED_IDS) {
      const idsArray = Array.from(this.state.clickedIds);
      const toRemove = idsArray.slice(0, idsArray.length - MAX_CLICKED_IDS / 2);
      toRemove.forEach(id => this.state.clickedIds.delete(id));
      log(`🧹 Очищено ${toRemove.length} старых ID`);
    }
  }

  // ========== МОНИТОРИНГ ЗДОРОВЬЯ СТРАНИЦЫ ==========
  
  startHealthMonitor() {
    // Проверяем каждые 30 секунд
    this.state.healthMonitorId = setInterval(() => this.checkPageHealth(), this.config.autoRefresh.checkInterval);
    
    // Слушаем ошибки сети
    window.addEventListener('error', (e) => this.onPageError(e));
    window.addEventListener('unhandledrejection', (e) => this.onPageError(e));
    
    // Слушаем потерю связи
    window.addEventListener('offline', () => {
      logAlways('⚠️ Потеря интернет-соединения');
      this.state.consecutiveErrors++;
    });
    
    window.addEventListener('online', () => {
      logAlways('✅ Интернет восстановлен');
      this.state.consecutiveErrors = 0;
      this.state.lastActivity = Date.now();
    });
  }

  onPageError(error) {
    this.state.consecutiveErrors++;
    log('Ошибка страницы:', error.message || error.reason || 'unknown');
    
    if (this.state.consecutiveErrors >= this.config.autoRefresh.maxErrors) {
      this.considerRefresh('Много ошибок подряд');
    }
  }

  checkPageHealth() {
    if (!this.config.autoRefresh.enabled || !this.state.enabled) return;
    
    const now = Date.now();
    const pageAge = now - this.state.pageLoadTime;
    const timeSinceActivity = now - this.state.lastActivity;
    
    // Не обновляем если страница слишком новая
    if (pageAge < this.config.autoRefresh.minPageAge) return;
    
    // Проверяем зависание (нет активности)
    if (timeSinceActivity > this.config.autoRefresh.maxInactivity) {
      this.considerRefresh('Нет активности ' + Math.round(timeSinceActivity / 60000) + ' мин');
      return;
    }
    
    // Проверяем много ошибок
    if (this.state.consecutiveErrors >= this.config.autoRefresh.maxErrors) {
      this.considerRefresh('Много ошибок');
      return;
    }
    
    // Проверяем что DOM ещё работает
    try {
      const testEl = document.querySelector('body');
      if (!testEl || !document.body) {
        this.considerRefresh('DOM не доступен');
        return;
      }
    } catch (e) {
      this.considerRefresh('Ошибка DOM');
      return;
    }
  }

  async considerRefresh(reason) {
    logAlways(`🔄 Рассматриваем обновление: ${reason}`);
    
    // Спрашиваем background нужно ли обновить
    const response = await this.sendMessage({ 
      action: 'shouldRefresh',
      reason,
      stats: this.state.stats
    });
    
    if (response?.shouldRefresh) {
      this.performRefresh(reason);
    }
  }

  performRefresh(reason) {
    logAlways(`🔄 Обновляем страницу: ${reason}`);
    
    // Показываем уведомление
    this.showToast('🔄 Обновление страницы...');
    
    // Отправляем статистику перед обновлением
    this.sendMessage({ 
      action: 'pageRefreshing',
      reason,
      stats: this.state.stats
    });
    
    // Небольшая задержка для отправки сообщения
    setTimeout(() => {
      window.location.reload();
    }, 500);
  }

  // Обновляем время активности при любом действии
  markActivity() {
    this.state.lastActivity = Date.now();
    this.state.consecutiveErrors = 0;
  }

  showToast(text) {
    try {
      const existing = document.querySelector('.exotic-toast');
      if (existing) existing.remove();
      const toast = document.createElement('div');
      toast.className = 'exotic-toast';
      // Безопасно устанавливаем текст (защита от XSS)
      toast.textContent = String(text).slice(0, 200);
      Object.assign(toast.style, {
        position: 'fixed',
        bottom: '16px',
        right: '16px',
        maxWidth: '320px',
        padding: '12px 16px',
        background: 'rgba(0,0,0,0.82)',
        color: '#fff',
        borderRadius: '10px',
        boxShadow: '0 8px 24px rgba(0,0,0,0.25)',
        fontSize: '14px',
        lineHeight: '1.4',
        zIndex: '2147483647',
        opacity: '0',
        transform: 'translateY(12px)',
        transition: 'opacity 0.2s ease, transform 0.2s ease'
      });
      document.body.appendChild(toast);
      requestAnimationFrame(() => {
        toast.style.opacity = '1';
        toast.style.transform = 'translateY(0)';
      });
      const hide = () => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(12px)';
        setTimeout(() => toast.remove(), 250);
      };
      const timeout = setTimeout(hide, TOAST_DURATION);
      window.addEventListener('unload', () => clearTimeout(timeout), { once: true });
    } catch (e) {
      console.log('Не удалось показать toast:', e);
    }
  }

  async loadConfig() {
    const response = await this.sendMessage({ action: 'getState' });
    
    if (response) {
      this.state.enabled = response.enabled || false;
      
      if (response.settings) {
        this.config.targetTexts = response.settings.targetTexts || this.config.targetTexts;
        this.config.checkInterval = response.settings.checkInterval || this.config.checkInterval;
        this.config.minDelay = response.settings.minDelay || this.config.minDelay;
        this.config.maxDelay = response.settings.maxDelay || this.config.maxDelay;
      }
    }
  }

  setupListeners() {
    api.runtime.onMessage.addListener((message, sender, sendResponse) => {
      switch (message.action) {
        case 'toggle':
          this.state.enabled = message.enabled;
          if (this.state.enabled) {
            this.startClicking();
          } else {
            this.stopClicking();
          }
          sendResponse({ success: true });
          break;
        case 'localNotify':
          this.showLocalNotification(message.title, message.message);
          sendResponse({ success: true });
          break;
        case 'playSound':
          this.playSound(message.type, message.volume);
          sendResponse({ success: true });
          break;
        case 'ping':
          // KeepAlive ping от background.js
          sendResponse({ success: true, timestamp: Date.now() });
          break;
      }
      return true;
    });
  }

  // ========== ЗВУКОВЫЕ УВЕДОМЛЕНИЯ ==========
  
  playSound(type = 'click', volume = 0.5) {
    try {
      // Проверка поддержки AudioContext (может отсутствовать на мобильных)
      const AudioContextClass = window.AudioContext || window.webkitAudioContext;
      if (!AudioContextClass) {
        console.log('[Exotic] AudioContext not supported');
        return;
      }
      
      const audioContext = new AudioContextClass();
      
      // Мобильные браузеры требуют возобновления AudioContext после жеста
      if (audioContext.state === 'suspended') {
        audioContext.resume().catch(() => {});
        // Если не удалось возобновить - выходим без ошибки
        if (audioContext.state === 'suspended') {
          console.log('[Exotic] AudioContext suspended, sound skipped');
          return;
        }
      }
      
      const oscillator = audioContext.createOscillator();
      const gainNode = audioContext.createGain();
      
      oscillator.connect(gainNode);
      gainNode.connect(audioContext.destination);
      
      // Разные звуки для разных событий
      switch (type) {
        case 'click':
          // Короткий "пинг" для клика
          oscillator.frequency.setValueAtTime(880, audioContext.currentTime); // A5
          oscillator.frequency.exponentialRampToValueAtTime(440, audioContext.currentTime + 0.1);
          gainNode.gain.setValueAtTime(volume * 0.3, audioContext.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.15);
          oscillator.start(audioContext.currentTime);
          oscillator.stop(audioContext.currentTime + 0.15);
          break;
          
        case 'enable':
          // Восходящий звук для включения
          oscillator.frequency.setValueAtTime(440, audioContext.currentTime);
          oscillator.frequency.exponentialRampToValueAtTime(880, audioContext.currentTime + 0.15);
          gainNode.gain.setValueAtTime(volume * 0.3, audioContext.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.2);
          oscillator.start(audioContext.currentTime);
          oscillator.stop(audioContext.currentTime + 0.2);
          break;
          
        case 'disable':
          // Нисходящий звук для выключения
          oscillator.frequency.setValueAtTime(880, audioContext.currentTime);
          oscillator.frequency.exponentialRampToValueAtTime(220, audioContext.currentTime + 0.2);
          gainNode.gain.setValueAtTime(volume * 0.3, audioContext.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.25);
          oscillator.start(audioContext.currentTime);
          oscillator.stop(audioContext.currentTime + 0.25);
          break;
          
        case 'error':
          // Двойной низкий звук для ошибки
          oscillator.frequency.setValueAtTime(220, audioContext.currentTime);
          gainNode.gain.setValueAtTime(volume * 0.4, audioContext.currentTime);
          gainNode.gain.setValueAtTime(0.01, audioContext.currentTime + 0.1);
          gainNode.gain.setValueAtTime(volume * 0.4, audioContext.currentTime + 0.15);
          gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.3);
          oscillator.start(audioContext.currentTime);
          oscillator.stop(audioContext.currentTime + 0.3);
          break;
          
        default:
          // Стандартный звук
          oscillator.frequency.setValueAtTime(660, audioContext.currentTime);
          gainNode.gain.setValueAtTime(volume * 0.2, audioContext.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.1);
          oscillator.start(audioContext.currentTime);
          oscillator.stop(audioContext.currentTime + 0.1);
      }
      
      // Закрываем контекст после завершения
      setTimeout(() => {
        if (audioContext.state !== 'closed') {
          audioContext.close().catch(() => {});
        }
      }, 500);
      
    } catch (err) {
      console.log('Sound playback error:', err);
    }
  }

  setupObserver() {
    log('👁️ Запуск MutationObserver...');
    
    // Инициализируем таймер дебаунса
    this.observerDebounceTimer = null;
    
    this.state.observer = new MutationObserver((mutations) => {
      // Проверяем только если автокликер включён
      if (!this.state.enabled || this.state.destroyed) return;
      
      // Проверяем есть ли добавленные узлы (новые элементы)
      const hasNewNodes = mutations.some(m => m.addedNodes.length > 0);
      
      if (hasNewNodes) {
        // Дебаунс 100мс - достаточно быстро, но не спамит
        clearTimeout(this.observerDebounceTimer);
        this.observerDebounceTimer = setTimeout(() => {
          // ДВОЙНАЯ ПРОВЕРКА - обязательно проверяем enabled ещё раз!
          if (this.state.enabled && !this.state.running && !this.state.destroyed) {
            log('👁️ Новые элементы, сканирую...');
            this.scanAndClick();
          }
        }, 100);
      }
    });
    
    // Наблюдаем за всем body
    this.state.observer.observe(document.body, {
      childList: true,
      subtree: true
    });
    
    log('✅ MutationObserver запущен');
  }

  // Адаптивный интервал - замедляется при отсутствии активности
  getAdaptiveInterval() {
    const timeSinceActivity = Date.now() - this.state.lastActivity;
    
    // Если не было активности более 2 минут - замедляемся
    if (timeSinceActivity > 2 * 60 * 1000) {
      return CHECK_INTERVAL * IDLE_SLOWDOWN_MULTIPLIER;
    }
    
    return CHECK_INTERVAL;
  }

  startMonitor() {
    log('⏱️ Мониторинг с адаптивным интервалом');
    
    const runCheck = () => {
      if (this.state.destroyed) return;
      
      if (this.state.enabled && !this.state.running) {
        this.scanAndClick();
      }
      
      // Планируем следующую проверку с адаптивным интервалом
      const nextInterval = this.getAdaptiveInterval();
      this.state.monitorId = setTimeout(runCheck, nextInterval);
    };
    
    // Запускаем первую проверку
    this.state.monitorId = setTimeout(runCheck, CHECK_INTERVAL);
    
    log('✅ Мониторинг запущен');
  }

  startClicking() {
    logAlways('▶️ Автокликер ВКЛЮЧЁН');
    this.state.enabled = true;
    // Сразу сканируем при включении
    this.scanAndClick();
  }

  stopClicking() {
    logAlways('⏹️ Автокликер ВЫКЛЮЧЕН');
    // Полностью останавливаем
    this.state.enabled = false;
    this.state.running = false;
    
    // Очищаем таймер дебаунса observer
    if (this.observerDebounceTimer) {
      clearTimeout(this.observerDebounceTimer);
      this.observerDebounceTimer = null;
    }
    
    // Скрываем отображение курса при отключении
    this.hideRateDisplay();
  }

  async scanAndClick() {
    // ТРОЙНАЯ ПРОВЕРКА перед началом
    if (this.state.destroyed || this.state.running || !this.state.enabled) {
      log('⛔ scanAndClick отменён: destroyed=', this.state.destroyed, 'running=', this.state.running, 'enabled=', this.state.enabled);
      return;
    }
    
    this.state.running = true;
    
    // Очищаем старые ID для предотвращения утечек памяти
    this.cleanupClickedIds();
    
    try {
      const elements = await this.findTargetElements();
      
      if (elements.length > 0) {
        // Ограничиваем количество обрабатываемых кнопок за раз
        const toProcess = elements.slice(0, BATCH_PROCESS_LIMIT);
        
        this.state.stats.found += toProcess.length;
        logAlways('🎯 НАЙДЕНО', elements.length, 'кнопок, обрабатываю', toProcess.length);
        this.showToast(`🎯 Найдено ${elements.length} заявок!`);
        
        // Сбрасываем счётчик простоя
        this.state.lastActivity = Date.now();
        
        for (let i = 0; i < toProcess.length; i++) {
          // ПРОВЕРКА перед каждым кликом
          if (!this.state.enabled || this.state.destroyed) {
            logAlways('⏹️ Клики остановлены - автокликер выключен');
            break;
          }
          
          const element = toProcess[i];
          const transactionId = element._transactionId;
          logAlways('👆 Кликаю', i + 1, '/', elements.length, '| ID:', transactionId || '?');
          
          const transactionData = this.extractTransactionData(element);
          
          // === RETRY ЛОГИКА ===
          let success = false;
          for (let attempt = 1; attempt <= this.config.maxRetries; attempt++) {
            // ПРОВЕРКА перед каждой попыткой
            if (!this.state.enabled || this.state.destroyed) {
              logAlways('⏹️ Retry остановлен - автокликер выключен');
              break;
            }
            
            log('Попытка', attempt, '/', this.config.maxRetries);
            
            if (await this.simulateClick(element)) {
              success = true;
              break;
            }
            
            if (attempt < this.config.maxRetries) {
              this.state.stats.retries++;
              log('Retry через', this.config.retryDelay, 'мс...');
              await this.delay(this.config.retryDelay);
            }
          }
          
          // Если выключили во время retry - выходим
          if (!this.state.enabled || this.state.destroyed) break;
          
          if (success) {
            this.state.clickedElements.add(element);
            if (transactionId) {
              this.state.clickedIds.add(transactionId);
            }
            this.state.stats.clicked++;
            this.markActivity(); // Отмечаем активность
            logAlways('✅ Успех! (кликнуто:', this.state.stats.clicked + ')');
            this.showToast('✅ Заявка взята в работу!');
            await this.reportClick(transactionData);
          } else {
            this.state.stats.failed++;
            logAlways('❌ Не удалось после', this.config.maxRetries, 'попыток');
          }
          
          // Минимальная задержка между кликами
          if (i < elements.length - 1) {
            await this.delay(this.config.minDelay + Math.random() * (this.config.maxDelay - this.config.minDelay));
          }
        }
      }
    } catch (error) {
      logAlways('❌ Ошибка:', error.message);
    } finally {
      this.state.running = false;
    }
  }

  async findTargetElements() {
    if (!document.body) return [];

    const elements = [];
    const foundButtons = new Set();
    
    log('========== ПОИСК КНОПОК ==========');
    
    // МЕТОД 1: Ищем ВСЕ кнопки и проверяем по тексту
    const allButtons = document.querySelectorAll('button');
    log('Всего кнопок:', allButtons.length);
    
    for (const button of allButtons) {
      // Получаем текст кнопки (включая вложенные элементы)
      const buttonText = (button.textContent || button.innerText || '').trim();
      const buttonTextLower = buttonText.toLowerCase();
      
      // ====== ИСКЛЮЧЕНИЯ - кнопки которые НЕ нужно нажимать ======
      const isExcludedButton = 
        buttonTextLower.includes('подтвердить') ||
        buttonTextLower.includes('подтверд') ||
        buttonTextLower.includes('confirm') ||
        buttonTextLower.includes('отклонить') ||
        buttonTextLower.includes('отмен') ||
        buttonTextLower.includes('cancel') ||
        buttonTextLower.includes('reject') ||
        buttonTextLower.includes('закрыть') ||
        buttonTextLower.includes('close') ||
        buttonTextLower.includes('удалить') ||
        buttonTextLower.includes('delete') ||
        buttonTextLower.includes('сохранить') ||
        buttonTextLower.includes('save') ||
        buttonTextLower.includes('отправить') ||
        buttonTextLower.includes('submit') ||
        buttonTextLower.includes('прикрепить') ||
        buttonTextLower.includes('attach') ||
        buttonTextLower.includes('загрузить') ||
        buttonTextLower.includes('upload') ||
        buttonTextLower.includes('выплатить') ||
        buttonTextLower.includes('выплата') ||
        buttonTextLower.includes('оплатить') ||
        buttonTextLower.includes('pay') ||
        // Проверяем что кнопка в модальном окне (не в таблице)
        button.closest('.modal') !== null ||
        button.closest('[role="dialog"]') !== null ||
        button.closest('.popup') !== null ||
        button.closest('.overlay') !== null;
      
      // Пропускаем исключённые кнопки
      if (isExcludedButton) {
        log('⛔ Исключена:', buttonText.substring(0, 30));
        continue;
      }
      
      // ====== ТОЧНЫЙ ПОИСК кнопки "В работу" ======
      const isWorkButton = 
        // Точные совпадения (приоритет)
        buttonTextLower === 'в работу' ||
        buttonTextLower === 'взять в работу' ||
        buttonTextLower === 'принять в работу' ||
        buttonTextLower === 'take to work' ||
        // Частичные совпадения (только если текст короткий - это кнопка)
        (buttonTextLower.includes('в работу') && buttonText.length < 30) ||
        // Символ на кнопке (специфичный для Exotic)
        (buttonText.includes('◎') && buttonText.length < 20) ||
        // Проверяем onclick на handleTeamStatusChange (специфичная функция Exotic)
        (button.getAttribute('onclick') || '').includes('handleTeamStatusChange');
      
      if (isWorkButton) {
        // Извлекаем ID транзакции из onclick или data-атрибутов
        const onclick = button.getAttribute('onclick') || '';
        const transactionIdMatch = onclick.match(/['"](\d{10,})['"]/) || 
                                   onclick.match(/(\d{10,})/) ||
                                   [null, button.closest('tr')?.querySelector('td')?.textContent?.trim()];
        const transactionId = transactionIdMatch?.[1];
        
        log('Кнопка:', buttonText.substring(0, 30), '| ID:', transactionId || '?');
        
        // Проверяем что кнопка кликабельна и транзакция не обработана
        const alreadyProcessed = transactionId && this.state.clickedIds.has(transactionId);
        
        if (!button.disabled && 
            button.offsetParent !== null &&
            !this.state.clickedElements.has(button) &&
            !alreadyProcessed &&
            !foundButtons.has(button)) {
          
          // Дополнительная проверка видимости
          const rect = button.getBoundingClientRect();
          const style = window.getComputedStyle(button);
          
          if (rect.width > 0 && 
              rect.height > 0 && 
              style.display !== 'none' && 
              style.visibility !== 'hidden') {
            
            // Сохраняем ID транзакции в кнопке для использования при клике
            button._transactionId = transactionId;
            elements.push(button);
            foundButtons.add(button);
            log('✅ Добавлена в очередь');
          } else {
            log('⚠️ Скрыта');
          }
        } else {
          log('⚠️ Недоступна:', button.disabled ? 'disabled' : alreadyProcessed ? 'уже обработана' : 'другое');
        }
      }
    }
    
    // МЕТОД 2: Поиск по классам Bootstrap (если метод 1 не нашёл)
    if (elements.length === 0) {
      log('Метод 1 пуст, пробуем классы...');
      
      // Ищем только кнопки с классом work в таблице (не в модалках)
      const bootstrapButtons = document.querySelectorAll(
        'table .btn-outline-success, table .btn-success, table [class*="work"], tr .btn-outline-success, tr .btn-success'
      );
      
      for (const button of bootstrapButtons) {
        if (foundButtons.has(button)) continue;
        
        const text = (button.textContent || '').toLowerCase();
        const buttonText = (button.textContent || '').trim();
        
        // Проверяем исключения
        const isExcluded = 
          text.includes('подтвердить') ||
          text.includes('отклонить') ||
          text.includes('отмен') ||
          text.includes('закрыть') ||
          button.closest('.modal') !== null ||
          button.closest('[role="dialog"]') !== null;
        
        if (isExcluded) continue;
        
        // Проверяем что это кнопка "В работу"
        if ((text.includes('в работу') || (text.includes('◎') && buttonText.length < 20)) && 
            !button.disabled && 
            button.offsetParent !== null &&
            !this.state.clickedElements.has(button)) {
          elements.push(button);
          foundButtons.add(button);
          log('✅ По классу');
        }
      }
    }
    
    // МЕТОД 3: Поиск по onclick атрибуту (только handleTeamStatusChange)
    if (elements.length === 0) {
      log('Пробуем onclick...');
      
      // Только специфичная функция Exotic
      const onclickButtons = document.querySelectorAll('[onclick*="handleTeamStatusChange"]');
      
      for (const button of onclickButtons) {
        if (foundButtons.has(button) || this.state.clickedElements.has(button)) continue;
        
        // Проверяем что не в модальном окне
        if (button.closest('.modal') || button.closest('[role="dialog"]')) continue;
        
        if (!button.disabled && button.offsetParent !== null) {
          elements.push(button);
          foundButtons.add(button);
          log('✅ По onclick');
        }
      }
    }
    
    log('ИТОГО:', elements.length);
    return elements;
  }

  isValidElement(element) {
    if (!element) return false;
    
    // Упрощённая проверка - только базовые условия
    return !element.disabled && 
           element.offsetParent !== null;
  }

  // УДАЛЁН: isClickableElement() - не использовался в коде

  async simulateClick(element) {
    log('=== КЛИК ===');
    
    try {
      // Запоминаем состояние DOM для проверки изменений
      const parentHTML = element.parentElement?.innerHTML?.length || 0;
      const rowElement = element.closest('tr');
      const rowHTML = rowElement?.innerHTML?.length || 0;
      
      // Скроллим к элементу с гарантией видимости
      element.scrollIntoView({ behavior: 'instant', block: 'center' });
      await this.delay(30);
      
      // Убеждаемся что элемент в области видимости
      const rect = element.getBoundingClientRect();
      if (rect.top < 0 || rect.bottom > window.innerHeight) {
        window.scrollBy(0, rect.top - window.innerHeight / 2);
        await this.delay(30);
      }
      
      const updatedRect = element.getBoundingClientRect();
      const centerX = updatedRect.left + updatedRect.width / 2;
      const centerY = updatedRect.top + updatedRect.height / 2;
      
      // ===== СПОСОБ 1: Прямой вызов onclick (самый надёжный) =====
      const onclickAttr = element.getAttribute('onclick');
      if (onclickAttr) {
        log('Способ 1: onclick attr');
        
        try {
          // 1.1: handleTeamStatusChange - специфичная функция Exotic
          if (onclickAttr.includes('handleTeamStatusChange')) {
            const match = onclickAttr.match(/handleTeamStatusChange\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]/);
            if (match) {
              log('handleTeamStatusChange:', match[1], match[2]);
              if (typeof window.handleTeamStatusChange === 'function') {
                window.handleTeamStatusChange(match[1], match[2]);
                log('✅ handleTeamStatusChange direct!');
                await this.delay(CLICK_VERIFICATION_DELAY);
                if (this.verifyClickSuccess(element, parentHTML, rowHTML, rowElement)) return true;
              }
            }
          }
          
          // 1.2: Любые другие функции в onclick
          const funcMatch = onclickAttr.match(/(\w+)\s*\(/);
          if (funcMatch && typeof window[funcMatch[1]] === 'function') {
            log('Вызов функции:', funcMatch[1]);
            try {
              // Парсим аргументы
              const argsMatch = onclickAttr.match(/\(([^)]*)\)/);
              if (argsMatch) {
                const args = argsMatch[1].split(',').map(a => {
                  const trimmed = a.trim().replace(/^['"]|['"]$/g, '');
                  return isNaN(trimmed) ? trimmed : Number(trimmed);
                });
                window[funcMatch[1]](...args);
                log('✅ Direct function call!');
                await this.delay(CLICK_VERIFICATION_DELAY);
                if (this.verifyClickSuccess(element, parentHTML, rowHTML, rowElement)) return true;
              }
            } catch (e) {
              log('Direct call fail:', e.message);
            }
          }
          
          // 1.3: Выполняем onclick напрямую через element.click()
          // (Методы с new Function и eval удалены для совместимости с Chrome Web Store)
        } catch (e) {
          log('onclick methods fail:', e.message);
        }
      }
      
      // ===== СПОСОБ 2: element.onclick() напрямую =====
      if (typeof element.onclick === 'function') {
        try {
          element.onclick(new MouseEvent('click', { bubbles: true }));
          log('✅ element.onclick()!');
          await this.delay(CLICK_VERIFICATION_DELAY);
          if (this.verifyClickSuccess(element, parentHTML, rowHTML, rowElement)) return true;
        } catch (e) {
          log('onclick() fail:', e.message);
        }
      }
      
      // ===== СПОСОБ 3: Native click() - самый простой =====
      try {
        element.click();
        log('Native click()');
        await this.delay(CLICK_VERIFICATION_DELAY);
        if (this.verifyClickSuccess(element, parentHTML, rowHTML, rowElement)) return true;
      } catch (e) {}
      
      // ===== СПОСОБ 4: Фокус + Enter =====
      try {
        element.focus();
        await this.delay(20);
        
        const enterDown = new KeyboardEvent('keydown', { 
          key: 'Enter', 
          code: 'Enter',
          keyCode: 13, 
          which: 13,
          bubbles: true,
          cancelable: true
        });
        const enterPress = new KeyboardEvent('keypress', { 
          key: 'Enter', 
          code: 'Enter',
          keyCode: 13, 
          which: 13,
          bubbles: true,
          cancelable: true
        });
        const enterUp = new KeyboardEvent('keyup', { 
          key: 'Enter', 
          code: 'Enter',
          keyCode: 13, 
          which: 13,
          bubbles: true,
          cancelable: true
        });
        
        element.dispatchEvent(enterDown);
        element.dispatchEvent(enterPress);
        element.dispatchEvent(enterUp);
        log('Enter events sent');
        await this.delay(CLICK_VERIFICATION_DELAY);
        if (this.verifyClickSuccess(element, parentHTML, rowHTML, rowElement)) return true;
      } catch (e) {}
      
      // ===== СПОСОБ 5: Полная симуляция мыши с Touch =====
      try {
        // Touch events для мобильных обработчиков
        const touchStart = new TouchEvent('touchstart', {
          bubbles: true,
          cancelable: true,
          touches: [new Touch({ identifier: 1, target: element, clientX: centerX, clientY: centerY })]
        });
        const touchEnd = new TouchEvent('touchend', {
          bubbles: true,
          cancelable: true,
          changedTouches: [new Touch({ identifier: 1, target: element, clientX: centerX, clientY: centerY })]
        });
        element.dispatchEvent(touchStart);
        await this.delay(10);
        element.dispatchEvent(touchEnd);
        log('Touch events');
      } catch (e) {}
      
      // Mouse events - полная последовательность
      const mouseEvents = [
        'pointerenter', 'pointerover', 'pointermove',
        'mouseenter', 'mouseover', 'mousemove', 
        'pointerdown', 'mousedown',
        'focus',
        'pointerup', 'mouseup', 
        'click'
      ];
      
      for (const eventType of mouseEvents) {
        try {
          const isPointer = eventType.startsWith('pointer');
          const EventClass = isPointer ? PointerEvent : (eventType === 'focus' ? FocusEvent : MouseEvent);
          
          const eventInit = eventType === 'focus' ? { bubbles: true } : {
            view: window,
            bubbles: true,
            cancelable: true,
            composed: true,
            clientX: centerX,
            clientY: centerY,
            screenX: window.screenX + centerX,
            screenY: window.screenY + centerY,
            pageX: centerX + window.scrollX,
            pageY: centerY + window.scrollY,
            button: 0,
            buttons: eventType.includes('down') ? 1 : 0,
            relatedTarget: null,
            pointerId: 1,
            pointerType: 'mouse',
            isPrimary: true,
            detail: eventType === 'click' ? 1 : 0
          };
          
          const event = new EventClass(eventType, eventInit);
          element.dispatchEvent(event);
          await this.delay(5);
        } catch (e) {}
      }
      log('Mouse events done');
      await this.delay(CLICK_VERIFICATION_DELAY);
      if (this.verifyClickSuccess(element, parentHTML, rowHTML, rowElement)) return true;
      
      // ===== СПОСОБ 6: jQuery если есть =====
      if (window.jQuery || window.$) {
        try {
          const $ = window.jQuery || window.$;
          $(element).trigger('click');
          $(element).trigger('mousedown').trigger('mouseup').trigger('click');
          log('jQuery triggered');
          await this.delay(CLICK_VERIFICATION_DELAY);
          if (this.verifyClickSuccess(element, parentHTML, rowHTML, rowElement)) return true;
        } catch (e) {}
      }
      
      // ===== СПОСОБ 7: InputEvent для input/button =====
      try {
        element.dispatchEvent(new InputEvent('input', { bubbles: true }));
        element.dispatchEvent(new Event('change', { bubbles: true }));
        element.dispatchEvent(new Event('submit', { bubbles: true }));
      } catch (e) {}
      
      // ===== СПОСОБ 8: HTMLElement.prototype.click =====
      try {
        HTMLElement.prototype.click.call(element);
        log('Prototype click');
        await this.delay(CLICK_VERIFICATION_DELAY);
        if (this.verifyClickSuccess(element, parentHTML, rowHTML, rowElement)) return true;
      } catch (e) {}
      
      // Если дошли сюда - клик отправлен всеми способами
      log('All methods executed');
      return true;
    } catch (error) {
      log('❌ Click error:', error.message);
      return false;
    }
  }

  // Проверка успешности клика по изменениям в DOM
  verifyClickSuccess(element, oldParentHTML, oldRowHTML, rowElement) {
    try {
      // Кнопка исчезла или стала disabled
      if (!document.contains(element) || element.disabled) {
        log('✅ Кнопка исчезла/disabled');
        return true;
      }
      
      // Изменился родитель
      const newParentHTML = element.parentElement?.innerHTML?.length || 0;
      if (Math.abs(newParentHTML - oldParentHTML) > 10) {
        log('✅ DOM изменился');
        return true;
      }
      
      // Изменилась строка таблицы
      if (rowElement) {
        const newRowHTML = rowElement.innerHTML?.length || 0;
        if (Math.abs(newRowHTML - oldRowHTML) > 10) {
          log('✅ Row изменился');
          return true;
        }
        
        // Проверяем статус в строке
        const rowText = rowElement.textContent?.toLowerCase() || '';
        if (rowText.includes('в работе') || rowText.includes('in progress') || rowText.includes('processing')) {
          log('✅ Статус изменён');
          return true;
        }
      }
      
      // Кнопка больше не видна
      const rect = element.getBoundingClientRect();
      if (rect.width === 0 || rect.height === 0) {
        log('✅ Кнопка скрыта');
        return true;
      }
      
      return false;
    } catch (e) {
      return false;
    }
  }

  extractTransactionData(buttonElement) {
    try {
      // Ищем строку таблицы (tr) которая содержит кнопку
      let row = buttonElement.closest('tr');
      
      // Если не нашли tr, ищем родительский контейнер с данными
      if (!row) {
        row = buttonElement.closest('[class*="row"]') || 
              buttonElement.closest('[class*="item"]') ||
              buttonElement.closest('[class*="transaction"]') ||
              buttonElement.parentElement?.parentElement?.parentElement;
      }
      
      if (!row) {
        return null;
      }

      // Получаем все ячейки/элементы в строке
      const cells = row.querySelectorAll('td, [class*="cell"], [class*="col"]');
      const allText = row.innerText || row.textContent || '';
      
      // Пытаемся извлечь данные разными способами
      const data = {
        transactionId: null,
        amount: null,
        method: null,
        created: null,
        requisites: null,
        bank: null
      };

      // Способ 1: Парсим ячейки таблицы по порядку (как на скриншоте)
      if (cells.length >= 6) {
        data.transactionId = this.cleanText(cells[0]?.innerText);
        data.amount = this.cleanText(cells[1]?.innerText);
        data.method = this.cleanText(cells[2]?.innerText);
        // cells[3] - метки (пропускаем)
        data.created = this.cleanText(cells[4]?.innerText);
        // cells[5] - осталось (пропускаем)
        data.requisites = this.cleanText(cells[6]?.innerText);
        data.bank = this.cleanText(cells[7]?.innerText);
      }

      // Способ 2: Ищем по паттернам в тексте
      if (!data.transactionId) {
        // ID транзакции - длинное число
        const idMatch = allText.match(/(\d{8,})/);
        if (idMatch) data.transactionId = idMatch[1];
      }

      if (!data.amount) {
        // Сумма с символом рубля или числом
        const amountMatch = allText.match(/([\d\s]+[₽P]|[\d\s]+руб)/i);
        if (amountMatch) data.amount = amountMatch[1].trim();
      }

      if (!data.requisites) {
        // Реквизиты - номер карты (16 цифр с пробелами)
        const cardMatch = allText.match(/(\d{4}\s?\d{4}\s?\d{4}\s?\d{4})/);
        if (cardMatch) data.requisites = cardMatch[1];
      }

      if (!data.bank) {
        // Банк
        const bankPatterns = ['sberbank', 'tinkoff', 'vtb', 'alfa', 'raiffeisen', 'gazprom', 'сбер', 'тинькофф', 'втб', 'альфа'];
        const lowerText = allText.toLowerCase();
        for (const bank of bankPatterns) {
          if (lowerText.includes(bank)) {
            data.bank = bank.charAt(0).toUpperCase() + bank.slice(1);
            break;
          }
        }
        // Или ищем ru-* паттерн
        const ruBankMatch = allText.match(/ru-(\w+)/i);
        if (ruBankMatch) data.bank = ruBankMatch[1];
      }

      if (!data.method) {
        // Способ оплаты
        if (allText.toLowerCase().includes('карт')) data.method = 'Банковская карта';
        else if (allText.toLowerCase().includes('sbp') || allText.toLowerCase().includes('сбп')) data.method = 'СБП';
      }

      if (!data.created) {
        // Дата создания
        const dateMatch = allText.match(/(\d{1,2}\s+\w+\.?\s+\d{1,2}:\d{2})/);
        if (dateMatch) data.created = dateMatch[1];
      }

      return data;
    } catch (error) {
      console.error('Ошибка извлечения данных транзакции:', error);
      return null;
    }
  }

  cleanText(text) {
    if (!text) return null;
    return text.replace(/\s+/g, ' ').trim() || null;
  }

  async reportClick(transactionData = null) {
    const clickTime = Date.now();
    
    await this.sendMessage({
      action: 'updateStats',
      data: { 
        clicks: 1,
        clickTime: clickTime,
        successful: 1,
        transaction: transactionData
      }
    });
  }

  getRandomDelay() {
    return Math.random() * (this.config.maxDelay - this.config.minDelay) + this.config.minDelay;
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async sendMessage(message) {
    return new Promise((resolve) => {
      // Проверяем доступность API (может быть null на мобильных)
      if (!api || !api.runtime || !api.runtime.id) {
        resolve(null);
        return;
      }

      try {
        api.runtime.sendMessage(message, (response) => {
          const err = api.runtime?.lastError;
          if (err) {
            const msg = String(err.message || '').toLowerCase();
            if (msg.includes('context invalidated') || msg.includes('receiving end')) {
              // Extension context is gone (reload/disabled) — stop work silently
              this.state.enabled = false;
            }
            resolve(null);
          } else {
            resolve(response);
          }
        });
      } catch (e) {
        console.log('[Exotic] sendMessage error:', e.message);
        resolve(null);
      }
    });
  }

  async showLocalNotification(title, message) {
    // Always show a toast in-page for visibility
    this.showToast(`${title}: ${message}`);

    if (!('Notification' in window)) return;
    let permission = Notification.permission;
    if (permission === 'default' && !this.state.notificationPermissionRequested) {
      this.state.notificationPermissionRequested = true;
      try {
        permission = await Notification.requestPermission();
      } catch (_) {
        permission = 'denied';
      }
    }
    if (permission !== 'granted') return;
    try {
      // Для Service Worker (мобильные браузеры)
      if (navigator.serviceWorker?.ready) {
        const registration = await navigator.serviceWorker.ready;
        if (registration.showNotification) {
          await registration.showNotification(title, {
            body: message,
            icon: api.runtime?.getURL ? api.runtime.getURL('icons/icon128.png') : undefined
          });
          return;
        }
      }
      // Fallback для десктопа
      if (typeof Notification !== 'undefined' && Notification.permission === 'granted') {
        new Notification(title, {
          body: message,
          icon: api.runtime?.getURL ? api.runtime.getURL('icons/icon128.png') : undefined
        });
      }
    } catch (error) {
      // Игнорируем ошибки уведомлений - не критично
      console.warn('Notification not available:', error.message);
    }
  }
  
  // Метод для просмотра статистики в консоли
  getStats() {
    return {
      ...this.state.stats,
      enabled: this.state.enabled,
      processedIds: this.state.clickedIds.size,
      successRate: this.state.stats.clicked > 0 
        ? Math.round(this.state.stats.clicked / (this.state.stats.clicked + this.state.stats.failed) * 100) + '%'
        : 'N/A'
    };
  }
}

// Запускаем на всех страницах exotic.company (включая gate-1, gate-2 и т.д.)
let exoticInstance = null;
if (window.location.hostname.includes('exotic.company')) {
  exoticInstance = new ExoticAutoclicker();
  
  // Глобальный доступ для отладки
  window.ExoticStats = () => {
    const stats = exoticInstance?.getStats();
    console.table(stats);
    return stats;
  };
  
  logAlways('💡 Для статистики введите: ExoticStats()');
}