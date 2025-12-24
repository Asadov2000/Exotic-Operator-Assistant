
const api = typeof browser !== 'undefined' ? browser : (typeof chrome !== 'undefined' ? chrome : null);

class ExoticAutoclicker {
  constructor() {
    this.state = {
      enabled: false,
      running: false,
      clickedElements: new WeakSet(),
      observer: null,
      monitorId: null,
      destroyed: false,
      notificationPermissionRequested: false
    };

    this.config = {
      targetTexts: ['В работу', 'Взять в работу', 'Принять в работу', 'Take to work'],
      checkInterval: 2000,
      minDelay: 5000,
      maxDelay: 60000
    };

    this.setupListeners();
    this.setupObserver();
    this.startMonitor();
    this.loadConfig();
  }

}

// Ограничение: только payout_transaction
if (window.location.href.startsWith('https://exotic.company/operator/payout_transaction')) {
  new ExoticAutoclicker();
}

  showToast(text) {
    try {
      const existing = document.querySelector('.exotic-toast');
      if (existing) existing.remove();
      const toast = document.createElement('div');
      toast.className = 'exotic-toast';
      toast.textContent = text;
      Object.assign(toast.style, {
        position: 'fixed',
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
      const timeout = setTimeout(hide, 2500);
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
      }
      return true;
    });
  }

  setupObserver() {
    let lastRun = 0;
    this.state.observer = new MutationObserver((mutations) => {
      const now = Date.now();
      // Не чаще раза в 500мс
      if (now - lastRun < 500) return;
      // Проверяем, есть ли изменения по нужным селекторам
      const relevant = mutations.some(m => {
        if (m.type === 'attributes') {
          return m.target && (m.target.classList?.contains('workButton') || m.target.classList?.contains('take-work-btn'));
        }
        return true;
      });
      if (relevant && this.state.enabled && !this.state.running && !this.state.destroyed) {
        lastRun = now;
        this.scanAndClick();
      }
    });
    this.state.observer.observe(document.body, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ['style', 'class', 'disabled']
    });
  }

  startMonitor() {
    this.state.monitorId = setInterval(() => {
      if (this.state.destroyed) return;
      if (this.state.enabled && !this.state.running) {
        this.scanAndClick();
      }
    }, this.config.checkInterval);
  }

  startClicking() {
    console.log('Exotic Assistant: Запущен');
    this.scanAndClick();
  }

  stopClicking() {
    console.log('Exotic Assistant: Остановлен');
    this.state.running = false;
  }

  async scanAndClick() {
    if (this.state.destroyed || this.state.running || !this.state.enabled) return;
    
    this.state.running = true;
    
    try {
      const elements = await this.findTargetElements();
      
      if (elements.length > 0) {
        for (let i = 0; i < elements.length; i++) {
          if (!this.state.enabled) break;
          
          const element = elements[i];
          
          // Извлекаем данные транзакции из строки таблицы
          const transactionData = this.extractTransactionData(element);
          
          if (await this.simulateClick(element)) {
            this.state.clickedElements.add(element);
            await this.reportClick(transactionData);
          }
          
          if (i < elements.length - 1) {
            await this.delay(this.getRandomDelay());
          }
        }
      }
    } catch (error) {
    } finally {
      this.state.running = false;
    }
  }

  async findTargetElements() {
    if (!document.body) return [];

    const elements = [];
    // Ищем все кнопки с нужными классами
    const buttons = document.querySelectorAll('button.btn-outline-success.d-inline-flex.align-items-center.justify-content-center');
    for (const button of buttons) {
      const buttonText = button.textContent.trim().toLowerCase();
      for (const targetText of this.config.targetTexts) {
        if (buttonText.includes(targetText.toLowerCase()) && this.isValidElement(button) && !this.state.clickedElements.has(button)) {
          elements.push(button);
        }
      }
    }
    return elements;
  }

  isValidElement(element) {
    if (!element) return false;
    
    const style = window.getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    
    return style.display !== 'none' &&
           style.visibility !== 'hidden' &&
           style.opacity !== '0' &&
           rect.width > 0 &&
           rect.height > 0 &&
           !element.disabled &&
           element.offsetParent !== null;
  }

  isClickableElement(element) {
    const tag = element.tagName.toLowerCase();
    const role = element.getAttribute('role');
    
    return ['button', 'a', 'input', 'select'].includes(tag) ||
           role === 'button' ||
           element.onclick ||
           element.hasAttribute('onclick') ||
           window.getComputedStyle(element).cursor === 'pointer';
  }

  async simulateClick(element) {
    try {
      const rect = element.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const centerY = rect.top + rect.height / 2;
      
      const events = ['mouseover', 'mousedown', 'mouseup', 'click'];
      
      for (const eventType of events) {
        const event = new MouseEvent(eventType, {
          view: window,
          bubbles: true,
          cancelable: true,
          clientX: centerX + (Math.random() * 10 - 5),
          clientY: centerY + (Math.random() * 10 - 5),
          button: 0
        });
        
        element.dispatchEvent(event);
        await this.delay(50 + Math.random() * 50);
      }
      
      if (typeof element.click === 'function') {
        element.click();
      }
      
      return true;
    } catch (error) {
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
      if (!api.runtime?.id) {
        resolve(null);
        return;
      }

      api.runtime.sendMessage(message, (response) => {
        const err = api.runtime.lastError;
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
      new Notification(title, {
        body: message,
        icon: api.runtime?.getURL ? api.runtime.getURL('icons/icon128.png') : undefined
      });
    } catch (error) {
    }
  }

  showToast(text) {
    try {
      const existing = document.querySelector('.exotic-toast');
      if (existing) existing.remove();

      const toast = document.createElement('div');
      toast.className = 'exotic-toast';
      toast.textContent = text;
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
      setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(12px)';
        setTimeout(() => toast.remove(), 250);
      }, 2500);
    } catch (e) {
      console.log('Не удалось показать toast:', e);
    }
  }
}

new ExoticAutoclicker();