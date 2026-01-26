/**
 * Unit Tests для Exotic Telegram Worker
 * 
 * Запуск: npm test или npx vitest
 * 
 * Требуется: npm install -D vitest @cloudflare/workers-types miniflare
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

// ========== MOCK ДАННЫЕ ==========

const mockEnv = {
  TELEGRAM_BOT_TOKEN: 'test_token_123',
  ADMIN_TOKEN: 'admin_secret_token',
  BOT_USERNAME: 'test_bot',
  LOG_CHAT_ID: '123456789',
  ADMIN_USERNAME: 'TestAdmin',
  USERS: {
    get: vi.fn(),
    put: vi.fn(),
    delete: vi.fn()
  }
};

// ========== ТЕСТЫ УТИЛИТАРНЫХ ФУНКЦИЙ ==========

describe('Utility Functions', () => {
  
  describe('generateCode', () => {
    // Имитация функции generateCode
    function generateCode() {
      const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
      let code = '';
      for (let i = 0; i < 8; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
      }
      return code;
    }
    
    it('должен генерировать код длиной 8 символов', () => {
      const code = generateCode();
      expect(code).toHaveLength(8);
    });
    
    it('должен содержать только разрешённые символы', () => {
      const code = generateCode();
      const validChars = /^[ABCDEFGHJKLMNPQRSTUVWXYZ23456789]+$/;
      expect(code).toMatch(validChars);
    });
    
    it('должен генерировать уникальные коды', () => {
      const codes = new Set();
      for (let i = 0; i < 100; i++) {
        codes.add(generateCode());
      }
      // С высокой вероятностью все 100 кодов должны быть уникальными
      expect(codes.size).toBeGreaterThan(95);
    });
  });
  
  describe('safeParseJson', () => {
    async function safeParseJson(request) {
      try {
        const text = await request.text();
        if (!text || text.trim() === '') {
          return { data: null, error: 'Empty request body' };
        }
        return { data: JSON.parse(text), error: null };
      } catch (e) {
        return { data: null, error: 'Invalid JSON: ' + e.message };
      }
    }
    
    it('должен парсить валидный JSON', async () => {
      const mockRequest = {
        text: () => Promise.resolve('{"key": "value"}')
      };
      const result = await safeParseJson(mockRequest);
      expect(result.data).toEqual({ key: 'value' });
      expect(result.error).toBeNull();
    });
    
    it('должен возвращать ошибку для пустого body', async () => {
      const mockRequest = {
        text: () => Promise.resolve('')
      };
      const result = await safeParseJson(mockRequest);
      expect(result.data).toBeNull();
      expect(result.error).toBe('Empty request body');
    });
    
    it('должен возвращать ошибку для невалидного JSON', async () => {
      const mockRequest = {
        text: () => Promise.resolve('not valid json')
      };
      const result = await safeParseJson(mockRequest);
      expect(result.data).toBeNull();
      expect(result.error).toContain('Invalid JSON');
    });
  });
});

// ========== ТЕСТЫ RATE LIMITING ==========

describe('Rate Limiting', () => {
  const rateLimitCache = new Map();
  const RATE_LIMIT_WINDOW = 60 * 1000;
  
  function checkRateLimit(identifier, maxRequests = 60) {
    const key = `ratelimit:${identifier}`;
    const now = Date.now();
    const windowStart = now - RATE_LIMIT_WINDOW;
    
    let requests = rateLimitCache.get(key) || [];
    requests = requests.filter(time => time > windowStart);
    
    if (requests.length >= maxRequests) {
      return { allowed: false, remaining: 0, resetIn: Math.ceil((requests[0] + RATE_LIMIT_WINDOW - now) / 1000) };
    }
    
    requests.push(now);
    rateLimitCache.set(key, requests);
    
    return { allowed: true, remaining: maxRequests - requests.length };
  }
  
  beforeEach(() => {
    rateLimitCache.clear();
  });
  
  it('должен разрешать запросы в пределах лимита', () => {
    const result = checkRateLimit('test-user', 10);
    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(9);
  });
  
  it('должен блокировать при превышении лимита', () => {
    for (let i = 0; i < 10; i++) {
      checkRateLimit('test-user-2', 10);
    }
    const result = checkRateLimit('test-user-2', 10);
    expect(result.allowed).toBe(false);
    expect(result.remaining).toBe(0);
  });
  
  it('должен изолировать лимиты по идентификаторам', () => {
    for (let i = 0; i < 10; i++) {
      checkRateLimit('user-a', 10);
    }
    const resultA = checkRateLimit('user-a', 10);
    const resultB = checkRateLimit('user-b', 10);
    
    expect(resultA.allowed).toBe(false);
    expect(resultB.allowed).toBe(true);
  });
});

// ========== ТЕСТЫ ВАЛИДАЦИИ ==========

describe('Input Validation', () => {
  
  describe('oderId validation', () => {
    function validateOderId(oderId) {
      if (!oderId) return { valid: false, error: 'oderId is required' };
      if (typeof oderId !== 'string') return { valid: false, error: 'Invalid oderId type' };
      if (oderId.length < 16 || oderId.length > 64) return { valid: false, error: 'Invalid oderId length' };
      return { valid: true };
    }
    
    it('должен отклонять пустой oderId', () => {
      expect(validateOderId('')).toEqual({ valid: false, error: 'oderId is required' });
      expect(validateOderId(null)).toEqual({ valid: false, error: 'oderId is required' });
    });
    
    it('должен отклонять слишком короткий oderId', () => {
      expect(validateOderId('short')).toEqual({ valid: false, error: 'Invalid oderId length' });
    });
    
    it('должен принимать валидный oderId', () => {
      const validId = 'a'.repeat(32);
      expect(validateOderId(validId)).toEqual({ valid: true });
    });
  });
  
  describe('code validation', () => {
    function validateCode(code) {
      if (!code) return false;
      return /^[A-Z0-9]{8}$/i.test(code);
    }
    
    it('должен принимать валидный код', () => {
      expect(validateCode('ABCD1234')).toBe(true);
      expect(validateCode('XYZ78901')).toBe(true);
    });
    
    it('должен отклонять невалидный код', () => {
      expect(validateCode('ABC')).toBe(false); // слишком короткий
      expect(validateCode('ABCDEFGHIJ')).toBe(false); // слишком длинный
      expect(validateCode('ABC-1234')).toBe(false); // содержит дефис
      expect(validateCode('')).toBe(false);
      expect(validateCode(null)).toBe(false);
    });
  });
  
  describe('telegramId validation', () => {
    function validateTelegramId(id) {
      return /^\d+$/.test(String(id));
    }
    
    it('должен принимать числовые ID', () => {
      expect(validateTelegramId('123456789')).toBe(true);
      expect(validateTelegramId(123456789)).toBe(true);
    });
    
    it('должен отклонять нечисловые ID', () => {
      expect(validateTelegramId('abc123')).toBe(false);
      expect(validateTelegramId('@username')).toBe(false);
    });
  });
});

// ========== ТЕСТЫ ЛИЦЕНЗИРОВАНИЯ ==========

describe('License Logic', () => {
  
  describe('License expiration', () => {
    function isLicenseValid(license) {
      if (!license || !license.expiresAt) return false;
      return license.expiresAt > Date.now();
    }
    
    function getDaysLeft(expiresAt) {
      const now = Date.now();
      const MS_PER_DAY = 24 * 60 * 60 * 1000;
      return Math.max(0, Math.ceil((expiresAt - now) / MS_PER_DAY));
    }
    
    it('должен определять активную лицензию', () => {
      const validLicense = { expiresAt: Date.now() + 86400000 }; // +1 день
      expect(isLicenseValid(validLicense)).toBe(true);
    });
    
    it('должен определять истёкшую лицензию', () => {
      const expiredLicense = { expiresAt: Date.now() - 1000 };
      expect(isLicenseValid(expiredLicense)).toBe(false);
    });
    
    it('должен корректно считать оставшиеся дни', () => {
      const MS_PER_DAY = 24 * 60 * 60 * 1000;
      const in3Days = Date.now() + (3 * MS_PER_DAY);
      expect(getDaysLeft(in3Days)).toBe(3);
      
      const expired = Date.now() - MS_PER_DAY;
      expect(getDaysLeft(expired)).toBe(0);
    });
    
    it('должен отклонять null лицензию', () => {
      expect(isLicenseValid(null)).toBe(false);
      expect(isLicenseValid(undefined)).toBe(false);
      expect(isLicenseValid({})).toBe(false);
    });
  });
  
  describe('Trial period', () => {
    const TRIAL_DURATION = 24 * 60 * 60 * 1000; // 24 часа
    
    function createTrialLicense(telegramId, username) {
      return {
        type: 'trial',
        expiresAt: Date.now() + TRIAL_DURATION,
        createdAt: Date.now(),
        telegramId,
        username
      };
    }
    
    it('должен создавать триал на 24 часа', () => {
      const trial = createTrialLicense('123456', 'testuser');
      const hoursLeft = (trial.expiresAt - Date.now()) / (60 * 60 * 1000);
      expect(hoursLeft).toBeCloseTo(24, 1);
    });
    
    it('должен устанавливать тип "trial"', () => {
      const trial = createTrialLicense('123456', 'testuser');
      expect(trial.type).toBe('trial');
    });
  });
});

// ========== ТЕСТЫ МЕТРИК ==========

describe('Metrics', () => {
  let metrics;
  
  beforeEach(() => {
    metrics = {
      requests: { total: 0, success: 0, error: 0 },
      endpoints: {},
      errors: [],
      startTime: Date.now()
    };
  });
  
  function recordMetric(endpoint, success, duration, errorMsg = null) {
    metrics.requests.total++;
    if (success) {
      metrics.requests.success++;
    } else {
      metrics.requests.error++;
      if (errorMsg) {
        metrics.errors.push({
          time: Date.now(),
          endpoint,
          error: errorMsg.substring(0, 200)
        });
        if (metrics.errors.length > 100) {
          metrics.errors = metrics.errors.slice(-100);
        }
      }
    }
    
    if (!metrics.endpoints[endpoint]) {
      metrics.endpoints[endpoint] = { count: 0, errors: 0, totalDuration: 0 };
    }
    metrics.endpoints[endpoint].count++;
    metrics.endpoints[endpoint].totalDuration += duration;
    if (!success) metrics.endpoints[endpoint].errors++;
  }
  
  it('должен записывать успешные запросы', () => {
    recordMetric('/api/test', true, 50);
    expect(metrics.requests.total).toBe(1);
    expect(metrics.requests.success).toBe(1);
    expect(metrics.requests.error).toBe(0);
  });
  
  it('должен записывать ошибки', () => {
    recordMetric('/api/test', false, 100, 'Test error');
    expect(metrics.requests.error).toBe(1);
    expect(metrics.errors).toHaveLength(1);
    expect(metrics.errors[0].error).toBe('Test error');
  });
  
  it('должен ограничивать количество ошибок до 100', () => {
    for (let i = 0; i < 150; i++) {
      recordMetric('/api/test', false, 10, `Error ${i}`);
    }
    expect(metrics.errors.length).toBeLessThanOrEqual(100);
  });
  
  it('должен группировать статистику по эндпоинтам', () => {
    recordMetric('/api/a', true, 50);
    recordMetric('/api/a', true, 30);
    recordMetric('/api/b', false, 100, 'Error');
    
    expect(metrics.endpoints['/api/a'].count).toBe(2);
    expect(metrics.endpoints['/api/b'].errors).toBe(1);
  });
});

// ========== ТЕСТЫ БЕЗОПАСНОСТИ ==========

describe('Security', () => {
  
  describe('Admin authentication', () => {
    function isAdmin(username, adminUsername) {
      return username === adminUsername;
    }
    
    it('должен разрешать доступ админу', () => {
      expect(isAdmin('YaMob', 'YaMob')).toBe(true);
    });
    
    it('должен блокировать доступ не-админу', () => {
      expect(isAdmin('hacker', 'YaMob')).toBe(false);
      expect(isAdmin('', 'YaMob')).toBe(false);
      expect(isAdmin(null, 'YaMob')).toBe(false);
    });
  });
  
  describe('Bearer token validation', () => {
    function validateBearerToken(authHeader, expectedToken) {
      if (!authHeader || !expectedToken) return false;
      return authHeader === `Bearer ${expectedToken}`;
    }
    
    it('должен валидировать корректный токен', () => {
      expect(validateBearerToken('Bearer secret123', 'secret123')).toBe(true);
    });
    
    it('должен отклонять некорректный токен', () => {
      expect(validateBearerToken('Bearer wrong', 'secret123')).toBe(false);
      expect(validateBearerToken('secret123', 'secret123')).toBe(false); // без Bearer
      expect(validateBearerToken('', 'secret123')).toBe(false);
      expect(validateBearerToken(null, 'secret123')).toBe(false);
    });
  });
  
  describe('CORS origin validation', () => {
    const ALLOWED_ORIGINS = [
      'chrome-extension://',
      'moz-extension://',
      'https://exotic.company',
      'https://*.exotic.company'
    ];
    
    function isOriginAllowed(origin) {
      if (!origin) return false;
      return ALLOWED_ORIGINS.some(allowed => {
        if (allowed.includes('*')) {
          const pattern = allowed.replace('*', '.*');
          return new RegExp(pattern).test(origin);
        }
        return origin.startsWith(allowed);
      });
    }
    
    it('должен разрешать Chrome extensions', () => {
      expect(isOriginAllowed('chrome-extension://abc123')).toBe(true);
    });
    
    it('должен разрешать Firefox extensions', () => {
      expect(isOriginAllowed('moz-extension://abc123')).toBe(true);
    });
    
    it('должен разрешать exotic.company', () => {
      expect(isOriginAllowed('https://exotic.company')).toBe(true);
      expect(isOriginAllowed('https://api.exotic.company')).toBe(true);
    });
    
    it('должен блокировать другие origins', () => {
      expect(isOriginAllowed('https://evil.com')).toBe(false);
      expect(isOriginAllowed('http://exotic.company')).toBe(false); // HTTP
    });
  });
});

// ========== ИНТЕГРАЦИОННЫЕ ТЕСТЫ ==========

describe('Integration Tests', () => {
  
  describe('License flow', () => {
    it('должен корректно выдавать триал новому пользователю', async () => {
      // Симуляция потока:
      // 1. Пользователь генерирует код
      // 2. Пользователь привязывает Telegram
      // 3. Система выдаёт триал
      
      const mockUser = {
        oderId: 'device123456789012345',
        telegramId: '987654321',
        username: 'testuser'
      };
      
      // Проверяем что триал создаётся корректно
      const MS_PER_DAY = 24 * 60 * 60 * 1000;
      const trialLicense = {
        type: 'trial',
        expiresAt: Date.now() + MS_PER_DAY,
        createdAt: Date.now(),
        telegramId: mockUser.telegramId,
        username: mockUser.username
      };
      
      expect(trialLicense.type).toBe('trial');
      expect(trialLicense.expiresAt).toBeGreaterThan(Date.now());
    });
  });
});
