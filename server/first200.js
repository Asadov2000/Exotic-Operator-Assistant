// ========== КОНСТАНТЫ ==========

// Версия бота и расширения
const BOT_VERSION = '5.0';
const EXTENSION_VERSION = '5.0';

// ========== СИСТЕМА МЕТРИК ==========
const metrics = {
  requests: { total: 0, success: 0, error: 0 },
  endpoints: {},
  errors: [],
  startTime: Date.now(),
  lastReset: Date.now()
};

// ========== КЭШИРОВАНИЕ ==========
const cache = new Map();
const CACHE_TTL = 60 * 1000; // 1 минута

function getCached(key) {
  const item = cache.get(key);
  if (!item) return null;
  if (Date.now() > item.expires) {
    cache.delete(key);
    return null;
  }
  return item.value;
}

function setCache(key, value, ttl = CACHE_TTL) {
  cache.set(key, { value, expires: Date.now() + ttl });
  // Очистка старых записей
  if (cache.size > 5000) {
    const now = Date.now();
    for (const [k, v] of cache) {
      if (v.expires < now) cache.delete(k);
    }
  }
}

// ========== RATE LIMITING ДЛЯ БОТА ==========
const botRateLimits = new Map();
const BOT_RATE_LIMIT = {
  messages: 20,      // сообщений в минуту на пользователя
  callbacks: 30,     // callback'ов в минуту на пользователя
  window: 60 * 1000  // 1 минута
};

function checkBotRateLimit(telegramId, type = 'messages') {
  const key = `${telegramId}:${type}`;
  const now = Date.now();
  const windowStart = now - BOT_RATE_LIMIT.window;
  
  let requests = botRateLimits.get(key) || [];
  requests = requests.filter(time => time > windowStart);
  
  const limit = type === 'callbacks' ? BOT_RATE_LIMIT.callbacks : BOT_RATE_LIMIT.messages;
  
  if (requests.length >= limit) {
    return { 
      allowed: false, 
      remaining: 0, 
      resetIn: Math.ceil((requests[0] + BOT_RATE_LIMIT.window - now) / 1000),
      message: `⏳ Слишком много запросов. Подождите ${Math.ceil((requests[0] + BOT_RATE_LIMIT.window - now) / 1000)} сек.`
    };
  }
  
  requests.push(now);
  botRateLimits.set(key, requests);
  
  // Очистка
  if (botRateLimits.size > 10000) {
    const entries = Array.from(botRateLimits.entries());
    entries.slice(0, 5000).forEach(([k]) => botRateLimits.delete(k));
  }
  
  return { allowed: true, remaining: limit - requests.length };
}

// ========== СИСТЕМА УВЕДОМЛЕНИЙ ОБ ИСТЕЧЕНИИ ==========
const EXPIRATION_NOTIFICATIONS = {
  3: { sent: false, emoji: '⚠️', text: '3 дня' },
  1: { sent: false, emoji: '🔴', text: '1 день' },
  0: { sent: false, emoji: '❌', text: 'сегодня' }
};

// Функция для отправки уведомлений об истечении подписки
async function sendExpirationNotification(env, telegramId, daysLeft, licenseType) {
  const notifKey = `expiry_notif:${telegramId}:${daysLeft}`;
  
  // Проверяем, не отправляли ли уже
  const alreadySent = await env.USERS.get(notifKey);
  if (alreadySent) return false;
  
  // Проверяем настройки пользователя
  const userSettings = await getUserSettings(env, telegramId);
  if (!userSettings.notifications.expiryReminders) {
    return false; // Напоминания отключены
  }
  
  const typeText = LICENSE_TYPES[licenseType]?.name || 'Подписка';
  
  let message;
  if (daysLeft === 3) {
    message = `⚠️ *Напоминание*\n\n` +
      `Ваша подписка *${typeText}* истекает через *3 дня*.\n\n` +
      `Продлите заранее, чтобы автокликер работал без перерывов!`;
  } else if (daysLeft === 1) {
    message = `🔴 *Внимание!*\n\n` +
      `Ваша подписка *${typeText}* истекает *завтра*!\n\n` +
      `Не забудьте продлить, иначе автокликер остановится.`;
  } else if (daysLeft === 0) {
    message = `❌ *Подписка истекает сегодня!*\n\n` +
      `Ваша подписка *${typeText}* заканчивается сегодня.\n\n` +
      `Продлите сейчас, чтобы не потерять доступ к автокликеру!`;
  }
  
  if (message) {
    await sendTelegramMessage(env, telegramId, message, {
      reply_markup: {
        inline_keyboard: [
          [{ text: '💳 Продлить подписку', callback_data: 'buy' }],
          [{ text: '💬 Написать @YaMob', url: 'https://t.me/YaMob' }]
        ]
      }
    });
    
    // Помечаем что уведомление отправлено (TTL 7 дней)
    await env.USERS.put(notifKey, 'sent', { expirationTtl: 7 * 24 * 60 * 60 });
    
    return true;
  }
  
  return false;
}

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
      // Храним только последние 100 ошибок
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

// Структурированное логирование для внешних сервисов (Sentry-like)
function structuredLog(level, message, context = {}) {
  const logEntry = {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...context,
    env: 'production'
  };
  
  // Console для Cloudflare Workers Logs
  if (level === 'error') {
    console.error(JSON.stringify(logEntry));
  } else if (level === 'warn') {
    console.warn(JSON.stringify(logEntry));
  } else {
    console.log(JSON.stringify(logEntry));
  }
  
  return logEntry;
}

// Разрешённые origins для CORS (добавьте домены расширения)
const ALLOWED_ORIGINS = [
  'chrome-extension://',
  'moz-extension://',
  'https://exotic.company',
  'https://*.exotic.company'
];

function getCorsHeaders(origin) {
  // Проверяем, разрешён ли origin
  const isAllowed = ALLOWED_ORIGINS.some(allowed => {
    if (allowed.includes('*')) {
      const pattern = allowed.replace('*', '.*');
      return new RegExp(pattern).test(origin);
    }
export default {};