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
    return origin?.startsWith(allowed);
  });
  
  return {
    'Access-Control-Allow-Origin': isAllowed ? origin : ALLOWED_ORIGINS[0],
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
  };
}

// CORS headers (безопасная версия - используем getCorsHeaders() с проверкой origin)
// Для внутренних запросов где origin не важен
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': 'https://exotic.company',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

// Временные константы
const MS_PER_MINUTE = 60 * 1000;
const MS_PER_HOUR = 60 * MS_PER_MINUTE;
const MS_PER_DAY = 24 * MS_PER_HOUR;
const CODE_EXPIRATION_TTL = 1800; // 30 минут

// Пробный период
const TRIAL_DURATION_NO_REFERRAL = 12 * MS_PER_HOUR; // 12 часов для новых без реферала
const TRIAL_DURATION_WITH_REFERRAL = MS_PER_DAY; // 24 часа для приглашённых по реферальной ссылке
const TRIAL_DURATION = TRIAL_DURATION_NO_REFERRAL; // По умолчанию 12 часов

// Реферальная система
const REFERRAL_BONUS_HOURS = 24; // Бонус часов рефереру когда реферал оформит подписку
const REFERRAL_TRIAL_HOURS = 24; // Пробный период для приглашённого (24 часа)
const REFERRAL_CODE_PREFIX = 'REF_';

// ========== СКРЫТАЯ АДМИН-ПАНЕЛЬ ==========
// Секретная фраза для доступа к админке берётся из env.ADMIN_SECRET_PHRASE
// Добавьте через: wrangler secret put ADMIN_SECRET_PHRASE
// Сессии админов хранятся 1 час
const ADMIN_SESSION_TTL = 60 * 60; // 1 час в секундах

// Проверка админского доступа (по telegramId, не по username для безопасности)
async function isAdminById(env, telegramId) {
  const adminId = env.ADMIN_TELEGRAM_ID;
  return adminId && String(telegramId) === String(adminId);
}

// ========== СИСТЕМА КОНТРОЛЯ УСТРОЙСТВ ==========
// Level 1/2 = 1 устройство, Level 3 = 2 устройства
// Устройства идентифицируются по deviceId (oderId)

// Получить список устройств пользователя
async function getUserDevices(env, telegramId) {
  const data = await env.USERS.get(`devices:${telegramId}`);
  if (!data) return { devices: [], lastUpdated: null };
  return JSON.parse(data);
}

// Сохранить список устройств
async function saveUserDevices(env, telegramId, devicesData) {
  devicesData.lastUpdated = Date.now();
  await env.USERS.put(`devices:${telegramId}`, JSON.stringify(devicesData));
}

// Проверить и зарегистрировать устройство
async function checkAndRegisterDevice(env, telegramId, deviceId, license) {
  if (!deviceId) {
    return { allowed: true, reason: 'NO_DEVICE_ID' }; // Для совместимости со старыми версиями
  }
  
  const licenseType = LICENSE_TYPES[license?.type] || LICENSE_TYPES.trial;
  const maxDevices = licenseType.maxDevices || 1;
  
  const devicesData = await getUserDevices(env, telegramId);
  const devices = devicesData.devices || [];
  
  // Проверяем, есть ли уже это устройство
  const existingIndex = devices.findIndex(d => d.deviceId === deviceId);
  if (existingIndex >= 0) {
    // Устройство уже зарегистрировано - обновляем lastSeen
    devices[existingIndex].lastSeen = Date.now();
    await saveUserDevices(env, telegramId, { devices });
    return { allowed: true, reason: 'DEVICE_EXISTS', deviceCount: devices.length, maxDevices };
  }
  
  // Устройство новое - проверяем лимит
  if (devices.length >= maxDevices) {
    // Лимит достигнут
    return { 
      allowed: false, 
      reason: 'DEVICE_LIMIT', 
      deviceCount: devices.length, 
      maxDevices,
      currentDevices: devices.map(d => ({
        deviceId: d.deviceId.substring(0, 8) + '...',
        lastSeen: d.lastSeen,
        addedAt: d.addedAt
      }))
    };
  }
  
  // Регистрируем новое устройство
  devices.push({
    deviceId,
    addedAt: Date.now(),
    lastSeen: Date.now()
  });
  await saveUserDevices(env, telegramId, { devices });
  
  return { 
    allowed: true, 
    reason: 'DEVICE_REGISTERED', 
    deviceCount: devices.length, 
    maxDevices 
  };
}

// Удалить устройство (для отвязки)
async function removeDevice(env, telegramId, deviceId) {
  const devicesData = await getUserDevices(env, telegramId);
  const devices = devicesData.devices || [];
  
  const newDevices = devices.filter(d => d.deviceId !== deviceId);
  if (newDevices.length === devices.length) {
    return { success: false, reason: 'DEVICE_NOT_FOUND' };
  }
  
  await saveUserDevices(env, telegramId, { devices: newDevices });
  return { success: true, removedDeviceId: deviceId };
}

// Очистить все устройства пользователя
async function clearAllDevices(env, telegramId) {
  await saveUserDevices(env, telegramId, { devices: [] });
  return { success: true };
}

// Проверка активной админ-сессии
async function hasAdminSession(env, telegramId) {
  const sessionKey = `admin_session:${telegramId}`;
  const session = await env.USERS.get(sessionKey);
  return !!session;
}

// Создать админ-сессию
async function createAdminSession(env, telegramId) {
  const sessionKey = `admin_session:${telegramId}`;
  const sessionData = {
    createdAt: Date.now(),
    telegramId: String(telegramId)
  };
  await env.USERS.put(sessionKey, JSON.stringify(sessionData), { expirationTtl: ADMIN_SESSION_TTL });
  return true;
}

// Удалить админ-сессию (выход)
async function destroyAdminSession(env, telegramId) {
  const sessionKey = `admin_session:${telegramId}`;
  await env.USERS.delete(sessionKey);
}

// Проверить и продлить сессию админа (возвращает true если активна)
async function checkAndExtendAdminSession(env, telegramId) {
  const hasSession = await hasAdminSession(env, telegramId);
  if (hasSession) {
    // Продлеваем сессию при каждой активности
    await createAdminSession(env, telegramId);
    return true;
  }
  return false;
}

// Показать сообщение об истечении сессии
async function showSessionExpiredMessage(env, chatId, messageId) {
  await editTelegramMessage(env, chatId, messageId,
    `🔒 *Сессия истекла*\n\n` +
    `Для продолжения работы введите\nсекретную фразу заново.`,
    { reply_markup: { inline_keyboard: [[{ text: '← Меню', callback_data: 'menu' }]] }}
  );
}

// Получить все pending группы
async function getAllPendingGroups(env) {
  const groups = [];
  try {
    const list = await env.USERS.list({ prefix: 'pending_group:' });
    for (const key of list.keys) {
      const data = await env.USERS.get(key.name);
      if (data) {
        const group = JSON.parse(data);
        group.chatId = key.name.replace('pending_group:', '');
        groups.push(group);
      }
    }
  } catch (e) {
    console.error('Error listing pending groups:', e);
  }
  return groups;
}

// Получить статистику для админки
async function getAdminStats(env) {
  const stats = {
    totalUsers: 0,
    activeUsers: 0,
    trialUsers: 0,
    level1Users: 0,
    level2Users: 0,
    level3Users: 0,
    pendingGroups: 0,
    approvedGroups: 0,
    todayConnections: 0
  };
  
  try {
    // Подсчёт пользователей (выборочно для производительности)
    const usersList = await env.USERS.list({ prefix: 'user:', limit: 1000 });
    stats.totalUsers = usersList.keys.length;
    
    // Подсчёт лицензий по уровням
    const licenseList = await env.USERS.list({ prefix: 'license:tg:', limit: 1000 });
    const now = Date.now();
    
    for (const key of licenseList.keys) {
      const data = await env.USERS.get(key.name);
      if (data) {
        const license = JSON.parse(data);
        if (license.expiresAt > now) {
          stats.activeUsers++;
          // Считаем по типам
          switch (license.type) {
            case 'trial':
              stats.trialUsers++;
              break;
            case 'level1':
            case 'basic':
            case 'subscription':
              stats.level1Users++;
              break;
            case 'level2':
            case 'premium':
              stats.level2Users++;
              break;
            case 'level3':
            case 'max':
              stats.level3Users++;
              break;
            default:
              stats.level1Users++; // По умолчанию Level 1
          }
        }
      }
    }
    
    // Pending группы
    const pendingList = await env.USERS.list({ prefix: 'pending_group:' });
    stats.pendingGroups = pendingList.keys.length;
    
    // Одобренные группы
    const groupOwnersList = await env.USERS.list({ prefix: 'group_owner:' });
    stats.approvedGroups = groupOwnersList.keys.length;
    
  } catch (e) {
    console.error('Error getting admin stats:', e);
  }
  
  return stats;
}

// Получить список всех пользователей с лицензиями
async function getAllLicensedUsers(env, limit = 50) {
  const users = [];
  try {
    const list = await env.USERS.list({ prefix: 'license:tg:', limit });
    for (const key of list.keys) {
      const telegramId = key.name.replace('license:tg:', '');
      const data = await env.USERS.get(key.name);
      if (data) {
        const license = JSON.parse(data);
        users.push({
          telegramId,
          ...license,
          isActive: license.expiresAt > Date.now()
        });
      }
    }
  } catch (e) {
    console.error('Error listing users:', e);
  }
  return users.sort((a, b) => b.expiresAt - a.expiresAt);
}

// ========== ПОИСК ПОЛЬЗОВАТЕЛЯ ПО USERNAME ==========

// Кэш username -> telegramId (хранится в KV)
async function saveUsernameMapping(env, username, telegramId) {
  if (!username) return;
  const cleanUsername = username.toLowerCase().replace('@', '');
  await env.USERS.put(`username:${cleanUsername}`, String(telegramId));
}

// Найти telegramId по username
async function findTelegramIdByUsername(env, username) {
  if (!username) return null;
  const cleanUsername = username.toLowerCase().replace('@', '');
  
  // Сначала проверяем кэш
  const cachedId = await env.USERS.get(`username:${cleanUsername}`);
  if (cachedId) return cachedId;
  
  // Если не в кэше - ищем в user:* записях
  try {
    const usersList = await env.USERS.list({ prefix: 'user:', limit: 1000 });
    for (const key of usersList.keys) {
      const data = await env.USERS.get(key.name);
      if (data) {
        const user = JSON.parse(data);
        if (user.username && user.username.toLowerCase() === cleanUsername) {
          // Сохраняем в кэш для будущего
          await saveUsernameMapping(env, cleanUsername, user.telegramId);
          return user.telegramId;
        }
      }
    }
  } catch (e) {
    console.error('Error finding user by username:', e);
  }
  
  return null;
}

// Получить информацию о пользователе (по ID или username)
async function getUserInfo(env, target) {
  const cleanTarget = target.replace('@', '');
  const isTelegramId = /^\d+$/.test(cleanTarget);
  
  let telegramId = isTelegramId ? cleanTarget : null;
  let username = isTelegramId ? null : cleanTarget;
  
  // Если это username - ищем telegramId
  if (!isTelegramId) {
    telegramId = await findTelegramIdByUsername(env, cleanTarget);
    if (!telegramId) {
      return { found: false, error: 'USER_NOT_FOUND', username: cleanTarget };
    }
  }
  
  // Получаем лицензию
  const licenseData = await env.USERS.get(`license:tg:${telegramId}`);
  const license = licenseData ? JSON.parse(licenseData) : null;
  
  // Получаем привязку (oderId)
  const oderId = await env.USERS.get(`tg:${telegramId}`);
  let userData = null;
  if (oderId) {
    const userDataRaw = await env.USERS.get(`user:${oderId}`);
    if (userDataRaw) {
      userData = JSON.parse(userDataRaw);
      username = userData.username || username;
    }
  }
  
  // Получаем устройства
  const devicesData = await getUserDevices(env, telegramId);
  
  return {
    found: true,
    telegramId,
    username,
    license,
    userData,
    devices: devicesData.devices || [],
    isConnected: !!oderId
  };
}

// Rate limiting константы
const RATE_LIMIT_WINDOW = 60 * 1000; // 1 минута
const RATE_LIMIT_MAX_REQUESTS = 60; // Максимум запросов в минуту

// Telegram IP ranges (для верификации webhook)
const TELEGRAM_SUBNETS = [
  '149.154.160.0/20',
  '91.108.4.0/22'
];

// Проверка IP в подсети (простая реализация)
function isIPInSubnet(ip, subnet) {
  if (!ip || !subnet) return false;
  const [subnetIP, bits] = subnet.split('/');
  const mask = ~((1 << (32 - parseInt(bits))) - 1);
  
  const ipToInt = (ip) => {
    const parts = ip.split('.').map(Number);
    return (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3];
  };
  
  return (ipToInt(ip) & mask) === (ipToInt(subnetIP) & mask);
}

function isFromTelegram(ip) {
  if (!ip) return false;
  return TELEGRAM_SUBNETS.some(subnet => isIPInSubnet(ip, subnet));
}

// Экранирование специальных символов Markdown для Telegram
function escapeMarkdown(text) {
  if (!text) return '';
  return String(text)
    .replace(/\\/g, '\\\\')
    .replace(/\*/g, '\\*')
    .replace(/_/g, '\\_')
    .replace(/\[/g, '\\[')
    .replace(/\]/g, '\\]')
    .replace(/`/g, '\\`');
}

// Rate limiter (простая реализация для KV)
const rateLimitCache = new Map();

async function checkRateLimit(env, identifier, maxRequests = RATE_LIMIT_MAX_REQUESTS) {
  const key = `ratelimit:${identifier}`;
  const now = Date.now();
  const windowStart = now - RATE_LIMIT_WINDOW;
  
  // Используем in-memory cache для быстрой проверки
  let requests = rateLimitCache.get(key) || [];
  requests = requests.filter(time => time > windowStart);
  
  if (requests.length >= maxRequests) {
    return { allowed: false, remaining: 0, resetIn: Math.ceil((requests[0] + RATE_LIMIT_WINDOW - now) / 1000) };
  }
  
  requests.push(now);
  rateLimitCache.set(key, requests);
  
  // Очистка старых записей
  if (rateLimitCache.size > 10000) {
    const entries = Array.from(rateLimitCache.entries());
    entries.slice(0, 5000).forEach(([k]) => rateLimitCache.delete(k));
  }
  
  return { allowed: true, remaining: maxRequests - requests.length };
}

// Безопасный парсинг JSON
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

// ========== СИСТЕМА ЛОГИРОВАНИЯ ==========

// Красивое логирование в Telegram чат
async function logToChat(env, message, level = 'info', extra = {}) {
  const botToken = env.TELEGRAM_BOT_TOKEN;
  const logChatId = env.LOG_CHAT_ID; // Теперь из переменных окружения
  
  if (!botToken || !logChatId) {
    console.log('[LOG] No bot token or log chat ID, skipping log:', message);
    return;
  }
  
  // Используем timestamp вместо глобального счетчика
  const logId = Date.now().toString(36).toUpperCase();
  
  const config = {
    info: { icon: 'ℹ️', title: 'ИНФО', color: '🔵' },
    warn: { icon: '⚠️', title: 'ВНИМАНИЕ', color: '🟡' },
    error: { icon: '❌', title: 'ОШИБКА', color: '🔴' },
    success: { icon: '✅', title: 'УСПЕХ', color: '🟢' },
    admin: { icon: '👑', title: 'АДМИН', color: '🟣' },
    user: { icon: '👤', title: 'ПОЛЬЗОВАТЕЛЬ', color: '🔵' },
    trial: { icon: '🎁', title: 'ПРОБНЫЙ ПЕРИОД', color: '🟢' },
    license: { icon: '💎', title: 'ЛИЦЕНЗИЯ', color: '🟣' },
    connect: { icon: '🔗', title: 'ПРИВЯЗКА', color: '🔵' },
    webhook: { icon: '🤖', title: 'WEBHOOK', color: '⚪' },
    system: { icon: '⚙️', title: 'СИСТЕМА', color: '⚪' }
  };
  
  const cfg = config[level] || config.info;
  
  const time = new Date().toLocaleString('ru-RU', { 
    timeZone: 'Europe/Moscow',
    day: '2-digit',
    month: '2-digit',
    year: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
  
  // Формируем красивое сообщение
  let text = `${cfg.color} ${cfg.icon} *${cfg.title}* #${logId}\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  text += `${message}\n\n`;
  
  // Добавляем дополнительные данные если есть
  if (extra.telegramId) {
    text += `🆔 TG ID: \`${extra.telegramId}\`\n`;
  }
  if (extra.username) {
    text += `👤 Username: @${extra.username}\n`;
  }
  if (extra.action) {
    text += `🎯 Действие: ${extra.action}\n`;
  }
  if (extra.details) {
    text += `📋 Детали: ${extra.details}\n`;
  }
  
  text += `\n━━━━━━━━━━━━━━━━━━━━\n`;
  text += `🕐 ${time} MSK`;
  
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    
    const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: logChatId,
        text: text,
        parse_mode: 'Markdown',
        disable_notification: level !== 'error'
      }),
      signal: controller.signal
    });
    
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      console.error('[LOG] Telegram API HTTP error:', response.status);
      return;
    }
    
    const result = await response.json();
    if (!result.ok) {
      console.error('[LOG] Telegram API error:', result.description);
    }
  } catch (e) {
    if (e.name === 'AbortError') {
      console.error('[LOG] Request timeout');
    } else {
      console.error('[LOG] Failed to send log:', e.message);
    }
  }
}

function jsonResponse(data, status = 200, origin = null) {
  const headers = {
    'Content-Type': 'application/json',
    'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'X-XSS-Protection': '1; mode=block',
    'Content-Security-Policy': "default-src 'none'; frame-ancestors 'none'",
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Permissions-Policy': 'geolocation=(), microphone=(), camera=()',
    ...(origin ? getCorsHeaders(origin) : CORS_HEADERS)
  };
  return new Response(JSON.stringify(data), { status, headers });
}

function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 8; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}

// Вспомогательная функция для получения данных лицензии
async function getLicenseData(env, oderId) {
  // Получаем данные пользователя по oderId
  const userData = await env.USERS.get(`user:${oderId}`);
  if (!userData) {
    return { connected: false, error: 'NOT_CONNECTED' };
  }
  
  const user = JSON.parse(userData);
  const telegramId = user.telegramId;
  const username = user.username;
  
  // Получаем лицензию по Telegram ID (более надёжно) или username
  let licenseData = null;
  
  // Сначала пробуем по telegramId
  if (telegramId) {
    const licenseByTgId = await env.USERS.get(`license:tg:${telegramId}`);
    if (licenseByTgId) {
      licenseData = JSON.parse(licenseByTgId);
    }
  }
  
  // Если не нашли, пробуем по username
  if (!licenseData && username) {
    const licenseByUsername = await env.USERS.get(`license:${username}`);
    if (licenseByUsername) {
      licenseData = JSON.parse(licenseByUsername);
    }
  }
  
  // Если нет лицензии - выдаём пробный период автоматически (12 часов для обычных)
  if (!licenseData) {
    // Проверяем, был ли уже выдан пробный период
    const trialUsed = await env.USERS.get(`trial:${telegramId || username}`);
    
    if (!trialUsed) {
      // Выдаём пробный период на 12 часов (без реферала)
      const trialExpiresAt = Date.now() + TRIAL_DURATION_NO_REFERRAL;
      const trialHours = Math.round(TRIAL_DURATION_NO_REFERRAL / MS_PER_HOUR);
      licenseData = {
        type: 'trial',
        expiresAt: trialExpiresAt,
        createdAt: Date.now(),
        telegramId: telegramId,
        username: username
      };
      
      // Сохраняем лицензию
      if (telegramId) {
        await env.USERS.put(`license:tg:${telegramId}`, JSON.stringify(licenseData));
      }
      if (username) {
        await env.USERS.put(`license:${username}`, JSON.stringify(licenseData));
      }
      
      // Отмечаем что пробный период использован
      await env.USERS.put(`trial:${telegramId || username}`, 'used');
      
      // Логируем выдачу пробного периода
      await logToChat(env, 
        `🎁 *Пробный период активирован!*\n\n` +
        `Новый пользователь получил ${trialHours} часов бесплатного доступа`,
        'trial',
        {
          telegramId: telegramId || 'нет',
          username: username || 'нет',
          action: 'Автоматическая выдача триала',
          details: `Срок: ${trialHours} часов`
        }
      );
    }
  }
  
  return {
    connected: true,
    user: user,
    license: licenseData
  };
}

async function handleLicenseCheck(request, env) {
  try {
    // Rate limiting по IP
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `license:${clientIP}`);
    if (!rateLimit.allowed) {
      return jsonResponse({ 
        valid: false, 
        error: 'RATE_LIMITED', 
        message: `Слишком много запросов. Повторите через ${rateLimit.resetIn}с` 
      }, 429);
    }
    
    const { data, error } = await safeParseJson(request);
    if (error) {
      return jsonResponse({ valid: false, error: 'INVALID_REQUEST', message: error }, 400);
    }
    
    const { oderId, deviceId } = data || {};
    
    if (!oderId) {
      return jsonResponse({ valid: false, error: 'oderId is required' });
    }
    
    const result = await getLicenseData(env, oderId);
    
    if (!result.connected) {
      return jsonResponse({ 
        valid: false, 
        error: result.error,
        message: 'Telegram не подключён' 
      });
    }
    
    const license = result.license;
    
    if (!license) {
      return jsonResponse({ 
        valid: false, 
        error: 'NO_LICENSE',
        message: 'Лицензия не найдена'
      });
    }
    
    const now = Date.now();
    const isValid = license.expiresAt > now;
    
    // Проверка лимита устройств (только для активной лицензии)
    if (isValid && deviceId && result.user.telegramId) {
      const deviceCheck = await checkAndRegisterDevice(env, result.user.telegramId, deviceId, license);
      if (!deviceCheck.allowed) {
        return jsonResponse({
          valid: false,
          error: 'DEVICE_LIMIT',
          message: `Достигнут лимит устройств (${deviceCheck.maxDevices}). Отвяжите другое устройство в боте.`,
          deviceCount: deviceCheck.deviceCount,
          maxDevices: deviceCheck.maxDevices,
          currentDevices: deviceCheck.currentDevices
        });
      }
    }
    
    const daysLeft = Math.max(0, Math.ceil((license.expiresAt - now) / MS_PER_DAY));
    const licenseType = LICENSE_TYPES[license.type] || LICENSE_TYPES.trial;
    
    return jsonResponse({
      valid: isValid,
      type: license.type || 'subscription',
      expiresAt: license.expiresAt,
      daysLeft: daysLeft,
      maxDevices: licenseType.maxDevices,
      error: isValid ? null : 'EXPIRED',
      username: result.user.username
    });
    
  } catch (error) {
    console.error('License check error:', error);
    return jsonResponse({ valid: false, error: 'SERVER_ERROR', message: error.message }, 500);
  }
}

async function handleGenerateCode(request, env) {
  try {
    // Rate limiting
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `code:${clientIP}`, 10); // 10 кодов в минуту макс
    if (!rateLimit.allowed) {
      return jsonResponse({ 
        error: 'RATE_LIMITED', 
        message: `Слишком много запросов. Повторите через ${rateLimit.resetIn}с` 
      }, 429);
    }
    
    const { data, error } = await safeParseJson(request);
    if (error) {
      return jsonResponse({ error: 'INVALID_REQUEST', message: error }, 400);
    }
    
    const { oderId, deviceId } = data || {};
    
    if (!oderId) {
      return jsonResponse({ error: 'oderId is required' }, 400);
    }
    
    // Валидация oderId
    if (typeof oderId !== 'string' || oderId.length < 16 || oderId.length > 64) {
      return jsonResponse({ error: 'Invalid oderId format' }, 400);
    }
    
    // ВАЖНО: Удаляем старую привязку при генерации нового кода
    // Это нужно чтобы при перепривязке к другому Telegram аккаунту
    // статус показывал "не подключено" до завершения новой привязки
    const oldUserData = await env.USERS.get(`user:${oderId}`);
    if (oldUserData) {
      const oldUser = JSON.parse(oldUserData);
      // Удаляем обратную связь tg -> oderId
      if (oldUser.telegramId) {
        await env.USERS.delete(`tg:${oldUser.telegramId}`);
      }
      // Удаляем привязку user
      await env.USERS.delete(`user:${oderId}`);
      
      // Логируем
      await logToChat(env,
        `🔄 *Сброс привязки*\n\n` +
        `Пользователь запросил новый код, старая привязка удалена`,
        'connect',
        {
          telegramId: oldUser.telegramId || 'нет',
          username: oldUser.username || 'нет',
          action: 'Генерация нового кода',
          details: 'Старая привязка удалена'
        }
      );
    }
    
    const code = generateCode();
    
    // Сохраняем код с данными для связки
    await env.USERS.put(`code:${code}`, JSON.stringify({ 
      oderId, 
      deviceId,
      createdAt: Date.now()
    }), {
      expirationTtl: CODE_EXPIRATION_TTL
    });
    
    const botUsername = env.BOT_USERNAME || 'clickuved_bot';
    const link = `https://t.me/${botUsername}?start=${code}`;
    
    return jsonResponse({ 
      success: true, 
      code,
      link,
      expiresIn: 1800
    });
    
  } catch (error) {
    console.error('Generate code error:', error);
    return jsonResponse({ error: 'SERVER_ERROR', message: error.message }, 500);
  }
}

async function handleStatus(request, env) {
  try {
    // Rate limiting
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `status:${clientIP}`);
    if (!rateLimit.allowed) {
      return jsonResponse({ error: 'RATE_LIMITED' }, 429);
    }
    
    const { data, error } = await safeParseJson(request);
    if (error) {
      return jsonResponse({ error: 'INVALID_REQUEST', message: error }, 400);
    }
    
    const { oderId } = data || {};
    
    if (!oderId) {
      return jsonResponse({ error: 'oderId is required' }, 400);
    }
    
    const result = await getLicenseData(env, oderId);
    
    if (!result.connected) {
      return jsonResponse({ connected: false });
    }
    
    const license = result.license;
    const now = Date.now();
    
    if (!license) {
      return jsonResponse({ 
        connected: true, 
        username: result.user.username,
        licensed: false
      });
    }
    
    const isValid = license.expiresAt > now;
    const daysLeft = Math.max(0, Math.ceil((license.expiresAt - now) / (24 * 60 * 60 * 1000)));
    
    return jsonResponse({ 
      connected: true, 
      username: result.user.username,
      telegramId: result.user.telegramId,
      licensed: isValid,
      licenseType: license.type,
      expiresAt: license.expiresAt,
      daysLeft: daysLeft
    });
    
  } catch (error) {
    console.error('Status check error:', error);
    return jsonResponse({ error: 'SERVER_ERROR', message: error.message }, 500);
  }
}

// Эндпоинт для привязки Telegram (вызывается ботом)
async function handleTelegramConnect(request, env) {
  try {
    // Rate limiting для подключений
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `connect:${clientIP}`, 20); // 20 подключений в минуту
    if (!rateLimit.allowed) {
      return jsonResponse({ error: 'RATE_LIMITED', message: `Слишком много запросов. Повторите через ${rateLimit.resetIn}с` }, 429);
    }
    
    const { data, error } = await safeParseJson(request);
    if (error) {
      return jsonResponse({ error: 'INVALID_REQUEST', message: error }, 400);
    }
    
    const { code, telegramId, username, firstName } = data || {};
    
    if (!code || !telegramId) {
      return jsonResponse({ error: 'code and telegramId are required' }, 400);
    }
    
    // Валидация кода (только буквы и цифры, 8 символов)
    if (!/^[A-Z0-9]{8}$/i.test(code)) {
      return jsonResponse({ error: 'Invalid code format' }, 400);
    }
    
    // Валидация telegramId (должен быть числом)
    if (!/^\d+$/.test(String(telegramId))) {
      return jsonResponse({ error: 'Invalid telegramId' }, 400);
    }
    
    // Получаем данные по коду
    const codeData = await env.USERS.get(`code:${code}`);
    if (!codeData) {
      return jsonResponse({ error: 'Code expired or invalid', code: 'INVALID_CODE' }, 400);
    }
    
    const { oderId, deviceId } = JSON.parse(codeData);
    
    // Сохраняем связку user -> telegram
    const userData = {
      oderId,
      deviceId,
      telegramId,
      username: username || null,
      firstName: firstName || null,
      connectedAt: Date.now()
    };
    
    await env.USERS.put(`user:${oderId}`, JSON.stringify(userData));
    
    // Также сохраняем обратную связь telegram -> oderId для быстрого поиска
    await env.USERS.put(`tg:${telegramId}`, oderId);
    
    // Удаляем использованный код
    await env.USERS.delete(`code:${code}`);
    
    // Проверяем/выдаём лицензию
    const result = await getLicenseData(env, oderId);
    
    return jsonResponse({ 
      success: true,
      message: 'Successfully connected',
      license: result.license ? {
        valid: result.license.expiresAt > Date.now(),
        type: result.license.type,
        daysLeft: Math.max(0, Math.ceil((result.license.expiresAt - Date.now()) / (24 * 60 * 60 * 1000)))
      } : null
    });
    
  } catch (error) {
    console.error('Telegram connect error:', error);
    return jsonResponse({ error: 'SERVER_ERROR', message: error.message }, 500);
  }
}

// Эндпоинт для выдачи/продления лицензии (админский)
async function handleGrantLicense(request, env) {
  try {
    const authHeader = request.headers.get('Authorization');
    const adminToken = env.ADMIN_TOKEN;
    
    if (!adminToken || authHeader !== `Bearer ${adminToken}`) {
      // Логируем попытку несанкционированного доступа
      const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
      console.warn(`Unauthorized admin access attempt from ${clientIP}`);
      return jsonResponse({ error: 'Unauthorized' }, 401);
    }
    
    const { data, error } = await safeParseJson(request);
    if (error) {
      return jsonResponse({ error: 'INVALID_REQUEST', message: error }, 400);
    }
    
    const { telegramId, username, days, type = 'subscription' } = data || {};
    
    if (!telegramId && !username) {
      return jsonResponse({ error: 'telegramId or username is required' }, 400);
    }
    
    // Валидация days
    if (!days || !Number.isInteger(days) || days <= 0 || days > 365) {
      return jsonResponse({ error: 'days must be positive integer (1-365)' }, 400);
    }
    
    // Получаем текущую лицензию если есть
    let currentLicense = null;
    if (telegramId) {
      const existing = await env.USERS.get(`license:tg:${telegramId}`);
      if (existing) currentLicense = JSON.parse(existing);
    }
    if (!currentLicense && username) {
      const existing = await env.USERS.get(`license:${username}`);
      if (existing) currentLicense = JSON.parse(existing);
    }
    
    // Вычисляем новую дату истечения
    let baseTime = Date.now();
    
    // Если есть активная лицензия - продлеваем от неё
    if (currentLicense && currentLicense.expiresAt > Date.now()) {
      baseTime = currentLicense.expiresAt;
    }
    
    const expiresAt = baseTime + days * 24 * 60 * 60 * 1000;
    
    const licenseData = {
      type: type,
      expiresAt: expiresAt,
      createdAt: currentLicense?.createdAt || Date.now(),
      updatedAt: Date.now(),
      telegramId: telegramId,
      username: username,
      grantedDays: days
    };
    
    // Сохраняем по обоим ключам
    if (telegramId) {
      await env.USERS.put(`license:tg:${telegramId}`, JSON.stringify(licenseData));
      // Добавляем в очередь напоминаний
      await addToReminderQueue(env, telegramId, expiresAt);
    }
    if (username) {
      await env.USERS.put(`license:${username}`, JSON.stringify(licenseData));
    }
    
    const daysLeft = Math.ceil((expiresAt - Date.now()) / (24 * 60 * 60 * 1000));
    
    return jsonResponse({
      success: true,
      license: {
        type: type,
        expiresAt: expiresAt,
        daysLeft: daysLeft
      },
      message: `License granted for ${days} days. Total days left: ${daysLeft}`
    });
    
  } catch (error) {
    console.error('Grant license error:', error);
    return jsonResponse({ error: 'SERVER_ERROR', message: error.message }, 500);
  }
}

// ========== СИСТЕМА ГРУППОВЫХ ЧАТОВ ==========

// Новая система уровней подписок:
// trial (Пробник) - 1 день, только ЛС
// level1 (Уровень 1) - только ЛС с ботом
// level2 (Уровень 2) - ЛС + общий чат, уведомления только в общий чат
// level3 (Уровень 3) - полный функционал, выбор куда уведомления

const LICENSE_TYPES = {
  trial: { 
    name: '🎁 Пробный', 
    level: 0,
    groupChats: false, 
    maxDevices: 1, 
    maxChats: 0,
    notifyOptions: ['dm'],  // Только ЛС
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: true,
      groupNotify: false,
      selectNotify: false
    },
    description: 'До 24ч • Уведомления в ЛС'
  },
  level1: { 
    name: '⭐ Уровень 1', 
    level: 1,
    groupChats: false, 
    maxDevices: 1, 
    maxChats: 0,
    notifyOptions: ['dm'],  // Только ЛС
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: true,
      groupNotify: false,
      selectNotify: false
    },
    description: '1 устройство • Уведомления в ЛС'
  },
  level2: { 
    name: '⭐⭐ Уровень 2', 
    level: 2,
    groupChats: true, 
    maxDevices: 1, 
    maxChats: 1,
    notifyOptions: ['group'],  // Только в общий чат
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: false,
      groupNotify: true,
      selectNotify: false
    },
    description: 'ЛС + общий чат • Уведомления в чат'
  },
  level3: { 
    name: '⭐⭐⭐ Уровень 3', 
    level: 3,
    groupChats: true, 
    maxDevices: 2, 
    maxChats: 1,
    notifyOptions: ['dm', 'group', 'both'],  // Выбор
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: true,
      groupNotify: true,
      selectNotify: true,
      priority: true
    },
    description: '2 устройства • Выбор уведомлений'
  }
};

// Алиасы для обратной совместимости
LICENSE_TYPES.basic = LICENSE_TYPES.level1;
LICENSE_TYPES.subscription = LICENSE_TYPES.level1;
LICENSE_TYPES.premium = LICENSE_TYPES.level2;
LICENSE_TYPES.max = LICENSE_TYPES.level3;

// ========== НАСТРОЙКИ УВЕДОМЛЕНИЙ ПОЛЬЗОВАТЕЛЯ В БОТЕ ==========

const DEFAULT_USER_SETTINGS = {
  notifications: {
    clicks: true,          // Уведомления о кликах (взятых заявках)
    expiryReminders: true, // Напоминания об истечении подписки
    news: true,            // Новости и обновления
    sound: true            // Звук уведомлений (в Telegram не работает, но для совместимости)
  },
  // Куда отправлять уведомления: 'dm' | 'group' | 'both'
  notifyDestination: 'dm',
  createdAt: null,
  updatedAt: null
};

// Получить настройки пользователя
async function getUserSettings(env, telegramId) {
  const data = await env.USERS.get(`settings:${telegramId}`);
  if (!data) {
    return { ...DEFAULT_USER_SETTINGS, createdAt: Date.now() };
  }
  const parsed = JSON.parse(data);
  return { 
    ...DEFAULT_USER_SETTINGS, 
    ...parsed,
    notifications: { ...DEFAULT_USER_SETTINGS.notifications, ...(parsed.notifications || {}) }
  };
}

// Сохранить настройки пользователя
async function saveUserSettings(env, telegramId, settings) {
  const current = await getUserSettings(env, telegramId);
  const updated = {
    ...current,
    ...settings,
    notifications: { ...current.notifications, ...(settings.notifications || {}) },
    updatedAt: Date.now()
  };
  await env.USERS.put(`settings:${telegramId}`, JSON.stringify(updated));
  return updated;
}

// Переключить настройку уведомлений
async function toggleNotificationSetting(env, telegramId, settingKey) {
  const settings = await getUserSettings(env, telegramId);
  if (settings.notifications.hasOwnProperty(settingKey)) {
    settings.notifications[settingKey] = !settings.notifications[settingKey];
    await saveUserSettings(env, telegramId, settings);
  }
  return settings;
}

// ========== ГРУППОВЫЕ ЧАТЫ ==========

// Получить настройки групповых чатов пользователя
async function getUserGroupChats(env, telegramId) {
  const data = await env.USERS.get(`groupchats:${telegramId}`);
  if (!data) return { chats: [], pending: [] };
  return JSON.parse(data);
}

// Сохранить настройки групповых чатов
async function saveUserGroupChats(env, telegramId, groupData) {
  await env.USERS.put(`groupchats:${telegramId}`, JSON.stringify(groupData));
}

// Проверить право на групповые чаты
async function canUseGroupChats(env, telegramId) {
  const oderId = await env.USERS.get(`tg:${telegramId}`);
  if (!oderId) return { allowed: false, reason: 'NOT_CONNECTED' };
  
  const result = await getLicenseData(env, oderId);
  if (!result.license || result.license.expiresAt < Date.now()) {
    return { allowed: false, reason: 'NO_LICENSE' };
  }
  
  const licenseType = LICENSE_TYPES[result.license.type] || LICENSE_TYPES.trial;
  if (!licenseType.groupChats) {
    return { allowed: false, reason: 'UPGRADE_REQUIRED', currentType: result.license.type };
  }
  
  const maxChats = licenseType.maxChats || 1;
  return { allowed: true, license: result.license, maxChats };
}

// Запрос на добавление бота в групповой чат (ожидает одобрения админа)
async function requestGroupChatApproval(env, telegramId, chatId, chatTitle, requestedBy) {
  const pendingKey = `pending_group:${chatId}`;
  
  // Проверяем нет ли уже запроса
  const existing = await env.USERS.get(pendingKey);
  if (existing) {
    return { success: false, error: 'REQUEST_PENDING' };
  }
  
  const requestData = {
    chatId,
    chatTitle,
    requestedBy: telegramId,
    requestedByUsername: requestedBy,
    requestedAt: Date.now(),
    status: 'pending'
  };
  
  await env.USERS.put(pendingKey, JSON.stringify(requestData), { expirationTtl: 7 * 24 * 60 * 60 }); // 7 дней
  
  // Получаем ID админа для прямого уведомления
  const adminUsername = env.ADMIN_USERNAME || 'YaMob';
  const adminTelegramId = env.ADMIN_TELEGRAM_ID; // Нужно добавить в переменные окружения
  
  const approvalMsg = `📨 *Запрос на групповой чат*\n\n` +
    `👤 От: @${requestedBy || telegramId}\n` +
    `🆔 TG ID: \`${telegramId}\`\n` +
    `💬 Чат: "${escapeMarkdown(chatTitle)}"\n` +
    `🆔 Chat ID: \`${chatId}\`\n\n` +
    `✅ Для одобрения:\n\`/approve\\_group ${chatId}\`\n\n` +
    `❌ Для отклонения:\n\`/reject\\_group ${chatId}\``;
  
  // Отправляем уведомление админу напрямую (если есть ADMIN_TELEGRAM_ID)
  if (adminTelegramId) {
    try {
      await sendTelegramMessage(env, adminTelegramId, approvalMsg, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: '✅ Одобрить', callback_data: `approve_group:${chatId}` },
              { text: '❌ Отклонить', callback_data: `reject_group:${chatId}` }
            ]
          ]
        }
      });
    } catch (e) {
      console.error('Failed to send approval notification to admin:', e);
    }
  }
  
  // Также отправляем в лог-чат (если настроен)
  await logToChat(env,
    `📨 *Запрос на групповой чат*\n\n` +
    `👤 От: @${requestedBy || telegramId}\n` +
    `💬 Чат: "${chatTitle}"\n` +
    `🆔 Chat ID: \`${chatId}\`\n\n` +
    `Используйте /approve_group ${chatId} для одобрения`,
    'admin',
    { telegramId, action: 'Запрос группы' }
  );
  
  return { success: true };
}

// Одобрить групповой чат (только админ)
async function approveGroupChat(env, chatId, approvedBy) {
  const pendingKey = `pending_group:${chatId}`;
  const pendingData = await env.USERS.get(pendingKey);
  
  if (!pendingData) {
    return { success: false, error: 'REQUEST_NOT_FOUND' };
  }
  
  const request = JSON.parse(pendingData);
  const telegramId = request.requestedBy;
  
  // Получаем текущие чаты пользователя
  const groupData = await getUserGroupChats(env, telegramId);
  
  // Добавляем чат
  groupData.chats.push({
    chatId: chatId,
    chatTitle: request.chatTitle,
    approvedAt: Date.now(),
    approvedBy: approvedBy
  });
  
  // Удаляем из pending
  groupData.pending = (groupData.pending || []).filter(p => p.chatId !== chatId);
  
  await saveUserGroupChats(env, telegramId, groupData);
  await env.USERS.delete(pendingKey);
  
  // Сохраняем обратную связь чат -> пользователь
  await env.USERS.put(`group_owner:${chatId}`, telegramId);
  
  return { success: true, telegramId };
}

// ========== СТАТИСТИКА ПОЛЬЗОВАТЕЛЯ ==========

// Получить статистику пользователя
async function getUserStats(env, telegramId) {
  const data = await env.USERS.get(`stats:${telegramId}`);
  if (!data) {
    return {
      totalClicks: 0,
      todayClicks: 0,
      lastClickDate: null,
      registeredAt: Date.now(),
      referralCode: null,
      referredBy: null,
      referralCount: 0
    };
  }
  return JSON.parse(data);
}

// Сохранить статистику пользователя
async function saveUserStats(env, telegramId, stats) {
  await env.USERS.put(`stats:${telegramId}`, JSON.stringify(stats));
}

// Записать клик пользователя
async function recordUserClick(env, telegramId) {
  const stats = await getUserStats(env, telegramId);
  const today = new Date().toISOString().slice(0, 10);
  
  // Если новый день - сбрасываем todayClicks
  if (stats.lastClickDate !== today) {
    stats.todayClicks = 0;
    stats.lastClickDate = today;
  }
  
  stats.totalClicks++;
  stats.todayClicks++;
  
  await saveUserStats(env, telegramId, stats);
  return stats;
}

// ========== РЕФЕРАЛЬНАЯ СИСТЕМА ==========

// Генерировать реферальный код
function generateReferralCode(telegramId) {
  const hash = String(telegramId).split('').reduce((a, b) => {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);
  return REFERRAL_CODE_PREFIX + Math.abs(hash).toString(36).toUpperCase().slice(0, 6);
}

// Получить реферальный код пользователя (создаёт если нет)
async function getUserReferralCode(env, telegramId) {
  let stats = await getUserStats(env, telegramId);
  
  if (!stats.referralCode) {
    stats.referralCode = generateReferralCode(telegramId);
    await saveUserStats(env, telegramId, stats);
  }
  
  // Сохраняем связь код -> telegramId
  await env.USERS.put(`refcode:${stats.referralCode}`, telegramId);
  
  return stats.referralCode;
}

// Применить реферальный код
async function applyReferralCode(env, newUserTelegramId, referralCode) {
  // Находим владельца кода
  const referrerTelegramId = await env.USERS.get(`refcode:${referralCode}`);
  if (!referrerTelegramId) {
    return { success: false, error: 'INVALID_CODE' };
  }
  
  if (referrerTelegramId === newUserTelegramId) {
    return { success: false, error: 'SELF_REFERRAL' };
  }
  
  // Проверяем, не использовал ли новый юзер уже реферальный код
  const newUserStats = await getUserStats(env, newUserTelegramId);
  if (newUserStats.referredBy) {
    return { success: false, error: 'ALREADY_REFERRED' };
  }
  
  // Записываем реферала (бонус НЕ даём - только когда оформит подписку)
  newUserStats.referredBy = referrerTelegramId;
  newUserStats.referralBonusPending = true; // Бонус ожидает оформления подписки
  await saveUserStats(env, newUserTelegramId, newUserStats);
  
  // Увеличиваем счётчик рефералов у реферера (переходов)
  const referrerStats = await getUserStats(env, referrerTelegramId);
  referrerStats.referralCount = (referrerStats.referralCount || 0) + 1;
  referrerStats.referralPendingBonus = (referrerStats.referralPendingBonus || 0) + 1; // Ожидающих бонусов
  await saveUserStats(env, referrerTelegramId, referrerStats);
  
  // НЕ даём бонус рефереру сразу - только при оформлении подписки
  
  // Даём пробный период приглашённому ТОЛЬКО если это новый пользователь без лицензии (24 часа)
  const newUserOderId = await env.USERS.get(`tg:${newUserTelegramId}`);
  let gaveTrialToNewUser = false;
  
  if (newUserOderId) {
    const newUserResult = await getLicenseData(env, newUserOderId);
    // Даём пробник только если НЕТ активной лицензии
    if (!newUserResult.license || newUserResult.license.expiresAt < Date.now()) {
      const trialLicense = {
        type: 'trial',
        active: true,
        expiresAt: Date.now() + REFERRAL_TRIAL_HOURS * MS_PER_HOUR, // 24 часа для приглашённых
        createdAt: Date.now(),
        updatedAt: Date.now(),
        features: LICENSE_TYPES.trial.features,
        referralBonus: true
      };
      await env.USERS.put(`license:tg:${newUserTelegramId}`, JSON.stringify(trialLicense));
      gaveTrialToNewUser = true;
    }
  }
  
  return { 
    success: true, 
    referrerTelegramId, 
    gaveTrialToNewUser,
    newUserTrialHours: gaveTrialToNewUser ? REFERRAL_TRIAL_HOURS : 0
  };
}

// Функция для выдачи бонуса рефереру когда реферал оформил подписку (+24 часа)
async function grantReferralBonusToReferrer(env, referralTelegramId) {
  const userStats = await getUserStats(env, referralTelegramId);
  
  // Проверяем есть ли реферер и ожидающий бонус
  if (!userStats.referredBy || !userStats.referralBonusPending) {
    return { success: false, reason: 'NO_PENDING_BONUS' };
  }
  
  const referrerTelegramId = userStats.referredBy;
  
  // Снимаем флаг ожидания
  userStats.referralBonusPending = false;
  await saveUserStats(env, referralTelegramId, userStats);
  
  // Уменьшаем счётчик ожидающих у реферера
  const referrerStats = await getUserStats(env, referrerTelegramId);
  referrerStats.referralPendingBonus = Math.max(0, (referrerStats.referralPendingBonus || 0) - 1);
  referrerStats.referralPaidBonus = (referrerStats.referralPaidBonus || 0) + 1; // Оплаченных
  await saveUserStats(env, referrerTelegramId, referrerStats);
  
  // Даём бонус рефереру (+24 часа)
  const oderId = await env.USERS.get(`tg:${referrerTelegramId}`);
  if (oderId) {
    const result = await getLicenseData(env, oderId);
    if (result.license) {
      const newExpiry = Math.max(result.license.expiresAt, Date.now()) + REFERRAL_BONUS_HOURS * MS_PER_HOUR;
      result.license.expiresAt = newExpiry;
      result.license.updatedAt = Date.now();
      await env.USERS.put(`license:tg:${referrerTelegramId}`, JSON.stringify(result.license));
    } else {
      // Если нет лицензии - даём пробник на 24 часа
      const trialLicense = {
        type: 'trial',
        active: true,
        expiresAt: Date.now() + REFERRAL_BONUS_HOURS * MS_PER_HOUR,
        createdAt: Date.now(),
        updatedAt: Date.now(),
        features: LICENSE_TYPES.trial.features,
        referralBonus: true
      };
      await env.USERS.put(`license:tg:${referrerTelegramId}`, JSON.stringify(trialLicense));
    }
  }
  
  return { success: true, referrerTelegramId, bonusHours: REFERRAL_BONUS_HOURS };
}

// ========== НАПОМИНАНИЯ ОБ ИСТЕЧЕНИИ ==========

// Проверить и отправить напоминания (вызывается по cron)
async function checkAndSendExpirationReminders(env) {
  // Эта функция вызывается через Cloudflare Cron Triggers
  // Для работы нужно настроить cron в wrangler.toml
  
  const remindDays = [3, 1, 0]; // За 3 дня, за 1 день, и в день истечения
  const now = Date.now();
  const botToken = env.TELEGRAM_BOT_TOKEN;
  
  if (!botToken) return { sent: 0 };
  
  let sentCount = 0;
  
  // Получаем список пользователей для напоминания
  // (в реальности нужен list operation для KV, но он платный)
  // Используем отслеживание через отдельный ключ
  
  const reminderQueue = await env.USERS.get('reminder_queue');
  if (!reminderQueue) return { sent: 0 };
  
  const queue = JSON.parse(reminderQueue);
  const newQueue = [];
  
  for (const item of queue) {
    const { telegramId, expiresAt, lastReminder, expiredNotified } = item;
    const daysLeft = Math.ceil((expiresAt - now) / MS_PER_DAY);
    
    // Проверяем истекла ли подписка и нужно ли уведомление
    if (expiresAt <= now && !expiredNotified) {
      // Подписка истекла - отправляем уведомление
      try {
        await sendTelegramMessage(env, telegramId, 
          `❌ *Подписка закончилась*\n\n` +
          `Ваша подписка истекла.\n` +
          `Автокликер больше не работает.\n\n` +
          `📩 Для продления свяжитесь со мной:\n` +
          `@YaMob`,
          {
            reply_markup: {
              inline_keyboard: [
                [{ text: '💬 Написать @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: '📊 Статус', callback_data: 'status' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          }
        );
        item.expiredNotified = true;
        sentCount++;
      } catch (e) {
        console.error('Expiration notification error:', e);
      }
      // Оставляем в очереди чтобы не отправлять повторно
      newQueue.push(item);
      continue;
    }
    
    // Проверяем нужно ли напоминание (до истечения)
    for (const reminderDay of remindDays) {
      if (daysLeft === reminderDay && reminderDay > 0) {
        const reminderKey = `${telegramId}_${reminderDay}`;
        if (lastReminder !== reminderKey) {
          // Отправляем напоминание
          try {
            await sendTelegramMessage(env, telegramId, 
              `⏰ *Напоминание о подписке*\n\n` +
              `Ваша подписка истекает через *${daysLeft}* ${getDaysWord(daysLeft)}!\n\n` +
              `Продлите сейчас, чтобы не потерять доступ к автокликеру.`,
              {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '💳 Продлить', callback_data: 'buy' }],
                    [{ text: '📊 Статус', callback_data: 'status' }]
                  ]
                }
              }
            );
            item.lastReminder = reminderKey;
            sentCount++;
          } catch (e) {
            console.error('Reminder error:', e);
          }
        }
        break;
      }
    }
    
    // Если подписка ещё не истекла или уже отправлено уведомление об истечении - оставляем
    // Удаляем только тех, у кого истекло и уведомление отправлено > 7 дней назад
    const expiredMoreThan7Days = expiresAt < (now - 7 * MS_PER_DAY);
    if (!expiredMoreThan7Days) {
      newQueue.push(item);
    }
  }
  
  await env.USERS.put('reminder_queue', JSON.stringify(newQueue));
  return { sent: sentCount };
}

// Добавить пользователя в очередь напоминаний
async function addToReminderQueue(env, telegramId, expiresAt) {
  let queue = [];
  const queueData = await env.USERS.get('reminder_queue');
  if (queueData) queue = JSON.parse(queueData);
  
  // Удаляем старую запись если есть
  queue = queue.filter(item => item.telegramId !== telegramId);
  
  // Добавляем новую
  queue.push({ telegramId, expiresAt, lastReminder: null });
  
  await env.USERS.put('reminder_queue', JSON.stringify(queue));
}

// Склонение слова "день"
function getDaysWord(days) {
  if (days === 1) return 'день';
  if (days >= 2 && days <= 4) return 'дня';
  return 'дней';
}

async function handleNotify(request, env) {
  try {
    // Rate limiting для уведомлений
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `notify:${clientIP}`, 30); // 30 уведомлений в минуту
    if (!rateLimit.allowed) {
      return jsonResponse({ error: 'RATE_LIMITED' }, 429);
    }
    
    const { data, error } = await safeParseJson(request);
    if (error) {
      return jsonResponse({ error: 'INVALID_REQUEST', message: error }, 400);
    }
    
    const { oderId, transaction } = data || {};
    
    if (!oderId) {
      return jsonResponse({ error: 'oderId is required' }, 400);
    }
    
    // Валидация oderId
    if (typeof oderId !== 'string' || oderId.length < 16 || oderId.length > 64) {
      return jsonResponse({ error: 'Invalid oderId format' }, 400);
    }
    
    // Валидация transaction (если передана)
    if (transaction && typeof transaction !== 'object') {
      return jsonResponse({ error: 'Invalid transaction format' }, 400);
    }
    
    const result = await getLicenseData(env, oderId);
    
    if (!result.connected) {
      return jsonResponse({ error: 'User not connected', code: 'NOT_CONNECTED' }, 404);
    }
    
    if (!result.license) {
      return jsonResponse({ error: 'No license', code: 'NO_LICENSE' }, 403);
    }
    
    if (result.license.expiresAt < Date.now()) {
      return jsonResponse({ error: 'License expired', code: 'EXPIRED' }, 403);
    }
    
    // Проверяем настройки пользователя - включены ли уведомления о заявках
    const telegramId = result.user.telegramId;
    const userSettings = await getUserSettings(env, telegramId);
    
    console.log(`[NOTIFY] User ${telegramId}, clicks enabled: ${userSettings.notifications.clicks}`);
    
    if (!userSettings.notifications.clicks) {
      // Уведомления отключены пользователем, но клик записываем
      if (telegramId) {
        await recordUserClick(env, telegramId);
      }
      return jsonResponse({ success: true, notificationsSent: 0, reason: 'NOTIFICATIONS_DISABLED' });
    }
    
    // Формируем сообщение с экранированием пользовательских данных
    let message = `✅ *Заявка взята в работу!*\n\n`;
    if (transaction) {
      if (transaction.transactionId) {
        // transactionId в бэктиках - не экранируем
        message += `🆔 *ID:* \`${String(transaction.transactionId).slice(0, 50)}\`\n`;
      }
      if (transaction.amount) {
        message += `💰 *Сумма:* ${escapeMarkdown(String(transaction.amount).slice(0, 30))}\n`;
      }
      if (transaction.method) {
        message += `💳 *Способ:* ${escapeMarkdown(String(transaction.method).slice(0, 50))}\n`;
      }
      if (transaction.created) {
        message += `📅 *Создана:* ${escapeMarkdown(String(transaction.created).slice(0, 30))}\n`;
      }
      if (transaction.requisites) {
        // requisites в бэктиках - не экранируем, но ограничиваем длину
        message += `🔢 *Реквизиты:* \`${String(transaction.requisites).slice(0, 50)}\`\n`;
      }
      if (transaction.bank) {
        message += `🏦 *Банк:* ${escapeMarkdown(String(transaction.bank).slice(0, 50))}\n`;
      }
    }
    
    const time = new Date().toLocaleString('ru-RU', { 
      timeZone: 'Europe/Moscow',
      hour: '2-digit', 
      minute: '2-digit',
      second: '2-digit'
    });
    message += `\n⏰ *Время:* ${time} (МСК)`;
    
    // Отправляем уведомление в Telegram
    const botToken = env.TELEGRAM_BOT_TOKEN;
    
    // Записываем клик в статистику
    if (telegramId) {
      await recordUserClick(env, telegramId);
    }
    
    let sentTo = [];
    
    if (botToken && telegramId) {
      const licenseType = LICENSE_TYPES[result.license.type];
      const canUseDm = licenseType ? licenseType.dmNotifications : true;
      const canUseGroup = licenseType ? licenseType.groupNotifications : false;
      
      // Определяем куда отправлять на основе уровня подписки и настроек
      // trial, level1: только в ЛС
      // level2: только в группу (если есть), иначе в ЛС
      // level3: по настройке notifyDestination
      
      let sendToDm = false;
      let sendToGroup = false;
      
      const licenseLevel = result.license.type;
      const notifyDest = userSettings.notifyDestination || 'dm';
      
      if (licenseLevel === 'trial' || licenseLevel === 'level1') {
        // Только ЛС
        sendToDm = true;
        sendToGroup = false;
      } else if (licenseLevel === 'level2') {
        // Только группа (если есть), иначе fallback в ЛС
        sendToGroup = true;
        sendToDm = false; // fallback будет ниже если нет группы
      } else if (licenseLevel === 'level3') {
        // По настройке пользователя
        if (notifyDest === 'dm') {
          sendToDm = true;
        } else if (notifyDest === 'group') {
          sendToGroup = true;
        } else if (notifyDest === 'both') {
          sendToDm = true;
          sendToGroup = true;
        }
      } else {
        // Для других/старых типов - по умолчанию в ЛС
        sendToDm = canUseDm;
        sendToGroup = canUseGroup;
      }
      
      // Отправляем в ЛС если нужно
      if (sendToDm) {
        try {
          const personalResult = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: telegramId,
              text: message,
              parse_mode: 'Markdown'
            })
          });
          
          if (personalResult.ok) {
            sentTo.push('personal');
          } else {
            const errorData = await personalResult.json();
            console.error('Personal message error:', errorData.description);
          }
        } catch (tgError) {
          console.error('Telegram personal send error:', tgError);
        }
      }
      
      // Отправляем в групповые чаты если нужно
      if (sendToGroup && canUseGroup) {
        const groupData = await getUserGroupChats(env, telegramId);
        
        if (groupData.chats && groupData.chats.length > 0) {
          // Добавляем имя пользователя в сообщение для групп
          const username = result.user.username;
          const groupMessage = `👤 *${username ? '@' + username : 'Пользователь'}*\n\n${message}`;
          
          for (const chat of groupData.chats) {
            try {
              const groupResult = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  chat_id: chat.chatId,
                  text: groupMessage,
                  parse_mode: 'Markdown'
export default {};