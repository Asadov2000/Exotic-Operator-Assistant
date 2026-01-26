// ========== КОНСТАНТЫ ==========

// Версия бота и расширения
const BOT_VERSION = '5.0';
const EXTENSION_VERSION = '5.0';

// Глобальный статус бота (можно выключить через админ-команды)
let BOT_ENABLED = true;

// Белый список чатов (автоматическое одобрение, без запроса к админу)
// Формат: chatId -> { owner: telegramId, addedAt, note }
const WHITELISTED_CHATS = new Map();

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

// ========== СИСТЕМА ВЕРСИОНИРОВАНИЯ ЛИЦЕНЗИЙ ==========
// Для мгновенной синхронизации между ботом и расширением
// При изменении лицензии увеличивается версия, расширение сравнивает версии

async function incrementLicenseVersion(env, telegramId) {
  const versionKey = `license_version:${telegramId}`;
  const currentVersion = parseInt(await env.USERS.get(versionKey) || '0');
  const newVersion = currentVersion + 1;
  await env.USERS.put(versionKey, String(newVersion), { expirationTtl: 86400 * 90 }); // 90 дней TTL
  return newVersion;
}

async function getLicenseVersion(env, telegramId) {
  const versionKey = `license_version:${telegramId}`;
  return parseInt(await env.USERS.get(versionKey) || '0');
}

// Rate limiting константы (оптимизировано для 100+ пользователей)
const RATE_LIMIT_WINDOW = 60 * 1000; // 1 минута
const RATE_LIMIT_MAX_REQUESTS = 120; // Увеличено до 120 запросов в минуту на IP

// Telegram IP ranges (для верификации webhook)
// Официальные IP Telegram: https://core.telegram.org/bots/webhooks#the-short-version
const TELEGRAM_SUBNETS = [
  '149.154.160.0/20',
  '91.108.4.0/22',
  '91.108.8.0/22',
  '91.108.12.0/22',
  '91.108.16.0/22',
  '91.108.20.0/22',
  '91.108.56.0/22',
  '185.76.151.0/24'
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
  // Cloudflare может проксировать запросы, поэтому IP может быть не Telegram
  // Проверяем подсети Telegram
  const isTelegramIP = TELEGRAM_SUBNETS.some(subnet => isIPInSubnet(ip, subnet));
  return isTelegramIP;
}

// ========== УПРАВЛЕНИЕ БОТОМ ==========

// Проверка статуса бота (включён/выключен)
async function isBotEnabled(env) {
  // Сначала проверяем in-memory флаг
  if (!BOT_ENABLED) return false;
  
  // Затем проверяем KV (для персистентности)
  const status = await env.USERS.get('bot:enabled');
  return status !== 'false';
}

// Установить статус бота
async function setBotEnabled(env, enabled) {
  BOT_ENABLED = enabled;
  await env.USERS.put('bot:enabled', String(enabled));
}

// ========== БЕЛЫЙ СПИСОК ЧАТОВ ==========

// Проверка чата в белом списке
async function isChatWhitelisted(env, chatId) {
  const data = await env.USERS.get(`whitelist:chat:${chatId}`);
  return data ? JSON.parse(data) : null;
}

// Добавить чат в белый список
async function addChatToWhitelist(env, chatId, ownerTelegramId, note = '') {
  const data = {
    chatId: String(chatId),
    owner: ownerTelegramId,
    addedAt: Date.now(),
    note: note
  };
  await env.USERS.put(`whitelist:chat:${chatId}`, JSON.stringify(data));
  WHITELISTED_CHATS.set(String(chatId), data);
  return data;
}

// Удалить чат из белого списка
async function removeChatFromWhitelist(env, chatId) {
  await env.USERS.delete(`whitelist:chat:${chatId}`);
  WHITELISTED_CHATS.delete(String(chatId));
}

// Получить все чаты из белого списка
async function getWhitelistedChats(env) {
  // KV list не поддерживается напрямую, используем кэш или список ключей
  // Возвращаем in-memory кэш
  return Array.from(WHITELISTED_CHATS.values());
}

// Загрузить белый список из KV в память (вызывается при старте)
async function loadWhitelistCache(env) {
  // В Cloudflare Workers нет list(), поэтому используем отдельный индекс
  const indexData = await env.USERS.get('whitelist:index');
  if (indexData) {
    const chatIds = JSON.parse(indexData);
    for (const chatId of chatIds) {
      const data = await env.USERS.get(`whitelist:chat:${chatId}`);
      if (data) {
        WHITELISTED_CHATS.set(chatId, JSON.parse(data));
      }
    }
  }
}

// Сохранить индекс белого списка
async function saveWhitelistIndex(env) {
  const chatIds = Array.from(WHITELISTED_CHATS.keys());
  await env.USERS.put('whitelist:index', JSON.stringify(chatIds));
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

// ========== ВАЛИДАЦИЯ ВХОДНЫХ ДАННЫХ ==========

// Санитизация строк (защита от injection)
function sanitizeString(str, maxLength = 100) {
  if (typeof str !== 'string') return '';
  // Удаляем контрольные символы и ограничиваем длину
  return str
    .replace(/[\x00-\x1F\x7F]/g, '') // Удаляем контрольные символы
    .trim()
    .substring(0, maxLength);
}

// Валидация oderId (должен быть hex-строкой)
function isValidOderId(oderId) {
  if (typeof oderId !== 'string') return false;
  // oderId должен быть 16-64 символа, hex или alphanumeric
  return /^[a-f0-9A-Za-z\-]{16,64}$/.test(oderId);
}

// Валидация telegramId (только цифры)
function isValidTelegramId(telegramId) {
  if (!telegramId) return false;
  return /^\d{1,20}$/.test(String(telegramId));
}

// Валидация username (Telegram формат)
function isValidUsername(username) {
  if (!username) return false;
  // Telegram username: 5-32 символа, буквы, цифры и _
  const clean = String(username).replace('@', '');
  return /^[a-zA-Z][a-zA-Z0-9_]{4,31}$/.test(clean);
}

// Валидация кода привязки
function isValidBindCode(code) {
  if (!code || typeof code !== 'string') return false;
  return /^[A-Z0-9]{8}$/i.test(code);
}

// Безопасный парсинг JSON
async function safeParseJson(request) {
  try {
    const text = await request.text();
    if (!text || text.trim() === '') {
      return { data: null, error: 'Empty request body' };
    }
    // Ограничиваем размер тела запроса (защита от DoS)
    if (text.length > 100000) { // 100KB max
      return { data: null, error: 'Request body too large' };
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
    // Rate limiting по IP (увеличено для 100+ пользователей)
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `license:${clientIP}`, 200); // 200 запросов/мин
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
      return jsonResponse({ valid: false, error: 'oderId is required' }, 400);
    }
    
    // БЕЗОПАСНОСТЬ: Валидация oderId
    if (!isValidOderId(oderId)) {
      return jsonResponse({ valid: false, error: 'Invalid oderId format' }, 400);
    }
    
    // БЕЗОПАСНОСТЬ: Валидация deviceId если передан
    if (deviceId && !isValidOderId(deviceId)) {
      return jsonResponse({ valid: false, error: 'Invalid deviceId format' }, 400);
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
    
    // БЕЗОПАСНОСТЬ: Строгая валидация oderId
    if (!isValidOderId(oderId)) {
      return jsonResponse({ error: 'Invalid oderId format' }, 400);
    }
    
    // БЕЗОПАСНОСТЬ: Валидация deviceId если передан
    if (deviceId && !isValidOderId(deviceId)) {
      return jsonResponse({ error: 'Invalid deviceId format' }, 400);
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
    // Rate limiting - увеличенный лимит для 100+ пользователей
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `status:${clientIP}`, 300); // 300 запросов в минуту
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
    
    // ОПТИМИЗАЦИЯ: Валидация oderId сразу
    if (!isValidOderId(oderId)) {
      return jsonResponse({ error: 'Invalid oderId format' }, 400);
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

// ========== REAL-TIME ПОДКЛЮЧЕНИЕ (SSE) ==========
// Server-Sent Events для мгновенного оповещения о подключении Telegram
// Расширение подключается к этому endpoint и ждёт события connected
async function handleConnectionStream(request, env) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  const oderId = url.searchParams.get('oderId');
  
  if (!code || !oderId) {
    return new Response('Missing code or oderId', { status: 400 });
  }
  
  // Валидация
  if (!isValidBindCode(code) || !isValidOderId(oderId)) {
    return new Response('Invalid parameters', { status: 400 });
  }
  
  // Rate limiting
  const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
  const rateLimit = await checkRateLimit(env, `stream:${clientIP}`, 10); // 10 потоков в минуту
  if (!rateLimit.allowed) {
    return new Response('Rate limited', { status: 429 });
  }
  
  // Создаём ReadableStream для SSE
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();
  
  // Отправляем ping каждые 15 секунд чтобы соединение не закрылось
  // и проверяем статус подключения
  let checkCount = 0;
  const maxChecks = 120; // 30 минут максимум (120 * 15 сек)
  
  const checkConnection = async () => {
    checkCount++;
    
    try {
      // Проверяем, подключился ли пользователь
      const userData = await env.USERS.get(`user:${oderId}`);
      
      if (userData) {
        const user = JSON.parse(userData);
        
        // Получаем данные лицензии
        const result = await getLicenseData(env, oderId);
        
        // Отправляем событие connected
        const eventData = JSON.stringify({
          event: 'connected',
          username: user.username,
          telegramId: user.telegramId,
          license: result.license ? {
            valid: result.license.expiresAt > Date.now(),
            type: result.license.type,
            expiresAt: result.license.expiresAt
          } : null
        });
        
        await writer.write(encoder.encode(`event: connected\ndata: ${eventData}\n\n`));
        await writer.close();
        return true; // Подключение найдено, закрываем поток
      }
      
      // Отправляем ping (keep-alive)
      await writer.write(encoder.encode(`: ping ${Date.now()}\n\n`));
      
      // Продолжаем проверять если не превысили лимит
      if (checkCount < maxChecks) {
        return false;
      } else {
        // Время истекло
        await writer.write(encoder.encode(`event: timeout\ndata: {"error":"timeout"}\n\n`));
        await writer.close();
        return true;
      }
    } catch (error) {
      console.error('SSE check error:', error);
      try {
        await writer.write(encoder.encode(`event: error\ndata: {"error":"${error.message}"}\n\n`));
        await writer.close();
      } catch (e) {}
      return true;
    }
  };
  
  // Запускаем проверку с помощью алармов Cloudflare Workers
  // Первая проверка сразу, затем каждые 500мс первые 20 раз, потом каждые 2 сек
  const runChecks = async () => {
    // Быстрые проверки первые 10 секунд (каждые 500мс)
    for (let i = 0; i < 20; i++) {
      if (await checkConnection()) return;
      await new Promise(r => setTimeout(r, 500));
    }
    
    // Затем каждые 2 секунды
    for (let i = 0; i < maxChecks - 20; i++) {
      if (await checkConnection()) return;
      await new Promise(r => setTimeout(r, 2000));
    }
  };
  
  // Запускаем проверки в фоне (не блокируя ответ)
  // @ts-ignore - ctx.waitUntil доступен в Cloudflare Workers
  runChecks();
  
  // Возвращаем SSE ответ
  return new Response(readable, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET',
      'X-Accel-Buffering': 'no'
    }
  });
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
    
    // БЕЗОПАСНОСТЬ: Валидация кода
    if (!isValidBindCode(code)) {
      return jsonResponse({ error: 'Invalid code format' }, 400);
    }
    
    // БЕЗОПАСНОСТЬ: Валидация telegramId
    if (!isValidTelegramId(telegramId)) {
      return jsonResponse({ error: 'Invalid telegramId' }, 400);
    }
    
    // БЕЗОПАСНОСТЬ: Санитизация username и firstName
    const safeUsername = username ? sanitizeString(username, 32) : null;
    const safeFirstName = firstName ? sanitizeString(firstName, 64) : null;
    
    // Получаем данные по коду
    const codeData = await env.USERS.get(`code:${code.toUpperCase()}`);
    if (!codeData) {
      return jsonResponse({ error: 'Code expired or invalid', code: 'INVALID_CODE' }, 400);
    }
    
    const { oderId, deviceId } = JSON.parse(codeData);
    
    // Сохраняем связку user -> telegram (используем санитизированные данные)
    const userData = {
      oderId,
      deviceId,
      telegramId: String(telegramId),
      username: safeUsername,
      firstName: safeFirstName,
      connectedAt: Date.now()
    };
    
    await env.USERS.put(`user:${oderId}`, JSON.stringify(userData));
    
    // Также сохраняем обратную связь telegram -> oderId для быстрого поиска
    await env.USERS.put(`tg:${telegramId}`, oderId);
    
    // Удаляем использованный код
    await env.USERS.delete(`code:${code.toUpperCase()}`);
    
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

// Одобрить групповой чат (админ или автоматически из белого списка)
async function approveGroupChat(env, chatId, approvedBy, autoApprove = false) {
  const pendingKey = `pending_group:${chatId}`;
  const pendingData = await env.USERS.get(pendingKey);
  
  let telegramId;
  let chatTitle = 'Unknown';
  
  if (pendingData) {
    // Есть запрос на одобрение
    const request = JSON.parse(pendingData);
    telegramId = request.requestedBy;
    chatTitle = request.chatTitle;
  } else if (autoApprove) {
    // Автоодобрение из белого списка - владелец = approvedBy
    telegramId = approvedBy;
  } else {
    return { success: false, error: 'REQUEST_NOT_FOUND' };
  }
  
  // Получаем текущие чаты пользователя
  const groupData = await getUserGroupChats(env, telegramId);
  
  // Проверяем не добавлен ли уже этот чат
  const alreadyExists = groupData.chats.some(c => String(c.chatId) === String(chatId));
  if (alreadyExists) {
    // Уже добавлен - просто очищаем pending
    if (pendingData) {
      await env.USERS.delete(pendingKey);
    }
    return { success: true, telegramId, alreadyExists: true };
  }
  
  // Добавляем чат
  groupData.chats.push({
    chatId: String(chatId),
    chatTitle: chatTitle,
    approvedAt: Date.now(),
    approvedBy: approvedBy,
    autoApproved: autoApprove
  });
  
  // Удаляем из pending
  groupData.pending = (groupData.pending || []).filter(p => String(p.chatId) !== String(chatId));
  
  await saveUserGroupChats(env, telegramId, groupData);
  
  if (pendingData) {
    await env.USERS.delete(pendingKey);
  }
  
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
                })
              });
              
              if (groupResult.ok) {
                sentTo.push(`group:${chat.chatId}`);
              } else {
                // Если бот удалён из группы - убираем её
                const errorData = await groupResult.json();
                if (errorData.error_code === 403 || errorData.description?.includes('bot was kicked')) {
                  groupData.chats = groupData.chats.filter(c => c.chatId !== chat.chatId);
                  await saveUserGroupChats(env, telegramId, groupData);
                }
              }
            } catch (e) {
              console.error(`Group ${chat.chatId} send error:`, e);
            }
          }
        } else if (licenseLevel === 'level2' && !sendToDm) {
          // Level2 fallback: если нет группы, отправляем в ЛС
          try {
            const personalResult = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: telegramId,
                text: message + `\n\n_💡 Подключите групповой чат для уведомлений_`,
                parse_mode: 'Markdown'
              })
            });
            
            if (personalResult.ok) {
              sentTo.push('personal_fallback');
            }
          } catch (e) {
            console.error('Fallback personal send error:', e);
          }
        }
      }
    }
    
    return jsonResponse({ success: true, sentTo, message });
    
  } catch (error) {
    console.error('Notify error:', error);
    return jsonResponse({ error: 'SERVER_ERROR', message: error.message }, 500);
  }
}

// Универсальная функция для показа панели (fallback)
async function showAdminPanel(env, chatId, stats, pendingGroups, messageId = null) {
  const adminMsg = `🔐 *АДМИН-ПАНЕЛЬ*\n` +
    `━━━━━━━━━━━━━━━━━━━━\n\n` +
    `📊 *Статистика:*\n` +
    `├ 👥 Всего: ${stats.totalUsers}\n` +
    `├ ✅ Активных: ${stats.activeUsers}\n` +
    `├ ⭐ Level 1: ${stats.level1Users || 0}\n` +
    `├ ⭐⭐ Level 2: ${stats.level2Users || 0}\n` +
    `├ ⭐⭐⭐ Level 3: ${stats.level3Users || 0}\n` +
    `└ 🎁 Trial: ${stats.trialUsers}\n\n` +
    `💬 *Чаты:*\n` +
    `├ ⏳ Ожидают: ${pendingGroups.length}\n` +
    `└ ✅ Одобрено: ${stats.approvedGroups}\n\n` +
    `_Сессия: 1 час_`;
    
  const keyboard = {
    inline_keyboard: [
      // Ряд 1: Ожидающие группы (если есть)
      pendingGroups.length > 0 ? [
        { text: `🔔 Заявки на чаты (${pendingGroups.length})`, callback_data: 'adm_pending' }
      ] : [],
      // Ряд 2: Подписки
      [
        { text: '🎁 Выдать подписку', callback_data: 'adm_grant' },
        { text: '🔍 Найти юзера', callback_data: 'adm_search' }
      ],
      // Ряд 3: Списки
      [
        { text: '👥 Все подписки', callback_data: 'adm_users' },
        { text: '💬 Все чаты', callback_data: 'adm_chats' }
      ],
      // Ряд 4: Инструменты
      [
        { text: '📢 Рассылка', callback_data: 'adm_broadcast' },
        { text: '📈 Статистика', callback_data: 'adm_stats' }
      ],
      // Ряд 5: Система
      [
        { text: '🔧 Система', callback_data: 'adm_system' },
        { text: '🔄', callback_data: 'adm_refresh' }
      ],
      // Ряд 6: Выход
      [
        { text: '🚪 Выход', callback_data: 'adm_logout' }
      ]
    ].filter(row => row.length > 0)
  };
  
  if (messageId) {
    await editTelegramMessage(env, chatId, messageId, adminMsg, { reply_markup: keyboard });
  } else {
    await sendTelegramMessage(env, chatId, adminMsg, { reply_markup: keyboard });
  }
}

// Централизованный обработчик ошибок
async function handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups) {
  // Пробуем fallback: новое сообщение с панелью
  await showAdminPanel(env, chatId, stats, pendingGroups);
  // Логируем ошибку
  await logToChat(env, `❌ Ошибка админ-панели: ${err && err.message ? err.message : err}`);
}

// === TELEGRAM BOT WEBHOOK HANDLERS ===

async function sendTelegramMessage(env, chatId, text, options = {}) {
  const botToken = env.TELEGRAM_BOT_TOKEN;
  if (!botToken) {
    console.error('TELEGRAM_BOT_TOKEN not set');
    return false;
  }
  
  try {
    const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: text,
        parse_mode: 'Markdown',
        ...options
      })
    });
    return await response.json();
  } catch (error) {
    console.error('Telegram send error:', error);
    return false;
  }
}

async function editTelegramMessage(env, chatId, messageId, text, options = {}) {
  const botToken = env.TELEGRAM_BOT_TOKEN;
  if (!botToken) return false;
  
  try {
    const response = await fetch(`https://api.telegram.org/bot${botToken}/editMessageText`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        message_id: messageId,
        text: text,
        parse_mode: 'Markdown',
        ...options
      })
    });
    return await response.json();
  } catch (error) {
    console.error('Telegram edit error:', error);
    return false;
  }
}

async function answerCallbackQuery(env, callbackQueryId, text = '', showAlert = false) {
  const botToken = env.TELEGRAM_BOT_TOKEN;
  if (!botToken) return false;
  
  try {
    await fetch(`https://api.telegram.org/bot${botToken}/answerCallbackQuery`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        callback_query_id: callbackQueryId,
        text: text,
        show_alert: showAlert
      })
    });
    return true;
  } catch (error) {
    console.error('Answer callback error:', error);
    return false;
  }
}

// === ВИЗУАЛЬНЫЕ ХЕЛПЕРЫ ===

// Генерация прогресс-бара
function getProgressBar(percent, length = 10) {
  const filled = Math.round((percent / 100) * length);
  const empty = length - filled;
  return '▓'.repeat(filled) + '░'.repeat(empty);
}

// Форматирование времени
function formatTimeLeft(ms) {
  const hours = Math.floor(ms / (60 * 60 * 1000));
  const days = Math.floor(hours / 24);
  
  if (days > 0) {
    return `${days} дн.`;
  } else if (hours > 0) {
    return `${hours} ч.`;
  } else {
    const minutes = Math.floor(ms / (60 * 1000));
    return `${minutes} мин.`;
  }
}

// Получить эмодзи статуса
function getStatusEmoji(daysLeft, type) {
  if (type === 'trial') return '🎁';
  if (daysLeft > 14) return '💎';
  if (daysLeft > 7) return '⭐';
  if (daysLeft > 3) return '⏰';
  return '⚠️';
}

// === КЛАВИАТУРЫ ===

// Главное меню - для новых пользователей
function getMainMenuKeyboard() {
  return {
    inline_keyboard: [
      [{ text: '🔗 Подключить расширение', callback_data: 'how_to_connect' }],
      [
        { text: '📊 Статус', callback_data: 'status' },
        { text: '📈 Статистика', callback_data: 'mystats' }
      ],
      [
        { text: '❓ Помощь', callback_data: 'help' },
        { text: '🎟️ Реферал', callback_data: 'referral' }
      ],
      [
        { text: '� Установка', callback_data: 'install' },
        { text: '📰 Новости', callback_data: 'news' }
      ],
      [
        { text: '�💳 Купить', callback_data: 'buy' },
        { text: '💬 Поддержка', url: 'https://t.me/YaMob' }
      ]
    ]
  };
}

// Меню для привязанных пользователей с активной подпиской
function getUserMenuKeyboard(isValid, hasGroupChats = false) {
  const keyboard = [];
  
  if (isValid) {
    keyboard.push([
      { text: '📊 Статус', callback_data: 'status' },
      { text: '📈 Статистика', callback_data: 'mystats' }
    ]);
    keyboard.push([
      { text: '🔔 Тест уведомлений', callback_data: 'test_notify' },
      { text: '⚙️ Настройки', callback_data: 'settings' }
    ]);
    if (hasGroupChats) {
      keyboard.push([{ text: '💬 Групповые чаты', callback_data: 'groups_menu' }]);
    }
    keyboard.push([
      { text: '🎟️ Пригласить друга', callback_data: 'referral' },
      { text: '❓ Помощь', callback_data: 'help' }
    ]);
  } else {
    keyboard.push([{ text: '💳 Купить подписку', callback_data: 'buy' }]);
    keyboard.push([
      { text: '📊 Статус', callback_data: 'status' },
      { text: '💬 Поддержка', url: 'https://t.me/YaMob' }
    ]);
  }
  
  return { inline_keyboard: keyboard };
}

// Кнопка назад
function getBackButton() {
  return {
    inline_keyboard: [
      [{ text: '← Меню', callback_data: 'menu' }]
    ]
  };
}

// Клавиатура статуса
function getStatusKeyboard(isValid, type) {
  if (isValid) {
    return {
      inline_keyboard: [
        [{ text: '🔔 Проверить уведомления', callback_data: 'test_notify' }],
        [
          { text: '🔄 Обновить', callback_data: 'status' },
          { text: '← Меню', callback_data: 'menu' }
        ]
      ]
    };
  }
  return {
    inline_keyboard: [
      [{ text: '💳 Купить подписку', callback_data: 'buy' }],
      [
        { text: '🔄 Обновить', callback_data: 'status' },
        { text: '← Меню', callback_data: 'menu' }
      ]
    ]
  };
}

async function handleTelegramWebhook(request, env) {
  try {
    const clientIP = request.headers.get('CF-Connecting-IP');
    
    // БЕЗОПАСНОСТЬ: Проверка IP от Telegram (ВКЛЮЧЕНА по умолчанию)
    // Отключить можно через env.SKIP_TELEGRAM_IP_CHECK = 'true' (не рекомендуется)
    if (env.SKIP_TELEGRAM_IP_CHECK !== 'true') {
      if (!isFromTelegram(clientIP)) {
        console.warn(`[SECURITY] Webhook blocked from non-Telegram IP: ${clientIP}`);
        return jsonResponse({ ok: false, error: 'Forbidden' }, 403);
      }
    }
    
    // БЕЗОПАСНОСТЬ: Проверка секретного токена от Telegram (ОБЯЗАТЕЛЬНО если настроен)
    const secretToken = env.TELEGRAM_WEBHOOK_SECRET;
    if (secretToken) {
      const headerToken = request.headers.get('X-Telegram-Bot-Api-Secret-Token');
      if (!headerToken || headerToken !== secretToken) {
        console.warn(`[SECURITY] Invalid webhook secret from IP: ${clientIP}`);
        return jsonResponse({ ok: false, error: 'Forbidden' }, 403);
      }
    } else {
      // Логируем предупреждение если secret не настроен
      console.warn('[SECURITY WARNING] TELEGRAM_WEBHOOK_SECRET not configured!');
    }
    
    const { data: update, error } = await safeParseJson(request);
    if (error) {
      console.error('Webhook parse error:', error);
      return jsonResponse({ ok: false, error: 'Invalid JSON' }, 400);
    }
    
    console.log('Webhook received:', JSON.stringify(update).substring(0, 500));
    
    // Функция проверки админа (ТОЛЬКО по Telegram ID для безопасности)
    const isAdmin = async (telegramId) => {
      return await isAdminById(env, telegramId);
    };
    
    // Функция проверки - это групповой чат или ЛС
    const isGroupChat = (chat) => {
      return chat && (chat.type === 'group' || chat.type === 'supergroup');
    };
    
    // ========== ПРОВЕРКА СТАТУСА БОТА ==========
    // Админские команды работают всегда, остальные - только если бот включён
    const botEnabled = await isBotEnabled(env);
    const fromUserId = update.message?.from?.id || update.callback_query?.from?.id;
    const isAdminUser = fromUserId ? await isAdmin(String(fromUserId)) : false;
    
    // Если бот выключен и это не админ - игнорируем
    if (!botEnabled && !isAdminUser) {
      console.log('[BOT DISABLED] Ignoring non-admin message');
      return jsonResponse({ ok: true });
    }
    
    // === ОБРАБОТКА INLINE ЗАПРОСОВ ===
    if (update.inline_query) {
      const inlineQuery = update.inline_query;
      const telegramId = String(inlineQuery.from.id);
      const query = inlineQuery.query.toLowerCase().trim();
      
      // Rate limit для inline
      const rateCheck = checkBotRateLimit(telegramId, 'callbacks');
      if (!rateCheck.allowed) {
        return jsonResponse({ ok: true });
      }
      
      // Получаем реферальный код пользователя
      const refCode = await getUserReferralCode(env, telegramId);
      const botUsername = env.BOT_USERNAME || 'clickuved_bot';
      
      const results = [];
      
      // Показываем карточку с реф-ссылкой
      if (query === '' || query.includes('реф') || query.includes('ref') || query.includes('invite')) {
        results.push({
          type: 'article',
          id: 'ref_' + telegramId,
          title: '🎁 Пригласить друга',
          description: 'Отправить реферальную ссылку',
          input_message_content: {
            message_text: `🎁 *Приглашение в Exotic Assistant*\n\n` +
              `Привет! Попробуй автокликер для Exotic.\n\n` +
              `✅ Автоматически берёт заявки\n` +
              `✅ Уведомления в Telegram\n` +
              `✅ Подробная статистика\n\n` +
              `🎟️ Мой реферальный код: \`${refCode}\`\n\n` +
              `👉 Перейди к боту: @${botUsername}\n` +
              `И введи команду: \`/ref ${refCode}\`\n\n` +
              `_Или перейди по ссылке:_\n` +
              `https://t.me/${botUsername}?start=ref_${refCode}`,
            parse_mode: 'Markdown'
          },
          reply_markup: {
            inline_keyboard: [
              [{ text: '🤖 Перейти к боту', url: `https://t.me/${botUsername}?start=ref_${refCode}` }]
            ]
          }
        });
      }
      
      // Отвечаем на inline запрос
      await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/answerInlineQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inline_query_id: inlineQuery.id,
          results: results,
          cache_time: 300, // кэш 5 минут
          is_personal: true
        })
      });
      
      return jsonResponse({ ok: true });
    }
    
    // === ОБРАБОТКА CALLBACK КНОПОК ===
    if (update.callback_query) {
      const callback = update.callback_query;
      const chatId = callback.message.chat.id;
      const messageId = callback.message.message_id;
      const telegramId = String(callback.from.id);
      const data = callback.data;
      const firstName = callback.from.first_name;
      const chatType = callback.message.chat.type;
      const isGroup = chatType === 'group' || chatType === 'supergroup';
      const username = callback.from.username;
      
      console.log(`[CALLBACK] data="${data}" from=${telegramId} chatId=${chatId} msgId=${messageId} type=${chatType}`);
      
      // Rate limiting для callback
      const rateCheck = checkBotRateLimit(telegramId, 'callbacks');
      if (!rateCheck.allowed) {
        await answerCallbackQuery(env, callback.id, rateCheck.message, true);
        return jsonResponse({ ok: true });
      }
      
      // Подтверждаем получение callback
      await answerCallbackQuery(env, callback.id);
      
      // Обработка callback для групповых чатов
      if (isGroup && data === 'group_status') {
        const existingOderId = await env.USERS.get(`tg:${telegramId}`);
        const botUsername = env.BOT_USERNAME || 'clickuved_bot';
        
        if (!existingOderId) {
          await editTelegramMessage(env, chatId, messageId, 
            `📊 @${username || telegramId}, ваш статус:\n\n` +
            `❌ Расширение не подключено\n\n` +
            `Напишите боту в ЛС для подключения.`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '🤖 Подключить', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
          return jsonResponse({ ok: true });
        }
        
        const result = await getLicenseData(env, existingOderId);
        
        if (!result.license) {
          await editTelegramMessage(env, chatId, messageId, 
            `📊 @${username || telegramId}, ваш статус:\n\n` +
            `✅ Расширение подключено\n` +
            `❌ Подписка не найдена`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '💳 Купить', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
          return jsonResponse({ ok: true });
        }
        
        const license = result.license;
        const now = Date.now();
        const isValid = license.expiresAt > now;
        const timeLeft = license.expiresAt - now;
        const daysLeft = Math.max(0, Math.ceil(timeLeft / (24 * 60 * 60 * 1000)));
        const hoursLeft = Math.max(0, Math.ceil(timeLeft / (60 * 60 * 1000)));
        
        const emoji = isValid ? getStatusEmoji(daysLeft, license.type) : '❌';
        const typeText = LICENSE_TYPES[license.type]?.name || 'Премиум';
        const timeDisplay = license.type === 'trial' ? `${hoursLeft} ч.` : `${daysLeft} дн.`;
        
        await editTelegramMessage(env, chatId, messageId, 
          `📊 @${username || telegramId}, ваш статус:\n\n` +
          `${emoji} ${typeText} • ${isValid ? 'Активна' : 'Истекла'}\n` +
          `⏰ ${isValid ? `Осталось: ${timeDisplay}` : 'Продлите подписку'}`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '📊 Подробнее', url: `https://t.me/${botUsername}` }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      switch (data) {
        case 'menu': {
          const menuText = `🏠 *Меню*\n\nВыберите раздел:`;
          
          await editTelegramMessage(env, chatId, messageId, menuText, {
            reply_markup: getMainMenuKeyboard()
          });
          break;
        }
        
        case 'status': {
          // Сначала проверяем привязку
          const existingOderId = await env.USERS.get(`tg:${telegramId}`);
          
          if (!existingOderId) {
            await editTelegramMessage(env, chatId, messageId, 
              `📊 *Статус*\n\n` +
              `❌ Расширение не подключено\n\n` +
              `Сначала привяжите расширение\nчерез код в настройках`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '🔗 Подключить', callback_data: 'how_to_connect' }],
                  [{ text: ' Тарифы', callback_data: 'buy' }],
                  [{ text: '← Меню', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          // Получаем лицензию через getLicenseData (создаст триал если нет)
          const result = await getLicenseData(env, existingOderId);
          
          if (!result.license) {
            await editTelegramMessage(env, chatId, messageId, 
              `📊 *Статус*\n\n` +
              `✅ Расширение подключено\n` +
              `❌ Подписка не найдена\n\n` +
              `Напишите @YaMob для\nактивации подписки`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '💳 Купить подписку', callback_data: 'buy' }],
                  [{ text: '💬 Поддержка', callback_data: 'support' }],
                  [{ text: '← Меню', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          const license = result.license;
          const licenseType = LICENSE_TYPES[license.type] || LICENSE_TYPES.trial;
          const now = Date.now();
          const isValid = license.expiresAt > now;
          const timeLeft = license.expiresAt - now;
          const daysLeft = Math.max(0, Math.ceil(timeLeft / (24 * 60 * 60 * 1000)));
          const hoursLeft = Math.max(0, Math.ceil(timeLeft / (60 * 60 * 1000)));
          const expiresDate = new Date(license.expiresAt).toLocaleDateString('ru-RU');
          
          // Получаем информацию об устройствах
          const devicesData = await getUserDevices(env, telegramId);
          const deviceCount = devicesData.devices?.length || 0;
          const maxDevices = licenseType.maxDevices || 1;
          
          let message;
          if (isValid) {
            const emoji = getStatusEmoji(daysLeft, license.type);
            const typeText = licenseType.name || 'Премиум';
            const timeDisplay = license.type === 'trial' ? `${hoursLeft} ч.` : `${daysLeft} дн.`;
            
            // Прогресс-бар (для trial 24ч, для подписки 30дн)
            const maxTime = license.type === 'trial' ? 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
            const percent = Math.min(100, Math.round((timeLeft / maxTime) * 100));
            const bar = getProgressBar(percent);
            
            message = `📊 *Статус подписки*\n\n`;
            message += `${emoji} ${typeText} • Активна\n`;
            message += `${bar} ${percent}%\n\n`;
            message += `⏰ Осталось: *${timeDisplay}*\n`;
            message += `📅 До: ${expiresDate}\n`;
            message += `📱 Устройства: ${deviceCount}/${maxDevices}`;
            
            if (deviceCount > 0 && deviceCount >= maxDevices) {
              message += ` ⚠️`;
            }
            
            if (daysLeft <= 3 && license.type !== 'trial') {
              message += `\n\n⚠️ _Скоро истечёт_`;
            }
          } else {
            message = `📊 *Статус подписки*\n\n`;
            message += `❌ Истекла ${expiresDate}\n`;
            message += `📱 Устройства: ${deviceCount}/${maxDevices}\n\n`;
            message += `_Продлите для работы_`;
          }
          
          // Клавиатура с кнопкой управления устройствами
          const keyboard = {
            inline_keyboard: []
          };
          
          if (isValid) {
            keyboard.inline_keyboard.push([{ text: '💳 Продлить', callback_data: 'buy' }]);
            if (deviceCount > 0) {
              keyboard.inline_keyboard.push([{ text: '📱 Мои устройства', callback_data: 'devices' }]);
            }
            keyboard.inline_keyboard.push([
              { text: '🔄 Обновить', callback_data: 'status' },
              { text: '← Меню', callback_data: 'menu' }
            ]);
          } else {
            keyboard.inline_keyboard.push(
              [{ text: '💳 Купить подписку', callback_data: 'buy' }],
              [
                { text: '🔄 Обновить', callback_data: 'status' },
                { text: '← Меню', callback_data: 'menu' }
              ]
            );
          }
          
          // Проверяем и отправляем уведомления об истечии (3, 1, 0 дней)
          if (isValid && license.type !== 'trial') {
            if (daysLeft <= 3 && daysLeft > 1) {
              await sendExpirationNotification(env, telegramId, 3, license.type);
            } else if (daysLeft === 1) {
              await sendExpirationNotification(env, telegramId, 1, license.type);
            } else if (daysLeft === 0) {
              await sendExpirationNotification(env, telegramId, 0, license.type);
            }
          }
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: keyboard
          });
          break;
        }
        
        case 'devices': {
          // Показать список устройств пользователя
          const devicesData = await getUserDevices(env, telegramId);
          const devices = devicesData.devices || [];
          
          // Получаем лицензию для maxDevices
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          let maxDevices = 1;
          if (oderId) {
            const licResult = await getLicenseData(env, oderId);
            if (licResult.license) {
              const licType = LICENSE_TYPES[licResult.license.type] || LICENSE_TYPES.trial;
              maxDevices = licType.maxDevices || 1;
            }
          }
          
          let message = `📱 *Мои устройства* (${devices.length}/${maxDevices})\n\n`;
          
          if (devices.length === 0) {
            message += `_Нет привязанных устройств_`;
          } else {
            devices.forEach((device, index) => {
              const shortId = device.deviceId.substring(0, 12) + '...';
              const addedDate = new Date(device.addedAt).toLocaleDateString('ru-RU');
              const lastSeenDate = new Date(device.lastSeen).toLocaleDateString('ru-RU');
              message += `${index + 1}. \`${shortId}\`\n`;
              message += `   📅 Добавлено: ${addedDate}\n`;
              message += `   👁 Послед. акт.: ${lastSeenDate}\n\n`;
            });
          }
          
          if (devices.length >= maxDevices) {
            message += `\n⚠️ _Лимит достигнут. Отвяжите устройство для добавления нового._`;
          }
          
          // Создаём кнопки для удаления каждого устройства
          const keyboard = {
            inline_keyboard: []
          };
          
          devices.forEach((device, index) => {
            keyboard.inline_keyboard.push([
              { text: `🗑 Отвязать устройство ${index + 1}`, callback_data: `remove_device:${device.deviceId.substring(0, 32)}` }
            ]);
          });
          
          keyboard.inline_keyboard.push([{ text: '← Статус', callback_data: 'status' }]);
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: keyboard
          });
          break;
        }
        
        case 'help': {
          const helpMsg = `📖 *Exotic Assistant v${BOT_VERSION}*

🖱️ *Автокликер*
Автоматически берёт заявки "В работу"

📊 *Статистика*
Клики за день • Всего • Графики

🔔 *Уведомления*
Push • Telegram • Звук

⚙️ *Настройки*
Интервал • Задержка • Громкость

🎟️ *Реферальная программа*
Пригласи друга — получи +1 день!`;
          
          await editTelegramMessage(env, chatId, messageId, helpMsg, {
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '🔗 Подключить', callback_data: 'how_to_connect' },
                  { text: '❓ FAQ', callback_data: 'faq' }
                ],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'how_to_connect': {
          const connectMsg = `🔗 *Как подключить расширение*

1️⃣ Установите расширение в браузер
2️⃣ Нажмите на иконку расширения
3️⃣ Откройте "Настройки" ⚙️
4️⃣ Нажмите "Подключить Telegram"
5️⃣ Перейдите по ссылке или отправьте код

✅ *После привязки:*
• 24 часа бесплатного доступа
• Уведомления о заявках
• Автокликер активен`;
          
          await editTelegramMessage(env, chatId, messageId, connectMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '📊 Проверить статус', callback_data: 'status' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'buy': {
          const buyMsg = `💳 *Тарифы Exotic Assistant v${BOT_VERSION}*

━━━━━━━━━━━━━━━━━━━━

⭐ *Уровень 1* — 15 USDT/мес
├ ✅ Автокликер заявок
├ ✅ Уведомления в ЛС бота
├ ✅ Статистика
├ 📱 1 устройство
└ 🎁 24ч пробный период (новым)

━━━━━━━━━━━━━━━━━━━━

⭐⭐ *Уровень 2* — 30 USDT/мес
├ ✅ Всё из Уровень 1
├ ✅ Групповой чат для команды
├ 🔔 Уведомления только в группу
└ 📱 До 2 устройств

━━━━━━━━━━━━━━━━━━━━

⭐⭐⭐ *Уровень 3 (Максимум)* — 35 USDT/мес
├ ✅ Полный функционал
├ ✅ Выбор куда слать уведомления
├ ✅ ЛС + Группа одновременно
├ ✅ VIP поддержка
└ 📱 До 3 устройств

━━━━━━━━━━━━━━━━━━━━

💰 *Оплата:* USDT (TRC20)

_Для покупки напишите в поддержку_ 👇`;
          
          await editTelegramMessage(env, chatId, messageId, buyMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '💬 Написать @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: '📊 Сравнить тарифы', callback_data: 'compare_plans' }],
                [
                  { text: '📊 Статус', callback_data: 'status' },
                  { text: '← Меню', callback_data: 'menu' }
                ]
              ]
            }
          });
          break;
        }
        
        case 'compare_plans': {
          const compareMsg = `📋 *Сравнение тарифов*

| Функция | L1 | L2 | L3 |
|---------|:--:|:--:|:--:|
| Автокликер | ✅ | ✅ | ✅ |
| Уведомления ЛС | ✅ | ❌ | ✅ |
| Уведомления Группа | ❌ | ✅ | ✅ |
| Выбор куда слать | ❌ | ❌ | ✅ |
| Груп. чат | ❌ | ✅ | ✅ |
| VIP поддержка | ❌ | ❌ | ✅ |
| Устройств | 1 | 2 | 3 |

💰 *Цены:*
• Уровень 1: 15 USDT/мес
• Уровень 2: 30 USDT/мес
• Уровень 3: 35 USDT/мес`;
          
          await editTelegramMessage(env, chatId, messageId, compareMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '💬 Купить @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: '← Тарифы', callback_data: 'buy' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'clear_devices': {
          // Очистить все устройства пользователя
          await clearAllDevices(env, telegramId);
          
          const clearMsg = '✅ *Устройства отвязаны*\n\nВсе устройства были удалены из вашего аккаунта.\n\nПри следующем использовании расширения устройство будет добавлено автоматически.';
          
          await editTelegramMessage(env, chatId, messageId, clearMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '📱 Мои устройства', callback_data: 'my_devices' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'my_devices': {
          // Показать устройства (callback версия команды /devices)
          const licenseData = await env.USERS.get('license:tg:' + telegramId);
          const license = licenseData ? JSON.parse(licenseData) : null;
          const licenseType = LICENSE_TYPES[license?.type] || LICENSE_TYPES.trial;
          const maxDevices = licenseType.maxDevices || 1;
          
          const devicesData = await getUserDevices(env, telegramId);
          const devices = devicesData.devices || [];
          
          let message = '📱 *Ваши устройства*\n\n';
          message += '📦 Подписка: *' + licenseType.name + '*\n';
          message += '🔢 Использовано: ' + devices.length + '/' + maxDevices + '\n\n';
          
          if (devices.length === 0) {
            message += '_Нет подключённых устройств_';
          } else {
            message += '*Список:*\n';
            devices.forEach((d, i) => {
              const lastSeenDate = new Date(d.lastSeen).toLocaleDateString('ru-RU');
              message += (i + 1) + '. ' + d.deviceId.substring(0, 12) + '... (' + lastSeenDate + ')\n';
            });
          }
          
          const buttons = [];
          if (devices.length > 0) {
            buttons.push([{ text: '🗑 Отвязать все', callback_data: 'clear_devices' }]);
          }
          buttons.push([{ text: '← Меню', callback_data: 'menu' }]);
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: { inline_keyboard: buttons }
          });
          break;
        }
        
        case 'support': {
          // Обработчик кнопки поддержки
          const supportMsg = '💬 *Поддержка*\n\nЕсли у вас есть вопросы или проблемы:\n\n📩 Напишите: @YaMob\n⏰ Время ответа: до 24 часов\n\n*Частые вопросы:*\n• Как подключить расширение?\n• Как продлить подписку?\n• Не работает автокликер?\n\nНажмите кнопку ниже 👇';
          
          await editTelegramMessage(env, chatId, messageId, supportMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '💬 Написать @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: '❓ FAQ', callback_data: 'faq' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'mystats': {
          // Личная статистика пользователя (аналог команды /mystats)
          const stats = await getUserStats(env, telegramId);
          const oderId = await env.USERS.get('tg:' + telegramId);
          
          let licenseInfo = '❌ Не подключён';
          let licenseType = null;
          if (oderId) {
            const result = await getLicenseData(env, oderId);
            if (result.license) {
              const daysLeft = Math.max(0, Math.ceil((result.license.expiresAt - Date.now()) / MS_PER_DAY));
              const isActive = result.license.expiresAt > Date.now();
              const typeText = LICENSE_TYPES[result.license.type]?.name || result.license.type;
              licenseInfo = isActive 
                ? '✅ ' + typeText + ' (' + daysLeft + ' ' + getDaysWord(daysLeft) + ')'
                : '❌ ' + typeText + ' (истекла)';
              licenseType = result.license.type;
            }
          }
          
          const referralCode = await getUserReferralCode(env, telegramId);
          
          // Формируем красивую статистику
          const todayClicks = stats.todayClicks || 0;
          const totalClicks = stats.totalClicks || 0;
          const referralCount = stats.referralCount || 0;
          
          // Эмодзи для достижений
          let achievementBadge = '';
          if (totalClicks >= 1000) achievementBadge = '🏆 ';
          else if (totalClicks >= 500) achievementBadge = '🥇 ';
          else if (totalClicks >= 100) achievementBadge = '🥈 ';
          else if (totalClicks >= 10) achievementBadge = '🥉 ';
          
          const paidBonuses = stats.referralPaidBonus || 0;
          const pendingBonuses = stats.referralPendingBonus || 0;
          
          const message = '📈 *Ваша статистика*\n\n' +
            achievementBadge + '👆 *Сегодня:* ' + todayClicks + ' кликов\n' +
            '📊 *Всего:* ' + totalClicks + ' кликов\n\n' +
            '💎 *Подписка:* ' + licenseInfo + '\n\n' +
            '👥 *Рефералов:* ' + referralCount + '\n' +
            '🎁 *Получено бонусов:* ' + paidBonuses + ' дн.\n' +
            (pendingBonuses > 0 ? '⏳ *Ожидают оплаты:* ' + pendingBonuses + '\n' : '') +
            '\n🎟️ *Ваш код:* ' + referralCode + '\n\n' +
            '_Бонус +1 день когда реферал оформит подписку!_';
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '📤 Поделиться кодом', callback_data: 'share_ref' }],
                [
                  { text: '🔄 Обновить', callback_data: 'mystats' },
                  { text: '← Меню', callback_data: 'menu' }
                ]
              ]
            }
          });
          break;
        }
        
        case 'referral': {
          // Информация о реферальной программе
          const refCode = await getUserReferralCode(env, telegramId);
          const stats = await getUserStats(env, telegramId);
          const botUsername = env.BOT_USERNAME || 'clickuved_bot';
          
          const paidBonuses = stats.referralPaidBonus || 0;
          const pendingBonuses = stats.referralPendingBonus || 0;
          
          const refMsg = `🎟️ *Реферальная программа*\n\n` +
            `Приглашайте друзей и получайте\n` +
            `*+1 день* к подписке!\n\n` +
            `ℹ️ *Как это работает:*\n` +
            `1\ufe0f⃣ Друг переходит по вашей ссылке\n` +
            `2\ufe0f⃣ Друг получает 1 день пробника\n` +
            `3\ufe0f⃣ Когда друг оформит подписку — вы получаете +1 день!\n\n` +
            `📊 *Ваши рефералы:* ${stats.referralCount || 0}\n` +
            `🎁 *Получено бонусов:* ${paidBonuses} дн.\n` +
            (pendingBonuses > 0 ? `⏳ *Ожидают оформления:* ${pendingBonuses}\n` : '') +
            `\n🎟️ *Ваш код:* \`${refCode}\`\n\n` +
            `👉 Ссылка для друзей:\n` +
            `\`https://t.me/${botUsername}?start=ref_${refCode}\``;
          
          await editTelegramMessage(env, chatId, messageId, refMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '📤 Поделиться', callback_data: 'share_ref' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'settings': {
          // Настройки пользователя - меню уведомлений
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          
          if (!oderId) {
            await editTelegramMessage(env, chatId, messageId,
              `⚙️ *Настройки*\n\n` +
              `❌ Расширение не подключено\n\n` +
              `Подключите расширение для доступа к настройкам.`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '🔗 Подключить', callback_data: 'how_to_connect' }],
                  [{ text: '← Меню', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          const result = await getLicenseData(env, oderId);
          const userSettings = await getUserSettings(env, telegramId);
          const notif = userSettings.notifications;
          const licenseType = result.license ? result.license.type : null;
          const canUseGroups = result.license && LICENSE_TYPES[licenseType]?.groupChats;
          const canChooseDest = licenseType === 'level3'; // Только level3 может выбирать
          
          // Иконки статуса
          const icon = (enabled) => enabled ? '✅' : '❌';
          
          // Описание куда приходят уведомления
          let destText = '';
          if (licenseType === 'trial' || licenseType === 'level1') {
            destText = `📍 Уведомления: в личные сообщения`;
          } else if (licenseType === 'level2') {
            destText = `📍 Уведомления: в групповой чат`;
          } else if (licenseType === 'level3') {
            const destMap = { dm: 'ЛС', group: 'Группа', both: 'ЛС + Группа' };
            const currentDest = userSettings.notifyDestination || 'dm';
            destText = `📍 Уведомления: ${destMap[currentDest] || 'ЛС'}`;
          }
          
          const settingsMsg = `⚙️ *Настройки уведомлений*\n\n` +
            `${icon(notif.clicks)} Уведомления о заявках\n` +
            `${icon(notif.expiryReminders)} Напоминания о подписке\n` +
            `${icon(notif.news)} Новости и обновления\n\n` +
            (destText ? `${destText}\n\n` : '') +
            `_Нажмите на кнопку чтобы переключить_`;
          
          const keyboard = [
            [
              { text: `${icon(notif.clicks)} Заявки`, callback_data: 'toggle_notif_clicks' },
              { text: `${icon(notif.expiryReminders)} Подписка`, callback_data: 'toggle_notif_expiry' }
            ],
            [{ text: `${icon(notif.news)} Новости`, callback_data: 'toggle_notif_news' }],
            [{ text: '🔔 Тест уведомлений', callback_data: 'test_notify' }]
          ];
          
          // Выбор места уведомлений для level3
          if (canChooseDest) {
            keyboard.push([{ text: '📍 Куда отправлять', callback_data: 'notify_dest_menu' }]);
          }
          
          if (canUseGroups) {
            keyboard.push([{ text: '💬 Групповые чаты', callback_data: 'groups_menu' }]);
          }
          
          keyboard.push([{ text: '← Меню', callback_data: 'menu' }]);
          
          await editTelegramMessage(env, chatId, messageId, settingsMsg, {
            reply_markup: { inline_keyboard: keyboard }
          });
          break;
        }
        
        // Меню выбора места отправки уведомлений (только level3)
        case 'notify_dest_menu': {
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          const result = await getLicenseData(env, oderId || '');
          
          if (!result.license || result.license.type !== 'level3') {
            await answerCallbackQuery(env, callback.id, 'Доступно только для Уровень 3', true);
            break;
          }
          
          const userSettings = await getUserSettings(env, telegramId);
          const currentDest = userSettings.notifyDestination || 'dm';
          
          const check = (val) => currentDest === val ? '✅ ' : '';
          
          const msg = `📍 *Куда отправлять уведомления?*\n\n` +
            `Выберите, куда будут приходить уведомления о заявках:\n\n` +
            `• *ЛС* — в личные сообщения бота\n` +
            `• *Группа* — в подключённый групповой чат\n` +
            `• *Оба* — и туда, и туда`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [
                { text: `${check('dm')}📱 ЛС`, callback_data: 'set_dest_dm' },
                { text: `${check('group')}💬 Группа`, callback_data: 'set_dest_group' }
              ],
              [{ text: `${check('both')}📱+💬 Оба`, callback_data: 'set_dest_both' }],
              [{ text: '← Настройки', callback_data: 'settings' }]
            ]}
          });
          break;
        }
        
        case 'set_dest_dm':
        case 'set_dest_group':
        case 'set_dest_both': {
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          const result = await getLicenseData(env, oderId || '');
          
          if (!result.license || result.license.type !== 'level3') {
            await answerCallbackQuery(env, callback.id, 'Доступно только для Уровень 3', true);
            break;
          }
          
          const destMap = {
            'set_dest_dm': 'dm',
            'set_dest_group': 'group',
            'set_dest_both': 'both'
          };
          
          const newDest = destMap[action];
          const userSettings = await getUserSettings(env, telegramId);
          userSettings.notifyDestination = newDest;
          await saveUserSettings(env, telegramId, userSettings);
          
          const destNames = { dm: 'ЛС', group: 'Группа', both: 'ЛС + Группа' };
          await answerCallbackQuery(env, callback.id, `✅ Уведомления: ${destNames[newDest]}`);
          
          // Возвращаемся в настройки
          // Перенаправляем обратно в меню
          const check = (val) => newDest === val ? '✅ ' : '';
          
          const msg = `📍 *Куда отправлять уведомления?*\n\n` +
            `✅ Выбрано: *${destNames[newDest]}*`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [
                { text: `${check('dm')}📱 ЛС`, callback_data: 'set_dest_dm' },
                { text: `${check('group')}💬 Группа`, callback_data: 'set_dest_group' }
              ],
              [{ text: `${check('both')}📱+💬 Оба`, callback_data: 'set_dest_both' }],
              [{ text: '← Настройки', callback_data: 'settings' }]
            ]}
          });
          break;
        }
        
        // Переключатели уведомлений
        case 'toggle_notif_clicks':
        case 'toggle_notif_expiry':
        case 'toggle_notif_news': {
          const settingMap = {
            'toggle_notif_clicks': 'clicks',
            'toggle_notif_expiry': 'expiryReminders',
            'toggle_notif_news': 'news'
          };
          
          const settingKey = settingMap[action];
          const updatedSettings = await toggleNotificationSetting(env, telegramId, settingKey);
          const notif = updatedSettings.notifications;
          
          const icon = (enabled) => enabled ? '✅' : '❌';
          
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          const result = await getLicenseData(env, oderId || '');
          const licenseType = result.license ? result.license.type : null;
          const canUseGroups = result.license && LICENSE_TYPES[licenseType]?.groupChats;
          const canChooseDest = licenseType === 'level3';
          
          // Описание куда приходят уведомления
          let destText = '';
          if (licenseType === 'trial' || licenseType === 'level1') {
            destText = `📍 Уведомления: в личные сообщения`;
          } else if (licenseType === 'level2') {
            destText = `📍 Уведомления: в групповой чат`;
          } else if (licenseType === 'level3') {
            const destMap = { dm: 'ЛС', group: 'Группа', both: 'ЛС + Группа' };
            const currentDest = updatedSettings.notifyDestination || 'dm';
            destText = `📍 Уведомления: ${destMap[currentDest] || 'ЛС'}`;
          }
          
          const settingsMsg = `⚙️ *Настройки уведомлений*\n\n` +
            `${icon(notif.clicks)} Уведомления о заявках\n` +
            `${icon(notif.expiryReminders)} Напоминания о подписке\n` +
            `${icon(notif.news)} Новости и обновления\n\n` +
            (destText ? `${destText}\n\n` : '') +
            `_Настройка изменена!_`;
          
          const keyboard = [
            [
              { text: `${icon(notif.clicks)} Заявки`, callback_data: 'toggle_notif_clicks' },
              { text: `${icon(notif.expiryReminders)} Подписка`, callback_data: 'toggle_notif_expiry' }
            ],
            [{ text: `${icon(notif.news)} Новости`, callback_data: 'toggle_notif_news' }],
            [{ text: '🔔 Тест уведомлений', callback_data: 'test_notify' }]
          ];
          
          if (canChooseDest) {
            keyboard.push([{ text: '📍 Куда отправлять', callback_data: 'notify_dest_menu' }]);
          }
          
          if (canUseGroups) {
            keyboard.push([{ text: '💬 Групповые чаты', callback_data: 'groups_menu' }]);
          }
          
          keyboard.push([{ text: '← Меню', callback_data: 'menu' }]);
          
          await editTelegramMessage(env, chatId, messageId, settingsMsg, {
            reply_markup: { inline_keyboard: keyboard }
          });
          
          await answerCallbackQuery(env, callback.id, 'Настройка изменена!');
          break;
        }
        
        case 'groups_menu': {
          // Меню групповых чатов
          const canUse = await canUseGroupChats(env, telegramId);
          
          if (!canUse.allowed) {
            let errorMsg = '';
            switch (canUse.reason) {
              case 'NOT_CONNECTED':
                errorMsg = '❌ Сначала привяжите расширение';
                break;
              case 'NO_LICENSE':
                errorMsg = '❌ Нет активной подписки';
                break;
              case 'UPGRADE_REQUIRED':
                errorMsg = `❌ Доступно только для *Уровень 2* и *Уровень 3*\n\nВаша подписка: ${canUse.currentType || 'Базовая'}`;
                break;
              default:
                errorMsg = '❌ Нет доступа к групповым чатам';
            }
            
            await editTelegramMessage(env, chatId, messageId, 
              `💬 *Групповые чаты*\n\n${errorMsg}`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '💳 Улучшить подписку', callback_data: 'buy' }],
                  [{ text: '← Меню', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          const groupData = await getUserGroupChats(env, telegramId);
          
          let message = `💬 *Групповые чаты*\n\n`;
          
          if (groupData.chats.length === 0) {
            message += `У вас пока нет подключённых групп.\n\n`;
            message += `*Как подключить:*\n`;
            message += `1️⃣ Добавьте бота @clickuved\\_bot в группу\n`;
            message += `2️⃣ Бот запросит одобрение администратора\n`;
            message += `3️⃣ После одобрения уведомления будут приходить в группу`;
          } else {
            message += `*Подключённые группы (${groupData.chats.length}):*\n\n`;
            groupData.chats.forEach((chat, i) => {
              message += `${i + 1}. ${chat.chatTitle}\n`;
            });
            message += `\n_Лимит: ${canUse.maxChats} группы_`;
          }
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '📖 Инструкция', callback_data: 'groups_help' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'groups_help': {
          const helpMsg = `📖 *Как подключить групповой чат*\n\n` +
            `1️⃣ Создайте группу в Telegram\n` +
            `2️⃣ Добавьте бота @${env.BOT_USERNAME || 'clickuved_bot'}\n` +
            `3️⃣ Бот отправит запрос администратору\n` +
            `4️⃣ После одобрения уведомления\n     будут приходить в группу\n\n` +
            `⚠️ *Лимит:* 1 группа на аккаунт\n` +
            `💎 *Требуется:* Premium или Максимум`;
          
          await editTelegramMessage(env, chatId, messageId, helpMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '← Назад', callback_data: 'groups_menu' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'faq': {
          const faqMsg = `❓ *Частые вопросы*

*🔒 Безопасно ли расширение?*
Да, работает только на exotic.company
Не собирает личные данные

*📱 Сколько устройств?*
• Премиум: 1 устройство
• Премиум+: до 3 устройств
• Максимум: до 5 устройств

*⏰ Что если подписка истечёт?*
Автоклик остановится
Данные и статистика сохранятся

*💰 Есть возврат?*
Да, в течение 24ч если не работали

*🎟️ Есть пробный период?*
Да, 24 часа после подключения

*💳 Какие тарифы?*
• Премиум: 30 USDT/мес
• Премиум+: 40 USDT/мес

*🔄 Как продлить подписку?*
Напишите @YaMob`;
          
          await editTelegramMessage(env, chatId, messageId, faqMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '💳 Тарифы', callback_data: 'buy' }],
                [{ text: '💬 Задать вопрос', url: 'https://t.me/YaMob' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'install': {
          const installMsg = `📥 *Установка расширения*

*Chrome / Edge / Brave / Opera:*
1️⃣ Откройте Chrome Web Store
2️⃣ Найдите "Exotic Assistant"
3️⃣ Нажмите "Установить"
4️⃣ Подтвердите установку
5️⃣ Готово! ✅

⚙️ *После установки:*
• Нажмите на иконку расширения
• Откройте "Настройки"
• Подключите Telegram
• Получите 24ч бесплатно! 🎁`;
          
          await editTelegramMessage(env, chatId, messageId, installMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '🌐 Chrome Web Store', url: 'https://chromewebstore.google.com/' }],
                [{ text: '🔗 Подключить Telegram', callback_data: 'how_to_connect' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'news': {
          const newsMsg = `📰 *Новости и обновления*

*v${BOT_VERSION} — Январь 2026*
━━━━━━━━━━━━━━━━━━━━
✅ Улучшен автокликер (100% надёжность)
✅ Добавлены уведомления об истечении
✅ Inline режим для шаринга
✅ Rate limiting от спама
✅ Новые разделы в меню

*Планируется:*
🔜 Статистика по дням
🔜 Экспорт данных
🔜 Тёмная тема

💡 Есть идеи? Пишите @YaMob`;
          
          await editTelegramMessage(env, chatId, messageId, newsMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '💬 Предложить идею', url: 'https://t.me/YaMob' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'share_ref': {
          // Получаем реферальный код и отправляем сообщение для пересылки
          const refCode = await getUserReferralCode(env, telegramId);
          const botUsername = env.BOT_USERNAME || 'clickuved_bot';
          
          const shareMsg = `🎁 *Приглашение в Exotic Assistant*\n\n` +
            `Привет! Попробуй автокликер для Exotic.\n\n` +
            `🎟️ Мой реферальный код: \`${refCode}\`\n\n` +
            `👉 Перейди к боту: @${botUsername}\n` +
            `И введи команду: \`/ref ${refCode}\``;
          
          await sendTelegramMessage(env, chatId, shareMsg);
          await answerCallbackQuery(env, callback.id, 'Сообщение отправлено, перешлите его другу!');
          break;
        }
        
        case 'test_notify': {
          // Проверяем привязку и лицензию через getLicenseData
          const testOderId = await env.USERS.get(`tg:${telegramId}`);
          
          if (!testOderId) {
            await editTelegramMessage(env, chatId, messageId,
              `🔔 *Тест уведомлений*\n\n` +
              `❌ Расширение не подключено\n\n` +
              `Чтобы получать уведомления о заявках, \nсначала подключите расширение.`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '🔗 Как подключить', callback_data: 'how_to_connect' }],
                  [{ text: '← Меню', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          // Используем getLicenseData для корректной проверки лицензии
          const testResult = await getLicenseData(env, testOderId);
          
          if (!testResult.license) {
            await editTelegramMessage(env, chatId, messageId,
              `🔔 *Тест уведомлений*\n\n` +
              `❌ Нет активной подписки\n\n` +
              `Для получения уведомлений о заявках\nнужна активная подписка.`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '💳 Купить подписку', callback_data: 'buy' }],
                  [{ text: '💬 Поддержка', callback_data: 'support' }],
                  [{ text: '← Меню', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          if (testResult.license.expiresAt < Date.now()) {
            await editTelegramMessage(env, chatId, messageId,
              `🔔 *Тест уведомлений*\n\n` +
              `❌ Подписка истекла\n\n` +
              `Продлите подписку для получения\nуведомлений о новых заявках.`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '💳 Продлить подписку', callback_data: 'buy' }],
                  [{ text: '← Меню', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          // Отправляем тестовое уведомление
          const time = new Date().toLocaleString('ru-RU', { 
            timeZone: 'Europe/Moscow',
            hour: '2-digit', 
            minute: '2-digit'
          });
          
          const testMsg = `🔔 *Новая заявка!*\n\n` +
            `💰 Сумма: *1 000 ₽*\n` +
            `💳 Способ: Тестовая карта\n` +
            `🔢 Реквизиты: \`0000 0000 0000 0000\`\n` +
            `🏦 Банк: Тест\n\n` +
            `⏰ ${time} (МСК)`;
          
          await sendTelegramMessage(env, chatId, testMsg);
          
          await editTelegramMessage(env, chatId, messageId,
            `✅ *Тест успешен!*\n\n` +
            `Тестовое уведомление отправлено.\nЕсли видите его выше — всё работает! 👆`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '🔔 Повторить тест', callback_data: 'test_notify' }],
                [{ text: '📊 Мой статус', callback_data: 'status' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }}
          );
          break;
        }
        
        // === СЕКРЕТНАЯ АДМИН-ПАНЕЛЬ (CONTROL PANEL) ===
        // Перенаправление старых cp_* на новые adm_*
        case 'cp_pending_groups': {
          // Редирект на новый обработчик
          action = 'adm_pending';
          // fall through
        }
        
        case 'cp_users': {
          // Редирект на новый обработчик
          action = 'adm_users';
          // fall through
        }
        
        case 'cp_stats': {
          // Редирект на новый обработчик
          action = 'adm_stats';
          // fall through - обработается в adm_stats
        }
        
        case 'cp_grant': {
          // Редирект на новый обработчик
          action = 'adm_grant';
          // fall through
        }
        
        case 'cp_check': {
          // Редирект на новый обработчик
          action = 'adm_search';
          // fall through
        }
        
        case 'cp_broadcast': {
          // Редирект на новый обработчик
          action = 'adm_broadcast';
          // fall through
        }
        
        case 'cp_system': {
          try {
            if (!await checkAndExtendAdminSession(env, telegramId)) {
              await showSessionExpiredMessage(env, chatId, messageId);
              break;
            }
            const uptimeMs = Date.now() - metrics.startTime;
            const uptimeHours = Math.floor(uptimeMs / (60 * 60 * 1000));
            const uptimeMinutes = Math.floor((uptimeMs % (60 * 60 * 1000)) / (60 * 1000));
            const msg = `🔧 *Система*\n\n` +
              `📦 Версия бота: ${BOT_VERSION}\n` +
              `📦 Версия расширения: ${EXTENSION_VERSION}\n` +
              `⏱ Uptime: ${uptimeHours}ч ${uptimeMinutes}м\n\n` +
              `*Конфигурация:*\n` +
              `├ BOT\_USERNAME: ${env.BOT_USERNAME || 'не задан'}\n` +
              `├ ADMIN\_USERNAME: ${env.ADMIN_USERNAME || 'не задан'}\n` +
              `├ LOG\_CHAT\_ID: ${env.LOG_CHAT_ID ? '✅' : '❌'}\n` +
              `├ ADMIN\_TELEGRAM\_ID: ${env.ADMIN_TELEGRAM_ID ? '✅' : '❌'}\n` +
              `└ WEBHOOK\_SECRET: ${env.TELEGRAM_WEBHOOK_SECRET ? '✅' : '❌'}`;
            try {
              await editTelegramMessage(env, chatId, messageId, msg, {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '📋 Логи', callback_data: 'cp_logs' }],
                    [{ text: '🔄 Webhook Info', callback_data: 'cp_webhook' }],
                    [{ text: '← Назад', callback_data: 'adm_back' }]
                  ]
                }
              });
            } catch (err) {
              const stats = await getAdminStats(env);
              const pendingGroups = await getAllPendingGroups(env);
              await handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups);
            }
          } catch (err) {
            const stats = await getAdminStats(env);
            const pendingGroups = await getAllPendingGroups(env);
            await handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups);
          }
          break;
        }
        
        case 'cp_logs': {
          try {
            if (!await checkAndExtendAdminSession(env, telegramId)) {
              await showSessionExpiredMessage(env, chatId, messageId);
              break;
            }
            let msg = `📋 *Последние ошибки*\n\n`;
            if (metrics.errors.length === 0) {
              msg += `_Ошибок нет_ ✨`;
            } else {
              const recentErrors = metrics.errors.slice(-10);
              for (const err of recentErrors) {
                const time = new Date(err.time).toLocaleTimeString('ru-RU');
                msg += `🔴 ${time} - ${err.endpoint}\n`;
                msg += `\`${err.error.substring(0, 50)}\`\n\n`;
              }
            }
            try {
              await editTelegramMessage(env, chatId, messageId, msg, {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '🔄 Обновить', callback_data: 'cp_logs' }],
                    [{ text: '← Назад', callback_data: 'adm_back' }]
                  ]
                }
              });
            } catch (err) {
              const stats = await getAdminStats(env);
              const pendingGroups = await getAllPendingGroups(env);
              await handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups);
            }
          } catch (err) {
            const stats = await getAdminStats(env);
            const pendingGroups = await getAllPendingGroups(env);
            await handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups);
          }
          break;
        }
        
        case 'cp_webhook': {
          try {
            if (!await checkAndExtendAdminSession(env, telegramId)) {
              await showSessionExpiredMessage(env, chatId, messageId);
              break;
            }
            try {
              const response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getWebhookInfo`);
              const data = await response.json();
              const webhookInfo = data.result || {};
              const msg = `🔗 *Webhook Info*\n\n` +
                `URL: \`${webhookInfo.url || 'не установлен'}\`\n` +
                `Pending: ${webhookInfo.pending_update_count || 0}\n` +
                `Last Error: ${webhookInfo.last_error_message || 'нет'}\n` +
                `Max Connections: ${webhookInfo.max_connections || 40}`;
              await editTelegramMessage(env, chatId, messageId, msg, {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '🔄 Обновить', callback_data: 'cp_webhook' }],
                    [{ text: '← Назад', callback_data: 'adm_back' }]
                  ]
                }
              });
            } catch (err) {
              const stats = await getAdminStats(env);
              const pendingGroups = await getAllPendingGroups(env);
              await handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups);
            }
          } catch (err) {
            await answerCallbackQuery(env, callback.id, 'Ошибка получения webhook info', true);
          }
          break;
        }
        
        case 'cp_logout': {
          await destroyAdminSession(env, telegramId);
          await editTelegramMessage(env, chatId, messageId,
            `🚪 *Сессия завершена*\n\n` +
            `Вы вышли из панели управления.`,
            { reply_markup: getMainMenuKeyboard() }
          );
          break;
        }
        
        case 'cp_back': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const stats = await getAdminStats(env);
          const pendingGroups = await getAllPendingGroups(env);
          await showAdminPanel(env, chatId, stats, pendingGroups, messageId);
          break;
        }
        
        // === НОВЫЕ ОБРАБОТЧИКИ АДМИН-ПАНЕЛИ (adm_*) ===
        case 'adm_pending': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const pendingGroups = await getAllPendingGroups(env);
          if (pendingGroups.length === 0) {
            await editTelegramMessage(env, chatId, messageId,
              `✅ *Нет ожидающих заявок*\n\n` +
              `Все заявки на подключение чатов обработаны.`,
              { reply_markup: { inline_keyboard: [[{ text: '← Назад', callback_data: 'adm_back' }]] }}
            );
            break;
          }
          
          let msg = `🔔 *Заявки на чаты (${pendingGroups.length})*\n` +
            `━━━━━━━━━━━━━━━━━━━━\n\n`;
          const buttons = [];
          
          for (const group of pendingGroups.slice(0, 5)) {
            const requestDate = new Date(group.requestedAt).toLocaleDateString('ru-RU');
            const safeTitle = escapeMarkdown(group.chatTitle || 'Без названия');
            msg += `📌 *${safeTitle}*\n`;
            msg += `├ ID: \`${group.chatId}\`\n`;
            msg += `├ От: @${group.requestedByUsername || 'unknown'}\n`;
            msg += `├ TG ID: \`${group.requestedBy}\`\n`;
            msg += `└ Дата: ${requestDate}\n\n`;
            
            buttons.push([
              { text: `✅ Одобрить`, callback_data: `approve_group:${group.chatId}` },
              { text: `❌ Отклонить`, callback_data: `reject_group:${group.chatId}` }
            ]);
          }
          
          if (pendingGroups.length > 5) {
            msg += `\n_... и ещё ${pendingGroups.length - 5} заявок_`;
          }
          
          buttons.push([{ text: '← Назад', callback_data: 'adm_back' }]);
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: buttons }
          });
          break;
        }
        
        case 'adm_grant': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const msg = `🎁 *Выдача подписки*\n` +
            `━━━━━━━━━━━━━━━━━━━━\n\n` +
            `*Уровни подписок:*\n` +
            `• \`trial\` — Пробный (1 день)\n` +
            `• \`level1\` — Уровень 1 (только ЛС)\n` +
            `• \`level2\` — Уровень 2 (ЛС + чат)\n` +
            `• \`level3\` — Уровень 3 (полный)\n\n` +
            `*Команды:*\n` +
            `\`/grant ID дней уровень\`\n\n` +
            `*Примеры:*\n` +
            `\`/grant 123456789 30 level1\`\n` +
            `\`/grant 123456789 30 level2\`\n` +
            `\`/grant 123456789 30 level3\``;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '← Назад', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_search': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const msg = `🔍 *Поиск пользователя*\n` +
            `━━━━━━━━━━━━━━━━━━━━\n\n` +
            `Отправьте команду:\n` +
            `\`/check 123456789\`\n\n` +
            `Или перешлите сообщение пользователя.`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '← Назад', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_users': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const users = await getAllLicensedUsers(env, 15);
          const now = Date.now();
          
          let msg = `👥 *Активные подписки*\n` +
            `━━━━━━━━━━━━━━━━━━━━\n\n`;
          
          if (users.length === 0) {
            msg += `_Нет активных подписок_`;
          } else {
            for (const user of users) {
              const daysLeft = Math.ceil((user.expiresAt - now) / (24 * 60 * 60 * 1000));
              const status = user.isActive ? '✅' : '❌';
              const typeInfo = LICENSE_TYPES[user.type] || { name: user.type };
              msg += `${status} \`${user.telegramId}\`\n`;
              msg += `   ${typeInfo.name} • ${daysLeft > 0 ? daysLeft + 'д' : 'истекла'}\n`;
            }
          }
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '🔄 Обновить', callback_data: 'adm_users' }],
              [{ text: '← Назад', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_chats': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          // Получаем одобренные чаты
          const groupOwnersList = await env.USERS.list({ prefix: 'group_owner:', limit: 20 });
          
          let msg = `💬 *Одобренные чаты*\n` +
            `━━━━━━━━━━━━━━━━━━━━\n\n`;
          
          if (groupOwnersList.keys.length === 0) {
            msg += `_Нет одобренных чатов_`;
          } else {
            for (const key of groupOwnersList.keys) {
              const chatId2 = key.name.replace('group_owner:', '');
              const ownerData = await env.USERS.get(key.name);
              if (ownerData) {
                const owner = JSON.parse(ownerData);
                msg += `📌 \`${chatId2}\`\n`;
                msg += `   Владелец: \`${owner.telegramId || 'unknown'}\`\n`;
              }
            }
          }
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '🔄 Обновить', callback_data: 'adm_chats' }],
              [{ text: '← Назад', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_broadcast': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const msg = `📢 *Рассылка*\n` +
            `━━━━━━━━━━━━━━━━━━━━\n\n` +
            `Отправьте команду:\n` +
            `\`/broadcast Текст сообщения\`\n\n` +
            `⚠️ _Будет отправлено всем активным пользователям_`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '← Назад', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_stats': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const stats = await getAdminStats(env);
          const uptimeMs = Date.now() - metrics.startTime;
          const uptimeHours = Math.floor(uptimeMs / (60 * 60 * 1000));
          
          const msg = `📈 *Детальная статистика*\n` +
            `━━━━━━━━━━━━━━━━━━━━\n\n` +
            `👥 *Пользователи:*\n` +
            `├ Всего: ${stats.totalUsers}\n` +
            `├ Активных: ${stats.activeUsers}\n` +
            `├ 🎁 Trial: ${stats.trialUsers}\n` +
            `├ ⭐ Level 1: ${stats.level1Users}\n` +
            `├ ⭐⭐ Level 2: ${stats.level2Users}\n` +
            `└ ⭐⭐⭐ Level 3: ${stats.level3Users}\n\n` +
            `💬 *Чаты:*\n` +
            `├ Ожидают: ${stats.pendingGroups}\n` +
            `└ Одобрено: ${stats.approvedGroups}\n\n` +
            `🔧 *Система:*\n` +
            `├ Uptime: ${uptimeHours}ч\n` +
            `├ Запросов: ${metrics.requests.total}\n` +
            `└ Ошибок: ${metrics.requests.error}`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '🔄 Обновить', callback_data: 'adm_stats' }],
              [{ text: '← Назад', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_refresh': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const stats = await getAdminStats(env);
          const pendingGroups = await getAllPendingGroups(env);
          await showAdminPanel(env, chatId, stats, pendingGroups, messageId);
          await answerCallbackQuery(env, callback.id, '✅ Обновлено');
          break;
        }
        
        case 'adm_logout': {
          await destroyAdminSession(env, telegramId);
          await editTelegramMessage(env, chatId, messageId,
            `🚪 *Сессия завершена*\n\n` +
            `Вы вышли из админ-панели.`,
            { reply_markup: { inline_keyboard: [[{ text: '← Меню', callback_data: 'menu' }]] }}
          );
          break;
        }
        
        case 'adm_system': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const uptimeMs = Date.now() - metrics.startTime;
          const uptimeHours = Math.floor(uptimeMs / (60 * 60 * 1000));
          const uptimeMinutes = Math.floor((uptimeMs % (60 * 60 * 1000)) / (60 * 1000));
          
          const msg = `🔧 *Система*\n` +
            `━━━━━━━━━━━━━━━━━━━━\n\n` +
            `📦 *Версии:*\n` +
            `├ Бот: ${BOT_VERSION}\n` +
            `└ Расширение: ${EXTENSION_VERSION}\n\n` +
            `⏱ *Uptime:* ${uptimeHours}ч ${uptimeMinutes}м\n\n` +
            `⚙️ *Конфигурация:*\n` +
            `├ LOG\\_CHAT: ${env.LOG_CHAT_ID ? '✅' : '❌'}\n` +
            `├ ADMIN\\_TG\\_ID: ${env.ADMIN_TELEGRAM_ID ? '✅' : '❌'}\n` +
            `└ WEBHOOK\\_SECRET: ${env.TELEGRAM_WEBHOOK_SECRET ? '✅' : '❌'}\n\n` +
            `📈 *API:*\n` +
            `├ Запросов: ${metrics.requests.total}\n` +
            `└ Ошибок: ${metrics.requests.error}`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '📋 Логи ошибок', callback_data: 'cp_logs' }],
              [{ text: '🔗 Webhook Info', callback_data: 'cp_webhook' }],
              [{ text: '🔄 Обновить', callback_data: 'adm_system' }],
              [{ text: '← Назад', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_back': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const stats = await getAdminStats(env);
          const pendingGroups = await getAllPendingGroups(env);
          await showAdminPanel(env, chatId, stats, pendingGroups, messageId);
          break;
        }
        
        // === СТАРЫЕ АДМИНСКИЕ CALLBACK (для совместимости) ===
        case 'admin_back': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, 'Нет доступа', true);
            break;
          }
          
          const adminMsg = `👑 *Админ-панель*

/grant ID 7 — выдать
/revoke ID — отозвать
/check ID — проверить`;
          
          await editTelegramMessage(env, chatId, messageId, adminMsg, {
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '🔍 Проверить', callback_data: 'admin_check_prompt' },
                  { text: '🎁 Выдать', callback_data: 'admin_grant_prompt' }
                ],
                [{ text: '📈 Статистика', callback_data: 'admin_stats' }],
                [{ text: '← Меню', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'admin_check_prompt': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, 'Нет доступа', true);
            break;
          }
          
          await editTelegramMessage(env, chatId, messageId, 
            `🔍 *Проверка пользователя*\n\n` +
            `Отправьте команду:\n` +
            `\`/check 123456789\` — по ID`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '← Админ', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        case 'admin_grant_prompt': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, 'Нет доступа', true);
            break;
          }
          
          await editTelegramMessage(env, chatId, messageId, 
            `🎁 *Выдача подписки*\n\n` +
            `\`/grant ID 7\` — 7 дней\n` +
            `\`/grant ID 30\` — 30 дней\n` +
            `\`/grant 123456 90 level2\` — по ID`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '← Админ', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        case 'admin_users': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, 'Нет доступа', true);
            break;
          }
          
          await editTelegramMessage(env, chatId, messageId, 
            `📊 *Пользователи*\n\n` +
            `\`/check 123456789\` — по ID`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '← Админ', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        case 'admin_stats': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, 'Нет доступа', true);
            break;
          }
          
          const now = new Date().toLocaleString('ru-RU', { 
            timeZone: 'Europe/Moscow',
            day: '2-digit',
            month: '2-digit',
            hour: '2-digit',
            minute: '2-digit'
          });
          
          const botUsername = env.BOT_USERNAME || 'clickuved_bot';
          
          await editTelegramMessage(env, chatId, messageId, 
            `📈 *Статистика бота*\n\n` +
            `🤖 @${botUsername.replace(/_/g, '\\_')}\n` +
            `📦 Версия: ${BOT_VERSION}\n` +
            `🕐 ${now} MSK\n` +
            `✅ KV: Работает`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '🔄 Обновить', callback_data: 'admin_stats' }],
                [{ text: '← Админ', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        default: {
          // Обработка remove_device:deviceId
          if (data.startsWith('remove_device:')) {
            const deviceIdPart = data.split(':')[1];
            
            // Находим устройство по частичному ID
            const devicesData = await getUserDevices(env, telegramId);
            const devices = devicesData.devices || [];
            const device = devices.find(d => d.deviceId.startsWith(deviceIdPart));
            
            if (device) {
              const result = await removeDevice(env, telegramId, device.deviceId);
              if (result.success) {
                await answerCallbackQuery(env, callback.id, '✅ Устройство отвязано', false);
              } else {
                await answerCallbackQuery(env, callback.id, '❌ Ошибка', true);
              }
            } else {
              await answerCallbackQuery(env, callback.id, '❌ Устройство не найдено', true);
            }
            
            // Обновляем список устройств
            const newDevicesData = await getUserDevices(env, telegramId);
            const newDevices = newDevicesData.devices || [];
            
            let maxDevices = 1;
            const oderId = await env.USERS.get(`tg:${telegramId}`);
            if (oderId) {
              const licResult = await getLicenseData(env, oderId);
              if (licResult.license) {
                const licType = LICENSE_TYPES[licResult.license.type] || LICENSE_TYPES.trial;
                maxDevices = licType.maxDevices || 1;
              }
            }
            
            let message = `📱 *Мои устройства* (${newDevices.length}/${maxDevices})\n\n`;
            
            if (newDevices.length === 0) {
              message += `_Нет привязанных устройств_`;
            } else {
              newDevices.forEach((d, index) => {
                const shortId = d.deviceId.substring(0, 12) + '...';
                const addedDate = new Date(d.addedAt).toLocaleDateString('ru-RU');
                message += `${index + 1}. \`${shortId}\`\n`;
                message += `   📅 Добавлено: ${addedDate}\n\n`;
              });
            }
            
            const keyboard = { inline_keyboard: [] };
            newDevices.forEach((d, index) => {
              keyboard.inline_keyboard.push([
                { text: `🗑 Отвязать устройство ${index + 1}`, callback_data: `remove_device:${d.deviceId.substring(0, 32)}` }
              ]);
            });
            keyboard.inline_keyboard.push([{ text: '← Статус', callback_data: 'status' }]);
            
            await editTelegramMessage(env, chatId, messageId, message, { reply_markup: keyboard });
            break;
          }
          
          // Обработка approve_group:chatId и reject_group:chatId
          if (data.startsWith('approve_group:') || data.startsWith('reject_group:')) {
            // Проверяем сессию админа
            if (!await checkAndExtendAdminSession(env, telegramId)) {
              await showSessionExpiredMessage(env, chatId, messageId);
              break;
            }
            
            const groupChatId = data.split(':')[1];
            const isApprove = data.startsWith('approve_group:');
            
            if (isApprove) {
              const result = await approveGroupChat(env, groupChatId, username);
              
              if (result.success) {
                // Уведомляем пользователя
                if (result.telegramId) {
                  await sendTelegramMessage(env, result.telegramId,
                    `✅ *Группа одобрена!*\n\n` +
                    `Ваш групповой чат был одобрен.\n` +
                    `Теперь вы будете получать уведомления о заявках в группу.`
                  );
                }
                
                // Уведомляем в группу
                await sendTelegramMessage(env, groupChatId,
                  `✅ *Группа активирована!*\n\n` +
                  `Теперь сюда будут приходить уведомления о заявках.`
                );
                
                await editTelegramMessage(env, chatId, messageId,
                  `✅ *Группа одобрена*\n\n` +
                  `Chat ID: \`${groupChatId}\`\n` +
                  `Одобрил: @${username}`,
                  { reply_markup: { inline_keyboard: [[{ text: '← Админ-панель', callback_data: 'adm_back' }]] }}
                );
              } else {
                await sendTelegramMessage(env, chatId, 
                  `❌ Ошибка: ${result.error}`
                );
              }
            } else {
              // Отклонение
              const pendingKey = `pending_group:${groupChatId}`;
              const pendingData = await env.USERS.get(pendingKey);
              
              if (pendingData) {
                const request = JSON.parse(pendingData);
                await env.USERS.delete(pendingKey);
                
                // Уведомляем пользователя
                if (request.requestedBy) {
                  await sendTelegramMessage(env, request.requestedBy,
                    `❌ *Запрос отклонён*\n\n` +
                    `Ваш запрос на групповой чат был отклонён.\n` +
                    `Для уточнения напишите @YaMob`
                  );
                }
                
                // Уведомляем в группу
                await sendTelegramMessage(env, groupChatId,
                  `❌ *Запрос отклонён*\n\n` +
                  `Администратор отклонил запрос на уведомления.`
                );
                
                await editTelegramMessage(env, chatId, messageId,
                  `❌ *Группа отклонена*\n\n` +
                  `Chat ID: \`${groupChatId}\`\n` +
                  `Отклонил: @${username}`,
                  { reply_markup: { inline_keyboard: [[{ text: '← Админ-панель', callback_data: 'adm_back' }]] }}
                );
              } else {
                await sendTelegramMessage(env, chatId, 'Запрос не найден');
              }
            }
            break;
          }
          
          // Проверяем админские callback с параметрами
          if (data.startsWith('grant_') || data.startsWith('revoke_')) {
            const callbackTelegramId = String(callback.from.id);
            
            if (!await isAdmin(callbackTelegramId)) {
              await answerCallbackQuery(env, callback.id, 'Нет доступа', true);
              break;
            }
            
            if (data.startsWith('grant_')) {
              // grant_123456789_7
              const parts = data.split('_');
              const target = parts[1];
              const days = parseInt(parts[2]);
              const isTelegramId = /^\d+$/.test(target);
              
              let currentLicense = null;
              if (isTelegramId) {
                const existing = await env.USERS.get(`license:tg:${target}`);
                if (existing) currentLicense = JSON.parse(existing);
              }
              
              let baseTime = Date.now();
              if (currentLicense && currentLicense.expiresAt > Date.now()) {
                baseTime = currentLicense.expiresAt;
              }
              
              const expiresAt = baseTime + days * 24 * 60 * 60 * 1000;
              
              const licenseData = {
                type: 'subscription',
                expiresAt,
                createdAt: currentLicense?.createdAt || Date.now(),
                updatedAt: Date.now(),
                telegramId: isTelegramId ? target : null,
                grantedBy: username,
                grantedDays: days
              };
              
              if (isTelegramId) {
                await env.USERS.put(`license:tg:${target}`, JSON.stringify(licenseData));
              }
              
              const daysLeft = Math.ceil((expiresAt - Date.now()) / (24 * 60 * 60 * 1000));
              const expiresDate = new Date(expiresAt).toLocaleDateString('ru-RU');
              
              await sendTelegramMessage(env, chatId, 
                `✅ *Выдано*\n\n` +
                `👤 ${target}\n` +
                `📅 +${days} дн. → ${daysLeft} дн.\n` +
                `📆 До: ${expiresDate}`
              );
              
              // Логируем
              await logToChat(env,
                `💎 *Выдано*`,
                'license',
                {
                  telegramId: isTelegramId ? target : 'нет',
                  username: isTelegramId ? 'нет' : target,
                  action: `+${days} дн.`,
                  details: `@${username}`
                }
              );
              
              // Уведомляем пользователя
              if (isTelegramId) {
                try {
                  await sendTelegramMessage(env, target, 
                    `🎉 *Подписка +${days} дн.*\n` +
                    `📅 До: ${expiresDate}`
                  );
                } catch (e) {}
              }
            } else if (data.startsWith('revoke_')) {
              const target = data.replace('revoke_', '');
              const isTelegramId = /^\d+$/.test(target);
              
              if (isTelegramId) {
                await env.USERS.delete(`license:tg:${target}`);
              } else {
                await env.USERS.delete(`license:${target}`);
              }
              
              await sendTelegramMessage(env, chatId, 
                `✅ *Отозвано*\n\n👤 ${isTelegramId ? target : '@' + target}`
              );
              
              // Логируем
              await logToChat(env,
                `❌ *Отозвано*`,
                'admin',
                {
                  telegramId: isTelegramId ? target : 'нет',
                  username: isTelegramId ? 'нет' : target,
                  action: 'Отзыв',
                  details: `@${username}`
                }
              );
            }
          } else {
            await answerCallbackQuery(env, callback.id, 'Неизвестная команда', true);
          }
        }
      }
      
      return jsonResponse({ ok: true });
    }
    
    // === ОБРАБОТКА ТЕКСТОВЫХ КОМАНД ===
    // isAdmin уже определён выше (проверяет по telegramId)
    
    // Rate limiting для обычных сообщений
    if (update.message) {
      const msgTelegramId = String(update.message.from.id);
      
      // Админы не ограничены
      if (!await isAdmin(msgTelegramId)) {
        const rateCheck = checkBotRateLimit(msgTelegramId, 'messages');
        if (!rateCheck.allowed) {
          // Не отвечаем на спам, просто игнорируем
          return jsonResponse({ ok: true });
        }
      }
    }
    
    // ========== СЕКРЕТНАЯ АДМИН-ПАНЕЛЬ ==========
    // Вход по секретной фразе (скрытая от обычных пользователей)
    // ОБЯЗАТЕЛЬНО настройте через: wrangler secret put ADMIN_SECRET_PHRASE
    const adminSecretPhrase = env.ADMIN_SECRET_PHRASE;
    
    // Секретная фраза ДОЛЖНА быть настроена, иначе админка отключена
    if (adminSecretPhrase && update.message?.text === adminSecretPhrase) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      // Проверяем по Telegram ID (более безопасно чем по username)
      const isAdminUser = await isAdminById(env, telegramId);
      
      if (!isAdminUser) {
        // Не даём знать что команда существует - просто игнорируем
        // Как будто это обычный текст
        await sendTelegramMessage(env, chatId, 
          `🤔 Не понимаю.\n\nВыберите из меню:`,
          { reply_markup: getMainMenuKeyboard() }
        );
        return jsonResponse({ ok: true });
      }
      
      // Создаём админ-сессию
      await createAdminSession(env, telegramId);
      
      // Удаляем сообщение с секретной фразой (для безопасности)
      try {
        await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, message_id: update.message.message_id })
        });
      } catch (e) {}
      
      // Получаем статистику и pending группы
      const stats = await getAdminStats(env);
      const pendingGroups = await getAllPendingGroups(env);
      
      const adminMsg = `🔐 *CONTROL PANEL v${BOT_VERSION}*\n\n` +
        `📊 *Активные пользователи:* ${stats.activeUsers}\n` +
        `├ 🎁 Trial: ${stats.trialUsers}\n` +
        `├ ⭐ Level 1: ${stats.level1Users}\n` +
        `├ ⭐⭐ Level 2: ${stats.level2Users}\n` +
        `└ ⭐⭐⭐ Level 3: ${stats.level3Users}\n\n` +
        `💬 Групп: ${stats.approvedGroups} одобр. / ${stats.pendingGroups} ожид.\n\n` +
        `_Сессия активна 1 час_`;
      
      const keyboard = {
        inline_keyboard: [
          [
            { text: '⏳ Ожидающие группы (' + pendingGroups.length + ')', callback_data: 'cp_pending_groups' }
          ],
          [
            { text: '👥 Пользователи', callback_data: 'cp_users' },
            { text: '📊 Статистика', callback_data: 'cp_stats' }
          ],
          [
            { text: '🎁 Выдать подписку', callback_data: 'cp_grant' },
            { text: '🔍 Проверить', callback_data: 'cp_check' }
          ],
          [
            { text: '📢 Рассылка', callback_data: 'cp_broadcast' },
            { text: '🔧 Система', callback_data: 'cp_system' }
          ],
          [
            { text: '🚪 Выход', callback_data: 'cp_logout' }
          ]
        ]
      };
      
      await sendTelegramMessage(env, chatId, adminMsg, { reply_markup: keyboard });
      return jsonResponse({ ok: true });
    }
    
    // Старая команда /admin для обратной совместимости
    if (update.message?.text === '/admin') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        // Не показываем что команда существует
        await sendTelegramMessage(env, chatId, 
          `🤔 Не понимаю.\n\nВыберите из меню:`,
          { reply_markup: getMainMenuKeyboard() }
        );
        return jsonResponse({ ok: true });
      }
      
      // Показываем подсказку о новой админке
      await sendTelegramMessage(env, chatId, 
        `ℹ️ *Админ-панель обновлена*\n\n` +
        `Для доступа используйте секретную фразу.\n` +
        `_Старые команды /grant, /check, /revoke всё ещё работают._`
      );
      return jsonResponse({ ok: true });
    }
    
    // Команда /grant - выдать подписку
    if (update.message?.text?.startsWith('/grant')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 3) {
        await sendTelegramMessage(env, chatId, 
          `❌ *Формат:* \`/grant @user дней [уровень]\`\n\n` +
          `*Уровни:* trial, level1, level2, level3\n` +
          `По умолчанию: level1\n\n` +
          `*Примеры:*\n` +
          `\`/grant @username 30\` — level1\n` +
          `\`/grant @username 30 level2\`\n` +
          `\`/grant 123456789 7 level3\``
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      const days = parseInt(parts[2]);
      const licenseType = parts[3] || 'level1';
      
      if (isNaN(days) || days <= 0) {
        await sendTelegramMessage(env, chatId, '❌ Укажите дни (число > 0)');
        return jsonResponse({ ok: true });
      }
      
      // Проверяем валидность типа лицензии
      const validTypes = ['trial', 'level1', 'level2', 'level3'];
      if (!validTypes.includes(licenseType)) {
        await sendTelegramMessage(env, chatId, 
          `❌ Неверный тип: \`${licenseType}\`\n\n` +
          `Допустимые: ${validTypes.join(', ')}`
        );
        return jsonResponse({ ok: true });
      }
      
      // Ищем пользователя (по ID или username)
      const userInfo = await getUserInfo(env, target);
      
      // Определяем telegramId для сохранения
      let targetTelegramId = null;
      let targetUsername = target;
      const isTelegramId = /^\d+$/.test(target);
      
      if (isTelegramId) {
        targetTelegramId = target;
        targetUsername = userInfo.found ? userInfo.username : null;
      } else {
        if (userInfo.found) {
          targetTelegramId = userInfo.telegramId;
          targetUsername = userInfo.username || target;
        } else {
          // Пользователь не найден по username - выдаём только по username ключу
          // (будет применено когда пользователь зарегистрируется)
          await sendTelegramMessage(env, chatId, 
            `⚠️ *Пользователь @${target} не найден*\n\n` +
            `Лицензия будет сохранена по username.\n` +
            `Когда пользователь подключится — она активируется.`
          );
        }
      }
      
      // Получаем текущую лицензию
      let currentLicense = null;
      if (targetTelegramId) {
        const existing = await env.USERS.get(`license:tg:${targetTelegramId}`);
        if (existing) currentLicense = JSON.parse(existing);
      }
      if (!currentLicense && targetUsername) {
        const existing = await env.USERS.get(`license:${targetUsername.toLowerCase()}`);
        if (existing) currentLicense = JSON.parse(existing);
      }
      
      // Вычисляем новую дату
      let baseTime = Date.now();
      if (currentLicense && currentLicense.expiresAt > Date.now()) {
        baseTime = currentLicense.expiresAt;
      }
      
      const expiresAt = baseTime + days * 24 * 60 * 60 * 1000;
      
      const licenseData = {
        type: licenseType,
        expiresAt: expiresAt,
        createdAt: currentLicense?.createdAt || Date.now(),
        updatedAt: Date.now(),
        telegramId: targetTelegramId,
        username: targetUsername,
        grantedBy: username,
        grantedDays: days
      };
      
      // Сохраняем лицензию
      if (targetTelegramId) {
        await env.USERS.put(`license:tg:${targetTelegramId}`, JSON.stringify(licenseData));
        
        // Сохраняем маппинг username → telegramId
        if (targetUsername) {
          await saveUsernameMapping(env, targetUsername, targetTelegramId);
        }
        
        // Проверяем реферальный бонус - если это первая платная подписка
        // и пользователь пришёл по реферальной ссылке
        if (licenseType !== 'trial' && (!currentLicense || currentLicense.type === 'trial')) {
          const bonusResult = await grantReferralBonusToReferrer(env, targetTelegramId);
          if (bonusResult.success) {
            // Уведомляем реферера о полученном бонусе
            try {
              await sendTelegramMessage(env, bonusResult.referrerTelegramId, 
                `🎉 *Бонус получен!*\n\n` +
                `Ваш реферал оформил подписку!\n` +
                `🎁 Вам начислен +1 день к подписке.`
              );
            } catch (e) {}
            
            // Логируем
            await logToChat(env,
              `🎟️ *Реф. бонус*\n\n` +
              `Реферер: ${bonusResult.referrerTelegramId}\n` +
              `Реферал: ${targetTelegramId}`,
              'success',
              { action: 'Бонус за реферала' }
            );
          }
        }
        
        // ВАЖНО: Увеличиваем версию лицензии для мгновенной синхронизации с расширением
        await incrementLicenseVersion(env, targetTelegramId);
        
      } else if (targetUsername) {
        // Сохраняем по username для последующей активации
        await env.USERS.put(`license:${targetUsername.toLowerCase()}`, JSON.stringify(licenseData));
      }
      
      const daysLeft = Math.ceil((expiresAt - Date.now()) / (24 * 60 * 60 * 1000));
      const expiresDate = new Date(expiresAt).toLocaleDateString('ru-RU');
      const typeName = LICENSE_TYPES[licenseType]?.name || licenseType;
      
      const displayUser = targetUsername ? `@${targetUsername}` : targetTelegramId;
      
      await sendTelegramMessage(env, chatId, 
        `✅ *Выдано*\n\n` +
        `👤 ${displayUser}\n` +
        `🆔 ID: ${targetTelegramId || 'определится позже'}\n` +
        `📦 ${typeName}\n` +
        `📅 +${days} дн. → ${daysLeft} дн.\n` +
        `📆 До: ${expiresDate}`
      );
      
      // Логируем
      await logToChat(env,
        `💎 *Выдано*`,
        'license',
        {
          telegramId: targetTelegramId || 'нет',
          username: targetUsername || 'нет',
          action: `+${days} дн. (${typeName})`,
          details: `@${username}`
        }
      );
      
      // Уведомляем пользователя
      if (targetTelegramId) {
        try {
          await sendTelegramMessage(env, targetTelegramId, 
            `🎉 *Подписка +${days} дн.*\n` +
            `📅 До: ${expiresDate}`
          );
        } catch (e) {}
      }
      
      return jsonResponse({ ok: true });
    }
    
    // Команда /revoke - отозвать подписку
    if (update.message?.text?.startsWith('/revoke')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          `❌ Формат: \`/revoke @user\` или \`/revoke ID\``
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      
      // Ищем пользователя (по ID или username)
      const userInfo = await getUserInfo(env, target);
      const isTelegramId = /^\d+$/.test(target);
      
      // Удаляем лицензию
      let deleted = false;
      let revokedTelegramId = null;
      
      if (userInfo.found && userInfo.telegramId) {
        await env.USERS.delete(`license:tg:${userInfo.telegramId}`);
        revokedTelegramId = userInfo.telegramId;
        deleted = true;
      } else if (isTelegramId) {
        await env.USERS.delete(`license:tg:${target}`);
        revokedTelegramId = target;
        deleted = true;
      } else {
        await env.USERS.delete(`license:${target.toLowerCase()}`);
        deleted = true;
      }
      
      // ВАЖНО: Увеличиваем версию лицензии для мгновенной синхронизации с расширением
      if (revokedTelegramId) {
        await incrementLicenseVersion(env, revokedTelegramId);
      }
      
      const displayUser = userInfo.found ? 
        `@${userInfo.username || target} (ID: ${userInfo.telegramId})` : 
        (isTelegramId ? target : `@${target}`);
      
      await sendTelegramMessage(env, chatId, 
        `✅ *Отозвано*\n\n👤 ${displayUser}`
      );
      
      // Логируем
      await logToChat(env,
        `❌ *Отозвано*`,
        'admin',
        {
          telegramId: userInfo.telegramId || (isTelegramId ? target : 'нет'),
          username: userInfo.username || (!isTelegramId ? target : 'нет'),
          action: 'Отзыв',
          details: `ID: ${telegramId}`
        }
      );
      
      return jsonResponse({ ok: true });
    }
    
    // Команда /check - проверить пользователя
    if (update.message?.text?.startsWith('/check')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          `❌ Формат: \`/check @user\` или \`/check ID\``
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      
      // Ищем пользователя (по ID или username)
      const userInfo = await getUserInfo(env, target);
      const isTelegramId = /^\d+$/.test(target);
      
      let licenseData = null;
      if (userInfo.found && userInfo.telegramId) {
        const data = await env.USERS.get(`license:tg:${userInfo.telegramId}`);
        if (data) licenseData = JSON.parse(data);
      } else if (isTelegramId) {
        const data = await env.USERS.get(`license:tg:${target}`);
        if (data) licenseData = JSON.parse(data);
      } else {
        const data = await env.USERS.get(`license:${target.toLowerCase()}`);
        if (data) licenseData = JSON.parse(data);
      }
      
      const displayUser = userInfo.found ? 
        `@${userInfo.username || target}` : 
        (isTelegramId ? target : `@${target}`);
      
      if (!licenseData) {
        await sendTelegramMessage(env, chatId, 
          `❌ *Лицензия не найдена*\n\n` +
          `👤 ${displayUser}\n` +
          `🆔 ID: ${userInfo.telegramId || (isTelegramId ? target : 'неизвестен')}\n\n` +
          (userInfo.found ? `ℹ️ Пользователь зарегистрирован, но подписки нет` : `ℹ️ Пользователь не найден в базе`),
          { reply_markup: {
            inline_keyboard: [
              [{ text: '← Админ', callback_data: 'admin_back' }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      const now = Date.now();
      const isValid = licenseData.expiresAt > now;
      const timeLeft = licenseData.expiresAt - now;
      const daysLeft = Math.max(0, Math.ceil(timeLeft / (24 * 60 * 60 * 1000)));
      const expiresDate = new Date(licenseData.expiresAt).toLocaleDateString('ru-RU');
      
      // Прогресс-бар
      const maxTime = 30 * 24 * 60 * 60 * 1000;
      const percent = isValid ? Math.min(100, Math.round((timeLeft / maxTime) * 100)) : 0;
      const bar = getProgressBar(percent);
      
      const emoji = isValid ? getStatusEmoji(daysLeft, licenseData.type) : '❌';
      const typeText = LICENSE_TYPES[licenseData.type]?.name || 'Премиум';
      
      // Получаем информацию об устройствах
      const targetTgId = userInfo.telegramId || (isTelegramId ? target : null);
      let devicesInfo = '';
      if (targetTgId) {
        const devicesData = await env.USERS.get(`devices:${targetTgId}`);
        if (devicesData) {
          const devices = JSON.parse(devicesData);
          devicesInfo = `\n📱 Устройств: ${devices.length}`;
        }
      }
      
      const checkMsg = `🔍 *Пользователь*\n\n` +
        `👤 ${displayUser}\n` +
        `🆔 ID: ${targetTgId || 'неизвестен'}\n\n` +
        `${emoji} ${typeText} • ${isValid ? 'Активна' : 'Истекла'}\n` +
        `${bar} ${percent}%\n\n` +
        `⏰ ${daysLeft} дн. • До: ${expiresDate}` +
        devicesInfo +
        (licenseData.grantedBy ? `\n👑 Выдал: @${licenseData.grantedBy}` : '');
      
      const checkTargetId = targetTgId || target;
      
      await sendTelegramMessage(env, chatId, checkMsg, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: '🎁 +7', callback_data: `grant_${checkTargetId}_7` },
              { text: '🎁 +30', callback_data: `grant_${checkTargetId}_30` },
              { text: '🎁 +90', callback_data: `grant_${checkTargetId}_90` }
            ],
            [{ text: '❌ Отозвать', callback_data: `revoke_${checkTargetId}` }],
            [{ text: '← Админ', callback_data: 'admin_back' }]
          ]
        }
      });
      
      return jsonResponse({ ok: true });
    }
    
    // Команда /users
    if (update.message?.text === '/users') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      await sendTelegramMessage(env, chatId, 
        `📊 *Пользователи*\n\n` +
        `\`/check @user\` — проверить по username\n` +
        `\`/check ID\` — проверить по ID`
      );
      
      return jsonResponse({ ok: true });
    }
    
    // Команда /stats
    if (update.message?.text === '/stats') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const now = new Date().toLocaleString('ru-RU', { 
        timeZone: 'Europe/Moscow',
        day: '2-digit',
        month: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
      });
      
      const botUsername = env.BOT_USERNAME || 'clickuved_bot';
      
      await sendTelegramMessage(env, chatId, 
        `📈 *Статистика бота*\n\n` +
        `🤖 @${botUsername.replace(/_/g, '\\_')}\n` +
        `📦 Версия: ${BOT_VERSION}\n` +
        `🕐 ${now} MSK\n` +
        `✅ KV: Работает`
      );
      
      return jsonResponse({ ok: true });
    }
    
    // Команда /broadcast - рассылка всем активным пользователям (только админ)
    if (update.message?.text?.startsWith('/broadcast')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      // Проверяем админа по ID
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const text = update.message.text.replace('/broadcast', '').trim();
      
      if (!text) {
        await sendTelegramMessage(env, chatId, 
          `📢 *Рассылка*\n\n` +
          `Формат: \`/broadcast Текст сообщения\`\n\n` +
          `⚠️ _Сообщение будет отправлено всем активным пользователям_`
        );
        return jsonResponse({ ok: true });
      }
      
      // Получаем всех пользователей с активными лицензиями
      const users = await getAllLicensedUsers(env, 1000);
      const activeUsers = users.filter(u => u.isActive);
      
      let sent = 0;
      let failed = 0;
      
      // Уведомляем о начале рассылки
      await sendTelegramMessage(env, chatId, 
        `📢 *Начинаю рассылку...*\n\n` +
        `👥 Получателей: ${activeUsers.length}`
      );
      
      // Отправляем с задержкой чтобы не превысить лимиты Telegram
      for (const user of activeUsers) {
        try {
          await sendTelegramMessage(env, user.telegramId, 
            `📢 *Объявление*\n\n${text}`,
            { reply_markup: getMainMenuKeyboard() }
          );
          sent++;
          // Задержка 50мс между сообщениями
          await new Promise(r => setTimeout(r, 50));
        } catch (e) {
          failed++;
        }
      }
      
      await sendTelegramMessage(env, chatId, 
        `✅ *Рассылка завершена*\n\n` +
        `📤 Отправлено: ${sent}\n` +
        `❌ Ошибок: ${failed}`
      );
      
      // Логируем
      await logToChat(env,
        `📢 *Рассылка*\n\n` +
        `👤 От: @${username}\n` +
        `📤 Отправлено: ${sent}\n` +
        `❌ Ошибок: ${failed}`,
        'admin'
      );
      
      return jsonResponse({ ok: true });
    }
    
    // ========== КОМАНДЫ СТАТИСТИКИ И РЕФЕРАЛОВ ==========
    
    // Команда /mystats - статистика пользователя
    if (update.message?.text === '/mystats') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      const stats = await getUserStats(env, telegramId);
      const oderId = await env.USERS.get(`tg:${telegramId}`);
      
      let licenseInfo = '❌ Не подключён';
      if (oderId) {
        const result = await getLicenseData(env, oderId);
        if (result.license) {
          const daysLeft = Math.max(0, Math.ceil((result.license.expiresAt - Date.now()) / MS_PER_DAY));
          const isActive = result.license.expiresAt > Date.now();
          const typeText = LICENSE_TYPES[result.license.type]?.name || result.license.type;
          licenseInfo = isActive 
            ? `✅ ${typeText} (${daysLeft} ${getDaysWord(daysLeft)})`
            : `❌ ${typeText} (истекла)`;
        }
      }
      
      const referralCode = await getUserReferralCode(env, telegramId);
      const paidBonuses = stats.referralPaidBonus || 0;
      const pendingBonuses = stats.referralPendingBonus || 0;
      
      const message = `📊 *Ваша статистика*\n\n` +
        `👆 *Кликов сегодня:* ${stats.todayClicks || 0}\n` +
        `📈 *Всего кликов:* ${stats.totalClicks || 0}\n\n` +
        `💎 *Подписка:* ${licenseInfo}\n\n` +
        `👥 *Рефералов:* ${stats.referralCount || 0}\n` +
        `🎁 *Бонусов получено:* ${paidBonuses} дн.\n` +
        (pendingBonuses > 0 ? `⏳ *Ожидают оплаты:* ${pendingBonuses}\n` : '') +
        `🎟️ *Ваш код:* \`${referralCode}\`\n\n` +
        `_Бонус +1 день когда реферал оформит подписку!_`;
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: {
          inline_keyboard: [
            [{ text: '📤 Поделиться кодом', callback_data: 'share_ref' }],
            [
              { text: '🔄 Обновить', callback_data: 'mystats' },
              { text: '← Меню', callback_data: 'menu' }
            ]
          ]
        }
      });
      return jsonResponse({ ok: true });
    }
    
    // Команда /ref - ввод реферального кода
    if (update.message?.text?.startsWith('/ref')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const parts = update.message.text.split(' ');
      
      if (parts.length < 2) {
        const myCode = await getUserReferralCode(env, telegramId);
        await sendTelegramMessage(env, chatId, 
          `🎟️ *Реферальная система*\n\n` +
          `*Ваш код:* \`${myCode}\`\n\n` +
          `Чтобы ввести чужой код:\n` +
          `\`/ref КОД\`\n\n` +
          `_Друг получит 1 день пробника._\n` +
          `_Вы получите +1 день, когда друг оформит подписку!_`
        );
        return jsonResponse({ ok: true });
      }
      
      const refCode = parts[1].toUpperCase();
      
      // Проверяем формат кода
      if (!refCode.startsWith(REFERRAL_CODE_PREFIX)) {
        await sendTelegramMessage(env, chatId, 
          `❌ *Неверный формат кода*\n\n` +
          `Код должен начинаться с \`${REFERRAL_CODE_PREFIX}\``
        );
        return jsonResponse({ ok: true });
      }
      
      const result = await applyReferralCode(env, telegramId, refCode);
      
      if (result.success) {
        // Сообщение новому пользователю
        if (result.gaveTrialToNewUser) {
          await sendTelegramMessage(env, chatId, 
            `✅ *Реферальный код применён!*\n\n` +
            `Спасибо за регистрацию по приглашению!\n` +
            `🎁 Вам начислен 1 день пробного периода!`
          );
        } else {
          await sendTelegramMessage(env, chatId, 
            `✅ *Реферальный код применён!*\n\n` +
            `Спасибо за регистрацию по приглашению!\n` +
            `_Пробник не начислен, т.к. у вас уже есть активная подписка._`
          );
        }
        
        // Уведомляем реферера - НОВОЕ СООБЩЕНИЕ
        try {
          await sendTelegramMessage(env, result.referrerTelegramId, 
            `👤 *Новый реферал!*\n\n` +
            `По вашей ссылке перешёл новый пользователь!\n\n` +
            `⏳ *Бонус +1 день* будет начислен,\n` +
            `когда реферал оформит любую подписку.`
          );
        } catch (e) {}
        
        // Логируем
        await logToChat(env,
          `🎟️ *Реферал*\n\n` +
          `Новый: ${telegramId}\n` +
          `Реферер: ${result.referrerTelegramId}`,
          'success',
          { action: 'Реферальный код', details: refCode }
        );
      } else {
        const errorMessages = {
          'INVALID_CODE': 'Код не найден',
          'SELF_REFERRAL': 'Нельзя использовать свой код',
          'ALREADY_REFERRED': 'Вы уже использовали реферальный код'
        };
        await sendTelegramMessage(env, chatId, 
          `❌ *Ошибка*\n\n${errorMessages[result.error] || result.error}`
        );
      }
      
      return jsonResponse({ ok: true });
    }
    
    // ========== КОМАНДЫ УПРАВЛЕНИЯ БОТОМ (АДМИН) ==========
    
    // Команда /bot_off - выключить бота
    if (update.message?.text === '/bot_off' || update.message?.text === '/stop_bot') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      await setBotEnabled(env, false);
      await sendTelegramMessage(env, chatId, 
        `🔴 *Бот выключен*\n\n` +
        `Бот перестанет отвечать на сообщения пользователей.\n` +
        `Админские команды продолжат работать.\n\n` +
        `Для включения: /bot_on`
      );
      return jsonResponse({ ok: true });
    }
    
    // Команда /bot_on - включить бота
    if (update.message?.text === '/bot_on' || update.message?.text === '/start_bot') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      await setBotEnabled(env, true);
      await sendTelegramMessage(env, chatId, 
        `🟢 *Бот включён*\n\n` +
        `Бот снова отвечает на сообщения пользователей.`
      );
      return jsonResponse({ ok: true });
    }
    
    // Команда /bot_status - статус бота
    if (update.message?.text === '/bot_status') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const enabled = await isBotEnabled(env);
      await sendTelegramMessage(env, chatId, 
        `📊 *Статус бота*\n\n` +
        `Состояние: ${enabled ? '🟢 Включён' : '🔴 Выключен'}\n` +
        `Версия: ${BOT_VERSION}\n` +
        `Uptime: ${Math.floor((Date.now() - metrics.startTime) / 1000 / 60)} мин`
      );
      return jsonResponse({ ok: true });
    }
    
    // Команда /whitelist_add - добавить чат в белый список
    if (update.message?.text?.startsWith('/whitelist_add') || update.message?.text?.startsWith('/wl_add')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 3) {
        await sendTelegramMessage(env, chatId, 
          `❌ Формат: \`/whitelist_add chatId ownerTelegramId [заметка]\`\n\n` +
          `Пример: \`/whitelist_add -1001234567890 123456789 Чат оператора\``
        );
        return jsonResponse({ ok: true });
      }
      
      const targetChatId = parts[1];
      const ownerTgId = parts[2];
      const note = parts.slice(3).join(' ') || '';
      
      const data = await addChatToWhitelist(env, targetChatId, ownerTgId, note);
      await saveWhitelistIndex(env);
      
      await sendTelegramMessage(env, chatId, 
        `✅ *Чат добавлен в белый список*\n\n` +
        `🆔 Chat ID: \`${targetChatId}\`\n` +
        `👤 Владелец: \`${ownerTgId}\`\n` +
        `📝 Заметка: ${note || '—'}\n\n` +
        `Теперь этот чат будет одобрен автоматически.`
      );
      return jsonResponse({ ok: true });
    }
    
    // Команда /whitelist_remove - удалить чат из белого списка
    if (update.message?.text?.startsWith('/whitelist_remove') || update.message?.text?.startsWith('/wl_remove')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          `❌ Формат: \`/whitelist_remove chatId\``
        );
        return jsonResponse({ ok: true });
      }
      
      const targetChatId = parts[1];
      await removeChatFromWhitelist(env, targetChatId);
      await saveWhitelistIndex(env);
      
      await sendTelegramMessage(env, chatId, 
        `✅ Чат \`${targetChatId}\` удалён из белого списка.`
      );
      return jsonResponse({ ok: true });
    }
    
    // Команда /whitelist - показать белый список
    if (update.message?.text === '/whitelist' || update.message?.text === '/wl') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      // Загружаем кэш если пуст
      if (WHITELISTED_CHATS.size === 0) {
        await loadWhitelistCache(env);
      }
      
      const chats = Array.from(WHITELISTED_CHATS.values());
      
      if (chats.length === 0) {
        await sendTelegramMessage(env, chatId, 
          `📋 *Белый список чатов*\n\n_Пусто_\n\n` +
          `Добавить: \`/whitelist_add chatId ownerTgId [заметка]\``
        );
        return jsonResponse({ ok: true });
      }
      
      let message = `📋 *Белый список чатов* (${chats.length})\n\n`;
      chats.forEach((c, i) => {
        const date = new Date(c.addedAt).toLocaleDateString('ru-RU');
        message += `${i+1}. \`${c.chatId}\`\n`;
        message += `   👤 Владелец: \`${c.owner}\`\n`;
        message += `   📅 Добавлен: ${date}\n`;
        if (c.note) message += `   📝 ${c.note}\n`;
        message += `\n`;
      });
      
      await sendTelegramMessage(env, chatId, message);
      return jsonResponse({ ok: true });
    }
    
    // ========== КОМАНДЫ ГРУППОВЫХ ЧАТОВ ==========
    
    // Команда /approve_group - одобрить групповой чат (админ)
    if (update.message?.text?.startsWith('/approve_group')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          `❌ Формат: \`/approve_group chatId\``
        );
        return jsonResponse({ ok: true });
      }
      
      const targetChatId = parts[1];
      const result = await approveGroupChat(env, targetChatId, telegramId);
      
      if (result.success) {
        await sendTelegramMessage(env, chatId, 
          `✅ *Групповой чат одобрен!*\n\n` +
          `🆔 Chat ID: \`${targetChatId}\`\n` +
          `👤 Владелец: ${result.telegramId}`
        );
        
        // Уведомляем пользователя
        try {
          await sendTelegramMessage(env, result.telegramId, 
            `✅ *Ваш групповой чат одобрен!*\n\n` +
            `Теперь уведомления будут приходить и в группу.`
          );
        } catch (e) {}
        
        // Отправляем приветствие в группу
        try {
          await sendTelegramMessage(env, targetChatId, 
            `🤖 *Exotic Assistant подключён!*\n\n` +
            `Теперь уведомления о заявках будут приходить в этот чат.`
          );
        } catch (e) {}
      } else {
        await sendTelegramMessage(env, chatId, 
          `❌ Ошибка: ${result.error}`
        );
      }
      return jsonResponse({ ok: true });
    }
    
    // Команда /devices - управление устройствами (для пользователя)
    if (update.message?.text === '/devices') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      // Проверяем подключение
      const existingOderId = await env.USERS.get(`tg:${telegramId}`);
      if (!existingOderId) {
        await sendTelegramMessage(env, chatId, 
          `❌ *Расширение не подключено*\n\n` +
          `Сначала подключите расширение через код.`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '🔗 Как подключить', callback_data: 'how_to_connect' }],
              [{ text: '← Меню', callback_data: 'menu' }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      // Получаем информацию о лицензии и устройствах
      const licenseData = await env.USERS.get(`license:tg:${telegramId}`);
      const license = licenseData ? JSON.parse(licenseData) : null;
      const licenseType = LICENSE_TYPES[license?.type] || LICENSE_TYPES.trial;
      const maxDevices = licenseType.maxDevices || 1;
      
      const devicesData = await getUserDevices(env, telegramId);
      const devices = devicesData.devices || [];
      
      let message = `📱 *Ваши устройства*\n\n`;
      message += `📦 Подписка: *${licenseType.name}*\n`;
      message += `🔢 Использовано: ${devices.length}/${maxDevices}\n\n`;
      
      if (devices.length === 0) {
        message += `_Нет подключённых устройств_\n\n`;
        message += `Устройство добавится автоматически\nпри первой проверке лицензии.`;
      } else {
        message += `*Список устройств:*\n`;
        devices.forEach((d, i) => {
          const addedDate = new Date(d.addedAt).toLocaleDateString('ru-RU');
          const lastSeenDate = new Date(d.lastSeen).toLocaleDateString('ru-RU');
          const deviceShort = d.deviceId.substring(0, 12) + '...';
          message += `\n${i + 1}. \`${deviceShort}\`\n`;
          message += `   📅 Добавлен: ${addedDate}\n`;
          message += `   👁 Активен: ${lastSeenDate}`;
        });
      }
      
      // Кнопки действий
      const buttons = [];
      if (devices.length > 0) {
        buttons.push([{ text: '🗑 Отвязать все устройства', callback_data: 'clear_devices' }]);
      }
      if (maxDevices < 2) {
        buttons.push([{ text: '⬆️ Больше устройств', callback_data: 'buy' }]);
      }
      buttons.push([{ text: '← Меню', callback_data: 'menu' }]);
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: { inline_keyboard: buttons }
      });
      
      return jsonResponse({ ok: true });
    }
    
    // Команда /groups - управление группами (для пользователя)
    if (update.message?.text === '/groups') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      const canUse = await canUseGroupChats(env, telegramId);
      
      if (!canUse.allowed) {
        let errorMsg = '';
        switch (canUse.reason) {
          case 'NOT_CONNECTED':
            errorMsg = '❌ Сначала привяжите расширение';
            break;
          case 'NO_LICENSE':
            errorMsg = '❌ Нет активной подписки';
            break;
          case 'UPGRADE_REQUIRED':
            errorMsg = `❌ Групповые чаты доступны только для *Premium* и *Максимум*\n\n` +
                       `Ваша подписка: ${canUse.currentType || 'Базовая'}`;
            break;
          default:
            errorMsg = '❌ Нет доступа к групповым чатам';
        }
        
        await sendTelegramMessage(env, chatId, errorMsg, {
          reply_markup: {
            inline_keyboard: [
              [{ text: '💳 Улучшить подписку', callback_data: 'buy' }],
              [{ text: '← Меню', callback_data: 'menu' }]
            ]
          }
        });
        return jsonResponse({ ok: true });
      }
      
      const groupData = await getUserGroupChats(env, telegramId);
      
      let message = `💬 *Групповые чаты*\n\n`;
      
      if (groupData.chats.length === 0) {
        message += `У вас пока нет подключённых групп.\n\n`;
        message += `*Как подключить:*\n`;
        message += `1️⃣ Добавьте бота @clickuved\\_bot в группу\n`;
        message += `2️⃣ Бот запросит одобрение администратора\n`;
        message += `3️⃣ После одобрения уведомления будут приходить в группу`;
      } else {
        message += `*Подключённые группы (${groupData.chats.length}):*\n\n`;
        groupData.chats.forEach((chat, i) => {
          message += `${i + 1}. ${chat.chatTitle}\n`;
        });
        message += `\n_Лимит: ${canUse.maxChats} группы_`;
      }
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: {
          inline_keyboard: [
            [{ text: '📖 Инструкция', callback_data: 'group_help' }],
            [{ text: '← Меню', callback_data: 'menu' }]
          ]
        }
      });
      
      return jsonResponse({ ok: true });
    }
    
    // Команда /grant_premium - выдать Premium подписку (для групп)
    if (update.message?.text?.startsWith('/grant_premium')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '❌ Нет доступа');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 3) {
        await sendTelegramMessage(env, chatId, 
          `❌ Формат: \`/grant_premium ID дней\`\n\n` +
          `Типы: level2, level3`
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      const days = parseInt(parts[2]);
      const type = parts[3] || 'level2'; // level2 или level3
      
      if (isNaN(days) || days <= 0) {
        await sendTelegramMessage(env, chatId, '❌ Укажите дни (число > 0)');
        return jsonResponse({ ok: true });
      }
      
      const isTelegramId = /^\d+$/.test(target);
      
      let currentLicense = null;
      if (isTelegramId) {
        const existing = await env.USERS.get(`license:tg:${target}`);
        if (existing) currentLicense = JSON.parse(existing);
      } else {
        const existing = await env.USERS.get(`license:${target}`);
        if (existing) currentLicense = JSON.parse(existing);
      }
      
      let baseTime = Date.now();
      if (currentLicense && currentLicense.expiresAt > Date.now()) {
        baseTime = currentLicense.expiresAt;
      }
      
      const expiresAt = baseTime + days * 24 * 60 * 60 * 1000;
      
      const licenseData = {
        type: type, // 'premium' или 'max'
        expiresAt: expiresAt,
        createdAt: currentLicense?.createdAt || Date.now(),
        updatedAt: Date.now(),
        telegramId: isTelegramId ? target : null,
        username: isTelegramId ? null : target,
        grantedBy: username,
        grantedDays: days
      };
      
      if (isTelegramId) {
        await env.USERS.put(`license:tg:${target}`, JSON.stringify(licenseData));
      } else {
        await env.USERS.put(`license:${target}`, JSON.stringify(licenseData));
      }
      
      const daysLeft = Math.ceil((expiresAt - Date.now()) / (24 * 60 * 60 * 1000));
      const typeInfo = LICENSE_TYPES[type] || LICENSE_TYPES.premium;
      
      await sendTelegramMessage(env, chatId, 
        `✅ *${typeInfo.name} выдан!*\n\n` +
        `👤 ${isTelegramId ? target : '@' + target}\n` +
        `📅 +${days} дн. → ${daysLeft} дн.\n` +
        `💬 Группы: ${typeInfo.groupChats ? '✅' : '❌'}\n` +
        `📱 Устройств: ${typeInfo.maxDevices}`
      );
      
      return jsonResponse({ ok: true });
    }
    
    // Обработка добавления бота в группу
    if (update.my_chat_member) {
      const member = update.my_chat_member;
      const chat = member.chat;
      const newChatMember = member.new_chat_member;
      const fromUser = member.from;
      
      // Проверяем наличие необходимых данных
      if (!newChatMember || !chat || !fromUser) {
        return jsonResponse({ ok: true });
      }
      
      const newStatus = newChatMember.status;
      
      // Бот добавлен в группу
      if (chat.type === 'group' || chat.type === 'supergroup') {
        if (newStatus === 'member' || newStatus === 'administrator') {
          const telegramId = String(fromUser.id);
          const username = fromUser.username;
          
          // ОПТИМИЗАЦИЯ: Проверяем белый список - если чат там, одобряем сразу
          const whitelistData = await isChatWhitelisted(env, chat.id);
          if (whitelistData) {
            // Чат в белом списке - одобряем автоматически
            const ownerTgId = whitelistData.owner || telegramId;
            
            // Проверяем право на групповые чаты у владельца
            const canUse = await canUseGroupChats(env, ownerTgId);
            if (!canUse.allowed) {
              // Если владелец из белого списка потерял лицензию - используем того кто добавил
              const canUseAdder = await canUseGroupChats(env, telegramId);
              if (!canUseAdder.allowed) {
                await sendTelegramMessage(env, chat.id, 
                  `❌ *Ошибка лицензии*\n\n` +
                  `У владельца из белого списка нет активной подписки.\n` +
                  `Обратитесь к администратору.`
                );
                return jsonResponse({ ok: true });
              }
            }
            
            // Автоматическое одобрение
            const result = await approveGroupChat(env, chat.id, ownerTgId, true);
            
            await sendTelegramMessage(env, chat.id, 
              `🤖 *Exotic Assistant подключён!*\n\n` +
              `✅ Чат одобрен автоматически (белый список)\n` +
              `Теперь уведомления о заявках будут приходить сюда.`
            );
            
            // Логируем
            await logToChat(env,
              `✅ *Автоодобрение из белого списка*\n\n` +
              `💬 Чат: ${escapeMarkdown(chat.title)}\n` +
              `🆔 Chat ID: \`${chat.id}\`\n` +
              `👤 Добавил: @${username || telegramId}`,
              'connect',
              { telegramId, chatId: chat.id, action: 'whitelist_auto_approve' }
            );
            
            return jsonResponse({ ok: true });
          }
          
          // Проверяем право на групповые чаты
          const canUse = await canUseGroupChats(env, telegramId);
          
          if (!canUse.allowed) {
            await sendTelegramMessage(env, chat.id, 
              `❌ *Групповые чаты недоступны*\n\n` +
              `@${username || telegramId}, для использования бота в группах ` +
              `необходима подписка Premium или Максимум.\n\n` +
              `Напишите боту лично для подробностей.`
            );
            return jsonResponse({ ok: true });
          }
          
          // Проверяем лимит групп
          const groupData = await getUserGroupChats(env, telegramId);
          if (groupData.chats.length >= canUse.maxChats) {
            await sendTelegramMessage(env, chat.id, 
              `❌ *Лимит групп достигнут*\n\n` +
              `У вас уже ${groupData.chats.length}/${canUse.maxChats} групп.\n` +
              `Отключите одну из групп через /groups`
            );
            return jsonResponse({ ok: true });
          }
          
          // Запрашиваем одобрение админа
          const result = await requestGroupChatApproval(env, telegramId, chat.id, chat.title, username);
          
          if (result.success) {
            await sendTelegramMessage(env, chat.id, 
              `⏳ *Запрос отправлен*\n\n` +
              `Администратор рассмотрит вашу заявку.\n` +
              `Вы получите уведомление после одобрения.`
            );
          } else if (result.error === 'REQUEST_PENDING') {
            await sendTelegramMessage(env, chat.id, 
              `⏳ *Запрос уже отправлен*\n\n` +
              `Ожидайте одобрения администратора.`
            );
          }
          
          return jsonResponse({ ok: true });
        }
        
        // Бот удалён из группы
        if (newStatus === 'left' || newStatus === 'kicked') {
          // Удаляем группу из списка владельца
          const ownerTgId = await env.USERS.get(`group_owner:${chat.id}`);
          if (ownerTgId) {
            const groupData = await getUserGroupChats(env, ownerTgId);
            groupData.chats = groupData.chats.filter(c => c.chatId !== chat.id);
            await saveUserGroupChats(env, ownerTgId, groupData);
            await env.USERS.delete(`group_owner:${chat.id}`);
          }
          return jsonResponse({ ok: true });
        }
      }
    }
    
    // === ОБРАБОТКА СООБЩЕНИЙ В ГРУППОВЫХ ЧАТАХ ===
    if (update.message && (update.message.chat.type === 'group' || update.message.chat.type === 'supergroup')) {
      const chat = update.message.chat;
      const chatId = chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const text = update.message.text || '';
      const botUsername = env.BOT_USERNAME || 'clickuved_bot';
      
      // Сохраняем маппинг username → telegramId
      if (username) {
        await saveUsernameMapping(env, username, telegramId);
      }
      
      // Проверяем - это команда боту или упоминание бота
      const isDirectCommand = text.startsWith('/') && (
        text.includes(`@${botUsername}`) || 
        !text.includes('@') // команда без @ адресована всем ботам
      );
      const isBotMention = text.toLowerCase().includes(`@${botUsername.toLowerCase()}`);
      const isReplyToBot = update.message.reply_to_message?.from?.username === botUsername;
      
      // Если это не команда боту - игнорируем
      if (!isDirectCommand && !isBotMention && !isReplyToBot) {
        return jsonResponse({ ok: true });
      }
      
      // Убираем @username из команды
      const cleanText = text.replace(new RegExp(`@${botUsername}`, 'gi'), '').trim();
      
      // Проверяем - группа одобрена для этого пользователя
      const groupOwner = await env.USERS.get(`group_owner:${chatId}`);
      
      // Команда /start в группе - показываем приветствие
      if (cleanText.startsWith('/start')) {
        if (!groupOwner) {
          await sendTelegramMessage(env, chatId, 
            `👋 *Exotic Assistant*\n\n` +
            `Для работы бота в этой группе\n` +
            `необходимо добавить его как администратора.\n\n` +
            `Владелец группы должен иметь\n` +
            `подписку *Level 2* или выше.\n\n` +
            `🔗 Напишите боту лично:\n@${botUsername}`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '🤖 Написать боту', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
        } else {
          await sendTelegramMessage(env, chatId, 
            `👋 *Exotic Assistant*\n\n` +
            `Бот активен в этой группе!\n\n` +
            `📊 Используйте \`/status\` для проверки`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '📊 Статус', callback_data: 'group_status' }],
                [{ text: '🤖 Написать боту', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
        }
        return jsonResponse({ ok: true });
      }
      
      // Команда /status в группе
      if (cleanText.startsWith('/status')) {
        // Проверяем подписку пользователя
        const existingOderId = await env.USERS.get(`tg:${telegramId}`);
        
        if (!existingOderId) {
          await sendTelegramMessage(env, chatId, 
            `📊 @${username || telegramId}, ваш статус:\n\n` +
            `❌ Расширение не подключено\n\n` +
            `Напишите боту в ЛС для подключения.`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '🤖 Подключить', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
          return jsonResponse({ ok: true });
        }
        
        const result = await getLicenseData(env, existingOderId);
        
        if (!result.license) {
          await sendTelegramMessage(env, chatId, 
            `📊 @${username || telegramId}, ваш статус:\n\n` +
            `✅ Расширение подключено\n` +
            `❌ Подписка не найдена`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '💳 Купить', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
          return jsonResponse({ ok: true });
        }
        
        const license = result.license;
        const now = Date.now();
        const isValid = license.expiresAt > now;
        const timeLeft = license.expiresAt - now;
        const daysLeft = Math.max(0, Math.ceil(timeLeft / (24 * 60 * 60 * 1000)));
        const hoursLeft = Math.max(0, Math.ceil(timeLeft / (60 * 60 * 1000)));
        
        const emoji = isValid ? getStatusEmoji(daysLeft, license.type) : '❌';
        const typeText = LICENSE_TYPES[license.type]?.name || 'Премиум';
        const timeDisplay = license.type === 'trial' ? `${hoursLeft} ч.` : `${daysLeft} дн.`;
        
        await sendTelegramMessage(env, chatId, 
          `📊 @${username || telegramId}, ваш статус:\n\n` +
          `${emoji} ${typeText} • ${isValid ? 'Активна' : 'Истекла'}\n` +
          `⏰ ${isValid ? `Осталось: ${timeDisplay}` : 'Продлите подписку'}`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '📊 Подробнее', url: `https://t.me/${botUsername}` }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      // Команда /help в группе
      if (cleanText.startsWith('/help')) {
        await sendTelegramMessage(env, chatId, 
          `📖 *Exotic Assistant*\n\n` +
          `Доступные команды в группе:\n\n` +
          `\`/status\` — ваш статус подписки\n` +
          `\`/id\` — узнать ID\n` +
          `\`/help\` — это сообщение\n\n` +
          `Для полного функционала\n` +
          `напишите боту в ЛС.`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '🤖 Написать боту', url: `https://t.me/${botUsername}` }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      // Команда /id в группе
      if (cleanText.startsWith('/id')) {
        let message = `🆔 *Информация об ID*\n\n`;
        message += `👤 *@${username || telegramId}:*\n`;
        message += `└ ID: \`${telegramId}\`\n`;
        message += `\n💬 *Этот чат:*\n`;
        message += `├ ID: \`${chatId}\`\n`;
        message += `└ Название: ${chat.title || 'Без названия'}`;
        
        await sendTelegramMessage(env, chatId, message);
        return jsonResponse({ ok: true });
      }
      
      // Неизвестная команда в группе
      if (cleanText.startsWith('/')) {
        await sendTelegramMessage(env, chatId, 
          `❓ Неизвестная команда.\n\n` +
          `Используйте \`/help\` для списка команд.`
        );
        return jsonResponse({ ok: true });
      }
      
      // Упоминание бота без команды
      if (isBotMention) {
        await sendTelegramMessage(env, chatId, 
          `👋 Используйте \`/help\` для списка команд.`
        );
        return jsonResponse({ ok: true });
      }
      
      return jsonResponse({ ok: true });
    }
    
    // Обработка команды /start
    if (update.message?.text?.startsWith('/start')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const firstName = update.message.from.first_name;
      
      // Сохраняем маппинг username → telegramId для поиска
      if (username) {
        await saveUsernameMapping(env, username, telegramId);
      }
      
      const parts = update.message.text.split(' ');
      const param = parts[1];
      
      // Проверяем реферальную ссылку (ref_REF_XXXX)
      if (param && param.startsWith('ref_')) {
        const refCode = param.replace('ref_', '');
        const result = await applyReferralCode(env, telegramId, refCode);
        
        if (result.success) {
          // Сообщение новому пользователю
          if (result.gaveTrialToNewUser) {
            await sendTelegramMessage(env, chatId, 
              `🎉 *Добро пожаловать!*\n\n` +
              `Вы пришли по приглашению друга.\n` +
              `🎁 Вам начислен 1 день пробного периода!`,
              { reply_markup: getMainMenuKeyboard() }
            );
          } else {
            await sendTelegramMessage(env, chatId, 
              `🎉 *Добро пожаловать!*\n\n` +
              `Вы пришли по приглашению друга.\n` +
              `_Пробник не начислен, т.к. у вас уже есть подписка._`,
              { reply_markup: getMainMenuKeyboard() }
            );
          }
          
          // Уведомляем реферера - НОВЫЙ ТЕКСТ
          try {
            await sendTelegramMessage(env, result.referrerTelegramId, 
              `👤 *Новый реферал!*\n\n` +
              `По вашей ссылке перешёл новый пользователь!\n\n` +
              `⏳ *Бонус +1 день* будет начислен,\n` +
              `когда реферал оформит любую подписку.`
            );
          } catch (e) {}
          
          // Логируем
          await logToChat(env,
            `🎟️ *Реферал*\n\n` +
            `Новый: ${telegramId}\n` +
            `Реферер: ${result.referrerTelegramId}`,
            'success',
            { action: 'Реферальный код', details: refCode }
          );
        } else {
          // Код недействителен, но всё равно приветствуем
          await sendTelegramMessage(env, chatId, 
            `👋 *Добро пожаловать!*\n\n` +
            `Exotic Operator Assistant — ваш\n` +
            `помощник для работы с заявками.\n\n` +
            `🔗 Подключите расширение для начала:`,
            { reply_markup: getMainMenuKeyboard() }
          );
        }
        return jsonResponse({ ok: true });
      }
      
      // Привязка аккаунта по коду
      if (param) {
        const codeData = await env.USERS.get(`code:${param}`);
        
        if (!codeData) {
          await sendTelegramMessage(env, chatId, 
            `❌ *Код недействителен*\n\nПолучите новый в настройках`,
            { reply_markup: getBackButton() }
          );
          return jsonResponse({ ok: true });
        }
        
        const { oderId, deviceId } = JSON.parse(codeData);
        
        // Сохраняем связку
        const userData = {
          oderId,
          deviceId,
          telegramId,
          username: username || null,
          firstName: firstName || null,
          connectedAt: Date.now()
        };
        
        await env.USERS.put(`user:${oderId}`, JSON.stringify(userData));
        await env.USERS.put(`tg:${telegramId}`, oderId);
        await env.USERS.delete(`code:${param}`);
        
        // Логируем
        await logToChat(env,
          `🔗 *Привязан*`,
          'connect',
          {
            telegramId,
            username: username || 'нет',
            action: 'Код текстом',
            details: `${param.substring(0, 3)}***`
          }
        );
        
        const result = await getLicenseData(env, oderId);
        
        let message = `✅ *Привязано!*\n\n`;
        
        if (result.license) {
          const timeLeft = result.license.expiresAt - Date.now();
          const hoursLeft = Math.ceil(timeLeft / (60 * 60 * 1000));
          const daysLeft = Math.ceil(timeLeft / (24 * 60 * 60 * 1000));
          
          if (result.license.type === 'trial') {
            message += `🎁 Пробный: *${hoursLeft} ч.*`;
          } else {
            message += `💎 Премиум: *${daysLeft} дн.*`;
          }
        }
        
        await sendTelegramMessage(env, chatId, message, {
          reply_markup: {
            inline_keyboard: [
              [
                { text: '📊 Статус', callback_data: 'status' },
                { text: '← Меню', callback_data: 'menu' }
              ]
            ]
          }
        });
      } else {
        // Проверяем привязку
        const existingOderId = await env.USERS.get(`tg:${telegramId}`);
        
        let welcomeMsg;
        let keyboard;
        
        if (existingOderId) {
          // Есть привязка - получаем лицензию через getLicenseData
          // Это также автоматически создаст пробный период если лицензии нет
          const result = await getLicenseData(env, existingOderId);
          const stats = await getUserStats(env, telegramId);
          const canUseGroups = result.license && LICENSE_TYPES[result.license.type]?.groupChats;
          
          if (result.license) {
            const license = result.license;
            const isValid = license.expiresAt > Date.now();
            const timeLeft = license.expiresAt - Date.now();
            const daysLeft = Math.ceil(timeLeft / (24 * 60 * 60 * 1000));
            const hoursLeft = Math.ceil(timeLeft / (60 * 60 * 1000));
            
            // Прогресс-бар
            const maxTime = license.type === 'trial' ? 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
            const percent = isValid ? Math.min(100, Math.round((timeLeft / maxTime) * 100)) : 0;
            const bar = getProgressBar(percent);
            
            const emoji = isValid ? getStatusEmoji(daysLeft, license.type) : '❌';
            const timeDisplay = license.type === 'trial' ? `${hoursLeft} ч.` : `${daysLeft} дн.`;
            const typeName = LICENSE_TYPES[license.type]?.name || license.type;
            
            welcomeMsg = `👋 *${firstName || 'Привет'}!*\n\n`;
            
            if (isValid) {
              welcomeMsg += `${emoji} *${typeName}* — ${timeDisplay}\n`;
              welcomeMsg += `${bar} ${percent}%\n\n`;
              welcomeMsg += `📊 Кликов сегодня: *${stats.todayClicks || 0}*`;
            } else {
              welcomeMsg += `❌ Подписка истекла\n\n`;
              welcomeMsg += `Продлите для продолжения работы`;
            }
            
            keyboard = getUserMenuKeyboard(isValid, canUseGroups);
          } else {
            // Привязка есть, но лицензия не создалась (странный случай)
            welcomeMsg = `👋 *${firstName || 'Привет'}!*\n\n✅ Расширение подключено\n\n⚠️ Подписка не найдена`;
            keyboard = {
              inline_keyboard: [
                [{ text: '💳 Купить подписку', callback_data: 'buy' }],
                [{ text: '💬 Поддержка', callback_data: 'support' }]
              ]
            };
          }
        } else {
          // Новый пользователь - не привязан
          welcomeMsg = `👋 *${firstName || 'Привет'}!*

*Exotic Assistant v${BOT_VERSION}* 🤖

🖱️ Автоклик по заявкам
📊 Статистика и аналитика
🔔 Уведомления в Telegram
🎁 24ч бесплатно при подключении

*Как начать:*
1️⃣ Установите расширение
2️⃣ Получите код в настройках
3️⃣ Отправьте код сюда`;
          
          keyboard = getMainMenuKeyboard();
        }
        
        await sendTelegramMessage(env, chatId, welcomeMsg, {
          reply_markup: keyboard
        });
        
        // Логируем
        await logToChat(env,
          `🚀 *${existingOderId ? 'Вход' : 'Новый'}*`,
          'user',
          {
            telegramId,
            username: username || 'нет',
            action: '/start',
            details: existingOderId ? 'Привязан' : 'Новый'
          }
        );
      }
    }
    
    // /status
    else if (update.message?.text === '/status') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      // Проверяем привязку
      const existingOderId = await env.USERS.get(`tg:${telegramId}`);
      
      if (!existingOderId) {
        await sendTelegramMessage(env, chatId, 
          `📊 *Статус*\n\n❌ Расширение не подключено\n\nПривяжите расширение через код`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '🔗 Подключить', callback_data: 'how_to_connect' }],
              [{ text: ' Тарифы', callback_data: 'buy' }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      // Получаем лицензию через getLicenseData
      const result = await getLicenseData(env, existingOderId);
      
      if (!result.license) {
        await sendTelegramMessage(env, chatId, 
          `📊 *Статус*\n\n✅ Расширение подключено\n❌ Подписка не найдена`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '💳 Купить подписку', callback_data: 'buy' }],
              [{ text: '💬 Поддержка', callback_data: 'support' }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      const license = result.license;
      const now = Date.now();
      const isValid = license.expiresAt > now;
      const timeLeft = license.expiresAt - now;
      const daysLeft = Math.max(0, Math.ceil(timeLeft / (24 * 60 * 60 * 1000)));
      const hoursLeft = Math.max(0, Math.ceil(timeLeft / (60 * 60 * 1000)));
      const expiresDate = new Date(license.expiresAt).toLocaleDateString('ru-RU');
      
      let message;
      if (isValid) {
        const emoji = getStatusEmoji(daysLeft, license.type);
        const typeText = LICENSE_TYPES[license.type]?.name || 'Премиум';
        const timeDisplay = license.type === 'trial' ? `${hoursLeft} ч.` : `${daysLeft} дн.`;
        
        // Прогресс-бар (для trial 24ч, для подписки 30дн)
        const maxTime = license.type === 'trial' ? 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
        const percent = Math.min(100, Math.round((timeLeft / maxTime) * 100));
        const bar = getProgressBar(percent);
        
        message = `📊 *Статус*\n\n`;
        message += `${emoji} ${typeText}\n`;
        message += `${bar} ${percent}%\n\n`;
        message += `⏰ ${timeDisplay} • ${expiresDate}`;
        
        if (daysLeft <= 3 && license.type !== 'trial') {
          message += `\n\n⚠️ _Скоро истечёт_`;
        }
      } else {
        message = `📊 *Статус*\n\n`;
        message += `❌ Истекла ${expiresDate}`;
      }
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: getStatusKeyboard(isValid, license.type)
      });
    }
    
    // /help  
    else if (update.message?.text === '/help') {
      const chatId = update.message.chat.id;
      
      const helpMsg = `📖 *Exotic Assistant v${BOT_VERSION}*

🖱️ *Автокликер*
Автоматически берёт заявки "В работу"

📊 *Статистика*
Клики за день • Всего • Графики

🔔 *Уведомления*
Push • Telegram • Звук

⚙️ *Настройки*
Интервал • Задержка • Громкость

🎟️ *Реферальная программа*
Пригласи друга — получи +1 день!`;
      
      await sendTelegramMessage(env, chatId, helpMsg, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: '🔗 Подключить', callback_data: 'how_to_connect' },
              { text: '❓ FAQ', callback_data: 'faq' }
            ],
            [{ text: ' Тарифы', callback_data: 'buy' }],
            [{ text: '← Меню', callback_data: 'menu' }]
          ]
        }
      });
    }
    
    // /menu
    else if (update.message?.text === '/menu') {
      const chatId = update.message.chat.id;
      
      await sendTelegramMessage(env, chatId, `🏠 *Меню*`, {
        reply_markup: getMainMenuKeyboard()
      });
    }
    
    // /id — показать ID пользователя и чата
    else if (update.message?.text === '/id') {
      const chat = update.message.chat;
      const chatId = chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const firstName = update.message.from.first_name;
      
      let message = `🆔 *Информация об ID*\n\n`;
      message += `👤 *Вы:*\n`;
      message += `├ ID: \`${telegramId}\`\n`;
      if (username) message += `├ Username: @${username}\n`;
      if (firstName) message += `└ Имя: ${firstName}\n`;
      
      if (chat.type === 'group' || chat.type === 'supergroup') {
        message += `\n💬 *Этот чат:*\n`;
        message += `├ ID: \`${chatId}\`\n`;
        message += `├ Тип: ${chat.type === 'supergroup' ? 'Супергруппа' : 'Группа'}\n`;
        if (chat.title) message += `└ Название: ${chat.title}\n`;
      } else if (chat.type === 'private') {
        message += `\n💬 *Тип чата:* Личные сообщения`;
      }
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: {
          inline_keyboard: [
            [{ text: '← Меню', callback_data: 'menu' }]
          ]
        }
      });
    }
    
    // Код привязки (8 символов)
    else if (update.message?.text && /^[A-Z0-9]{8}$/i.test(update.message.text.trim())) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const firstName = update.message.from.first_name;
      const code = update.message.text.trim().toUpperCase();
      
      const codeData = await env.USERS.get(`code:${code}`);
      
      if (codeData) {
        const { oderId, deviceId } = JSON.parse(codeData);
        
        const userData = {
          oderId,
          deviceId,
          telegramId,
          username: username || null,
          firstName: firstName || null,
          connectedAt: Date.now()
        };
        
        await env.USERS.put(`user:${oderId}`, JSON.stringify(userData));
        await env.USERS.put(`tg:${telegramId}`, oderId);
        await env.USERS.delete(`code:${code}`);
        
        // Логируем
        await logToChat(env,
          `🔗 *Привязан*`,
          'connect',
          {
            telegramId,
            username: username || 'нет',
            action: 'Код текстом',
            details: `${code.substring(0, 3)}***`
          }
        );
        
        const result = await getLicenseData(env, oderId);
        
        let message = `✅ *Привязано!*\n\n`;
        
        if (result.license) {
          const timeLeft = result.license.expiresAt - Date.now();
          const hoursLeft = Math.ceil(timeLeft / (60 * 60 * 1000));
          const daysLeft = Math.ceil(timeLeft / (24 * 60 * 60 * 1000));
          
          if (result.license.type === 'trial') {
            message += `🎁 Пробный: *${hoursLeft} ч.*`;
          } else {
            message += `💎 Премиум: *${daysLeft} дн.*`;
          }
        }
        
        await sendTelegramMessage(env, chatId, message, {
          reply_markup: {
            inline_keyboard: [
              [
                { text: '📊 Статус', callback_data: 'status' },
                { text: '← Меню', callback_data: 'menu' }
              ]
            ]
          }
        });
      } else {
        await sendTelegramMessage(env, chatId, 
          `❌ *Код не найден*\n\nПолучите новый в настройках`,
          { reply_markup: getMainMenuKeyboard() }
        );
      }
    }
    
    // Неизвестное
    else if (update.message?.text) {
      const chatId = update.message.chat.id;
      
      await sendTelegramMessage(env, chatId, 
        `🤔 Не понимаю.\n\nВыберите из меню:`,
        { reply_markup: getMainMenuKeyboard() }
      );
    }
    
    return jsonResponse({ ok: true });
    
  } catch (error) {
    console.error('Webhook error:', error);
    
    await logToChat(env,
      `🚨 *Критическая ошибка*\n\n` +
      `📍 Path: \`${path}\`\n` +
      `❌ Error: ${error.message}`,
      'error'
    );
    
    return jsonResponse({ error: 'Internal server error', message: error.message }, 500);
  }
}

export default {
  // Cron handler для напоминаний об истечении подписки
  async scheduled(event, env, ctx) {
    ctx.waitUntil(checkAndSendExpirationReminders(env));
  },
  
  async fetch(request, env) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const path = url.pathname;
    
    // HTTPS проверка - редирект с HTTP
    if (url.protocol === 'http:' && !url.hostname.includes('localhost')) {
      return Response.redirect(`https://${url.host}${url.pathname}${url.search}`, 301);
    }
    
    // Security headers
    const securityHeaders = {
      'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'DENY',
      'X-XSS-Protection': '1; mode=block',
      'Referrer-Policy': 'strict-origin-when-cross-origin'
    };
    
    if (request.method === 'OPTIONS') {
      return new Response(null, { 
        headers: { ...CORS_HEADERS, ...securityHeaders } 
      });
    }
    
    try {
      // Health check эндпоинт для мониторинга
      if (path === '/api/health' && request.method === 'GET') {
        return jsonResponse({
          status: 'ok',
          version: BOT_VERSION,
          timestamp: Date.now(),
          uptime: Date.now() - metrics.startTime
        });
      }
      
      // Telegram Webhook
      if (path === '/webhook' && request.method === 'POST') {
        return await handleTelegramWebhook(request, env);
      }
      
      // Основные эндпоинты
      if (path === '/api/license/check' && request.method === 'POST') {
        return await handleLicenseCheck(request, env);
      }
      
      // ========== БЫСТРАЯ ПРОВЕРКА ВЕРСИИ ЛИЦЕНЗИИ ==========
      // Легковесный endpoint для частых проверок синхронизации
      // Возвращает только версию, расширение сравнивает с локальной
      if (path === '/api/license/version' && request.method === 'POST') {
        const { data } = await safeParseJson(request);
        const { oderId } = data || {};
        
        if (!oderId || !isValidOderId(oderId)) {
          return jsonResponse({ error: 'Invalid oderId' }, 400);
        }
        
        // Получаем telegramId по oderId
        const userData = await env.USERS.get(`user:${oderId}`);
        if (!userData) {
          return jsonResponse({ connected: false, version: 0 });
        }
        
        const user = JSON.parse(userData);
        const version = await getLicenseVersion(env, user.telegramId);
        
        return jsonResponse({ 
          connected: true, 
          version,
          timestamp: Date.now()
        });
      }
      
      if (path === '/api/generate-code' && request.method === 'POST') {
        return await handleGenerateCode(request, env);
      }
      if (path === '/api/status' && request.method === 'POST') {
        return await handleStatus(request, env);
      }
      if (path === '/api/notify' && request.method === 'POST') {
        return await handleNotify(request, env);
      }
      
      // ========== REAL-TIME ПОДКЛЮЧЕНИЕ (SSE) ==========
      // Server-Sent Events для мгновенного оповещения о подключении Telegram
      if (path === '/api/connect/stream' && request.method === 'GET') {
        return await handleConnectionStream(request, env);
      }
      
      // Telegram бот эндпоинты  
      if (path === '/api/telegram/connect' && request.method === 'POST') {
        return await handleTelegramConnect(request, env);
      }
      
      // ========== СИНХРОНИЗАЦИЯ НАСТРОЕК РАСШИРЕНИЯ ==========
      // Получить настройки из бота для расширения
      if (path === '/api/settings/get' && request.method === 'POST') {
        const { data, error } = await safeParseJson(request);
        if (error) return jsonResponse({ error: 'Invalid JSON' }, 400);
        
        const { oderId, telegramId } = data;
        
        if (!oderId && !telegramId) {
          return jsonResponse({ error: 'oderId or telegramId required' }, 400);
        }
        
        let tgId = telegramId;
        
        // Если передан oderId, получаем telegramId
        if (oderId && !telegramId) {
          const userData = await env.USERS.get(`user:${oderId}`);
          if (userData) {
            const user = JSON.parse(userData);
            tgId = user.telegramId;
          }
        }
        
        if (!tgId) {
          return jsonResponse({ error: 'User not found' }, 404);
        }
        
        // Получаем настройки из бота
        const botSettings = await getUserSettings(env, tgId);
        
        // Получаем лицензию
        const licenseData = await env.USERS.get(`license:tg:${tgId}`);
        let license = null;
        if (licenseData) {
          license = JSON.parse(licenseData);
        }
        
        recordMetric(path, true, Date.now() - startTime);
        return jsonResponse({
          ok: true,
          settings: {
            notifications: botSettings.notifications,
            syncedAt: Date.now()
          },
          license: license ? {
            type: license.type,
            expiresAt: license.expiresAt,
            daysLeft: Math.max(0, Math.ceil((license.expiresAt - Date.now()) / MS_PER_DAY))
          } : null
        });
      }
      
      // Сохранить настройки расширения в бот
      if (path === '/api/settings/sync' && request.method === 'POST') {
        const { data, error } = await safeParseJson(request);
        if (error) return jsonResponse({ error: 'Invalid JSON' }, 400);
        
        const { oderId, telegramId, settings } = data;
        
        if (!oderId && !telegramId) {
          return jsonResponse({ error: 'oderId or telegramId required' }, 400);
        }
        
        let tgId = telegramId;
        
        // Если передан oderId, получаем telegramId
        if (oderId && !telegramId) {
          const userData = await env.USERS.get(`user:${oderId}`);
          if (userData) {
            const user = JSON.parse(userData);
            tgId = user.telegramId;
          }
        }
        
        if (!tgId) {
          return jsonResponse({ error: 'User not found' }, 404);
        }
        
        // Получаем текущие настройки
        const currentSettings = await getUserSettings(env, tgId);
        
        // Обновляем настройки (только разрешённые поля)
        const updatedSettings = {
          ...currentSettings,
          notifications: {
            ...currentSettings.notifications,
            // Синхронизируем только эти настройки из расширения
            clicks: settings?.clickNotifications ?? currentSettings.notifications.clicks,
            sound: settings?.soundEnabled ?? currentSettings.notifications.sound
          },
          // Дополнительные данные из расширения
          extensionVersion: settings?.extensionVersion || null,
          lastSync: Date.now()
        };
        
        await saveUserSettings(env, tgId, updatedSettings);
        
        recordMetric(path, true, Date.now() - startTime);
        return jsonResponse({
          ok: true,
          message: 'Settings synced',
          syncedAt: Date.now()
        });
      }
      
      // Получить статус синхронизации
      if (path === '/api/sync/status' && request.method === 'POST') {
        const { data, error } = await safeParseJson(request);
        if (error) return jsonResponse({ error: 'Invalid JSON' }, 400);
        
        const { oderId } = data;
        
        if (!oderId) {
          return jsonResponse({ error: 'oderId required' }, 400);
        }
        
        const userData = await env.USERS.get(`user:${oderId}`);
        if (!userData) {
          return jsonResponse({ connected: false, error: 'NOT_CONNECTED' });
        }
        
        const user = JSON.parse(userData);
        const tgId = user.telegramId;
        
        // Получаем настройки и лицензию
        const botSettings = await getUserSettings(env, tgId);
        const licenseData = await env.USERS.get(`license:tg:${tgId}`);
        let license = null;
        if (licenseData) {
          license = JSON.parse(licenseData);
        }
        
        // Групповые чаты
        const groupData = await getUserGroupChats(env, tgId);
        
        recordMetric(path, true, Date.now() - startTime);
        return jsonResponse({
          connected: true,
          telegramId: tgId,
          username: user.username,
          license: license ? {
            valid: license.expiresAt > Date.now(),
            type: license.type,
            typeName: LICENSE_TYPES[license.type]?.name || license.type,
            expiresAt: license.expiresAt,
            daysLeft: Math.max(0, Math.ceil((license.expiresAt - Date.now()) / MS_PER_DAY))
          } : null,
          settings: botSettings.notifications,
          groups: {
            count: groupData.chats.length,
            pending: groupData.pending?.length || 0
          },
          lastSync: botSettings.lastSync || null,
          serverTime: Date.now(),
          version: BOT_VERSION
        });
      }
      
      // Админские эндпоинты
      if (path === '/api/admin/grant-license' && request.method === 'POST') {
        return await handleGrantLicense(request, env);
      }
      if (path === '/api/license/info' && request.method === 'POST') {
        return await handleLicenseInfo(request, env);
      }
      
      // Health check (минимальная информация для безопасности)
      if (path === '/api/health') {
        // Публичный endpoint - только базовый статус, без деталей системы
        return jsonResponse({ 
          status: 'ok', 
          version: BOT_VERSION,
          timestamp: Date.now()
        });
      }
      
      // Подробные метрики (только для админа)
      if (path === '/api/metrics') {
        const authHeader = request.headers.get('Authorization');
        const adminToken = env.ADMIN_TOKEN;
        
        if (!adminToken || authHeader !== `Bearer ${adminToken}`) {
          return jsonResponse({ error: 'Unauthorized' }, 401);
        }
        
        const endpointStats = Object.entries(metrics.endpoints).map(([endpoint, data]) => ({
          endpoint,
          count: data.count,
          errors: data.errors,
          avgDuration: data.count > 0 ? Math.round(data.totalDuration / data.count) + 'ms' : '0ms',
          errorRate: data.count > 0 ? ((data.errors / data.count) * 100).toFixed(2) + '%' : '0%'
        }));
        
        return jsonResponse({
          uptime: Date.now() - metrics.startTime,
          requests: metrics.requests,
          endpoints: endpointStats,
          recentErrors: metrics.errors.slice(-20),
          memory: 'N/A (Workers)'  
        });
      }
      
      // Webhook info для отладки (ТОЛЬКО с авторизацией)
      if (path === '/api/webhook-info') {
        // Требуем авторизацию для доступа к информации о webhook
        const authHeader = request.headers.get('Authorization');
        const adminToken = env.ADMIN_TOKEN;
        
        if (!adminToken || authHeader !== `Bearer ${adminToken}`) {
          return jsonResponse({ error: 'Unauthorized' }, 401);
        }
        
        const botToken = env.TELEGRAM_BOT_TOKEN;
        if (!botToken) {
          return jsonResponse({ error: 'TELEGRAM_BOT_TOKEN not set' }, 500);
        }
        try {
          const response = await fetch(`https://api.telegram.org/bot${botToken}/getWebhookInfo`);
          const data = await response.json();
          return jsonResponse(data);
        } catch (e) {
          return jsonResponse({ error: e.message }, 500);
        }
      }
      
      // Установка webhook (ТОЛЬКО с авторизацией)
      if (path === '/api/set-webhook') {
        // БЕЗОПАСНОСТЬ: Требуем авторизацию
        const authHeader = request.headers.get('Authorization');
        const adminToken = env.ADMIN_TOKEN;
        
        if (!adminToken || authHeader !== `Bearer ${adminToken}`) {
          return jsonResponse({ error: 'Unauthorized' }, 401);
        }
        
        const botToken = env.TELEGRAM_BOT_TOKEN;
        if (!botToken) {
          return jsonResponse({ error: 'TELEGRAM_BOT_TOKEN not set' }, 500);
        }
        const workerUrl = new URL(request.url).origin;
        const webhookUrl = `${workerUrl}/webhook`;
        
        // Добавляем secret_token если настроен
        let apiUrl = `https://api.telegram.org/bot${botToken}/setWebhook?url=${webhookUrl}`;
        if (env.TELEGRAM_WEBHOOK_SECRET) {
          apiUrl += `&secret_token=${env.TELEGRAM_WEBHOOK_SECRET}`;
        }
        
        try {
          const response = await fetch(apiUrl);
          const data = await response.json();
          
          // Логируем установку webhook
          await logToChat(env, 
            `🔧 Webhook установлен\n\n` +
            `📡 URL: \`${webhookUrl}\`\n` +
            `🔐 Secret: ${env.TELEGRAM_WEBHOOK_SECRET ? 'Установлен' : 'НЕ УСТАНОВЛЕН!'}\n` +
            `✅ Результат: ${data.ok ? 'Успешно' : 'Ошибка'}`,
            'system'
          );
          
          return jsonResponse({ ...data, webhookUrl, secretConfigured: !!env.TELEGRAM_WEBHOOK_SECRET });
        } catch (e) {
          return jsonResponse({ error: e.message }, 500);
        }
      }
      
      // Тестовый лог (ТОЛЬКО с авторизацией - предотвращает спам)
      if (path === '/api/test-log') {
        // БЕЗОПАСНОСТЬ: Требуем авторизацию для предотвращения спама
        const authHeader = request.headers.get('Authorization');
        const adminToken = env.ADMIN_TOKEN;
        
        if (!adminToken || authHeader !== `Bearer ${adminToken}`) {
          return jsonResponse({ error: 'Unauthorized' }, 401);
        }
        
        await logToChat(env, 
          `🧪 *Тестовое сообщение*\n\n` +
          `Если вы видите это сообщение, логирование работает корректно!`,
          'success',
          { 
            action: 'Тест системы логирования',
            details: `Запрос от: ${request.headers.get('CF-Connecting-IP') || 'unknown'}`
          }
        );
        recordMetric(path, true, Date.now() - startTime);
        return jsonResponse({ ok: true, message: 'Test log sent to configured chat' });
      }
      
      recordMetric(path, false, Date.now() - startTime, 'Not found');
      return jsonResponse({ error: 'Not found' }, 404);
      
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // Структурированное логирование ошибки
      structuredLog('error', error.message, {
        path,
        duration,
        stack: error.stack?.substring(0, 500),
        ip: request.headers.get('CF-Connecting-IP')
      });
      
      // Записываем метрику
      recordMetric(path, false, duration, error.message);
      
      // Логируем критические ошибки в Telegram
      await logToChat(env,
        `🚨 *Критическая ошибка*\n\n` +
        `📍 Path: \`${path}\`\n` +
        `❌ Error: ${error.message}`,
        'error'
      );
      
      return jsonResponse({ error: 'Internal server error', message: error.message }, 500);
    }
  }
};

