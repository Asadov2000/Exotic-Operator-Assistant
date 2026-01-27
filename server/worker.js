// ==========================================
// EXOTIC OPERATOR ASSISTANT BOT
// ==========================================

// ==================== TELEGRAM CLIENT ====================
class TelegramClient {
  constructor(botToken, options = {}) {
    this.botToken = botToken;
    this.baseUrl = `https://api.telegram.org/bot${botToken}`;
    this.timeout = options.timeout || 10000;
    this.maxRetries = options.maxRetries || 3;
    this.retryDelay = options.retryDelay || 1000;
  }

  async request(method, params = {}, retries = this.maxRetries) {
    if (!this.botToken) {
      console.error('[TG] Bot token not set');
      return { ok: false, error: 'Bot token not configured' };
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const response = await fetch(`${this.baseUrl}/${method}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(params),
          signal: controller.signal
        });

        clearTimeout(timeoutId);
        const result = await response.json();

        if (result.ok) return result;

        if (result.error_code === 429) {
          const retryAfter = (result.parameters?.retry_after || 5) * 1000;
          console.warn(`[TG] Rate limited, waiting ${retryAfter}ms`);
          if (attempt < retries) {
            await this.sleep(retryAfter);
            continue;
          }
        }

        if (result.error_code >= 500 && attempt < retries) {
          console.warn(`[TG] Server error ${result.error_code}, retry ${attempt}/${retries}`);
          await this.sleep(this.retryDelay * Math.pow(2, attempt - 1));
          continue;
        }

        console.error(`[TG] API error: ${result.description}`);
        return result;

      } catch (error) {
        clearTimeout(timeoutId);
        
        if (error.name === 'AbortError') {
          console.error(`[TG] Request timeout (attempt ${attempt}/${retries})`);
        } else {
          console.error(`[TG] Request error: ${error.message}`);
        }

        if (attempt < retries) {
          await this.sleep(this.retryDelay * Math.pow(2, attempt - 1));
          continue;
        }

        return { ok: false, error: error.message };
      }
    }

    return { ok: false, error: 'Max retries exceeded' };
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async sendMessage(chatId, text, options = {}) {
    return this.request('sendMessage', {
      chat_id: chatId,
      text: text,
      parse_mode: 'HTML',
      ...options
    });
  }

  async editMessage(chatId, messageId, text, options = {}) {
    return this.request('editMessageText', {
      chat_id: chatId,
      message_id: messageId,
      text: text,
      parse_mode: 'HTML',
      ...options
    });
  }

  async deleteMessage(chatId, messageId) {
    return this.request('deleteMessage', {
      chat_id: chatId,
      message_id: messageId
    }, 1);
  }

  async answerCallback(callbackQueryId, text = '', showAlert = false) {
    return this.request('answerCallbackQuery', {
      callback_query_id: callbackQueryId,
      text: text,
      show_alert: showAlert
    }, 1);
  }

  async sendPhoto(chatId, photo, caption = '', options = {}) {
    return this.request('sendPhoto', {
      chat_id: chatId,
      photo: photo,
      caption: caption,
      parse_mode: 'HTML',
      ...options
    });
  }

  async getWebhookInfo() {
    return this.request('getWebhookInfo', {}, 1);
  }

  async setWebhook(url, options = {}) {
    return this.request('setWebhook', { url, ...options }, 1);
  }

  async getChat(chatId) {
    return this.request('getChat', { chat_id: chatId }, 1);
  }

  async getChatMember(chatId, userId) {
    return this.request('getChatMember', {
      chat_id: chatId,
      user_id: userId
    }, 1);
  }
}

let telegramClient = null;
function getTelegramClient(env) {
  if (!telegramClient || telegramClient.botToken !== env.TELEGRAM_BOT_TOKEN) {
    telegramClient = new TelegramClient(env.TELEGRAM_BOT_TOKEN);
  }
  return telegramClient;
}

// ==================== CONSTANTS ====================
const LICENSE_TYPES = {
  promo: { name: 'Promo', emoji: '🎁', maxDevices: 1, canUseGroups: false, notifyOptions: ['dm'], price: 0, priceText: 'Бесплатно' },
  L1: { name: 'L1', emoji: '⭐', maxDevices: 1, canUseGroups: false, notifyOptions: ['dm'], price: 10, priceText: '10 USDT' },
  L2: { name: 'L2', emoji: '⭐⭐', maxDevices: 1, canUseGroups: true, notifyOptions: ['dm', 'group'], price: 15, priceText: '15 USDT' },
  L3: { name: 'L3', emoji: '⭐⭐⭐', maxDevices: 2, canUseGroups: true, notifyOptions: ['dm', 'group', 'both'], price: 20, priceText: '20 USDT' }
};

// Московское время
function getMoscowTime() {
  return new Date().toLocaleString('ru-RU', {
    timeZone: 'Europe/Moscow',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

const NOTIFY_OPTIONS = {
  dm: '📱 Личные сообщения',
  group: '👥 Групповой чат',
  both: '📱+👥 Оба варианта'
};

// Rate limiting
const rateLimits = new Map();
const ipRateLimits = new Map();
const csrfTokens = new Map();

const RATE_LIMITS = {
  telegram: { requests: 30, window: 60000 },      // 30 req/min для Telegram
  api: { requests: 60, window: 60000 },           // 60 req/min для API
  apiStrict: { requests: 10, window: 60000 },     // 10 req/min для критических API
  ip: { requests: 100, window: 60000 }            // 100 req/min на IP
};

function checkRateLimit(id, type = 'telegram') {
  const limits = RATE_LIMITS[type] || RATE_LIMITS.telegram;
  const store = type === 'ip' ? ipRateLimits : rateLimits;
  
  const now = Date.now();
  const windowStart = now - limits.window;
  let requests = store.get(id) || [];
  requests = requests.filter(t => t > windowStart);
  
  if (requests.length >= limits.requests) {
    return { allowed: false, resetIn: Math.ceil((requests[0] + limits.window - now) / 1000) };
  }
  
  requests.push(now);
  store.set(id, requests);
  
  // Очистка памяти
  if (store.size > 10000) {
    const entries = Array.from(store.entries());
    entries.slice(0, 5000).forEach(([k]) => store.delete(k));
  }
  
  return { allowed: true };
}

// CSRF токены для критических операций
function generateCsrfToken(userId) {
  const token = crypto.randomUUID();
  const expires = Date.now() + 10 * 60 * 1000; // 10 минут
  csrfTokens.set(`${userId}:${token}`, expires);
  
  // Очистка старых токенов
  if (csrfTokens.size > 5000) {
    const now = Date.now();
    for (const [k, v] of csrfTokens) {
      if (v < now) csrfTokens.delete(k);
    }
  }
  
  return token;
}

function validateCsrfToken(userId, token) {
  const key = `${userId}:${token}`;
  const expires = csrfTokens.get(key);
  if (!expires || expires < Date.now()) {
    csrfTokens.delete(key);
    return false;
  }
  csrfTokens.delete(key); // Одноразовый токен
  return true;
}

// Получить IP из запроса
function getClientIP(request) {
  return request.headers.get('CF-Connecting-IP') ||
         request.headers.get('X-Forwarded-For')?.split(',')[0]?.trim() ||
         request.headers.get('X-Real-IP') ||
         'unknown';
}

// ==================== CACHE ====================
const cache = new Map();
const CACHE_TTL = 60000;

function getCached(key) {
  const item = cache.get(key);
  if (!item || Date.now() > item.expires) {
    cache.delete(key);
    return null;
  }
  return item.value;
}

function setCache(key, value, ttl = CACHE_TTL) {
  cache.set(key, { value, expires: Date.now() + ttl });
  if (cache.size > 5000) {
    const now = Date.now();
    for (const [k, v] of cache) {
      if (v.expires < now) cache.delete(k);
    }
  }
}

// ==================== HELPERS ====================
function isAdmin(env, telegramId) {
  return String(telegramId) === String(env.ADMIN_TELEGRAM_ID);
}

function isGroupChat(chat) {
  return chat && (chat.type === 'group' || chat.type === 'supergroup');
}

function formatDate(timestamp) {
  if (!timestamp) return 'Не указана';
  return new Date(timestamp).toLocaleString('ru-RU', {
    timeZone: 'Europe/Moscow',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function formatDaysLeft(expiresAt) {
  if (!expiresAt) return 'Бессрочно';
  const days = Math.ceil((expiresAt - Date.now()) / (1000 * 60 * 60 * 24));
  if (days < 0) return 'Истекла';
  if (days === 0) return 'Сегодня';
  if (days === 1) return '1 день';
  if (days < 5) return `${days} дня`;
  return `${days} дней`;
}

function escapeHtml(text) {
  if (!text) return '';
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// ==================== DATABASE FUNCTIONS ====================
async function getUser(env, oderId) {
  const data = await env.USERS.get(`user:${oderId}`);
  return data ? JSON.parse(data) : null;
}

async function saveUser(env, oderId, userData) {
  await env.USERS.put(`user:${oderId}`, JSON.stringify(userData));
}

async function getUserByTelegramId(env, telegramId) {
  const mapping = await env.USERS.get(`tg_to_oder:${telegramId}`);
  if (!mapping) return null;
  return await getUser(env, mapping);
}

async function getLicense(env, telegramId) {
  const data = await env.USERS.get(`license:${telegramId}`);
  return data ? JSON.parse(data) : null;
}

async function saveLicense(env, telegramId, licenseData) {
  await env.USERS.put(`license:${telegramId}`, JSON.stringify(licenseData));
  // Увеличиваем версию для синхронизации с расширением
  const version = (await env.USERS.get(`license_version:${telegramId}`)) || '0';
  await env.USERS.put(`license_version:${telegramId}`, String(parseInt(version) + 1));
}

async function getSettings(env, telegramId) {
  const data = await env.USERS.get(`settings:${telegramId}`);
  return data ? JSON.parse(data) : {
    notifyDestination: 'dm',
    expiryReminders: true
  };
}

async function saveSettings(env, telegramId, settings) {
  await env.USERS.put(`settings:${telegramId}`, JSON.stringify(settings));
}

async function getDevices(env, telegramId) {
  try {
    const data = await env.USERS.get(`devices:${telegramId}`);
    if (!data) return { devices: [] };
    
    const parsed = JSON.parse(data);
    
    // Защита от некорректных данных
    if (!parsed || !Array.isArray(parsed.devices)) {
      return { devices: [] };
    }
    
    // Миграция: если старый формат (массив строк), конвертируем
    if (parsed.devices.length > 0 && typeof parsed.devices[0] === 'string') {
      parsed.devices = parsed.devices.map((id, index) => ({
        id,
        name: `Устройство ${index + 1}`,
        addedAt: Date.now(),
        lastSeen: Date.now()
      }));
      await env.USERS.put(`devices:${telegramId}`, JSON.stringify(parsed));
    }
    
    return parsed;
  } catch (error) {
    console.error('getDevices error:', error);
    return { devices: [] };
  }
}

async function saveDevices(env, telegramId, devicesData) {
  await env.USERS.put(`devices:${telegramId}`, JSON.stringify(devicesData));
}

// Найти устройство по ID
function findDevice(devices, deviceId) {
  return devices.devices.find(d => d.id === deviceId);
}

// Добавить устройство
async function addDevice(env, telegramId, deviceId, deviceName) {
  const devices = await getDevices(env, telegramId);
  
  // Проверяем уникальность имени
  let finalName = deviceName || 'Моё устройство';
  let counter = 1;
  const baseName = finalName;
  while (devices.devices.some(d => d.name === finalName)) {
    finalName = `${baseName} (${counter++})`;
  }
  
  devices.devices.push({
    id: deviceId,
    name: finalName,
    addedAt: Date.now(),
    lastSeen: Date.now()
  });
  
  await saveDevices(env, telegramId, devices);
  return finalName;
}

// Удалить устройство
async function removeDevice(env, telegramId, deviceId) {
  const devices = await getDevices(env, telegramId);
  const index = devices.devices.findIndex(d => d.id === deviceId);
  if (index === -1) return false;
  
  devices.devices.splice(index, 1);
  await saveDevices(env, telegramId, devices);
  return true;
}

// Обновить lastSeen устройства
async function updateDeviceLastSeen(env, telegramId, deviceId) {
  const devices = await getDevices(env, telegramId);
  const device = findDevice(devices, deviceId);
  if (device) {
    device.lastSeen = Date.now();
    await saveDevices(env, telegramId, devices);
  }
}

async function getUserGroups(env, telegramId) {
  const data = await env.USERS.get(`user_groups:${telegramId}`);
  return data ? JSON.parse(data) : { chats: [] };
}

async function saveUserGroups(env, telegramId, groupsData) {
  await env.USERS.put(`user_groups:${telegramId}`, JSON.stringify(groupsData));
}

// Все пользователи (для статистики и рассылки)
async function getAllUsers(env) {
  // Используем user_info как основной источник (там все зарегистрированные)
  const userInfoList = await env.USERS.list({ prefix: 'user_info:' });
  const licenseList = await env.USERS.list({ prefix: 'license:' });
  
  // Собираем уникальные ID из обоих источников
  const uniqueIds = new Set();
  
  for (const key of userInfoList.keys) {
    uniqueIds.add(key.name.replace('user_info:', ''));
  }
  
  for (const key of licenseList.keys) {
    uniqueIds.add(key.name.replace('license:', ''));
  }
  
  const users = [];
  
  for (const telegramId of uniqueIds) {
    // Пропускаем невалидные ID (должны быть числами)
    if (!/^\d+$/.test(telegramId)) continue;
    
    const license = await getLicense(env, telegramId);
    const settings = await getSettings(env, telegramId);
    const userInfo = await env.USERS.get(`user_info:${telegramId}`);
    
    users.push({
      telegramId,
      license,
      settings,
      info: userInfo ? JSON.parse(userInfo) : {}
    });
  }
  
  // Сортируем: сначала с активной подпиской, потом по дате регистрации
  const now = Date.now();
  users.sort((a, b) => {
    const aActive = a.license?.expiresAt > now ? 1 : 0;
    const bActive = b.license?.expiresAt > now ? 1 : 0;
    if (aActive !== bActive) return bActive - aActive;
    return (b.info?.createdAt || 0) - (a.info?.createdAt || 0);
  });
  
  return users;
}

// Сохранение информации о пользователе
async function saveUserInfo(env, telegramId, info) {
  await env.USERS.put(`user_info:${telegramId}`, JSON.stringify({
    ...info,
    lastSeen: Date.now()
  }));
}

// Групповые чаты
async function getPendingGroups(env) {
  const data = await env.USERS.get('pending_groups');
  return data ? JSON.parse(data) : [];
}

async function savePendingGroups(env, groups) {
  await env.USERS.put('pending_groups', JSON.stringify(groups));
}

async function getApprovedGroups(env) {
  const data = await env.USERS.get('approved_groups');
  return data ? JSON.parse(data) : [];
}

async function saveApprovedGroups(env, groups) {
  await env.USERS.put('approved_groups', JSON.stringify(groups));
}

// Бан лист
async function isBanned(env, telegramId) {
  const data = await env.USERS.get(`banned:${telegramId}`);
  return !!data;
}

async function setBanned(env, telegramId, banned) {
  if (banned) {
    await env.USERS.put(`banned:${telegramId}`, JSON.stringify({ bannedAt: Date.now() }));
  } else {
    await env.USERS.delete(`banned:${telegramId}`);
  }
}

// ==================== LOGGING ====================
async function logAction(env, action, details = {}) {
  const logChatId = env.LOG_CHAT_ID;
  if (!logChatId) return;

  const client = getTelegramClient(env);
  const time = getMoscowTime();
  
  const icons = {
    user_start: '👋',
    user_connect: '🔗',
    license_grant: '🎁',
    license_revoke: '❌',
    license_extend: '⏰',
    group_request: '📨',
    group_approve: '✅',
    group_reject: '🚫',
    group_remove: '🗑',
    broadcast: '📢',
    settings_change: '⚙️',
    admin_action: '🔐',
    error: '❗',
    system: '🔧',
    // API логи
    api_device_add: '📱',
    api_device_remove: '📵',
    api_device_rename: '✏️',
    api_license_check: '🔍',
    api_rate_limit: '⏱️',
    security_alert: '🚨'
  };

  let text = `${icons[action] || '📝'} <b>${action.toUpperCase()}</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n`;
  
  if (details.telegramId) text += `🆔 ID: <code>${details.telegramId}</code>\n`;
  if (details.username) text += `👤 User: @${escapeHtml(details.username)}\n`;
  if (details.firstName) text += `📛 Имя: ${escapeHtml(details.firstName)}\n`;
  if (details.license) text += `📋 Подписка: ${details.license}\n`;
  if (details.days) text += `📅 Дней: ${details.days}\n`;
  if (details.chatId) text += `💬 Чат: <code>${details.chatId}</code>\n`;
  if (details.chatTitle) text += `📌 Название: ${escapeHtml(details.chatTitle)}\n`;
  if (details.ip) text += `🌐 IP: <code>${details.ip}</code>\n`;
  if (details.deviceId) text += `📱 Device: <code>${details.deviceId.slice(0, 12)}...</code>\n`;
  if (details.deviceName) text += `📱 Имя: ${escapeHtml(details.deviceName)}\n`;
  if (details.message) text += `💬 ${escapeHtml(details.message)}\n`;
  if (details.error) text += `⚠️ Ошибка: ${escapeHtml(details.error)}\n`;
  
  text += `━━━━━━━━━━━━━━━━━━━━\n`;
  text += `🕐 ${time} MSK`;

  try {
    await client.sendMessage(logChatId, text, { disable_notification: action !== 'error' });
  } catch (e) {
    console.error('[LOG] Failed to send:', e.message);
  }
}

// ==================== USER MENU ====================
function getUserMainMenu() {
  return {
    inline_keyboard: [
      [{ text: '📊 Статистика', callback_data: 'user_stats' }],
      [{ text: '📱 Устройства', callback_data: 'user_devices' }],
      [{ text: '⚙️ Настройки', callback_data: 'user_settings' }],
      [{ text: '👥 Групповые чаты', callback_data: 'user_groups' }],
      [{ text: '💰 Цены и тарифы', callback_data: 'user_prices' }],
      [{ text: '❓ Помощь', callback_data: 'user_help' }],
      [{ text: '💬 Поддержка', callback_data: 'user_support' }]
    ]
  };
}

async function showUserMenu(env, chatId, messageId = null) {
  const client = getTelegramClient(env);
  
  const text = `🤖 <b>Exotic Operator Assistant</b>\n\n` +
    `Добро пожаловать! Это ваш персональный ассистент для работы с Exotic.\n\n` +
    `Выберите раздел:`;

  if (messageId) {
    await client.editMessage(chatId, messageId, text, { reply_markup: getUserMainMenu() });
  } else {
    await client.sendMessage(chatId, text, { reply_markup: getUserMainMenu() });
  }
}

async function showUserStats(env, chatId, telegramId, messageId) {
  const client = getTelegramClient(env);
  const license = await getLicense(env, telegramId);
  const settings = await getSettings(env, telegramId);
  const devices = await getDevices(env, telegramId);
  const userInfo = await env.USERS.get(`user_info:${telegramId}`);
  const info = userInfo ? JSON.parse(userInfo) : {};

  let text = `📊 <b>Статистика аккаунта</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;

  // Информация о подписке
  if (license && license.type && license.expiresAt > Date.now()) {
    const licenseInfo = LICENSE_TYPES[license.type] || { name: license.type, emoji: '📋' };
    text += `📋 <b>Подписка:</b> ${licenseInfo.emoji} ${licenseInfo.name}\n`;
    text += `⏳ <b>Истекает:</b> ${formatDaysLeft(license.expiresAt)}\n`;
    text += `📅 <b>Дата окончания:</b> ${formatDate(license.expiresAt)}\n\n`;
    
    // Куда приходят уведомления
    const notifyDest = NOTIFY_OPTIONS[settings.notifyDestination] || NOTIFY_OPTIONS.dm;
    text += `🔔 <b>Уведомления:</b> ${notifyDest}\n\n`;
  } else {
    text += `📋 <b>Подписка:</b> Отсутствует\n\n`;
    text += `💡 <i>Для получения подписки обратитесь к администратору.</i>\n\n`;
  }

  // Устройства
  const deviceCount = devices.devices ? devices.devices.length : 0;
  const maxDevices = license ? (LICENSE_TYPES[license.type]?.maxDevices || 1) : 0;
  text += `📱 <b>Устройства:</b> ${deviceCount}/${maxDevices}\n`;
  
  // Дата регистрации
  if (info.createdAt) {
    text += `📆 <b>Регистрация:</b> ${formatDate(info.createdAt)}\n`;
  }

  const keyboard = {
    inline_keyboard: [
      [{ text: '← Назад', callback_data: 'menu' }]
    ]
  };

  await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
}

async function showUserSettings(env, chatId, telegramId, messageId) {
  const client = getTelegramClient(env);
  const license = await getLicense(env, telegramId);
  const settings = await getSettings(env, telegramId);

  let text = `⚙️ <b>Настройки</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (!license || !license.type || license.expiresAt <= Date.now()) {
    text += `❌ У вас нет активной подписки.\n\n`;
    text += `Настройки уведомлений доступны только с подпиской.`;
    
    const keyboard = {
      inline_keyboard: [
        [{ text: '← Назад', callback_data: 'menu' }]
      ]
    };
    await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
    return;
  }

  const licenseInfo = LICENSE_TYPES[license.type];
  const currentDest = settings.notifyDestination || 'dm';

  text += `🔔 <b>Уведомления</b>\n\n`;
  text += `Текущий режим: <b>${NOTIFY_OPTIONS[currentDest]}</b>\n\n`;

  // Показываем доступные опции в зависимости от подписки
  if (licenseInfo.notifyOptions.length === 1) {
    text += `ℹ️ <i>С подпиской ${licenseInfo.name} доступны только личные сообщения.</i>\n\n`;
    text += `💡 <i>Обновите подписку для получения уведомлений в групповой чат.</i>`;
  } else {
    text += `Выберите куда отправлять уведомления:`;
  }

  // Создаём кнопки только для доступных опций
  const buttons = [];
  for (const option of licenseInfo.notifyOptions) {
    const isSelected = currentDest === option;
    const label = (isSelected ? '✓ ' : '') + NOTIFY_OPTIONS[option];
    buttons.push([{ text: label, callback_data: `set_notify_${option}` }]);
  }
  
  // Напоминания об истечении
  const reminderStatus = settings.expiryReminders !== false ? '✅' : '❌';
  buttons.push([{ text: `${reminderStatus} Напоминания об истечении`, callback_data: 'toggle_reminders' }]);
  
  buttons.push([{ text: '← Назад', callback_data: 'menu' }]);

  await client.editMessage(chatId, messageId, text, { reply_markup: { inline_keyboard: buttons } });
}

// ==================== USER DEVICES ====================
async function showUserDevices(env, chatId, telegramId, messageId) {
  const client = getTelegramClient(env);
  
  try {
    const license = await getLicense(env, telegramId);
    const devices = await getDevices(env, telegramId);

    let text = `📱 <b>Управление устройствами</b>\n`;
    text += `━━━━━━━━━━━━━━━━━━━━\n\n`;

    const maxDevices = license ? (LICENSE_TYPES[license.type]?.maxDevices || 1) : 0;
    
    if (!license || license.expiresAt <= Date.now()) {
      text += `❌ Для управления устройствами нужна активная подписка.\n\n`;
      text += `💡 <i>Обратитесь к @YaMob для получения подписки.</i>`;
      
      const keyboard = {
        inline_keyboard: [
          [{ text: '← Назад', callback_data: 'menu' }]
        ]
      };
      await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
      return;
    }

    text += `📊 <b>Использовано:</b> ${devices.devices.length}/${maxDevices}\n\n`;

    if (devices.devices.length === 0) {
      text += `У вас нет подключённых устройств.\n\n`;
      text += `<b>Как подключить:</b>\n`;
      text += `1. Откройте расширение в браузере\n`;
      text += `2. Перейдите в настройки\n`;
      text += `3. Нажмите "Получить код"\n`;
      text += `4. Отправьте код этому боту`;
    } else {
      text += `<b>Ваши устройства:</b>\n\n`;
      
      for (let i = 0; i < devices.devices.length; i++) {
        const device = devices.devices[i];
        const lastSeen = device.lastSeen ? formatTimeAgo(device.lastSeen) : 'никогда';
        text += `${i + 1}. <b>${escapeHtml(device.name || 'Без имени')}</b>\n`;
        text += `   └ Активность: ${lastSeen}\n\n`;
      }
    }

    // Создаём кнопки для удаления устройств
    const buttons = [];
    
    if (devices.devices.length > 0) {
      for (let i = 0; i < devices.devices.length; i++) {
        const device = devices.devices[i];
        const deviceName = device.name || `Устройство ${i + 1}`;
        const shortName = deviceName.length > 18 ? deviceName.substring(0, 18) + '...' : deviceName;
        buttons.push([{ 
          text: `🗑️ Удалить: ${shortName}`, 
          callback_data: `del_dev_${i}` 
        }]);
      }
    }
    
    buttons.push([{ text: '← Назад', callback_data: 'menu' }]);

    await client.editMessage(chatId, messageId, text, { reply_markup: { inline_keyboard: buttons } });
  } catch (error) {
    console.error('showUserDevices error:', error);
    await client.editMessage(chatId, messageId, 
      `❌ Ошибка загрузки устройств.\n\nПопробуйте позже.`,
      { reply_markup: { inline_keyboard: [[{ text: '← Назад', callback_data: 'menu' }]] } }
    );
  }
}

// Вспомогательные функции для устройств
function formatTimeAgo(timestamp) {
  const diff = Date.now() - timestamp;
  const minutes = Math.floor(diff / 60000);
  const hours = Math.floor(diff / 3600000);
  const days = Math.floor(diff / 86400000);
  
  if (minutes < 1) return 'только что';
  if (minutes < 60) return `${minutes} мин. назад`;
  if (hours < 24) return `${hours} ч. назад`;
  return `${days} дн. назад`;
}

async function showUserGroups(env, chatId, telegramId, messageId) {
  const client = getTelegramClient(env);
  const license = await getLicense(env, telegramId);
  const userGroups = await getUserGroups(env, telegramId);

  let text = `👥 <b>Групповые чаты</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (!license || !LICENSE_TYPES[license.type]?.canUseGroups) {
    text += `❌ Групповые чаты недоступны с вашей подпиской.\n\n`;
    text += `💡 <i>Обновите подписку до L2 или L3 для использования групповых чатов.</i>`;
    
    const keyboard = {
      inline_keyboard: [
        [{ text: '← Назад', callback_data: 'menu' }]
      ]
    };
    await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
    return;
  }

  if (!userGroups.chats || userGroups.chats.length === 0) {
    text += `У вас нет подключённых групповых чатов.\n\n`;
    text += `<b>Как подключить:</b>\n`;
    text += `1. Добавьте бота в групповой чат\n`;
    text += `2. Дайте боту права администратора\n`;
    text += `3. Напишите /connect в чате\n`;
    text += `4. Дождитесь одобрения администратора`;
  } else {
    text += `Ваши групповые чаты:\n\n`;
    
    for (const chat of userGroups.chats) {
      text += `📌 <b>${escapeHtml(chat.title)}</b>\n`;
      text += `   ID: <code>${chat.chatId}</code>\n\n`;
    }
  }

  const keyboard = {
    inline_keyboard: [
      [{ text: '← Назад', callback_data: 'menu' }]
    ]
  };

  await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
}

async function showUserHelp(env, chatId, messageId) {
  const client = getTelegramClient(env);

  let text = `❓ <b>Помощь</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  
  text += `<b>🤖 Что такое Exotic Operator Assistant?</b>\n`;
  text += `Это ассистент для операторов Exotic. Он автоматически отслеживает новые заявки и отправляет вам уведомления.\n\n`;
  
  text += `<b>📱 Как начать работу?</b>\n`;
  text += `1. Установите расширение для браузера\n`;
  text += `2. Откройте настройки расширения\n`;
  text += `3. Нажмите "Подключить Telegram"\n`;
  text += `4. Введите код в этот бот\n\n`;
  
  text += `<b>🔔 Уровни подписки:</b>\n`;
  text += `• <b>Promo</b> — пробный период, 1 устройство\n`;
  text += `• <b>L1</b> — базовый, 1 устройство\n`;
  text += `• <b>L2</b> — расширенный, 1 устройство + групповой чат\n`;
  text += `• <b>L3</b> — премиум, 2 устройства + все функции\n\n`;
  
  text += `<b>📋 Команды:</b>\n`;
  text += `/start — Главное меню\n`;
  text += `/stats — Статистика аккаунта\n`;
  text += `/settings — Настройки\n`;
  text += `/help — Эта справка\n\n`;
  
  text += `<b>👥 Групповые чаты (L2, L3):</b>\n`;
  text += `Вы можете получать уведомления в групповой чат.\n`;
  text += `Добавьте бота в группу и напишите /connect`;

  const keyboard = {
    inline_keyboard: [
      [{ text: '← Назад', callback_data: 'menu' }]
    ]
  };

  await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
}

async function showUserPrices(env, chatId, messageId) {
  const client = getTelegramClient(env);
  const adminUsername = env.ADMIN_USERNAME || 'YaMob';

  let text = `💰 <b>Цены и тарифы</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  
  text += `<b>🎁 Promo — Бесплатно</b>\n`;
  text += `├ Пробный период\n`;
  text += `├ 1 устройство\n`;
  text += `├ Уведомления в ЛС\n`;
  text += `└ Выдаётся администратором\n\n`;
  
  text += `<b>⭐ L1 — 10 USDT/мес</b>\n`;
  text += `├ 1 устройство\n`;
  text += `├ Уведомления в ЛС\n`;
  text += `└ Базовый функционал\n\n`;
  
  text += `<b>⭐⭐ L2 — 15 USDT/мес</b>\n`;
  text += `├ 1 устройство\n`;
  text += `├ Уведомления в ЛС или группу\n`;
  text += `└ Групповые чаты\n\n`;
  
  text += `<b>⭐⭐⭐ L3 — 20 USDT/мес</b>\n`;
  text += `├ 2 устройства\n`;
  text += `├ Уведомления в ЛС + группу\n`;
  text += `├ Все функции\n`;
  text += `└ Приоритетная поддержка\n\n`;
  
  text += `━━━━━━━━━━━━━━━━━━━━\n`;
  text += `💳 <b>Способы оплаты:</b> USDT (TRC20)\n\n`;
  text += `📨 Для покупки напишите @${adminUsername}`;

  const keyboard = {
    inline_keyboard: [
      [{ text: '📨 Купить подписку', url: `https://t.me/${adminUsername}` }],
      [{ text: '← Назад', callback_data: 'menu' }]
    ]
  };

  await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
}

async function showUserSupport(env, chatId, messageId) {
  const client = getTelegramClient(env);
  const adminUsername = env.ADMIN_USERNAME || 'YaMob';

  let text = `💬 <b>Поддержка</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  
  text += `Если у вас возникли вопросы или проблемы, свяжитесь с администратором:\n\n`;
  text += `👤 <b>@${adminUsername}</b>\n\n`;
  text += `<i>Обычно отвечаем в течение 24 часов.</i>`;

  const keyboard = {
    inline_keyboard: [
      [{ text: '📨 Написать', url: `https://t.me/${adminUsername}` }],
      [{ text: '← Назад', callback_data: 'menu' }]
    ]
  };

  await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
}

// ==================== ADMIN PANEL ====================
function getAdminMainMenu() {
  return {
    inline_keyboard: [
      [{ text: '📨 Заявки', callback_data: 'admin_requests' }, { text: '👥 Пользователи', callback_data: 'admin_users' }],
      [{ text: '📊 Статистика', callback_data: 'admin_stats' }, { text: '📢 Рассылка', callback_data: 'admin_broadcast' }],
      [{ text: '💬 Общие чаты', callback_data: 'admin_chats' }, { text: '📋 Команды', callback_data: 'admin_commands' }],
      [{ text: '🔄 Обновить', callback_data: 'admin_refresh' }]
    ]
  };
}

async function showAdminPanel(env, chatId, messageId = null) {
  const client = getTelegramClient(env);
  
  // Собираем статистику
  const users = await getAllUsers(env);
  const pendingGroups = await getPendingGroups(env);
  const approvedGroups = await getApprovedGroups(env);
  
  const now = Date.now();
  const stats = {
    total: users.length,
    active: users.filter(u => u.license?.expiresAt > now).length,
    promo: users.filter(u => u.license?.type === 'promo' && u.license?.expiresAt > now).length,
    L1: users.filter(u => u.license?.type === 'L1' && u.license?.expiresAt > now).length,
    L2: users.filter(u => u.license?.type === 'L2' && u.license?.expiresAt > now).length,
    L3: users.filter(u => u.license?.type === 'L3' && u.license?.expiresAt > now).length
  };

  let text = `🔐 <b>АДМИН-ПАНЕЛЬ</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  
  text += `📊 <b>Статистика:</b>\n`;
  text += `├ 👥 Всего: ${stats.total}\n`;
  text += `├ ✅ Активных: ${stats.active}\n`;
  text += `├ 🎁 Promo: ${stats.promo}\n`;
  text += `├ ⭐ L1: ${stats.L1}\n`;
  text += `├ ⭐⭐ L2: ${stats.L2}\n`;
  text += `└ ⭐⭐⭐ L3: ${stats.L3}\n\n`;
  
  text += `💬 <b>Чаты:</b>\n`;
  text += `├ ⏳ Заявки: ${pendingGroups.length}\n`;
  text += `└ ✅ Одобрено: ${approvedGroups.length}`;

  if (messageId) {
    await client.editMessage(chatId, messageId, text, { reply_markup: getAdminMainMenu() });
  } else {
    await client.sendMessage(chatId, text, { reply_markup: getAdminMainMenu() });
  }
}

async function showAdminRequests(env, chatId, messageId) {
  const client = getTelegramClient(env);
  const pendingGroups = await getPendingGroups(env);

  let text = `📨 <b>Заявки на чаты</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (pendingGroups.length === 0) {
    text += `Нет ожидающих заявок.`;
    
    const keyboard = {
      inline_keyboard: [
        [{ text: '← Назад', callback_data: 'admin_panel' }]
      ]
    };
    await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
    return;
  }

  const buttons = [];
  
  for (const group of pendingGroups.slice(0, 10)) {
    text += `📌 <b>${escapeHtml(group.title)}</b>\n`;
    text += `   ID: <code>${group.chatId}</code>\n`;
    text += `   От: @${escapeHtml(group.requestedBy?.username || 'unknown')}\n\n`;
    
    buttons.push([
      { text: `✅ ${group.title.slice(0, 15)}`, callback_data: `approve_${group.chatId}` },
      { text: `❌`, callback_data: `reject_${group.chatId}` }
    ]);
  }

  if (pendingGroups.length > 10) {
    text += `\n<i>...и ещё ${pendingGroups.length - 10} заявок</i>`;
  }

  buttons.push([{ text: '← Назад', callback_data: 'admin_panel' }]);

  await client.editMessage(chatId, messageId, text, { reply_markup: { inline_keyboard: buttons } });
}

async function showAdminUsers(env, chatId, messageId, page = 0) {
  const client = getTelegramClient(env);
  const users = await getAllUsers(env);
  
  const PAGE_SIZE = 10;
  const totalPages = Math.ceil(users.length / PAGE_SIZE);
  const startIdx = page * PAGE_SIZE;
  const pageUsers = users.slice(startIdx, startIdx + PAGE_SIZE);

  let text = `👥 <b>Пользователи</b> (${page + 1}/${totalPages || 1})\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (users.length === 0) {
    text += `Пользователей пока нет.`;
  } else {
    const now = Date.now();
    for (const user of pageUsers) {
      const info = user.info || {};
      const license = user.license;
      const isActive = license?.expiresAt > now;
      const licenseInfo = LICENSE_TYPES[license?.type];
      
      text += `${isActive ? '🟢' : '⚪'} `;
      
      // Отображаем username или имя, ID всегда в скобках
      if (info.username) {
        text += `@${escapeHtml(info.username)}`;
      } else if (info.firstName) {
        text += `${escapeHtml(info.firstName)}`;
      } else {
        text += `<code>${user.telegramId}</code>`;
      }
      
      text += ` — `;
      text += isActive ? `${licenseInfo?.emoji || ''} ${license.type}` : 'Нет подписки';
      text += `\n`;
    }
  }

  const buttons = [];
  const navRow = [];
  
  if (page > 0) {
    navRow.push({ text: '◀️', callback_data: `admin_users_${page - 1}` });
  }
  if (page < totalPages - 1) {
    navRow.push({ text: '▶️', callback_data: `admin_users_${page + 1}` });
  }
  
  if (navRow.length > 0) buttons.push(navRow);
  buttons.push([{ text: '🔍 Поиск', callback_data: 'admin_search_user' }]);
  buttons.push([{ text: '← Назад', callback_data: 'admin_panel' }]);

  await client.editMessage(chatId, messageId, text, { reply_markup: { inline_keyboard: buttons } });
}

async function showAdminStats(env, chatId, messageId) {
  const client = getTelegramClient(env);
  const users = await getAllUsers(env);
  const approvedGroups = await getApprovedGroups(env);
  
  const now = Date.now();
  const dayAgo = now - 24 * 60 * 60 * 1000;
  const weekAgo = now - 7 * 24 * 60 * 60 * 1000;

  let text = `📊 <b>Подробная статистика</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  
  text += `<b>👥 Пользователи:</b>\n`;
  text += `├ Всего: ${users.length}\n`;
  text += `├ Активных: ${users.filter(u => u.license?.expiresAt > now).length}\n`;
  text += `├ Новых за 24ч: ${users.filter(u => u.info?.createdAt > dayAgo).length}\n`;
  text += `└ Новых за неделю: ${users.filter(u => u.info?.createdAt > weekAgo).length}\n\n`;
  
  text += `<b>📋 По подпискам:</b>\n`;
  text += `├ 🎁 Promo: ${users.filter(u => u.license?.type === 'promo' && u.license?.expiresAt > now).length}\n`;
  text += `├ ⭐ L1: ${users.filter(u => u.license?.type === 'L1' && u.license?.expiresAt > now).length}\n`;
  text += `├ ⭐⭐ L2: ${users.filter(u => u.license?.type === 'L2' && u.license?.expiresAt > now).length}\n`;
  text += `└ ⭐⭐⭐ L3: ${users.filter(u => u.license?.type === 'L3' && u.license?.expiresAt > now).length}\n\n`;
  
  text += `<b>💬 Групповые чаты:</b>\n`;
  text += `└ Одобрено: ${approvedGroups.length}`;

  const keyboard = {
    inline_keyboard: [
      [{ text: '🔄 Обновить', callback_data: 'admin_stats' }],
      [{ text: '← Назад', callback_data: 'admin_panel' }]
    ]
  };

  await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
}

async function showAdminBroadcast(env, chatId, messageId) {
  const client = getTelegramClient(env);

  let text = `📢 <b>Рассылка</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  
  text += `Выберите аудиторию для рассылки:\n\n`;
  text += `После выбора отправьте сообщение (текст или фото с подписью).`;

  const keyboard = {
    inline_keyboard: [
      [{ text: '👥 Всем', callback_data: 'broadcast_all' }],
      [{ text: '🎁 Promo', callback_data: 'broadcast_promo' }, { text: '⭐ L1', callback_data: 'broadcast_L1' }],
      [{ text: '⭐⭐ L2', callback_data: 'broadcast_L2' }, { text: '⭐⭐⭐ L3', callback_data: 'broadcast_L3' }],
      [{ text: '❌ Без подписки', callback_data: 'broadcast_none' }],
      [{ text: '← Назад', callback_data: 'admin_panel' }]
    ]
  };

  await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
}

async function showAdminChats(env, chatId, messageId) {
  const client = getTelegramClient(env);
  const approvedGroups = await getApprovedGroups(env);

  let text = `💬 <b>Одобренные чаты</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (approvedGroups.length === 0) {
    text += `Нет одобренных чатов.`;
  } else {
    for (const group of approvedGroups) {
      text += `📌 <b>${escapeHtml(group.title)}</b>\n`;
      text += `   ID: <code>${group.chatId}</code>\n`;
      text += `   Владелец: @${escapeHtml(group.owner?.username || 'unknown')}\n\n`;
    }
  }

  const keyboard = {
    inline_keyboard: [
      [{ text: '← Назад', callback_data: 'admin_panel' }]
    ]
  };

  await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
}

async function showAdminCommands(env, chatId, messageId) {
  const client = getTelegramClient(env);

  let text = `📋 <b>Команды администратора</b>\n`;
  text += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  
  text += `<b>🎁 Управление подписками:</b>\n`;
  text += `<code>/grant @user L1 30</code> — выдать L1 на 30 дней\n`;
  text += `<code>/grant @user L2 30</code> — выдать L2 на 30 дней\n`;
  text += `<code>/grant @user L3 30</code> — выдать L3 на 30 дней\n`;
  text += `<code>/promo @user 7</code> — выдать Promo на 7 дней\n`;
  text += `<code>/revoke @user</code> — забрать подписку\n`;
  text += `<code>/revokeall chat_id</code> — снять подписки у всех в чате\n`;
  text += `<code>/resetall</code> — ⚠️ снять ВСЕ подписки у ВСЕХ\n`;
  text += `<code>/extend @user 14</code> — продлить на 14 дней\n\n`;
  
  text += `<b>👥 Управление пользователями:</b>\n`;
  text += `<code>/user @user</code> — информация (работает с @username и ID)\n`;
  text += `<code>/ban @user</code> — заблокировать\n`;
  text += `<code>/unban @user</code> — разблокировать\n\n`;
  
  text += `<b>💬 Управление чатами:</b>\n`;
  text += `<code>/approve 123456</code> — одобрить чат\n`;
  text += `<code>/reject 123456</code> — отклонить заявку\n`;
  text += `<code>/removechat 123456</code> — удалить чат\n\n`;
  
  text += `<b>⚙️ Система:</b>\n`;
  text += `<code>/stats</code> — быстрая статистика\n`;
  text += `<code>/broadcast</code> — рассылка\n`;
  text += `<code>/version</code> — версия бота\n`;
  text += `<code>/health</code> — состояние системы`;

  const keyboard = {
    inline_keyboard: [
      [{ text: '← Назад', callback_data: 'admin_panel' }]
    ]
  };

  await client.editMessage(chatId, messageId, text, { reply_markup: keyboard });
}

// ==================== ADMIN COMMANDS ====================
async function handleAdminCommand(env, chatId, telegramId, text, message) {
  const client = getTelegramClient(env);
  const parts = text.split(/\s+/);
  const command = parts[0].toLowerCase().replace('/', '');

  // Проверяем что это админ
  if (!isAdmin(env, telegramId)) {
    return;
  }

  switch (command) {
    case 'admin': {
      await showAdminPanel(env, chatId);
      break;
    }

    case 'grant': {
      // /grant @username L1 30
      const target = parts[1];
      const licenseType = parts[2]?.toUpperCase();
      const days = parseInt(parts[3]) || 30;

      if (!target || !licenseType) {
        await client.sendMessage(chatId, '❌ Использование: <code>/grant @username L1 30</code>');
        return;
      }

      const username = target.replace('@', '');
      const userInfo = await findUserByUsername(env, username);
      
      if (!userInfo) {
        await client.sendMessage(chatId, `❌ Пользователь @${username} не найден`);
        return;
      }

      if (!LICENSE_TYPES[licenseType] && licenseType !== 'PROMO') {
        await client.sendMessage(chatId, `❌ Неверный тип подписки. Доступные: L1, L2, L3, promo`);
        return;
      }

      const type = licenseType === 'PROMO' ? 'promo' : licenseType;
      const expiresAt = Date.now() + days * 24 * 60 * 60 * 1000;
      
      await saveLicense(env, userInfo.telegramId, {
        type,
        expiresAt,
        grantedBy: telegramId,
        grantedAt: Date.now()
      });

      await client.sendMessage(chatId, 
        `✅ Подписка выдана!\n\n` +
        `👤 @${username}\n` +
        `📋 Тип: ${LICENSE_TYPES[type]?.emoji || '🎁'} ${type}\n` +
        `📅 Срок: ${days} дней`
      );

      // Уведомляем пользователя
      await client.sendMessage(userInfo.telegramId,
        `🎉 <b>Вам выдана подписка!</b>\n\n` +
        `📋 Тип: ${LICENSE_TYPES[type]?.emoji || '🎁'} ${LICENSE_TYPES[type]?.name || type}\n` +
        `📅 Срок: ${days} дней\n\n` +
        `Спасибо за использование Exotic Operator Assistant!`
      );

      await logAction(env, 'license_grant', {
        telegramId: userInfo.telegramId,
        username,
        license: type,
        days
      });
      break;
    }

    case 'promo': {
      // /promo @username 7
      const target = parts[1];
      const days = parseInt(parts[2]) || 7;

      if (!target) {
        await client.sendMessage(chatId, '❌ Использование: <code>/promo @username 7</code>');
        return;
      }

      const username = target.replace('@', '');
      const userInfo = await findUserByUsername(env, username);
      
      if (!userInfo) {
        await client.sendMessage(chatId, `❌ Пользователь @${username} не найден`);
        return;
      }

      const expiresAt = Date.now() + days * 24 * 60 * 60 * 1000;
      
      await saveLicense(env, userInfo.telegramId, {
        type: 'promo',
        expiresAt,
        grantedBy: telegramId,
        grantedAt: Date.now()
      });

      await client.sendMessage(chatId, 
        `✅ Promo выдан!\n\n👤 @${username}\n📅 Срок: ${days} дней`
      );

      await client.sendMessage(userInfo.telegramId,
        `🎁 <b>Вам выдан промо-период!</b>\n\n` +
        `📅 Срок: ${days} дней\n\n` +
        `Попробуйте Exotic Operator Assistant!`
      );

      await logAction(env, 'license_grant', {
        telegramId: userInfo.telegramId,
        username,
        license: 'promo',
        days
      });
      break;
    }

    case 'revoke': {
      // /revoke @username
      const target = parts[1];

      if (!target) {
        await client.sendMessage(chatId, '❌ Использование: <code>/revoke @username</code>');
        return;
      }

      const username = target.replace('@', '');
      const userInfo = await findUserByUsername(env, username);
      
      if (!userInfo) {
        await client.sendMessage(chatId, `❌ Пользователь @${username} не найден`);
        return;
      }

      await env.USERS.delete(`license:${userInfo.telegramId}`);

      await client.sendMessage(chatId, `✅ Подписка у @${username} отозвана`);
      
      await client.sendMessage(userInfo.telegramId,
        `❌ <b>Ваша подписка была отозвана</b>\n\n` +
        `Для получения новой подписки свяжитесь с администратором.`
      );

      await logAction(env, 'license_revoke', {
        telegramId: userInfo.telegramId,
        username
      });
      break;
    }

    case 'extend': {
      // /extend @username 14
      const target = parts[1];
      const days = parseInt(parts[2]) || 14;

      if (!target) {
        await client.sendMessage(chatId, '❌ Использование: <code>/extend @username 14</code>');
        return;
      }

      const username = target.replace('@', '');
      const userInfo = await findUserByUsername(env, username);
      
      if (!userInfo) {
        await client.sendMessage(chatId, `❌ Пользователь @${username} не найден`);
        return;
      }

      const license = await getLicense(env, userInfo.telegramId);
      if (!license) {
        await client.sendMessage(chatId, `❌ У @${username} нет подписки`);
        return;
      }

      const newExpires = Math.max(license.expiresAt, Date.now()) + days * 24 * 60 * 60 * 1000;
      license.expiresAt = newExpires;
      await saveLicense(env, userInfo.telegramId, license);

      await client.sendMessage(chatId, 
        `✅ Подписка продлена!\n\n` +
        `👤 @${username}\n` +
        `📅 Добавлено: ${days} дней\n` +
        `📆 Новая дата: ${formatDate(newExpires)}`
      );

      await client.sendMessage(userInfo.telegramId,
        `⏰ <b>Ваша подписка продлена!</b>\n\n` +
        `📅 Добавлено: ${days} дней\n` +
        `📆 Действует до: ${formatDate(newExpires)}`
      );

      await logAction(env, 'license_extend', {
        telegramId: userInfo.telegramId,
        username,
        days
      });
      break;
    }

    case 'user': {
      // /user @username
      const target = parts[1];

      if (!target) {
        await client.sendMessage(chatId, '❌ Использование: <code>/user @username</code>');
        return;
      }

      const username = target.replace('@', '');
      const userInfo = await findUserByUsername(env, username);
      
      if (!userInfo) {
        await client.sendMessage(chatId, `❌ Пользователь @${username} не найден`);
        return;
      }

      const license = await getLicense(env, userInfo.telegramId);
      const settings = await getSettings(env, userInfo.telegramId);
      const devices = await getDevices(env, userInfo.telegramId);
      const banned = await isBanned(env, userInfo.telegramId);

      let text = `👤 <b>Информация о пользователе</b>\n`;
      text += `━━━━━━━━━━━━━━━━━━━━\n\n`;
      text += `🆔 ID: <code>${userInfo.telegramId}</code>\n`;
      text += `👤 Username: @${escapeHtml(username)}\n`;
      text += `📛 Имя: ${escapeHtml(userInfo.firstName || 'Не указано')}\n`;
      text += `🚫 Бан: ${banned ? 'Да' : 'Нет'}\n\n`;
      
      if (license && license.expiresAt > Date.now()) {
        const licenseInfo = LICENSE_TYPES[license.type];
        text += `📋 Подписка: ${licenseInfo?.emoji || ''} ${license.type}\n`;
        text += `⏳ Истекает: ${formatDaysLeft(license.expiresAt)}\n`;
        text += `📅 Дата: ${formatDate(license.expiresAt)}\n\n`;
      } else {
        text += `📋 Подписка: Нет\n\n`;
      }

      text += `📱 Устройств: ${devices.devices?.length || 0}\n`;
      text += `🔔 Уведомления: ${NOTIFY_OPTIONS[settings.notifyDestination] || 'ЛС'}`;

      await client.sendMessage(chatId, text);
      break;
    }

    case 'ban': {
      // /ban @username
      const target = parts[1];

      if (!target) {
        await client.sendMessage(chatId, '❌ Использование: <code>/ban @username</code>');
        return;
      }

      const username = target.replace('@', '');
      const userInfo = await findUserByUsername(env, username);
      
      if (!userInfo) {
        await client.sendMessage(chatId, `❌ Пользователь @${username} не найден`);
        return;
      }

      await setBanned(env, userInfo.telegramId, true);
      await client.sendMessage(chatId, `🚫 @${username} заблокирован`);
      
      await logAction(env, 'admin_action', {
        telegramId: userInfo.telegramId,
        username,
        message: 'Пользователь заблокирован'
      });
      break;
    }

    case 'unban': {
      // /unban @username
      const target = parts[1];

      if (!target) {
        await client.sendMessage(chatId, '❌ Использование: <code>/unban @username</code>');
        return;
      }

      const username = target.replace('@', '');
      const userInfo = await findUserByUsername(env, username);
      
      if (!userInfo) {
        await client.sendMessage(chatId, `❌ Пользователь @${username} не найден`);
        return;
      }

      await setBanned(env, userInfo.telegramId, false);
      await client.sendMessage(chatId, `✅ @${username} разблокирован`);
      
      await logAction(env, 'admin_action', {
        telegramId: userInfo.telegramId,
        username,
        message: 'Пользователь разблокирован'
      });
      break;
    }

    case 'approve': {
      // /approve 123456789
      const chatIdToApprove = parts[1];

      if (!chatIdToApprove) {
        await client.sendMessage(chatId, '❌ Использование: <code>/approve chat_id</code>');
        return;
      }

      const pendingGroups = await getPendingGroups(env);
      const groupIndex = pendingGroups.findIndex(g => String(g.chatId) === chatIdToApprove);
      
      if (groupIndex === -1) {
        await client.sendMessage(chatId, `❌ Заявка на чат ${chatIdToApprove} не найдена`);
        return;
      }

      const group = pendingGroups[groupIndex];
      pendingGroups.splice(groupIndex, 1);
      await savePendingGroups(env, pendingGroups);

      const approvedGroups = await getApprovedGroups(env);
      approvedGroups.push({
        ...group,
        approvedAt: Date.now(),
        approvedBy: telegramId
      });
      await saveApprovedGroups(env, approvedGroups);

      // Добавляем чат пользователю
      const userGroups = await getUserGroups(env, group.requestedBy.telegramId);
      userGroups.chats = userGroups.chats || [];
      userGroups.chats.push({
        chatId: group.chatId,
        title: group.title,
        addedAt: Date.now()
      });
      await saveUserGroups(env, group.requestedBy.telegramId, userGroups);

      await client.sendMessage(chatId, `✅ Чат "${group.title}" одобрен`);
      
      await client.sendMessage(group.chatId, 
        `✅ <b>Чат одобрен!</b>\n\n` +
        `Теперь вы будете получать уведомления в этот чат.`
      );

      await logAction(env, 'group_approve', {
        chatId: group.chatId,
        chatTitle: group.title,
        telegramId: group.requestedBy.telegramId,
        username: group.requestedBy.username
      });
      break;
    }

    case 'reject': {
      // /reject 123456789
      const chatIdToReject = parts[1];

      if (!chatIdToReject) {
        await client.sendMessage(chatId, '❌ Использование: <code>/reject chat_id</code>');
        return;
      }

      const pendingGroups = await getPendingGroups(env);
      const groupIndex = pendingGroups.findIndex(g => String(g.chatId) === chatIdToReject);
      
      if (groupIndex === -1) {
        await client.sendMessage(chatId, `❌ Заявка на чат ${chatIdToReject} не найдена`);
        return;
      }

      const group = pendingGroups[groupIndex];
      pendingGroups.splice(groupIndex, 1);
      await savePendingGroups(env, pendingGroups);

      await client.sendMessage(chatId, `❌ Заявка на чат "${group.title}" отклонена`);
      
      await client.sendMessage(group.chatId, 
        `❌ <b>Заявка отклонена</b>\n\n` +
        `Ваша заявка на подключение чата была отклонена.`
      );

      await logAction(env, 'group_reject', {
        chatId: group.chatId,
        chatTitle: group.title
      });
      break;
    }

    case 'removechat': {
      // /removechat 123456789
      const chatIdToRemove = parts[1];

      if (!chatIdToRemove) {
        await client.sendMessage(chatId, '❌ Использование: <code>/removechat chat_id</code>');
        return;
      }

      const approvedGroups = await getApprovedGroups(env);
      const groupIndex = approvedGroups.findIndex(g => String(g.chatId) === chatIdToRemove);
      
      if (groupIndex === -1) {
        await client.sendMessage(chatId, `❌ Чат ${chatIdToRemove} не найден в одобренных`);
        return;
      }

      const group = approvedGroups[groupIndex];
      approvedGroups.splice(groupIndex, 1);
      await saveApprovedGroups(env, approvedGroups);

      await client.sendMessage(chatId, `🗑 Чат "${group.title}" удалён`);

      await logAction(env, 'group_remove', {
        chatId: group.chatId,
        chatTitle: group.title
      });
      break;
    }

    case 'revokeall': {
      // /revokeall chat_id — снять подписки у всех пользователей чата
      const targetChatId = parts[1];

      if (!targetChatId) {
        await client.sendMessage(chatId, '❌ Использование: <code>/revokeall chat_id</code>');
        return;
      }

      // Находим чат в одобренных
      const approvedGroups = await getApprovedGroups(env);
      const group = approvedGroups.find(g => String(g.chatId) === targetChatId);
      
      if (!group) {
        await client.sendMessage(chatId, `❌ Чат ${targetChatId} не найден в одобренных`);
        return;
      }

      // Получаем всех пользователей у кого есть этот чат
      const allUsers = await getAllUsers(env);
      let revokedCount = 0;

      for (const user of allUsers) {
        const userGroups = await getUserGroups(env, user.telegramId);
        const hasThisChat = userGroups.chats?.some(c => String(c.chatId) === targetChatId);
        
        if (hasThisChat && user.license?.expiresAt > Date.now()) {
          await env.USERS.delete(`license:${user.telegramId}`);
          revokedCount++;
          
          // Уведомляем пользователя
          try {
            await client.sendMessage(user.telegramId,
              `❌ <b>Ваша подписка была отозвана</b>\n\n` +
              `Для получения новой подписки свяжитесь с администратором.`
            );
          } catch (e) {
            // Игнорируем ошибки отправки
          }
        }
      }

      await client.sendMessage(chatId, 
        `✅ Подписки отозваны!\n\n` +
        `💬 Чат: ${escapeHtml(group.title)}\n` +
        `👥 Отозвано: ${revokedCount} подписок`
      );

      await logAction(env, 'admin_action', {
        chatId: targetChatId,
        chatTitle: group.title,
        message: `Отозвано ${revokedCount} подписок в чате`
      });
      break;
    }

    case 'resetall': {
      // /resetall — снять ВСЕ подписки у ВСЕХ пользователей
      // Требуем подтверждение
      const confirm = parts[1];
      
      if (confirm !== 'CONFIRM') {
        await client.sendMessage(chatId, 
          `⚠️ <b>ВНИМАНИЕ!</b>\n\n` +
          `Эта команда снимет подписки у ВСЕХ пользователей проекта!\n\n` +
          `Для подтверждения отправьте:\n` +
          `<code>/resetall CONFIRM</code>`
        );
        return;
      }

      const allUsers = await getAllUsers(env);
      const now = Date.now();
      let revokedCount = 0;

      await client.sendMessage(chatId, `⏳ Начинаю снятие подписок...`);

      for (const user of allUsers) {
        if (user.license?.expiresAt > now) {
          await env.USERS.delete(`license:${user.telegramId}`);
          revokedCount++;
          
          // Уведомляем пользователя
          try {
            await client.sendMessage(user.telegramId,
              `❌ <b>Ваша подписка была отозвана</b>\n\n` +
              `Для получения новой подписки свяжитесь с администратором.`
            );
          } catch (e) {
            // Игнорируем ошибки отправки
          }
          
          // Небольшая задержка чтобы не превысить лимиты
          if (revokedCount % 10 === 0) {
            await new Promise(r => setTimeout(r, 100));
          }
        }
      }

      await client.sendMessage(chatId, 
        `✅ <b>Все подписки отозваны!</b>\n\n` +
        `👥 Всего пользователей: ${allUsers.length}\n` +
        `❌ Отозвано подписок: ${revokedCount}`
      );

      await logAction(env, 'admin_action', {
        message: `RESETALL: Отозвано ${revokedCount} подписок у всех пользователей`
      });
      break;
    }

    case 'stats': {
      const users = await getAllUsers(env);
      const now = Date.now();
      
      let text = `📊 <b>Быстрая статистика</b>\n`;
      text += `🕐 ${getMoscowTime()} MSK\n\n`;
      text += `👥 Всего: ${users.length}\n`;
      text += `✅ Активных: ${users.filter(u => u.license?.expiresAt > now).length}\n`;
      text += `🎁 Promo: ${users.filter(u => u.license?.type === 'promo' && u.license?.expiresAt > now).length}\n`;
      text += `⭐ L1: ${users.filter(u => u.license?.type === 'L1' && u.license?.expiresAt > now).length}\n`;
      text += `⭐⭐ L2: ${users.filter(u => u.license?.type === 'L2' && u.license?.expiresAt > now).length}\n`;
      text += `⭐⭐⭐ L3: ${users.filter(u => u.license?.type === 'L3' && u.license?.expiresAt > now).length}`;

      await client.sendMessage(chatId, text);
      break;
    }

    case 'version': {
      await client.sendMessage(chatId, `🤖 Exotic Operator Assistant\n🕐 ${getMoscowTime()} MSK`);
      break;
    }

    case 'health': {
      const webhookInfo = await client.getWebhookInfo();
      
      let text = `⚙️ <b>Состояние системы</b>\n`;
      text += `🕐 ${getMoscowTime()} MSK\n\n`;
      text += `🔗 Webhook: ${webhookInfo.result?.url ? '✅' : '❌'}\n`;
      text += `⏳ Pending: ${webhookInfo.result?.pending_update_count || 0}\n`;
      text += `❌ Last error: ${webhookInfo.result?.last_error_message || 'Нет'}`;

      await client.sendMessage(chatId, text);
      break;
    }
  }
}

async function findUserByUsernameOrId(env, query) {
  const cleanQuery = query.replace('@', '').trim();
  
  // Если это числовой ID
  if (/^\d+$/.test(cleanQuery)) {
    const data = await env.USERS.get(`user_info:${cleanQuery}`);
    if (data) {
      return {
        telegramId: cleanQuery,
        ...JSON.parse(data)
      };
    }
    // Проверяем есть ли лицензия для этого ID
    const license = await env.USERS.get(`license:${cleanQuery}`);
    if (license) {
      return { telegramId: cleanQuery };
    }
    return null;
  }
  
  // Ищем по username
  const list = await env.USERS.list({ prefix: 'user_info:' });
  
  for (const key of list.keys) {
    const data = await env.USERS.get(key.name);
    if (data) {
      const info = JSON.parse(data);
      if (info.username?.toLowerCase() === cleanQuery.toLowerCase()) {
        return {
          telegramId: key.name.replace('user_info:', ''),
          ...info
        };
      }
    }
  }
  
  return null;
}

// Для совместимости со старым кодом
async function findUserByUsername(env, username) {
  return findUserByUsernameOrId(env, username);
}

// ==================== WEBHOOK HANDLER ====================
async function handleTelegramWebhook(request, env) {
  try {
    const update = await request.json();
    const client = getTelegramClient(env);

    // Callback query (кнопки)
    if (update.callback_query) {
      const callback = update.callback_query;
      const chatId = callback.message.chat.id;
      const messageId = callback.message.message_id;
      const telegramId = String(callback.from.id);
      const data = callback.data;

      // Rate limiting
      const rateCheck = checkRateLimit(telegramId);
      if (!rateCheck.allowed) {
        await client.answerCallback(callback.id, `⏳ Подождите ${rateCheck.resetIn} сек.`, true);
        return jsonResponse({ ok: true });
      }

      // Проверка на бан
      if (await isBanned(env, telegramId)) {
        await client.answerCallback(callback.id, '🚫 Вы заблокированы', true);
        return jsonResponse({ ok: true });
      }

      await client.answerCallback(callback.id);

      // Админ callbacks
      if (data.startsWith('admin_') || data.startsWith('approve_') || data.startsWith('reject_') || data.startsWith('broadcast_')) {
        if (!isAdmin(env, telegramId)) {
          return jsonResponse({ ok: true });
        }

        switch (data) {
          case 'admin_panel':
            await showAdminPanel(env, chatId, messageId);
            break;
          case 'admin_requests':
            await showAdminRequests(env, chatId, messageId);
            break;
          case 'admin_users':
            await showAdminUsers(env, chatId, messageId);
            break;
          case 'admin_stats':
            await showAdminStats(env, chatId, messageId);
            break;
          case 'admin_broadcast':
            await showAdminBroadcast(env, chatId, messageId);
            break;
          case 'admin_chats':
            await showAdminChats(env, chatId, messageId);
            break;
          case 'admin_commands':
            await showAdminCommands(env, chatId, messageId);
            break;
          case 'admin_refresh':
            await showAdminPanel(env, chatId, messageId);
            break;
          case 'admin_search_user':
            await client.sendMessage(chatId, '🔍 Отправьте @username или ID для поиска:');
            await env.USERS.put(`state:${telegramId}`, 'search_user', { expirationTtl: 300 });
            break;
          default:
            // Пагинация пользователей
            if (data.startsWith('admin_users_')) {
              const page = parseInt(data.replace('admin_users_', ''));
              await showAdminUsers(env, chatId, messageId, page);
            }
            // Одобрение чата
            else if (data.startsWith('approve_')) {
              const groupChatId = data.replace('approve_', '');
              await handleAdminCommand(env, chatId, telegramId, `/approve ${groupChatId}`, null);
              await showAdminRequests(env, chatId, messageId);
            }
            // Отклонение чата
            else if (data.startsWith('reject_')) {
              const groupChatId = data.replace('reject_', '');
              await handleAdminCommand(env, chatId, telegramId, `/reject ${groupChatId}`, null);
              await showAdminRequests(env, chatId, messageId);
            }
            // Рассылка
            else if (data.startsWith('broadcast_')) {
              const target = data.replace('broadcast_', '');
              await client.sendMessage(chatId, 
                `📢 Отправьте сообщение для рассылки (${target === 'all' ? 'всем' : target}):\n\n` +
                `<i>Можно отправить текст или фото с подписью</i>`
              );
              await env.USERS.put(`broadcast:${telegramId}`, target, { expirationTtl: 300 });
            }
        }
        return jsonResponse({ ok: true });
      }

      // User callbacks
      switch (data) {
        case 'menu':
          await showUserMenu(env, chatId, messageId);
          break;
        case 'user_stats':
          await showUserStats(env, chatId, telegramId, messageId);
          break;
        case 'user_devices':
          await showUserDevices(env, chatId, telegramId, messageId);
          break;
        case 'user_settings':
          await showUserSettings(env, chatId, telegramId, messageId);
          break;
        case 'user_groups':
          await showUserGroups(env, chatId, telegramId, messageId);
          break;
        case 'user_prices':
          await showUserPrices(env, chatId, messageId);
          break;
        case 'user_help':
          await showUserHelp(env, chatId, messageId);
          break;
        case 'user_support':
          await showUserSupport(env, chatId, messageId);
          break;
        case 'toggle_reminders':
          const settings = await getSettings(env, telegramId);
          settings.expiryReminders = !settings.expiryReminders;
          await saveSettings(env, telegramId, settings);
          await showUserSettings(env, chatId, telegramId, messageId);
          await logAction(env, 'settings_change', {
            telegramId,
            username: callback.from.username,
            message: `Напоминания: ${settings.expiryReminders ? 'Вкл' : 'Выкл'}`
          });
          break;
        default:
          // Удаление устройства по индексу
          if (data.startsWith('del_dev_')) {
            const deviceIndex = parseInt(data.replace('del_dev_', ''), 10);
            const devices = await getDevices(env, telegramId);
            
            if (!isNaN(deviceIndex) && deviceIndex >= 0 && deviceIndex < devices.devices.length) {
              const device = devices.devices[deviceIndex];
              await removeDevice(env, telegramId, device.id);
              // Также удаляем связь user:{oderId}
              await env.USERS.delete(`user:${device.id}`);
              
              await logAction(env, 'device_removed', {
                telegramId,
                username: callback.from.username,
                message: `Удалено устройство: ${device.name}`
              });
            }
            
            await showUserDevices(env, chatId, telegramId, messageId);
          }
          // Настройка уведомлений
          else if (data.startsWith('set_notify_')) {
            const dest = data.replace('set_notify_', '');
            const license = await getLicense(env, telegramId);
            const licenseInfo = LICENSE_TYPES[license?.type];
            
            if (licenseInfo?.notifyOptions.includes(dest)) {
              const settings = await getSettings(env, telegramId);
              settings.notifyDestination = dest;
              await saveSettings(env, telegramId, settings);
              await showUserSettings(env, chatId, telegramId, messageId);
              
              await logAction(env, 'settings_change', {
                telegramId,
                username: callback.from.username,
                message: `Уведомления: ${NOTIFY_OPTIONS[dest]}`
              });
            }
          }
      }

      return jsonResponse({ ok: true });
    }

    // Сообщения
    if (update.message) {
      const message = update.message;
      const chatId = message.chat.id;
      const telegramId = String(message.from.id);
      const text = message.text || '';
      const isGroup = isGroupChat(message.chat);

      // В группах игнорируем всё кроме /connect
      if (isGroup) {
        if (text === '/connect' || text.startsWith('/connect@')) {
          await handleGroupConnect(env, message);
        }
        return jsonResponse({ ok: true });
      }

      // Rate limiting
      const rateCheck = checkRateLimit(telegramId);
      if (!rateCheck.allowed) {
        await client.sendMessage(chatId, `⏳ Подождите ${rateCheck.resetIn} секунд.`);
        return jsonResponse({ ok: true });
      }

      // Проверка на бан
      if (await isBanned(env, telegramId)) {
        await client.sendMessage(chatId, '🚫 Вы заблокированы. Обратитесь к администратору.');
        return jsonResponse({ ok: true });
      }

      // Сохраняем информацию о пользователе
      const existingInfo = await env.USERS.get(`user_info:${telegramId}`);
      await saveUserInfo(env, telegramId, {
        ...(existingInfo ? JSON.parse(existingInfo) : { createdAt: Date.now() }),
        username: message.from.username,
        firstName: message.from.first_name,
        lastName: message.from.last_name
      });

      // Проверяем состояние (для рассылки, поиска и т.д.)
      const broadcastTarget = await env.USERS.get(`broadcast:${telegramId}`);
      if (broadcastTarget && isAdmin(env, telegramId)) {
        await env.USERS.delete(`broadcast:${telegramId}`);
        await handleBroadcast(env, message, broadcastTarget);
        return jsonResponse({ ok: true });
      }

      const searchState = await env.USERS.get(`state:${telegramId}`);
      if (searchState === 'search_user' && isAdmin(env, telegramId)) {
        await env.USERS.delete(`state:${telegramId}`);
        await handleAdminCommand(env, chatId, telegramId, `/user ${text}`, message);
        return jsonResponse({ ok: true });
      }

      // Команды
      if (text.startsWith('/')) {
        const command = text.split(' ')[0].split('@')[0].toLowerCase();

        // Админские команды
        if (isAdmin(env, telegramId)) {
          const adminCommands = ['/admin', '/grant', '/promo', '/revoke', '/revokeall', '/resetall', '/extend', '/user', '/ban', '/unban', '/approve', '/reject', '/removechat', '/stats', '/version', '/health', '/broadcast'];
          if (adminCommands.some(cmd => command === cmd)) {
            await handleAdminCommand(env, chatId, telegramId, text, message);
            return jsonResponse({ ok: true });
          }
        }

        // Пользовательские команды
        switch (command) {
          case '/start':
            await logAction(env, 'user_start', {
              telegramId,
              username: message.from.username,
              firstName: message.from.first_name
            });
            
            // Проверяем есть ли параметр (код подключения)
            const startParam = text.split(' ')[1];
            if (startParam) {
              await handleConnectCode(env, chatId, telegramId, startParam, message.from);
            } else {
              await showUserMenu(env, chatId);
            }
            break;

          case '/stats':
            const statsMsg = await client.sendMessage(chatId, 'Загрузка...');
            await showUserStats(env, chatId, telegramId, statsMsg.result.message_id);
            break;

          case '/settings':
            const settingsMsg = await client.sendMessage(chatId, 'Загрузка...');
            await showUserSettings(env, chatId, telegramId, settingsMsg.result.message_id);
            break;

          case '/help':
            const helpMsg = await client.sendMessage(chatId, 'Загрузка...');
            await showUserHelp(env, chatId, helpMsg.result.message_id);
            break;

          default:
            await showUserMenu(env, chatId);
        }

        return jsonResponse({ ok: true });
      }

      // Проверяем код подключения (8 символов)
      if (/^[A-Z0-9]{8}$/.test(text.trim())) {
        await handleConnectCode(env, chatId, telegramId, text.trim(), message.from);
        return jsonResponse({ ok: true });
      }

      // Неизвестное сообщение - показываем меню
      await showUserMenu(env, chatId);
    }

    return jsonResponse({ ok: true });

  } catch (error) {
    console.error('Webhook error:', error);
    return jsonResponse({ ok: false, error: error.message }, 500);
  }
}

// ==================== GROUP CONNECT ====================
async function handleGroupConnect(env, message) {
  const client = getTelegramClient(env);
  const chatId = message.chat.id;
  const telegramId = String(message.from.id);
  const chatTitle = message.chat.title;

  // Проверяем подписку пользователя
  const license = await getLicense(env, telegramId);
  if (!license || !LICENSE_TYPES[license.type]?.canUseGroups) {
    await client.sendMessage(chatId, 
      `❌ <b>Групповые чаты недоступны</b>\n\n` +
      `Для использования групповых чатов нужна подписка L2 или L3.`
    );
    return;
  }

  // Проверяем не подан ли уже запрос
  const pendingGroups = await getPendingGroups(env);
  if (pendingGroups.some(g => String(g.chatId) === String(chatId))) {
    await client.sendMessage(chatId, 
      `⏳ <b>Заявка уже подана</b>\n\n` +
      `Ожидайте одобрения администратора.`
    );
    return;
  }

  // Проверяем не одобрен ли уже
  const approvedGroups = await getApprovedGroups(env);
  if (approvedGroups.some(g => String(g.chatId) === String(chatId))) {
    await client.sendMessage(chatId, 
      `✅ <b>Чат уже подключён</b>\n\n` +
      `Уведомления будут приходить сюда.`
    );
    return;
  }

  // Добавляем заявку
  pendingGroups.push({
    chatId: String(chatId),
    title: chatTitle,
    requestedBy: {
      telegramId,
      username: message.from.username,
      firstName: message.from.first_name
    },
    requestedAt: Date.now()
  });
  await savePendingGroups(env, pendingGroups);

  await client.sendMessage(chatId, 
    `📨 <b>Заявка отправлена!</b>\n\n` +
    `Администратор рассмотрит вашу заявку на подключение чата.\n` +
    `Обычно это занимает до 24 часов.`
  );

  await logAction(env, 'group_request', {
    chatId,
    chatTitle,
    telegramId,
    username: message.from.username
  });
}

// ==================== CONNECT CODE ====================
async function handleConnectCode(env, chatId, telegramId, code, from) {
  const client = getTelegramClient(env);

  // Ищем код
  const codeData = await env.USERS.get(`code:${code}`);
  if (!codeData) {
    await client.sendMessage(chatId, 
      `❌ <b>Код не найден</b>\n\n` +
      `Проверьте правильность кода или получите новый в расширении.`
    );
    return;
  }

  const { oderId, deviceName, createdAt } = JSON.parse(codeData);

  // Проверяем срок действия (10 минут)
  if (Date.now() - createdAt > 10 * 60 * 1000) {
    await env.USERS.delete(`code:${code}`);
    await client.sendMessage(chatId, 
      `❌ <b>Код истёк</b>\n\n` +
      `Получите новый код в расширении.`
    );
    return;
  }

  // Проверяем лимит устройств (если уже есть лицензия)
  const license = await getLicense(env, telegramId);
  if (license && license.expiresAt > Date.now()) {
    const devices = await getDevices(env, telegramId);
    const maxDevices = LICENSE_TYPES[license.type]?.maxDevices || 1;
    const existingDevice = findDevice(devices, oderId);
    
    if (!existingDevice && devices.devices.length >= maxDevices) {
      await client.sendMessage(chatId, 
        `❌ <b>Достигнут лимит устройств</b>\n\n` +
        `У вас уже подключено ${devices.devices.length} из ${maxDevices} устройств.\n\n` +
        `Удалите одно из устройств в настройках или в расширении.`,
        { reply_markup: getUserMainMenu() }
      );
      return;
    }
  }

  // Связываем аккаунты
  await env.USERS.put(`user:${oderId}`, JSON.stringify({
    oderId,
    telegramId,
    username: from.username,
    firstName: from.first_name,
    connectedAt: Date.now()
  }));
  await env.USERS.put(`tg_to_oder:${telegramId}`, oderId);
  
  // Добавляем устройство с именем
  const finalDeviceName = await addDevice(env, telegramId, oderId, deviceName || 'Моё устройство');
  
  await env.USERS.delete(`code:${code}`);

  await client.sendMessage(chatId, 
    `✅ <b>Устройство подключено!</b>\n\n` +
    `📱 <b>Название:</b> ${finalDeviceName}\n\n` +
    `Теперь вы будете получать уведомления о новых заявках.\n\n` +
    `Используйте меню для настройки.`,
    { reply_markup: getUserMainMenu() }
  );

  await logAction(env, 'user_connect', {
    telegramId,
    username: from.username,
    firstName: from.first_name,
    message: `Подключено устройство: ${finalDeviceName}`
  });
}

// ==================== BROADCAST ====================
async function handleBroadcast(env, message, target) {
  const client = getTelegramClient(env);
  const adminChatId = message.chat.id;

  const users = await getAllUsers(env);
  const now = Date.now();

  // Фильтруем пользователей
  let recipients = users;
  if (target === 'promo') {
    recipients = users.filter(u => u.license?.type === 'promo' && u.license?.expiresAt > now);
  } else if (target === 'L1') {
    recipients = users.filter(u => u.license?.type === 'L1' && u.license?.expiresAt > now);
  } else if (target === 'L2') {
    recipients = users.filter(u => u.license?.type === 'L2' && u.license?.expiresAt > now);
  } else if (target === 'L3') {
    recipients = users.filter(u => u.license?.type === 'L3' && u.license?.expiresAt > now);
  } else if (target === 'none') {
    recipients = users.filter(u => !u.license || u.license.expiresAt <= now);
  }

  await client.sendMessage(adminChatId, `📤 Начинаю рассылку для ${recipients.length} пользователей...`);

  let sent = 0;
  let failed = 0;

  for (const user of recipients) {
    try {
      if (message.photo) {
        // Фото с подписью
        const photo = message.photo[message.photo.length - 1].file_id;
        await client.sendPhoto(user.telegramId, photo, message.caption || '');
      } else if (message.text) {
        // Текст
        await client.sendMessage(user.telegramId, message.text);
      }
      sent++;
      
      // Небольшая задержка между сообщениями
      await new Promise(r => setTimeout(r, 50));
    } catch (e) {
      failed++;
    }
  }

  await client.sendMessage(adminChatId, 
    `✅ <b>Рассылка завершена!</b>\n\n` +
    `📤 Отправлено: ${sent}\n` +
    `❌ Ошибок: ${failed}`
  );

  await logAction(env, 'broadcast', {
    message: `Рассылка для ${target}: ${sent} успешно, ${failed} ошибок`
  });
}

// ==================== EXPIRY REMINDERS ====================
async function checkAndSendExpirationReminders(env) {
  const users = await getAllUsers(env);
  const now = Date.now();
  const client = getTelegramClient(env);

  for (const user of users) {
    if (!user.license || !user.settings?.expiryReminders) continue;

    const daysLeft = Math.ceil((user.license.expiresAt - now) / (1000 * 60 * 60 * 24));

    // Проверяем нужно ли отправить напоминание
    const remindDays = [3, 1, 0];
    for (const day of remindDays) {
      if (daysLeft === day) {
        const notifKey = `expiry_notif:${user.telegramId}:${day}`;
        const alreadySent = await env.USERS.get(notifKey);
        
        if (!alreadySent) {
          const emoji = day === 0 ? '❌' : day === 1 ? '🔴' : '⚠️';
          const dayText = day === 0 ? 'сегодня' : `через ${day} ${day === 1 ? 'день' : 'дня'}`;
          
          await client.sendMessage(user.telegramId,
            `${emoji} <b>Срок подписки истекает ${dayText}!</b>\n\n` +
            `Для продления свяжитесь с администратором.`
          );

          await env.USERS.put(notifKey, '1', { expirationTtl: 86400 });
        }
        break;
      }
    }
  }
}

// ==================== API HANDLERS ====================
async function handleLicenseCheck(request, env) {
  try {
    const clientIP = getClientIP(request);
    
    // Rate limit по IP
    const ipCheck = checkRateLimit(clientIP, 'ip');
    if (!ipCheck.allowed) {
      await logAction(env, 'api_rate_limit', { ip: clientIP, message: 'IP rate limit exceeded' });
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: ipCheck.resetIn }, 429);
    }
    
    const { oderId, deviceId, deviceName } = await request.json();

    if (!oderId) {
      return jsonResponse({ valid: false, error: 'oderId required' }, 400);
    }
    
    // Rate limit по oderId
    const apiCheck = checkRateLimit(oderId, 'api');
    if (!apiCheck.allowed) {
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: apiCheck.resetIn }, 429);
    }

    const user = await getUser(env, oderId);
    if (!user) {
      return jsonResponse({ valid: false, connected: false, error: 'NOT_CONNECTED' });
    }

    const license = await getLicense(env, user.telegramId);
    if (!license || license.expiresAt <= Date.now()) {
      return jsonResponse({
        valid: false,
        connected: true,
        error: 'NO_LICENSE'
      });
    }

    // Проверка устройства
    if (deviceId) {
      const devices = await getDevices(env, user.telegramId);
      const maxDevices = LICENSE_TYPES[license.type]?.maxDevices || 1;
      const existingDevice = findDevice(devices, deviceId);

      if (!existingDevice) {
        if (devices.devices.length >= maxDevices) {
          return jsonResponse({
            valid: false,
            connected: true,
            error: 'MAX_DEVICES',
            maxDevices,
            currentDevices: devices.devices.map(d => ({ name: d.name, addedAt: d.addedAt }))
          });
        }
        // Добавляем новое устройство с дефолтным именем
        await addDevice(env, user.telegramId, deviceId, deviceName);
        
        // Логируем добавление устройства
        await logAction(env, 'api_device_add', {
          telegramId: user.telegramId,
          ip: clientIP,
          deviceId,
          deviceName,
          message: `Новое устройство: ${deviceName || 'Без имени'}`
        });
      } else {
        // Обновляем lastSeen
        await updateDeviceLastSeen(env, user.telegramId, deviceId);
      }
    }

    // Вычисляем daysLeft
    const daysLeft = Math.ceil((license.expiresAt - Date.now()) / (24 * 60 * 60 * 1000));

    return jsonResponse({
      valid: true,
      connected: true,
      type: license.type,
      typeName: LICENSE_TYPES[license.type]?.name || license.type,
      expiresAt: license.expiresAt,
      daysLeft: daysLeft,
      features: LICENSE_TYPES[license.type]
    });

  } catch (error) {
    console.error('License check error:', error);
    return jsonResponse({ error: 'SERVER_ERROR' }, 500);
  }
}

async function handleGenerateCode(request, env) {
  try {
    const clientIP = getClientIP(request);
    
    // Строгий rate limit для генерации кодов
    const ipCheck = checkRateLimit(clientIP, 'apiStrict');
    if (!ipCheck.allowed) {
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: ipCheck.resetIn }, 429);
    }
    
    const { oderId, deviceName } = await request.json();

    if (!oderId) {
      return jsonResponse({ error: 'oderId required' }, 400);
    }

    // Генерируем код
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let code = '';
    for (let i = 0; i < 8; i++) {
      code += chars.charAt(Math.floor(Math.random() * chars.length));
    }

    await env.USERS.put(`code:${code}`, JSON.stringify({
      oderId,
      deviceName: deviceName || 'Моё устройство',
      createdAt: Date.now()
    }), { expirationTtl: 600 }); // 10 минут

    return jsonResponse({ code });

  } catch (error) {
    console.error('Generate code error:', error);
    return jsonResponse({ error: 'SERVER_ERROR' }, 500);
  }
}

// ==================== STATUS CHECK ====================
async function handleStatusCheck(request, env) {
  try {
    const clientIP = getClientIP(request);
    
    // Rate limit
    const ipCheck = checkRateLimit(clientIP, 'api');
    if (!ipCheck.allowed) {
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: ipCheck.resetIn }, 429);
    }
    
    const { oderId } = await request.json();

    if (!oderId) {
      return jsonResponse({ 
        connected: false, 
        error: 'oderId required' 
      }, 400);
    }

    // Проверяем связь по oderId
    const userData = await env.USERS.get(`user:${oderId}`);
    
    if (!userData) {
      return jsonResponse({ 
        connected: false,
        message: 'Account not connected'
      });
    }

    const user = JSON.parse(userData);
    
    // Проверяем лицензию
    const license = await getLicense(env, user.telegramId);
    const hasActiveLicense = license && license.expiresAt > Date.now();
    
    // Проверяем настройки
    const settings = await getSettings(env, user.telegramId);

    return jsonResponse({
      connected: true,
      telegramId: user.telegramId,
      username: user.username || null,
      firstName: user.firstName || null,
      connectedAt: user.connectedAt,
      license: hasActiveLicense ? {
        valid: true,
        type: license.type,
        typeName: LICENSE_TYPES[license.type]?.name || license.type,
        expiresAt: license.expiresAt,
        daysLeft: Math.ceil((license.expiresAt - Date.now()) / (24 * 60 * 60 * 1000))
      } : { valid: false },
      settings: {
        notifyDestination: settings.notifyDestination || 'dm',
        soundEnabled: settings.soundEnabled !== false,
        clicks: settings.clickNotifications !== false
      },
      version: license?.updatedAt || license?.grantedAt || Date.now()
    });

  } catch (error) {
    console.error('Status check error:', error);
    return jsonResponse({ 
      connected: false, 
      error: 'SERVER_ERROR' 
    }, 500);
  }
}

// ==================== LICENSE VERSION (быстрая проверка) ====================
async function handleLicenseVersion(request, env) {
  try {
    const clientIP = getClientIP(request);
    
    // Rate limit
    const ipCheck = checkRateLimit(clientIP, 'api');
    if (!ipCheck.allowed) {
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: ipCheck.resetIn }, 429);
    }
    
    const { oderId } = await request.json();

    if (!oderId) {
      return jsonResponse({ connected: false, error: 'oderId required' }, 400);
    }

    const user = await getUser(env, oderId);
    if (!user) {
      return jsonResponse({ connected: false });
    }

    const license = await getLicense(env, user.telegramId);
    
    // Версия = timestamp последнего изменения лицензии
    // Если лицензия изменилась - версия будет другой
    const version = license?.updatedAt || license?.grantedAt || 0;

    return jsonResponse({
      connected: true,
      version,
      hasLicense: !!(license && license.expiresAt > Date.now())
    });

  } catch (error) {
    console.error('License version error:', error);
    return jsonResponse({ connected: false, error: 'SERVER_ERROR' }, 500);
  }
}

// ==================== SETTINGS SYNC ====================
async function handleSettingsSync(request, env) {
  try {
    const clientIP = getClientIP(request);
    
    // Rate limit
    const ipCheck = checkRateLimit(clientIP, 'api');
    if (!ipCheck.allowed) {
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: ipCheck.resetIn }, 429);
    }
    
    const { oderId, settings } = await request.json();

    if (!oderId) {
      return jsonResponse({ ok: false, error: 'oderId required' }, 400);
    }

    const user = await getUser(env, oderId);
    if (!user) {
      return jsonResponse({ ok: false, error: 'NOT_CONNECTED' }, 404);
    }

    // Сохраняем информацию о расширении
    const existingSettings = await getSettings(env, user.telegramId);
    
    if (settings) {
      existingSettings.extensionVersion = settings.extensionVersion;
      existingSettings.lastExtensionSync = Date.now();
      await saveSettings(env, user.telegramId, existingSettings);
    }

    return jsonResponse({ ok: true });

  } catch (error) {
    console.error('Settings sync error:', error);
    return jsonResponse({ ok: false, error: 'SERVER_ERROR' }, 500);
  }
}

// ==================== DEVICES API ====================
async function handleDevicesList(request, env) {
  try {
    const clientIP = getClientIP(request);
    
    // Rate limit
    const ipCheck = checkRateLimit(clientIP, 'api');
    if (!ipCheck.allowed) {
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: ipCheck.resetIn }, 429);
    }
    
    const { oderId } = await request.json();

    if (!oderId) {
      return jsonResponse({ error: 'oderId required' }, 400);
    }

    const user = await getUser(env, oderId);
    if (!user) {
      return jsonResponse({ error: 'NOT_CONNECTED' }, 404);
    }

    const devices = await getDevices(env, user.telegramId);
    const license = await getLicense(env, user.telegramId);
    const maxDevices = license ? (LICENSE_TYPES[license.type]?.maxDevices || 1) : 0;
    
    // Генерируем CSRF токен для критических операций
    const csrfToken = generateCsrfToken(user.telegramId);

    return jsonResponse({
      devices: devices.devices.map(d => ({
        id: d.id,
        name: d.name,
        addedAt: d.addedAt,
        lastSeen: d.lastSeen,
        isCurrent: d.id === oderId // Текущее устройство помечаем
      })),
      maxDevices,
      csrfToken, // Для удаления устройств
      currentDeviceId: oderId
    });

  } catch (error) {
    console.error('Devices list error:', error);
    return jsonResponse({ error: 'SERVER_ERROR' }, 500);
  }
}

async function handleDeviceRemove(request, env) {
  try {
    const clientIP = getClientIP(request);
    
    // Строгий rate limit для критической операции
    const ipCheck = checkRateLimit(clientIP, 'apiStrict');
    if (!ipCheck.allowed) {
      await logAction(env, 'api_rate_limit', { ip: clientIP, message: 'Device remove rate limit' });
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: ipCheck.resetIn }, 429);
    }
    
    const { oderId, deviceIdToRemove, csrfToken } = await request.json();

    if (!oderId || !deviceIdToRemove) {
      return jsonResponse({ error: 'oderId and deviceIdToRemove required' }, 400);
    }

    const user = await getUser(env, oderId);
    if (!user) {
      return jsonResponse({ error: 'NOT_CONNECTED' }, 404);
    }
    
    // Проверка CSRF токена (если передан)
    if (csrfToken && !validateCsrfToken(user.telegramId, csrfToken)) {
      await logAction(env, 'security_alert', {
        telegramId: user.telegramId,
        ip: clientIP,
        message: 'Invalid CSRF token for device removal'
      });
      return jsonResponse({ error: 'INVALID_TOKEN' }, 403);
    }
    
    // Получаем информацию об устройстве до удаления
    const devicesBefore = await getDevices(env, user.telegramId);
    const deviceToRemove = findDevice(devicesBefore, deviceIdToRemove);

    const removed = await removeDevice(env, user.telegramId, deviceIdToRemove);
    
    if (!removed) {
      return jsonResponse({ error: 'DEVICE_NOT_FOUND' }, 404);
    }
    
    // Логируем удаление устройства
    await logAction(env, 'api_device_remove', {
      telegramId: user.telegramId,
      ip: clientIP,
      deviceId: deviceIdToRemove,
      deviceName: deviceToRemove?.name,
      message: `Удалено через API: ${deviceToRemove?.name || 'Unknown'}`
    });

    // Если удалили текущее устройство, очищаем связь user:{oderId}
    if (deviceIdToRemove === oderId) {
      await env.USERS.delete(`user:${oderId}`);
    }

    const devices = await getDevices(env, user.telegramId);

    return jsonResponse({ 
      success: true,
      devices: devices.devices.map(d => ({
        id: d.id,
        name: d.name,
        addedAt: d.addedAt,
        lastSeen: d.lastSeen,
        isCurrent: d.id === oderId
      }))
    });

  } catch (error) {
    console.error('Device remove error:', error);
    return jsonResponse({ error: 'SERVER_ERROR' }, 500);
  }
}

async function handleDeviceRename(request, env) {
  try {
    const clientIP = getClientIP(request);
    
    // Rate limit
    const ipCheck = checkRateLimit(clientIP, 'api');
    if (!ipCheck.allowed) {
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: ipCheck.resetIn }, 429);
    }
    
    const { oderId, deviceIdToRename, newName } = await request.json();

    if (!oderId || !deviceIdToRename || !newName) {
      return jsonResponse({ error: 'oderId, deviceIdToRename and newName required' }, 400);
    }

    if (newName.length > 30) {
      return jsonResponse({ error: 'Name too long (max 30 chars)' }, 400);
    }
    
    // Санитизация имени
    const sanitizedName = newName.trim().replace(/[<>]/g, '');
    if (sanitizedName.length === 0) {
      return jsonResponse({ error: 'Invalid name' }, 400);
    }

    const user = await getUser(env, oderId);
    if (!user) {
      return jsonResponse({ error: 'NOT_CONNECTED' }, 404);
    }

    const devices = await getDevices(env, user.telegramId);
    const device = findDevice(devices, deviceIdToRename);
    
    if (!device) {
      return jsonResponse({ error: 'DEVICE_NOT_FOUND' }, 404);
    }

    const oldName = device.name;
    
    // Проверяем уникальность имени
    if (devices.devices.some(d => d.name === sanitizedName && d.id !== deviceIdToRename)) {
      return jsonResponse({ error: 'NAME_ALREADY_EXISTS' }, 400);
    }

    device.name = sanitizedName;
    await saveDevices(env, user.telegramId, devices);
    
    // Логируем переименование
    await logAction(env, 'api_device_rename', {
      telegramId: user.telegramId,
      ip: clientIP,
      deviceId: deviceIdToRename,
      message: `${oldName} → ${sanitizedName}`
    });

    return jsonResponse({ 
      success: true,
      device: {
        id: device.id,
        name: device.name,
        addedAt: device.addedAt,
        lastSeen: device.lastSeen
      }
    });

  } catch (error) {
    console.error('Device rename error:', error);
    return jsonResponse({ error: 'SERVER_ERROR' }, 500);
  }
}

async function handleNotify(request, env) {
  try {
    const clientIP = getClientIP(request);
    
    // Rate limit (строгий для уведомлений)
    const ipCheck = checkRateLimit(clientIP, 'apiStrict');
    if (!ipCheck.allowed) {
      return jsonResponse({ error: 'Rate limit exceeded', retryAfter: ipCheck.resetIn }, 429);
    }
    
    const { oderId, message, type } = await request.json();

    if (!oderId || !message) {
      return jsonResponse({ error: 'oderId and message required' }, 400);
    }
    
    // Ограничение длины сообщения
    if (message.length > 4000) {
      return jsonResponse({ error: 'Message too long (max 4000 chars)' }, 400);
    }

    const user = await getUser(env, oderId);
    if (!user) {
      return jsonResponse({ error: 'User not connected' }, 404);
    }

    const license = await getLicense(env, user.telegramId);
    if (!license || license.expiresAt <= Date.now()) {
      return jsonResponse({ error: 'No active license' }, 403);
    }

    const settings = await getSettings(env, user.telegramId);
    const client = getTelegramClient(env);
    const sentTo = [];

    const dest = settings.notifyDestination || 'dm';

    // Отправляем в ЛС
    if (dest === 'dm' || dest === 'both') {
      const result = await client.sendMessage(user.telegramId, message);
      if (result.ok) sentTo.push('dm');
    }

    // Отправляем в группу
    if ((dest === 'group' || dest === 'both') && LICENSE_TYPES[license.type]?.canUseGroups) {
      const userGroups = await getUserGroups(env, user.telegramId);
      for (const group of userGroups.chats || []) {
        const result = await client.sendMessage(group.chatId, message);
        if (result.ok) sentTo.push(`group:${group.chatId}`);
      }
    }

    return jsonResponse({ success: true, sentTo });

  } catch (error) {
    console.error('Notify error:', error);
    return jsonResponse({ error: 'SERVER_ERROR' }, 500);
  }
}

// ==================== CORS & HELPERS ====================
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization'
};

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...CORS_HEADERS
    }
  });
}

// ==================== MAIN HANDLER ====================
export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(checkAndSendExpirationReminders(env));
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }

    try {
      // Webhook
      if (path === '/webhook' && request.method === 'POST') {
        return await handleTelegramWebhook(request, env);
      }

      // API
      if (path === '/api/license/check' && request.method === 'POST') {
        return await handleLicenseCheck(request, env);
      }

      if (path === '/api/generate-code' && request.method === 'POST') {
        return await handleGenerateCode(request, env);
      }

      if (path === '/api/notify' && request.method === 'POST') {
        return await handleNotify(request, env);
      }

      // Проверка статуса подключения
      if (path === '/api/status' && request.method === 'POST') {
        return await handleStatusCheck(request, env);
      }
      
      // Алиас для совместимости с расширением
      if (path === '/api/sync/status' && request.method === 'POST') {
        return await handleStatusCheck(request, env);
      }
      
      // Быстрая проверка версии лицензии
      if (path === '/api/license/version' && request.method === 'POST') {
        return await handleLicenseVersion(request, env);
      }
      
      // Синхронизация настроек из расширения
      if (path === '/api/settings/sync' && request.method === 'POST') {
        return await handleSettingsSync(request, env);
      }

      // API устройств
      if (path === '/api/devices/list' && request.method === 'POST') {
        return await handleDevicesList(request, env);
      }

      if (path === '/api/devices/remove' && request.method === 'POST') {
        return await handleDeviceRemove(request, env);
      }

      if (path === '/api/devices/rename' && request.method === 'POST') {
        return await handleDeviceRename(request, env);
      }

      if (path === '/api/health') {
        return jsonResponse({
          status: 'ok',
          timestamp: Date.now()
        });
      }

      // Set webhook (admin only)
      if (path === '/api/set-webhook') {
        const authHeader = request.headers.get('Authorization');
        if (!env.ADMIN_TOKEN || authHeader !== `Bearer ${env.ADMIN_TOKEN}`) {
          return jsonResponse({ error: 'Unauthorized' }, 401);
        }

        const client = getTelegramClient(env);
        const webhookUrl = `${url.origin}/webhook`;
        const result = await client.setWebhook(webhookUrl, {
          secret_token: env.TELEGRAM_WEBHOOK_SECRET
        });

        return jsonResponse(result);
      }

      return jsonResponse({ error: 'Not found' }, 404);

    } catch (error) {
      console.error('Handler error:', error);
      return jsonResponse({ error: 'Internal server error' }, 500);
    }
  }
};
