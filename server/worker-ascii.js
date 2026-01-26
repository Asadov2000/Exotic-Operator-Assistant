// ==========  ==========

//    
const BOT_VERSION = '5.0';
const EXTENSION_VERSION = '5.0';

// ==========   ==========
const metrics = {
  requests: { total: 0, success: 0, error: 0 },
  endpoints: {},
  errors: [],
  startTime: Date.now(),
  lastReset: Date.now()
};

// ==========  ==========
const cache = new Map();
const CACHE_TTL = 60 * 1000; // 1 

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
  //   
  if (cache.size > 5000) {
    const now = Date.now();
    for (const [k, v] of cache) {
      if (v.expires < now) cache.delete(k);
    }
  }
}

// ========== RATE LIMITING   ==========
const botRateLimits = new Map();
const BOT_RATE_LIMIT = {
  messages: 20,      //     
  callbacks: 30,     // callback'    
  window: 60 * 1000  // 1 
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
      message: `   .  ${Math.ceil((requests[0] + BOT_RATE_LIMIT.window - now) / 1000)} .`
    };
  }
  
  requests.push(now);
  botRateLimits.set(key, requests);
  
  // 
  if (botRateLimits.size > 10000) {
    const entries = Array.from(botRateLimits.entries());
    entries.slice(0, 5000).forEach(([k]) => botRateLimits.delete(k));
  }
  
  return { allowed: true, remaining: limit - requests.length };
}

// ==========     ==========
const EXPIRATION_NOTIFICATIONS = {
  3: { sent: false, emoji: '', text: '3 ' },
  1: { sent: false, emoji: '', text: '1 ' },
  0: { sent: false, emoji: '', text: '' }
};

//       
async function sendExpirationNotification(env, telegramId, daysLeft, licenseType) {
  const notifKey = `expiry_notif:${telegramId}:${daysLeft}`;
  
  // ,    
  const alreadySent = await env.USERS.get(notifKey);
  if (alreadySent) return false;
  
  //   
  const userSettings = await getUserSettings(env, telegramId);
  if (!userSettings.notifications.expiryReminders) {
    return false; //  
  }
  
  const typeText = LICENSE_TYPES[licenseType]?.name || '';
  
  let message;
  if (daysLeft === 3) {
    message = ` **\n\n` +
      `  *${typeText}*   *3 *.\n\n` +
      ` ,     !`;
  } else if (daysLeft === 1) {
    message = ` *!*\n\n` +
      `  *${typeText}*  **!\n\n` +
      `  ,   .`;
  } else if (daysLeft === 0) {
    message = ` *  !*\n\n` +
      `  *${typeText}*  .\n\n` +
      ` ,      !`;
  }
  
  if (message) {
    await sendTelegramMessage(env, telegramId, message, {
      reply_markup: {
        inline_keyboard: [
          [{ text: '  ', callback_data: 'buy' }],
          [{ text: '  @YaMob', url: 'https://t.me/YaMob' }]
        ]
      }
    });
    
    //     (TTL 7 )
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
      //    100 
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

//      (Sentry-like)
function structuredLog(level, message, context = {}) {
  const logEntry = {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...context,
    env: 'production'
  };
  
  // Console  Cloudflare Workers Logs
  if (level === 'error') {
    console.error(JSON.stringify(logEntry));
  } else if (level === 'warn') {
    console.warn(JSON.stringify(logEntry));
  } else {
    console.log(JSON.stringify(logEntry));
  }
  
  return logEntry;
}

//  origins  CORS (  )
const ALLOWED_ORIGINS = [
  'chrome-extension://',
  'moz-extension://',
  'https://exotic.company',
  'https://*.exotic.company'
];

function getCorsHeaders(origin) {
  // ,   origin
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

// CORS headers (  -  getCorsHeaders()   origin)
//     origin  
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': 'https://exotic.company',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

//  
const MS_PER_MINUTE = 60 * 1000;
const MS_PER_HOUR = 60 * MS_PER_MINUTE;
const MS_PER_DAY = 24 * MS_PER_HOUR;
const CODE_EXPIRATION_TTL = 1800; // 30 

//  
const TRIAL_DURATION_NO_REFERRAL = 12 * MS_PER_HOUR; // 12     
const TRIAL_DURATION_WITH_REFERRAL = MS_PER_DAY; // 24      
const TRIAL_DURATION = TRIAL_DURATION_NO_REFERRAL; //   12 

//  
const REFERRAL_BONUS_HOURS = 24; //       
const REFERRAL_TRIAL_HOURS = 24; //     (24 )
const REFERRAL_CODE_PREFIX = 'REF_';

// ==========  - ==========
//         env.ADMIN_SECRET_PHRASE
//  : wrangler secret put ADMIN_SECRET_PHRASE
//    1 
const ADMIN_SESSION_TTL = 60 * 60; // 1   

//    ( telegramId,   username  )
async function isAdminById(env, telegramId) {
  const adminId = env.ADMIN_TELEGRAM_ID;
  return adminId && String(telegramId) === String(adminId);
}

// ==========    ==========
// Level 1/2 = 1 , Level 3 = 2 
//    deviceId (oderId)

//    
async function getUserDevices(env, telegramId) {
  const data = await env.USERS.get(`devices:${telegramId}`);
  if (!data) return { devices: [], lastUpdated: null };
  return JSON.parse(data);
}

//   
async function saveUserDevices(env, telegramId, devicesData) {
  devicesData.lastUpdated = Date.now();
  await env.USERS.put(`devices:${telegramId}`, JSON.stringify(devicesData));
}

//    
async function checkAndRegisterDevice(env, telegramId, deviceId, license) {
  if (!deviceId) {
    return { allowed: true, reason: 'NO_DEVICE_ID' }; //     
  }
  
  const licenseType = LICENSE_TYPES[license?.type] || LICENSE_TYPES.trial;
  const maxDevices = licenseType.maxDevices || 1;
  
  const devicesData = await getUserDevices(env, telegramId);
  const devices = devicesData.devices || [];
  
  // ,     
  const existingIndex = devices.findIndex(d => d.deviceId === deviceId);
  if (existingIndex >= 0) {
    //    -  lastSeen
    devices[existingIndex].lastSeen = Date.now();
    await saveUserDevices(env, telegramId, { devices });
    return { allowed: true, reason: 'DEVICE_EXISTS', deviceCount: devices.length, maxDevices };
  }
  
  //   -  
  if (devices.length >= maxDevices) {
    //  
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
  
  //   
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

//   ( )
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

//    
async function clearAllDevices(env, telegramId) {
  await saveUserDevices(env, telegramId, { devices: [] });
  return { success: true };
}

//   -
async function hasAdminSession(env, telegramId) {
  const sessionKey = `admin_session:${telegramId}`;
  const session = await env.USERS.get(sessionKey);
  return !!session;
}

//  -
async function createAdminSession(env, telegramId) {
  const sessionKey = `admin_session:${telegramId}`;
  const sessionData = {
    createdAt: Date.now(),
    telegramId: String(telegramId)
  };
  await env.USERS.put(sessionKey, JSON.stringify(sessionData), { expirationTtl: ADMIN_SESSION_TTL });
  return true;
}

//  - ()
async function destroyAdminSession(env, telegramId) {
  const sessionKey = `admin_session:${telegramId}`;
  await env.USERS.delete(sessionKey);
}

//      ( true  )
async function checkAndExtendAdminSession(env, telegramId) {
  const hasSession = await hasAdminSession(env, telegramId);
  if (hasSession) {
    //     
    await createAdminSession(env, telegramId);
    return true;
  }
  return false;
}

//     
async function showSessionExpiredMessage(env, chatId, messageId) {
  await editTelegramMessage(env, chatId, messageId,
    ` * *\n\n` +
    `   \n  .`,
    { reply_markup: { inline_keyboard: [[{ text: ' ', callback_data: 'menu' }]] }}
  );
}

//   pending 
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

//    
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
    //   (  )
    const usersList = await env.USERS.list({ prefix: 'user:', limit: 1000 });
    stats.totalUsers = usersList.keys.length;
    
    //    
    const licenseList = await env.USERS.list({ prefix: 'license:tg:', limit: 1000 });
    const now = Date.now();
    
    for (const key of licenseList.keys) {
      const data = await env.USERS.get(key.name);
      if (data) {
        const license = JSON.parse(data);
        if (license.expiresAt > now) {
          stats.activeUsers++;
          //   
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
              stats.level1Users++; //   Level 1
          }
        }
      }
    }
    
    // Pending 
    const pendingList = await env.USERS.list({ prefix: 'pending_group:' });
    stats.pendingGroups = pendingList.keys.length;
    
    //  
    const groupOwnersList = await env.USERS.list({ prefix: 'group_owner:' });
    stats.approvedGroups = groupOwnersList.keys.length;
    
  } catch (e) {
    console.error('Error getting admin stats:', e);
  }
  
  return stats;
}

//      
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

// ==========    USERNAME ==========

//  username -> telegramId (  KV)
async function saveUsernameMapping(env, username, telegramId) {
  if (!username) return;
  const cleanUsername = username.toLowerCase().replace('@', '');
  await env.USERS.put(`username:${cleanUsername}`, String(telegramId));
}

//  telegramId  username
async function findTelegramIdByUsername(env, username) {
  if (!username) return null;
  const cleanUsername = username.toLowerCase().replace('@', '');
  
  //   
  const cachedId = await env.USERS.get(`username:${cleanUsername}`);
  if (cachedId) return cachedId;
  
  //     -   user:* 
  try {
    const usersList = await env.USERS.list({ prefix: 'user:', limit: 1000 });
    for (const key of usersList.keys) {
      const data = await env.USERS.get(key.name);
      if (data) {
        const user = JSON.parse(data);
        if (user.username && user.username.toLowerCase() === cleanUsername) {
          //     
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

//     ( ID  username)
async function getUserInfo(env, target) {
  const cleanTarget = target.replace('@', '');
  const isTelegramId = /^\d+$/.test(cleanTarget);
  
  let telegramId = isTelegramId ? cleanTarget : null;
  let username = isTelegramId ? null : cleanTarget;
  
  //   username -  telegramId
  if (!isTelegramId) {
    telegramId = await findTelegramIdByUsername(env, cleanTarget);
    if (!telegramId) {
      return { found: false, error: 'USER_NOT_FOUND', username: cleanTarget };
    }
  }
  
  //  
  const licenseData = await env.USERS.get(`license:tg:${telegramId}`);
  const license = licenseData ? JSON.parse(licenseData) : null;
  
  //   (oderId)
  const oderId = await env.USERS.get(`tg:${telegramId}`);
  let userData = null;
  if (oderId) {
    const userDataRaw = await env.USERS.get(`user:${oderId}`);
    if (userDataRaw) {
      userData = JSON.parse(userDataRaw);
      username = userData.username || username;
    }
  }
  
  //  
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

// Rate limiting 
const RATE_LIMIT_WINDOW = 60 * 1000; // 1 
const RATE_LIMIT_MAX_REQUESTS = 60; //    

// Telegram IP ranges (  webhook)
const TELEGRAM_SUBNETS = [
  '149.154.160.0/20',
  '91.108.4.0/22'
];

//  IP   ( )
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

//    Markdown  Telegram
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

// Rate limiter (   KV)
const rateLimitCache = new Map();

async function checkRateLimit(env, identifier, maxRequests = RATE_LIMIT_MAX_REQUESTS) {
  const key = `ratelimit:${identifier}`;
  const now = Date.now();
  const windowStart = now - RATE_LIMIT_WINDOW;
  
  //  in-memory cache   
  let requests = rateLimitCache.get(key) || [];
  requests = requests.filter(time => time > windowStart);
  
  if (requests.length >= maxRequests) {
    return { allowed: false, remaining: 0, resetIn: Math.ceil((requests[0] + RATE_LIMIT_WINDOW - now) / 1000) };
  }
  
  requests.push(now);
  rateLimitCache.set(key, requests);
  
  //   
  if (rateLimitCache.size > 10000) {
    const entries = Array.from(rateLimitCache.entries());
    entries.slice(0, 5000).forEach(([k]) => rateLimitCache.delete(k));
  }
  
  return { allowed: true, remaining: maxRequests - requests.length };
}

//   JSON
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

// ==========   ==========

//    Telegram 
async function logToChat(env, message, level = 'info', extra = {}) {
  const botToken = env.TELEGRAM_BOT_TOKEN;
  const logChatId = env.LOG_CHAT_ID; //    
  
  if (!botToken || !logChatId) {
    console.log('[LOG] No bot token or log chat ID, skipping log:', message);
    return;
  }
  
  //  timestamp   
  const logId = Date.now().toString(36).toUpperCase();
  
  const config = {
    info: { icon: '', title: '', color: '' },
    warn: { icon: '', title: '', color: '' },
    error: { icon: '', title: '', color: '' },
    success: { icon: '', title: '', color: '' },
    admin: { icon: '', title: '', color: '' },
    user: { icon: '', title: '', color: '' },
    trial: { icon: '', title: ' ', color: '' },
    license: { icon: '', title: '', color: '' },
    connect: { icon: '', title: '', color: '' },
    webhook: { icon: '', title: 'WEBHOOK', color: '' },
    system: { icon: '', title: '', color: '' }
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
  
  //   
  let text = `${cfg.color} ${cfg.icon} *${cfg.title}* #${logId}\n`;
  text += `\n\n`;
  text += `${message}\n\n`;
  
  //     
  if (extra.telegramId) {
    text += ` TG ID: \`${extra.telegramId}\`\n`;
  }
  if (extra.username) {
    text += ` Username: @${extra.username}\n`;
  }
  if (extra.action) {
    text += ` : ${extra.action}\n`;
  }
  if (extra.details) {
    text += ` : ${extra.details}\n`;
  }
  
  text += `\n\n`;
  text += ` ${time} MSK`;
  
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

//      
async function getLicenseData(env, oderId) {
  //     oderId
  const userData = await env.USERS.get(`user:${oderId}`);
  if (!userData) {
    return { connected: false, error: 'NOT_CONNECTED' };
  }
  
  const user = JSON.parse(userData);
  const telegramId = user.telegramId;
  const username = user.username;
  
  //    Telegram ID ( )  username
  let licenseData = null;
  
  //    telegramId
  if (telegramId) {
    const licenseByTgId = await env.USERS.get(`license:tg:${telegramId}`);
    if (licenseByTgId) {
      licenseData = JSON.parse(licenseByTgId);
    }
  }
  
  //   ,   username
  if (!licenseData && username) {
    const licenseByUsername = await env.USERS.get(`license:${username}`);
    if (licenseByUsername) {
      licenseData = JSON.parse(licenseByUsername);
    }
  }
  
  //    -     (12   )
  if (!licenseData) {
    // ,      
    const trialUsed = await env.USERS.get(`trial:${telegramId || username}`);
    
    if (!trialUsed) {
      //     12  ( )
      const trialExpiresAt = Date.now() + TRIAL_DURATION_NO_REFERRAL;
      const trialHours = Math.round(TRIAL_DURATION_NO_REFERRAL / MS_PER_HOUR);
      licenseData = {
        type: 'trial',
        expiresAt: trialExpiresAt,
        createdAt: Date.now(),
        telegramId: telegramId,
        username: username
      };
      
      //  
      if (telegramId) {
        await env.USERS.put(`license:tg:${telegramId}`, JSON.stringify(licenseData));
      }
      if (username) {
        await env.USERS.put(`license:${username}`, JSON.stringify(licenseData));
      }
      
      //     
      await env.USERS.put(`trial:${telegramId || username}`, 'used');
      
      //    
      await logToChat(env, 
        ` *  !*\n\n` +
        `   ${trialHours}   `,
        'trial',
        {
          telegramId: telegramId || '',
          username: username || '',
          action: '  ',
          details: `: ${trialHours} `
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
    // Rate limiting  IP
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `license:${clientIP}`);
    if (!rateLimit.allowed) {
      return jsonResponse({ 
        valid: false, 
        error: 'RATE_LIMITED', 
        message: `  .   ${rateLimit.resetIn}` 
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
        message: 'Telegram  ' 
      });
    }
    
    const license = result.license;
    
    if (!license) {
      return jsonResponse({ 
        valid: false, 
        error: 'NO_LICENSE',
        message: '  '
      });
    }
    
    const now = Date.now();
    const isValid = license.expiresAt > now;
    
    //    (   )
    if (isValid && deviceId && result.user.telegramId) {
      const deviceCheck = await checkAndRegisterDevice(env, result.user.telegramId, deviceId, license);
      if (!deviceCheck.allowed) {
        return jsonResponse({
          valid: false,
          error: 'DEVICE_LIMIT',
          message: `   (${deviceCheck.maxDevices}).     .`,
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
    const rateLimit = await checkRateLimit(env, `code:${clientIP}`, 10); // 10    
    if (!rateLimit.allowed) {
      return jsonResponse({ 
        error: 'RATE_LIMITED', 
        message: `  .   ${rateLimit.resetIn}` 
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
    
    //  oderId
    if (typeof oderId !== 'string' || oderId.length < 16 || oderId.length > 64) {
      return jsonResponse({ error: 'Invalid oderId format' }, 400);
    }
    
    // :       
    //        Telegram 
    //   " "    
    const oldUserData = await env.USERS.get(`user:${oderId}`);
    if (oldUserData) {
      const oldUser = JSON.parse(oldUserData);
      //    tg -> oderId
      if (oldUser.telegramId) {
        await env.USERS.delete(`tg:${oldUser.telegramId}`);
      }
      //   user
      await env.USERS.delete(`user:${oderId}`);
      
      // 
      await logToChat(env,
        ` * *\n\n` +
        `   ,   `,
        'connect',
        {
          telegramId: oldUser.telegramId || '',
          username: oldUser.username || '',
          action: '  ',
          details: '  '
        }
      );
    }
    
    const code = generateCode();
    
    //      
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

//    Telegram ( )
async function handleTelegramConnect(request, env) {
  try {
    // Rate limiting  
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `connect:${clientIP}`, 20); // 20   
    if (!rateLimit.allowed) {
      return jsonResponse({ error: 'RATE_LIMITED', message: `  .   ${rateLimit.resetIn}` }, 429);
    }
    
    const { data, error } = await safeParseJson(request);
    if (error) {
      return jsonResponse({ error: 'INVALID_REQUEST', message: error }, 400);
    }
    
    const { code, telegramId, username, firstName } = data || {};
    
    if (!code || !telegramId) {
      return jsonResponse({ error: 'code and telegramId are required' }, 400);
    }
    
    //   (   , 8 )
    if (!/^[A-Z0-9]{8}$/i.test(code)) {
      return jsonResponse({ error: 'Invalid code format' }, 400);
    }
    
    //  telegramId (  )
    if (!/^\d+$/.test(String(telegramId))) {
      return jsonResponse({ error: 'Invalid telegramId' }, 400);
    }
    
    //    
    const codeData = await env.USERS.get(`code:${code}`);
    if (!codeData) {
      return jsonResponse({ error: 'Code expired or invalid', code: 'INVALID_CODE' }, 400);
    }
    
    const { oderId, deviceId } = JSON.parse(codeData);
    
    //   user -> telegram
    const userData = {
      oderId,
      deviceId,
      telegramId,
      username: username || null,
      firstName: firstName || null,
      connectedAt: Date.now()
    };
    
    await env.USERS.put(`user:${oderId}`, JSON.stringify(userData));
    
    //     telegram -> oderId   
    await env.USERS.put(`tg:${telegramId}`, oderId);
    
    //   
    await env.USERS.delete(`code:${code}`);
    
    // / 
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

//   /  ()
async function handleGrantLicense(request, env) {
  try {
    const authHeader = request.headers.get('Authorization');
    const adminToken = env.ADMIN_TOKEN;
    
    if (!adminToken || authHeader !== `Bearer ${adminToken}`) {
      //    
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
    
    //  days
    if (!days || !Number.isInteger(days) || days <= 0 || days > 365) {
      return jsonResponse({ error: 'days must be positive integer (1-365)' }, 400);
    }
    
    //     
    let currentLicense = null;
    if (telegramId) {
      const existing = await env.USERS.get(`license:tg:${telegramId}`);
      if (existing) currentLicense = JSON.parse(existing);
    }
    if (!currentLicense && username) {
      const existing = await env.USERS.get(`license:${username}`);
      if (existing) currentLicense = JSON.parse(existing);
    }
    
    //    
    let baseTime = Date.now();
    
    //     -   
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
    
    //    
    if (telegramId) {
      await env.USERS.put(`license:tg:${telegramId}`, JSON.stringify(licenseData));
      //    
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

// ==========    ==========

//    :
// trial () - 1 ,  
// level1 ( 1) -    
// level2 ( 2) -  +  ,     
// level3 ( 3) -  ,   

const LICENSE_TYPES = {
  trial: { 
    name: ' ', 
    level: 0,
    groupChats: false, 
    maxDevices: 1, 
    maxChats: 0,
    notifyOptions: ['dm'],  //  
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: true,
      groupNotify: false,
      selectNotify: false
    },
    description: ' 24    '
  },
  level1: { 
    name: '  1', 
    level: 1,
    groupChats: false, 
    maxDevices: 1, 
    maxChats: 0,
    notifyOptions: ['dm'],  //  
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: true,
      groupNotify: false,
      selectNotify: false
    },
    description: '1     '
  },
  level2: { 
    name: '  2', 
    level: 2,
    groupChats: true, 
    maxDevices: 1, 
    maxChats: 1,
    notifyOptions: ['group'],  //    
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: false,
      groupNotify: true,
      selectNotify: false
    },
    description: ' +      '
  },
  level3: { 
    name: '  3', 
    level: 3,
    groupChats: true, 
    maxDevices: 2, 
    maxChats: 1,
    notifyOptions: ['dm', 'group', 'both'],  // 
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: true,
      groupNotify: true,
      selectNotify: true,
      priority: true
    },
    description: '2    '
  }
};

//    
LICENSE_TYPES.basic = LICENSE_TYPES.level1;
LICENSE_TYPES.subscription = LICENSE_TYPES.level1;
LICENSE_TYPES.premium = LICENSE_TYPES.level2;
LICENSE_TYPES.max = LICENSE_TYPES.level3;

// ==========      ==========

const DEFAULT_USER_SETTINGS = {
  notifications: {
    clicks: true,          //    ( )
    expiryReminders: true, //    
    news: true,            //   
    sound: true            //   ( Telegram  ,   )
  },
  //   : 'dm' | 'group' | 'both'
  notifyDestination: 'dm',
  createdAt: null,
  updatedAt: null
};

//   
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

//   
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

//   
async function toggleNotificationSetting(env, telegramId, settingKey) {
  const settings = await getUserSettings(env, telegramId);
  if (settings.notifications.hasOwnProperty(settingKey)) {
    settings.notifications[settingKey] = !settings.notifications[settingKey];
    await saveUserSettings(env, telegramId, settings);
  }
  return settings;
}

// ==========   ==========

//     
async function getUserGroupChats(env, telegramId) {
  const data = await env.USERS.get(`groupchats:${telegramId}`);
  if (!data) return { chats: [], pending: [] };
  return JSON.parse(data);
}

//    
async function saveUserGroupChats(env, telegramId, groupData) {
  await env.USERS.put(`groupchats:${telegramId}`, JSON.stringify(groupData));
}

//     
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

//        (  )
async function requestGroupChatApproval(env, telegramId, chatId, chatTitle, requestedBy) {
  const pendingKey = `pending_group:${chatId}`;
  
  //     
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
  
  await env.USERS.put(pendingKey, JSON.stringify(requestData), { expirationTtl: 7 * 24 * 60 * 60 }); // 7 
  
  //  ID    
  const adminUsername = env.ADMIN_USERNAME || 'YaMob';
  const adminTelegramId = env.ADMIN_TELEGRAM_ID; //     
  
  const approvalMsg = ` *   *\n\n` +
    ` : @${requestedBy || telegramId}\n` +
    ` TG ID: \`${telegramId}\`\n` +
    ` : "${escapeMarkdown(chatTitle)}"\n` +
    ` Chat ID: \`${chatId}\`\n\n` +
    `  :\n\`/approve\\_group ${chatId}\`\n\n` +
    `  :\n\`/reject\\_group ${chatId}\``;
  
  //     (  ADMIN_TELEGRAM_ID)
  if (adminTelegramId) {
    try {
      await sendTelegramMessage(env, adminTelegramId, approvalMsg, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: ' ', callback_data: `approve_group:${chatId}` },
              { text: ' ', callback_data: `reject_group:${chatId}` }
            ]
          ]
        }
      });
    } catch (e) {
      console.error('Failed to send approval notification to admin:', e);
    }
  }
  
  //    - ( )
  await logToChat(env,
    ` *   *\n\n` +
    ` : @${requestedBy || telegramId}\n` +
    ` : "${chatTitle}"\n` +
    ` Chat ID: \`${chatId}\`\n\n` +
    ` /approve_group ${chatId}  `,
    'admin',
    { telegramId, action: ' ' }
  );
  
  return { success: true };
}

//    ( )
async function approveGroupChat(env, chatId, approvedBy) {
  const pendingKey = `pending_group:${chatId}`;
  const pendingData = await env.USERS.get(pendingKey);
  
  if (!pendingData) {
    return { success: false, error: 'REQUEST_NOT_FOUND' };
  }
  
  const request = JSON.parse(pendingData);
  const telegramId = request.requestedBy;
  
  //    
  const groupData = await getUserGroupChats(env, telegramId);
  
  //  
  groupData.chats.push({
    chatId: chatId,
    chatTitle: request.chatTitle,
    approvedAt: Date.now(),
    approvedBy: approvedBy
  });
  
  //   pending
  groupData.pending = (groupData.pending || []).filter(p => p.chatId !== chatId);
  
  await saveUserGroupChats(env, telegramId, groupData);
  await env.USERS.delete(pendingKey);
  
  //     -> 
  await env.USERS.put(`group_owner:${chatId}`, telegramId);
  
  return { success: true, telegramId };
}

// ==========   ==========

//   
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

//   
async function saveUserStats(env, telegramId, stats) {
  await env.USERS.put(`stats:${telegramId}`, JSON.stringify(stats));
}

//   
async function recordUserClick(env, telegramId) {
  const stats = await getUserStats(env, telegramId);
  const today = new Date().toISOString().slice(0, 10);
  
  //    -  todayClicks
  if (stats.lastClickDate !== today) {
    stats.todayClicks = 0;
    stats.lastClickDate = today;
  }
  
  stats.totalClicks++;
  stats.todayClicks++;
  
  await saveUserStats(env, telegramId, stats);
  return stats;
}

// ==========   ==========

//   
function generateReferralCode(telegramId) {
  const hash = String(telegramId).split('').reduce((a, b) => {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);
  return REFERRAL_CODE_PREFIX + Math.abs(hash).toString(36).toUpperCase().slice(0, 6);
}

//     (  )
async function getUserReferralCode(env, telegramId) {
  let stats = await getUserStats(env, telegramId);
  
  if (!stats.referralCode) {
    stats.referralCode = generateReferralCode(telegramId);
    await saveUserStats(env, telegramId, stats);
  }
  
  //    -> telegramId
  await env.USERS.put(`refcode:${stats.referralCode}`, telegramId);
  
  return stats.referralCode;
}

//   
async function applyReferralCode(env, newUserTelegramId, referralCode) {
  //   
  const referrerTelegramId = await env.USERS.get(`refcode:${referralCode}`);
  if (!referrerTelegramId) {
    return { success: false, error: 'INVALID_CODE' };
  }
  
  if (referrerTelegramId === newUserTelegramId) {
    return { success: false, error: 'SELF_REFERRAL' };
  }
  
  // ,        
  const newUserStats = await getUserStats(env, newUserTelegramId);
  if (newUserStats.referredBy) {
    return { success: false, error: 'ALREADY_REFERRED' };
  }
  
  //   (   -    )
  newUserStats.referredBy = referrerTelegramId;
  newUserStats.referralBonusPending = true; //    
  await saveUserStats(env, newUserTelegramId, newUserStats);
  
  //      ()
  const referrerStats = await getUserStats(env, referrerTelegramId);
  referrerStats.referralCount = (referrerStats.referralCount || 0) + 1;
  referrerStats.referralPendingBonus = (referrerStats.referralPendingBonus || 0) + 1; //  
  await saveUserStats(env, referrerTelegramId, referrerStats);
  
  //      -    
  
  //            (24 )
  const newUserOderId = await env.USERS.get(`tg:${newUserTelegramId}`);
  let gaveTrialToNewUser = false;
  
  if (newUserOderId) {
    const newUserResult = await getLicenseData(env, newUserOderId);
    //       
    if (!newUserResult.license || newUserResult.license.expiresAt < Date.now()) {
      const trialLicense = {
        type: 'trial',
        active: true,
        expiresAt: Date.now() + REFERRAL_TRIAL_HOURS * MS_PER_HOUR, // 24   
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

//          (+24 )
async function grantReferralBonusToReferrer(env, referralTelegramId) {
  const userStats = await getUserStats(env, referralTelegramId);
  
  //       
  if (!userStats.referredBy || !userStats.referralBonusPending) {
    return { success: false, reason: 'NO_PENDING_BONUS' };
  }
  
  const referrerTelegramId = userStats.referredBy;
  
  //   
  userStats.referralBonusPending = false;
  await saveUserStats(env, referralTelegramId, userStats);
  
  //     
  const referrerStats = await getUserStats(env, referrerTelegramId);
  referrerStats.referralPendingBonus = Math.max(0, (referrerStats.referralPendingBonus || 0) - 1);
  referrerStats.referralPaidBonus = (referrerStats.referralPaidBonus || 0) + 1; // 
  await saveUserStats(env, referrerTelegramId, referrerStats);
  
  //    (+24 )
  const oderId = await env.USERS.get(`tg:${referrerTelegramId}`);
  if (oderId) {
    const result = await getLicenseData(env, oderId);
    if (result.license) {
      const newExpiry = Math.max(result.license.expiresAt, Date.now()) + REFERRAL_BONUS_HOURS * MS_PER_HOUR;
      result.license.expiresAt = newExpiry;
      result.license.updatedAt = Date.now();
      await env.USERS.put(`license:tg:${referrerTelegramId}`, JSON.stringify(result.license));
    } else {
      //    -    24 
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

// ==========    ==========

//     (  cron)
async function checkAndSendExpirationReminders(env) {
  //     Cloudflare Cron Triggers
  //     cron  wrangler.toml
  
  const remindDays = [3, 1, 0]; //  3 ,  1 ,    
  const now = Date.now();
  const botToken = env.TELEGRAM_BOT_TOKEN;
  
  if (!botToken) return { sent: 0 };
  
  let sentCount = 0;
  
  //     
  // (   list operation  KV,   )
  //     
  
  const reminderQueue = await env.USERS.get('reminder_queue');
  if (!reminderQueue) return { sent: 0 };
  
  const queue = JSON.parse(reminderQueue);
  const newQueue = [];
  
  for (const item of queue) {
    const { telegramId, expiresAt, lastReminder, expiredNotified } = item;
    const daysLeft = Math.ceil((expiresAt - now) / MS_PER_DAY);
    
    //        
    if (expiresAt <= now && !expiredNotified) {
      //   -  
      try {
        await sendTelegramMessage(env, telegramId, 
          ` * *\n\n` +
          `  .\n` +
          `   .\n\n` +
          `     :\n` +
          `@YaMob`,
          {
            reply_markup: {
              inline_keyboard: [
                [{ text: '  @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: ' ', callback_data: 'status' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          }
        );
        item.expiredNotified = true;
        sentCount++;
      } catch (e) {
        console.error('Expiration notification error:', e);
      }
      //       
      newQueue.push(item);
      continue;
    }
    
    //     ( )
    for (const reminderDay of remindDays) {
      if (daysLeft === reminderDay && reminderDay > 0) {
        const reminderKey = `${telegramId}_${reminderDay}`;
        if (lastReminder !== reminderKey) {
          //  
          try {
            await sendTelegramMessage(env, telegramId, 
              ` *  *\n\n` +
              `    *${daysLeft}* ${getDaysWord(daysLeft)}!\n\n` +
              ` ,      .`,
              {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: ' ', callback_data: 'buy' }],
                    [{ text: ' ', callback_data: 'status' }]
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
    
    //            - 
    //   ,       > 7  
    const expiredMoreThan7Days = expiresAt < (now - 7 * MS_PER_DAY);
    if (!expiredMoreThan7Days) {
      newQueue.push(item);
    }
  }
  
  await env.USERS.put('reminder_queue', JSON.stringify(newQueue));
  return { sent: sentCount };
}

//     
async function addToReminderQueue(env, telegramId, expiresAt) {
  let queue = [];
  const queueData = await env.USERS.get('reminder_queue');
  if (queueData) queue = JSON.parse(queueData);
  
  //     
  queue = queue.filter(item => item.telegramId !== telegramId);
  
  //  
  queue.push({ telegramId, expiresAt, lastReminder: null });
  
  await env.USERS.put('reminder_queue', JSON.stringify(queue));
}

//   ""
function getDaysWord(days) {
  if (days === 1) return '';
  if (days >= 2 && days <= 4) return '';
  return '';
}

async function handleNotify(request, env) {
  try {
    // Rate limiting  
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `notify:${clientIP}`, 30); // 30   
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
    
    //  oderId
    if (typeof oderId !== 'string' || oderId.length < 16 || oderId.length > 64) {
      return jsonResponse({ error: 'Invalid oderId format' }, 400);
    }
    
    //  transaction ( )
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
    
    //    -     
    const telegramId = result.user.telegramId;
    const userSettings = await getUserSettings(env, telegramId);
    
    console.log(`[NOTIFY] User ${telegramId}, clicks enabled: ${userSettings.notifications.clicks}`);
    
    if (!userSettings.notifications.clicks) {
      //   ,   
      if (telegramId) {
        await recordUserClick(env, telegramId);
      }
      return jsonResponse({ success: true, notificationsSent: 0, reason: 'NOTIFICATIONS_DISABLED' });
    }
    
    //      
    let message = ` *   !*\n\n`;
    if (transaction) {
      if (transaction.transactionId) {
        // transactionId   -  
        message += ` *ID:* \`${String(transaction.transactionId).slice(0, 50)}\`\n`;
      }
      if (transaction.amount) {
        message += ` *:* ${escapeMarkdown(String(transaction.amount).slice(0, 30))}\n`;
      }
      if (transaction.method) {
        message += ` *:* ${escapeMarkdown(String(transaction.method).slice(0, 50))}\n`;
      }
      if (transaction.created) {
        message += ` *:* ${escapeMarkdown(String(transaction.created).slice(0, 30))}\n`;
      }
      if (transaction.requisites) {
        // requisites   -  ,   
        message += ` *:* \`${String(transaction.requisites).slice(0, 50)}\`\n`;
      }
      if (transaction.bank) {
        message += ` *:* ${escapeMarkdown(String(transaction.bank).slice(0, 50))}\n`;
      }
    }
    
    const time = new Date().toLocaleString('ru-RU', { 
      timeZone: 'Europe/Moscow',
      hour: '2-digit', 
      minute: '2-digit',
      second: '2-digit'
    });
    message += `\n *:* ${time} ()`;
    
    //    Telegram
    const botToken = env.TELEGRAM_BOT_TOKEN;
    
    //    
    if (telegramId) {
      await recordUserClick(env, telegramId);
    }
    
    let sentTo = [];
    
    if (botToken && telegramId) {
      const licenseType = LICENSE_TYPES[result.license.type];
      const canUseDm = licenseType ? licenseType.dmNotifications : true;
      const canUseGroup = licenseType ? licenseType.groupNotifications : false;
      
      //         
      // trial, level1:   
      // level2:    ( ),   
      // level3:   notifyDestination
      
      let sendToDm = false;
      let sendToGroup = false;
      
      const licenseLevel = result.license.type;
      const notifyDest = userSettings.notifyDestination || 'dm';
      
      if (licenseLevel === 'trial' || licenseLevel === 'level1') {
        //  
        sendToDm = true;
        sendToGroup = false;
      } else if (licenseLevel === 'level2') {
        //   ( ),  fallback  
        sendToGroup = true;
        sendToDm = false; // fallback     
      } else if (licenseLevel === 'level3') {
        //   
        if (notifyDest === 'dm') {
          sendToDm = true;
        } else if (notifyDest === 'group') {
          sendToGroup = true;
        } else if (notifyDest === 'both') {
          sendToDm = true;
          sendToGroup = true;
        }
      } else {
        //  /  -    
        sendToDm = canUseDm;
        sendToGroup = canUseGroup;
      }
      
      //     
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
      
      //      
      if (sendToGroup && canUseGroup) {
        const groupData = await getUserGroupChats(env, telegramId);
        
        if (groupData.chats && groupData.chats.length > 0) {
          //       
          const username = result.user.username;
          const groupMessage = ` *${username ? '@' + username : ''}*\n\n${message}`;
          
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
                //      -  
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
          // Level2 fallback:   ,   
          try {
            const personalResult = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: telegramId,
                text: message + `\n\n_     _`,
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

//      (fallback)
async function showAdminPanel(env, chatId, stats, pendingGroups, messageId = null) {
  const adminMsg = ` *-*\n` +
    `\n\n` +
    ` *:*\n` +
    `  : ${stats.totalUsers}\n` +
    `  : ${stats.activeUsers}\n` +
    `  Level 1: ${stats.level1Users || 0}\n` +
    `  Level 2: ${stats.level2Users || 0}\n` +
    `  Level 3: ${stats.level3Users || 0}\n` +
    `  Trial: ${stats.trialUsers}\n\n` +
    ` *:*\n` +
    `  : ${pendingGroups.length}\n` +
    `  : ${stats.approvedGroups}\n\n` +
    `_: 1 _`;
    
  const keyboard = {
    inline_keyboard: [
      //  1:   ( )
      pendingGroups.length > 0 ? [
        { text: `    (${pendingGroups.length})`, callback_data: 'adm_pending' }
      ] : [],
      //  2: 
      [
        { text: '  ', callback_data: 'adm_grant' },
        { text: '  ', callback_data: 'adm_search' }
      ],
      //  3: 
      [
        { text: '  ', callback_data: 'adm_users' },
        { text: '  ', callback_data: 'adm_chats' }
      ],
      //  4: 
      [
        { text: ' ', callback_data: 'adm_broadcast' },
        { text: ' ', callback_data: 'adm_stats' }
      ],
      //  5: 
      [
        { text: ' ', callback_data: 'adm_system' },
        { text: '', callback_data: 'adm_refresh' }
      ],
      //  6: 
      [
        { text: ' ', callback_data: 'adm_logout' }
      ]
    ].filter(row => row.length > 0)
  };
  
  if (messageId) {
    await editTelegramMessage(env, chatId, messageId, adminMsg, { reply_markup: keyboard });
  } else {
    await sendTelegramMessage(env, chatId, adminMsg, { reply_markup: keyboard });
  }
}

//   
async function handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups) {
  //  fallback:    
  await showAdminPanel(env, chatId, stats, pendingGroups);
  //  
  await logToChat(env, `  -: ${err && err.message ? err.message : err}`);
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

// ===   ===

//  -
function getProgressBar(percent, length = 10) {
  const filled = Math.round((percent / 100) * length);
  const empty = length - filled;
  return ''.repeat(filled) + ''.repeat(empty);
}

//  
function formatTimeLeft(ms) {
  const hours = Math.floor(ms / (60 * 60 * 1000));
  const days = Math.floor(hours / 24);
  
  if (days > 0) {
    return `${days} .`;
  } else if (hours > 0) {
    return `${hours} .`;
  } else {
    const minutes = Math.floor(ms / (60 * 1000));
    return `${minutes} .`;
  }
}

//   
function getStatusEmoji(daysLeft, type) {
  if (type === 'trial') return '';
  if (daysLeft > 14) return '';
  if (daysLeft > 7) return '';
  if (daysLeft > 3) return '';
  return '';
}

// ===  ===

//   -   
function getMainMenuKeyboard() {
  return {
    inline_keyboard: [
      [{ text: '  ', callback_data: 'how_to_connect' }],
      [
        { text: ' ', callback_data: 'status' },
        { text: ' ', callback_data: 'mystats' }
      ],
      [
        { text: ' ', callback_data: 'help' },
        { text: ' ', callback_data: 'referral' }
      ],
      [
        { text: ' ', callback_data: 'install' },
        { text: ' ', callback_data: 'news' }
      ],
      [
        { text: ' ', callback_data: 'buy' },
        { text: ' ', url: 'https://t.me/YaMob' }
      ]
    ]
  };
}

//       
function getUserMenuKeyboard(isValid, hasGroupChats = false) {
  const keyboard = [];
  
  if (isValid) {
    keyboard.push([
      { text: ' ', callback_data: 'status' },
      { text: ' ', callback_data: 'mystats' }
    ]);
    keyboard.push([
      { text: '  ', callback_data: 'test_notify' },
      { text: ' ', callback_data: 'settings' }
    ]);
    if (hasGroupChats) {
      keyboard.push([{ text: '  ', callback_data: 'groups_menu' }]);
    }
    keyboard.push([
      { text: '  ', callback_data: 'referral' },
      { text: ' ', callback_data: 'help' }
    ]);
  } else {
    keyboard.push([{ text: '  ', callback_data: 'buy' }]);
    keyboard.push([
      { text: ' ', callback_data: 'status' },
      { text: ' ', url: 'https://t.me/YaMob' }
    ]);
  }
  
  return { inline_keyboard: keyboard };
}

//  
function getBackButton() {
  return {
    inline_keyboard: [
      [{ text: ' ', callback_data: 'menu' }]
    ]
  };
}

//  
function getStatusKeyboard(isValid, type) {
  if (isValid) {
    return {
      inline_keyboard: [
        [{ text: '  ', callback_data: 'test_notify' }],
        [
          { text: ' ', callback_data: 'status' },
          { text: ' ', callback_data: 'menu' }
        ]
      ]
    };
  }
  return {
    inline_keyboard: [
      [{ text: '  ', callback_data: 'buy' }],
      [
        { text: ' ', callback_data: 'status' },
        { text: ' ', callback_data: 'menu' }
      ]
    ]
  };
}

async function handleTelegramWebhook(request, env) {
  try {
    //  IP  Telegram (,  env.VERIFY_TELEGRAM_IP === 'true')
    if (env.VERIFY_TELEGRAM_IP === 'true') {
      const clientIP = request.headers.get('CF-Connecting-IP');
      if (!isFromTelegram(clientIP)) {
        console.warn(`Webhook from non-Telegram IP: ${clientIP}`);
        return jsonResponse({ ok: false, error: 'Forbidden' }, 403);
      }
    }
    
    //     Telegram ( )
    const secretToken = env.TELEGRAM_WEBHOOK_SECRET;
    if (secretToken) {
      const headerToken = request.headers.get('X-Telegram-Bot-Api-Secret-Token');
      if (headerToken !== secretToken) {
        console.warn('Invalid webhook secret token');
        return jsonResponse({ ok: false, error: 'Forbidden' }, 403);
      }
    }
    
    const { data: update, error } = await safeParseJson(request);
    if (error) {
      console.error('Webhook parse error:', error);
      return jsonResponse({ ok: false, error: 'Invalid JSON' }, 400);
    }
    
    console.log('Webhook received:', JSON.stringify(update).substring(0, 500));
    
    //    (  Telegram ID  )
    const isAdmin = async (telegramId) => {
      return await isAdminById(env, telegramId);
    };
    
    //   -     
    const isGroupChat = (chat) => {
      return chat && (chat.type === 'group' || chat.type === 'supergroup');
    };
    
    // ===  INLINE  ===
    if (update.inline_query) {
      const inlineQuery = update.inline_query;
      const telegramId = String(inlineQuery.from.id);
      const query = inlineQuery.query.toLowerCase().trim();
      
      // Rate limit  inline
      const rateCheck = checkBotRateLimit(telegramId, 'callbacks');
      if (!rateCheck.allowed) {
        return jsonResponse({ ok: true });
      }
      
      //    
      const refCode = await getUserReferralCode(env, telegramId);
      const botUsername = env.BOT_USERNAME || 'clickuved_bot';
      
      const results = [];
      
      //    -
      if (query === '' || query.includes('') || query.includes('ref') || query.includes('invite')) {
        results.push({
          type: 'article',
          id: 'ref_' + telegramId,
          title: '  ',
          description: '  ',
          input_message_content: {
            message_text: ` *  Exotic Assistant*\n\n` +
              `!    Exotic.\n\n` +
              `   \n` +
              `   Telegram\n` +
              `  \n\n` +
              `   : \`${refCode}\`\n\n` +
              `   : @${botUsername}\n` +
              `  : \`/ref ${refCode}\`\n\n` +
              `_   :_\n` +
              `https://t.me/${botUsername}?start=ref_${refCode}`,
            parse_mode: 'Markdown'
          },
          reply_markup: {
            inline_keyboard: [
              [{ text: '   ', url: `https://t.me/${botUsername}?start=ref_${refCode}` }]
            ]
          }
        });
      }
      
      //   inline 
      await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/answerInlineQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inline_query_id: inlineQuery.id,
          results: results,
          cache_time: 300, //  5 
          is_personal: true
        })
      });
      
      return jsonResponse({ ok: true });
    }
    
    // ===  CALLBACK  ===
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
      
      // Rate limiting  callback
      const rateCheck = checkBotRateLimit(telegramId, 'callbacks');
      if (!rateCheck.allowed) {
        await answerCallbackQuery(env, callback.id, rateCheck.message, true);
        return jsonResponse({ ok: true });
      }
      
      //   callback
      await answerCallbackQuery(env, callback.id);
      
      //  callback   
      if (isGroup && data === 'group_status') {
        const existingOderId = await env.USERS.get(`tg:${telegramId}`);
        const botUsername = env.BOT_USERNAME || 'clickuved_bot';
        
        if (!existingOderId) {
          await editTelegramMessage(env, chatId, messageId, 
            ` @${username || telegramId},  :\n\n` +
            `   \n\n` +
            `     .`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: ' ', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
          return jsonResponse({ ok: true });
        }
        
        const result = await getLicenseData(env, existingOderId);
        
        if (!result.license) {
          await editTelegramMessage(env, chatId, messageId, 
            ` @${username || telegramId},  :\n\n` +
            `  \n` +
            `   `,
            { reply_markup: {
              inline_keyboard: [
                [{ text: ' ', url: `https://t.me/${botUsername}` }]
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
        
        const emoji = isValid ? getStatusEmoji(daysLeft, license.type) : '';
        const typeText = LICENSE_TYPES[license.type]?.name || '';
        const timeDisplay = license.type === 'trial' ? `${hoursLeft} .` : `${daysLeft} .`;
        
        await editTelegramMessage(env, chatId, messageId, 
          ` @${username || telegramId},  :\n\n` +
          `${emoji} ${typeText}  ${isValid ? '' : ''}\n` +
          ` ${isValid ? `: ${timeDisplay}` : ' '}`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: ' ', url: `https://t.me/${botUsername}` }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      switch (data) {
        case 'menu': {
          const menuText = ` **\n\n :`;
          
          await editTelegramMessage(env, chatId, messageId, menuText, {
            reply_markup: getMainMenuKeyboard()
          });
          break;
        }
        
        case 'status': {
          //   
          const existingOderId = await env.USERS.get(`tg:${telegramId}`);
          
          if (!existingOderId) {
            await editTelegramMessage(env, chatId, messageId, 
              ` **\n\n` +
              `   \n\n` +
              `  \n   `,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: ' ', callback_data: 'how_to_connect' }],
                  [{ text: ' ', callback_data: 'buy' }],
                  [{ text: ' ', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          //    getLicenseData (   )
          const result = await getLicenseData(env, existingOderId);
          
          if (!result.license) {
            await editTelegramMessage(env, chatId, messageId, 
              ` **\n\n` +
              `  \n` +
              `   \n\n` +
              ` @YaMob \n `,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '  ', callback_data: 'buy' }],
                  [{ text: ' ', callback_data: 'support' }],
                  [{ text: ' ', callback_data: 'menu' }]
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
          
          //    
          const devicesData = await getUserDevices(env, telegramId);
          const deviceCount = devicesData.devices?.length || 0;
          const maxDevices = licenseType.maxDevices || 1;
          
          let message;
          if (isValid) {
            const emoji = getStatusEmoji(daysLeft, license.type);
            const typeText = licenseType.name || '';
            const timeDisplay = license.type === 'trial' ? `${hoursLeft} .` : `${daysLeft} .`;
            
            // - ( trial 24,   30)
            const maxTime = license.type === 'trial' ? 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
            const percent = Math.min(100, Math.round((timeLeft / maxTime) * 100));
            const bar = getProgressBar(percent);
            
            message = ` * *\n\n`;
            message += `${emoji} ${typeText}  \n`;
            message += `${bar} ${percent}%\n\n`;
            message += ` : *${timeDisplay}*\n`;
            message += ` : ${expiresDate}\n`;
            message += ` : ${deviceCount}/${maxDevices}`;
            
            if (deviceCount > 0 && deviceCount >= maxDevices) {
              message += ` `;
            }
            
            if (daysLeft <= 3 && license.type !== 'trial') {
              message += `\n\n _ _`;
            }
          } else {
            message = ` * *\n\n`;
            message += `  ${expiresDate}\n`;
            message += ` : ${deviceCount}/${maxDevices}\n\n`;
            message += `_  _`;
          }
          
          //     
          const keyboard = {
            inline_keyboard: []
          };
          
          if (isValid) {
            keyboard.inline_keyboard.push([{ text: ' ', callback_data: 'buy' }]);
            if (deviceCount > 0) {
              keyboard.inline_keyboard.push([{ text: '  ', callback_data: 'devices' }]);
            }
            keyboard.inline_keyboard.push([
              { text: ' ', callback_data: 'status' },
              { text: ' ', callback_data: 'menu' }
            ]);
          } else {
            keyboard.inline_keyboard.push(
              [{ text: '  ', callback_data: 'buy' }],
              [
                { text: ' ', callback_data: 'status' },
                { text: ' ', callback_data: 'menu' }
              ]
            );
          }
          
          //       (3, 1, 0 )
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
          //    
          const devicesData = await getUserDevices(env, telegramId);
          const devices = devicesData.devices || [];
          
          //    maxDevices
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          let maxDevices = 1;
          if (oderId) {
            const licResult = await getLicenseData(env, oderId);
            if (licResult.license) {
              const licType = LICENSE_TYPES[licResult.license.type] || LICENSE_TYPES.trial;
              maxDevices = licType.maxDevices || 1;
            }
          }
          
          let message = ` * * (${devices.length}/${maxDevices})\n\n`;
          
          if (devices.length === 0) {
            message += `_  _`;
          } else {
            devices.forEach((device, index) => {
              const shortId = device.deviceId.substring(0, 12) + '...';
              const addedDate = new Date(device.addedAt).toLocaleDateString('ru-RU');
              const lastSeenDate = new Date(device.lastSeen).toLocaleDateString('ru-RU');
              message += `${index + 1}. \`${shortId}\`\n`;
              message += `    : ${addedDate}\n`;
              message += `    . .: ${lastSeenDate}\n\n`;
            });
          }
          
          if (devices.length >= maxDevices) {
            message += `\n _ .     ._`;
          }
          
          //      
          const keyboard = {
            inline_keyboard: []
          };
          
          devices.forEach((device, index) => {
            keyboard.inline_keyboard.push([
              { text: `   ${index + 1}`, callback_data: `remove_device:${device.deviceId.substring(0, 32)}` }
            ]);
          });
          
          keyboard.inline_keyboard.push([{ text: ' ', callback_data: 'status' }]);
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: keyboard
          });
          break;
        }
        
        case 'help': {
          const helpMsg = ` *Exotic Assistant v${BOT_VERSION}*

 **
   " "

 **
      

 **
Push  Telegram  

 **
    

 * *
    +1 !`;
          
          await editTelegramMessage(env, chatId, messageId, helpMsg, {
            reply_markup: {
              inline_keyboard: [
                [
                  { text: ' ', callback_data: 'how_to_connect' },
                  { text: ' FAQ', callback_data: 'faq' }
                ],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'how_to_connect': {
          const connectMsg = ` *  *

1    
2    
3  "" 
4  " Telegram"
5      

 * :*
 24   
   
  `;
          
          await editTelegramMessage(env, chatId, messageId, connectMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '  ', callback_data: 'status' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'buy': {
          const buyMsg = ` * Exotic Assistant v${BOT_VERSION}*



 * 1*  15 USDT/
   
     
  
  1 
  24   ()



 * 2*  30 USDT/
     1
     
     
   2 



 * 3 ()*  35 USDT/
   
     
   +  
  VIP 
   3 



 *:* USDT (TRC20)

_    _ `;
          
          await editTelegramMessage(env, chatId, messageId, buyMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '  @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: '  ', callback_data: 'compare_plans' }],
                [
                  { text: ' ', callback_data: 'status' },
                  { text: ' ', callback_data: 'menu' }
                ]
              ]
            }
          });
          break;
        }
        
        case 'compare_plans': {
          const compareMsg = ` * *

|  | L1 | L2 | L3 |
|---------|:--:|:--:|:--:|
|  |  |  |  |
|   |  |  |  |
|   |  |  |  |
|    |  |  |  |
| .  |  |  |  |
| VIP  |  |  |  |
|  | 1 | 2 | 3 |

 *:*
  1: 15 USDT/
  2: 30 USDT/
  3: 35 USDT/`;`
          
          await editTelegramMessage(env, chatId, messageId, compareMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '  @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: ' ', callback_data: 'buy' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'clear_devices': {
          //    
          await clearAllDevices(env, telegramId);
          
          const clearMsg = ' * *\n\n      .\n\n       .';
          
          await editTelegramMessage(env, chatId, messageId, clearMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '  ', callback_data: 'my_devices' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'my_devices': {
          //   (callback   /devices)
          const licenseData = await env.USERS.get('license:tg:' + telegramId);
          const license = licenseData ? JSON.parse(licenseData) : null;
          const licenseType = LICENSE_TYPES[license?.type] || LICENSE_TYPES.trial;
          const maxDevices = licenseType.maxDevices || 1;
          
          const devicesData = await getUserDevices(env, telegramId);
          const devices = devicesData.devices || [];
          
          let message = ' * *\n\n';
          message += ' : *' + licenseType.name + '*\n';
          message += ' : ' + devices.length + '/' + maxDevices + '\n\n';
          
          if (devices.length === 0) {
            message += '_  _';
          } else {
            message += '*:*\n';
            devices.forEach((d, i) => {
              const lastSeenDate = new Date(d.lastSeen).toLocaleDateString('ru-RU');
              message += (i + 1) + '. ' + d.deviceId.substring(0, 12) + '... (' + lastSeenDate + ')\n';
            });
          }
          
          const buttons = [];
          if (devices.length > 0) {
            buttons.push([{ text: '  ', callback_data: 'clear_devices' }]);
          }
          buttons.push([{ text: ' ', callback_data: 'menu' }]);
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: { inline_keyboard: buttons }
          });
          break;
        }
        
        case 'support': {
          //   
          const supportMsg = ' **\n\n      :\n\n : @YaMob\n  :  24 \n\n* :*\n   ?\n   ?\n   ?\n\n   ';
          
          await editTelegramMessage(env, chatId, messageId, supportMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '  @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: ' FAQ', callback_data: 'faq' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'mystats': {
          //    (  /mystats)
          const stats = await getUserStats(env, telegramId);
          const oderId = await env.USERS.get('tg:' + telegramId);
          
          let licenseInfo = '  ';
          let licenseType = null;
          if (oderId) {
            const result = await getLicenseData(env, oderId);
            if (result.license) {
              const daysLeft = Math.max(0, Math.ceil((result.license.expiresAt - Date.now()) / MS_PER_DAY));
              const isActive = result.license.expiresAt > Date.now();
              const typeText = LICENSE_TYPES[result.license.type]?.name || result.license.type;
              licenseInfo = isActive 
                ? ' ' + typeText + ' (' + daysLeft + ' ' + getDaysWord(daysLeft) + ')'
                : ' ' + typeText + ' ()';
              licenseType = result.license.type;
            }
          }
          
          const referralCode = await getUserReferralCode(env, telegramId);
          
          //   
          const todayClicks = stats.todayClicks || 0;
          const totalClicks = stats.totalClicks || 0;
          const referralCount = stats.referralCount || 0;
          
          //   
          let achievementBadge = '';
          if (totalClicks >= 1000) achievementBadge = ' ';
          else if (totalClicks >= 500) achievementBadge = ' ';
          else if (totalClicks >= 100) achievementBadge = ' ';
          else if (totalClicks >= 10) achievementBadge = ' ';
          
          const paidBonuses = stats.referralPaidBonus || 0;
          const pendingBonuses = stats.referralPendingBonus || 0;
          
          const message = ' * *\n\n' +
            achievementBadge + ' *:* ' + todayClicks + ' \n' +
            ' *:* ' + totalClicks + ' \n\n' +
            ' *:* ' + licenseInfo + '\n\n' +
            ' *:* ' + referralCount + '\n' +
            ' * :* ' + paidBonuses + ' .\n' +
            (pendingBonuses > 0 ? ' * :* ' + pendingBonuses + '\n' : '') +
            '\n * :* ' + referralCode + '\n\n' +
            '_ +1     !_';
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '  ', callback_data: 'share_ref' }],
                [
                  { text: ' ', callback_data: 'mystats' },
                  { text: ' ', callback_data: 'menu' }
                ]
              ]
            }
          });
          break;
        }
        
        case 'referral': {
          //    
          const refCode = await getUserReferralCode(env, telegramId);
          const stats = await getUserStats(env, telegramId);
          const botUsername = env.BOT_USERNAME || 'clickuved_bot';
          
          const paidBonuses = stats.referralPaidBonus || 0;
          const pendingBonuses = stats.referralPendingBonus || 0;
          
          const refMsg = ` * *\n\n` +
            `   \n` +
            `*+1 *  !\n\n` +
            ` *  :*\n` +
            `1\ufe0f     \n` +
            `2\ufe0f   1  \n` +
            `3\ufe0f        +1 !\n\n` +
            ` * :* ${stats.referralCount || 0}\n` +
            ` * :* ${paidBonuses} .\n` +
            (pendingBonuses > 0 ? ` * :* ${pendingBonuses}\n` : '') +
            `\n * :* \`${refCode}\`\n\n` +
            `   :\n` +
            `\`https://t.me/${botUsername}?start=ref_${refCode}\``;
          
          await editTelegramMessage(env, chatId, messageId, refMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: ' ', callback_data: 'share_ref' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'settings': {
          //   -  
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          
          if (!oderId) {
            await editTelegramMessage(env, chatId, messageId,
              ` **\n\n` +
              `   \n\n` +
              `     .`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: ' ', callback_data: 'how_to_connect' }],
                  [{ text: ' ', callback_data: 'menu' }]
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
          const canChooseDest = licenseType === 'level3'; //  level3  
          
          //  
          const icon = (enabled) => enabled ? '' : '';
          
          //    
          let destText = '';
          if (licenseType === 'trial' || licenseType === 'level1') {
            destText = ` :   `;
          } else if (licenseType === 'level2') {
            destText = ` :   `;
          } else if (licenseType === 'level3') {
            const destMap = { dm: '', group: '', both: ' + ' };
            const currentDest = userSettings.notifyDestination || 'dm';
            destText = ` : ${destMap[currentDest] || ''}`;
          }
          
          const settingsMsg = ` * *\n\n` +
            `${icon(notif.clicks)}   \n` +
            `${icon(notif.expiryReminders)}   \n` +
            `${icon(notif.news)}   \n\n` +
            (destText ? `${destText}\n\n` : '') +
            `_    _`;
          
          const keyboard = [
            [
              { text: `${icon(notif.clicks)} `, callback_data: 'toggle_notif_clicks' },
              { text: `${icon(notif.expiryReminders)} `, callback_data: 'toggle_notif_expiry' }
            ],
            [{ text: `${icon(notif.news)} `, callback_data: 'toggle_notif_news' }],
            [{ text: '  ', callback_data: 'test_notify' }]
          ];
          
          //     level3
          if (canChooseDest) {
            keyboard.push([{ text: '  ', callback_data: 'notify_dest_menu' }]);
          }
          
          if (canUseGroups) {
            keyboard.push([{ text: '  ', callback_data: 'groups_menu' }]);
          }
          
          keyboard.push([{ text: ' ', callback_data: 'menu' }]);
          
          await editTelegramMessage(env, chatId, messageId, settingsMsg, {
            reply_markup: { inline_keyboard: keyboard }
          });
          break;
        }
        
        //      ( level3)
        case 'notify_dest_menu': {
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          const result = await getLicenseData(env, oderId || '');
          
          if (!result.license || result.license.type !== 'level3') {
            await answerCallbackQuery(env, callback.id, '    3', true);
            break;
          }
          
          const userSettings = await getUserSettings(env, telegramId);
          const currentDest = userSettings.notifyDestination || 'dm';
          
          const check = (val) => currentDest === val ? ' ' : '';
          
          const msg = ` *  ?*\n\n` +
            `,      :\n\n` +
            ` **     \n` +
            ` **     \n` +
            ` **   ,  `;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [
                { text: `${check('dm')} `, callback_data: 'set_dest_dm' },
                { text: `${check('group')} `, callback_data: 'set_dest_group' }
              ],
              [{ text: `${check('both')}+ `, callback_data: 'set_dest_both' }],
              [{ text: ' ', callback_data: 'settings' }]
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
            await answerCallbackQuery(env, callback.id, '    3', true);
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
          
          const destNames = { dm: '', group: '', both: ' + ' };
          await answerCallbackQuery(env, callback.id, ` : ${destNames[newDest]}`);
          
          //   
          //    
          const check = (val) => newDest === val ? ' ' : '';
          
          const msg = ` *  ?*\n\n` +
            ` : *${destNames[newDest]}*`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [
                { text: `${check('dm')} `, callback_data: 'set_dest_dm' },
                { text: `${check('group')} `, callback_data: 'set_dest_group' }
              ],
              [{ text: `${check('both')}+ `, callback_data: 'set_dest_both' }],
              [{ text: ' ', callback_data: 'settings' }]
            ]}
          });
          break;
        }
        
        //  
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
          
          const icon = (enabled) => enabled ? '' : '';
          
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          const result = await getLicenseData(env, oderId || '');
          const licenseType = result.license ? result.license.type : null;
          const canUseGroups = result.license && LICENSE_TYPES[licenseType]?.groupChats;
          const canChooseDest = licenseType === 'level3';
          
          //    
          let destText = '';
          if (licenseType === 'trial' || licenseType === 'level1') {
            destText = ` :   `;
          } else if (licenseType === 'level2') {
            destText = ` :   `;
          } else if (licenseType === 'level3') {
            const destMap = { dm: '', group: '', both: ' + ' };
            const currentDest = updatedSettings.notifyDestination || 'dm';
            destText = ` : ${destMap[currentDest] || ''}`;
          }
          
          const settingsMsg = ` * *\n\n` +
            `${icon(notif.clicks)}   \n` +
            `${icon(notif.expiryReminders)}   \n` +
            `${icon(notif.news)}   \n\n` +
            (destText ? `${destText}\n\n` : '') +
            `_ !_`;
          
          const keyboard = [
            [
              { text: `${icon(notif.clicks)} `, callback_data: 'toggle_notif_clicks' },
              { text: `${icon(notif.expiryReminders)} `, callback_data: 'toggle_notif_expiry' }
            ],
            [{ text: `${icon(notif.news)} `, callback_data: 'toggle_notif_news' }],
            [{ text: '  ', callback_data: 'test_notify' }]
          ];
          
          if (canChooseDest) {
            keyboard.push([{ text: '  ', callback_data: 'notify_dest_menu' }]);
          }
          
          if (canUseGroups) {
            keyboard.push([{ text: '  ', callback_data: 'groups_menu' }]);
          }
          
          keyboard.push([{ text: ' ', callback_data: 'menu' }]);
          
          await editTelegramMessage(env, chatId, messageId, settingsMsg, {
            reply_markup: { inline_keyboard: keyboard }
          });
          
          await answerCallbackQuery(env, callback.id, ' !');
          break;
        }
        
        case 'groups_menu': {
          //   
          const canUse = await canUseGroupChats(env, telegramId);
          
          if (!canUse.allowed) {
            let errorMsg = '';
            switch (canUse.reason) {
              case 'NOT_CONNECTED':
                errorMsg = '   ';
                break;
              case 'NO_LICENSE':
                errorMsg = '   ';
                break;
              case 'UPGRADE_REQUIRED':
                errorMsg = `    * 2*  * 3*\n\n : ${canUse.currentType || ''}`;
                break;
              default:
                errorMsg = '     ';
            }
            
            await editTelegramMessage(env, chatId, messageId, 
              ` * *\n\n${errorMsg}`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '  ', callback_data: 'buy' }],
                  [{ text: ' ', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          const groupData = await getUserGroupChats(env, telegramId);
          
          let message = ` * *\n\n`;
          
          if (groupData.chats.length === 0) {
            message += `     .\n\n`;
            message += `* :*\n`;
            message += `1   @clickuved\\_bot  \n`;
            message += `2    \n`;
            message += `3       `;
          } else {
            message += `*  (${groupData.chats.length}):*\n\n`;
            groupData.chats.forEach((chat, i) => {
              message += `${i + 1}. ${chat.chatTitle}\n`;
            });
            message += `\n_: ${canUse.maxChats} _`;
          }
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: {
              inline_keyboard: [
                [{ text: ' ', callback_data: 'groups_help' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'groups_help': {
          const helpMsg = ` *   *\n\n` +
            `1    Telegram\n` +
            `2   @${env.BOT_USERNAME || 'clickuved_bot'}\n` +
            `3    \n` +
            `4   \n        \n\n` +
            ` *:* 1   \n` +
            ` *:* Premium  `;
          
          await editTelegramMessage(env, chatId, messageId, helpMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: ' ', callback_data: 'groups_menu' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'faq': {
          const faqMsg = ` * *

*   ?*
,    exotic.company
   

*  ?*
 : 1 
 +:  3 
 :  5 

*    ?*
 
   

*  ?*
,   24   

*   ?*
, 24   

*  ?*
 : 30 USDT/
 +: 40 USDT/

*   ?*
 @YaMob`;
          
          await editTelegramMessage(env, chatId, messageId, faqMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: ' ', callback_data: 'buy' }],
                [{ text: '  ', url: 'https://t.me/YaMob' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'install': {
          const installMsg = ` * *

*Chrome / Edge / Brave / Opera:*
1  Chrome Web Store
2  "Exotic Assistant"
3  ""
4  
5 ! 

 * :*
    
  ""
  Telegram
  24 ! `;
          
          await editTelegramMessage(env, chatId, messageId, installMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: ' Chrome Web Store', url: 'https://chromewebstore.google.com/' }],
                [{ text: '  Telegram', callback_data: 'how_to_connect' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'news': {
          const newsMsg = ` *  *

*v${BOT_VERSION}   2026*

   (100% )
    
 Inline   
 Rate limiting  
    

*:*
   
  
  

  ?  @YaMob`;
          
          await editTelegramMessage(env, chatId, messageId, newsMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '  ', url: 'https://t.me/YaMob' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'share_ref': {
          //        
          const refCode = await getUserReferralCode(env, telegramId);
          const botUsername = env.BOT_USERNAME || 'clickuved_bot';
          
          const shareMsg = ` *  Exotic Assistant*\n\n` +
            `!    Exotic.\n\n` +
            `   : \`${refCode}\`\n\n` +
            `   : @${botUsername}\n` +
            `  : \`/ref ${refCode}\``;
          
          await sendTelegramMessage(env, chatId, shareMsg);
          await answerCallbackQuery(env, callback.id, ' ,   !');
          break;
        }
        
        case 'test_notify': {
          //      getLicenseData
          const testOderId = await env.USERS.get(`tg:${telegramId}`);
          
          if (!testOderId) {
            await editTelegramMessage(env, chatId, messageId,
              ` * *\n\n` +
              `   \n\n` +
              `    , \n  .`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '  ', callback_data: 'how_to_connect' }],
                  [{ text: ' ', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          //  getLicenseData    
          const testResult = await getLicenseData(env, testOderId);
          
          if (!testResult.license) {
            await editTelegramMessage(env, chatId, messageId,
              ` * *\n\n` +
              `   \n\n` +
              `    \n  .`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '  ', callback_data: 'buy' }],
                  [{ text: ' ', callback_data: 'support' }],
                  [{ text: ' ', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          if (testResult.license.expiresAt < Date.now()) {
            await editTelegramMessage(env, chatId, messageId,
              ` * *\n\n` +
              `  \n\n` +
              `   \n   .`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '  ', callback_data: 'buy' }],
                  [{ text: ' ', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          //   
          const time = new Date().toLocaleString('ru-RU', { 
            timeZone: 'Europe/Moscow',
            hour: '2-digit', 
            minute: '2-digit'
          });
          
          const testMsg = ` * !*\n\n` +
            ` : *1 000 *\n` +
            ` :  \n` +
            ` : \`0000 0000 0000 0000\`\n` +
            ` : \n\n` +
            ` ${time} ()`;
          
          await sendTelegramMessage(env, chatId, testMsg);
          
          await editTelegramMessage(env, chatId, messageId,
            ` * !*\n\n` +
            `  .\n      ! `,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '  ', callback_data: 'test_notify' }],
                [{ text: '  ', callback_data: 'status' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }}
          );
          break;
        }
        
        // ===  - (CONTROL PANEL) ===
        //   cp_*   adm_*
        case 'cp_pending_groups': {
          //    
          action = 'adm_pending';
          // fall through
        }
        
        case 'cp_users': {
          //    
          action = 'adm_users';
          // fall through
        }
        
        case 'cp_stats': {
          //    
          action = 'adm_stats';
          // fall through -   adm_stats
        }
        
        case 'cp_grant': {
          //    
          action = 'adm_grant';
          // fall through
        }
        
        case 'cp_check': {
          //    
          action = 'adm_search';
          // fall through
        }
        
        case 'cp_broadcast': {
          //    
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
            const msg = ` **\n\n` +
              `  : ${BOT_VERSION}\n` +
              `  : ${EXTENSION_VERSION}\n` +
              ` Uptime: ${uptimeHours} ${uptimeMinutes}\n\n` +
              `*:*\n` +
              ` BOT\_USERNAME: ${env.BOT_USERNAME || ' '}\n` +
              ` ADMIN\_USERNAME: ${env.ADMIN_USERNAME || ' '}\n` +
              ` LOG\_CHAT\_ID: ${env.LOG_CHAT_ID ? '' : ''}\n` +
              ` ADMIN\_TELEGRAM\_ID: ${env.ADMIN_TELEGRAM_ID ? '' : ''}\n` +
              ` WEBHOOK\_SECRET: ${env.TELEGRAM_WEBHOOK_SECRET ? '' : ''}`;
            try {
              await editTelegramMessage(env, chatId, messageId, msg, {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: ' ', callback_data: 'cp_logs' }],
                    [{ text: ' Webhook Info', callback_data: 'cp_webhook' }],
                    [{ text: ' ', callback_data: 'adm_back' }]
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
            let msg = ` * *\n\n`;
            if (metrics.errors.length === 0) {
              msg += `_ _ `;
            } else {
              const recentErrors = metrics.errors.slice(-10);
              for (const err of recentErrors) {
                const time = new Date(err.time).toLocaleTimeString('ru-RU');
                msg += ` ${time} - ${err.endpoint}\n`;
                msg += `\`${err.error.substring(0, 50)}\`\n\n`;
              }
            }
            try {
              await editTelegramMessage(env, chatId, messageId, msg, {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: ' ', callback_data: 'cp_logs' }],
                    [{ text: ' ', callback_data: 'adm_back' }]
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
              const msg = ` *Webhook Info*\n\n` +
                `URL: \`${webhookInfo.url || ' '}\`\n` +
                `Pending: ${webhookInfo.pending_update_count || 0}\n` +
                `Last Error: ${webhookInfo.last_error_message || ''}\n` +
                `Max Connections: ${webhookInfo.max_connections || 40}`;
              await editTelegramMessage(env, chatId, messageId, msg, {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: ' ', callback_data: 'cp_webhook' }],
                    [{ text: ' ', callback_data: 'adm_back' }]
                  ]
                }
              });
            } catch (err) {
              const stats = await getAdminStats(env);
              const pendingGroups = await getAllPendingGroups(env);
              await handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups);
            }
          } catch (err) {
            await answerCallbackQuery(env, callback.id, '  webhook info', true);
          }
          break;
        }
        
        case 'cp_logout': {
          await destroyAdminSession(env, telegramId);
          await editTelegramMessage(env, chatId, messageId,
            ` * *\n\n` +
            `    .`,
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
        
        // ===   - (adm_*) ===
        case 'adm_pending': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const pendingGroups = await getAllPendingGroups(env);
          if (pendingGroups.length === 0) {
            await editTelegramMessage(env, chatId, messageId,
              ` *  *\n\n` +
              `     .`,
              { reply_markup: { inline_keyboard: [[{ text: ' ', callback_data: 'adm_back' }]] }}
            );
            break;
          }
          
          let msg = ` *   (${pendingGroups.length})*\n` +
            `\n\n`;
          const buttons = [];
          
          for (const group of pendingGroups.slice(0, 5)) {
            const requestDate = new Date(group.requestedAt).toLocaleDateString('ru-RU');
            const safeTitle = escapeMarkdown(group.chatTitle || ' ');
            msg += ` *${safeTitle}*\n`;
            msg += ` ID: \`${group.chatId}\`\n`;
            msg += ` : @${group.requestedByUsername || 'unknown'}\n`;
            msg += ` TG ID: \`${group.requestedBy}\`\n`;
            msg += ` : ${requestDate}\n\n`;
            
            buttons.push([
              { text: ` `, callback_data: `approve_group:${group.chatId}` },
              { text: ` `, callback_data: `reject_group:${group.chatId}` }
            ]);
          }
          
          if (pendingGroups.length > 5) {
            msg += `\n_...   ${pendingGroups.length - 5} _`;
          }
          
          buttons.push([{ text: ' ', callback_data: 'adm_back' }]);
          
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
          
          const msg = ` * *\n` +
            `\n\n` +
            `* :*\n` +
            ` \`trial\`   (1 )\n` +
            ` \`level1\`   1 ( )\n` +
            ` \`level2\`   2 ( + )\n` +
            ` \`level3\`   3 ()\n\n` +
            `*:*\n` +
            `\`/grant ID  \`\n\n` +
            `*:*\n` +
            `\`/grant 123456789 30 level1\`\n` +
            `\`/grant 123456789 30 level2\`\n` +
            `\`/grant 123456789 30 level3\``;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: ' ', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_search': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const msg = ` * *\n` +
            `\n\n` +
            ` :\n` +
            `\`/check 123456789\`\n\n` +
            `   .`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: ' ', callback_data: 'adm_back' }]
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
          
          let msg = ` * *\n` +
            `\n\n`;
          
          if (users.length === 0) {
            msg += `_  _`;
          } else {
            for (const user of users) {
              const daysLeft = Math.ceil((user.expiresAt - now) / (24 * 60 * 60 * 1000));
              const status = user.isActive ? '' : '';
              const typeInfo = LICENSE_TYPES[user.type] || { name: user.type };
              msg += `${status} \`${user.telegramId}\`\n`;
              msg += `   ${typeInfo.name}  ${daysLeft > 0 ? daysLeft + '' : ''}\n`;
            }
          }
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: ' ', callback_data: 'adm_users' }],
              [{ text: ' ', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_chats': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          //   
          const groupOwnersList = await env.USERS.list({ prefix: 'group_owner:', limit: 20 });
          
          let msg = ` * *\n` +
            `\n\n`;
          
          if (groupOwnersList.keys.length === 0) {
            msg += `_  _`;
          } else {
            for (const key of groupOwnersList.keys) {
              const chatId2 = key.name.replace('group_owner:', '');
              const ownerData = await env.USERS.get(key.name);
              if (ownerData) {
                const owner = JSON.parse(ownerData);
                msg += ` \`${chatId2}\`\n`;
                msg += `   : \`${owner.telegramId || 'unknown'}\`\n`;
              }
            }
          }
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: ' ', callback_data: 'adm_chats' }],
              [{ text: ' ', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_broadcast': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const msg = ` **\n` +
            `\n\n` +
            ` :\n` +
            `\`/broadcast  \`\n\n` +
            ` _    _`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: ' ', callback_data: 'adm_back' }]
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
          
          const msg = ` * *\n` +
            `\n\n` +
            ` *:*\n` +
            ` : ${stats.totalUsers}\n` +
            ` : ${stats.activeUsers}\n` +
            `  Trial: ${stats.trialUsers}\n` +
            `  Level 1: ${stats.level1Users}\n` +
            `  Level 2: ${stats.level2Users}\n` +
            `  Level 3: ${stats.level3Users}\n\n` +
            ` *:*\n` +
            ` : ${stats.pendingGroups}\n` +
            ` : ${stats.approvedGroups}\n\n` +
            ` *:*\n` +
            ` Uptime: ${uptimeHours}\n` +
            ` : ${metrics.requests.total}\n` +
            ` : ${metrics.requests.error}`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: ' ', callback_data: 'adm_stats' }],
              [{ text: ' ', callback_data: 'adm_back' }]
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
          await answerCallbackQuery(env, callback.id, ' ');
          break;
        }
        
        case 'adm_logout': {
          await destroyAdminSession(env, telegramId);
          await editTelegramMessage(env, chatId, messageId,
            ` * *\n\n` +
            `   -.`,
            { reply_markup: { inline_keyboard: [[{ text: ' ', callback_data: 'menu' }]] }}
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
          
          const msg = ` **\n` +
            `\n\n` +
            ` *:*\n` +
            ` : ${BOT_VERSION}\n` +
            ` : ${EXTENSION_VERSION}\n\n` +
            ` *Uptime:* ${uptimeHours} ${uptimeMinutes}\n\n` +
            ` *:*\n` +
            ` LOG\\_CHAT: ${env.LOG_CHAT_ID ? '' : ''}\n` +
            ` ADMIN\\_TG\\_ID: ${env.ADMIN_TELEGRAM_ID ? '' : ''}\n` +
            ` WEBHOOK\\_SECRET: ${env.TELEGRAM_WEBHOOK_SECRET ? '' : ''}\n\n` +
            ` *API:*\n` +
            ` : ${metrics.requests.total}\n` +
            ` : ${metrics.requests.error}`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '  ', callback_data: 'cp_logs' }],
              [{ text: ' Webhook Info', callback_data: 'cp_webhook' }],
              [{ text: ' ', callback_data: 'adm_system' }],
              [{ text: ' ', callback_data: 'adm_back' }]
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
        
        // ===   CALLBACK ( ) ===
        case 'admin_back': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, ' ', true);
            break;
          }
          
          const adminMsg = ` *-*

/grant ID 7  
/revoke ID  
/check ID  `;
          
          await editTelegramMessage(env, chatId, messageId, adminMsg, {
            reply_markup: {
              inline_keyboard: [
                [
                  { text: ' ', callback_data: 'admin_check_prompt' },
                  { text: ' ', callback_data: 'admin_grant_prompt' }
                ],
                [{ text: ' ', callback_data: 'admin_stats' }],
                [{ text: ' ', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'admin_check_prompt': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, ' ', true);
            break;
          }
          
          await editTelegramMessage(env, chatId, messageId, 
            ` * *\n\n` +
            ` :\n` +
            `\`/check 123456789\`   ID`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: ' ', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        case 'admin_grant_prompt': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, ' ', true);
            break;
          }
          
          await editTelegramMessage(env, chatId, messageId, 
            ` * *\n\n` +
            `\`/grant ID 7\`  7 \n` +
            `\`/grant ID 30\`  30 \n` +
            `\`/grant 123456 90 level2\`   ID`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: ' ', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        case 'admin_users': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, ' ', true);
            break;
          }
          
          await editTelegramMessage(env, chatId, messageId, 
            ` **\n\n` +
            `\`/check 123456789\`   ID`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: ' ', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        case 'admin_stats': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, ' ', true);
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
            ` * *\n\n` +
            ` @${botUsername.replace(/_/g, '\\_')}\n` +
            ` : ${BOT_VERSION}\n` +
            ` ${now} MSK\n` +
            ` KV: `,
            { reply_markup: {
              inline_keyboard: [
                [{ text: ' ', callback_data: 'admin_stats' }],
                [{ text: ' ', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        default: {
          //  remove_device:deviceId
          if (data.startsWith('remove_device:')) {
            const deviceIdPart = data.split(':')[1];
            
            //     ID
            const devicesData = await getUserDevices(env, telegramId);
            const devices = devicesData.devices || [];
            const device = devices.find(d => d.deviceId.startsWith(deviceIdPart));
            
            if (device) {
              const result = await removeDevice(env, telegramId, device.deviceId);
              if (result.success) {
                await answerCallbackQuery(env, callback.id, '  ', false);
              } else {
                await answerCallbackQuery(env, callback.id, ' ', true);
              }
            } else {
              await answerCallbackQuery(env, callback.id, '   ', true);
            }
            
            //   
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
            
            let message = ` * * (${newDevices.length}/${maxDevices})\n\n`;
            
            if (newDevices.length === 0) {
              message += `_  _`;
            } else {
              newDevices.forEach((d, index) => {
                const shortId = d.deviceId.substring(0, 12) + '...';
                const addedDate = new Date(d.addedAt).toLocaleDateString('ru-RU');
                message += `${index + 1}. \`${shortId}\`\n`;
                message += `    : ${addedDate}\n\n`;
              });
            }
            
            const keyboard = { inline_keyboard: [] };
            newDevices.forEach((d, index) => {
              keyboard.inline_keyboard.push([
                { text: `   ${index + 1}`, callback_data: `remove_device:${d.deviceId.substring(0, 32)}` }
              ]);
            });
            keyboard.inline_keyboard.push([{ text: ' ', callback_data: 'status' }]);
            
            await editTelegramMessage(env, chatId, messageId, message, { reply_markup: keyboard });
            break;
          }
          
          //  approve_group:chatId  reject_group:chatId
          if (data.startsWith('approve_group:') || data.startsWith('reject_group:')) {
            //   
            if (!await checkAndExtendAdminSession(env, telegramId)) {
              await showSessionExpiredMessage(env, chatId, messageId);
              break;
            }
            
            const groupChatId = data.split(':')[1];
            const isApprove = data.startsWith('approve_group:');
            
            if (isApprove) {
              const result = await approveGroupChat(env, groupChatId, username);
              
              if (result.success) {
                //  
                if (result.telegramId) {
                  await sendTelegramMessage(env, result.telegramId,
                    ` * !*\n\n` +
                    `    .\n` +
                    `        .`
                  );
                }
                
                //   
                await sendTelegramMessage(env, groupChatId,
                  ` * !*\n\n` +
                  `      .`
                );
                
                await editTelegramMessage(env, chatId, messageId,
                  ` * *\n\n` +
                  `Chat ID: \`${groupChatId}\`\n` +
                  `: @${username}`,
                  { reply_markup: { inline_keyboard: [[{ text: ' -', callback_data: 'adm_back' }]] }}
                );
              } else {
                await sendTelegramMessage(env, chatId, 
                  ` : ${result.error}`
                );
              }
            } else {
              // 
              const pendingKey = `pending_group:${groupChatId}`;
              const pendingData = await env.USERS.get(pendingKey);
              
              if (pendingData) {
                const request = JSON.parse(pendingData);
                await env.USERS.delete(pendingKey);
                
                //  
                if (request.requestedBy) {
                  await sendTelegramMessage(env, request.requestedBy,
                    ` * *\n\n` +
                    `      .\n` +
                    `   @YaMob`
                  );
                }
                
                //   
                await sendTelegramMessage(env, groupChatId,
                  ` * *\n\n` +
                  `    .`
                );
                
                await editTelegramMessage(env, chatId, messageId,
                  ` * *\n\n` +
                  `Chat ID: \`${groupChatId}\`\n` +
                  `: @${username}`,
                  { reply_markup: { inline_keyboard: [[{ text: ' -', callback_data: 'adm_back' }]] }}
                );
              } else {
                await sendTelegramMessage(env, chatId, '  ');
              }
            }
            break;
          }
          
          //   callback  
          if (data.startsWith('grant_') || data.startsWith('revoke_')) {
            const callbackTelegramId = String(callback.from.id);
            
            if (!await isAdmin(callbackTelegramId)) {
              await answerCallbackQuery(env, callback.id, ' ', true);
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
                ` **\n\n` +
                ` ${target}\n` +
                ` +${days} .  ${daysLeft} .\n` +
                ` : ${expiresDate}`
              );
              
              // 
              await logToChat(env,
                ` **`,
                'license',
                {
                  telegramId: isTelegramId ? target : '',
                  username: isTelegramId ? '' : target,
                  action: `+${days} .`,
                  details: `@${username}`
                }
              );
              
              //  
              if (isTelegramId) {
                try {
                  await sendTelegramMessage(env, target, 
                    ` * +${days} .*\n` +
                    ` : ${expiresDate}`
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
                ` **\n\n ${isTelegramId ? target : '@' + target}`
              );
              
              // 
              await logToChat(env,
                ` **`,
                'admin',
                {
                  telegramId: isTelegramId ? target : '',
                  username: isTelegramId ? '' : target,
                  action: '',
                  details: `@${username}`
                }
              );
            }
          } else {
            await answerCallbackQuery(env, callback.id, ' ', true);
          }
        }
      }
      
      return jsonResponse({ ok: true });
    }
    
    // ===    ===
    // isAdmin    (  telegramId)
    
    // Rate limiting   
    if (update.message) {
      const msgTelegramId = String(update.message.from.id);
      
      //   
      if (!await isAdmin(msgTelegramId)) {
        const rateCheck = checkBotRateLimit(msgTelegramId, 'messages');
        if (!rateCheck.allowed) {
          //    ,  
          return jsonResponse({ ok: true });
        }
      }
    }
    
    // ==========  - ==========
    //     (   )
    // : env.ADMIN_SECRET_PHRASE,  fallback 
    const adminSecretPhrase = env.ADMIN_SECRET_PHRASE || 'flamingo1000';
    if (update.message?.text === adminSecretPhrase) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      //   Telegram ID (    username)
      const isAdminUser = await isAdminById(env, telegramId);
      
      if (!isAdminUser) {
        //       -  
        //     
        await sendTelegramMessage(env, chatId, 
          `  .\n\n  :`,
          { reply_markup: getMainMenuKeyboard() }
        );
        return jsonResponse({ ok: true });
      }
      
      //  -
      await createAdminSession(env, telegramId);
      
      //      ( )
      try {
        await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, message_id: update.message.message_id })
        });
      } catch (e) {}
      
      //    pending 
      const stats = await getAdminStats(env);
      const pendingGroups = await getAllPendingGroups(env);
      
      const adminMsg = ` *CONTROL PANEL v${BOT_VERSION}*\n\n` +
        ` * :* ${stats.activeUsers}\n` +
        `  Trial: ${stats.trialUsers}\n` +
        `  Level 1: ${stats.level1Users}\n` +
        `  Level 2: ${stats.level2Users}\n` +
        `  Level 3: ${stats.level3Users}\n\n` +
        ` : ${stats.approvedGroups} . / ${stats.pendingGroups} .\n\n` +
        `_  1 _`;
      
      const keyboard = {
        inline_keyboard: [
          [
            { text: '   (' + pendingGroups.length + ')', callback_data: 'cp_pending_groups' }
          ],
          [
            { text: ' ', callback_data: 'cp_users' },
            { text: ' ', callback_data: 'cp_stats' }
          ],
          [
            { text: '  ', callback_data: 'cp_grant' },
            { text: ' ', callback_data: 'cp_check' }
          ],
          [
            { text: ' ', callback_data: 'cp_broadcast' },
            { text: ' ', callback_data: 'cp_system' }
          ],
          [
            { text: ' ', callback_data: 'cp_logout' }
          ]
        ]
      };
      
      await sendTelegramMessage(env, chatId, adminMsg, { reply_markup: keyboard });
      return jsonResponse({ ok: true });
    }
    
    //   /admin   
    if (update.message?.text === '/admin') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        //     
        await sendTelegramMessage(env, chatId, 
          `  .\n\n  :`,
          { reply_markup: getMainMenuKeyboard() }
        );
        return jsonResponse({ ok: true });
      }
      
      //     
      await sendTelegramMessage(env, chatId, 
        ` *- *\n\n` +
        `    .\n` +
        `_  /grant, /check, /revoke   ._`
      );
      return jsonResponse({ ok: true });
    }
    
    //  /grant -  
    if (update.message?.text?.startsWith('/grant')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '  ');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 3) {
        await sendTelegramMessage(env, chatId, 
          ` *:* \`/grant @user  []\`\n\n` +
          `*:* trial, level1, level2, level3\n` +
          ` : level1\n\n` +
          `*:*\n` +
          `\`/grant @username 30\`  level1\n` +
          `\`/grant @username 30 level2\`\n` +
          `\`/grant 123456789 7 level3\``
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      const days = parseInt(parts[2]);
      const licenseType = parts[3] || 'level1';
      
      if (isNaN(days) || days <= 0) {
        await sendTelegramMessage(env, chatId, '   ( > 0)');
        return jsonResponse({ ok: true });
      }
      
      //    
      const validTypes = ['trial', 'level1', 'level2', 'level3'];
      if (!validTypes.includes(licenseType)) {
        await sendTelegramMessage(env, chatId, 
          `  : \`${licenseType}\`\n\n` +
          `: ${validTypes.join(', ')}`
        );
        return jsonResponse({ ok: true });
      }
      
      //   ( ID  username)
      const userInfo = await getUserInfo(env, target);
      
      //  telegramId  
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
          //     username -    username 
          // (    )
          await sendTelegramMessage(env, chatId, 
            ` * @${target}  *\n\n` +
            `    username.\n` +
            `     .`
          );
        }
      }
      
      //   
      let currentLicense = null;
      if (targetTelegramId) {
        const existing = await env.USERS.get(`license:tg:${targetTelegramId}`);
        if (existing) currentLicense = JSON.parse(existing);
      }
      if (!currentLicense && targetUsername) {
        const existing = await env.USERS.get(`license:${targetUsername.toLowerCase()}`);
        if (existing) currentLicense = JSON.parse(existing);
      }
      
      //   
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
      
      //  
      if (targetTelegramId) {
        await env.USERS.put(`license:tg:${targetTelegramId}`, JSON.stringify(licenseData));
        
        //   username  telegramId
        if (targetUsername) {
          await saveUsernameMapping(env, targetUsername, targetTelegramId);
        }
        
        //    -     
        //      
        if (licenseType !== 'trial' && (!currentLicense || currentLicense.type === 'trial')) {
          const bonusResult = await grantReferralBonusToReferrer(env, targetTelegramId);
          if (bonusResult.success) {
            //     
            try {
              await sendTelegramMessage(env, bonusResult.referrerTelegramId, 
                ` * !*\n\n` +
                `   !\n` +
                `   +1   .`
              );
            } catch (e) {}
            
            // 
            await logToChat(env,
              ` *. *\n\n` +
              `: ${bonusResult.referrerTelegramId}\n` +
              `: ${targetTelegramId}`,
              'success',
              { action: '  ' }
            );
          }
        }
      } else if (targetUsername) {
        //   username   
        await env.USERS.put(`license:${targetUsername.toLowerCase()}`, JSON.stringify(licenseData));
      }
      
      const daysLeft = Math.ceil((expiresAt - Date.now()) / (24 * 60 * 60 * 1000));
      const expiresDate = new Date(expiresAt).toLocaleDateString('ru-RU');
      const typeName = LICENSE_TYPES[licenseType]?.name || licenseType;
      
      const displayUser = targetUsername ? `@${targetUsername}` : targetTelegramId;
      
      await sendTelegramMessage(env, chatId, 
        ` **\n\n` +
        ` ${displayUser}\n` +
        ` ID: ${targetTelegramId || ' '}\n` +
        ` ${typeName}\n` +
        ` +${days} .  ${daysLeft} .\n` +
        ` : ${expiresDate}`
      );
      
      // 
      await logToChat(env,
        ` **`,
        'license',
        {
          telegramId: targetTelegramId || '',
          username: targetUsername || '',
          action: `+${days} . (${typeName})`,
          details: `@${username}`
        }
      );
      
      //  
      if (targetTelegramId) {
        try {
          await sendTelegramMessage(env, targetTelegramId, 
            ` * +${days} .*\n` +
            ` : ${expiresDate}`
          );
        } catch (e) {}
      }
      
      return jsonResponse({ ok: true });
    }
    
    //  /revoke -  
    if (update.message?.text?.startsWith('/revoke')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '  ');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          ` : \`/revoke @user\`  \`/revoke ID\``
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      
      //   ( ID  username)
      const userInfo = await getUserInfo(env, target);
      const isTelegramId = /^\d+$/.test(target);
      
      //  
      let deleted = false;
      if (userInfo.found && userInfo.telegramId) {
        await env.USERS.delete(`license:tg:${userInfo.telegramId}`);
        deleted = true;
      } else if (isTelegramId) {
        await env.USERS.delete(`license:tg:${target}`);
        deleted = true;
      } else {
        await env.USERS.delete(`license:${target.toLowerCase()}`);
        deleted = true;
      }
      
      const displayUser = userInfo.found ? 
        `@${userInfo.username || target} (ID: ${userInfo.telegramId})` : 
        (isTelegramId ? target : `@${target}`);
      
      await sendTelegramMessage(env, chatId, 
        ` **\n\n ${displayUser}`
      );
      
      // 
      await logToChat(env,
        ` **`,
        'admin',
        {
          telegramId: userInfo.telegramId || (isTelegramId ? target : ''),
          username: userInfo.username || (!isTelegramId ? target : ''),
          action: '',
          details: `ID: ${telegramId}`
        }
      );
      
      return jsonResponse({ ok: true });
    }
    
    //  /check -  
    if (update.message?.text?.startsWith('/check')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '  ');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          ` : \`/check @user\`  \`/check ID\``
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      
      //   ( ID  username)
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
          ` *  *\n\n` +
          ` ${displayUser}\n` +
          ` ID: ${userInfo.telegramId || (isTelegramId ? target : '')}\n\n` +
          (userInfo.found ? `  ,   ` : `     `),
          { reply_markup: {
            inline_keyboard: [
              [{ text: ' ', callback_data: 'admin_back' }]
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
      
      // -
      const maxTime = 30 * 24 * 60 * 60 * 1000;
      const percent = isValid ? Math.min(100, Math.round((timeLeft / maxTime) * 100)) : 0;
      const bar = getProgressBar(percent);
      
      const emoji = isValid ? getStatusEmoji(daysLeft, licenseData.type) : '';
      const typeText = LICENSE_TYPES[licenseData.type]?.name || '';
      
      //    
      const targetTgId = userInfo.telegramId || (isTelegramId ? target : null);
      let devicesInfo = '';
      if (targetTgId) {
        const devicesData = await env.USERS.get(`devices:${targetTgId}`);
        if (devicesData) {
          const devices = JSON.parse(devicesData);
          devicesInfo = `\n : ${devices.length}`;
        }
      }
      
      const checkMsg = ` **\n\n` +
        ` ${displayUser}\n` +
        ` ID: ${targetTgId || ''}\n\n` +
        `${emoji} ${typeText}  ${isValid ? '' : ''}\n` +
        `${bar} ${percent}%\n\n` +
        ` ${daysLeft} .  : ${expiresDate}` +
        devicesInfo +
        (licenseData.grantedBy ? `\n : @${licenseData.grantedBy}` : '');
      
      const checkTargetId = targetTgId || target;
      
      await sendTelegramMessage(env, chatId, checkMsg, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: ' +7', callback_data: `grant_${checkTargetId}_7` },
              { text: ' +30', callback_data: `grant_${checkTargetId}_30` },
              { text: ' +90', callback_data: `grant_${checkTargetId}_90` }
            ],
            [{ text: ' ', callback_data: `revoke_${checkTargetId}` }],
            [{ text: ' ', callback_data: 'admin_back' }]
          ]
        }
      });
      
      return jsonResponse({ ok: true });
    }
    
    //  /users
    if (update.message?.text === '/users') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '  ');
        return jsonResponse({ ok: true });
      }
      
      await sendTelegramMessage(env, chatId, 
        ` **\n\n` +
        `\`/check @user\`    username\n` +
        `\`/check ID\`    ID`
      );
      
      return jsonResponse({ ok: true });
    }
    
    //  /stats
    if (update.message?.text === '/stats') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '  ');
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
        ` * *\n\n` +
        ` @${botUsername.replace(/_/g, '\\_')}\n` +
        ` : ${BOT_VERSION}\n` +
        ` ${now} MSK\n` +
        ` KV: `
      );
      
      return jsonResponse({ ok: true });
    }
    
    //  /broadcast -     ( )
    if (update.message?.text?.startsWith('/broadcast')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      //    ID
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '  ');
        return jsonResponse({ ok: true });
      }
      
      const text = update.message.text.replace('/broadcast', '').trim();
      
      if (!text) {
        await sendTelegramMessage(env, chatId, 
          ` **\n\n` +
          `: \`/broadcast  \`\n\n` +
          ` _     _`
        );
        return jsonResponse({ ok: true });
      }
      
      //      
      const users = await getAllLicensedUsers(env, 1000);
      const activeUsers = users.filter(u => u.isActive);
      
      let sent = 0;
      let failed = 0;
      
      //    
      await sendTelegramMessage(env, chatId, 
        ` * ...*\n\n` +
        ` : ${activeUsers.length}`
      );
      
      //        Telegram
      for (const user of activeUsers) {
        try {
          await sendTelegramMessage(env, user.telegramId, 
            ` **\n\n${text}`,
            { reply_markup: getMainMenuKeyboard() }
          );
          sent++;
          //  50  
          await new Promise(r => setTimeout(r, 50));
        } catch (e) {
          failed++;
        }
      }
      
      await sendTelegramMessage(env, chatId, 
        ` * *\n\n` +
        ` : ${sent}\n` +
        ` : ${failed}`
      );
      
      // 
      await logToChat(env,
        ` **\n\n` +
        ` : @${username}\n` +
        ` : ${sent}\n` +
        ` : ${failed}`,
        'admin'
      );
      
      return jsonResponse({ ok: true });
    }
    
    // ==========     ==========
    
    //  /mystats -  
    if (update.message?.text === '/mystats') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      const stats = await getUserStats(env, telegramId);
      const oderId = await env.USERS.get(`tg:${telegramId}`);
      
      let licenseInfo = '  ';
      if (oderId) {
        const result = await getLicenseData(env, oderId);
        if (result.license) {
          const daysLeft = Math.max(0, Math.ceil((result.license.expiresAt - Date.now()) / MS_PER_DAY));
          const isActive = result.license.expiresAt > Date.now();
          const typeText = LICENSE_TYPES[result.license.type]?.name || result.license.type;
          licenseInfo = isActive 
            ? ` ${typeText} (${daysLeft} ${getDaysWord(daysLeft)})`
            : ` ${typeText} ()`;
        }
      }
      
      const referralCode = await getUserReferralCode(env, telegramId);
      const paidBonuses = stats.referralPaidBonus || 0;
      const pendingBonuses = stats.referralPendingBonus || 0;
      
      const message = ` * *\n\n` +
        ` * :* ${stats.todayClicks || 0}\n` +
        ` * :* ${stats.totalClicks || 0}\n\n` +
        ` *:* ${licenseInfo}\n\n` +
        ` *:* ${stats.referralCount || 0}\n` +
        ` * :* ${paidBonuses} .\n` +
        (pendingBonuses > 0 ? ` * :* ${pendingBonuses}\n` : '') +
        ` * :* \`${referralCode}\`\n\n` +
        `_ +1     !_`;
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: {
          inline_keyboard: [
            [{ text: '  ', callback_data: 'share_ref' }],
            [
              { text: ' ', callback_data: 'mystats' },
              { text: ' ', callback_data: 'menu' }
            ]
          ]
        }
      });
      return jsonResponse({ ok: true });
    }
    
    //  /ref -   
    if (update.message?.text?.startsWith('/ref')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const parts = update.message.text.split(' ');
      
      if (parts.length < 2) {
        const myCode = await getUserReferralCode(env, telegramId);
        await sendTelegramMessage(env, chatId, 
          ` * *\n\n` +
          `* :* \`${myCode}\`\n\n` +
          `   :\n` +
          `\`/ref \`\n\n` +
          `_  1  ._\n` +
          `_  +1 ,    !_`
        );
        return jsonResponse({ ok: true });
      }
      
      const refCode = parts[1].toUpperCase();
      
      //   
      if (!refCode.startsWith(REFERRAL_CODE_PREFIX)) {
        await sendTelegramMessage(env, chatId, 
          ` *  *\n\n` +
          `    \`${REFERRAL_CODE_PREFIX}\``
        );
        return jsonResponse({ ok: true });
      }
      
      const result = await applyReferralCode(env, telegramId, refCode);
      
      if (result.success) {
        //   
        if (result.gaveTrialToNewUser) {
          await sendTelegramMessage(env, chatId, 
            ` *  !*\n\n` +
            `    !\n` +
            `   1   !`
          );
        } else {
          await sendTelegramMessage(env, chatId, 
            ` *  !*\n\n` +
            `    !\n` +
            `_  , ..      ._`
          );
        }
        
        //   -  
        try {
          await sendTelegramMessage(env, result.referrerTelegramId, 
            ` * !*\n\n` +
            `     !\n\n` +
            ` * +1 *  ,\n` +
            `    .`
          );
        } catch (e) {}
        
        // 
        await logToChat(env,
          ` **\n\n` +
          `: ${telegramId}\n` +
          `: ${result.referrerTelegramId}`,
          'success',
          { action: ' ', details: refCode }
        );
      } else {
        const errorMessages = {
          'INVALID_CODE': '  ',
          'SELF_REFERRAL': '   ',
          'ALREADY_REFERRED': '    '
        };
        await sendTelegramMessage(env, chatId, 
          ` **\n\n${errorMessages[result.error] || result.error}`
        );
      }
      
      return jsonResponse({ ok: true });
    }
    
    // ==========    ==========
    
    //  /approve_group -    ()
    if (update.message?.text?.startsWith('/approve_group')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '  ');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          ` : \`/approve_group chatId\``
        );
        return jsonResponse({ ok: true });
      }
      
      const targetChatId = parts[1];
      const result = await approveGroupChat(env, targetChatId, telegramId);
      
      if (result.success) {
        await sendTelegramMessage(env, chatId, 
          ` *  !*\n\n` +
          ` Chat ID: \`${targetChatId}\`\n` +
          ` : ${result.telegramId}`
        );
        
        //  
        try {
          await sendTelegramMessage(env, result.telegramId, 
            ` *   !*\n\n` +
            `      .`
          );
        } catch (e) {}
        
        //    
        try {
          await sendTelegramMessage(env, targetChatId, 
            ` *Exotic Assistant !*\n\n` +
            `        .`
          );
        } catch (e) {}
      } else {
        await sendTelegramMessage(env, chatId, 
          ` : ${result.error}`
        );
      }
      return jsonResponse({ ok: true });
    }
    
    //  /devices -   ( )
    if (update.message?.text === '/devices') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      //  
      const existingOderId = await env.USERS.get(`tg:${telegramId}`);
      if (!existingOderId) {
        await sendTelegramMessage(env, chatId, 
          ` *  *\n\n` +
          `    .`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '  ', callback_data: 'how_to_connect' }],
              [{ text: ' ', callback_data: 'menu' }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      //      
      const licenseData = await env.USERS.get(`license:tg:${telegramId}`);
      const license = licenseData ? JSON.parse(licenseData) : null;
      const licenseType = LICENSE_TYPES[license?.type] || LICENSE_TYPES.trial;
      const maxDevices = licenseType.maxDevices || 1;
      
      const devicesData = await getUserDevices(env, telegramId);
      const devices = devicesData.devices || [];
      
      let message = ` * *\n\n`;
      message += ` : *${licenseType.name}*\n`;
      message += ` : ${devices.length}/${maxDevices}\n\n`;
      
      if (devices.length === 0) {
        message += `_  _\n\n`;
        message += `  \n   .`;
      } else {
        message += `* :*\n`;
        devices.forEach((d, i) => {
          const addedDate = new Date(d.addedAt).toLocaleDateString('ru-RU');
          const lastSeenDate = new Date(d.lastSeen).toLocaleDateString('ru-RU');
          const deviceShort = d.deviceId.substring(0, 12) + '...';
          message += `\n${i + 1}. \`${deviceShort}\`\n`;
          message += `    : ${addedDate}\n`;
          message += `    : ${lastSeenDate}`;
        });
      }
      
      //  
      const buttons = [];
      if (devices.length > 0) {
        buttons.push([{ text: '   ', callback_data: 'clear_devices' }]);
      }
      if (maxDevices < 2) {
        buttons.push([{ text: '  ', callback_data: 'buy' }]);
      }
      buttons.push([{ text: ' ', callback_data: 'menu' }]);
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: { inline_keyboard: buttons }
      });
      
      return jsonResponse({ ok: true });
    }
    
    //  /groups -   ( )
    if (update.message?.text === '/groups') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      const canUse = await canUseGroupChats(env, telegramId);
      
      if (!canUse.allowed) {
        let errorMsg = '';
        switch (canUse.reason) {
          case 'NOT_CONNECTED':
            errorMsg = '   ';
            break;
          case 'NO_LICENSE':
            errorMsg = '   ';
            break;
          case 'UPGRADE_REQUIRED':
            errorMsg = `      *Premium*  **\n\n` +
                       ` : ${canUse.currentType || ''}`;
            break;
          default:
            errorMsg = '     ';
        }
        
        await sendTelegramMessage(env, chatId, errorMsg, {
          reply_markup: {
            inline_keyboard: [
              [{ text: '  ', callback_data: 'buy' }],
              [{ text: ' ', callback_data: 'menu' }]
            ]
          }
        });
        return jsonResponse({ ok: true });
      }
      
      const groupData = await getUserGroupChats(env, telegramId);
      
      let message = ` * *\n\n`;
      
      if (groupData.chats.length === 0) {
        message += `     .\n\n`;
        message += `* :*\n`;
        message += `1   @clickuved\\_bot  \n`;
        message += `2    \n`;
        message += `3       `;
      } else {
        message += `*  (${groupData.chats.length}):*\n\n`;
        groupData.chats.forEach((chat, i) => {
          message += `${i + 1}. ${chat.chatTitle}\n`;
        });
        message += `\n_: ${canUse.maxChats} _`;
      }
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: {
          inline_keyboard: [
            [{ text: ' ', callback_data: 'group_help' }],
            [{ text: ' ', callback_data: 'menu' }]
          ]
        }
      });
      
      return jsonResponse({ ok: true });
    }
    
    //  /grant_premium -  Premium  ( )
    if (update.message?.text?.startsWith('/grant_premium')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '  ');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 3) {
        await sendTelegramMessage(env, chatId, 
          ` : \`/grant_premium ID \`\n\n` +
          `: level2, level3`
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      const days = parseInt(parts[2]);
      const type = parts[3] || 'level2'; // level2  level3
      
      if (isNaN(days) || days <= 0) {
        await sendTelegramMessage(env, chatId, '   ( > 0)');
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
        type: type, // 'premium'  'max'
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
        ` *${typeInfo.name} !*\n\n` +
        ` ${isTelegramId ? target : '@' + target}\n` +
        ` +${days} .  ${daysLeft} .\n` +
        ` : ${typeInfo.groupChats ? '' : ''}\n` +
        ` : ${typeInfo.maxDevices}`
      );
      
      return jsonResponse({ ok: true });
    }
    
    //     
    if (update.my_chat_member) {
      const member = update.my_chat_member;
      const chat = member.chat;
      const newStatus = member.new_chat_member.status;
      const fromUser = member.from;
      
      //    
      if (chat.type === 'group' || chat.type === 'supergroup') {
        if (newStatus === 'member' || newStatus === 'administrator') {
          const telegramId = String(fromUser.id);
          const username = fromUser.username;
          
          //     
          const canUse = await canUseGroupChats(env, telegramId);
          
          if (!canUse.allowed) {
            await sendTelegramMessage(env, chat.id, 
              ` *  *\n\n` +
              `@${username || telegramId},      ` +
              `  Premium  .\n\n` +
              `    .`
            );
            return jsonResponse({ ok: true });
          }
          
          //   
          const groupData = await getUserGroupChats(env, telegramId);
          if (groupData.chats.length >= canUse.maxChats) {
            await sendTelegramMessage(env, chat.id, 
              ` *  *\n\n` +
              `   ${groupData.chats.length}/${canUse.maxChats} .\n` +
              `     /groups`
            );
            return jsonResponse({ ok: true });
          }
          
          //   
          const result = await requestGroupChatApproval(env, telegramId, chat.id, chat.title, username);
          
          if (result.success) {
            await sendTelegramMessage(env, chat.id, 
              ` * *\n\n` +
              `   .\n` +
              `    .`
            );
          } else if (result.error === 'REQUEST_PENDING') {
            await sendTelegramMessage(env, chat.id, 
              ` *  *\n\n` +
              `  .`
            );
          }
          
          return jsonResponse({ ok: true });
        }
        
        //    
        if (newStatus === 'left' || newStatus === 'kicked') {
          //     
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
    
    // ===      ===
    if (update.message && (update.message.chat.type === 'group' || update.message.chat.type === 'supergroup')) {
      const chat = update.message.chat;
      const chatId = chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const text = update.message.text || '';
      const botUsername = env.BOT_USERNAME || 'clickuved_bot';
      
      //   username  telegramId
      if (username) {
        await saveUsernameMapping(env, username, telegramId);
      }
      
      //  -      
      const isDirectCommand = text.startsWith('/') && (
        text.includes(`@${botUsername}`) || 
        !text.includes('@') //   @   
      );
      const isBotMention = text.toLowerCase().includes(`@${botUsername.toLowerCase()}`);
      const isReplyToBot = update.message.reply_to_message?.from?.username === botUsername;
      
      //      - 
      if (!isDirectCommand && !isBotMention && !isReplyToBot) {
        return jsonResponse({ ok: true });
      }
      
      //  @username  
      const cleanText = text.replace(new RegExp(`@${botUsername}`, 'gi'), '').trim();
      
      //  -     
      const groupOwner = await env.USERS.get(`group_owner:${chatId}`);
      
      //  /start   -  
      if (cleanText.startsWith('/start')) {
        if (!groupOwner) {
          await sendTelegramMessage(env, chatId, 
            ` *Exotic Assistant*\n\n` +
            `     \n` +
            `    .\n\n` +
            `   \n` +
            ` *Level 2*  .\n\n` +
            `   :\n@${botUsername}`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '  ', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
        } else {
          await sendTelegramMessage(env, chatId, 
            ` *Exotic Assistant*\n\n` +
            `    !\n\n` +
            `  \`/status\`  `,
            { reply_markup: {
              inline_keyboard: [
                [{ text: ' ', callback_data: 'group_status' }],
                [{ text: '  ', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
        }
        return jsonResponse({ ok: true });
      }
      
      //  /status  
      if (cleanText.startsWith('/status')) {
        //   
        const existingOderId = await env.USERS.get(`tg:${telegramId}`);
        
        if (!existingOderId) {
          await sendTelegramMessage(env, chatId, 
            ` @${username || telegramId},  :\n\n` +
            `   \n\n` +
            `     .`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: ' ', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
          return jsonResponse({ ok: true });
        }
        
        const result = await getLicenseData(env, existingOderId);
        
        if (!result.license) {
          await sendTelegramMessage(env, chatId, 
            ` @${username || telegramId},  :\n\n` +
            `  \n` +
            `   `,
            { reply_markup: {
              inline_keyboard: [
                [{ text: ' ', url: `https://t.me/${botUsername}` }]
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
        
        const emoji = isValid ? getStatusEmoji(daysLeft, license.type) : '';
        const typeText = LICENSE_TYPES[license.type]?.name || '';
        const timeDisplay = license.type === 'trial' ? `${hoursLeft} .` : `${daysLeft} .`;
        
        await sendTelegramMessage(env, chatId, 
          ` @${username || telegramId},  :\n\n` +
          `${emoji} ${typeText}  ${isValid ? '' : ''}\n` +
          ` ${isValid ? `: ${timeDisplay}` : ' '}`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: ' ', url: `https://t.me/${botUsername}` }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      //  /help  
      if (cleanText.startsWith('/help')) {
        await sendTelegramMessage(env, chatId, 
          ` *Exotic Assistant*\n\n` +
          `   :\n\n` +
          `\`/status\`    \n` +
          `\`/id\`   ID\n` +
          `\`/help\`   \n\n` +
          `  \n` +
          `   .`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '  ', url: `https://t.me/${botUsername}` }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      //  /id  
      if (cleanText.startsWith('/id')) {
        let message = ` *  ID*\n\n`;
        message += ` *@${username || telegramId}:*\n`;
        message += ` ID: \`${telegramId}\`\n`;
        message += `\n * :*\n`;
        message += ` ID: \`${chatId}\`\n`;
        message += ` : ${chat.title || ' '}`;
        
        await sendTelegramMessage(env, chatId, message);
        return jsonResponse({ ok: true });
      }
      
      //    
      if (cleanText.startsWith('/')) {
        await sendTelegramMessage(env, chatId, 
          `  .\n\n` +
          ` \`/help\`   .`
        );
        return jsonResponse({ ok: true });
      }
      
      //    
      if (isBotMention) {
        await sendTelegramMessage(env, chatId, 
          `  \`/help\`   .`
        );
        return jsonResponse({ ok: true });
      }
      
      return jsonResponse({ ok: true });
    }
    
    //   /start
    if (update.message?.text?.startsWith('/start')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const firstName = update.message.from.first_name;
      
      //   username  telegramId  
      if (username) {
        await saveUsernameMapping(env, username, telegramId);
      }
      
      const parts = update.message.text.split(' ');
      const param = parts[1];
      
      //    (ref_REF_XXXX)
      if (param && param.startsWith('ref_')) {
        const refCode = param.replace('ref_', '');
        const result = await applyReferralCode(env, telegramId, refCode);
        
        if (result.success) {
          //   
          if (result.gaveTrialToNewUser) {
            await sendTelegramMessage(env, chatId, 
              ` * !*\n\n` +
              `    .\n` +
              `   1   !`,
              { reply_markup: getMainMenuKeyboard() }
            );
          } else {
            await sendTelegramMessage(env, chatId, 
              ` * !*\n\n` +
              `    .\n` +
              `_  , ..     ._`,
              { reply_markup: getMainMenuKeyboard() }
            );
          }
          
          //   -  
          try {
            await sendTelegramMessage(env, result.referrerTelegramId, 
              ` * !*\n\n` +
              `     !\n\n` +
              ` * +1 *  ,\n` +
              `    .`
            );
          } catch (e) {}
          
          // 
          await logToChat(env,
            ` **\n\n` +
            `: ${telegramId}\n` +
            `: ${result.referrerTelegramId}`,
            'success',
            { action: ' ', details: refCode }
          );
        } else {
          //  ,    
          await sendTelegramMessage(env, chatId, 
            ` * !*\n\n` +
            `Exotic Operator Assistant  \n` +
            `    .\n\n` +
            `    :`,
            { reply_markup: getMainMenuKeyboard() }
          );
        }
        return jsonResponse({ ok: true });
      }
      
      //    
      if (param) {
        const codeData = await env.USERS.get(`code:${param}`);
        
        if (!codeData) {
          await sendTelegramMessage(env, chatId, 
            ` * *\n\n   `,
            { reply_markup: getBackButton() }
          );
          return jsonResponse({ ok: true });
        }
        
        const { oderId, deviceId } = JSON.parse(codeData);
        
        //  
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
        
        // 
        await logToChat(env,
          ` **`,
          'connect',
          {
            telegramId,
            username: username || '',
            action: ' ',
            details: `${param.substring(0, 3)}***`
          }
        );
        
        const result = await getLicenseData(env, oderId);
        
        let message = ` *!*\n\n`;
        
        if (result.license) {
          const timeLeft = result.license.expiresAt - Date.now();
          const hoursLeft = Math.ceil(timeLeft / (60 * 60 * 1000));
          const daysLeft = Math.ceil(timeLeft / (24 * 60 * 60 * 1000));
          
          if (result.license.type === 'trial') {
            message += ` : *${hoursLeft} .*`;
          } else {
            message += ` : *${daysLeft} .*`;
          }
        }
        
        await sendTelegramMessage(env, chatId, message, {
          reply_markup: {
            inline_keyboard: [
              [
                { text: ' ', callback_data: 'status' },
                { text: ' ', callback_data: 'menu' }
              ]
            ]
          }
        });
      } else {
        //  
        const existingOderId = await env.USERS.get(`tg:${telegramId}`);
        
        let welcomeMsg;
        let keyboard;
        
        if (existingOderId) {
          //   -    getLicenseData
          //         
          const result = await getLicenseData(env, existingOderId);
          const stats = await getUserStats(env, telegramId);
          const canUseGroups = result.license && LICENSE_TYPES[result.license.type]?.groupChats;
          
          if (result.license) {
            const license = result.license;
            const isValid = license.expiresAt > Date.now();
            const timeLeft = license.expiresAt - Date.now();
            const daysLeft = Math.ceil(timeLeft / (24 * 60 * 60 * 1000));
            const hoursLeft = Math.ceil(timeLeft / (60 * 60 * 1000));
            
            // -
            const maxTime = license.type === 'trial' ? 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
            const percent = isValid ? Math.min(100, Math.round((timeLeft / maxTime) * 100)) : 0;
            const bar = getProgressBar(percent);
            
            const emoji = isValid ? getStatusEmoji(daysLeft, license.type) : '';
            const timeDisplay = license.type === 'trial' ? `${hoursLeft} .` : `${daysLeft} .`;
            const typeName = LICENSE_TYPES[license.type]?.name || license.type;
            
            welcomeMsg = ` *${firstName || ''}!*\n\n`;
            
            if (isValid) {
              welcomeMsg += `${emoji} *${typeName}*  ${timeDisplay}\n`;
              welcomeMsg += `${bar} ${percent}%\n\n`;
              welcomeMsg += `  : *${stats.todayClicks || 0}*`;
            } else {
              welcomeMsg += `  \n\n`;
              welcomeMsg += `   `;
            }
            
            keyboard = getUserMenuKeyboard(isValid, canUseGroups);
          } else {
            //  ,     ( )
            welcomeMsg = ` *${firstName || ''}!*\n\n  \n\n   `;
            keyboard = {
              inline_keyboard: [
                [{ text: '  ', callback_data: 'buy' }],
                [{ text: ' ', callback_data: 'support' }]
              ]
            };
          }
        } else {
          //   -  
          welcomeMsg = ` *${firstName || ''}!*

*Exotic Assistant v${BOT_VERSION}* 

   
   
   Telegram
 24   

* :*
1  
2    
3   `;
          
          keyboard = getMainMenuKeyboard();
        }
        
        await sendTelegramMessage(env, chatId, welcomeMsg, {
          reply_markup: keyboard
        });
        
        // 
        await logToChat(env,
          ` *${existingOderId ? '' : ''}*`,
          'user',
          {
            telegramId,
            username: username || '',
            action: '/start',
            details: existingOderId ? '' : ''
          }
        );
      }
    }
    
    // /status
    else if (update.message?.text === '/status') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      //  
      const existingOderId = await env.USERS.get(`tg:${telegramId}`);
      
      if (!existingOderId) {
        await sendTelegramMessage(env, chatId, 
          ` **\n\n   \n\n   `,
          { reply_markup: {
            inline_keyboard: [
              [{ text: ' ', callback_data: 'how_to_connect' }],
              [{ text: ' ', callback_data: 'buy' }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      //    getLicenseData
      const result = await getLicenseData(env, existingOderId);
      
      if (!result.license) {
        await sendTelegramMessage(env, chatId, 
          ` **\n\n  \n   `,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '  ', callback_data: 'buy' }],
              [{ text: ' ', callback_data: 'support' }]
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
        const typeText = LICENSE_TYPES[license.type]?.name || '';
        const timeDisplay = license.type === 'trial' ? `${hoursLeft} .` : `${daysLeft} .`;
        
        // - ( trial 24,   30)
        const maxTime = license.type === 'trial' ? 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
        const percent = Math.min(100, Math.round((timeLeft / maxTime) * 100));
        const bar = getProgressBar(percent);
        
        message = ` **\n\n`;
        message += `${emoji} ${typeText}\n`;
        message += `${bar} ${percent}%\n\n`;
        message += ` ${timeDisplay}  ${expiresDate}`;
        
        if (daysLeft <= 3 && license.type !== 'trial') {
          message += `\n\n _ _`;
        }
      } else {
        message = ` **\n\n`;
        message += `  ${expiresDate}`;
      }
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: getStatusKeyboard(isValid, license.type)
      });
    }
    
    // /help  
    else if (update.message?.text === '/help') {
      const chatId = update.message.chat.id;
      
      const helpMsg = ` *Exotic Assistant v${BOT_VERSION}*

 **
   " "

 **
      

 **
Push  Telegram  

 **
    

 * *
    +1 !`;
      
      await sendTelegramMessage(env, chatId, helpMsg, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: ' ', callback_data: 'how_to_connect' },
              { text: ' FAQ', callback_data: 'faq' }
            ],
            [{ text: ' ', callback_data: 'buy' }],
            [{ text: ' ', callback_data: 'menu' }]
          ]
        }
      });
    }
    
    // /menu
    else if (update.message?.text === '/menu') {
      const chatId = update.message.chat.id;
      
      await sendTelegramMessage(env, chatId, ` **`, {
        reply_markup: getMainMenuKeyboard()
      });
    }
    
    // /id   ID   
    else if (update.message?.text === '/id') {
      const chat = update.message.chat;
      const chatId = chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const firstName = update.message.from.first_name;
      
      let message = ` *  ID*\n\n`;
      message += ` *:*\n`;
      message += ` ID: \`${telegramId}\`\n`;
      if (username) message += ` Username: @${username}\n`;
      if (firstName) message += ` : ${firstName}\n`;
      
      if (chat.type === 'group' || chat.type === 'supergroup') {
        message += `\n * :*\n`;
        message += ` ID: \`${chatId}\`\n`;
        message += ` : ${chat.type === 'supergroup' ? '' : ''}\n`;
        if (chat.title) message += ` : ${chat.title}\n`;
      } else if (chat.type === 'private') {
        message += `\n * :*  `;
      }
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: {
          inline_keyboard: [
            [{ text: ' ', callback_data: 'menu' }]
          ]
        }
      });
    }
    
    //   (8 )
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
        
        // 
        await logToChat(env,
          ` **`,
          'connect',
          {
            telegramId,
            username: username || '',
            action: ' ',
            details: `${code.substring(0, 3)}***`
          }
        );
        
        const result = await getLicenseData(env, oderId);
        
        let message = ` *!*\n\n`;
        
        if (result.license) {
          const timeLeft = result.license.expiresAt - Date.now();
          const hoursLeft = Math.ceil(timeLeft / (60 * 60 * 1000));
          const daysLeft = Math.ceil(timeLeft / (24 * 60 * 60 * 1000));
          
          if (result.license.type === 'trial') {
            message += ` : *${hoursLeft} .*`;
          } else {
            message += ` : *${daysLeft} .*`;
          }
        }
        
        await sendTelegramMessage(env, chatId, message, {
          reply_markup: {
            inline_keyboard: [
              [
                { text: ' ', callback_data: 'status' },
                { text: ' ', callback_data: 'menu' }
              ]
            ]
          }
        });
      } else {
        await sendTelegramMessage(env, chatId, 
          ` *  *\n\n   `,
          { reply_markup: getMainMenuKeyboard() }
        );
      }
    }
    
    // 
    else if (update.message?.text) {
      const chatId = update.message.chat.id;
      
      await sendTelegramMessage(env, chatId, 
        `  .\n\n  :`,
        { reply_markup: getMainMenuKeyboard() }
      );
    }
    
    return jsonResponse({ ok: true });
    
  } catch (error) {
    console.error('Webhook error:', error);
    
    await logToChat(env,
      ` * *\n\n` +
      ` Path: \`${path}\`\n` +
      ` Error: ${error.message}`,
      'error'
    );
    
    return jsonResponse({ error: 'Internal server error', message: error.message }, 500);
  }
}

export default {
  // Cron handler     
  async scheduled(event, env, ctx) {
    ctx.waitUntil(checkAndSendExpirationReminders(env));
  },
  
  async fetch(request, env) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const path = url.pathname;
    
    // HTTPS  -   HTTP
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
      // Health check   
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
      
      //  
      if (path === '/api/license/check' && request.method === 'POST') {
        return await handleLicenseCheck(request, env);
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
      
      // Telegram    
      if (path === '/api/telegram/connect' && request.method === 'POST') {
        return await handleTelegramConnect(request, env);
      }
      
      // ==========    ==========
      //      
      if (path === '/api/settings/get' && request.method === 'POST') {
        const { data, error } = await safeParseJson(request);
        if (error) return jsonResponse({ error: 'Invalid JSON' }, 400);
        
        const { oderId, telegramId } = data;
        
        if (!oderId && !telegramId) {
          return jsonResponse({ error: 'oderId or telegramId required' }, 400);
        }
        
        let tgId = telegramId;
        
        //   oderId,  telegramId
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
        
        //    
        const botSettings = await getUserSettings(env, tgId);
        
        //  
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
      
      //     
      if (path === '/api/settings/sync' && request.method === 'POST') {
        const { data, error } = await safeParseJson(request);
        if (error) return jsonResponse({ error: 'Invalid JSON' }, 400);
        
        const { oderId, telegramId, settings } = data;
        
        if (!oderId && !telegramId) {
          return jsonResponse({ error: 'oderId or telegramId required' }, 400);
        }
        
        let tgId = telegramId;
        
        //   oderId,  telegramId
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
        
        //   
        const currentSettings = await getUserSettings(env, tgId);
        
        //   (  )
        const updatedSettings = {
          ...currentSettings,
          notifications: {
            ...currentSettings.notifications,
            //      
            clicks: settings?.clickNotifications ?? currentSettings.notifications.clicks,
            sound: settings?.soundEnabled ?? currentSettings.notifications.sound
          },
          //    
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
      
      //   
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
        
        //    
        const botSettings = await getUserSettings(env, tgId);
        const licenseData = await env.USERS.get(`license:tg:${tgId}`);
        let license = null;
        if (licenseData) {
          license = JSON.parse(licenseData);
        }
        
        //  
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
      
      //  
      if (path === '/api/admin/grant-license' && request.method === 'POST') {
        return await handleGrantLicense(request, env);
      }
      if (path === '/api/license/info' && request.method === 'POST') {
        return await handleLicenseInfo(request, env);
      }
      
      // Health check  
      if (path === '/api/health') {
        const uptime = Date.now() - metrics.startTime;
        const uptimeHours = Math.floor(uptime / MS_PER_HOUR);
        const uptimeMinutes = Math.floor((uptime % MS_PER_HOUR) / MS_PER_MINUTE);
        
        return jsonResponse({ 
          status: 'ok', 
          version: BOT_VERSION,
          timestamp: Date.now(),
          bot: env.BOT_USERNAME || 'clickuved_bot',
          hasToken: !!env.TELEGRAM_BOT_TOKEN,
          hasKV: !!env.USERS,
          hasLogChat: !!env.LOG_CHAT_ID,
          uptime: `${uptimeHours}h ${uptimeMinutes}m`,
          metrics: {
            requests: metrics.requests,
            errorRate: metrics.requests.total > 0 
              ? ((metrics.requests.error / metrics.requests.total) * 100).toFixed(2) + '%' 
              : '0%'
          }
        });
      }
      
      //   (  )
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
      
      // Webhook info  
      if (path === '/api/webhook-info') {
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
      
      //  webhook
      if (path === '/api/set-webhook') {
        const botToken = env.TELEGRAM_BOT_TOKEN;
        if (!botToken) {
          return jsonResponse({ error: 'TELEGRAM_BOT_TOKEN not set' }, 500);
        }
        const workerUrl = new URL(request.url).origin;
        const webhookUrl = `${workerUrl}/webhook`;
        try {
          const response = await fetch(`https://api.telegram.org/bot${botToken}/setWebhook?url=${webhookUrl}`);
          const data = await response.json();
          
          //   webhook
          await logToChat(env, 
            ` Webhook \n\n` +
            ` URL: \`${webhookUrl}\`\n` +
            ` : ${data.ok ? '' : ''}`,
            'system'
          );
          
          return jsonResponse({ ...data, webhookUrl });
        } catch (e) {
          return jsonResponse({ error: e.message }, 500);
        }
      }
      
      //   -    
      if (path === '/api/test-log') {
        await logToChat(env, 
          ` * *\n\n` +
          `    ,   !`,
          'success',
          { 
            action: '  ',
            details: ` : ${request.headers.get('CF-Connecting-IP') || 'unknown'}`
          }
        );
        recordMetric(path, true, Date.now() - startTime);
        return jsonResponse({ ok: true, message: 'Test log sent to configured chat' });
      }
      
      recordMetric(path, false, Date.now() - startTime, 'Not found');
      return jsonResponse({ error: 'Not found' }, 404);
      
    } catch (error) {
      const duration = Date.now() - startTime;
      
      //   
      structuredLog('error', error.message, {
        path,
        duration,
        stack: error.stack?.substring(0, 500),
        ip: request.headers.get('CF-Connecting-IP')
      });
      
      //  
      recordMetric(path, false, duration, error.message);
      
      //     Telegram
      await logToChat(env,
        ` * *\n\n` +
        ` Path: \`${path}\`\n` +
        ` Error: ${error.message}`,
        'error'
      );
      
      return jsonResponse({ error: 'Internal server error', message: error.message }, 500);
    }
  }
};

