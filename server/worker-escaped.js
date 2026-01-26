// ========== \u041a\u041e\u041d\u0421\u0422\u0410\u041d\u0422\u042b ==========

// \u0412\u0435\u0440\u0441\u0438\u044f \u0431\u043e\u0442\u0430 \u0438 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f
const BOT_VERSION = '5.0';
const EXTENSION_VERSION = '5.0';

// ========== \u0421\u0418\u0421\u0422\u0415\u041c\u0410 \u041c\u0415\u0422\u0420\u0418\u041a ==========
const metrics = {
  requests: { total: 0, success: 0, error: 0 },
  endpoints: {},
  errors: [],
  startTime: Date.now(),
  lastReset: Date.now()
};

// ========== \u041a\u042d\u0428\u0418\u0420\u041e\u0412\u0410\u041d\u0418\u0415 ==========
const cache = new Map();
const CACHE_TTL = 60 * 1000; // 1 \u043c\u0438\u043d\u0443\u0442\u0430

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
  // \u041e\u0447\u0438\u0441\u0442\u043a\u0430 \u0441\u0442\u0430\u0440\u044b\u0445 \u0437\u0430\u043f\u0438\u0441\u0435\u0439
  if (cache.size > 5000) {
    const now = Date.now();
    for (const [k, v] of cache) {
      if (v.expires < now) cache.delete(k);
    }
  }
}

// ========== RATE LIMITING \u0414\u041b\u042f \u0411\u041e\u0422\u0410 ==========
const botRateLimits = new Map();
const BOT_RATE_LIMIT = {
  messages: 20,      // \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439 \u0432 \u043c\u0438\u043d\u0443\u0442\u0443 \u043d\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
  callbacks: 30,     // callback'\u043e\u0432 \u0432 \u043c\u0438\u043d\u0443\u0442\u0443 \u043d\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
  window: 60 * 1000  // 1 \u043c\u0438\u043d\u0443\u0442\u0430
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
      message: `\u23f3 \u0421\u043b\u0438\u0448\u043a\u043e\u043c \u043c\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u0440\u043e\u0441\u043e\u0432. \u041f\u043e\u0434\u043e\u0436\u0434\u0438\u0442\u0435 ${Math.ceil((requests[0] + BOT_RATE_LIMIT.window - now) / 1000)} \u0441\u0435\u043a.`
    };
  }
  
  requests.push(now);
  botRateLimits.set(key, requests);
  
  // \u041e\u0447\u0438\u0441\u0442\u043a\u0430
  if (botRateLimits.size > 10000) {
    const entries = Array.from(botRateLimits.entries());
    entries.slice(0, 5000).forEach(([k]) => botRateLimits.delete(k));
  }
  
  return { allowed: true, remaining: limit - requests.length };
}

// ========== \u0421\u0418\u0421\u0422\u0415\u041c\u0410 \u0423\u0412\u0415\u0414\u041e\u041c\u041b\u0415\u041d\u0418\u0419 \u041e\u0411 \u0418\u0421\u0422\u0415\u0427\u0415\u041d\u0418\u0418 ==========
const EXPIRATION_NOTIFICATIONS = {
  3: { sent: false, emoji: '\u26a0\ufe0f', text: '3 \u0434\u043d\u044f' },
  1: { sent: false, emoji: '\u{1f534}', text: '1 \u0434\u0435\u043d\u044c' },
  0: { sent: false, emoji: '\u274c', text: '\u0441\u0435\u0433\u043e\u0434\u043d\u044f' }
};

// \u0424\u0443\u043d\u043a\u0446\u0438\u044f \u0434\u043b\u044f \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0438 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439 \u043e\u0431 \u0438\u0441\u0442\u0435\u0447\u0435\u043d\u0438\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438
async function sendExpirationNotification(env, telegramId, daysLeft, licenseType) {
  const notifKey = `expiry_notif:${telegramId}:${daysLeft}`;
  
  // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c, \u043d\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u043b\u0438 \u043b\u0438 \u0443\u0436\u0435
  const alreadySent = await env.USERS.get(notifKey);
  if (alreadySent) return false;
  
  // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
  const userSettings = await getUserSettings(env, telegramId);
  if (!userSettings.notifications.expiryReminders) {
    return false; // \u041d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u044f \u043e\u0442\u043a\u043b\u044e\u0447\u0435\u043d\u044b
  }
  
  const typeText = LICENSE_TYPES[licenseType]?.name || '\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430';
  
  let message;
  if (daysLeft === 3) {
    message = `\u26a0\ufe0f *\u041d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435*\n\n` +
      `\u0412\u0430\u0448\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 *${typeText}* \u0438\u0441\u0442\u0435\u043a\u0430\u0435\u0442 \u0447\u0435\u0440\u0435\u0437 *3 \u0434\u043d\u044f*.\n\n` +
      `\u041f\u0440\u043e\u0434\u043b\u0438\u0442\u0435 \u0437\u0430\u0440\u0430\u043d\u0435\u0435, \u0447\u0442\u043e\u0431\u044b \u0430\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440 \u0440\u0430\u0431\u043e\u0442\u0430\u043b \u0431\u0435\u0437 \u043f\u0435\u0440\u0435\u0440\u044b\u0432\u043e\u0432!`;
  } else if (daysLeft === 1) {
    message = `\u{1f534} *\u0412\u043d\u0438\u043c\u0430\u043d\u0438\u0435!*\n\n` +
      `\u0412\u0430\u0448\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 *${typeText}* \u0438\u0441\u0442\u0435\u043a\u0430\u0435\u0442 *\u0437\u0430\u0432\u0442\u0440\u0430*!\n\n` +
      `\u041d\u0435 \u0437\u0430\u0431\u0443\u0434\u044c\u0442\u0435 \u043f\u0440\u043e\u0434\u043b\u0438\u0442\u044c, \u0438\u043d\u0430\u0447\u0435 \u0430\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440 \u043e\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u0441\u044f.`;
  } else if (daysLeft === 0) {
    message = `\u274c *\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0438\u0441\u0442\u0435\u043a\u0430\u0435\u0442 \u0441\u0435\u0433\u043e\u0434\u043d\u044f!*\n\n` +
      `\u0412\u0430\u0448\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 *${typeText}* \u0437\u0430\u043a\u0430\u043d\u0447\u0438\u0432\u0430\u0435\u0442\u0441\u044f \u0441\u0435\u0433\u043e\u0434\u043d\u044f.\n\n` +
      `\u041f\u0440\u043e\u0434\u043b\u0438\u0442\u0435 \u0441\u0435\u0439\u0447\u0430\u0441, \u0447\u0442\u043e\u0431\u044b \u043d\u0435 \u043f\u043e\u0442\u0435\u0440\u044f\u0442\u044c \u0434\u043e\u0441\u0442\u0443\u043f \u043a \u0430\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440\u0443!`;
  }
  
  if (message) {
    await sendTelegramMessage(env, telegramId, message, {
      reply_markup: {
        inline_keyboard: [
          [{ text: '\u{1f4b3} \u041f\u0440\u043e\u0434\u043b\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
          [{ text: '\u{1f4ac} \u041d\u0430\u043f\u0438\u0441\u0430\u0442\u044c @YaMob', url: 'https://t.me/YaMob' }]
        ]
      }
    });
    
    // \u041f\u043e\u043c\u0435\u0447\u0430\u0435\u043c \u0447\u0442\u043e \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e (TTL 7 \u0434\u043d\u0435\u0439)
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
      // \u0425\u0440\u0430\u043d\u0438\u043c \u0442\u043e\u043b\u044c\u043a\u043e \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 100 \u043e\u0448\u0438\u0431\u043e\u043a
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

// \u0421\u0442\u0440\u0443\u043a\u0442\u0443\u0440\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u043e\u0435 \u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0434\u043b\u044f \u0432\u043d\u0435\u0448\u043d\u0438\u0445 \u0441\u0435\u0440\u0432\u0438\u0441\u043e\u0432 (Sentry-like)
function structuredLog(level, message, context = {}) {
  const logEntry = {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...context,
    env: 'production'
  };
  
  // Console \u0434\u043b\u044f Cloudflare Workers Logs
  if (level === 'error') {
    console.error(JSON.stringify(logEntry));
  } else if (level === 'warn') {
    console.warn(JSON.stringify(logEntry));
  } else {
    console.log(JSON.stringify(logEntry));
  }
  
  return logEntry;
}

// \u0420\u0430\u0437\u0440\u0435\u0448\u0451\u043d\u043d\u044b\u0435 origins \u0434\u043b\u044f CORS (\u0434\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u0434\u043e\u043c\u0435\u043d\u044b \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f)
const ALLOWED_ORIGINS = [
  'chrome-extension://',
  'moz-extension://',
  'https://exotic.company',
  'https://*.exotic.company'
];

function getCorsHeaders(origin) {
  // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c, \u0440\u0430\u0437\u0440\u0435\u0448\u0451\u043d \u043b\u0438 origin
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

// CORS headers (\u0431\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u0430\u044f \u0432\u0435\u0440\u0441\u0438\u044f - \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0435\u043c getCorsHeaders() \u0441 \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u043e\u0439 origin)
// \u0414\u043b\u044f \u0432\u043d\u0443\u0442\u0440\u0435\u043d\u043d\u0438\u0445 \u0437\u0430\u043f\u0440\u043e\u0441\u043e\u0432 \u0433\u0434\u0435 origin \u043d\u0435 \u0432\u0430\u0436\u0435\u043d
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': 'https://exotic.company',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

// \u0412\u0440\u0435\u043c\u0435\u043d\u043d\u044b\u0435 \u043a\u043e\u043d\u0441\u0442\u0430\u043d\u0442\u044b
const MS_PER_MINUTE = 60 * 1000;
const MS_PER_HOUR = 60 * MS_PER_MINUTE;
const MS_PER_DAY = 24 * MS_PER_HOUR;
const CODE_EXPIRATION_TTL = 1800; // 30 \u043c\u0438\u043d\u0443\u0442

// \u041f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434
const TRIAL_DURATION_NO_REFERRAL = 12 * MS_PER_HOUR; // 12 \u0447\u0430\u0441\u043e\u0432 \u0434\u043b\u044f \u043d\u043e\u0432\u044b\u0445 \u0431\u0435\u0437 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u0430
const TRIAL_DURATION_WITH_REFERRAL = MS_PER_DAY; // 24 \u0447\u0430\u0441\u0430 \u0434\u043b\u044f \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0451\u043d\u043d\u044b\u0445 \u043f\u043e \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u043e\u0439 \u0441\u0441\u044b\u043b\u043a\u0435
const TRIAL_DURATION = TRIAL_DURATION_NO_REFERRAL; // \u041f\u043e \u0443\u043c\u043e\u043b\u0447\u0430\u043d\u0438\u044e 12 \u0447\u0430\u0441\u043e\u0432

// \u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0438\u0441\u0442\u0435\u043c\u0430
const REFERRAL_BONUS_HOURS = 24; // \u0411\u043e\u043d\u0443\u0441 \u0447\u0430\u0441\u043e\u0432 \u0440\u0435\u0444\u0435\u0440\u0435\u0440\u0443 \u043a\u043e\u0433\u0434\u0430 \u0440\u0435\u0444\u0435\u0440\u0430\u043b \u043e\u0444\u043e\u0440\u043c\u0438\u0442 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443
const REFERRAL_TRIAL_HOURS = 24; // \u041f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434 \u0434\u043b\u044f \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0451\u043d\u043d\u043e\u0433\u043e (24 \u0447\u0430\u0441\u0430)
const REFERRAL_CODE_PREFIX = 'REF_';

// ========== \u0421\u041a\u0420\u042b\u0422\u0410\u042f \u0410\u0414\u041c\u0418\u041d-\u041f\u0410\u041d\u0415\u041b\u042c ==========
// \u0421\u0435\u043a\u0440\u0435\u0442\u043d\u0430\u044f \u0444\u0440\u0430\u0437\u0430 \u0434\u043b\u044f \u0434\u043e\u0441\u0442\u0443\u043f\u0430 \u043a \u0430\u0434\u043c\u0438\u043d\u043a\u0435 \u0431\u0435\u0440\u0451\u0442\u0441\u044f \u0438\u0437 env.ADMIN_SECRET_PHRASE
// \u0414\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u0447\u0435\u0440\u0435\u0437: wrangler secret put ADMIN_SECRET_PHRASE
// \u0421\u0435\u0441\u0441\u0438\u0438 \u0430\u0434\u043c\u0438\u043d\u043e\u0432 \u0445\u0440\u0430\u043d\u044f\u0442\u0441\u044f 1 \u0447\u0430\u0441
const ADMIN_SESSION_TTL = 60 * 60; // 1 \u0447\u0430\u0441 \u0432 \u0441\u0435\u043a\u0443\u043d\u0434\u0430\u0445

// \u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u0430\u0434\u043c\u0438\u043d\u0441\u043a\u043e\u0433\u043e \u0434\u043e\u0441\u0442\u0443\u043f\u0430 (\u043f\u043e telegramId, \u043d\u0435 \u043f\u043e username \u0434\u043b\u044f \u0431\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u043e\u0441\u0442\u0438)
async function isAdminById(env, telegramId) {
  const adminId = env.ADMIN_TELEGRAM_ID;
  return adminId && String(telegramId) === String(adminId);
}

// ========== \u0421\u0418\u0421\u0422\u0415\u041c\u0410 \u041a\u041e\u041d\u0422\u0420\u041e\u041b\u042f \u0423\u0421\u0422\u0420\u041e\u0419\u0421\u0422\u0412 ==========
// Level 1/2 = 1 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e, Level 3 = 2 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430
// \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430 \u0438\u0434\u0435\u043d\u0442\u0438\u0444\u0438\u0446\u0438\u0440\u0443\u044e\u0442\u0441\u044f \u043f\u043e deviceId (oderId)

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0441\u043f\u0438\u0441\u043e\u043a \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
async function getUserDevices(env, telegramId) {
  const data = await env.USERS.get(`devices:${telegramId}`);
  if (!data) return { devices: [], lastUpdated: null };
  return JSON.parse(data);
}

// \u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c \u0441\u043f\u0438\u0441\u043e\u043a \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432
async function saveUserDevices(env, telegramId, devicesData) {
  devicesData.lastUpdated = Date.now();
  await env.USERS.put(`devices:${telegramId}`, JSON.stringify(devicesData));
}

// \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u0438 \u0437\u0430\u0440\u0435\u0433\u0438\u0441\u0442\u0440\u0438\u0440\u043e\u0432\u0430\u0442\u044c \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e
async function checkAndRegisterDevice(env, telegramId, deviceId, license) {
  if (!deviceId) {
    return { allowed: true, reason: 'NO_DEVICE_ID' }; // \u0414\u043b\u044f \u0441\u043e\u0432\u043c\u0435\u0441\u0442\u0438\u043c\u043e\u0441\u0442\u0438 \u0441\u043e \u0441\u0442\u0430\u0440\u044b\u043c\u0438 \u0432\u0435\u0440\u0441\u0438\u044f\u043c\u0438
  }
  
  const licenseType = LICENSE_TYPES[license?.type] || LICENSE_TYPES.trial;
  const maxDevices = licenseType.maxDevices || 1;
  
  const devicesData = await getUserDevices(env, telegramId);
  const devices = devicesData.devices || [];
  
  // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c, \u0435\u0441\u0442\u044c \u043b\u0438 \u0443\u0436\u0435 \u044d\u0442\u043e \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e
  const existingIndex = devices.findIndex(d => d.deviceId === deviceId);
  if (existingIndex >= 0) {
    // \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u0443\u0436\u0435 \u0437\u0430\u0440\u0435\u0433\u0438\u0441\u0442\u0440\u0438\u0440\u043e\u0432\u0430\u043d\u043e - \u043e\u0431\u043d\u043e\u0432\u043b\u044f\u0435\u043c lastSeen
    devices[existingIndex].lastSeen = Date.now();
    await saveUserDevices(env, telegramId, { devices });
    return { allowed: true, reason: 'DEVICE_EXISTS', deviceCount: devices.length, maxDevices };
  }
  
  // \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u043d\u043e\u0432\u043e\u0435 - \u043f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043b\u0438\u043c\u0438\u0442
  if (devices.length >= maxDevices) {
    // \u041b\u0438\u043c\u0438\u0442 \u0434\u043e\u0441\u0442\u0438\u0433\u043d\u0443\u0442
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
  
  // \u0420\u0435\u0433\u0438\u0441\u0442\u0440\u0438\u0440\u0443\u0435\u043c \u043d\u043e\u0432\u043e\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e
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

// \u0423\u0434\u0430\u043b\u0438\u0442\u044c \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e (\u0434\u043b\u044f \u043e\u0442\u0432\u044f\u0437\u043a\u0438)
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

// \u041e\u0447\u0438\u0441\u0442\u0438\u0442\u044c \u0432\u0441\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
async function clearAllDevices(env, telegramId) {
  await saveUserDevices(env, telegramId, { devices: [] });
  return { success: true };
}

// \u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0439 \u0430\u0434\u043c\u0438\u043d-\u0441\u0435\u0441\u0441\u0438\u0438
async function hasAdminSession(env, telegramId) {
  const sessionKey = `admin_session:${telegramId}`;
  const session = await env.USERS.get(sessionKey);
  return !!session;
}

// \u0421\u043e\u0437\u0434\u0430\u0442\u044c \u0430\u0434\u043c\u0438\u043d-\u0441\u0435\u0441\u0441\u0438\u044e
async function createAdminSession(env, telegramId) {
  const sessionKey = `admin_session:${telegramId}`;
  const sessionData = {
    createdAt: Date.now(),
    telegramId: String(telegramId)
  };
  await env.USERS.put(sessionKey, JSON.stringify(sessionData), { expirationTtl: ADMIN_SESSION_TTL });
  return true;
}

// \u0423\u0434\u0430\u043b\u0438\u0442\u044c \u0430\u0434\u043c\u0438\u043d-\u0441\u0435\u0441\u0441\u0438\u044e (\u0432\u044b\u0445\u043e\u0434)
async function destroyAdminSession(env, telegramId) {
  const sessionKey = `admin_session:${telegramId}`;
  await env.USERS.delete(sessionKey);
}

// \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u0438 \u043f\u0440\u043e\u0434\u043b\u0438\u0442\u044c \u0441\u0435\u0441\u0441\u0438\u044e \u0430\u0434\u043c\u0438\u043d\u0430 (\u0432\u043e\u0437\u0432\u0440\u0430\u0449\u0430\u0435\u0442 true \u0435\u0441\u043b\u0438 \u0430\u043a\u0442\u0438\u0432\u043d\u0430)
async function checkAndExtendAdminSession(env, telegramId) {
  const hasSession = await hasAdminSession(env, telegramId);
  if (hasSession) {
    // \u041f\u0440\u043e\u0434\u043b\u0435\u0432\u0430\u0435\u043c \u0441\u0435\u0441\u0441\u0438\u044e \u043f\u0440\u0438 \u043a\u0430\u0436\u0434\u043e\u0439 \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u0438
    await createAdminSession(env, telegramId);
    return true;
  }
  return false;
}

// \u041f\u043e\u043a\u0430\u0437\u0430\u0442\u044c \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u043e\u0431 \u0438\u0441\u0442\u0435\u0447\u0435\u043d\u0438\u0438 \u0441\u0435\u0441\u0441\u0438\u0438
async function showSessionExpiredMessage(env, chatId, messageId) {
  await editTelegramMessage(env, chatId, messageId,
    `\u{1f512} *\u0421\u0435\u0441\u0441\u0438\u044f \u0438\u0441\u0442\u0435\u043a\u043b\u0430*\n\n` +
    `\u0414\u043b\u044f \u043f\u0440\u043e\u0434\u043e\u043b\u0436\u0435\u043d\u0438\u044f \u0440\u0430\u0431\u043e\u0442\u044b \u0432\u0432\u0435\u0434\u0438\u0442\u0435\n\u0441\u0435\u043a\u0440\u0435\u0442\u043d\u0443\u044e \u0444\u0440\u0430\u0437\u0443 \u0437\u0430\u043d\u043e\u0432\u043e.`,
    { reply_markup: { inline_keyboard: [[{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]] }}
  );
}

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0432\u0441\u0435 pending \u0433\u0440\u0443\u043f\u043f\u044b
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

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0443 \u0434\u043b\u044f \u0430\u0434\u043c\u0438\u043d\u043a\u0438
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
    // \u041f\u043e\u0434\u0441\u0447\u0451\u0442 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439 (\u0432\u044b\u0431\u043e\u0440\u043e\u0447\u043d\u043e \u0434\u043b\u044f \u043f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0438\u0442\u0435\u043b\u044c\u043d\u043e\u0441\u0442\u0438)
    const usersList = await env.USERS.list({ prefix: 'user:', limit: 1000 });
    stats.totalUsers = usersList.keys.length;
    
    // \u041f\u043e\u0434\u0441\u0447\u0451\u0442 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0439 \u043f\u043e \u0443\u0440\u043e\u0432\u043d\u044f\u043c
    const licenseList = await env.USERS.list({ prefix: 'license:tg:', limit: 1000 });
    const now = Date.now();
    
    for (const key of licenseList.keys) {
      const data = await env.USERS.get(key.name);
      if (data) {
        const license = JSON.parse(data);
        if (license.expiresAt > now) {
          stats.activeUsers++;
          // \u0421\u0447\u0438\u0442\u0430\u0435\u043c \u043f\u043e \u0442\u0438\u043f\u0430\u043c
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
              stats.level1Users++; // \u041f\u043e \u0443\u043c\u043e\u043b\u0447\u0430\u043d\u0438\u044e Level 1
          }
        }
      }
    }
    
    // Pending \u0433\u0440\u0443\u043f\u043f\u044b
    const pendingList = await env.USERS.list({ prefix: 'pending_group:' });
    stats.pendingGroups = pendingList.keys.length;
    
    // \u041e\u0434\u043e\u0431\u0440\u0435\u043d\u043d\u044b\u0435 \u0433\u0440\u0443\u043f\u043f\u044b
    const groupOwnersList = await env.USERS.list({ prefix: 'group_owner:' });
    stats.approvedGroups = groupOwnersList.keys.length;
    
  } catch (e) {
    console.error('Error getting admin stats:', e);
  }
  
  return stats;
}

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0441\u043f\u0438\u0441\u043e\u043a \u0432\u0441\u0435\u0445 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439 \u0441 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044f\u043c\u0438
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

// ========== \u041f\u041e\u0418\u0421\u041a \u041f\u041e\u041b\u042c\u0417\u041e\u0412\u0410\u0422\u0415\u041b\u042f \u041f\u041e USERNAME ==========

// \u041a\u044d\u0448 username -> telegramId (\u0445\u0440\u0430\u043d\u0438\u0442\u0441\u044f \u0432 KV)
async function saveUsernameMapping(env, username, telegramId) {
  if (!username) return;
  const cleanUsername = username.toLowerCase().replace('@', '');
  await env.USERS.put(`username:${cleanUsername}`, String(telegramId));
}

// \u041d\u0430\u0439\u0442\u0438 telegramId \u043f\u043e username
async function findTelegramIdByUsername(env, username) {
  if (!username) return null;
  const cleanUsername = username.toLowerCase().replace('@', '');
  
  // \u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043a\u044d\u0448
  const cachedId = await env.USERS.get(`username:${cleanUsername}`);
  if (cachedId) return cachedId;
  
  // \u0415\u0441\u043b\u0438 \u043d\u0435 \u0432 \u043a\u044d\u0448\u0435 - \u0438\u0449\u0435\u043c \u0432 user:* \u0437\u0430\u043f\u0438\u0441\u044f\u0445
  try {
    const usersList = await env.USERS.list({ prefix: 'user:', limit: 1000 });
    for (const key of usersList.keys) {
      const data = await env.USERS.get(key.name);
      if (data) {
        const user = JSON.parse(data);
        if (user.username && user.username.toLowerCase() === cleanUsername) {
          // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u0432 \u043a\u044d\u0448 \u0434\u043b\u044f \u0431\u0443\u0434\u0443\u0449\u0435\u0433\u043e
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

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044e \u043e \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435 (\u043f\u043e ID \u0438\u043b\u0438 username)
async function getUserInfo(env, target) {
  const cleanTarget = target.replace('@', '');
  const isTelegramId = /^\d+$/.test(cleanTarget);
  
  let telegramId = isTelegramId ? cleanTarget : null;
  let username = isTelegramId ? null : cleanTarget;
  
  // \u0415\u0441\u043b\u0438 \u044d\u0442\u043e username - \u0438\u0449\u0435\u043c telegramId
  if (!isTelegramId) {
    telegramId = await findTelegramIdByUsername(env, cleanTarget);
    if (!telegramId) {
      return { found: false, error: 'USER_NOT_FOUND', username: cleanTarget };
    }
  }
  
  // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e
  const licenseData = await env.USERS.get(`license:tg:${telegramId}`);
  const license = licenseData ? JSON.parse(licenseData) : null;
  
  // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0443 (oderId)
  const oderId = await env.USERS.get(`tg:${telegramId}`);
  let userData = null;
  if (oderId) {
    const userDataRaw = await env.USERS.get(`user:${oderId}`);
    if (userDataRaw) {
      userData = JSON.parse(userDataRaw);
      username = userData.username || username;
    }
  }
  
  // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430
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

// Rate limiting \u043a\u043e\u043d\u0441\u0442\u0430\u043d\u0442\u044b
const RATE_LIMIT_WINDOW = 60 * 1000; // 1 \u043c\u0438\u043d\u0443\u0442\u0430
const RATE_LIMIT_MAX_REQUESTS = 60; // \u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c \u0437\u0430\u043f\u0440\u043e\u0441\u043e\u0432 \u0432 \u043c\u0438\u043d\u0443\u0442\u0443

// Telegram IP ranges (\u0434\u043b\u044f \u0432\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u0438 webhook)
const TELEGRAM_SUBNETS = [
  '149.154.160.0/20',
  '91.108.4.0/22'
];

// \u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 IP \u0432 \u043f\u043e\u0434\u0441\u0435\u0442\u0438 (\u043f\u0440\u043e\u0441\u0442\u0430\u044f \u0440\u0435\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u044f)
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

// \u042d\u043a\u0440\u0430\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0441\u043f\u0435\u0446\u0438\u0430\u043b\u044c\u043d\u044b\u0445 \u0441\u0438\u043c\u0432\u043e\u043b\u043e\u0432 Markdown \u0434\u043b\u044f Telegram
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

// Rate limiter (\u043f\u0440\u043e\u0441\u0442\u0430\u044f \u0440\u0435\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u044f \u0434\u043b\u044f KV)
const rateLimitCache = new Map();

async function checkRateLimit(env, identifier, maxRequests = RATE_LIMIT_MAX_REQUESTS) {
  const key = `ratelimit:${identifier}`;
  const now = Date.now();
  const windowStart = now - RATE_LIMIT_WINDOW;
  
  // \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0435\u043c in-memory cache \u0434\u043b\u044f \u0431\u044b\u0441\u0442\u0440\u043e\u0439 \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438
  let requests = rateLimitCache.get(key) || [];
  requests = requests.filter(time => time > windowStart);
  
  if (requests.length >= maxRequests) {
    return { allowed: false, remaining: 0, resetIn: Math.ceil((requests[0] + RATE_LIMIT_WINDOW - now) / 1000) };
  }
  
  requests.push(now);
  rateLimitCache.set(key, requests);
  
  // \u041e\u0447\u0438\u0441\u0442\u043a\u0430 \u0441\u0442\u0430\u0440\u044b\u0445 \u0437\u0430\u043f\u0438\u0441\u0435\u0439
  if (rateLimitCache.size > 10000) {
    const entries = Array.from(rateLimitCache.entries());
    entries.slice(0, 5000).forEach(([k]) => rateLimitCache.delete(k));
  }
  
  return { allowed: true, remaining: maxRequests - requests.length };
}

// \u0411\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u044b\u0439 \u043f\u0430\u0440\u0441\u0438\u043d\u0433 JSON
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

// ========== \u0421\u0418\u0421\u0422\u0415\u041c\u0410 \u041b\u041e\u0413\u0418\u0420\u041e\u0412\u0410\u041d\u0418\u042f ==========

// \u041a\u0440\u0430\u0441\u0438\u0432\u043e\u0435 \u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0432 Telegram \u0447\u0430\u0442
async function logToChat(env, message, level = 'info', extra = {}) {
  const botToken = env.TELEGRAM_BOT_TOKEN;
  const logChatId = env.LOG_CHAT_ID; // \u0422\u0435\u043f\u0435\u0440\u044c \u0438\u0437 \u043f\u0435\u0440\u0435\u043c\u0435\u043d\u043d\u044b\u0445 \u043e\u043a\u0440\u0443\u0436\u0435\u043d\u0438\u044f
  
  if (!botToken || !logChatId) {
    console.log('[LOG] No bot token or log chat ID, skipping log:', message);
    return;
  }
  
  // \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0435\u043c timestamp \u0432\u043c\u0435\u0441\u0442\u043e \u0433\u043b\u043e\u0431\u0430\u043b\u044c\u043d\u043e\u0433\u043e \u0441\u0447\u0435\u0442\u0447\u0438\u043a\u0430
  const logId = Date.now().toString(36).toUpperCase();
  
  const config = {
    info: { icon: '\u2139\ufe0f', title: '\u0418\u041d\u0424\u041e', color: '\u{1f535}' },
    warn: { icon: '\u26a0\ufe0f', title: '\u0412\u041d\u0418\u041c\u0410\u041d\u0418\u0415', color: '\u{1f7e1}' },
    error: { icon: '\u274c', title: '\u041e\u0428\u0418\u0411\u041a\u0410', color: '\u{1f534}' },
    success: { icon: '\u2705', title: '\u0423\u0421\u041f\u0415\u0425', color: '\u{1f7e2}' },
    admin: { icon: '\u{1f451}', title: '\u0410\u0414\u041c\u0418\u041d', color: '\u{1f7e3}' },
    user: { icon: '\u{1f464}', title: '\u041f\u041e\u041b\u042c\u0417\u041e\u0412\u0410\u0422\u0415\u041b\u042c', color: '\u{1f535}' },
    trial: { icon: '\u{1f381}', title: '\u041f\u0420\u041e\u0411\u041d\u042b\u0419 \u041f\u0415\u0420\u0418\u041e\u0414', color: '\u{1f7e2}' },
    license: { icon: '\u{1f48e}', title: '\u041b\u0418\u0426\u0415\u041d\u0417\u0418\u042f', color: '\u{1f7e3}' },
    connect: { icon: '\u{1f517}', title: '\u041f\u0420\u0418\u0412\u042f\u0417\u041a\u0410', color: '\u{1f535}' },
    webhook: { icon: '\u{1f916}', title: 'WEBHOOK', color: '\u26aa' },
    system: { icon: '\u2699\ufe0f', title: '\u0421\u0418\u0421\u0422\u0415\u041c\u0410', color: '\u26aa' }
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
  
  // \u0424\u043e\u0440\u043c\u0438\u0440\u0443\u0435\u043c \u043a\u0440\u0430\u0441\u0438\u0432\u043e\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435
  let text = `${cfg.color} ${cfg.icon} *${cfg.title}* #${logId}\n`;
  text += `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n`;
  text += `${message}\n\n`;
  
  // \u0414\u043e\u0431\u0430\u0432\u043b\u044f\u0435\u043c \u0434\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u0435 \u0434\u0430\u043d\u043d\u044b\u0435 \u0435\u0441\u043b\u0438 \u0435\u0441\u0442\u044c
  if (extra.telegramId) {
    text += `\u{1f194} TG ID: \`${extra.telegramId}\`\n`;
  }
  if (extra.username) {
    text += `\u{1f464} Username: @${extra.username}\n`;
  }
  if (extra.action) {
    text += `\u{1f3af} \u0414\u0435\u0439\u0441\u0442\u0432\u0438\u0435: ${extra.action}\n`;
  }
  if (extra.details) {
    text += `\u{1f4cb} \u0414\u0435\u0442\u0430\u043b\u0438: ${extra.details}\n`;
  }
  
  text += `\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n`;
  text += `\u{1f550} ${time} MSK`;
  
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

// \u0412\u0441\u043f\u043e\u043c\u043e\u0433\u0430\u0442\u0435\u043b\u044c\u043d\u0430\u044f \u0444\u0443\u043d\u043a\u0446\u0438\u044f \u0434\u043b\u044f \u043f\u043e\u043b\u0443\u0447\u0435\u043d\u0438\u044f \u0434\u0430\u043d\u043d\u044b\u0445 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438
async function getLicenseData(env, oderId) {
  // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0434\u0430\u043d\u043d\u044b\u0435 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f \u043f\u043e oderId
  const userData = await env.USERS.get(`user:${oderId}`);
  if (!userData) {
    return { connected: false, error: 'NOT_CONNECTED' };
  }
  
  const user = JSON.parse(userData);
  const telegramId = user.telegramId;
  const username = user.username;
  
  // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e \u043f\u043e Telegram ID (\u0431\u043e\u043b\u0435\u0435 \u043d\u0430\u0434\u0451\u0436\u043d\u043e) \u0438\u043b\u0438 username
  let licenseData = null;
  
  // \u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u0440\u043e\u0431\u0443\u0435\u043c \u043f\u043e telegramId
  if (telegramId) {
    const licenseByTgId = await env.USERS.get(`license:tg:${telegramId}`);
    if (licenseByTgId) {
      licenseData = JSON.parse(licenseByTgId);
    }
  }
  
  // \u0415\u0441\u043b\u0438 \u043d\u0435 \u043d\u0430\u0448\u043b\u0438, \u043f\u0440\u043e\u0431\u0443\u0435\u043c \u043f\u043e username
  if (!licenseData && username) {
    const licenseByUsername = await env.USERS.get(`license:${username}`);
    if (licenseByUsername) {
      licenseData = JSON.parse(licenseByUsername);
    }
  }
  
  // \u0415\u0441\u043b\u0438 \u043d\u0435\u0442 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438 - \u0432\u044b\u0434\u0430\u0451\u043c \u043f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434 \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438 (12 \u0447\u0430\u0441\u043e\u0432 \u0434\u043b\u044f \u043e\u0431\u044b\u0447\u043d\u044b\u0445)
  if (!licenseData) {
    // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c, \u0431\u044b\u043b \u043b\u0438 \u0443\u0436\u0435 \u0432\u044b\u0434\u0430\u043d \u043f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434
    const trialUsed = await env.USERS.get(`trial:${telegramId || username}`);
    
    if (!trialUsed) {
      // \u0412\u044b\u0434\u0430\u0451\u043c \u043f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434 \u043d\u0430 12 \u0447\u0430\u0441\u043e\u0432 (\u0431\u0435\u0437 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u0430)
      const trialExpiresAt = Date.now() + TRIAL_DURATION_NO_REFERRAL;
      const trialHours = Math.round(TRIAL_DURATION_NO_REFERRAL / MS_PER_HOUR);
      licenseData = {
        type: 'trial',
        expiresAt: trialExpiresAt,
        createdAt: Date.now(),
        telegramId: telegramId,
        username: username
      };
      
      // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e
      if (telegramId) {
        await env.USERS.put(`license:tg:${telegramId}`, JSON.stringify(licenseData));
      }
      if (username) {
        await env.USERS.put(`license:${username}`, JSON.stringify(licenseData));
      }
      
      // \u041e\u0442\u043c\u0435\u0447\u0430\u0435\u043c \u0447\u0442\u043e \u043f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434 \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d
      await env.USERS.put(`trial:${telegramId || username}`, 'used');
      
      // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c \u0432\u044b\u0434\u0430\u0447\u0443 \u043f\u0440\u043e\u0431\u043d\u043e\u0433\u043e \u043f\u0435\u0440\u0438\u043e\u0434\u0430
      await logToChat(env, 
        `\u{1f381} *\u041f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434 \u0430\u043a\u0442\u0438\u0432\u0438\u0440\u043e\u0432\u0430\u043d!*\n\n` +
        `\u041d\u043e\u0432\u044b\u0439 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u043f\u043e\u043b\u0443\u0447\u0438\u043b ${trialHours} \u0447\u0430\u0441\u043e\u0432 \u0431\u0435\u0441\u043f\u043b\u0430\u0442\u043d\u043e\u0433\u043e \u0434\u043e\u0441\u0442\u0443\u043f\u0430`,
        'trial',
        {
          telegramId: telegramId || '\u043d\u0435\u0442',
          username: username || '\u043d\u0435\u0442',
          action: '\u0410\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u0432\u044b\u0434\u0430\u0447\u0430 \u0442\u0440\u0438\u0430\u043b\u0430',
          details: `\u0421\u0440\u043e\u043a: ${trialHours} \u0447\u0430\u0441\u043e\u0432`
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
    // Rate limiting \u043f\u043e IP
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `license:${clientIP}`);
    if (!rateLimit.allowed) {
      return jsonResponse({ 
        valid: false, 
        error: 'RATE_LIMITED', 
        message: `\u0421\u043b\u0438\u0448\u043a\u043e\u043c \u043c\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u0440\u043e\u0441\u043e\u0432. \u041f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u0447\u0435\u0440\u0435\u0437 ${rateLimit.resetIn}\u0441` 
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
        message: 'Telegram \u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d' 
      });
    }
    
    const license = result.license;
    
    if (!license) {
      return jsonResponse({ 
        valid: false, 
        error: 'NO_LICENSE',
        message: '\u041b\u0438\u0446\u0435\u043d\u0437\u0438\u044f \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430'
      });
    }
    
    const now = Date.now();
    const isValid = license.expiresAt > now;
    
    // \u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043b\u0438\u043c\u0438\u0442\u0430 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432 (\u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0439 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438)
    if (isValid && deviceId && result.user.telegramId) {
      const deviceCheck = await checkAndRegisterDevice(env, result.user.telegramId, deviceId, license);
      if (!deviceCheck.allowed) {
        return jsonResponse({
          valid: false,
          error: 'DEVICE_LIMIT',
          message: `\u0414\u043e\u0441\u0442\u0438\u0433\u043d\u0443\u0442 \u043b\u0438\u043c\u0438\u0442 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432 (${deviceCheck.maxDevices}). \u041e\u0442\u0432\u044f\u0436\u0438\u0442\u0435 \u0434\u0440\u0443\u0433\u043e\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u0432 \u0431\u043e\u0442\u0435.`,
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
    const rateLimit = await checkRateLimit(env, `code:${clientIP}`, 10); // 10 \u043a\u043e\u0434\u043e\u0432 \u0432 \u043c\u0438\u043d\u0443\u0442\u0443 \u043c\u0430\u043a\u0441
    if (!rateLimit.allowed) {
      return jsonResponse({ 
        error: 'RATE_LIMITED', 
        message: `\u0421\u043b\u0438\u0448\u043a\u043e\u043c \u043c\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u0440\u043e\u0441\u043e\u0432. \u041f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u0447\u0435\u0440\u0435\u0437 ${rateLimit.resetIn}\u0441` 
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
    
    // \u0412\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u044f oderId
    if (typeof oderId !== 'string' || oderId.length < 16 || oderId.length > 64) {
      return jsonResponse({ error: 'Invalid oderId format' }, 400);
    }
    
    // \u0412\u0410\u0416\u041d\u041e: \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u0441\u0442\u0430\u0440\u0443\u044e \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0443 \u043f\u0440\u0438 \u0433\u0435\u043d\u0435\u0440\u0430\u0446\u0438\u0438 \u043d\u043e\u0432\u043e\u0433\u043e \u043a\u043e\u0434\u0430
    // \u042d\u0442\u043e \u043d\u0443\u0436\u043d\u043e \u0447\u0442\u043e\u0431\u044b \u043f\u0440\u0438 \u043f\u0435\u0440\u0435\u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0435 \u043a \u0434\u0440\u0443\u0433\u043e\u043c\u0443 Telegram \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u0443
    // \u0441\u0442\u0430\u0442\u0443\u0441 \u043f\u043e\u043a\u0430\u0437\u044b\u0432\u0430\u043b "\u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e" \u0434\u043e \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0438\u044f \u043d\u043e\u0432\u043e\u0439 \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0438
    const oldUserData = await env.USERS.get(`user:${oderId}`);
    if (oldUserData) {
      const oldUser = JSON.parse(oldUserData);
      // \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u043e\u0431\u0440\u0430\u0442\u043d\u0443\u044e \u0441\u0432\u044f\u0437\u044c tg -> oderId
      if (oldUser.telegramId) {
        await env.USERS.delete(`tg:${oldUser.telegramId}`);
      }
      // \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0443 user
      await env.USERS.delete(`user:${oderId}`);
      
      // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
      await logToChat(env,
        `\u{1f504} *\u0421\u0431\u0440\u043e\u0441 \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0438*\n\n` +
        `\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u0437\u0430\u043f\u0440\u043e\u0441\u0438\u043b \u043d\u043e\u0432\u044b\u0439 \u043a\u043e\u0434, \u0441\u0442\u0430\u0440\u0430\u044f \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0430 \u0443\u0434\u0430\u043b\u0435\u043d\u0430`,
        'connect',
        {
          telegramId: oldUser.telegramId || '\u043d\u0435\u0442',
          username: oldUser.username || '\u043d\u0435\u0442',
          action: '\u0413\u0435\u043d\u0435\u0440\u0430\u0446\u0438\u044f \u043d\u043e\u0432\u043e\u0433\u043e \u043a\u043e\u0434\u0430',
          details: '\u0421\u0442\u0430\u0440\u0430\u044f \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0430 \u0443\u0434\u0430\u043b\u0435\u043d\u0430'
        }
      );
    }
    
    const code = generateCode();
    
    // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043a\u043e\u0434 \u0441 \u0434\u0430\u043d\u043d\u044b\u043c\u0438 \u0434\u043b\u044f \u0441\u0432\u044f\u0437\u043a\u0438
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

// \u042d\u043d\u0434\u043f\u043e\u0438\u043d\u0442 \u0434\u043b\u044f \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0438 Telegram (\u0432\u044b\u0437\u044b\u0432\u0430\u0435\u0442\u0441\u044f \u0431\u043e\u0442\u043e\u043c)
async function handleTelegramConnect(request, env) {
  try {
    // Rate limiting \u0434\u043b\u044f \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u0439
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `connect:${clientIP}`, 20); // 20 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u0439 \u0432 \u043c\u0438\u043d\u0443\u0442\u0443
    if (!rateLimit.allowed) {
      return jsonResponse({ error: 'RATE_LIMITED', message: `\u0421\u043b\u0438\u0448\u043a\u043e\u043c \u043c\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u0440\u043e\u0441\u043e\u0432. \u041f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u0447\u0435\u0440\u0435\u0437 ${rateLimit.resetIn}\u0441` }, 429);
    }
    
    const { data, error } = await safeParseJson(request);
    if (error) {
      return jsonResponse({ error: 'INVALID_REQUEST', message: error }, 400);
    }
    
    const { code, telegramId, username, firstName } = data || {};
    
    if (!code || !telegramId) {
      return jsonResponse({ error: 'code and telegramId are required' }, 400);
    }
    
    // \u0412\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u044f \u043a\u043e\u0434\u0430 (\u0442\u043e\u043b\u044c\u043a\u043e \u0431\u0443\u043a\u0432\u044b \u0438 \u0446\u0438\u0444\u0440\u044b, 8 \u0441\u0438\u043c\u0432\u043e\u043b\u043e\u0432)
    if (!/^[A-Z0-9]{8}$/i.test(code)) {
      return jsonResponse({ error: 'Invalid code format' }, 400);
    }
    
    // \u0412\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u044f telegramId (\u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u0447\u0438\u0441\u043b\u043e\u043c)
    if (!/^\d+$/.test(String(telegramId))) {
      return jsonResponse({ error: 'Invalid telegramId' }, 400);
    }
    
    // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0434\u0430\u043d\u043d\u044b\u0435 \u043f\u043e \u043a\u043e\u0434\u0443
    const codeData = await env.USERS.get(`code:${code}`);
    if (!codeData) {
      return jsonResponse({ error: 'Code expired or invalid', code: 'INVALID_CODE' }, 400);
    }
    
    const { oderId, deviceId } = JSON.parse(codeData);
    
    // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u0441\u0432\u044f\u0437\u043a\u0443 user -> telegram
    const userData = {
      oderId,
      deviceId,
      telegramId,
      username: username || null,
      firstName: firstName || null,
      connectedAt: Date.now()
    };
    
    await env.USERS.put(`user:${oderId}`, JSON.stringify(userData));
    
    // \u0422\u0430\u043a\u0436\u0435 \u0441\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043e\u0431\u0440\u0430\u0442\u043d\u0443\u044e \u0441\u0432\u044f\u0437\u044c telegram -> oderId \u0434\u043b\u044f \u0431\u044b\u0441\u0442\u0440\u043e\u0433\u043e \u043f\u043e\u0438\u0441\u043a\u0430
    await env.USERS.put(`tg:${telegramId}`, oderId);
    
    // \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d\u043d\u044b\u0439 \u043a\u043e\u0434
    await env.USERS.delete(`code:${code}`);
    
    // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c/\u0432\u044b\u0434\u0430\u0451\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e
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

// \u042d\u043d\u0434\u043f\u043e\u0438\u043d\u0442 \u0434\u043b\u044f \u0432\u044b\u0434\u0430\u0447\u0438/\u043f\u0440\u043e\u0434\u043b\u0435\u043d\u0438\u044f \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438 (\u0430\u0434\u043c\u0438\u043d\u0441\u043a\u0438\u0439)
async function handleGrantLicense(request, env) {
  try {
    const authHeader = request.headers.get('Authorization');
    const adminToken = env.ADMIN_TOKEN;
    
    if (!adminToken || authHeader !== `Bearer ${adminToken}`) {
      // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c \u043f\u043e\u043f\u044b\u0442\u043a\u0443 \u043d\u0435\u0441\u0430\u043d\u043a\u0446\u0438\u043e\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u043e\u0433\u043e \u0434\u043e\u0441\u0442\u0443\u043f\u0430
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
    
    // \u0412\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u044f days
    if (!days || !Number.isInteger(days) || days <= 0 || days > 365) {
      return jsonResponse({ error: 'days must be positive integer (1-365)' }, 400);
    }
    
    // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0442\u0435\u043a\u0443\u0449\u0443\u044e \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e \u0435\u0441\u043b\u0438 \u0435\u0441\u0442\u044c
    let currentLicense = null;
    if (telegramId) {
      const existing = await env.USERS.get(`license:tg:${telegramId}`);
      if (existing) currentLicense = JSON.parse(existing);
    }
    if (!currentLicense && username) {
      const existing = await env.USERS.get(`license:${username}`);
      if (existing) currentLicense = JSON.parse(existing);
    }
    
    // \u0412\u044b\u0447\u0438\u0441\u043b\u044f\u0435\u043c \u043d\u043e\u0432\u0443\u044e \u0434\u0430\u0442\u0443 \u0438\u0441\u0442\u0435\u0447\u0435\u043d\u0438\u044f
    let baseTime = Date.now();
    
    // \u0415\u0441\u043b\u0438 \u0435\u0441\u0442\u044c \u0430\u043a\u0442\u0438\u0432\u043d\u0430\u044f \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044f - \u043f\u0440\u043e\u0434\u043b\u0435\u0432\u0430\u0435\u043c \u043e\u0442 \u043d\u0435\u0451
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
    
    // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043f\u043e \u043e\u0431\u043e\u0438\u043c \u043a\u043b\u044e\u0447\u0430\u043c
    if (telegramId) {
      await env.USERS.put(`license:tg:${telegramId}`, JSON.stringify(licenseData));
      // \u0414\u043e\u0431\u0430\u0432\u043b\u044f\u0435\u043c \u0432 \u043e\u0447\u0435\u0440\u0435\u0434\u044c \u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0439
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

// ========== \u0421\u0418\u0421\u0422\u0415\u041c\u0410 \u0413\u0420\u0423\u041f\u041f\u041e\u0412\u042b\u0425 \u0427\u0410\u0422\u041e\u0412 ==========

// \u041d\u043e\u0432\u0430\u044f \u0441\u0438\u0441\u0442\u0435\u043c\u0430 \u0443\u0440\u043e\u0432\u043d\u0435\u0439 \u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a:
// trial (\u041f\u0440\u043e\u0431\u043d\u0438\u043a) - 1 \u0434\u0435\u043d\u044c, \u0442\u043e\u043b\u044c\u043a\u043e \u041b\u0421
// level1 (\u0423\u0440\u043e\u0432\u0435\u043d\u044c 1) - \u0442\u043e\u043b\u044c\u043a\u043e \u041b\u0421 \u0441 \u0431\u043e\u0442\u043e\u043c
// level2 (\u0423\u0440\u043e\u0432\u0435\u043d\u044c 2) - \u041b\u0421 + \u043e\u0431\u0449\u0438\u0439 \u0447\u0430\u0442, \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u043e\u0431\u0449\u0438\u0439 \u0447\u0430\u0442
// level3 (\u0423\u0440\u043e\u0432\u0435\u043d\u044c 3) - \u043f\u043e\u043b\u043d\u044b\u0439 \u0444\u0443\u043d\u043a\u0446\u0438\u043e\u043d\u0430\u043b, \u0432\u044b\u0431\u043e\u0440 \u043a\u0443\u0434\u0430 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f

const LICENSE_TYPES = {
  trial: { 
    name: '\u{1f381} \u041f\u0440\u043e\u0431\u043d\u044b\u0439', 
    level: 0,
    groupChats: false, 
    maxDevices: 1, 
    maxChats: 0,
    notifyOptions: ['dm'],  // \u0422\u043e\u043b\u044c\u043a\u043e \u041b\u0421
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: true,
      groupNotify: false,
      selectNotify: false
    },
    description: '\u0414\u043e 24\u0447 \u2022 \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0432 \u041b\u0421'
  },
  level1: { 
    name: '\u2b50 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 1', 
    level: 1,
    groupChats: false, 
    maxDevices: 1, 
    maxChats: 0,
    notifyOptions: ['dm'],  // \u0422\u043e\u043b\u044c\u043a\u043e \u041b\u0421
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: true,
      groupNotify: false,
      selectNotify: false
    },
    description: '1 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u2022 \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0432 \u041b\u0421'
  },
  level2: { 
    name: '\u2b50\u2b50 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 2', 
    level: 2,
    groupChats: true, 
    maxDevices: 1, 
    maxChats: 1,
    notifyOptions: ['group'],  // \u0422\u043e\u043b\u044c\u043a\u043e \u0432 \u043e\u0431\u0449\u0438\u0439 \u0447\u0430\u0442
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: false,
      groupNotify: true,
      selectNotify: false
    },
    description: '\u041b\u0421 + \u043e\u0431\u0449\u0438\u0439 \u0447\u0430\u0442 \u2022 \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0432 \u0447\u0430\u0442'
  },
  level3: { 
    name: '\u2b50\u2b50\u2b50 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 3', 
    level: 3,
    groupChats: true, 
    maxDevices: 2, 
    maxChats: 1,
    notifyOptions: ['dm', 'group', 'both'],  // \u0412\u044b\u0431\u043e\u0440
    features: {
      autoclick: true,
      notifications: true,
      stats: true,
      dmNotify: true,
      groupNotify: true,
      selectNotify: true,
      priority: true
    },
    description: '2 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430 \u2022 \u0412\u044b\u0431\u043e\u0440 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439'
  }
};

// \u0410\u043b\u0438\u0430\u0441\u044b \u0434\u043b\u044f \u043e\u0431\u0440\u0430\u0442\u043d\u043e\u0439 \u0441\u043e\u0432\u043c\u0435\u0441\u0442\u0438\u043c\u043e\u0441\u0442\u0438
LICENSE_TYPES.basic = LICENSE_TYPES.level1;
LICENSE_TYPES.subscription = LICENSE_TYPES.level1;
LICENSE_TYPES.premium = LICENSE_TYPES.level2;
LICENSE_TYPES.max = LICENSE_TYPES.level3;

// ========== \u041d\u0410\u0421\u0422\u0420\u041e\u0419\u041a\u0418 \u0423\u0412\u0415\u0414\u041e\u041c\u041b\u0415\u041d\u0418\u0419 \u041f\u041e\u041b\u042c\u0417\u041e\u0412\u0410\u0422\u0415\u041b\u042f \u0412 \u0411\u041e\u0422\u0415 ==========

const DEFAULT_USER_SETTINGS = {
  notifications: {
    clicks: true,          // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u043a\u043b\u0438\u043a\u0430\u0445 (\u0432\u0437\u044f\u0442\u044b\u0445 \u0437\u0430\u044f\u0432\u043a\u0430\u0445)
    expiryReminders: true, // \u041d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u044f \u043e\u0431 \u0438\u0441\u0442\u0435\u0447\u0435\u043d\u0438\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438
    news: true,            // \u041d\u043e\u0432\u043e\u0441\u0442\u0438 \u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f
    sound: true            // \u0417\u0432\u0443\u043a \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439 (\u0432 Telegram \u043d\u0435 \u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442, \u043d\u043e \u0434\u043b\u044f \u0441\u043e\u0432\u043c\u0435\u0441\u0442\u0438\u043c\u043e\u0441\u0442\u0438)
  },
  // \u041a\u0443\u0434\u0430 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f: 'dm' | 'group' | 'both'
  notifyDestination: 'dm',
  createdAt: null,
  updatedAt: null
};

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
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

// \u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
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

// \u041f\u0435\u0440\u0435\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0443 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439
async function toggleNotificationSetting(env, telegramId, settingKey) {
  const settings = await getUserSettings(env, telegramId);
  if (settings.notifications.hasOwnProperty(settingKey)) {
    settings.notifications[settingKey] = !settings.notifications[settingKey];
    await saveUserSettings(env, telegramId, settings);
  }
  return settings;
}

// ========== \u0413\u0420\u0423\u041f\u041f\u041e\u0412\u042b\u0415 \u0427\u0410\u0422\u042b ==========

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0445 \u0447\u0430\u0442\u043e\u0432 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
async function getUserGroupChats(env, telegramId) {
  const data = await env.USERS.get(`groupchats:${telegramId}`);
  if (!data) return { chats: [], pending: [] };
  return JSON.parse(data);
}

// \u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0445 \u0447\u0430\u0442\u043e\u0432
async function saveUserGroupChats(env, telegramId, groupData) {
  await env.USERS.put(`groupchats:${telegramId}`, JSON.stringify(groupData));
}

// \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u043f\u0440\u0430\u0432\u043e \u043d\u0430 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b
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

// \u0417\u0430\u043f\u0440\u043e\u0441 \u043d\u0430 \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u0431\u043e\u0442\u0430 \u0432 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 (\u043e\u0436\u0438\u0434\u0430\u0435\u0442 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u044f \u0430\u0434\u043c\u0438\u043d\u0430)
async function requestGroupChatApproval(env, telegramId, chatId, chatTitle, requestedBy) {
  const pendingKey = `pending_group:${chatId}`;
  
  // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043d\u0435\u0442 \u043b\u0438 \u0443\u0436\u0435 \u0437\u0430\u043f\u0440\u043e\u0441\u0430
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
  
  await env.USERS.put(pendingKey, JSON.stringify(requestData), { expirationTtl: 7 * 24 * 60 * 60 }); // 7 \u0434\u043d\u0435\u0439
  
  // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c ID \u0430\u0434\u043c\u0438\u043d\u0430 \u0434\u043b\u044f \u043f\u0440\u044f\u043c\u043e\u0433\u043e \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f
  const adminUsername = env.ADMIN_USERNAME || 'YaMob';
  const adminTelegramId = env.ADMIN_TELEGRAM_ID; // \u041d\u0443\u0436\u043d\u043e \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0432 \u043f\u0435\u0440\u0435\u043c\u0435\u043d\u043d\u044b\u0435 \u043e\u043a\u0440\u0443\u0436\u0435\u043d\u0438\u044f
  
  const approvalMsg = `\u{1f4e8} *\u0417\u0430\u043f\u0440\u043e\u0441 \u043d\u0430 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442*\n\n` +
    `\u{1f464} \u041e\u0442: @${requestedBy || telegramId}\n` +
    `\u{1f194} TG ID: \`${telegramId}\`\n` +
    `\u{1f4ac} \u0427\u0430\u0442: "${escapeMarkdown(chatTitle)}"\n` +
    `\u{1f194} Chat ID: \`${chatId}\`\n\n` +
    `\u2705 \u0414\u043b\u044f \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u044f:\n\`/approve\\_group ${chatId}\`\n\n` +
    `\u274c \u0414\u043b\u044f \u043e\u0442\u043a\u043b\u043e\u043d\u0435\u043d\u0438\u044f:\n\`/reject\\_group ${chatId}\``;
  
  // \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435 \u0430\u0434\u043c\u0438\u043d\u0443 \u043d\u0430\u043f\u0440\u044f\u043c\u0443\u044e (\u0435\u0441\u043b\u0438 \u0435\u0441\u0442\u044c ADMIN_TELEGRAM_ID)
  if (adminTelegramId) {
    try {
      await sendTelegramMessage(env, adminTelegramId, approvalMsg, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: '\u2705 \u041e\u0434\u043e\u0431\u0440\u0438\u0442\u044c', callback_data: `approve_group:${chatId}` },
              { text: '\u274c \u041e\u0442\u043a\u043b\u043e\u043d\u0438\u0442\u044c', callback_data: `reject_group:${chatId}` }
            ]
          ]
        }
      });
    } catch (e) {
      console.error('Failed to send approval notification to admin:', e);
    }
  }
  
  // \u0422\u0430\u043a\u0436\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0432 \u043b\u043e\u0433-\u0447\u0430\u0442 (\u0435\u0441\u043b\u0438 \u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043d)
  await logToChat(env,
    `\u{1f4e8} *\u0417\u0430\u043f\u0440\u043e\u0441 \u043d\u0430 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442*\n\n` +
    `\u{1f464} \u041e\u0442: @${requestedBy || telegramId}\n` +
    `\u{1f4ac} \u0427\u0430\u0442: "${chatTitle}"\n` +
    `\u{1f194} Chat ID: \`${chatId}\`\n\n` +
    `\u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\u0442\u0435 /approve_group ${chatId} \u0434\u043b\u044f \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u044f`,
    'admin',
    { telegramId, action: '\u0417\u0430\u043f\u0440\u043e\u0441 \u0433\u0440\u0443\u043f\u043f\u044b' }
  );
  
  return { success: true };
}

// \u041e\u0434\u043e\u0431\u0440\u0438\u0442\u044c \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 (\u0442\u043e\u043b\u044c\u043a\u043e \u0430\u0434\u043c\u0438\u043d)
async function approveGroupChat(env, chatId, approvedBy) {
  const pendingKey = `pending_group:${chatId}`;
  const pendingData = await env.USERS.get(pendingKey);
  
  if (!pendingData) {
    return { success: false, error: 'REQUEST_NOT_FOUND' };
  }
  
  const request = JSON.parse(pendingData);
  const telegramId = request.requestedBy;
  
  // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0442\u0435\u043a\u0443\u0449\u0438\u0435 \u0447\u0430\u0442\u044b \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
  const groupData = await getUserGroupChats(env, telegramId);
  
  // \u0414\u043e\u0431\u0430\u0432\u043b\u044f\u0435\u043c \u0447\u0430\u0442
  groupData.chats.push({
    chatId: chatId,
    chatTitle: request.chatTitle,
    approvedAt: Date.now(),
    approvedBy: approvedBy
  });
  
  // \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u0438\u0437 pending
  groupData.pending = (groupData.pending || []).filter(p => p.chatId !== chatId);
  
  await saveUserGroupChats(env, telegramId, groupData);
  await env.USERS.delete(pendingKey);
  
  // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043e\u0431\u0440\u0430\u0442\u043d\u0443\u044e \u0441\u0432\u044f\u0437\u044c \u0447\u0430\u0442 -> \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c
  await env.USERS.put(`group_owner:${chatId}`, telegramId);
  
  return { success: true, telegramId };
}

// ========== \u0421\u0422\u0410\u0422\u0418\u0421\u0422\u0418\u041a\u0410 \u041f\u041e\u041b\u042c\u0417\u041e\u0412\u0410\u0422\u0415\u041b\u042f ==========

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0443 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
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

// \u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0443 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
async function saveUserStats(env, telegramId, stats) {
  await env.USERS.put(`stats:${telegramId}`, JSON.stringify(stats));
}

// \u0417\u0430\u043f\u0438\u0441\u0430\u0442\u044c \u043a\u043b\u0438\u043a \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
async function recordUserClick(env, telegramId) {
  const stats = await getUserStats(env, telegramId);
  const today = new Date().toISOString().slice(0, 10);
  
  // \u0415\u0441\u043b\u0438 \u043d\u043e\u0432\u044b\u0439 \u0434\u0435\u043d\u044c - \u0441\u0431\u0440\u0430\u0441\u044b\u0432\u0430\u0435\u043c todayClicks
  if (stats.lastClickDate !== today) {
    stats.todayClicks = 0;
    stats.lastClickDate = today;
  }
  
  stats.totalClicks++;
  stats.todayClicks++;
  
  await saveUserStats(env, telegramId, stats);
  return stats;
}

// ========== \u0420\u0415\u0424\u0415\u0420\u0410\u041b\u042c\u041d\u0410\u042f \u0421\u0418\u0421\u0422\u0415\u041c\u0410 ==========

// \u0413\u0435\u043d\u0435\u0440\u0438\u0440\u043e\u0432\u0430\u0442\u044c \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434
function generateReferralCode(telegramId) {
  const hash = String(telegramId).split('').reduce((a, b) => {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);
  return REFERRAL_CODE_PREFIX + Math.abs(hash).toString(36).toUpperCase().slice(0, 6);
}

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f (\u0441\u043e\u0437\u0434\u0430\u0451\u0442 \u0435\u0441\u043b\u0438 \u043d\u0435\u0442)
async function getUserReferralCode(env, telegramId) {
  let stats = await getUserStats(env, telegramId);
  
  if (!stats.referralCode) {
    stats.referralCode = generateReferralCode(telegramId);
    await saveUserStats(env, telegramId, stats);
  }
  
  // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u0441\u0432\u044f\u0437\u044c \u043a\u043e\u0434 -> telegramId
  await env.USERS.put(`refcode:${stats.referralCode}`, telegramId);
  
  return stats.referralCode;
}

// \u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434
async function applyReferralCode(env, newUserTelegramId, referralCode) {
  // \u041d\u0430\u0445\u043e\u0434\u0438\u043c \u0432\u043b\u0430\u0434\u0435\u043b\u044c\u0446\u0430 \u043a\u043e\u0434\u0430
  const referrerTelegramId = await env.USERS.get(`refcode:${referralCode}`);
  if (!referrerTelegramId) {
    return { success: false, error: 'INVALID_CODE' };
  }
  
  if (referrerTelegramId === newUserTelegramId) {
    return { success: false, error: 'SELF_REFERRAL' };
  }
  
  // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c, \u043d\u0435 \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043b \u043b\u0438 \u043d\u043e\u0432\u044b\u0439 \u044e\u0437\u0435\u0440 \u0443\u0436\u0435 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434
  const newUserStats = await getUserStats(env, newUserTelegramId);
  if (newUserStats.referredBy) {
    return { success: false, error: 'ALREADY_REFERRED' };
  }
  
  // \u0417\u0430\u043f\u0438\u0441\u044b\u0432\u0430\u0435\u043c \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u0430 (\u0431\u043e\u043d\u0443\u0441 \u041d\u0415 \u0434\u0430\u0451\u043c - \u0442\u043e\u043b\u044c\u043a\u043e \u043a\u043e\u0433\u0434\u0430 \u043e\u0444\u043e\u0440\u043c\u0438\u0442 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443)
  newUserStats.referredBy = referrerTelegramId;
  newUserStats.referralBonusPending = true; // \u0411\u043e\u043d\u0443\u0441 \u043e\u0436\u0438\u0434\u0430\u0435\u0442 \u043e\u0444\u043e\u0440\u043c\u043b\u0435\u043d\u0438\u044f \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438
  await saveUserStats(env, newUserTelegramId, newUserStats);
  
  // \u0423\u0432\u0435\u043b\u0438\u0447\u0438\u0432\u0430\u0435\u043c \u0441\u0447\u0451\u0442\u0447\u0438\u043a \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u043e\u0432 \u0443 \u0440\u0435\u0444\u0435\u0440\u0435\u0440\u0430 (\u043f\u0435\u0440\u0435\u0445\u043e\u0434\u043e\u0432)
  const referrerStats = await getUserStats(env, referrerTelegramId);
  referrerStats.referralCount = (referrerStats.referralCount || 0) + 1;
  referrerStats.referralPendingBonus = (referrerStats.referralPendingBonus || 0) + 1; // \u041e\u0436\u0438\u0434\u0430\u044e\u0449\u0438\u0445 \u0431\u043e\u043d\u0443\u0441\u043e\u0432
  await saveUserStats(env, referrerTelegramId, referrerStats);
  
  // \u041d\u0415 \u0434\u0430\u0451\u043c \u0431\u043e\u043d\u0443\u0441 \u0440\u0435\u0444\u0435\u0440\u0435\u0440\u0443 \u0441\u0440\u0430\u0437\u0443 - \u0442\u043e\u043b\u044c\u043a\u043e \u043f\u0440\u0438 \u043e\u0444\u043e\u0440\u043c\u043b\u0435\u043d\u0438\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438
  
  // \u0414\u0430\u0451\u043c \u043f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434 \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0451\u043d\u043d\u043e\u043c\u0443 \u0422\u041e\u041b\u042c\u041a\u041e \u0435\u0441\u043b\u0438 \u044d\u0442\u043e \u043d\u043e\u0432\u044b\u0439 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u0431\u0435\u0437 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438 (24 \u0447\u0430\u0441\u0430)
  const newUserOderId = await env.USERS.get(`tg:${newUserTelegramId}`);
  let gaveTrialToNewUser = false;
  
  if (newUserOderId) {
    const newUserResult = await getLicenseData(env, newUserOderId);
    // \u0414\u0430\u0451\u043c \u043f\u0440\u043e\u0431\u043d\u0438\u043a \u0442\u043e\u043b\u044c\u043a\u043e \u0435\u0441\u043b\u0438 \u041d\u0415\u0422 \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0439 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438
    if (!newUserResult.license || newUserResult.license.expiresAt < Date.now()) {
      const trialLicense = {
        type: 'trial',
        active: true,
        expiresAt: Date.now() + REFERRAL_TRIAL_HOURS * MS_PER_HOUR, // 24 \u0447\u0430\u0441\u0430 \u0434\u043b\u044f \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0451\u043d\u043d\u044b\u0445
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

// \u0424\u0443\u043d\u043a\u0446\u0438\u044f \u0434\u043b\u044f \u0432\u044b\u0434\u0430\u0447\u0438 \u0431\u043e\u043d\u0443\u0441\u0430 \u0440\u0435\u0444\u0435\u0440\u0435\u0440\u0443 \u043a\u043e\u0433\u0434\u0430 \u0440\u0435\u0444\u0435\u0440\u0430\u043b \u043e\u0444\u043e\u0440\u043c\u0438\u043b \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443 (+24 \u0447\u0430\u0441\u0430)
async function grantReferralBonusToReferrer(env, referralTelegramId) {
  const userStats = await getUserStats(env, referralTelegramId);
  
  // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0435\u0441\u0442\u044c \u043b\u0438 \u0440\u0435\u0444\u0435\u0440\u0435\u0440 \u0438 \u043e\u0436\u0438\u0434\u0430\u044e\u0449\u0438\u0439 \u0431\u043e\u043d\u0443\u0441
  if (!userStats.referredBy || !userStats.referralBonusPending) {
    return { success: false, reason: 'NO_PENDING_BONUS' };
  }
  
  const referrerTelegramId = userStats.referredBy;
  
  // \u0421\u043d\u0438\u043c\u0430\u0435\u043c \u0444\u043b\u0430\u0433 \u043e\u0436\u0438\u0434\u0430\u043d\u0438\u044f
  userStats.referralBonusPending = false;
  await saveUserStats(env, referralTelegramId, userStats);
  
  // \u0423\u043c\u0435\u043d\u044c\u0448\u0430\u0435\u043c \u0441\u0447\u0451\u0442\u0447\u0438\u043a \u043e\u0436\u0438\u0434\u0430\u044e\u0449\u0438\u0445 \u0443 \u0440\u0435\u0444\u0435\u0440\u0435\u0440\u0430
  const referrerStats = await getUserStats(env, referrerTelegramId);
  referrerStats.referralPendingBonus = Math.max(0, (referrerStats.referralPendingBonus || 0) - 1);
  referrerStats.referralPaidBonus = (referrerStats.referralPaidBonus || 0) + 1; // \u041e\u043f\u043b\u0430\u0447\u0435\u043d\u043d\u044b\u0445
  await saveUserStats(env, referrerTelegramId, referrerStats);
  
  // \u0414\u0430\u0451\u043c \u0431\u043e\u043d\u0443\u0441 \u0440\u0435\u0444\u0435\u0440\u0435\u0440\u0443 (+24 \u0447\u0430\u0441\u0430)
  const oderId = await env.USERS.get(`tg:${referrerTelegramId}`);
  if (oderId) {
    const result = await getLicenseData(env, oderId);
    if (result.license) {
      const newExpiry = Math.max(result.license.expiresAt, Date.now()) + REFERRAL_BONUS_HOURS * MS_PER_HOUR;
      result.license.expiresAt = newExpiry;
      result.license.updatedAt = Date.now();
      await env.USERS.put(`license:tg:${referrerTelegramId}`, JSON.stringify(result.license));
    } else {
      // \u0415\u0441\u043b\u0438 \u043d\u0435\u0442 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438 - \u0434\u0430\u0451\u043c \u043f\u0440\u043e\u0431\u043d\u0438\u043a \u043d\u0430 24 \u0447\u0430\u0441\u0430
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

// ========== \u041d\u0410\u041f\u041e\u041c\u0418\u041d\u0410\u041d\u0418\u042f \u041e\u0411 \u0418\u0421\u0422\u0415\u0427\u0415\u041d\u0418\u0418 ==========

// \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u0438 \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u044f (\u0432\u044b\u0437\u044b\u0432\u0430\u0435\u0442\u0441\u044f \u043f\u043e cron)
async function checkAndSendExpirationReminders(env) {
  // \u042d\u0442\u0430 \u0444\u0443\u043d\u043a\u0446\u0438\u044f \u0432\u044b\u0437\u044b\u0432\u0430\u0435\u0442\u0441\u044f \u0447\u0435\u0440\u0435\u0437 Cloudflare Cron Triggers
  // \u0414\u043b\u044f \u0440\u0430\u0431\u043e\u0442\u044b \u043d\u0443\u0436\u043d\u043e \u043d\u0430\u0441\u0442\u0440\u043e\u0438\u0442\u044c cron \u0432 wrangler.toml
  
  const remindDays = [3, 1, 0]; // \u0417\u0430 3 \u0434\u043d\u044f, \u0437\u0430 1 \u0434\u0435\u043d\u044c, \u0438 \u0432 \u0434\u0435\u043d\u044c \u0438\u0441\u0442\u0435\u0447\u0435\u043d\u0438\u044f
  const now = Date.now();
  const botToken = env.TELEGRAM_BOT_TOKEN;
  
  if (!botToken) return { sent: 0 };
  
  let sentCount = 0;
  
  // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0441\u043f\u0438\u0441\u043e\u043a \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439 \u0434\u043b\u044f \u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u044f
  // (\u0432 \u0440\u0435\u0430\u043b\u044c\u043d\u043e\u0441\u0442\u0438 \u043d\u0443\u0436\u0435\u043d list operation \u0434\u043b\u044f KV, \u043d\u043e \u043e\u043d \u043f\u043b\u0430\u0442\u043d\u044b\u0439)
  // \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0435\u043c \u043e\u0442\u0441\u043b\u0435\u0436\u0438\u0432\u0430\u043d\u0438\u0435 \u0447\u0435\u0440\u0435\u0437 \u043e\u0442\u0434\u0435\u043b\u044c\u043d\u044b\u0439 \u043a\u043b\u044e\u0447
  
  const reminderQueue = await env.USERS.get('reminder_queue');
  if (!reminderQueue) return { sent: 0 };
  
  const queue = JSON.parse(reminderQueue);
  const newQueue = [];
  
  for (const item of queue) {
    const { telegramId, expiresAt, lastReminder, expiredNotified } = item;
    const daysLeft = Math.ceil((expiresAt - now) / MS_PER_DAY);
    
    // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0438\u0441\u0442\u0435\u043a\u043b\u0430 \u043b\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0438 \u043d\u0443\u0436\u043d\u043e \u043b\u0438 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435
    if (expiresAt <= now && !expiredNotified) {
      // \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0438\u0441\u0442\u0435\u043a\u043b\u0430 - \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435
      try {
        await sendTelegramMessage(env, telegramId, 
          `\u274c *\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0437\u0430\u043a\u043e\u043d\u0447\u0438\u043b\u0430\u0441\u044c*\n\n` +
          `\u0412\u0430\u0448\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0438\u0441\u0442\u0435\u043a\u043b\u0430.\n` +
          `\u0410\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440 \u0431\u043e\u043b\u044c\u0448\u0435 \u043d\u0435 \u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442.\n\n` +
          `\u{1f4e9} \u0414\u043b\u044f \u043f\u0440\u043e\u0434\u043b\u0435\u043d\u0438\u044f \u0441\u0432\u044f\u0436\u0438\u0442\u0435\u0441\u044c \u0441\u043e \u043c\u043d\u043e\u0439:\n` +
          `@YaMob`,
          {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4ac} \u041d\u0430\u043f\u0438\u0441\u0430\u0442\u044c @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          }
        );
        item.expiredNotified = true;
        sentCount++;
      } catch (e) {
        console.error('Expiration notification error:', e);
      }
      // \u041e\u0441\u0442\u0430\u0432\u043b\u044f\u0435\u043c \u0432 \u043e\u0447\u0435\u0440\u0435\u0434\u0438 \u0447\u0442\u043e\u0431\u044b \u043d\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0442\u044c \u043f\u043e\u0432\u0442\u043e\u0440\u043d\u043e
      newQueue.push(item);
      continue;
    }
    
    // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043d\u0443\u0436\u043d\u043e \u043b\u0438 \u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 (\u0434\u043e \u0438\u0441\u0442\u0435\u0447\u0435\u043d\u0438\u044f)
    for (const reminderDay of remindDays) {
      if (daysLeft === reminderDay && reminderDay > 0) {
        const reminderKey = `${telegramId}_${reminderDay}`;
        if (lastReminder !== reminderKey) {
          // \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435
          try {
            await sendTelegramMessage(env, telegramId, 
              `\u23f0 *\u041d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u043e \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0435*\n\n` +
              `\u0412\u0430\u0448\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0438\u0441\u0442\u0435\u043a\u0430\u0435\u0442 \u0447\u0435\u0440\u0435\u0437 *${daysLeft}* ${getDaysWord(daysLeft)}!\n\n` +
              `\u041f\u0440\u043e\u0434\u043b\u0438\u0442\u0435 \u0441\u0435\u0439\u0447\u0430\u0441, \u0447\u0442\u043e\u0431\u044b \u043d\u0435 \u043f\u043e\u0442\u0435\u0440\u044f\u0442\u044c \u0434\u043e\u0441\u0442\u0443\u043f \u043a \u0430\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440\u0443.`,
              {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '\u{1f4b3} \u041f\u0440\u043e\u0434\u043b\u0438\u0442\u044c', callback_data: 'buy' }],
                    [{ text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' }]
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
    
    // \u0415\u0441\u043b\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0435\u0449\u0451 \u043d\u0435 \u0438\u0441\u0442\u0435\u043a\u043b\u0430 \u0438\u043b\u0438 \u0443\u0436\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435 \u043e\u0431 \u0438\u0441\u0442\u0435\u0447\u0435\u043d\u0438\u0438 - \u043e\u0441\u0442\u0430\u0432\u043b\u044f\u0435\u043c
    // \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u0442\u043e\u043b\u044c\u043a\u043e \u0442\u0435\u0445, \u0443 \u043a\u043e\u0433\u043e \u0438\u0441\u0442\u0435\u043a\u043b\u043e \u0438 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e > 7 \u0434\u043d\u0435\u0439 \u043d\u0430\u0437\u0430\u0434
    const expiredMoreThan7Days = expiresAt < (now - 7 * MS_PER_DAY);
    if (!expiredMoreThan7Days) {
      newQueue.push(item);
    }
  }
  
  await env.USERS.put('reminder_queue', JSON.stringify(newQueue));
  return { sent: sentCount };
}

// \u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f \u0432 \u043e\u0447\u0435\u0440\u0435\u0434\u044c \u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0439
async function addToReminderQueue(env, telegramId, expiresAt) {
  let queue = [];
  const queueData = await env.USERS.get('reminder_queue');
  if (queueData) queue = JSON.parse(queueData);
  
  // \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u0441\u0442\u0430\u0440\u0443\u044e \u0437\u0430\u043f\u0438\u0441\u044c \u0435\u0441\u043b\u0438 \u0435\u0441\u0442\u044c
  queue = queue.filter(item => item.telegramId !== telegramId);
  
  // \u0414\u043e\u0431\u0430\u0432\u043b\u044f\u0435\u043c \u043d\u043e\u0432\u0443\u044e
  queue.push({ telegramId, expiresAt, lastReminder: null });
  
  await env.USERS.put('reminder_queue', JSON.stringify(queue));
}

// \u0421\u043a\u043b\u043e\u043d\u0435\u043d\u0438\u0435 \u0441\u043b\u043e\u0432\u0430 "\u0434\u0435\u043d\u044c"
function getDaysWord(days) {
  if (days === 1) return '\u0434\u0435\u043d\u044c';
  if (days >= 2 && days <= 4) return '\u0434\u043d\u044f';
  return '\u0434\u043d\u0435\u0439';
}

async function handleNotify(request, env) {
  try {
    // Rate limiting \u0434\u043b\u044f \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    const rateLimit = await checkRateLimit(env, `notify:${clientIP}`, 30); // 30 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439 \u0432 \u043c\u0438\u043d\u0443\u0442\u0443
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
    
    // \u0412\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u044f oderId
    if (typeof oderId !== 'string' || oderId.length < 16 || oderId.length > 64) {
      return jsonResponse({ error: 'Invalid oderId format' }, 400);
    }
    
    // \u0412\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u044f transaction (\u0435\u0441\u043b\u0438 \u043f\u0435\u0440\u0435\u0434\u0430\u043d\u0430)
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
    
    // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f - \u0432\u043a\u043b\u044e\u0447\u0435\u043d\u044b \u043b\u0438 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445
    const telegramId = result.user.telegramId;
    const userSettings = await getUserSettings(env, telegramId);
    
    console.log(`[NOTIFY] User ${telegramId}, clicks enabled: ${userSettings.notifications.clicks}`);
    
    if (!userSettings.notifications.clicks) {
      // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e\u0442\u043a\u043b\u044e\u0447\u0435\u043d\u044b \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u043c, \u043d\u043e \u043a\u043b\u0438\u043a \u0437\u0430\u043f\u0438\u0441\u044b\u0432\u0430\u0435\u043c
      if (telegramId) {
        await recordUserClick(env, telegramId);
      }
      return jsonResponse({ success: true, notificationsSent: 0, reason: 'NOTIFICATIONS_DISABLED' });
    }
    
    // \u0424\u043e\u0440\u043c\u0438\u0440\u0443\u0435\u043c \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0441 \u044d\u043a\u0440\u0430\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c\u0441\u043a\u0438\u0445 \u0434\u0430\u043d\u043d\u044b\u0445
    let message = `\u2705 *\u0417\u0430\u044f\u0432\u043a\u0430 \u0432\u0437\u044f\u0442\u0430 \u0432 \u0440\u0430\u0431\u043e\u0442\u0443!*\n\n`;
    if (transaction) {
      if (transaction.transactionId) {
        // transactionId \u0432 \u0431\u044d\u043a\u0442\u0438\u043a\u0430\u0445 - \u043d\u0435 \u044d\u043a\u0440\u0430\u043d\u0438\u0440\u0443\u0435\u043c
        message += `\u{1f194} *ID:* \`${String(transaction.transactionId).slice(0, 50)}\`\n`;
      }
      if (transaction.amount) {
        message += `\u{1f4b0} *\u0421\u0443\u043c\u043c\u0430:* ${escapeMarkdown(String(transaction.amount).slice(0, 30))}\n`;
      }
      if (transaction.method) {
        message += `\u{1f4b3} *\u0421\u043f\u043e\u0441\u043e\u0431:* ${escapeMarkdown(String(transaction.method).slice(0, 50))}\n`;
      }
      if (transaction.created) {
        message += `\u{1f4c5} *\u0421\u043e\u0437\u0434\u0430\u043d\u0430:* ${escapeMarkdown(String(transaction.created).slice(0, 30))}\n`;
      }
      if (transaction.requisites) {
        // requisites \u0432 \u0431\u044d\u043a\u0442\u0438\u043a\u0430\u0445 - \u043d\u0435 \u044d\u043a\u0440\u0430\u043d\u0438\u0440\u0443\u0435\u043c, \u043d\u043e \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0438\u0432\u0430\u0435\u043c \u0434\u043b\u0438\u043d\u0443
        message += `\u{1f522} *\u0420\u0435\u043a\u0432\u0438\u0437\u0438\u0442\u044b:* \`${String(transaction.requisites).slice(0, 50)}\`\n`;
      }
      if (transaction.bank) {
        message += `\u{1f3e6} *\u0411\u0430\u043d\u043a:* ${escapeMarkdown(String(transaction.bank).slice(0, 50))}\n`;
      }
    }
    
    const time = new Date().toLocaleString('ru-RU', { 
      timeZone: 'Europe/Moscow',
      hour: '2-digit', 
      minute: '2-digit',
      second: '2-digit'
    });
    message += `\n\u23f0 *\u0412\u0440\u0435\u043c\u044f:* ${time} (\u041c\u0421\u041a)`;
    
    // \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435 \u0432 Telegram
    const botToken = env.TELEGRAM_BOT_TOKEN;
    
    // \u0417\u0430\u043f\u0438\u0441\u044b\u0432\u0430\u0435\u043c \u043a\u043b\u0438\u043a \u0432 \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0443
    if (telegramId) {
      await recordUserClick(env, telegramId);
    }
    
    let sentTo = [];
    
    if (botToken && telegramId) {
      const licenseType = LICENSE_TYPES[result.license.type];
      const canUseDm = licenseType ? licenseType.dmNotifications : true;
      const canUseGroup = licenseType ? licenseType.groupNotifications : false;
      
      // \u041e\u043f\u0440\u0435\u0434\u0435\u043b\u044f\u0435\u043c \u043a\u0443\u0434\u0430 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0442\u044c \u043d\u0430 \u043e\u0441\u043d\u043e\u0432\u0435 \u0443\u0440\u043e\u0432\u043d\u044f \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438 \u0438 \u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043a
      // trial, level1: \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u041b\u0421
      // level2: \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u0433\u0440\u0443\u043f\u043f\u0443 (\u0435\u0441\u043b\u0438 \u0435\u0441\u0442\u044c), \u0438\u043d\u0430\u0447\u0435 \u0432 \u041b\u0421
      // level3: \u043f\u043e \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0435 notifyDestination
      
      let sendToDm = false;
      let sendToGroup = false;
      
      const licenseLevel = result.license.type;
      const notifyDest = userSettings.notifyDestination || 'dm';
      
      if (licenseLevel === 'trial' || licenseLevel === 'level1') {
        // \u0422\u043e\u043b\u044c\u043a\u043e \u041b\u0421
        sendToDm = true;
        sendToGroup = false;
      } else if (licenseLevel === 'level2') {
        // \u0422\u043e\u043b\u044c\u043a\u043e \u0433\u0440\u0443\u043f\u043f\u0430 (\u0435\u0441\u043b\u0438 \u0435\u0441\u0442\u044c), \u0438\u043d\u0430\u0447\u0435 fallback \u0432 \u041b\u0421
        sendToGroup = true;
        sendToDm = false; // fallback \u0431\u0443\u0434\u0435\u0442 \u043d\u0438\u0436\u0435 \u0435\u0441\u043b\u0438 \u043d\u0435\u0442 \u0433\u0440\u0443\u043f\u043f\u044b
      } else if (licenseLevel === 'level3') {
        // \u041f\u043e \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0435 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
        if (notifyDest === 'dm') {
          sendToDm = true;
        } else if (notifyDest === 'group') {
          sendToGroup = true;
        } else if (notifyDest === 'both') {
          sendToDm = true;
          sendToGroup = true;
        }
      } else {
        // \u0414\u043b\u044f \u0434\u0440\u0443\u0433\u0438\u0445/\u0441\u0442\u0430\u0440\u044b\u0445 \u0442\u0438\u043f\u043e\u0432 - \u043f\u043e \u0443\u043c\u043e\u043b\u0447\u0430\u043d\u0438\u044e \u0432 \u041b\u0421
        sendToDm = canUseDm;
        sendToGroup = canUseGroup;
      }
      
      // \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0432 \u041b\u0421 \u0435\u0441\u043b\u0438 \u043d\u0443\u0436\u043d\u043e
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
      
      // \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0432 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b \u0435\u0441\u043b\u0438 \u043d\u0443\u0436\u043d\u043e
      if (sendToGroup && canUseGroup) {
        const groupData = await getUserGroupChats(env, telegramId);
        
        if (groupData.chats && groupData.chats.length > 0) {
          // \u0414\u043e\u0431\u0430\u0432\u043b\u044f\u0435\u043c \u0438\u043c\u044f \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f \u0432 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0434\u043b\u044f \u0433\u0440\u0443\u043f\u043f
          const username = result.user.username;
          const groupMessage = `\u{1f464} *${username ? '@' + username : '\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c'}*\n\n${message}`;
          
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
                // \u0415\u0441\u043b\u0438 \u0431\u043e\u0442 \u0443\u0434\u0430\u043b\u0451\u043d \u0438\u0437 \u0433\u0440\u0443\u043f\u043f\u044b - \u0443\u0431\u0438\u0440\u0430\u0435\u043c \u0435\u0451
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
          // Level2 fallback: \u0435\u0441\u043b\u0438 \u043d\u0435\u0442 \u0433\u0440\u0443\u043f\u043f\u044b, \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0432 \u041b\u0421
          try {
            const personalResult = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: telegramId,
                text: message + `\n\n_\u{1f4a1} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u0435 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 \u0434\u043b\u044f \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439_`,
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

// \u0423\u043d\u0438\u0432\u0435\u0440\u0441\u0430\u043b\u044c\u043d\u0430\u044f \u0444\u0443\u043d\u043a\u0446\u0438\u044f \u0434\u043b\u044f \u043f\u043e\u043a\u0430\u0437\u0430 \u043f\u0430\u043d\u0435\u043b\u0438 (fallback)
async function showAdminPanel(env, chatId, stats, pendingGroups, messageId = null) {
  const adminMsg = `\u{1f510} *\u0410\u0414\u041c\u0418\u041d-\u041f\u0410\u041d\u0415\u041b\u042c*\n` +
    `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n` +
    `\u{1f4ca} *\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430:*\n` +
    `\u251c \u{1f465} \u0412\u0441\u0435\u0433\u043e: ${stats.totalUsers}\n` +
    `\u251c \u2705 \u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0445: ${stats.activeUsers}\n` +
    `\u251c \u2b50 Level 1: ${stats.level1Users || 0}\n` +
    `\u251c \u2b50\u2b50 Level 2: ${stats.level2Users || 0}\n` +
    `\u251c \u2b50\u2b50\u2b50 Level 3: ${stats.level3Users || 0}\n` +
    `\u2514 \u{1f381} Trial: ${stats.trialUsers}\n\n` +
    `\u{1f4ac} *\u0427\u0430\u0442\u044b:*\n` +
    `\u251c \u23f3 \u041e\u0436\u0438\u0434\u0430\u044e\u0442: ${pendingGroups.length}\n` +
    `\u2514 \u2705 \u041e\u0434\u043e\u0431\u0440\u0435\u043d\u043e: ${stats.approvedGroups}\n\n` +
    `_\u0421\u0435\u0441\u0441\u0438\u044f: 1 \u0447\u0430\u0441_`;
    
  const keyboard = {
    inline_keyboard: [
      // \u0420\u044f\u0434 1: \u041e\u0436\u0438\u0434\u0430\u044e\u0449\u0438\u0435 \u0433\u0440\u0443\u043f\u043f\u044b (\u0435\u0441\u043b\u0438 \u0435\u0441\u0442\u044c)
      pendingGroups.length > 0 ? [
        { text: `\u{1f514} \u0417\u0430\u044f\u0432\u043a\u0438 \u043d\u0430 \u0447\u0430\u0442\u044b (${pendingGroups.length})`, callback_data: 'adm_pending' }
      ] : [],
      // \u0420\u044f\u0434 2: \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0438
      [
        { text: '\u{1f381} \u0412\u044b\u0434\u0430\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'adm_grant' },
        { text: '\u{1f50d} \u041d\u0430\u0439\u0442\u0438 \u044e\u0437\u0435\u0440\u0430', callback_data: 'adm_search' }
      ],
      // \u0420\u044f\u0434 3: \u0421\u043f\u0438\u0441\u043a\u0438
      [
        { text: '\u{1f465} \u0412\u0441\u0435 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438', callback_data: 'adm_users' },
        { text: '\u{1f4ac} \u0412\u0441\u0435 \u0447\u0430\u0442\u044b', callback_data: 'adm_chats' }
      ],
      // \u0420\u044f\u0434 4: \u0418\u043d\u0441\u0442\u0440\u0443\u043c\u0435\u043d\u0442\u044b
      [
        { text: '\u{1f4e2} \u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430', callback_data: 'adm_broadcast' },
        { text: '\u{1f4c8} \u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430', callback_data: 'adm_stats' }
      ],
      // \u0420\u044f\u0434 5: \u0421\u0438\u0441\u0442\u0435\u043c\u0430
      [
        { text: '\u{1f527} \u0421\u0438\u0441\u0442\u0435\u043c\u0430', callback_data: 'adm_system' },
        { text: '\u{1f504}', callback_data: 'adm_refresh' }
      ],
      // \u0420\u044f\u0434 6: \u0412\u044b\u0445\u043e\u0434
      [
        { text: '\u{1f6aa} \u0412\u044b\u0445\u043e\u0434', callback_data: 'adm_logout' }
      ]
    ].filter(row => row.length > 0)
  };
  
  if (messageId) {
    await editTelegramMessage(env, chatId, messageId, adminMsg, { reply_markup: keyboard });
  } else {
    await sendTelegramMessage(env, chatId, adminMsg, { reply_markup: keyboard });
  }
}

// \u0426\u0435\u043d\u0442\u0440\u0430\u043b\u0438\u0437\u043e\u0432\u0430\u043d\u043d\u044b\u0439 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a \u043e\u0448\u0438\u0431\u043e\u043a
async function handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups) {
  // \u041f\u0440\u043e\u0431\u0443\u0435\u043c fallback: \u043d\u043e\u0432\u043e\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0441 \u043f\u0430\u043d\u0435\u043b\u044c\u044e
  await showAdminPanel(env, chatId, stats, pendingGroups);
  // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c \u043e\u0448\u0438\u0431\u043a\u0443
  await logToChat(env, `\u274c \u041e\u0448\u0438\u0431\u043a\u0430 \u0430\u0434\u043c\u0438\u043d-\u043f\u0430\u043d\u0435\u043b\u0438: ${err && err.message ? err.message : err}`);
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

// === \u0412\u0418\u0417\u0423\u0410\u041b\u042c\u041d\u042b\u0415 \u0425\u0415\u041b\u041f\u0415\u0420\u042b ===

// \u0413\u0435\u043d\u0435\u0440\u0430\u0446\u0438\u044f \u043f\u0440\u043e\u0433\u0440\u0435\u0441\u0441-\u0431\u0430\u0440\u0430
function getProgressBar(percent, length = 10) {
  const filled = Math.round((percent / 100) * length);
  const empty = length - filled;
  return '\u2593'.repeat(filled) + '\u2591'.repeat(empty);
}

// \u0424\u043e\u0440\u043c\u0430\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0432\u0440\u0435\u043c\u0435\u043d\u0438
function formatTimeLeft(ms) {
  const hours = Math.floor(ms / (60 * 60 * 1000));
  const days = Math.floor(hours / 24);
  
  if (days > 0) {
    return `${days} \u0434\u043d.`;
  } else if (hours > 0) {
    return `${hours} \u0447.`;
  } else {
    const minutes = Math.floor(ms / (60 * 1000));
    return `${minutes} \u043c\u0438\u043d.`;
  }
}

// \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u044d\u043c\u043e\u0434\u0437\u0438 \u0441\u0442\u0430\u0442\u0443\u0441\u0430
function getStatusEmoji(daysLeft, type) {
  if (type === 'trial') return '\u{1f381}';
  if (daysLeft > 14) return '\u{1f48e}';
  if (daysLeft > 7) return '\u2b50';
  if (daysLeft > 3) return '\u23f0';
  return '\u26a0\ufe0f';
}

// === \u041a\u041b\u0410\u0412\u0418\u0410\u0422\u0423\u0420\u042b ===

// \u0413\u043b\u0430\u0432\u043d\u043e\u0435 \u043c\u0435\u043d\u044e - \u0434\u043b\u044f \u043d\u043e\u0432\u044b\u0445 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439
function getMainMenuKeyboard() {
  return {
    inline_keyboard: [
      [{ text: '\u{1f517} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435', callback_data: 'how_to_connect' }],
      [
        { text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' },
        { text: '\u{1f4c8} \u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430', callback_data: 'mystats' }
      ],
      [
        { text: '\u2753 \u041f\u043e\u043c\u043e\u0449\u044c', callback_data: 'help' },
        { text: '\u{1f39f}\ufe0f \u0420\u0435\u0444\u0435\u0440\u0430\u043b', callback_data: 'referral' }
      ],
      [
        { text: '\ufffd \u0423\u0441\u0442\u0430\u043d\u043e\u0432\u043a\u0430', callback_data: 'install' },
        { text: '\u{1f4f0} \u041d\u043e\u0432\u043e\u0441\u0442\u0438', callback_data: 'news' }
      ],
      [
        { text: '\ufffd\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c', callback_data: 'buy' },
        { text: '\u{1f4ac} \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430', url: 'https://t.me/YaMob' }
      ]
    ]
  };
}

// \u041c\u0435\u043d\u044e \u0434\u043b\u044f \u043f\u0440\u0438\u0432\u044f\u0437\u0430\u043d\u043d\u044b\u0445 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439 \u0441 \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0439 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u043e\u0439
function getUserMenuKeyboard(isValid, hasGroupChats = false) {
  const keyboard = [];
  
  if (isValid) {
    keyboard.push([
      { text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' },
      { text: '\u{1f4c8} \u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430', callback_data: 'mystats' }
    ]);
    keyboard.push([
      { text: '\u{1f514} \u0422\u0435\u0441\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439', callback_data: 'test_notify' },
      { text: '\u2699\ufe0f \u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438', callback_data: 'settings' }
    ]);
    if (hasGroupChats) {
      keyboard.push([{ text: '\u{1f4ac} \u0413\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b', callback_data: 'groups_menu' }]);
    }
    keyboard.push([
      { text: '\u{1f39f}\ufe0f \u041f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u044c \u0434\u0440\u0443\u0433\u0430', callback_data: 'referral' },
      { text: '\u2753 \u041f\u043e\u043c\u043e\u0449\u044c', callback_data: 'help' }
    ]);
  } else {
    keyboard.push([{ text: '\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }]);
    keyboard.push([
      { text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' },
      { text: '\u{1f4ac} \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430', url: 'https://t.me/YaMob' }
    ]);
  }
  
  return { inline_keyboard: keyboard };
}

// \u041a\u043d\u043e\u043f\u043a\u0430 \u043d\u0430\u0437\u0430\u0434
function getBackButton() {
  return {
    inline_keyboard: [
      [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
    ]
  };
}

// \u041a\u043b\u0430\u0432\u0438\u0430\u0442\u0443\u0440\u0430 \u0441\u0442\u0430\u0442\u0443\u0441\u0430
function getStatusKeyboard(isValid, type) {
  if (isValid) {
    return {
      inline_keyboard: [
        [{ text: '\u{1f514} \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f', callback_data: 'test_notify' }],
        [
          { text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'status' },
          { text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }
        ]
      ]
    };
  }
  return {
    inline_keyboard: [
      [{ text: '\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
      [
        { text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'status' },
        { text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }
      ]
    ]
  };
}

async function handleTelegramWebhook(request, env) {
  try {
    // \u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 IP \u043e\u0442 Telegram (\u043e\u043f\u0446\u0438\u043e\u043d\u0430\u043b\u044c\u043d\u043e, \u0435\u0441\u043b\u0438 env.VERIFY_TELEGRAM_IP === 'true')
    if (env.VERIFY_TELEGRAM_IP === 'true') {
      const clientIP = request.headers.get('CF-Connecting-IP');
      if (!isFromTelegram(clientIP)) {
        console.warn(`Webhook from non-Telegram IP: ${clientIP}`);
        return jsonResponse({ ok: false, error: 'Forbidden' }, 403);
      }
    }
    
    // \u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u0441\u0435\u043a\u0440\u0435\u0442\u043d\u043e\u0433\u043e \u0442\u043e\u043a\u0435\u043d\u0430 \u043e\u0442 Telegram (\u0435\u0441\u043b\u0438 \u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043d)
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
    
    // \u0424\u0443\u043d\u043a\u0446\u0438\u044f \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438 \u0430\u0434\u043c\u0438\u043d\u0430 (\u0422\u041e\u041b\u042c\u041a\u041e \u043f\u043e Telegram ID \u0434\u043b\u044f \u0431\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u043e\u0441\u0442\u0438)
    const isAdmin = async (telegramId) => {
      return await isAdminById(env, telegramId);
    };
    
    // \u0424\u0443\u043d\u043a\u0446\u0438\u044f \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438 - \u044d\u0442\u043e \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 \u0438\u043b\u0438 \u041b\u0421
    const isGroupChat = (chat) => {
      return chat && (chat.type === 'group' || chat.type === 'supergroup');
    };
    
    // === \u041e\u0411\u0420\u0410\u0411\u041e\u0422\u041a\u0410 INLINE \u0417\u0410\u041f\u0420\u041e\u0421\u041e\u0412 ===
    if (update.inline_query) {
      const inlineQuery = update.inline_query;
      const telegramId = String(inlineQuery.from.id);
      const query = inlineQuery.query.toLowerCase().trim();
      
      // Rate limit \u0434\u043b\u044f inline
      const rateCheck = checkBotRateLimit(telegramId, 'callbacks');
      if (!rateCheck.allowed) {
        return jsonResponse({ ok: true });
      }
      
      // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
      const refCode = await getUserReferralCode(env, telegramId);
      const botUsername = env.BOT_USERNAME || 'clickuved_bot';
      
      const results = [];
      
      // \u041f\u043e\u043a\u0430\u0437\u044b\u0432\u0430\u0435\u043c \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0443 \u0441 \u0440\u0435\u0444-\u0441\u0441\u044b\u043b\u043a\u043e\u0439
      if (query === '' || query.includes('\u0440\u0435\u0444') || query.includes('ref') || query.includes('invite')) {
        results.push({
          type: 'article',
          id: 'ref_' + telegramId,
          title: '\u{1f381} \u041f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u044c \u0434\u0440\u0443\u0433\u0430',
          description: '\u041e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u0443\u044e \u0441\u0441\u044b\u043b\u043a\u0443',
          input_message_content: {
            message_text: `\u{1f381} *\u041f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435 \u0432 Exotic Assistant*\n\n` +
              `\u041f\u0440\u0438\u0432\u0435\u0442! \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439 \u0430\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440 \u0434\u043b\u044f Exotic.\n\n` +
              `\u2705 \u0410\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438 \u0431\u0435\u0440\u0451\u0442 \u0437\u0430\u044f\u0432\u043a\u0438\n` +
              `\u2705 \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0432 Telegram\n` +
              `\u2705 \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u0430\u044f \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430\n\n` +
              `\u{1f39f}\ufe0f \u041c\u043e\u0439 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434: \`${refCode}\`\n\n` +
              `\u{1f449} \u041f\u0435\u0440\u0435\u0439\u0434\u0438 \u043a \u0431\u043e\u0442\u0443: @${botUsername}\n` +
              `\u0418 \u0432\u0432\u0435\u0434\u0438 \u043a\u043e\u043c\u0430\u043d\u0434\u0443: \`/ref ${refCode}\`\n\n` +
              `_\u0418\u043b\u0438 \u043f\u0435\u0440\u0435\u0439\u0434\u0438 \u043f\u043e \u0441\u0441\u044b\u043b\u043a\u0435:_\n` +
              `https://t.me/${botUsername}?start=ref_${refCode}`,
            parse_mode: 'Markdown'
          },
          reply_markup: {
            inline_keyboard: [
              [{ text: '\u{1f916} \u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u043a \u0431\u043e\u0442\u0443', url: `https://t.me/${botUsername}?start=ref_${refCode}` }]
            ]
          }
        });
      }
      
      // \u041e\u0442\u0432\u0435\u0447\u0430\u0435\u043c \u043d\u0430 inline \u0437\u0430\u043f\u0440\u043e\u0441
      await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/answerInlineQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inline_query_id: inlineQuery.id,
          results: results,
          cache_time: 300, // \u043a\u044d\u0448 5 \u043c\u0438\u043d\u0443\u0442
          is_personal: true
        })
      });
      
      return jsonResponse({ ok: true });
    }
    
    // === \u041e\u0411\u0420\u0410\u0411\u041e\u0422\u041a\u0410 CALLBACK \u041a\u041d\u041e\u041f\u041e\u041a ===
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
      
      // Rate limiting \u0434\u043b\u044f callback
      const rateCheck = checkBotRateLimit(telegramId, 'callbacks');
      if (!rateCheck.allowed) {
        await answerCallbackQuery(env, callback.id, rateCheck.message, true);
        return jsonResponse({ ok: true });
      }
      
      // \u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0430\u0435\u043c \u043f\u043e\u043b\u0443\u0447\u0435\u043d\u0438\u0435 callback
      await answerCallbackQuery(env, callback.id);
      
      // \u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430 callback \u0434\u043b\u044f \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0445 \u0447\u0430\u0442\u043e\u0432
      if (isGroup && data === 'group_status') {
        const existingOderId = await env.USERS.get(`tg:${telegramId}`);
        const botUsername = env.BOT_USERNAME || 'clickuved_bot';
        
        if (!existingOderId) {
          await editTelegramMessage(env, chatId, messageId, 
            `\u{1f4ca} @${username || telegramId}, \u0432\u0430\u0448 \u0441\u0442\u0430\u0442\u0443\u0441:\n\n` +
            `\u274c \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n\n` +
            `\u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 \u0431\u043e\u0442\u0443 \u0432 \u041b\u0421 \u0434\u043b\u044f \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f.`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f916} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
          return jsonResponse({ ok: true });
        }
        
        const result = await getLicenseData(env, existingOderId);
        
        if (!result.license) {
          await editTelegramMessage(env, chatId, messageId, 
            `\u{1f4ca} @${username || telegramId}, \u0432\u0430\u0448 \u0441\u0442\u0430\u0442\u0443\u0441:\n\n` +
            `\u2705 \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n` +
            `\u274c \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c', url: `https://t.me/${botUsername}` }]
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
        
        const emoji = isValid ? getStatusEmoji(daysLeft, license.type) : '\u274c';
        const typeText = LICENSE_TYPES[license.type]?.name || '\u041f\u0440\u0435\u043c\u0438\u0443\u043c';
        const timeDisplay = license.type === 'trial' ? `${hoursLeft} \u0447.` : `${daysLeft} \u0434\u043d.`;
        
        await editTelegramMessage(env, chatId, messageId, 
          `\u{1f4ca} @${username || telegramId}, \u0432\u0430\u0448 \u0441\u0442\u0430\u0442\u0443\u0441:\n\n` +
          `${emoji} ${typeText} \u2022 ${isValid ? '\u0410\u043a\u0442\u0438\u0432\u043d\u0430' : '\u0418\u0441\u0442\u0435\u043a\u043b\u0430'}\n` +
          `\u23f0 ${isValid ? `\u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c: ${timeDisplay}` : '\u041f\u0440\u043e\u0434\u043b\u0438\u0442\u0435 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443'}`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '\u{1f4ca} \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u0435\u0435', url: `https://t.me/${botUsername}` }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      switch (data) {
        case 'menu': {
          const menuText = `\u{1f3e0} *\u041c\u0435\u043d\u044e*\n\n\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0440\u0430\u0437\u0434\u0435\u043b:`;
          
          await editTelegramMessage(env, chatId, messageId, menuText, {
            reply_markup: getMainMenuKeyboard()
          });
          break;
        }
        
        case 'status': {
          // \u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0443
          const existingOderId = await env.USERS.get(`tg:${telegramId}`);
          
          if (!existingOderId) {
            await editTelegramMessage(env, chatId, messageId, 
              `\u{1f4ca} *\u0421\u0442\u0430\u0442\u0443\u0441*\n\n` +
              `\u274c \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n\n` +
              `\u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u0440\u0438\u0432\u044f\u0436\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435\n\u0447\u0435\u0440\u0435\u0437 \u043a\u043e\u0434 \u0432 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430\u0445`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '\u{1f517} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c', callback_data: 'how_to_connect' }],
                  [{ text: ' \u0422\u0430\u0440\u0438\u0444\u044b', callback_data: 'buy' }],
                  [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e \u0447\u0435\u0440\u0435\u0437 getLicenseData (\u0441\u043e\u0437\u0434\u0430\u0441\u0442 \u0442\u0440\u0438\u0430\u043b \u0435\u0441\u043b\u0438 \u043d\u0435\u0442)
          const result = await getLicenseData(env, existingOderId);
          
          if (!result.license) {
            await editTelegramMessage(env, chatId, messageId, 
              `\u{1f4ca} *\u0421\u0442\u0430\u0442\u0443\u0441*\n\n` +
              `\u2705 \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n` +
              `\u274c \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430\n\n` +
              `\u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 @YaMob \u0434\u043b\u044f\n\u0430\u043a\u0442\u0438\u0432\u0430\u0446\u0438\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
                  [{ text: '\u{1f4ac} \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430', callback_data: 'support' }],
                  [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
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
          
          // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044e \u043e\u0431 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430\u0445
          const devicesData = await getUserDevices(env, telegramId);
          const deviceCount = devicesData.devices?.length || 0;
          const maxDevices = licenseType.maxDevices || 1;
          
          let message;
          if (isValid) {
            const emoji = getStatusEmoji(daysLeft, license.type);
            const typeText = licenseType.name || '\u041f\u0440\u0435\u043c\u0438\u0443\u043c';
            const timeDisplay = license.type === 'trial' ? `${hoursLeft} \u0447.` : `${daysLeft} \u0434\u043d.`;
            
            // \u041f\u0440\u043e\u0433\u0440\u0435\u0441\u0441-\u0431\u0430\u0440 (\u0434\u043b\u044f trial 24\u0447, \u0434\u043b\u044f \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438 30\u0434\u043d)
            const maxTime = license.type === 'trial' ? 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
            const percent = Math.min(100, Math.round((timeLeft / maxTime) * 100));
            const bar = getProgressBar(percent);
            
            message = `\u{1f4ca} *\u0421\u0442\u0430\u0442\u0443\u0441 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438*\n\n`;
            message += `${emoji} ${typeText} \u2022 \u0410\u043a\u0442\u0438\u0432\u043d\u0430\n`;
            message += `${bar} ${percent}%\n\n`;
            message += `\u23f0 \u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c: *${timeDisplay}*\n`;
            message += `\u{1f4c5} \u0414\u043e: ${expiresDate}\n`;
            message += `\u{1f4f1} \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430: ${deviceCount}/${maxDevices}`;
            
            if (deviceCount > 0 && deviceCount >= maxDevices) {
              message += ` \u26a0\ufe0f`;
            }
            
            if (daysLeft <= 3 && license.type !== 'trial') {
              message += `\n\n\u26a0\ufe0f _\u0421\u043a\u043e\u0440\u043e \u0438\u0441\u0442\u0435\u0447\u0451\u0442_`;
            }
          } else {
            message = `\u{1f4ca} *\u0421\u0442\u0430\u0442\u0443\u0441 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438*\n\n`;
            message += `\u274c \u0418\u0441\u0442\u0435\u043a\u043b\u0430 ${expiresDate}\n`;
            message += `\u{1f4f1} \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430: ${deviceCount}/${maxDevices}\n\n`;
            message += `_\u041f\u0440\u043e\u0434\u043b\u0438\u0442\u0435 \u0434\u043b\u044f \u0440\u0430\u0431\u043e\u0442\u044b_`;
          }
          
          // \u041a\u043b\u0430\u0432\u0438\u0430\u0442\u0443\u0440\u0430 \u0441 \u043a\u043d\u043e\u043f\u043a\u043e\u0439 \u0443\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u044f \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430\u043c\u0438
          const keyboard = {
            inline_keyboard: []
          };
          
          if (isValid) {
            keyboard.inline_keyboard.push([{ text: '\u{1f4b3} \u041f\u0440\u043e\u0434\u043b\u0438\u0442\u044c', callback_data: 'buy' }]);
            if (deviceCount > 0) {
              keyboard.inline_keyboard.push([{ text: '\u{1f4f1} \u041c\u043e\u0438 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430', callback_data: 'devices' }]);
            }
            keyboard.inline_keyboard.push([
              { text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'status' },
              { text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }
            ]);
          } else {
            keyboard.inline_keyboard.push(
              [{ text: '\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
              [
                { text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'status' },
                { text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }
              ]
            );
          }
          
          // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0438 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e\u0431 \u0438\u0441\u0442\u0435\u0447\u0438\u0438 (3, 1, 0 \u0434\u043d\u0435\u0439)
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
          // \u041f\u043e\u043a\u0430\u0437\u0430\u0442\u044c \u0441\u043f\u0438\u0441\u043e\u043a \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
          const devicesData = await getUserDevices(env, telegramId);
          const devices = devicesData.devices || [];
          
          // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e \u0434\u043b\u044f maxDevices
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          let maxDevices = 1;
          if (oderId) {
            const licResult = await getLicenseData(env, oderId);
            if (licResult.license) {
              const licType = LICENSE_TYPES[licResult.license.type] || LICENSE_TYPES.trial;
              maxDevices = licType.maxDevices || 1;
            }
          }
          
          let message = `\u{1f4f1} *\u041c\u043e\u0438 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430* (${devices.length}/${maxDevices})\n\n`;
          
          if (devices.length === 0) {
            message += `_\u041d\u0435\u0442 \u043f\u0440\u0438\u0432\u044f\u0437\u0430\u043d\u043d\u044b\u0445 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432_`;
          } else {
            devices.forEach((device, index) => {
              const shortId = device.deviceId.substring(0, 12) + '...';
              const addedDate = new Date(device.addedAt).toLocaleDateString('ru-RU');
              const lastSeenDate = new Date(device.lastSeen).toLocaleDateString('ru-RU');
              message += `${index + 1}. \`${shortId}\`\n`;
              message += `   \u{1f4c5} \u0414\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u043e: ${addedDate}\n`;
              message += `   \u{1f441} \u041f\u043e\u0441\u043b\u0435\u0434. \u0430\u043a\u0442.: ${lastSeenDate}\n\n`;
            });
          }
          
          if (devices.length >= maxDevices) {
            message += `\n\u26a0\ufe0f _\u041b\u0438\u043c\u0438\u0442 \u0434\u043e\u0441\u0442\u0438\u0433\u043d\u0443\u0442. \u041e\u0442\u0432\u044f\u0436\u0438\u0442\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u0434\u043b\u044f \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u0438\u044f \u043d\u043e\u0432\u043e\u0433\u043e._`;
          }
          
          // \u0421\u043e\u0437\u0434\u0430\u0451\u043c \u043a\u043d\u043e\u043f\u043a\u0438 \u0434\u043b\u044f \u0443\u0434\u0430\u043b\u0435\u043d\u0438\u044f \u043a\u0430\u0436\u0434\u043e\u0433\u043e \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430
          const keyboard = {
            inline_keyboard: []
          };
          
          devices.forEach((device, index) => {
            keyboard.inline_keyboard.push([
              { text: `\u{1f5d1} \u041e\u0442\u0432\u044f\u0437\u0430\u0442\u044c \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e ${index + 1}`, callback_data: `remove_device:${device.deviceId.substring(0, 32)}` }
            ]);
          });
          
          keyboard.inline_keyboard.push([{ text: '\u2190 \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' }]);
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: keyboard
          });
          break;
        }
        
        case 'help': {
          const helpMsg = `\u{1f4d6} *Exotic Assistant v${BOT_VERSION}*

\u{1f5b1}\ufe0f *\u0410\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440*
\u0410\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438 \u0431\u0435\u0440\u0451\u0442 \u0437\u0430\u044f\u0432\u043a\u0438 "\u0412 \u0440\u0430\u0431\u043e\u0442\u0443"

\u{1f4ca} *\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430*
\u041a\u043b\u0438\u043a\u0438 \u0437\u0430 \u0434\u0435\u043d\u044c \u2022 \u0412\u0441\u0435\u0433\u043e \u2022 \u0413\u0440\u0430\u0444\u0438\u043a\u0438

\u{1f514} *\u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f*
Push \u2022 Telegram \u2022 \u0417\u0432\u0443\u043a

\u2699\ufe0f *\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438*
\u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u2022 \u0417\u0430\u0434\u0435\u0440\u0436\u043a\u0430 \u2022 \u0413\u0440\u043e\u043c\u043a\u043e\u0441\u0442\u044c

\u{1f39f}\ufe0f *\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u0430\u044f \u043f\u0440\u043e\u0433\u0440\u0430\u043c\u043c\u0430*
\u041f\u0440\u0438\u0433\u043b\u0430\u0441\u0438 \u0434\u0440\u0443\u0433\u0430 \u2014 \u043f\u043e\u043b\u0443\u0447\u0438 +1 \u0434\u0435\u043d\u044c!`;
          
          await editTelegramMessage(env, chatId, messageId, helpMsg, {
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '\u{1f517} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c', callback_data: 'how_to_connect' },
                  { text: '\u2753 FAQ', callback_data: 'faq' }
                ],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'how_to_connect': {
          const connectMsg = `\u{1f517} *\u041a\u0430\u043a \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435*

1\ufe0f\u20e3 \u0423\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u0432 \u0431\u0440\u0430\u0443\u0437\u0435\u0440
2\ufe0f\u20e3 \u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u043d\u0430 \u0438\u043a\u043e\u043d\u043a\u0443 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f
3\ufe0f\u20e3 \u041e\u0442\u043a\u0440\u043e\u0439\u0442\u0435 "\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438" \u2699\ufe0f
4\ufe0f\u20e3 \u041d\u0430\u0436\u043c\u0438\u0442\u0435 "\u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c Telegram"
5\ufe0f\u20e3 \u041f\u0435\u0440\u0435\u0439\u0434\u0438\u0442\u0435 \u043f\u043e \u0441\u0441\u044b\u043b\u043a\u0435 \u0438\u043b\u0438 \u043e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u043a\u043e\u0434

\u2705 *\u041f\u043e\u0441\u043b\u0435 \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0438:*
\u2022 24 \u0447\u0430\u0441\u0430 \u0431\u0435\u0441\u043f\u043b\u0430\u0442\u043d\u043e\u0433\u043e \u0434\u043e\u0441\u0442\u0443\u043f\u0430
\u2022 \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445
\u2022 \u0410\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440 \u0430\u043a\u0442\u0438\u0432\u0435\u043d`;
          
          await editTelegramMessage(env, chatId, messageId, connectMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4ca} \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u0441\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'buy': {
          const buyMsg = `\u{1f4b3} *\u0422\u0430\u0440\u0438\u0444\u044b Exotic Assistant v${BOT_VERSION}*

\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501

\u2b50 *\u0423\u0440\u043e\u0432\u0435\u043d\u044c 1* \u2014 15 USDT/\u043c\u0435\u0441
\u251c \u2705 \u0410\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440 \u0437\u0430\u044f\u0432\u043e\u043a
\u251c \u2705 \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0432 \u041b\u0421 \u0431\u043e\u0442\u0430
\u251c \u2705 \u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430
\u251c \u{1f4f1} 1 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e
\u2514 \u{1f381} 24\u0447 \u043f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434 (\u043d\u043e\u0432\u044b\u043c)

\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501

\u2b50\u2b50 *\u0423\u0440\u043e\u0432\u0435\u043d\u044c 2* \u2014 30 USDT/\u043c\u0435\u0441
\u251c \u2705 \u0412\u0441\u0451 \u0438\u0437 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 1
\u251c \u2705 \u0413\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 \u0434\u043b\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u044b
\u251c \u{1f514} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u0433\u0440\u0443\u043f\u043f\u0443
\u2514 \u{1f4f1} \u0414\u043e 2 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432

\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501

\u2b50\u2b50\u2b50 *\u0423\u0440\u043e\u0432\u0435\u043d\u044c 3 (\u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c)* \u2014 35 USDT/\u043c\u0435\u0441
\u251c \u2705 \u041f\u043e\u043b\u043d\u044b\u0439 \u0444\u0443\u043d\u043a\u0446\u0438\u043e\u043d\u0430\u043b
\u251c \u2705 \u0412\u044b\u0431\u043e\u0440 \u043a\u0443\u0434\u0430 \u0441\u043b\u0430\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f
\u251c \u2705 \u041b\u0421 + \u0413\u0440\u0443\u043f\u043f\u0430 \u043e\u0434\u043d\u043e\u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e
\u251c \u2705 VIP \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430
\u2514 \u{1f4f1} \u0414\u043e 3 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432

\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501

\u{1f4b0} *\u041e\u043f\u043b\u0430\u0442\u0430:* USDT (TRC20)

_\u0414\u043b\u044f \u043f\u043e\u043a\u0443\u043f\u043a\u0438 \u043d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 \u0432 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0443_ \u{1f447}`;
          
          await editTelegramMessage(env, chatId, messageId, buyMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4ac} \u041d\u0430\u043f\u0438\u0441\u0430\u0442\u044c @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: '\u{1f4ca} \u0421\u0440\u0430\u0432\u043d\u0438\u0442\u044c \u0442\u0430\u0440\u0438\u0444\u044b', callback_data: 'compare_plans' }],
                [
                  { text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' },
                  { text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }
                ]
              ]
            }
          });
          break;
        }
        
        case 'compare_plans': {
          const compareMsg = `\u{1f4cb} *\u0421\u0440\u0430\u0432\u043d\u0435\u043d\u0438\u0435 \u0442\u0430\u0440\u0438\u0444\u043e\u0432*

| \u0424\u0443\u043d\u043a\u0446\u0438\u044f | L1 | L2 | L3 |
|---------|:--:|:--:|:--:|
| \u0410\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440 | \u2705 | \u2705 | \u2705 |
| \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u041b\u0421 | \u2705 | \u274c | \u2705 |
| \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0413\u0440\u0443\u043f\u043f\u0430 | \u274c | \u2705 | \u2705 |
| \u0412\u044b\u0431\u043e\u0440 \u043a\u0443\u0434\u0430 \u0441\u043b\u0430\u0442\u044c | \u274c | \u274c | \u2705 |
| \u0413\u0440\u0443\u043f. \u0447\u0430\u0442 | \u274c | \u2705 | \u2705 |
| VIP \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430 | \u274c | \u274c | \u2705 |
| \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432 | 1 | 2 | 3 |

\u{1f4b0} *\u0426\u0435\u043d\u044b:*
\u2022 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 1: 15 USDT/\u043c\u0435\u0441
\u2022 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 2: 30 USDT/\u043c\u0435\u0441
\u2022 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 3: 35 USDT/\u043c\u0435\u0441`;`
          
          await editTelegramMessage(env, chatId, messageId, compareMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4ac} \u041a\u0443\u043f\u0438\u0442\u044c @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: '\u2190 \u0422\u0430\u0440\u0438\u0444\u044b', callback_data: 'buy' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'clear_devices': {
          // \u041e\u0447\u0438\u0441\u0442\u0438\u0442\u044c \u0432\u0441\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
          await clearAllDevices(env, telegramId);
          
          const clearMsg = '\u2705 *\u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430 \u043e\u0442\u0432\u044f\u0437\u0430\u043d\u044b*\n\n\u0412\u0441\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430 \u0431\u044b\u043b\u0438 \u0443\u0434\u0430\u043b\u0435\u043d\u044b \u0438\u0437 \u0432\u0430\u0448\u0435\u0433\u043e \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u0430.\n\n\u041f\u0440\u0438 \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u043c \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d\u0438\u0438 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u0431\u0443\u0434\u0435\u0442 \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u043e \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438.';
          
          await editTelegramMessage(env, chatId, messageId, clearMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4f1} \u041c\u043e\u0438 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430', callback_data: 'my_devices' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'my_devices': {
          // \u041f\u043e\u043a\u0430\u0437\u0430\u0442\u044c \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430 (callback \u0432\u0435\u0440\u0441\u0438\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u044b /devices)
          const licenseData = await env.USERS.get('license:tg:' + telegramId);
          const license = licenseData ? JSON.parse(licenseData) : null;
          const licenseType = LICENSE_TYPES[license?.type] || LICENSE_TYPES.trial;
          const maxDevices = licenseType.maxDevices || 1;
          
          const devicesData = await getUserDevices(env, telegramId);
          const devices = devicesData.devices || [];
          
          let message = '\u{1f4f1} *\u0412\u0430\u0448\u0438 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430*\n\n';
          message += '\u{1f4e6} \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430: *' + licenseType.name + '*\n';
          message += '\u{1f522} \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d\u043e: ' + devices.length + '/' + maxDevices + '\n\n';
          
          if (devices.length === 0) {
            message += '_\u041d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d\u043d\u044b\u0445 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432_';
          } else {
            message += '*\u0421\u043f\u0438\u0441\u043e\u043a:*\n';
            devices.forEach((d, i) => {
              const lastSeenDate = new Date(d.lastSeen).toLocaleDateString('ru-RU');
              message += (i + 1) + '. ' + d.deviceId.substring(0, 12) + '... (' + lastSeenDate + ')\n';
            });
          }
          
          const buttons = [];
          if (devices.length > 0) {
            buttons.push([{ text: '\u{1f5d1} \u041e\u0442\u0432\u044f\u0437\u0430\u0442\u044c \u0432\u0441\u0435', callback_data: 'clear_devices' }]);
          }
          buttons.push([{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]);
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: { inline_keyboard: buttons }
          });
          break;
        }
        
        case 'support': {
          // \u041e\u0431\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a \u043a\u043d\u043e\u043f\u043a\u0438 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438
          const supportMsg = '\u{1f4ac} *\u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430*\n\n\u0415\u0441\u043b\u0438 \u0443 \u0432\u0430\u0441 \u0435\u0441\u0442\u044c \u0432\u043e\u043f\u0440\u043e\u0441\u044b \u0438\u043b\u0438 \u043f\u0440\u043e\u0431\u043b\u0435\u043c\u044b:\n\n\u{1f4e9} \u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435: @YaMob\n\u23f0 \u0412\u0440\u0435\u043c\u044f \u043e\u0442\u0432\u0435\u0442\u0430: \u0434\u043e 24 \u0447\u0430\u0441\u043e\u0432\n\n*\u0427\u0430\u0441\u0442\u044b\u0435 \u0432\u043e\u043f\u0440\u043e\u0441\u044b:*\n\u2022 \u041a\u0430\u043a \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435?\n\u2022 \u041a\u0430\u043a \u043f\u0440\u043e\u0434\u043b\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443?\n\u2022 \u041d\u0435 \u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442 \u0430\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440?\n\n\u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u043a\u043d\u043e\u043f\u043a\u0443 \u043d\u0438\u0436\u0435 \u{1f447}';
          
          await editTelegramMessage(env, chatId, messageId, supportMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4ac} \u041d\u0430\u043f\u0438\u0441\u0430\u0442\u044c @YaMob', url: 'https://t.me/YaMob' }],
                [{ text: '\u2753 FAQ', callback_data: 'faq' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'mystats': {
          // \u041b\u0438\u0447\u043d\u0430\u044f \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f (\u0430\u043d\u0430\u043b\u043e\u0433 \u043a\u043e\u043c\u0430\u043d\u0434\u044b /mystats)
          const stats = await getUserStats(env, telegramId);
          const oderId = await env.USERS.get('tg:' + telegramId);
          
          let licenseInfo = '\u274c \u041d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d';
          let licenseType = null;
          if (oderId) {
            const result = await getLicenseData(env, oderId);
            if (result.license) {
              const daysLeft = Math.max(0, Math.ceil((result.license.expiresAt - Date.now()) / MS_PER_DAY));
              const isActive = result.license.expiresAt > Date.now();
              const typeText = LICENSE_TYPES[result.license.type]?.name || result.license.type;
              licenseInfo = isActive 
                ? '\u2705 ' + typeText + ' (' + daysLeft + ' ' + getDaysWord(daysLeft) + ')'
                : '\u274c ' + typeText + ' (\u0438\u0441\u0442\u0435\u043a\u043b\u0430)';
              licenseType = result.license.type;
            }
          }
          
          const referralCode = await getUserReferralCode(env, telegramId);
          
          // \u0424\u043e\u0440\u043c\u0438\u0440\u0443\u0435\u043c \u043a\u0440\u0430\u0441\u0438\u0432\u0443\u044e \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0443
          const todayClicks = stats.todayClicks || 0;
          const totalClicks = stats.totalClicks || 0;
          const referralCount = stats.referralCount || 0;
          
          // \u042d\u043c\u043e\u0434\u0437\u0438 \u0434\u043b\u044f \u0434\u043e\u0441\u0442\u0438\u0436\u0435\u043d\u0438\u0439
          let achievementBadge = '';
          if (totalClicks >= 1000) achievementBadge = '\u{1f3c6} ';
          else if (totalClicks >= 500) achievementBadge = '\u{1f947} ';
          else if (totalClicks >= 100) achievementBadge = '\u{1f948} ';
          else if (totalClicks >= 10) achievementBadge = '\u{1f949} ';
          
          const paidBonuses = stats.referralPaidBonus || 0;
          const pendingBonuses = stats.referralPendingBonus || 0;
          
          const message = '\u{1f4c8} *\u0412\u0430\u0448\u0430 \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430*\n\n' +
            achievementBadge + '\u{1f446} *\u0421\u0435\u0433\u043e\u0434\u043d\u044f:* ' + todayClicks + ' \u043a\u043b\u0438\u043a\u043e\u0432\n' +
            '\u{1f4ca} *\u0412\u0441\u0435\u0433\u043e:* ' + totalClicks + ' \u043a\u043b\u0438\u043a\u043e\u0432\n\n' +
            '\u{1f48e} *\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430:* ' + licenseInfo + '\n\n' +
            '\u{1f465} *\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u043e\u0432:* ' + referralCount + '\n' +
            '\u{1f381} *\u041f\u043e\u043b\u0443\u0447\u0435\u043d\u043e \u0431\u043e\u043d\u0443\u0441\u043e\u0432:* ' + paidBonuses + ' \u0434\u043d.\n' +
            (pendingBonuses > 0 ? '\u23f3 *\u041e\u0436\u0438\u0434\u0430\u044e\u0442 \u043e\u043f\u043b\u0430\u0442\u044b:* ' + pendingBonuses + '\n' : '') +
            '\n\u{1f39f}\ufe0f *\u0412\u0430\u0448 \u043a\u043e\u0434:* ' + referralCode + '\n\n' +
            '_\u0411\u043e\u043d\u0443\u0441 +1 \u0434\u0435\u043d\u044c \u043a\u043e\u0433\u0434\u0430 \u0440\u0435\u0444\u0435\u0440\u0430\u043b \u043e\u0444\u043e\u0440\u043c\u0438\u0442 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443!_';
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4e4} \u041f\u043e\u0434\u0435\u043b\u0438\u0442\u044c\u0441\u044f \u043a\u043e\u0434\u043e\u043c', callback_data: 'share_ref' }],
                [
                  { text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'mystats' },
                  { text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }
                ]
              ]
            }
          });
          break;
        }
        
        case 'referral': {
          // \u0418\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044f \u043e \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u043e\u0439 \u043f\u0440\u043e\u0433\u0440\u0430\u043c\u043c\u0435
          const refCode = await getUserReferralCode(env, telegramId);
          const stats = await getUserStats(env, telegramId);
          const botUsername = env.BOT_USERNAME || 'clickuved_bot';
          
          const paidBonuses = stats.referralPaidBonus || 0;
          const pendingBonuses = stats.referralPendingBonus || 0;
          
          const refMsg = `\u{1f39f}\ufe0f *\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u0430\u044f \u043f\u0440\u043e\u0433\u0440\u0430\u043c\u043c\u0430*\n\n` +
            `\u041f\u0440\u0438\u0433\u043b\u0430\u0448\u0430\u0439\u0442\u0435 \u0434\u0440\u0443\u0437\u0435\u0439 \u0438 \u043f\u043e\u043b\u0443\u0447\u0430\u0439\u0442\u0435\n` +
            `*+1 \u0434\u0435\u043d\u044c* \u043a \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0435!\n\n` +
            `\u2139\ufe0f *\u041a\u0430\u043a \u044d\u0442\u043e \u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442:*\n` +
            `1\ufe0f\u20e3 \u0414\u0440\u0443\u0433 \u043f\u0435\u0440\u0435\u0445\u043e\u0434\u0438\u0442 \u043f\u043e \u0432\u0430\u0448\u0435\u0439 \u0441\u0441\u044b\u043b\u043a\u0435\n` +
            `2\ufe0f\u20e3 \u0414\u0440\u0443\u0433 \u043f\u043e\u043b\u0443\u0447\u0430\u0435\u0442 1 \u0434\u0435\u043d\u044c \u043f\u0440\u043e\u0431\u043d\u0438\u043a\u0430\n` +
            `3\ufe0f\u20e3 \u041a\u043e\u0433\u0434\u0430 \u0434\u0440\u0443\u0433 \u043e\u0444\u043e\u0440\u043c\u0438\u0442 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443 \u2014 \u0432\u044b \u043f\u043e\u043b\u0443\u0447\u0430\u0435\u0442\u0435 +1 \u0434\u0435\u043d\u044c!\n\n` +
            `\u{1f4ca} *\u0412\u0430\u0448\u0438 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044b:* ${stats.referralCount || 0}\n` +
            `\u{1f381} *\u041f\u043e\u043b\u0443\u0447\u0435\u043d\u043e \u0431\u043e\u043d\u0443\u0441\u043e\u0432:* ${paidBonuses} \u0434\u043d.\n` +
            (pendingBonuses > 0 ? `\u23f3 *\u041e\u0436\u0438\u0434\u0430\u044e\u0442 \u043e\u0444\u043e\u0440\u043c\u043b\u0435\u043d\u0438\u044f:* ${pendingBonuses}\n` : '') +
            `\n\u{1f39f}\ufe0f *\u0412\u0430\u0448 \u043a\u043e\u0434:* \`${refCode}\`\n\n` +
            `\u{1f449} \u0421\u0441\u044b\u043b\u043a\u0430 \u0434\u043b\u044f \u0434\u0440\u0443\u0437\u0435\u0439:\n` +
            `\`https://t.me/${botUsername}?start=ref_${refCode}\``;
          
          await editTelegramMessage(env, chatId, messageId, refMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4e4} \u041f\u043e\u0434\u0435\u043b\u0438\u0442\u044c\u0441\u044f', callback_data: 'share_ref' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'settings': {
          // \u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f - \u043c\u0435\u043d\u044e \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          
          if (!oderId) {
            await editTelegramMessage(env, chatId, messageId,
              `\u2699\ufe0f *\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438*\n\n` +
              `\u274c \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n\n` +
              `\u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u0434\u043b\u044f \u0434\u043e\u0441\u0442\u0443\u043f\u0430 \u043a \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430\u043c.`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '\u{1f517} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c', callback_data: 'how_to_connect' }],
                  [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
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
          const canChooseDest = licenseType === 'level3'; // \u0422\u043e\u043b\u044c\u043a\u043e level3 \u043c\u043e\u0436\u0435\u0442 \u0432\u044b\u0431\u0438\u0440\u0430\u0442\u044c
          
          // \u0418\u043a\u043e\u043d\u043a\u0438 \u0441\u0442\u0430\u0442\u0443\u0441\u0430
          const icon = (enabled) => enabled ? '\u2705' : '\u274c';
          
          // \u041e\u043f\u0438\u0441\u0430\u043d\u0438\u0435 \u043a\u0443\u0434\u0430 \u043f\u0440\u0438\u0445\u043e\u0434\u044f\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f
          let destText = '';
          if (licenseType === 'trial' || licenseType === 'level1') {
            destText = `\u{1f4cd} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f: \u0432 \u043b\u0438\u0447\u043d\u044b\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f`;
          } else if (licenseType === 'level2') {
            destText = `\u{1f4cd} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f: \u0432 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442`;
          } else if (licenseType === 'level3') {
            const destMap = { dm: '\u041b\u0421', group: '\u0413\u0440\u0443\u043f\u043f\u0430', both: '\u041b\u0421 + \u0413\u0440\u0443\u043f\u043f\u0430' };
            const currentDest = userSettings.notifyDestination || 'dm';
            destText = `\u{1f4cd} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f: ${destMap[currentDest] || '\u041b\u0421'}`;
          }
          
          const settingsMsg = `\u2699\ufe0f *\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439*\n\n` +
            `${icon(notif.clicks)} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445\n` +
            `${icon(notif.expiryReminders)} \u041d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u044f \u043e \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0435\n` +
            `${icon(notif.news)} \u041d\u043e\u0432\u043e\u0441\u0442\u0438 \u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f\n\n` +
            (destText ? `${destText}\n\n` : '') +
            `_\u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u043d\u0430 \u043a\u043d\u043e\u043f\u043a\u0443 \u0447\u0442\u043e\u0431\u044b \u043f\u0435\u0440\u0435\u043a\u043b\u044e\u0447\u0438\u0442\u044c_`;
          
          const keyboard = [
            [
              { text: `${icon(notif.clicks)} \u0417\u0430\u044f\u0432\u043a\u0438`, callback_data: 'toggle_notif_clicks' },
              { text: `${icon(notif.expiryReminders)} \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430`, callback_data: 'toggle_notif_expiry' }
            ],
            [{ text: `${icon(notif.news)} \u041d\u043e\u0432\u043e\u0441\u0442\u0438`, callback_data: 'toggle_notif_news' }],
            [{ text: '\u{1f514} \u0422\u0435\u0441\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439', callback_data: 'test_notify' }]
          ];
          
          // \u0412\u044b\u0431\u043e\u0440 \u043c\u0435\u0441\u0442\u0430 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439 \u0434\u043b\u044f level3
          if (canChooseDest) {
            keyboard.push([{ text: '\u{1f4cd} \u041a\u0443\u0434\u0430 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0442\u044c', callback_data: 'notify_dest_menu' }]);
          }
          
          if (canUseGroups) {
            keyboard.push([{ text: '\u{1f4ac} \u0413\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b', callback_data: 'groups_menu' }]);
          }
          
          keyboard.push([{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]);
          
          await editTelegramMessage(env, chatId, messageId, settingsMsg, {
            reply_markup: { inline_keyboard: keyboard }
          });
          break;
        }
        
        // \u041c\u0435\u043d\u044e \u0432\u044b\u0431\u043e\u0440\u0430 \u043c\u0435\u0441\u0442\u0430 \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0438 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439 (\u0442\u043e\u043b\u044c\u043a\u043e level3)
        case 'notify_dest_menu': {
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          const result = await getLicenseData(env, oderId || '');
          
          if (!result.license || result.license.type !== 'level3') {
            await answerCallbackQuery(env, callback.id, '\u0414\u043e\u0441\u0442\u0443\u043f\u043d\u043e \u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f \u0423\u0440\u043e\u0432\u0435\u043d\u044c 3', true);
            break;
          }
          
          const userSettings = await getUserSettings(env, telegramId);
          const currentDest = userSettings.notifyDestination || 'dm';
          
          const check = (val) => currentDest === val ? '\u2705 ' : '';
          
          const msg = `\u{1f4cd} *\u041a\u0443\u0434\u0430 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f?*\n\n` +
            `\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435, \u043a\u0443\u0434\u0430 \u0431\u0443\u0434\u0443\u0442 \u043f\u0440\u0438\u0445\u043e\u0434\u0438\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445:\n\n` +
            `\u2022 *\u041b\u0421* \u2014 \u0432 \u043b\u0438\u0447\u043d\u044b\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f \u0431\u043e\u0442\u0430\n` +
            `\u2022 *\u0413\u0440\u0443\u043f\u043f\u0430* \u2014 \u0432 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d\u043d\u044b\u0439 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442\n` +
            `\u2022 *\u041e\u0431\u0430* \u2014 \u0438 \u0442\u0443\u0434\u0430, \u0438 \u0442\u0443\u0434\u0430`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [
                { text: `${check('dm')}\u{1f4f1} \u041b\u0421`, callback_data: 'set_dest_dm' },
                { text: `${check('group')}\u{1f4ac} \u0413\u0440\u0443\u043f\u043f\u0430`, callback_data: 'set_dest_group' }
              ],
              [{ text: `${check('both')}\u{1f4f1}+\u{1f4ac} \u041e\u0431\u0430`, callback_data: 'set_dest_both' }],
              [{ text: '\u2190 \u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438', callback_data: 'settings' }]
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
            await answerCallbackQuery(env, callback.id, '\u0414\u043e\u0441\u0442\u0443\u043f\u043d\u043e \u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f \u0423\u0440\u043e\u0432\u0435\u043d\u044c 3', true);
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
          
          const destNames = { dm: '\u041b\u0421', group: '\u0413\u0440\u0443\u043f\u043f\u0430', both: '\u041b\u0421 + \u0413\u0440\u0443\u043f\u043f\u0430' };
          await answerCallbackQuery(env, callback.id, `\u2705 \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f: ${destNames[newDest]}`);
          
          // \u0412\u043e\u0437\u0432\u0440\u0430\u0449\u0430\u0435\u043c\u0441\u044f \u0432 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438
          // \u041f\u0435\u0440\u0435\u043d\u0430\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u043e\u0431\u0440\u0430\u0442\u043d\u043e \u0432 \u043c\u0435\u043d\u044e
          const check = (val) => newDest === val ? '\u2705 ' : '';
          
          const msg = `\u{1f4cd} *\u041a\u0443\u0434\u0430 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f?*\n\n` +
            `\u2705 \u0412\u044b\u0431\u0440\u0430\u043d\u043e: *${destNames[newDest]}*`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [
                { text: `${check('dm')}\u{1f4f1} \u041b\u0421`, callback_data: 'set_dest_dm' },
                { text: `${check('group')}\u{1f4ac} \u0413\u0440\u0443\u043f\u043f\u0430`, callback_data: 'set_dest_group' }
              ],
              [{ text: `${check('both')}\u{1f4f1}+\u{1f4ac} \u041e\u0431\u0430`, callback_data: 'set_dest_both' }],
              [{ text: '\u2190 \u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438', callback_data: 'settings' }]
            ]}
          });
          break;
        }
        
        // \u041f\u0435\u0440\u0435\u043a\u043b\u044e\u0447\u0430\u0442\u0435\u043b\u0438 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439
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
          
          const icon = (enabled) => enabled ? '\u2705' : '\u274c';
          
          const oderId = await env.USERS.get(`tg:${telegramId}`);
          const result = await getLicenseData(env, oderId || '');
          const licenseType = result.license ? result.license.type : null;
          const canUseGroups = result.license && LICENSE_TYPES[licenseType]?.groupChats;
          const canChooseDest = licenseType === 'level3';
          
          // \u041e\u043f\u0438\u0441\u0430\u043d\u0438\u0435 \u043a\u0443\u0434\u0430 \u043f\u0440\u0438\u0445\u043e\u0434\u044f\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f
          let destText = '';
          if (licenseType === 'trial' || licenseType === 'level1') {
            destText = `\u{1f4cd} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f: \u0432 \u043b\u0438\u0447\u043d\u044b\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f`;
          } else if (licenseType === 'level2') {
            destText = `\u{1f4cd} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f: \u0432 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442`;
          } else if (licenseType === 'level3') {
            const destMap = { dm: '\u041b\u0421', group: '\u0413\u0440\u0443\u043f\u043f\u0430', both: '\u041b\u0421 + \u0413\u0440\u0443\u043f\u043f\u0430' };
            const currentDest = updatedSettings.notifyDestination || 'dm';
            destText = `\u{1f4cd} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f: ${destMap[currentDest] || '\u041b\u0421'}`;
          }
          
          const settingsMsg = `\u2699\ufe0f *\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439*\n\n` +
            `${icon(notif.clicks)} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445\n` +
            `${icon(notif.expiryReminders)} \u041d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u044f \u043e \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0435\n` +
            `${icon(notif.news)} \u041d\u043e\u0432\u043e\u0441\u0442\u0438 \u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f\n\n` +
            (destText ? `${destText}\n\n` : '') +
            `_\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430 \u0438\u0437\u043c\u0435\u043d\u0435\u043d\u0430!_`;
          
          const keyboard = [
            [
              { text: `${icon(notif.clicks)} \u0417\u0430\u044f\u0432\u043a\u0438`, callback_data: 'toggle_notif_clicks' },
              { text: `${icon(notif.expiryReminders)} \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430`, callback_data: 'toggle_notif_expiry' }
            ],
            [{ text: `${icon(notif.news)} \u041d\u043e\u0432\u043e\u0441\u0442\u0438`, callback_data: 'toggle_notif_news' }],
            [{ text: '\u{1f514} \u0422\u0435\u0441\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439', callback_data: 'test_notify' }]
          ];
          
          if (canChooseDest) {
            keyboard.push([{ text: '\u{1f4cd} \u041a\u0443\u0434\u0430 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0442\u044c', callback_data: 'notify_dest_menu' }]);
          }
          
          if (canUseGroups) {
            keyboard.push([{ text: '\u{1f4ac} \u0413\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b', callback_data: 'groups_menu' }]);
          }
          
          keyboard.push([{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]);
          
          await editTelegramMessage(env, chatId, messageId, settingsMsg, {
            reply_markup: { inline_keyboard: keyboard }
          });
          
          await answerCallbackQuery(env, callback.id, '\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430 \u0438\u0437\u043c\u0435\u043d\u0435\u043d\u0430!');
          break;
        }
        
        case 'groups_menu': {
          // \u041c\u0435\u043d\u044e \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0445 \u0447\u0430\u0442\u043e\u0432
          const canUse = await canUseGroupChats(env, telegramId);
          
          if (!canUse.allowed) {
            let errorMsg = '';
            switch (canUse.reason) {
              case 'NOT_CONNECTED':
                errorMsg = '\u274c \u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u0440\u0438\u0432\u044f\u0436\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435';
                break;
              case 'NO_LICENSE':
                errorMsg = '\u274c \u041d\u0435\u0442 \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0439 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438';
                break;
              case 'UPGRADE_REQUIRED':
                errorMsg = `\u274c \u0414\u043e\u0441\u0442\u0443\u043f\u043d\u043e \u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f *\u0423\u0440\u043e\u0432\u0435\u043d\u044c 2* \u0438 *\u0423\u0440\u043e\u0432\u0435\u043d\u044c 3*\n\n\u0412\u0430\u0448\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430: ${canUse.currentType || '\u0411\u0430\u0437\u043e\u0432\u0430\u044f'}`;
                break;
              default:
                errorMsg = '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430 \u043a \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u043c \u0447\u0430\u0442\u0430\u043c';
            }
            
            await editTelegramMessage(env, chatId, messageId, 
              `\u{1f4ac} *\u0413\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b*\n\n${errorMsg}`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '\u{1f4b3} \u0423\u043b\u0443\u0447\u0448\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
                  [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          const groupData = await getUserGroupChats(env, telegramId);
          
          let message = `\u{1f4ac} *\u0413\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b*\n\n`;
          
          if (groupData.chats.length === 0) {
            message += `\u0423 \u0432\u0430\u0441 \u043f\u043e\u043a\u0430 \u043d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d\u043d\u044b\u0445 \u0433\u0440\u0443\u043f\u043f.\n\n`;
            message += `*\u041a\u0430\u043a \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c:*\n`;
            message += `1\ufe0f\u20e3 \u0414\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u0431\u043e\u0442\u0430 @clickuved\\_bot \u0432 \u0433\u0440\u0443\u043f\u043f\u0443\n`;
            message += `2\ufe0f\u20e3 \u0411\u043e\u0442 \u0437\u0430\u043f\u0440\u043e\u0441\u0438\u0442 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u0435 \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430\n`;
            message += `3\ufe0f\u20e3 \u041f\u043e\u0441\u043b\u0435 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u044f \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0431\u0443\u0434\u0443\u0442 \u043f\u0440\u0438\u0445\u043e\u0434\u0438\u0442\u044c \u0432 \u0433\u0440\u0443\u043f\u043f\u0443`;
          } else {
            message += `*\u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d\u043d\u044b\u0435 \u0433\u0440\u0443\u043f\u043f\u044b (${groupData.chats.length}):*\n\n`;
            groupData.chats.forEach((chat, i) => {
              message += `${i + 1}. ${chat.chatTitle}\n`;
            });
            message += `\n_\u041b\u0438\u043c\u0438\u0442: ${canUse.maxChats} \u0433\u0440\u0443\u043f\u043f\u044b_`;
          }
          
          await editTelegramMessage(env, chatId, messageId, message, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4d6} \u0418\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u044f', callback_data: 'groups_help' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'groups_help': {
          const helpMsg = `\u{1f4d6} *\u041a\u0430\u043a \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442*\n\n` +
            `1\ufe0f\u20e3 \u0421\u043e\u0437\u0434\u0430\u0439\u0442\u0435 \u0433\u0440\u0443\u043f\u043f\u0443 \u0432 Telegram\n` +
            `2\ufe0f\u20e3 \u0414\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u0431\u043e\u0442\u0430 @${env.BOT_USERNAME || 'clickuved_bot'}\n` +
            `3\ufe0f\u20e3 \u0411\u043e\u0442 \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442 \u0437\u0430\u043f\u0440\u043e\u0441 \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0443\n` +
            `4\ufe0f\u20e3 \u041f\u043e\u0441\u043b\u0435 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u044f \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f\n     \u0431\u0443\u0434\u0443\u0442 \u043f\u0440\u0438\u0445\u043e\u0434\u0438\u0442\u044c \u0432 \u0433\u0440\u0443\u043f\u043f\u0443\n\n` +
            `\u26a0\ufe0f *\u041b\u0438\u043c\u0438\u0442:* 1 \u0433\u0440\u0443\u043f\u043f\u0430 \u043d\u0430 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\n` +
            `\u{1f48e} *\u0422\u0440\u0435\u0431\u0443\u0435\u0442\u0441\u044f:* Premium \u0438\u043b\u0438 \u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c`;
          
          await editTelegramMessage(env, chatId, messageId, helpMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'groups_menu' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'faq': {
          const faqMsg = `\u2753 *\u0427\u0430\u0441\u0442\u044b\u0435 \u0432\u043e\u043f\u0440\u043e\u0441\u044b*

*\u{1f512} \u0411\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u043e \u043b\u0438 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435?*
\u0414\u0430, \u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442 \u0442\u043e\u043b\u044c\u043a\u043e \u043d\u0430 exotic.company
\u041d\u0435 \u0441\u043e\u0431\u0438\u0440\u0430\u0435\u0442 \u043b\u0438\u0447\u043d\u044b\u0435 \u0434\u0430\u043d\u043d\u044b\u0435

*\u{1f4f1} \u0421\u043a\u043e\u043b\u044c\u043a\u043e \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432?*
\u2022 \u041f\u0440\u0435\u043c\u0438\u0443\u043c: 1 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e
\u2022 \u041f\u0440\u0435\u043c\u0438\u0443\u043c+: \u0434\u043e 3 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432
\u2022 \u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c: \u0434\u043e 5 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432

*\u23f0 \u0427\u0442\u043e \u0435\u0441\u043b\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0438\u0441\u0442\u0435\u0447\u0451\u0442?*
\u0410\u0432\u0442\u043e\u043a\u043b\u0438\u043a \u043e\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u0441\u044f
\u0414\u0430\u043d\u043d\u044b\u0435 \u0438 \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u0441\u043e\u0445\u0440\u0430\u043d\u044f\u0442\u0441\u044f

*\u{1f4b0} \u0415\u0441\u0442\u044c \u0432\u043e\u0437\u0432\u0440\u0430\u0442?*
\u0414\u0430, \u0432 \u0442\u0435\u0447\u0435\u043d\u0438\u0435 24\u0447 \u0435\u0441\u043b\u0438 \u043d\u0435 \u0440\u0430\u0431\u043e\u0442\u0430\u043b\u0438

*\u{1f39f}\ufe0f \u0415\u0441\u0442\u044c \u043f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434?*
\u0414\u0430, 24 \u0447\u0430\u0441\u0430 \u043f\u043e\u0441\u043b\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f

*\u{1f4b3} \u041a\u0430\u043a\u0438\u0435 \u0442\u0430\u0440\u0438\u0444\u044b?*
\u2022 \u041f\u0440\u0435\u043c\u0438\u0443\u043c: 30 USDT/\u043c\u0435\u0441
\u2022 \u041f\u0440\u0435\u043c\u0438\u0443\u043c+: 40 USDT/\u043c\u0435\u0441

*\u{1f504} \u041a\u0430\u043a \u043f\u0440\u043e\u0434\u043b\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443?*
\u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 @YaMob`;
          
          await editTelegramMessage(env, chatId, messageId, faqMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4b3} \u0422\u0430\u0440\u0438\u0444\u044b', callback_data: 'buy' }],
                [{ text: '\u{1f4ac} \u0417\u0430\u0434\u0430\u0442\u044c \u0432\u043e\u043f\u0440\u043e\u0441', url: 'https://t.me/YaMob' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'install': {
          const installMsg = `\u{1f4e5} *\u0423\u0441\u0442\u0430\u043d\u043e\u0432\u043a\u0430 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f*

*Chrome / Edge / Brave / Opera:*
1\ufe0f\u20e3 \u041e\u0442\u043a\u0440\u043e\u0439\u0442\u0435 Chrome Web Store
2\ufe0f\u20e3 \u041d\u0430\u0439\u0434\u0438\u0442\u0435 "Exotic Assistant"
3\ufe0f\u20e3 \u041d\u0430\u0436\u043c\u0438\u0442\u0435 "\u0423\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c"
4\ufe0f\u20e3 \u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u0442\u0435 \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u043a\u0443
5\ufe0f\u20e3 \u0413\u043e\u0442\u043e\u0432\u043e! \u2705

\u2699\ufe0f *\u041f\u043e\u0441\u043b\u0435 \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u043a\u0438:*
\u2022 \u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u043d\u0430 \u0438\u043a\u043e\u043d\u043a\u0443 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f
\u2022 \u041e\u0442\u043a\u0440\u043e\u0439\u0442\u0435 "\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438"
\u2022 \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u0435 Telegram
\u2022 \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 24\u0447 \u0431\u0435\u0441\u043f\u043b\u0430\u0442\u043d\u043e! \u{1f381}`;
          
          await editTelegramMessage(env, chatId, messageId, installMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f310} Chrome Web Store', url: 'https://chromewebstore.google.com/' }],
                [{ text: '\u{1f517} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c Telegram', callback_data: 'how_to_connect' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'news': {
          const newsMsg = `\u{1f4f0} *\u041d\u043e\u0432\u043e\u0441\u0442\u0438 \u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f*

*v${BOT_VERSION} \u2014 \u042f\u043d\u0432\u0430\u0440\u044c 2026*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u2705 \u0423\u043b\u0443\u0447\u0448\u0435\u043d \u0430\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440 (100% \u043d\u0430\u0434\u0451\u0436\u043d\u043e\u0441\u0442\u044c)
\u2705 \u0414\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u044b \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e\u0431 \u0438\u0441\u0442\u0435\u0447\u0435\u043d\u0438\u0438
\u2705 Inline \u0440\u0435\u0436\u0438\u043c \u0434\u043b\u044f \u0448\u0430\u0440\u0438\u043d\u0433\u0430
\u2705 Rate limiting \u043e\u0442 \u0441\u043f\u0430\u043c\u0430
\u2705 \u041d\u043e\u0432\u044b\u0435 \u0440\u0430\u0437\u0434\u0435\u043b\u044b \u0432 \u043c\u0435\u043d\u044e

*\u041f\u043b\u0430\u043d\u0438\u0440\u0443\u0435\u0442\u0441\u044f:*
\u{1f51c} \u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u043f\u043e \u0434\u043d\u044f\u043c
\u{1f51c} \u042d\u043a\u0441\u043f\u043e\u0440\u0442 \u0434\u0430\u043d\u043d\u044b\u0445
\u{1f51c} \u0422\u0451\u043c\u043d\u0430\u044f \u0442\u0435\u043c\u0430

\u{1f4a1} \u0415\u0441\u0442\u044c \u0438\u0434\u0435\u0438? \u041f\u0438\u0448\u0438\u0442\u0435 @YaMob`;
          
          await editTelegramMessage(env, chatId, messageId, newsMsg, {
            reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4ac} \u041f\u0440\u0435\u0434\u043b\u043e\u0436\u0438\u0442\u044c \u0438\u0434\u0435\u044e', url: 'https://t.me/YaMob' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'share_ref': {
          // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434 \u0438 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0434\u043b\u044f \u043f\u0435\u0440\u0435\u0441\u044b\u043b\u043a\u0438
          const refCode = await getUserReferralCode(env, telegramId);
          const botUsername = env.BOT_USERNAME || 'clickuved_bot';
          
          const shareMsg = `\u{1f381} *\u041f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435 \u0432 Exotic Assistant*\n\n` +
            `\u041f\u0440\u0438\u0432\u0435\u0442! \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439 \u0430\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440 \u0434\u043b\u044f Exotic.\n\n` +
            `\u{1f39f}\ufe0f \u041c\u043e\u0439 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434: \`${refCode}\`\n\n` +
            `\u{1f449} \u041f\u0435\u0440\u0435\u0439\u0434\u0438 \u043a \u0431\u043e\u0442\u0443: @${botUsername}\n` +
            `\u0418 \u0432\u0432\u0435\u0434\u0438 \u043a\u043e\u043c\u0430\u043d\u0434\u0443: \`/ref ${refCode}\``;
          
          await sendTelegramMessage(env, chatId, shareMsg);
          await answerCallbackQuery(env, callback.id, '\u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e, \u043f\u0435\u0440\u0435\u0448\u043b\u0438\u0442\u0435 \u0435\u0433\u043e \u0434\u0440\u0443\u0433\u0443!');
          break;
        }
        
        case 'test_notify': {
          // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0443 \u0438 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e \u0447\u0435\u0440\u0435\u0437 getLicenseData
          const testOderId = await env.USERS.get(`tg:${telegramId}`);
          
          if (!testOderId) {
            await editTelegramMessage(env, chatId, messageId,
              `\u{1f514} *\u0422\u0435\u0441\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439*\n\n` +
              `\u274c \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n\n` +
              `\u0427\u0442\u043e\u0431\u044b \u043f\u043e\u043b\u0443\u0447\u0430\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445, \n\u0441\u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435.`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '\u{1f517} \u041a\u0430\u043a \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c', callback_data: 'how_to_connect' }],
                  [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          // \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0435\u043c getLicenseData \u0434\u043b\u044f \u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u043e\u0439 \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438
          const testResult = await getLicenseData(env, testOderId);
          
          if (!testResult.license) {
            await editTelegramMessage(env, chatId, messageId,
              `\u{1f514} *\u0422\u0435\u0441\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439*\n\n` +
              `\u274c \u041d\u0435\u0442 \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0439 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438\n\n` +
              `\u0414\u043b\u044f \u043f\u043e\u043b\u0443\u0447\u0435\u043d\u0438\u044f \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439 \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445\n\u043d\u0443\u0436\u043d\u0430 \u0430\u043a\u0442\u0438\u0432\u043d\u0430\u044f \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430.`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
                  [{ text: '\u{1f4ac} \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430', callback_data: 'support' }],
                  [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          if (testResult.license.expiresAt < Date.now()) {
            await editTelegramMessage(env, chatId, messageId,
              `\u{1f514} *\u0422\u0435\u0441\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439*\n\n` +
              `\u274c \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0438\u0441\u0442\u0435\u043a\u043b\u0430\n\n` +
              `\u041f\u0440\u043e\u0434\u043b\u0438\u0442\u0435 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443 \u0434\u043b\u044f \u043f\u043e\u043b\u0443\u0447\u0435\u043d\u0438\u044f\n\u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439 \u043e \u043d\u043e\u0432\u044b\u0445 \u0437\u0430\u044f\u0432\u043a\u0430\u0445.`,
              { reply_markup: {
                inline_keyboard: [
                  [{ text: '\u{1f4b3} \u041f\u0440\u043e\u0434\u043b\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
                  [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
                ]
              }}
            );
            break;
          }
          
          // \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0442\u0435\u0441\u0442\u043e\u0432\u043e\u0435 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435
          const time = new Date().toLocaleString('ru-RU', { 
            timeZone: 'Europe/Moscow',
            hour: '2-digit', 
            minute: '2-digit'
          });
          
          const testMsg = `\u{1f514} *\u041d\u043e\u0432\u0430\u044f \u0437\u0430\u044f\u0432\u043a\u0430!*\n\n` +
            `\u{1f4b0} \u0421\u0443\u043c\u043c\u0430: *1 000 \u20bd*\n` +
            `\u{1f4b3} \u0421\u043f\u043e\u0441\u043e\u0431: \u0422\u0435\u0441\u0442\u043e\u0432\u0430\u044f \u043a\u0430\u0440\u0442\u0430\n` +
            `\u{1f522} \u0420\u0435\u043a\u0432\u0438\u0437\u0438\u0442\u044b: \`0000 0000 0000 0000\`\n` +
            `\u{1f3e6} \u0411\u0430\u043d\u043a: \u0422\u0435\u0441\u0442\n\n` +
            `\u23f0 ${time} (\u041c\u0421\u041a)`;
          
          await sendTelegramMessage(env, chatId, testMsg);
          
          await editTelegramMessage(env, chatId, messageId,
            `\u2705 *\u0422\u0435\u0441\u0442 \u0443\u0441\u043f\u0435\u0448\u0435\u043d!*\n\n` +
            `\u0422\u0435\u0441\u0442\u043e\u0432\u043e\u0435 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e.\n\u0415\u0441\u043b\u0438 \u0432\u0438\u0434\u0438\u0442\u0435 \u0435\u0433\u043e \u0432\u044b\u0448\u0435 \u2014 \u0432\u0441\u0451 \u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442! \u{1f446}`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f514} \u041f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u044c \u0442\u0435\u0441\u0442', callback_data: 'test_notify' }],
                [{ text: '\u{1f4ca} \u041c\u043e\u0439 \u0441\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }}
          );
          break;
        }
        
        // === \u0421\u0415\u041a\u0420\u0415\u0422\u041d\u0410\u042f \u0410\u0414\u041c\u0418\u041d-\u041f\u0410\u041d\u0415\u041b\u042c (CONTROL PANEL) ===
        // \u041f\u0435\u0440\u0435\u043d\u0430\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u0441\u0442\u0430\u0440\u044b\u0445 cp_* \u043d\u0430 \u043d\u043e\u0432\u044b\u0435 adm_*
        case 'cp_pending_groups': {
          // \u0420\u0435\u0434\u0438\u0440\u0435\u043a\u0442 \u043d\u0430 \u043d\u043e\u0432\u044b\u0439 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a
          action = 'adm_pending';
          // fall through
        }
        
        case 'cp_users': {
          // \u0420\u0435\u0434\u0438\u0440\u0435\u043a\u0442 \u043d\u0430 \u043d\u043e\u0432\u044b\u0439 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a
          action = 'adm_users';
          // fall through
        }
        
        case 'cp_stats': {
          // \u0420\u0435\u0434\u0438\u0440\u0435\u043a\u0442 \u043d\u0430 \u043d\u043e\u0432\u044b\u0439 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a
          action = 'adm_stats';
          // fall through - \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442\u0441\u044f \u0432 adm_stats
        }
        
        case 'cp_grant': {
          // \u0420\u0435\u0434\u0438\u0440\u0435\u043a\u0442 \u043d\u0430 \u043d\u043e\u0432\u044b\u0439 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a
          action = 'adm_grant';
          // fall through
        }
        
        case 'cp_check': {
          // \u0420\u0435\u0434\u0438\u0440\u0435\u043a\u0442 \u043d\u0430 \u043d\u043e\u0432\u044b\u0439 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a
          action = 'adm_search';
          // fall through
        }
        
        case 'cp_broadcast': {
          // \u0420\u0435\u0434\u0438\u0440\u0435\u043a\u0442 \u043d\u0430 \u043d\u043e\u0432\u044b\u0439 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a
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
            const msg = `\u{1f527} *\u0421\u0438\u0441\u0442\u0435\u043c\u0430*\n\n` +
              `\u{1f4e6} \u0412\u0435\u0440\u0441\u0438\u044f \u0431\u043e\u0442\u0430: ${BOT_VERSION}\n` +
              `\u{1f4e6} \u0412\u0435\u0440\u0441\u0438\u044f \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f: ${EXTENSION_VERSION}\n` +
              `\u23f1 Uptime: ${uptimeHours}\u0447 ${uptimeMinutes}\u043c\n\n` +
              `*\u041a\u043e\u043d\u0444\u0438\u0433\u0443\u0440\u0430\u0446\u0438\u044f:*\n` +
              `\u251c BOT\_USERNAME: ${env.BOT_USERNAME || '\u043d\u0435 \u0437\u0430\u0434\u0430\u043d'}\n` +
              `\u251c ADMIN\_USERNAME: ${env.ADMIN_USERNAME || '\u043d\u0435 \u0437\u0430\u0434\u0430\u043d'}\n` +
              `\u251c LOG\_CHAT\_ID: ${env.LOG_CHAT_ID ? '\u2705' : '\u274c'}\n` +
              `\u251c ADMIN\_TELEGRAM\_ID: ${env.ADMIN_TELEGRAM_ID ? '\u2705' : '\u274c'}\n` +
              `\u2514 WEBHOOK\_SECRET: ${env.TELEGRAM_WEBHOOK_SECRET ? '\u2705' : '\u274c'}`;
            try {
              await editTelegramMessage(env, chatId, messageId, msg, {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '\u{1f4cb} \u041b\u043e\u0433\u0438', callback_data: 'cp_logs' }],
                    [{ text: '\u{1f504} Webhook Info', callback_data: 'cp_webhook' }],
                    [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
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
            let msg = `\u{1f4cb} *\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 \u043e\u0448\u0438\u0431\u043a\u0438*\n\n`;
            if (metrics.errors.length === 0) {
              msg += `_\u041e\u0448\u0438\u0431\u043e\u043a \u043d\u0435\u0442_ \u2728`;
            } else {
              const recentErrors = metrics.errors.slice(-10);
              for (const err of recentErrors) {
                const time = new Date(err.time).toLocaleTimeString('ru-RU');
                msg += `\u{1f534} ${time} - ${err.endpoint}\n`;
                msg += `\`${err.error.substring(0, 50)}\`\n\n`;
              }
            }
            try {
              await editTelegramMessage(env, chatId, messageId, msg, {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'cp_logs' }],
                    [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
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
              const msg = `\u{1f517} *Webhook Info*\n\n` +
                `URL: \`${webhookInfo.url || '\u043d\u0435 \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d'}\`\n` +
                `Pending: ${webhookInfo.pending_update_count || 0}\n` +
                `Last Error: ${webhookInfo.last_error_message || '\u043d\u0435\u0442'}\n` +
                `Max Connections: ${webhookInfo.max_connections || 40}`;
              await editTelegramMessage(env, chatId, messageId, msg, {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'cp_webhook' }],
                    [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
                  ]
                }
              });
            } catch (err) {
              const stats = await getAdminStats(env);
              const pendingGroups = await getAllPendingGroups(env);
              await handleAdminPanelError(env, chatId, messageId, err, stats, pendingGroups);
            }
          } catch (err) {
            await answerCallbackQuery(env, callback.id, '\u041e\u0448\u0438\u0431\u043a\u0430 \u043f\u043e\u043b\u0443\u0447\u0435\u043d\u0438\u044f webhook info', true);
          }
          break;
        }
        
        case 'cp_logout': {
          await destroyAdminSession(env, telegramId);
          await editTelegramMessage(env, chatId, messageId,
            `\u{1f6aa} *\u0421\u0435\u0441\u0441\u0438\u044f \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430*\n\n` +
            `\u0412\u044b \u0432\u044b\u0448\u043b\u0438 \u0438\u0437 \u043f\u0430\u043d\u0435\u043b\u0438 \u0443\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u044f.`,
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
        
        // === \u041d\u041e\u0412\u042b\u0415 \u041e\u0411\u0420\u0410\u0411\u041e\u0422\u0427\u0418\u041a\u0418 \u0410\u0414\u041c\u0418\u041d-\u041f\u0410\u041d\u0415\u041b\u0418 (adm_*) ===
        case 'adm_pending': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const pendingGroups = await getAllPendingGroups(env);
          if (pendingGroups.length === 0) {
            await editTelegramMessage(env, chatId, messageId,
              `\u2705 *\u041d\u0435\u0442 \u043e\u0436\u0438\u0434\u0430\u044e\u0449\u0438\u0445 \u0437\u0430\u044f\u0432\u043e\u043a*\n\n` +
              `\u0412\u0441\u0435 \u0437\u0430\u044f\u0432\u043a\u0438 \u043d\u0430 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u0435 \u0447\u0430\u0442\u043e\u0432 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u043d\u044b.`,
              { reply_markup: { inline_keyboard: [[{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]] }}
            );
            break;
          }
          
          let msg = `\u{1f514} *\u0417\u0430\u044f\u0432\u043a\u0438 \u043d\u0430 \u0447\u0430\u0442\u044b (${pendingGroups.length})*\n` +
            `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n`;
          const buttons = [];
          
          for (const group of pendingGroups.slice(0, 5)) {
            const requestDate = new Date(group.requestedAt).toLocaleDateString('ru-RU');
            const safeTitle = escapeMarkdown(group.chatTitle || '\u0411\u0435\u0437 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u044f');
            msg += `\u{1f4cc} *${safeTitle}*\n`;
            msg += `\u251c ID: \`${group.chatId}\`\n`;
            msg += `\u251c \u041e\u0442: @${group.requestedByUsername || 'unknown'}\n`;
            msg += `\u251c TG ID: \`${group.requestedBy}\`\n`;
            msg += `\u2514 \u0414\u0430\u0442\u0430: ${requestDate}\n\n`;
            
            buttons.push([
              { text: `\u2705 \u041e\u0434\u043e\u0431\u0440\u0438\u0442\u044c`, callback_data: `approve_group:${group.chatId}` },
              { text: `\u274c \u041e\u0442\u043a\u043b\u043e\u043d\u0438\u0442\u044c`, callback_data: `reject_group:${group.chatId}` }
            ]);
          }
          
          if (pendingGroups.length > 5) {
            msg += `\n_... \u0438 \u0435\u0449\u0451 ${pendingGroups.length - 5} \u0437\u0430\u044f\u0432\u043e\u043a_`;
          }
          
          buttons.push([{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]);
          
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
          
          const msg = `\u{1f381} *\u0412\u044b\u0434\u0430\u0447\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438*\n` +
            `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n` +
            `*\u0423\u0440\u043e\u0432\u043d\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a:*\n` +
            `\u2022 \`trial\` \u2014 \u041f\u0440\u043e\u0431\u043d\u044b\u0439 (1 \u0434\u0435\u043d\u044c)\n` +
            `\u2022 \`level1\` \u2014 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 1 (\u0442\u043e\u043b\u044c\u043a\u043e \u041b\u0421)\n` +
            `\u2022 \`level2\` \u2014 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 2 (\u041b\u0421 + \u0447\u0430\u0442)\n` +
            `\u2022 \`level3\` \u2014 \u0423\u0440\u043e\u0432\u0435\u043d\u044c 3 (\u043f\u043e\u043b\u043d\u044b\u0439)\n\n` +
            `*\u041a\u043e\u043c\u0430\u043d\u0434\u044b:*\n` +
            `\`/grant ID \u0434\u043d\u0435\u0439 \u0443\u0440\u043e\u0432\u0435\u043d\u044c\`\n\n` +
            `*\u041f\u0440\u0438\u043c\u0435\u0440\u044b:*\n` +
            `\`/grant 123456789 30 level1\`\n` +
            `\`/grant 123456789 30 level2\`\n` +
            `\`/grant 123456789 30 level3\``;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_search': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const msg = `\u{1f50d} *\u041f\u043e\u0438\u0441\u043a \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f*\n` +
            `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n` +
            `\u041e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u043a\u043e\u043c\u0430\u043d\u0434\u0443:\n` +
            `\`/check 123456789\`\n\n` +
            `\u0418\u043b\u0438 \u043f\u0435\u0440\u0435\u0448\u043b\u0438\u0442\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f.`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
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
          
          let msg = `\u{1f465} *\u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0435 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438*\n` +
            `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n`;
          
          if (users.length === 0) {
            msg += `_\u041d\u0435\u0442 \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a_`;
          } else {
            for (const user of users) {
              const daysLeft = Math.ceil((user.expiresAt - now) / (24 * 60 * 60 * 1000));
              const status = user.isActive ? '\u2705' : '\u274c';
              const typeInfo = LICENSE_TYPES[user.type] || { name: user.type };
              msg += `${status} \`${user.telegramId}\`\n`;
              msg += `   ${typeInfo.name} \u2022 ${daysLeft > 0 ? daysLeft + '\u0434' : '\u0438\u0441\u0442\u0435\u043a\u043b\u0430'}\n`;
            }
          }
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'adm_users' }],
              [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_chats': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u043d\u044b\u0435 \u0447\u0430\u0442\u044b
          const groupOwnersList = await env.USERS.list({ prefix: 'group_owner:', limit: 20 });
          
          let msg = `\u{1f4ac} *\u041e\u0434\u043e\u0431\u0440\u0435\u043d\u043d\u044b\u0435 \u0447\u0430\u0442\u044b*\n` +
            `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n`;
          
          if (groupOwnersList.keys.length === 0) {
            msg += `_\u041d\u0435\u0442 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u043d\u044b\u0445 \u0447\u0430\u0442\u043e\u0432_`;
          } else {
            for (const key of groupOwnersList.keys) {
              const chatId2 = key.name.replace('group_owner:', '');
              const ownerData = await env.USERS.get(key.name);
              if (ownerData) {
                const owner = JSON.parse(ownerData);
                msg += `\u{1f4cc} \`${chatId2}\`\n`;
                msg += `   \u0412\u043b\u0430\u0434\u0435\u043b\u0435\u0446: \`${owner.telegramId || 'unknown'}\`\n`;
              }
            }
          }
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'adm_chats' }],
              [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
            ]}
          });
          break;
        }
        
        case 'adm_broadcast': {
          if (!await checkAndExtendAdminSession(env, telegramId)) {
            await showSessionExpiredMessage(env, chatId, messageId);
            break;
          }
          
          const msg = `\u{1f4e2} *\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430*\n` +
            `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n` +
            `\u041e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u043a\u043e\u043c\u0430\u043d\u0434\u0443:\n` +
            `\`/broadcast \u0422\u0435\u043a\u0441\u0442 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f\`\n\n` +
            `\u26a0\ufe0f _\u0411\u0443\u0434\u0435\u0442 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e \u0432\u0441\u0435\u043c \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f\u043c_`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
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
          
          const msg = `\u{1f4c8} *\u0414\u0435\u0442\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430*\n` +
            `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n` +
            `\u{1f465} *\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438:*\n` +
            `\u251c \u0412\u0441\u0435\u0433\u043e: ${stats.totalUsers}\n` +
            `\u251c \u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0445: ${stats.activeUsers}\n` +
            `\u251c \u{1f381} Trial: ${stats.trialUsers}\n` +
            `\u251c \u2b50 Level 1: ${stats.level1Users}\n` +
            `\u251c \u2b50\u2b50 Level 2: ${stats.level2Users}\n` +
            `\u2514 \u2b50\u2b50\u2b50 Level 3: ${stats.level3Users}\n\n` +
            `\u{1f4ac} *\u0427\u0430\u0442\u044b:*\n` +
            `\u251c \u041e\u0436\u0438\u0434\u0430\u044e\u0442: ${stats.pendingGroups}\n` +
            `\u2514 \u041e\u0434\u043e\u0431\u0440\u0435\u043d\u043e: ${stats.approvedGroups}\n\n` +
            `\u{1f527} *\u0421\u0438\u0441\u0442\u0435\u043c\u0430:*\n` +
            `\u251c Uptime: ${uptimeHours}\u0447\n` +
            `\u251c \u0417\u0430\u043f\u0440\u043e\u0441\u043e\u0432: ${metrics.requests.total}\n` +
            `\u2514 \u041e\u0448\u0438\u0431\u043e\u043a: ${metrics.requests.error}`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'adm_stats' }],
              [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
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
          await answerCallbackQuery(env, callback.id, '\u2705 \u041e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u043e');
          break;
        }
        
        case 'adm_logout': {
          await destroyAdminSession(env, telegramId);
          await editTelegramMessage(env, chatId, messageId,
            `\u{1f6aa} *\u0421\u0435\u0441\u0441\u0438\u044f \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430*\n\n` +
            `\u0412\u044b \u0432\u044b\u0448\u043b\u0438 \u0438\u0437 \u0430\u0434\u043c\u0438\u043d-\u043f\u0430\u043d\u0435\u043b\u0438.`,
            { reply_markup: { inline_keyboard: [[{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]] }}
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
          
          const msg = `\u{1f527} *\u0421\u0438\u0441\u0442\u0435\u043c\u0430*\n` +
            `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n` +
            `\u{1f4e6} *\u0412\u0435\u0440\u0441\u0438\u0438:*\n` +
            `\u251c \u0411\u043e\u0442: ${BOT_VERSION}\n` +
            `\u2514 \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435: ${EXTENSION_VERSION}\n\n` +
            `\u23f1 *Uptime:* ${uptimeHours}\u0447 ${uptimeMinutes}\u043c\n\n` +
            `\u2699\ufe0f *\u041a\u043e\u043d\u0444\u0438\u0433\u0443\u0440\u0430\u0446\u0438\u044f:*\n` +
            `\u251c LOG\\_CHAT: ${env.LOG_CHAT_ID ? '\u2705' : '\u274c'}\n` +
            `\u251c ADMIN\\_TG\\_ID: ${env.ADMIN_TELEGRAM_ID ? '\u2705' : '\u274c'}\n` +
            `\u2514 WEBHOOK\\_SECRET: ${env.TELEGRAM_WEBHOOK_SECRET ? '\u2705' : '\u274c'}\n\n` +
            `\u{1f4c8} *API:*\n` +
            `\u251c \u0417\u0430\u043f\u0440\u043e\u0441\u043e\u0432: ${metrics.requests.total}\n` +
            `\u2514 \u041e\u0448\u0438\u0431\u043e\u043a: ${metrics.requests.error}`;
          
          await editTelegramMessage(env, chatId, messageId, msg, {
            reply_markup: { inline_keyboard: [
              [{ text: '\u{1f4cb} \u041b\u043e\u0433\u0438 \u043e\u0448\u0438\u0431\u043e\u043a', callback_data: 'cp_logs' }],
              [{ text: '\u{1f517} Webhook Info', callback_data: 'cp_webhook' }],
              [{ text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'adm_system' }],
              [{ text: '\u2190 \u041d\u0430\u0437\u0430\u0434', callback_data: 'adm_back' }]
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
        
        // === \u0421\u0422\u0410\u0420\u042b\u0415 \u0410\u0414\u041c\u0418\u041d\u0421\u041a\u0418\u0415 CALLBACK (\u0434\u043b\u044f \u0441\u043e\u0432\u043c\u0435\u0441\u0442\u0438\u043c\u043e\u0441\u0442\u0438) ===
        case 'admin_back': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, '\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430', true);
            break;
          }
          
          const adminMsg = `\u{1f451} *\u0410\u0434\u043c\u0438\u043d-\u043f\u0430\u043d\u0435\u043b\u044c*

/grant ID 7 \u2014 \u0432\u044b\u0434\u0430\u0442\u044c
/revoke ID \u2014 \u043e\u0442\u043e\u0437\u0432\u0430\u0442\u044c
/check ID \u2014 \u043f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c`;
          
          await editTelegramMessage(env, chatId, messageId, adminMsg, {
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '\u{1f50d} \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c', callback_data: 'admin_check_prompt' },
                  { text: '\u{1f381} \u0412\u044b\u0434\u0430\u0442\u044c', callback_data: 'admin_grant_prompt' }
                ],
                [{ text: '\u{1f4c8} \u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430', callback_data: 'admin_stats' }],
                [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
              ]
            }
          });
          break;
        }
        
        case 'admin_check_prompt': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, '\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430', true);
            break;
          }
          
          await editTelegramMessage(env, chatId, messageId, 
            `\u{1f50d} *\u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f*\n\n` +
            `\u041e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u043a\u043e\u043c\u0430\u043d\u0434\u0443:\n` +
            `\`/check 123456789\` \u2014 \u043f\u043e ID`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u2190 \u0410\u0434\u043c\u0438\u043d', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        case 'admin_grant_prompt': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, '\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430', true);
            break;
          }
          
          await editTelegramMessage(env, chatId, messageId, 
            `\u{1f381} *\u0412\u044b\u0434\u0430\u0447\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438*\n\n` +
            `\`/grant ID 7\` \u2014 7 \u0434\u043d\u0435\u0439\n` +
            `\`/grant ID 30\` \u2014 30 \u0434\u043d\u0435\u0439\n` +
            `\`/grant 123456 90 level2\` \u2014 \u043f\u043e ID`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u2190 \u0410\u0434\u043c\u0438\u043d', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        case 'admin_users': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, '\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430', true);
            break;
          }
          
          await editTelegramMessage(env, chatId, messageId, 
            `\u{1f4ca} *\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438*\n\n` +
            `\`/check 123456789\` \u2014 \u043f\u043e ID`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u2190 \u0410\u0434\u043c\u0438\u043d', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        case 'admin_stats': {
          const callbackTelegramId = String(callback.from.id);
          
          if (!await isAdmin(callbackTelegramId)) {
            await answerCallbackQuery(env, callback.id, '\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430', true);
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
            `\u{1f4c8} *\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u0431\u043e\u0442\u0430*\n\n` +
            `\u{1f916} @${botUsername.replace(/_/g, '\\_')}\n` +
            `\u{1f4e6} \u0412\u0435\u0440\u0441\u0438\u044f: ${BOT_VERSION}\n` +
            `\u{1f550} ${now} MSK\n` +
            `\u2705 KV: \u0420\u0430\u0431\u043e\u0442\u0430\u0435\u0442`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'admin_stats' }],
                [{ text: '\u2190 \u0410\u0434\u043c\u0438\u043d', callback_data: 'admin_back' }]
              ]
            }}
          );
          break;
        }
        
        default: {
          // \u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430 remove_device:deviceId
          if (data.startsWith('remove_device:')) {
            const deviceIdPart = data.split(':')[1];
            
            // \u041d\u0430\u0445\u043e\u0434\u0438\u043c \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u043f\u043e \u0447\u0430\u0441\u0442\u0438\u0447\u043d\u043e\u043c\u0443 ID
            const devicesData = await getUserDevices(env, telegramId);
            const devices = devicesData.devices || [];
            const device = devices.find(d => d.deviceId.startsWith(deviceIdPart));
            
            if (device) {
              const result = await removeDevice(env, telegramId, device.deviceId);
              if (result.success) {
                await answerCallbackQuery(env, callback.id, '\u2705 \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u043e\u0442\u0432\u044f\u0437\u0430\u043d\u043e', false);
              } else {
                await answerCallbackQuery(env, callback.id, '\u274c \u041e\u0448\u0438\u0431\u043a\u0430', true);
              }
            } else {
              await answerCallbackQuery(env, callback.id, '\u274c \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e', true);
            }
            
            // \u041e\u0431\u043d\u043e\u0432\u043b\u044f\u0435\u043c \u0441\u043f\u0438\u0441\u043e\u043a \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432
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
            
            let message = `\u{1f4f1} *\u041c\u043e\u0438 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430* (${newDevices.length}/${maxDevices})\n\n`;
            
            if (newDevices.length === 0) {
              message += `_\u041d\u0435\u0442 \u043f\u0440\u0438\u0432\u044f\u0437\u0430\u043d\u043d\u044b\u0445 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432_`;
            } else {
              newDevices.forEach((d, index) => {
                const shortId = d.deviceId.substring(0, 12) + '...';
                const addedDate = new Date(d.addedAt).toLocaleDateString('ru-RU');
                message += `${index + 1}. \`${shortId}\`\n`;
                message += `   \u{1f4c5} \u0414\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u043e: ${addedDate}\n\n`;
              });
            }
            
            const keyboard = { inline_keyboard: [] };
            newDevices.forEach((d, index) => {
              keyboard.inline_keyboard.push([
                { text: `\u{1f5d1} \u041e\u0442\u0432\u044f\u0437\u0430\u0442\u044c \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e ${index + 1}`, callback_data: `remove_device:${d.deviceId.substring(0, 32)}` }
              ]);
            });
            keyboard.inline_keyboard.push([{ text: '\u2190 \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' }]);
            
            await editTelegramMessage(env, chatId, messageId, message, { reply_markup: keyboard });
            break;
          }
          
          // \u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430 approve_group:chatId \u0438 reject_group:chatId
          if (data.startsWith('approve_group:') || data.startsWith('reject_group:')) {
            // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0441\u0435\u0441\u0441\u0438\u044e \u0430\u0434\u043c\u0438\u043d\u0430
            if (!await checkAndExtendAdminSession(env, telegramId)) {
              await showSessionExpiredMessage(env, chatId, messageId);
              break;
            }
            
            const groupChatId = data.split(':')[1];
            const isApprove = data.startsWith('approve_group:');
            
            if (isApprove) {
              const result = await approveGroupChat(env, groupChatId, username);
              
              if (result.success) {
                // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
                if (result.telegramId) {
                  await sendTelegramMessage(env, result.telegramId,
                    `\u2705 *\u0413\u0440\u0443\u043f\u043f\u0430 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0430!*\n\n` +
                    `\u0412\u0430\u0448 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 \u0431\u044b\u043b \u043e\u0434\u043e\u0431\u0440\u0435\u043d.\n` +
                    `\u0422\u0435\u043f\u0435\u0440\u044c \u0432\u044b \u0431\u0443\u0434\u0435\u0442\u0435 \u043f\u043e\u043b\u0443\u0447\u0430\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445 \u0432 \u0433\u0440\u0443\u043f\u043f\u0443.`
                  );
                }
                
                // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u0432 \u0433\u0440\u0443\u043f\u043f\u0443
                await sendTelegramMessage(env, groupChatId,
                  `\u2705 *\u0413\u0440\u0443\u043f\u043f\u0430 \u0430\u043a\u0442\u0438\u0432\u0438\u0440\u043e\u0432\u0430\u043d\u0430!*\n\n` +
                  `\u0422\u0435\u043f\u0435\u0440\u044c \u0441\u044e\u0434\u0430 \u0431\u0443\u0434\u0443\u0442 \u043f\u0440\u0438\u0445\u043e\u0434\u0438\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445.`
                );
                
                await editTelegramMessage(env, chatId, messageId,
                  `\u2705 *\u0413\u0440\u0443\u043f\u043f\u0430 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0430*\n\n` +
                  `Chat ID: \`${groupChatId}\`\n` +
                  `\u041e\u0434\u043e\u0431\u0440\u0438\u043b: @${username}`,
                  { reply_markup: { inline_keyboard: [[{ text: '\u2190 \u0410\u0434\u043c\u0438\u043d-\u043f\u0430\u043d\u0435\u043b\u044c', callback_data: 'adm_back' }]] }}
                );
              } else {
                await sendTelegramMessage(env, chatId, 
                  `\u274c \u041e\u0448\u0438\u0431\u043a\u0430: ${result.error}`
                );
              }
            } else {
              // \u041e\u0442\u043a\u043b\u043e\u043d\u0435\u043d\u0438\u0435
              const pendingKey = `pending_group:${groupChatId}`;
              const pendingData = await env.USERS.get(pendingKey);
              
              if (pendingData) {
                const request = JSON.parse(pendingData);
                await env.USERS.delete(pendingKey);
                
                // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
                if (request.requestedBy) {
                  await sendTelegramMessage(env, request.requestedBy,
                    `\u274c *\u0417\u0430\u043f\u0440\u043e\u0441 \u043e\u0442\u043a\u043b\u043e\u043d\u0451\u043d*\n\n` +
                    `\u0412\u0430\u0448 \u0437\u0430\u043f\u0440\u043e\u0441 \u043d\u0430 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 \u0431\u044b\u043b \u043e\u0442\u043a\u043b\u043e\u043d\u0451\u043d.\n` +
                    `\u0414\u043b\u044f \u0443\u0442\u043e\u0447\u043d\u0435\u043d\u0438\u044f \u043d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 @YaMob`
                  );
                }
                
                // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u0432 \u0433\u0440\u0443\u043f\u043f\u0443
                await sendTelegramMessage(env, groupChatId,
                  `\u274c *\u0417\u0430\u043f\u0440\u043e\u0441 \u043e\u0442\u043a\u043b\u043e\u043d\u0451\u043d*\n\n` +
                  `\u0410\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u043e\u0442\u043a\u043b\u043e\u043d\u0438\u043b \u0437\u0430\u043f\u0440\u043e\u0441 \u043d\u0430 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f.`
                );
                
                await editTelegramMessage(env, chatId, messageId,
                  `\u274c *\u0413\u0440\u0443\u043f\u043f\u0430 \u043e\u0442\u043a\u043b\u043e\u043d\u0435\u043d\u0430*\n\n` +
                  `Chat ID: \`${groupChatId}\`\n` +
                  `\u041e\u0442\u043a\u043b\u043e\u043d\u0438\u043b: @${username}`,
                  { reply_markup: { inline_keyboard: [[{ text: '\u2190 \u0410\u0434\u043c\u0438\u043d-\u043f\u0430\u043d\u0435\u043b\u044c', callback_data: 'adm_back' }]] }}
                );
              } else {
                await sendTelegramMessage(env, chatId, '\u0417\u0430\u043f\u0440\u043e\u0441 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d');
              }
            }
            break;
          }
          
          // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0430\u0434\u043c\u0438\u043d\u0441\u043a\u0438\u0435 callback \u0441 \u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440\u0430\u043c\u0438
          if (data.startsWith('grant_') || data.startsWith('revoke_')) {
            const callbackTelegramId = String(callback.from.id);
            
            if (!await isAdmin(callbackTelegramId)) {
              await answerCallbackQuery(env, callback.id, '\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430', true);
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
                `\u2705 *\u0412\u044b\u0434\u0430\u043d\u043e*\n\n` +
                `\u{1f464} ${target}\n` +
                `\u{1f4c5} +${days} \u0434\u043d. \u2192 ${daysLeft} \u0434\u043d.\n` +
                `\u{1f4c6} \u0414\u043e: ${expiresDate}`
              );
              
              // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
              await logToChat(env,
                `\u{1f48e} *\u0412\u044b\u0434\u0430\u043d\u043e*`,
                'license',
                {
                  telegramId: isTelegramId ? target : '\u043d\u0435\u0442',
                  username: isTelegramId ? '\u043d\u0435\u0442' : target,
                  action: `+${days} \u0434\u043d.`,
                  details: `@${username}`
                }
              );
              
              // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
              if (isTelegramId) {
                try {
                  await sendTelegramMessage(env, target, 
                    `\u{1f389} *\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 +${days} \u0434\u043d.*\n` +
                    `\u{1f4c5} \u0414\u043e: ${expiresDate}`
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
                `\u2705 *\u041e\u0442\u043e\u0437\u0432\u0430\u043d\u043e*\n\n\u{1f464} ${isTelegramId ? target : '@' + target}`
              );
              
              // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
              await logToChat(env,
                `\u274c *\u041e\u0442\u043e\u0437\u0432\u0430\u043d\u043e*`,
                'admin',
                {
                  telegramId: isTelegramId ? target : '\u043d\u0435\u0442',
                  username: isTelegramId ? '\u043d\u0435\u0442' : target,
                  action: '\u041e\u0442\u0437\u044b\u0432',
                  details: `@${username}`
                }
              );
            }
          } else {
            await answerCallbackQuery(env, callback.id, '\u041d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u0430\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u0430', true);
          }
        }
      }
      
      return jsonResponse({ ok: true });
    }
    
    // === \u041e\u0411\u0420\u0410\u0411\u041e\u0422\u041a\u0410 \u0422\u0415\u041a\u0421\u0422\u041e\u0412\u042b\u0425 \u041a\u041e\u041c\u0410\u041d\u0414 ===
    // isAdmin \u0443\u0436\u0435 \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0451\u043d \u0432\u044b\u0448\u0435 (\u043f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u0442 \u043f\u043e telegramId)
    
    // Rate limiting \u0434\u043b\u044f \u043e\u0431\u044b\u0447\u043d\u044b\u0445 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439
    if (update.message) {
      const msgTelegramId = String(update.message.from.id);
      
      // \u0410\u0434\u043c\u0438\u043d\u044b \u043d\u0435 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u044b
      if (!await isAdmin(msgTelegramId)) {
        const rateCheck = checkBotRateLimit(msgTelegramId, 'messages');
        if (!rateCheck.allowed) {
          // \u041d\u0435 \u043e\u0442\u0432\u0435\u0447\u0430\u0435\u043c \u043d\u0430 \u0441\u043f\u0430\u043c, \u043f\u0440\u043e\u0441\u0442\u043e \u0438\u0433\u043d\u043e\u0440\u0438\u0440\u0443\u0435\u043c
          return jsonResponse({ ok: true });
        }
      }
    }
    
    // ========== \u0421\u0415\u041a\u0420\u0415\u0422\u041d\u0410\u042f \u0410\u0414\u041c\u0418\u041d-\u041f\u0410\u041d\u0415\u041b\u042c ==========
    // \u0412\u0445\u043e\u0434 \u043f\u043e \u0441\u0435\u043a\u0440\u0435\u0442\u043d\u043e\u0439 \u0444\u0440\u0430\u0437\u0435 (\u0441\u043a\u0440\u044b\u0442\u0430\u044f \u043e\u0442 \u043e\u0431\u044b\u0447\u043d\u044b\u0445 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439)
    // \u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442: env.ADMIN_SECRET_PHRASE, \u0438\u043d\u0430\u0447\u0435 fallback \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435
    const adminSecretPhrase = env.ADMIN_SECRET_PHRASE || 'flamingo1000';
    if (update.message?.text === adminSecretPhrase) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043f\u043e Telegram ID (\u0431\u043e\u043b\u0435\u0435 \u0431\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u043e \u0447\u0435\u043c \u043f\u043e username)
      const isAdminUser = await isAdminById(env, telegramId);
      
      if (!isAdminUser) {
        // \u041d\u0435 \u0434\u0430\u0451\u043c \u0437\u043d\u0430\u0442\u044c \u0447\u0442\u043e \u043a\u043e\u043c\u0430\u043d\u0434\u0430 \u0441\u0443\u0449\u0435\u0441\u0442\u0432\u0443\u0435\u0442 - \u043f\u0440\u043e\u0441\u0442\u043e \u0438\u0433\u043d\u043e\u0440\u0438\u0440\u0443\u0435\u043c
        // \u041a\u0430\u043a \u0431\u0443\u0434\u0442\u043e \u044d\u0442\u043e \u043e\u0431\u044b\u0447\u043d\u044b\u0439 \u0442\u0435\u043a\u0441\u0442
        await sendTelegramMessage(env, chatId, 
          `\u{1f914} \u041d\u0435 \u043f\u043e\u043d\u0438\u043c\u0430\u044e.\n\n\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0437 \u043c\u0435\u043d\u044e:`,
          { reply_markup: getMainMenuKeyboard() }
        );
        return jsonResponse({ ok: true });
      }
      
      // \u0421\u043e\u0437\u0434\u0430\u0451\u043c \u0430\u0434\u043c\u0438\u043d-\u0441\u0435\u0441\u0441\u0438\u044e
      await createAdminSession(env, telegramId);
      
      // \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0441 \u0441\u0435\u043a\u0440\u0435\u0442\u043d\u043e\u0439 \u0444\u0440\u0430\u0437\u043e\u0439 (\u0434\u043b\u044f \u0431\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u043e\u0441\u0442\u0438)
      try {
        await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, message_id: update.message.message_id })
        });
      } catch (e) {}
      
      // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0443 \u0438 pending \u0433\u0440\u0443\u043f\u043f\u044b
      const stats = await getAdminStats(env);
      const pendingGroups = await getAllPendingGroups(env);
      
      const adminMsg = `\u{1f510} *CONTROL PANEL v${BOT_VERSION}*\n\n` +
        `\u{1f4ca} *\u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0435 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438:* ${stats.activeUsers}\n` +
        `\u251c \u{1f381} Trial: ${stats.trialUsers}\n` +
        `\u251c \u2b50 Level 1: ${stats.level1Users}\n` +
        `\u251c \u2b50\u2b50 Level 2: ${stats.level2Users}\n` +
        `\u2514 \u2b50\u2b50\u2b50 Level 3: ${stats.level3Users}\n\n` +
        `\u{1f4ac} \u0413\u0440\u0443\u043f\u043f: ${stats.approvedGroups} \u043e\u0434\u043e\u0431\u0440. / ${stats.pendingGroups} \u043e\u0436\u0438\u0434.\n\n` +
        `_\u0421\u0435\u0441\u0441\u0438\u044f \u0430\u043a\u0442\u0438\u0432\u043d\u0430 1 \u0447\u0430\u0441_`;
      
      const keyboard = {
        inline_keyboard: [
          [
            { text: '\u23f3 \u041e\u0436\u0438\u0434\u0430\u044e\u0449\u0438\u0435 \u0433\u0440\u0443\u043f\u043f\u044b (' + pendingGroups.length + ')', callback_data: 'cp_pending_groups' }
          ],
          [
            { text: '\u{1f465} \u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438', callback_data: 'cp_users' },
            { text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430', callback_data: 'cp_stats' }
          ],
          [
            { text: '\u{1f381} \u0412\u044b\u0434\u0430\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'cp_grant' },
            { text: '\u{1f50d} \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c', callback_data: 'cp_check' }
          ],
          [
            { text: '\u{1f4e2} \u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430', callback_data: 'cp_broadcast' },
            { text: '\u{1f527} \u0421\u0438\u0441\u0442\u0435\u043c\u0430', callback_data: 'cp_system' }
          ],
          [
            { text: '\u{1f6aa} \u0412\u044b\u0445\u043e\u0434', callback_data: 'cp_logout' }
          ]
        ]
      };
      
      await sendTelegramMessage(env, chatId, adminMsg, { reply_markup: keyboard });
      return jsonResponse({ ok: true });
    }
    
    // \u0421\u0442\u0430\u0440\u0430\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u0430 /admin \u0434\u043b\u044f \u043e\u0431\u0440\u0430\u0442\u043d\u043e\u0439 \u0441\u043e\u0432\u043c\u0435\u0441\u0442\u0438\u043c\u043e\u0441\u0442\u0438
    if (update.message?.text === '/admin') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        // \u041d\u0435 \u043f\u043e\u043a\u0430\u0437\u044b\u0432\u0430\u0435\u043c \u0447\u0442\u043e \u043a\u043e\u043c\u0430\u043d\u0434\u0430 \u0441\u0443\u0449\u0435\u0441\u0442\u0432\u0443\u0435\u0442
        await sendTelegramMessage(env, chatId, 
          `\u{1f914} \u041d\u0435 \u043f\u043e\u043d\u0438\u043c\u0430\u044e.\n\n\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0437 \u043c\u0435\u043d\u044e:`,
          { reply_markup: getMainMenuKeyboard() }
        );
        return jsonResponse({ ok: true });
      }
      
      // \u041f\u043e\u043a\u0430\u0437\u044b\u0432\u0430\u0435\u043c \u043f\u043e\u0434\u0441\u043a\u0430\u0437\u043a\u0443 \u043e \u043d\u043e\u0432\u043e\u0439 \u0430\u0434\u043c\u0438\u043d\u043a\u0435
      await sendTelegramMessage(env, chatId, 
        `\u2139\ufe0f *\u0410\u0434\u043c\u0438\u043d-\u043f\u0430\u043d\u0435\u043b\u044c \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0430*\n\n` +
        `\u0414\u043b\u044f \u0434\u043e\u0441\u0442\u0443\u043f\u0430 \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\u0442\u0435 \u0441\u0435\u043a\u0440\u0435\u0442\u043d\u0443\u044e \u0444\u0440\u0430\u0437\u0443.\n` +
        `_\u0421\u0442\u0430\u0440\u044b\u0435 \u043a\u043e\u043c\u0430\u043d\u0434\u044b /grant, /check, /revoke \u0432\u0441\u0451 \u0435\u0449\u0451 \u0440\u0430\u0431\u043e\u0442\u0430\u044e\u0442._`
      );
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /grant - \u0432\u044b\u0434\u0430\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443
    if (update.message?.text?.startsWith('/grant')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 3) {
        await sendTelegramMessage(env, chatId, 
          `\u274c *\u0424\u043e\u0440\u043c\u0430\u0442:* \`/grant @user \u0434\u043d\u0435\u0439 [\u0443\u0440\u043e\u0432\u0435\u043d\u044c]\`\n\n` +
          `*\u0423\u0440\u043e\u0432\u043d\u0438:* trial, level1, level2, level3\n` +
          `\u041f\u043e \u0443\u043c\u043e\u043b\u0447\u0430\u043d\u0438\u044e: level1\n\n` +
          `*\u041f\u0440\u0438\u043c\u0435\u0440\u044b:*\n` +
          `\`/grant @username 30\` \u2014 level1\n` +
          `\`/grant @username 30 level2\`\n` +
          `\`/grant 123456789 7 level3\``
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      const days = parseInt(parts[2]);
      const licenseType = parts[3] || 'level1';
      
      if (isNaN(days) || days <= 0) {
        await sendTelegramMessage(env, chatId, '\u274c \u0423\u043a\u0430\u0436\u0438\u0442\u0435 \u0434\u043d\u0438 (\u0447\u0438\u0441\u043b\u043e > 0)');
        return jsonResponse({ ok: true });
      }
      
      // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0432\u0430\u043b\u0438\u0434\u043d\u043e\u0441\u0442\u044c \u0442\u0438\u043f\u0430 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438
      const validTypes = ['trial', 'level1', 'level2', 'level3'];
      if (!validTypes.includes(licenseType)) {
        await sendTelegramMessage(env, chatId, 
          `\u274c \u041d\u0435\u0432\u0435\u0440\u043d\u044b\u0439 \u0442\u0438\u043f: \`${licenseType}\`\n\n` +
          `\u0414\u043e\u043f\u0443\u0441\u0442\u0438\u043c\u044b\u0435: ${validTypes.join(', ')}`
        );
        return jsonResponse({ ok: true });
      }
      
      // \u0418\u0449\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f (\u043f\u043e ID \u0438\u043b\u0438 username)
      const userInfo = await getUserInfo(env, target);
      
      // \u041e\u043f\u0440\u0435\u0434\u0435\u043b\u044f\u0435\u043c telegramId \u0434\u043b\u044f \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u0438\u044f
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
          // \u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d \u043f\u043e username - \u0432\u044b\u0434\u0430\u0451\u043c \u0442\u043e\u043b\u044c\u043a\u043e \u043f\u043e username \u043a\u043b\u044e\u0447\u0443
          // (\u0431\u0443\u0434\u0435\u0442 \u043f\u0440\u0438\u043c\u0435\u043d\u0435\u043d\u043e \u043a\u043e\u0433\u0434\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u0437\u0430\u0440\u0435\u0433\u0438\u0441\u0442\u0440\u0438\u0440\u0443\u0435\u0442\u0441\u044f)
          await sendTelegramMessage(env, chatId, 
            `\u26a0\ufe0f *\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c @${target} \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d*\n\n` +
            `\u041b\u0438\u0446\u0435\u043d\u0437\u0438\u044f \u0431\u0443\u0434\u0435\u0442 \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u0430 \u043f\u043e username.\n` +
            `\u041a\u043e\u0433\u0434\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u0441\u044f \u2014 \u043e\u043d\u0430 \u0430\u043a\u0442\u0438\u0432\u0438\u0440\u0443\u0435\u0442\u0441\u044f.`
          );
        }
      }
      
      // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0442\u0435\u043a\u0443\u0449\u0443\u044e \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e
      let currentLicense = null;
      if (targetTelegramId) {
        const existing = await env.USERS.get(`license:tg:${targetTelegramId}`);
        if (existing) currentLicense = JSON.parse(existing);
      }
      if (!currentLicense && targetUsername) {
        const existing = await env.USERS.get(`license:${targetUsername.toLowerCase()}`);
        if (existing) currentLicense = JSON.parse(existing);
      }
      
      // \u0412\u044b\u0447\u0438\u0441\u043b\u044f\u0435\u043c \u043d\u043e\u0432\u0443\u044e \u0434\u0430\u0442\u0443
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
      
      // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e
      if (targetTelegramId) {
        await env.USERS.put(`license:tg:${targetTelegramId}`, JSON.stringify(licenseData));
        
        // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043c\u0430\u043f\u043f\u0438\u043d\u0433 username \u2192 telegramId
        if (targetUsername) {
          await saveUsernameMapping(env, targetUsername, targetTelegramId);
        }
        
        // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u0431\u043e\u043d\u0443\u0441 - \u0435\u0441\u043b\u0438 \u044d\u0442\u043e \u043f\u0435\u0440\u0432\u0430\u044f \u043f\u043b\u0430\u0442\u043d\u0430\u044f \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430
        // \u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u043f\u0440\u0438\u0448\u0451\u043b \u043f\u043e \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u043e\u0439 \u0441\u0441\u044b\u043b\u043a\u0435
        if (licenseType !== 'trial' && (!currentLicense || currentLicense.type === 'trial')) {
          const bonusResult = await grantReferralBonusToReferrer(env, targetTelegramId);
          if (bonusResult.success) {
            // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u0440\u0435\u0444\u0435\u0440\u0435\u0440\u0430 \u043e \u043f\u043e\u043b\u0443\u0447\u0435\u043d\u043d\u043e\u043c \u0431\u043e\u043d\u0443\u0441\u0435
            try {
              await sendTelegramMessage(env, bonusResult.referrerTelegramId, 
                `\u{1f389} *\u0411\u043e\u043d\u0443\u0441 \u043f\u043e\u043b\u0443\u0447\u0435\u043d!*\n\n` +
                `\u0412\u0430\u0448 \u0440\u0435\u0444\u0435\u0440\u0430\u043b \u043e\u0444\u043e\u0440\u043c\u0438\u043b \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443!\n` +
                `\u{1f381} \u0412\u0430\u043c \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d +1 \u0434\u0435\u043d\u044c \u043a \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0435.`
              );
            } catch (e) {}
            
            // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
            await logToChat(env,
              `\u{1f39f}\ufe0f *\u0420\u0435\u0444. \u0431\u043e\u043d\u0443\u0441*\n\n` +
              `\u0420\u0435\u0444\u0435\u0440\u0435\u0440: ${bonusResult.referrerTelegramId}\n` +
              `\u0420\u0435\u0444\u0435\u0440\u0430\u043b: ${targetTelegramId}`,
              'success',
              { action: '\u0411\u043e\u043d\u0443\u0441 \u0437\u0430 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u0430' }
            );
          }
        }
      } else if (targetUsername) {
        // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043f\u043e username \u0434\u043b\u044f \u043f\u043e\u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0439 \u0430\u043a\u0442\u0438\u0432\u0430\u0446\u0438\u0438
        await env.USERS.put(`license:${targetUsername.toLowerCase()}`, JSON.stringify(licenseData));
      }
      
      const daysLeft = Math.ceil((expiresAt - Date.now()) / (24 * 60 * 60 * 1000));
      const expiresDate = new Date(expiresAt).toLocaleDateString('ru-RU');
      const typeName = LICENSE_TYPES[licenseType]?.name || licenseType;
      
      const displayUser = targetUsername ? `@${targetUsername}` : targetTelegramId;
      
      await sendTelegramMessage(env, chatId, 
        `\u2705 *\u0412\u044b\u0434\u0430\u043d\u043e*\n\n` +
        `\u{1f464} ${displayUser}\n` +
        `\u{1f194} ID: ${targetTelegramId || '\u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0438\u0442\u0441\u044f \u043f\u043e\u0437\u0436\u0435'}\n` +
        `\u{1f4e6} ${typeName}\n` +
        `\u{1f4c5} +${days} \u0434\u043d. \u2192 ${daysLeft} \u0434\u043d.\n` +
        `\u{1f4c6} \u0414\u043e: ${expiresDate}`
      );
      
      // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
      await logToChat(env,
        `\u{1f48e} *\u0412\u044b\u0434\u0430\u043d\u043e*`,
        'license',
        {
          telegramId: targetTelegramId || '\u043d\u0435\u0442',
          username: targetUsername || '\u043d\u0435\u0442',
          action: `+${days} \u0434\u043d. (${typeName})`,
          details: `@${username}`
        }
      );
      
      // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
      if (targetTelegramId) {
        try {
          await sendTelegramMessage(env, targetTelegramId, 
            `\u{1f389} *\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 +${days} \u0434\u043d.*\n` +
            `\u{1f4c5} \u0414\u043e: ${expiresDate}`
          );
        } catch (e) {}
      }
      
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /revoke - \u043e\u0442\u043e\u0437\u0432\u0430\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443
    if (update.message?.text?.startsWith('/revoke')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          `\u274c \u0424\u043e\u0440\u043c\u0430\u0442: \`/revoke @user\` \u0438\u043b\u0438 \`/revoke ID\``
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      
      // \u0418\u0449\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f (\u043f\u043e ID \u0438\u043b\u0438 username)
      const userInfo = await getUserInfo(env, target);
      const isTelegramId = /^\d+$/.test(target);
      
      // \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e
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
        `\u2705 *\u041e\u0442\u043e\u0437\u0432\u0430\u043d\u043e*\n\n\u{1f464} ${displayUser}`
      );
      
      // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
      await logToChat(env,
        `\u274c *\u041e\u0442\u043e\u0437\u0432\u0430\u043d\u043e*`,
        'admin',
        {
          telegramId: userInfo.telegramId || (isTelegramId ? target : '\u043d\u0435\u0442'),
          username: userInfo.username || (!isTelegramId ? target : '\u043d\u0435\u0442'),
          action: '\u041e\u0442\u0437\u044b\u0432',
          details: `ID: ${telegramId}`
        }
      );
      
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /check - \u043f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
    if (update.message?.text?.startsWith('/check')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          `\u274c \u0424\u043e\u0440\u043c\u0430\u0442: \`/check @user\` \u0438\u043b\u0438 \`/check ID\``
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      
      // \u0418\u0449\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f (\u043f\u043e ID \u0438\u043b\u0438 username)
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
          `\u274c *\u041b\u0438\u0446\u0435\u043d\u0437\u0438\u044f \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430*\n\n` +
          `\u{1f464} ${displayUser}\n` +
          `\u{1f194} ID: ${userInfo.telegramId || (isTelegramId ? target : '\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u0435\u043d')}\n\n` +
          (userInfo.found ? `\u2139\ufe0f \u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u0437\u0430\u0440\u0435\u0433\u0438\u0441\u0442\u0440\u0438\u0440\u043e\u0432\u0430\u043d, \u043d\u043e \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438 \u043d\u0435\u0442` : `\u2139\ufe0f \u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d \u0432 \u0431\u0430\u0437\u0435`),
          { reply_markup: {
            inline_keyboard: [
              [{ text: '\u2190 \u0410\u0434\u043c\u0438\u043d', callback_data: 'admin_back' }]
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
      
      // \u041f\u0440\u043e\u0433\u0440\u0435\u0441\u0441-\u0431\u0430\u0440
      const maxTime = 30 * 24 * 60 * 60 * 1000;
      const percent = isValid ? Math.min(100, Math.round((timeLeft / maxTime) * 100)) : 0;
      const bar = getProgressBar(percent);
      
      const emoji = isValid ? getStatusEmoji(daysLeft, licenseData.type) : '\u274c';
      const typeText = LICENSE_TYPES[licenseData.type]?.name || '\u041f\u0440\u0435\u043c\u0438\u0443\u043c';
      
      // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044e \u043e\u0431 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430\u0445
      const targetTgId = userInfo.telegramId || (isTelegramId ? target : null);
      let devicesInfo = '';
      if (targetTgId) {
        const devicesData = await env.USERS.get(`devices:${targetTgId}`);
        if (devicesData) {
          const devices = JSON.parse(devicesData);
          devicesInfo = `\n\u{1f4f1} \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432: ${devices.length}`;
        }
      }
      
      const checkMsg = `\u{1f50d} *\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c*\n\n` +
        `\u{1f464} ${displayUser}\n` +
        `\u{1f194} ID: ${targetTgId || '\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u0435\u043d'}\n\n` +
        `${emoji} ${typeText} \u2022 ${isValid ? '\u0410\u043a\u0442\u0438\u0432\u043d\u0430' : '\u0418\u0441\u0442\u0435\u043a\u043b\u0430'}\n` +
        `${bar} ${percent}%\n\n` +
        `\u23f0 ${daysLeft} \u0434\u043d. \u2022 \u0414\u043e: ${expiresDate}` +
        devicesInfo +
        (licenseData.grantedBy ? `\n\u{1f451} \u0412\u044b\u0434\u0430\u043b: @${licenseData.grantedBy}` : '');
      
      const checkTargetId = targetTgId || target;
      
      await sendTelegramMessage(env, chatId, checkMsg, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: '\u{1f381} +7', callback_data: `grant_${checkTargetId}_7` },
              { text: '\u{1f381} +30', callback_data: `grant_${checkTargetId}_30` },
              { text: '\u{1f381} +90', callback_data: `grant_${checkTargetId}_90` }
            ],
            [{ text: '\u274c \u041e\u0442\u043e\u0437\u0432\u0430\u0442\u044c', callback_data: `revoke_${checkTargetId}` }],
            [{ text: '\u2190 \u0410\u0434\u043c\u0438\u043d', callback_data: 'admin_back' }]
          ]
        }
      });
      
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /users
    if (update.message?.text === '/users') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430');
        return jsonResponse({ ok: true });
      }
      
      await sendTelegramMessage(env, chatId, 
        `\u{1f4ca} *\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438*\n\n` +
        `\`/check @user\` \u2014 \u043f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u043f\u043e username\n` +
        `\`/check ID\` \u2014 \u043f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u043f\u043e ID`
      );
      
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /stats
    if (update.message?.text === '/stats') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430');
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
        `\u{1f4c8} *\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u0431\u043e\u0442\u0430*\n\n` +
        `\u{1f916} @${botUsername.replace(/_/g, '\\_')}\n` +
        `\u{1f4e6} \u0412\u0435\u0440\u0441\u0438\u044f: ${BOT_VERSION}\n` +
        `\u{1f550} ${now} MSK\n` +
        `\u2705 KV: \u0420\u0430\u0431\u043e\u0442\u0430\u0435\u0442`
      );
      
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /broadcast - \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0430 \u0432\u0441\u0435\u043c \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f\u043c (\u0442\u043e\u043b\u044c\u043a\u043e \u0430\u0434\u043c\u0438\u043d)
    if (update.message?.text?.startsWith('/broadcast')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0430\u0434\u043c\u0438\u043d\u0430 \u043f\u043e ID
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430');
        return jsonResponse({ ok: true });
      }
      
      const text = update.message.text.replace('/broadcast', '').trim();
      
      if (!text) {
        await sendTelegramMessage(env, chatId, 
          `\u{1f4e2} *\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430*\n\n` +
          `\u0424\u043e\u0440\u043c\u0430\u0442: \`/broadcast \u0422\u0435\u043a\u0441\u0442 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f\`\n\n` +
          `\u26a0\ufe0f _\u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0431\u0443\u0434\u0435\u0442 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e \u0432\u0441\u0435\u043c \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f\u043c_`
        );
        return jsonResponse({ ok: true });
      }
      
      // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0432\u0441\u0435\u0445 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439 \u0441 \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u043c\u0438 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044f\u043c\u0438
      const users = await getAllLicensedUsers(env, 1000);
      const activeUsers = users.filter(u => u.isActive);
      
      let sent = 0;
      let failed = 0;
      
      // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u043e \u043d\u0430\u0447\u0430\u043b\u0435 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438
      await sendTelegramMessage(env, chatId, 
        `\u{1f4e2} *\u041d\u0430\u0447\u0438\u043d\u0430\u044e \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0443...*\n\n` +
        `\u{1f465} \u041f\u043e\u043b\u0443\u0447\u0430\u0442\u0435\u043b\u0435\u0439: ${activeUsers.length}`
      );
      
      // \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u0441 \u0437\u0430\u0434\u0435\u0440\u0436\u043a\u043e\u0439 \u0447\u0442\u043e\u0431\u044b \u043d\u0435 \u043f\u0440\u0435\u0432\u044b\u0441\u0438\u0442\u044c \u043b\u0438\u043c\u0438\u0442\u044b Telegram
      for (const user of activeUsers) {
        try {
          await sendTelegramMessage(env, user.telegramId, 
            `\u{1f4e2} *\u041e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u0435*\n\n${text}`,
            { reply_markup: getMainMenuKeyboard() }
          );
          sent++;
          // \u0417\u0430\u0434\u0435\u0440\u0436\u043a\u0430 50\u043c\u0441 \u043c\u0435\u0436\u0434\u0443 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f\u043c\u0438
          await new Promise(r => setTimeout(r, 50));
        } catch (e) {
          failed++;
        }
      }
      
      await sendTelegramMessage(env, chatId, 
        `\u2705 *\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430*\n\n` +
        `\u{1f4e4} \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: ${sent}\n` +
        `\u274c \u041e\u0448\u0438\u0431\u043e\u043a: ${failed}`
      );
      
      // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
      await logToChat(env,
        `\u{1f4e2} *\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430*\n\n` +
        `\u{1f464} \u041e\u0442: @${username}\n` +
        `\u{1f4e4} \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: ${sent}\n` +
        `\u274c \u041e\u0448\u0438\u0431\u043e\u043a: ${failed}`,
        'admin'
      );
      
      return jsonResponse({ ok: true });
    }
    
    // ========== \u041a\u041e\u041c\u0410\u041d\u0414\u042b \u0421\u0422\u0410\u0422\u0418\u0421\u0422\u0418\u041a\u0418 \u0418 \u0420\u0415\u0424\u0415\u0420\u0410\u041b\u041e\u0412 ==========
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /mystats - \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
    if (update.message?.text === '/mystats') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      const stats = await getUserStats(env, telegramId);
      const oderId = await env.USERS.get(`tg:${telegramId}`);
      
      let licenseInfo = '\u274c \u041d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d';
      if (oderId) {
        const result = await getLicenseData(env, oderId);
        if (result.license) {
          const daysLeft = Math.max(0, Math.ceil((result.license.expiresAt - Date.now()) / MS_PER_DAY));
          const isActive = result.license.expiresAt > Date.now();
          const typeText = LICENSE_TYPES[result.license.type]?.name || result.license.type;
          licenseInfo = isActive 
            ? `\u2705 ${typeText} (${daysLeft} ${getDaysWord(daysLeft)})`
            : `\u274c ${typeText} (\u0438\u0441\u0442\u0435\u043a\u043b\u0430)`;
        }
      }
      
      const referralCode = await getUserReferralCode(env, telegramId);
      const paidBonuses = stats.referralPaidBonus || 0;
      const pendingBonuses = stats.referralPendingBonus || 0;
      
      const message = `\u{1f4ca} *\u0412\u0430\u0448\u0430 \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430*\n\n` +
        `\u{1f446} *\u041a\u043b\u0438\u043a\u043e\u0432 \u0441\u0435\u0433\u043e\u0434\u043d\u044f:* ${stats.todayClicks || 0}\n` +
        `\u{1f4c8} *\u0412\u0441\u0435\u0433\u043e \u043a\u043b\u0438\u043a\u043e\u0432:* ${stats.totalClicks || 0}\n\n` +
        `\u{1f48e} *\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430:* ${licenseInfo}\n\n` +
        `\u{1f465} *\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u043e\u0432:* ${stats.referralCount || 0}\n` +
        `\u{1f381} *\u0411\u043e\u043d\u0443\u0441\u043e\u0432 \u043f\u043e\u043b\u0443\u0447\u0435\u043d\u043e:* ${paidBonuses} \u0434\u043d.\n` +
        (pendingBonuses > 0 ? `\u23f3 *\u041e\u0436\u0438\u0434\u0430\u044e\u0442 \u043e\u043f\u043b\u0430\u0442\u044b:* ${pendingBonuses}\n` : '') +
        `\u{1f39f}\ufe0f *\u0412\u0430\u0448 \u043a\u043e\u0434:* \`${referralCode}\`\n\n` +
        `_\u0411\u043e\u043d\u0443\u0441 +1 \u0434\u0435\u043d\u044c \u043a\u043e\u0433\u0434\u0430 \u0440\u0435\u0444\u0435\u0440\u0430\u043b \u043e\u0444\u043e\u0440\u043c\u0438\u0442 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443!_`;
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: {
          inline_keyboard: [
            [{ text: '\u{1f4e4} \u041f\u043e\u0434\u0435\u043b\u0438\u0442\u044c\u0441\u044f \u043a\u043e\u0434\u043e\u043c', callback_data: 'share_ref' }],
            [
              { text: '\u{1f504} \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c', callback_data: 'mystats' },
              { text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }
            ]
          ]
        }
      });
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /ref - \u0432\u0432\u043e\u0434 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u043e\u0433\u043e \u043a\u043e\u0434\u0430
    if (update.message?.text?.startsWith('/ref')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const parts = update.message.text.split(' ');
      
      if (parts.length < 2) {
        const myCode = await getUserReferralCode(env, telegramId);
        await sendTelegramMessage(env, chatId, 
          `\u{1f39f}\ufe0f *\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0438\u0441\u0442\u0435\u043c\u0430*\n\n` +
          `*\u0412\u0430\u0448 \u043a\u043e\u0434:* \`${myCode}\`\n\n` +
          `\u0427\u0442\u043e\u0431\u044b \u0432\u0432\u0435\u0441\u0442\u0438 \u0447\u0443\u0436\u043e\u0439 \u043a\u043e\u0434:\n` +
          `\`/ref \u041a\u041e\u0414\`\n\n` +
          `_\u0414\u0440\u0443\u0433 \u043f\u043e\u043b\u0443\u0447\u0438\u0442 1 \u0434\u0435\u043d\u044c \u043f\u0440\u043e\u0431\u043d\u0438\u043a\u0430._\n` +
          `_\u0412\u044b \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 +1 \u0434\u0435\u043d\u044c, \u043a\u043e\u0433\u0434\u0430 \u0434\u0440\u0443\u0433 \u043e\u0444\u043e\u0440\u043c\u0438\u0442 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443!_`
        );
        return jsonResponse({ ok: true });
      }
      
      const refCode = parts[1].toUpperCase();
      
      // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0444\u043e\u0440\u043c\u0430\u0442 \u043a\u043e\u0434\u0430
      if (!refCode.startsWith(REFERRAL_CODE_PREFIX)) {
        await sendTelegramMessage(env, chatId, 
          `\u274c *\u041d\u0435\u0432\u0435\u0440\u043d\u044b\u0439 \u0444\u043e\u0440\u043c\u0430\u0442 \u043a\u043e\u0434\u0430*\n\n` +
          `\u041a\u043e\u0434 \u0434\u043e\u043b\u0436\u0435\u043d \u043d\u0430\u0447\u0438\u043d\u0430\u0442\u044c\u0441\u044f \u0441 \`${REFERRAL_CODE_PREFIX}\``
        );
        return jsonResponse({ ok: true });
      }
      
      const result = await applyReferralCode(env, telegramId, refCode);
      
      if (result.success) {
        // \u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u043d\u043e\u0432\u043e\u043c\u0443 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044e
        if (result.gaveTrialToNewUser) {
          await sendTelegramMessage(env, chatId, 
            `\u2705 *\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434 \u043f\u0440\u0438\u043c\u0435\u043d\u0451\u043d!*\n\n` +
            `\u0421\u043f\u0430\u0441\u0438\u0431\u043e \u0437\u0430 \u0440\u0435\u0433\u0438\u0441\u0442\u0440\u0430\u0446\u0438\u044e \u043f\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u044e!\n` +
            `\u{1f381} \u0412\u0430\u043c \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d 1 \u0434\u0435\u043d\u044c \u043f\u0440\u043e\u0431\u043d\u043e\u0433\u043e \u043f\u0435\u0440\u0438\u043e\u0434\u0430!`
          );
        } else {
          await sendTelegramMessage(env, chatId, 
            `\u2705 *\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434 \u043f\u0440\u0438\u043c\u0435\u043d\u0451\u043d!*\n\n` +
            `\u0421\u043f\u0430\u0441\u0438\u0431\u043e \u0437\u0430 \u0440\u0435\u0433\u0438\u0441\u0442\u0440\u0430\u0446\u0438\u044e \u043f\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u044e!\n` +
            `_\u041f\u0440\u043e\u0431\u043d\u0438\u043a \u043d\u0435 \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d, \u0442.\u043a. \u0443 \u0432\u0430\u0441 \u0443\u0436\u0435 \u0435\u0441\u0442\u044c \u0430\u043a\u0442\u0438\u0432\u043d\u0430\u044f \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430._`
          );
        }
        
        // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u0440\u0435\u0444\u0435\u0440\u0435\u0440\u0430 - \u041d\u041e\u0412\u041e\u0415 \u0421\u041e\u041e\u0411\u0429\u0415\u041d\u0418\u0415
        try {
          await sendTelegramMessage(env, result.referrerTelegramId, 
            `\u{1f464} *\u041d\u043e\u0432\u044b\u0439 \u0440\u0435\u0444\u0435\u0440\u0430\u043b!*\n\n` +
            `\u041f\u043e \u0432\u0430\u0448\u0435\u0439 \u0441\u0441\u044b\u043b\u043a\u0435 \u043f\u0435\u0440\u0435\u0448\u0451\u043b \u043d\u043e\u0432\u044b\u0439 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c!\n\n` +
            `\u23f3 *\u0411\u043e\u043d\u0443\u0441 +1 \u0434\u0435\u043d\u044c* \u0431\u0443\u0434\u0435\u0442 \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d,\n` +
            `\u043a\u043e\u0433\u0434\u0430 \u0440\u0435\u0444\u0435\u0440\u0430\u043b \u043e\u0444\u043e\u0440\u043c\u0438\u0442 \u043b\u044e\u0431\u0443\u044e \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443.`
          );
        } catch (e) {}
        
        // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
        await logToChat(env,
          `\u{1f39f}\ufe0f *\u0420\u0435\u0444\u0435\u0440\u0430\u043b*\n\n` +
          `\u041d\u043e\u0432\u044b\u0439: ${telegramId}\n` +
          `\u0420\u0435\u0444\u0435\u0440\u0435\u0440: ${result.referrerTelegramId}`,
          'success',
          { action: '\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434', details: refCode }
        );
      } else {
        const errorMessages = {
          'INVALID_CODE': '\u041a\u043e\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d',
          'SELF_REFERRAL': '\u041d\u0435\u043b\u044c\u0437\u044f \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u044c \u0441\u0432\u043e\u0439 \u043a\u043e\u0434',
          'ALREADY_REFERRED': '\u0412\u044b \u0443\u0436\u0435 \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043b\u0438 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434'
        };
        await sendTelegramMessage(env, chatId, 
          `\u274c *\u041e\u0448\u0438\u0431\u043a\u0430*\n\n${errorMessages[result.error] || result.error}`
        );
      }
      
      return jsonResponse({ ok: true });
    }
    
    // ========== \u041a\u041e\u041c\u0410\u041d\u0414\u042b \u0413\u0420\u0423\u041f\u041f\u041e\u0412\u042b\u0425 \u0427\u0410\u0422\u041e\u0412 ==========
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /approve_group - \u043e\u0434\u043e\u0431\u0440\u0438\u0442\u044c \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 (\u0430\u0434\u043c\u0438\u043d)
    if (update.message?.text?.startsWith('/approve_group')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 2) {
        await sendTelegramMessage(env, chatId, 
          `\u274c \u0424\u043e\u0440\u043c\u0430\u0442: \`/approve_group chatId\``
        );
        return jsonResponse({ ok: true });
      }
      
      const targetChatId = parts[1];
      const result = await approveGroupChat(env, targetChatId, telegramId);
      
      if (result.success) {
        await sendTelegramMessage(env, chatId, 
          `\u2705 *\u0413\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 \u043e\u0434\u043e\u0431\u0440\u0435\u043d!*\n\n` +
          `\u{1f194} Chat ID: \`${targetChatId}\`\n` +
          `\u{1f464} \u0412\u043b\u0430\u0434\u0435\u043b\u0435\u0446: ${result.telegramId}`
        );
        
        // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
        try {
          await sendTelegramMessage(env, result.telegramId, 
            `\u2705 *\u0412\u0430\u0448 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u043e\u0439 \u0447\u0430\u0442 \u043e\u0434\u043e\u0431\u0440\u0435\u043d!*\n\n` +
            `\u0422\u0435\u043f\u0435\u0440\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0431\u0443\u0434\u0443\u0442 \u043f\u0440\u0438\u0445\u043e\u0434\u0438\u0442\u044c \u0438 \u0432 \u0433\u0440\u0443\u043f\u043f\u0443.`
          );
        } catch (e) {}
        
        // \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u043c \u043f\u0440\u0438\u0432\u0435\u0442\u0441\u0442\u0432\u0438\u0435 \u0432 \u0433\u0440\u0443\u043f\u043f\u0443
        try {
          await sendTelegramMessage(env, targetChatId, 
            `\u{1f916} *Exotic Assistant \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d!*\n\n` +
            `\u0422\u0435\u043f\u0435\u0440\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u043e \u0437\u0430\u044f\u0432\u043a\u0430\u0445 \u0431\u0443\u0434\u0443\u0442 \u043f\u0440\u0438\u0445\u043e\u0434\u0438\u0442\u044c \u0432 \u044d\u0442\u043e\u0442 \u0447\u0430\u0442.`
          );
        } catch (e) {}
      } else {
        await sendTelegramMessage(env, chatId, 
          `\u274c \u041e\u0448\u0438\u0431\u043a\u0430: ${result.error}`
        );
      }
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /devices - \u0443\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430\u043c\u0438 (\u0434\u043b\u044f \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f)
    if (update.message?.text === '/devices') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u0435
      const existingOderId = await env.USERS.get(`tg:${telegramId}`);
      if (!existingOderId) {
        await sendTelegramMessage(env, chatId, 
          `\u274c *\u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e*\n\n` +
          `\u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u0447\u0435\u0440\u0435\u0437 \u043a\u043e\u0434.`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '\u{1f517} \u041a\u0430\u043a \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c', callback_data: 'how_to_connect' }],
              [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044e \u043e \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438 \u0438 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430\u0445
      const licenseData = await env.USERS.get(`license:tg:${telegramId}`);
      const license = licenseData ? JSON.parse(licenseData) : null;
      const licenseType = LICENSE_TYPES[license?.type] || LICENSE_TYPES.trial;
      const maxDevices = licenseType.maxDevices || 1;
      
      const devicesData = await getUserDevices(env, telegramId);
      const devices = devicesData.devices || [];
      
      let message = `\u{1f4f1} *\u0412\u0430\u0448\u0438 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430*\n\n`;
      message += `\u{1f4e6} \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430: *${licenseType.name}*\n`;
      message += `\u{1f522} \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d\u043e: ${devices.length}/${maxDevices}\n\n`;
      
      if (devices.length === 0) {
        message += `_\u041d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d\u043d\u044b\u0445 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432_\n\n`;
        message += `\u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u0441\u044f \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438\n\u043f\u0440\u0438 \u043f\u0435\u0440\u0432\u043e\u0439 \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0435 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438.`;
      } else {
        message += `*\u0421\u043f\u0438\u0441\u043e\u043a \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432:*\n`;
        devices.forEach((d, i) => {
          const addedDate = new Date(d.addedAt).toLocaleDateString('ru-RU');
          const lastSeenDate = new Date(d.lastSeen).toLocaleDateString('ru-RU');
          const deviceShort = d.deviceId.substring(0, 12) + '...';
          message += `\n${i + 1}. \`${deviceShort}\`\n`;
          message += `   \u{1f4c5} \u0414\u043e\u0431\u0430\u0432\u043b\u0435\u043d: ${addedDate}\n`;
          message += `   \u{1f441} \u0410\u043a\u0442\u0438\u0432\u0435\u043d: ${lastSeenDate}`;
        });
      }
      
      // \u041a\u043d\u043e\u043f\u043a\u0438 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0439
      const buttons = [];
      if (devices.length > 0) {
        buttons.push([{ text: '\u{1f5d1} \u041e\u0442\u0432\u044f\u0437\u0430\u0442\u044c \u0432\u0441\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430', callback_data: 'clear_devices' }]);
      }
      if (maxDevices < 2) {
        buttons.push([{ text: '\u2b06\ufe0f \u0411\u043e\u043b\u044c\u0448\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432', callback_data: 'buy' }]);
      }
      buttons.push([{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]);
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: { inline_keyboard: buttons }
      });
      
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /groups - \u0443\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u0433\u0440\u0443\u043f\u043f\u0430\u043c\u0438 (\u0434\u043b\u044f \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f)
    if (update.message?.text === '/groups') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      const canUse = await canUseGroupChats(env, telegramId);
      
      if (!canUse.allowed) {
        let errorMsg = '';
        switch (canUse.reason) {
          case 'NOT_CONNECTED':
            errorMsg = '\u274c \u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u0440\u0438\u0432\u044f\u0436\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435';
            break;
          case 'NO_LICENSE':
            errorMsg = '\u274c \u041d\u0435\u0442 \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0439 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438';
            break;
          case 'UPGRADE_REQUIRED':
            errorMsg = `\u274c \u0413\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b \u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f *Premium* \u0438 *\u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c*\n\n` +
                       `\u0412\u0430\u0448\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430: ${canUse.currentType || '\u0411\u0430\u0437\u043e\u0432\u0430\u044f'}`;
            break;
          default:
            errorMsg = '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430 \u043a \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u043c \u0447\u0430\u0442\u0430\u043c';
        }
        
        await sendTelegramMessage(env, chatId, errorMsg, {
          reply_markup: {
            inline_keyboard: [
              [{ text: '\u{1f4b3} \u0423\u043b\u0443\u0447\u0448\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
              [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
            ]
          }
        });
        return jsonResponse({ ok: true });
      }
      
      const groupData = await getUserGroupChats(env, telegramId);
      
      let message = `\u{1f4ac} *\u0413\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b*\n\n`;
      
      if (groupData.chats.length === 0) {
        message += `\u0423 \u0432\u0430\u0441 \u043f\u043e\u043a\u0430 \u043d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d\u043d\u044b\u0445 \u0433\u0440\u0443\u043f\u043f.\n\n`;
        message += `*\u041a\u0430\u043a \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c:*\n`;
        message += `1\ufe0f\u20e3 \u0414\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u0431\u043e\u0442\u0430 @clickuved\\_bot \u0432 \u0433\u0440\u0443\u043f\u043f\u0443\n`;
        message += `2\ufe0f\u20e3 \u0411\u043e\u0442 \u0437\u0430\u043f\u0440\u043e\u0441\u0438\u0442 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u0435 \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430\n`;
        message += `3\ufe0f\u20e3 \u041f\u043e\u0441\u043b\u0435 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u044f \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0431\u0443\u0434\u0443\u0442 \u043f\u0440\u0438\u0445\u043e\u0434\u0438\u0442\u044c \u0432 \u0433\u0440\u0443\u043f\u043f\u0443`;
      } else {
        message += `*\u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d\u043d\u044b\u0435 \u0433\u0440\u0443\u043f\u043f\u044b (${groupData.chats.length}):*\n\n`;
        groupData.chats.forEach((chat, i) => {
          message += `${i + 1}. ${chat.chatTitle}\n`;
        });
        message += `\n_\u041b\u0438\u043c\u0438\u0442: ${canUse.maxChats} \u0433\u0440\u0443\u043f\u043f\u044b_`;
      }
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: {
          inline_keyboard: [
            [{ text: '\u{1f4d6} \u0418\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u044f', callback_data: 'group_help' }],
            [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
          ]
        }
      });
      
      return jsonResponse({ ok: true });
    }
    
    // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /grant_premium - \u0432\u044b\u0434\u0430\u0442\u044c Premium \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443 (\u0434\u043b\u044f \u0433\u0440\u0443\u043f\u043f)
    if (update.message?.text?.startsWith('/grant_premium')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      if (!await isAdmin(telegramId)) {
        await sendTelegramMessage(env, chatId, '\u274c \u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0430');
        return jsonResponse({ ok: true });
      }
      
      const parts = update.message.text.split(' ');
      if (parts.length < 3) {
        await sendTelegramMessage(env, chatId, 
          `\u274c \u0424\u043e\u0440\u043c\u0430\u0442: \`/grant_premium ID \u0434\u043d\u0435\u0439\`\n\n` +
          `\u0422\u0438\u043f\u044b: level2, level3`
        );
        return jsonResponse({ ok: true });
      }
      
      const target = parts[1].replace('@', '');
      const days = parseInt(parts[2]);
      const type = parts[3] || 'level2'; // level2 \u0438\u043b\u0438 level3
      
      if (isNaN(days) || days <= 0) {
        await sendTelegramMessage(env, chatId, '\u274c \u0423\u043a\u0430\u0436\u0438\u0442\u0435 \u0434\u043d\u0438 (\u0447\u0438\u0441\u043b\u043e > 0)');
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
        type: type, // 'premium' \u0438\u043b\u0438 'max'
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
        `\u2705 *${typeInfo.name} \u0432\u044b\u0434\u0430\u043d!*\n\n` +
        `\u{1f464} ${isTelegramId ? target : '@' + target}\n` +
        `\u{1f4c5} +${days} \u0434\u043d. \u2192 ${daysLeft} \u0434\u043d.\n` +
        `\u{1f4ac} \u0413\u0440\u0443\u043f\u043f\u044b: ${typeInfo.groupChats ? '\u2705' : '\u274c'}\n` +
        `\u{1f4f1} \u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432: ${typeInfo.maxDevices}`
      );
      
      return jsonResponse({ ok: true });
    }
    
    // \u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430 \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u0438\u044f \u0431\u043e\u0442\u0430 \u0432 \u0433\u0440\u0443\u043f\u043f\u0443
    if (update.my_chat_member) {
      const member = update.my_chat_member;
      const chat = member.chat;
      const newStatus = member.new_chat_member.status;
      const fromUser = member.from;
      
      // \u0411\u043e\u0442 \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d \u0432 \u0433\u0440\u0443\u043f\u043f\u0443
      if (chat.type === 'group' || chat.type === 'supergroup') {
        if (newStatus === 'member' || newStatus === 'administrator') {
          const telegramId = String(fromUser.id);
          const username = fromUser.username;
          
          // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043f\u0440\u0430\u0432\u043e \u043d\u0430 \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b
          const canUse = await canUseGroupChats(env, telegramId);
          
          if (!canUse.allowed) {
            await sendTelegramMessage(env, chat.id, 
              `\u274c *\u0413\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b*\n\n` +
              `@${username || telegramId}, \u0434\u043b\u044f \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d\u0438\u044f \u0431\u043e\u0442\u0430 \u0432 \u0433\u0440\u0443\u043f\u043f\u0430\u0445 ` +
              `\u043d\u0435\u043e\u0431\u0445\u043e\u0434\u0438\u043c\u0430 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 Premium \u0438\u043b\u0438 \u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c.\n\n` +
              `\u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 \u0431\u043e\u0442\u0443 \u043b\u0438\u0447\u043d\u043e \u0434\u043b\u044f \u043f\u043e\u0434\u0440\u043e\u0431\u043d\u043e\u0441\u0442\u0435\u0439.`
            );
            return jsonResponse({ ok: true });
          }
          
          // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043b\u0438\u043c\u0438\u0442 \u0433\u0440\u0443\u043f\u043f
          const groupData = await getUserGroupChats(env, telegramId);
          if (groupData.chats.length >= canUse.maxChats) {
            await sendTelegramMessage(env, chat.id, 
              `\u274c *\u041b\u0438\u043c\u0438\u0442 \u0433\u0440\u0443\u043f\u043f \u0434\u043e\u0441\u0442\u0438\u0433\u043d\u0443\u0442*\n\n` +
              `\u0423 \u0432\u0430\u0441 \u0443\u0436\u0435 ${groupData.chats.length}/${canUse.maxChats} \u0433\u0440\u0443\u043f\u043f.\n` +
              `\u041e\u0442\u043a\u043b\u044e\u0447\u0438\u0442\u0435 \u043e\u0434\u043d\u0443 \u0438\u0437 \u0433\u0440\u0443\u043f\u043f \u0447\u0435\u0440\u0435\u0437 /groups`
            );
            return jsonResponse({ ok: true });
          }
          
          // \u0417\u0430\u043f\u0440\u0430\u0448\u0438\u0432\u0430\u0435\u043c \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u0435 \u0430\u0434\u043c\u0438\u043d\u0430
          const result = await requestGroupChatApproval(env, telegramId, chat.id, chat.title, username);
          
          if (result.success) {
            await sendTelegramMessage(env, chat.id, 
              `\u23f3 *\u0417\u0430\u043f\u0440\u043e\u0441 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d*\n\n` +
              `\u0410\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0440\u0430\u0441\u0441\u043c\u043e\u0442\u0440\u0438\u0442 \u0432\u0430\u0448\u0443 \u0437\u0430\u044f\u0432\u043a\u0443.\n` +
              `\u0412\u044b \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435 \u043f\u043e\u0441\u043b\u0435 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u044f.`
            );
          } else if (result.error === 'REQUEST_PENDING') {
            await sendTelegramMessage(env, chat.id, 
              `\u23f3 *\u0417\u0430\u043f\u0440\u043e\u0441 \u0443\u0436\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d*\n\n` +
              `\u041e\u0436\u0438\u0434\u0430\u0439\u0442\u0435 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0438\u044f \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430.`
            );
          }
          
          return jsonResponse({ ok: true });
        }
        
        // \u0411\u043e\u0442 \u0443\u0434\u0430\u043b\u0451\u043d \u0438\u0437 \u0433\u0440\u0443\u043f\u043f\u044b
        if (newStatus === 'left' || newStatus === 'kicked') {
          // \u0423\u0434\u0430\u043b\u044f\u0435\u043c \u0433\u0440\u0443\u043f\u043f\u0443 \u0438\u0437 \u0441\u043f\u0438\u0441\u043a\u0430 \u0432\u043b\u0430\u0434\u0435\u043b\u044c\u0446\u0430
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
    
    // === \u041e\u0411\u0420\u0410\u0411\u041e\u0422\u041a\u0410 \u0421\u041e\u041e\u0411\u0429\u0415\u041d\u0418\u0419 \u0412 \u0413\u0420\u0423\u041f\u041f\u041e\u0412\u042b\u0425 \u0427\u0410\u0422\u0410\u0425 ===
    if (update.message && (update.message.chat.type === 'group' || update.message.chat.type === 'supergroup')) {
      const chat = update.message.chat;
      const chatId = chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const text = update.message.text || '';
      const botUsername = env.BOT_USERNAME || 'clickuved_bot';
      
      // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043c\u0430\u043f\u043f\u0438\u043d\u0433 username \u2192 telegramId
      if (username) {
        await saveUsernameMapping(env, username, telegramId);
      }
      
      // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c - \u044d\u0442\u043e \u043a\u043e\u043c\u0430\u043d\u0434\u0430 \u0431\u043e\u0442\u0443 \u0438\u043b\u0438 \u0443\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u0431\u043e\u0442\u0430
      const isDirectCommand = text.startsWith('/') && (
        text.includes(`@${botUsername}`) || 
        !text.includes('@') // \u043a\u043e\u043c\u0430\u043d\u0434\u0430 \u0431\u0435\u0437 @ \u0430\u0434\u0440\u0435\u0441\u043e\u0432\u0430\u043d\u0430 \u0432\u0441\u0435\u043c \u0431\u043e\u0442\u0430\u043c
      );
      const isBotMention = text.toLowerCase().includes(`@${botUsername.toLowerCase()}`);
      const isReplyToBot = update.message.reply_to_message?.from?.username === botUsername;
      
      // \u0415\u0441\u043b\u0438 \u044d\u0442\u043e \u043d\u0435 \u043a\u043e\u043c\u0430\u043d\u0434\u0430 \u0431\u043e\u0442\u0443 - \u0438\u0433\u043d\u043e\u0440\u0438\u0440\u0443\u0435\u043c
      if (!isDirectCommand && !isBotMention && !isReplyToBot) {
        return jsonResponse({ ok: true });
      }
      
      // \u0423\u0431\u0438\u0440\u0430\u0435\u043c @username \u0438\u0437 \u043a\u043e\u043c\u0430\u043d\u0434\u044b
      const cleanText = text.replace(new RegExp(`@${botUsername}`, 'gi'), '').trim();
      
      // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c - \u0433\u0440\u0443\u043f\u043f\u0430 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0430 \u0434\u043b\u044f \u044d\u0442\u043e\u0433\u043e \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
      const groupOwner = await env.USERS.get(`group_owner:${chatId}`);
      
      // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /start \u0432 \u0433\u0440\u0443\u043f\u043f\u0435 - \u043f\u043e\u043a\u0430\u0437\u044b\u0432\u0430\u0435\u043c \u043f\u0440\u0438\u0432\u0435\u0442\u0441\u0442\u0432\u0438\u0435
      if (cleanText.startsWith('/start')) {
        if (!groupOwner) {
          await sendTelegramMessage(env, chatId, 
            `\u{1f44b} *Exotic Assistant*\n\n` +
            `\u0414\u043b\u044f \u0440\u0430\u0431\u043e\u0442\u044b \u0431\u043e\u0442\u0430 \u0432 \u044d\u0442\u043e\u0439 \u0433\u0440\u0443\u043f\u043f\u0435\n` +
            `\u043d\u0435\u043e\u0431\u0445\u043e\u0434\u0438\u043c\u043e \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0435\u0433\u043e \u043a\u0430\u043a \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430.\n\n` +
            `\u0412\u043b\u0430\u0434\u0435\u043b\u0435\u0446 \u0433\u0440\u0443\u043f\u043f\u044b \u0434\u043e\u043b\u0436\u0435\u043d \u0438\u043c\u0435\u0442\u044c\n` +
            `\u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443 *Level 2* \u0438\u043b\u0438 \u0432\u044b\u0448\u0435.\n\n` +
            `\u{1f517} \u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 \u0431\u043e\u0442\u0443 \u043b\u0438\u0447\u043d\u043e:\n@${botUsername}`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f916} \u041d\u0430\u043f\u0438\u0441\u0430\u0442\u044c \u0431\u043e\u0442\u0443', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
        } else {
          await sendTelegramMessage(env, chatId, 
            `\u{1f44b} *Exotic Assistant*\n\n` +
            `\u0411\u043e\u0442 \u0430\u043a\u0442\u0438\u0432\u0435\u043d \u0432 \u044d\u0442\u043e\u0439 \u0433\u0440\u0443\u043f\u043f\u0435!\n\n` +
            `\u{1f4ca} \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\u0442\u0435 \`/status\` \u0434\u043b\u044f \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'group_status' }],
                [{ text: '\u{1f916} \u041d\u0430\u043f\u0438\u0441\u0430\u0442\u044c \u0431\u043e\u0442\u0443', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
        }
        return jsonResponse({ ok: true });
      }
      
      // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /status \u0432 \u0433\u0440\u0443\u043f\u043f\u0435
      if (cleanText.startsWith('/status')) {
        // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f
        const existingOderId = await env.USERS.get(`tg:${telegramId}`);
        
        if (!existingOderId) {
          await sendTelegramMessage(env, chatId, 
            `\u{1f4ca} @${username || telegramId}, \u0432\u0430\u0448 \u0441\u0442\u0430\u0442\u0443\u0441:\n\n` +
            `\u274c \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n\n` +
            `\u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 \u0431\u043e\u0442\u0443 \u0432 \u041b\u0421 \u0434\u043b\u044f \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f.`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f916} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c', url: `https://t.me/${botUsername}` }]
              ]
            }}
          );
          return jsonResponse({ ok: true });
        }
        
        const result = await getLicenseData(env, existingOderId);
        
        if (!result.license) {
          await sendTelegramMessage(env, chatId, 
            `\u{1f4ca} @${username || telegramId}, \u0432\u0430\u0448 \u0441\u0442\u0430\u0442\u0443\u0441:\n\n` +
            `\u2705 \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n` +
            `\u274c \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430`,
            { reply_markup: {
              inline_keyboard: [
                [{ text: '\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c', url: `https://t.me/${botUsername}` }]
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
        
        const emoji = isValid ? getStatusEmoji(daysLeft, license.type) : '\u274c';
        const typeText = LICENSE_TYPES[license.type]?.name || '\u041f\u0440\u0435\u043c\u0438\u0443\u043c';
        const timeDisplay = license.type === 'trial' ? `${hoursLeft} \u0447.` : `${daysLeft} \u0434\u043d.`;
        
        await sendTelegramMessage(env, chatId, 
          `\u{1f4ca} @${username || telegramId}, \u0432\u0430\u0448 \u0441\u0442\u0430\u0442\u0443\u0441:\n\n` +
          `${emoji} ${typeText} \u2022 ${isValid ? '\u0410\u043a\u0442\u0438\u0432\u043d\u0430' : '\u0418\u0441\u0442\u0435\u043a\u043b\u0430'}\n` +
          `\u23f0 ${isValid ? `\u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c: ${timeDisplay}` : '\u041f\u0440\u043e\u0434\u043b\u0438\u0442\u0435 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443'}`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '\u{1f4ca} \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u0435\u0435', url: `https://t.me/${botUsername}` }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /help \u0432 \u0433\u0440\u0443\u043f\u043f\u0435
      if (cleanText.startsWith('/help')) {
        await sendTelegramMessage(env, chatId, 
          `\u{1f4d6} *Exotic Assistant*\n\n` +
          `\u0414\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0435 \u043a\u043e\u043c\u0430\u043d\u0434\u044b \u0432 \u0433\u0440\u0443\u043f\u043f\u0435:\n\n` +
          `\`/status\` \u2014 \u0432\u0430\u0448 \u0441\u0442\u0430\u0442\u0443\u0441 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438\n` +
          `\`/id\` \u2014 \u0443\u0437\u043d\u0430\u0442\u044c ID\n` +
          `\`/help\` \u2014 \u044d\u0442\u043e \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435\n\n` +
          `\u0414\u043b\u044f \u043f\u043e\u043b\u043d\u043e\u0433\u043e \u0444\u0443\u043d\u043a\u0446\u0438\u043e\u043d\u0430\u043b\u0430\n` +
          `\u043d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 \u0431\u043e\u0442\u0443 \u0432 \u041b\u0421.`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '\u{1f916} \u041d\u0430\u043f\u0438\u0441\u0430\u0442\u044c \u0431\u043e\u0442\u0443', url: `https://t.me/${botUsername}` }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      // \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /id \u0432 \u0433\u0440\u0443\u043f\u043f\u0435
      if (cleanText.startsWith('/id')) {
        let message = `\u{1f194} *\u0418\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044f \u043e\u0431 ID*\n\n`;
        message += `\u{1f464} *@${username || telegramId}:*\n`;
        message += `\u2514 ID: \`${telegramId}\`\n`;
        message += `\n\u{1f4ac} *\u042d\u0442\u043e\u0442 \u0447\u0430\u0442:*\n`;
        message += `\u251c ID: \`${chatId}\`\n`;
        message += `\u2514 \u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435: ${chat.title || '\u0411\u0435\u0437 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u044f'}`;
        
        await sendTelegramMessage(env, chatId, message);
        return jsonResponse({ ok: true });
      }
      
      // \u041d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u0430\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u0430 \u0432 \u0433\u0440\u0443\u043f\u043f\u0435
      if (cleanText.startsWith('/')) {
        await sendTelegramMessage(env, chatId, 
          `\u2753 \u041d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u0430\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u0430.\n\n` +
          `\u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\u0442\u0435 \`/help\` \u0434\u043b\u044f \u0441\u043f\u0438\u0441\u043a\u0430 \u043a\u043e\u043c\u0430\u043d\u0434.`
        );
        return jsonResponse({ ok: true });
      }
      
      // \u0423\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0435 \u0431\u043e\u0442\u0430 \u0431\u0435\u0437 \u043a\u043e\u043c\u0430\u043d\u0434\u044b
      if (isBotMention) {
        await sendTelegramMessage(env, chatId, 
          `\u{1f44b} \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\u0442\u0435 \`/help\` \u0434\u043b\u044f \u0441\u043f\u0438\u0441\u043a\u0430 \u043a\u043e\u043c\u0430\u043d\u0434.`
        );
        return jsonResponse({ ok: true });
      }
      
      return jsonResponse({ ok: true });
    }
    
    // \u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430 \u043a\u043e\u043c\u0430\u043d\u0434\u044b /start
    if (update.message?.text?.startsWith('/start')) {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const firstName = update.message.from.first_name;
      
      // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u043c\u0430\u043f\u043f\u0438\u043d\u0433 username \u2192 telegramId \u0434\u043b\u044f \u043f\u043e\u0438\u0441\u043a\u0430
      if (username) {
        await saveUsernameMapping(env, username, telegramId);
      }
      
      const parts = update.message.text.split(' ');
      const param = parts[1];
      
      // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u0443\u044e \u0441\u0441\u044b\u043b\u043a\u0443 (ref_REF_XXXX)
      if (param && param.startsWith('ref_')) {
        const refCode = param.replace('ref_', '');
        const result = await applyReferralCode(env, telegramId, refCode);
        
        if (result.success) {
          // \u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u043d\u043e\u0432\u043e\u043c\u0443 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044e
          if (result.gaveTrialToNewUser) {
            await sendTelegramMessage(env, chatId, 
              `\u{1f389} *\u0414\u043e\u0431\u0440\u043e \u043f\u043e\u0436\u0430\u043b\u043e\u0432\u0430\u0442\u044c!*\n\n` +
              `\u0412\u044b \u043f\u0440\u0438\u0448\u043b\u0438 \u043f\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u044e \u0434\u0440\u0443\u0433\u0430.\n` +
              `\u{1f381} \u0412\u0430\u043c \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d 1 \u0434\u0435\u043d\u044c \u043f\u0440\u043e\u0431\u043d\u043e\u0433\u043e \u043f\u0435\u0440\u0438\u043e\u0434\u0430!`,
              { reply_markup: getMainMenuKeyboard() }
            );
          } else {
            await sendTelegramMessage(env, chatId, 
              `\u{1f389} *\u0414\u043e\u0431\u0440\u043e \u043f\u043e\u0436\u0430\u043b\u043e\u0432\u0430\u0442\u044c!*\n\n` +
              `\u0412\u044b \u043f\u0440\u0438\u0448\u043b\u0438 \u043f\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u044e \u0434\u0440\u0443\u0433\u0430.\n` +
              `_\u041f\u0440\u043e\u0431\u043d\u0438\u043a \u043d\u0435 \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d, \u0442.\u043a. \u0443 \u0432\u0430\u0441 \u0443\u0436\u0435 \u0435\u0441\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0430._`,
              { reply_markup: getMainMenuKeyboard() }
            );
          }
          
          // \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u044f\u0435\u043c \u0440\u0435\u0444\u0435\u0440\u0435\u0440\u0430 - \u041d\u041e\u0412\u042b\u0419 \u0422\u0415\u041a\u0421\u0422
          try {
            await sendTelegramMessage(env, result.referrerTelegramId, 
              `\u{1f464} *\u041d\u043e\u0432\u044b\u0439 \u0440\u0435\u0444\u0435\u0440\u0430\u043b!*\n\n` +
              `\u041f\u043e \u0432\u0430\u0448\u0435\u0439 \u0441\u0441\u044b\u043b\u043a\u0435 \u043f\u0435\u0440\u0435\u0448\u0451\u043b \u043d\u043e\u0432\u044b\u0439 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c!\n\n` +
              `\u23f3 *\u0411\u043e\u043d\u0443\u0441 +1 \u0434\u0435\u043d\u044c* \u0431\u0443\u0434\u0435\u0442 \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d,\n` +
              `\u043a\u043e\u0433\u0434\u0430 \u0440\u0435\u0444\u0435\u0440\u0430\u043b \u043e\u0444\u043e\u0440\u043c\u0438\u0442 \u043b\u044e\u0431\u0443\u044e \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443.`
            );
          } catch (e) {}
          
          // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
          await logToChat(env,
            `\u{1f39f}\ufe0f *\u0420\u0435\u0444\u0435\u0440\u0430\u043b*\n\n` +
            `\u041d\u043e\u0432\u044b\u0439: ${telegramId}\n` +
            `\u0420\u0435\u0444\u0435\u0440\u0435\u0440: ${result.referrerTelegramId}`,
            'success',
            { action: '\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u043a\u043e\u0434', details: refCode }
          );
        } else {
          // \u041a\u043e\u0434 \u043d\u0435\u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0442\u0435\u043b\u0435\u043d, \u043d\u043e \u0432\u0441\u0451 \u0440\u0430\u0432\u043d\u043e \u043f\u0440\u0438\u0432\u0435\u0442\u0441\u0442\u0432\u0443\u0435\u043c
          await sendTelegramMessage(env, chatId, 
            `\u{1f44b} *\u0414\u043e\u0431\u0440\u043e \u043f\u043e\u0436\u0430\u043b\u043e\u0432\u0430\u0442\u044c!*\n\n` +
            `Exotic Operator Assistant \u2014 \u0432\u0430\u0448\n` +
            `\u043f\u043e\u043c\u043e\u0449\u043d\u0438\u043a \u0434\u043b\u044f \u0440\u0430\u0431\u043e\u0442\u044b \u0441 \u0437\u0430\u044f\u0432\u043a\u0430\u043c\u0438.\n\n` +
            `\u{1f517} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u0434\u043b\u044f \u043d\u0430\u0447\u0430\u043b\u0430:`,
            { reply_markup: getMainMenuKeyboard() }
          );
        }
        return jsonResponse({ ok: true });
      }
      
      // \u041f\u0440\u0438\u0432\u044f\u0437\u043a\u0430 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u0430 \u043f\u043e \u043a\u043e\u0434\u0443
      if (param) {
        const codeData = await env.USERS.get(`code:${param}`);
        
        if (!codeData) {
          await sendTelegramMessage(env, chatId, 
            `\u274c *\u041a\u043e\u0434 \u043d\u0435\u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0442\u0435\u043b\u0435\u043d*\n\n\u041f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 \u043d\u043e\u0432\u044b\u0439 \u0432 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430\u0445`,
            { reply_markup: getBackButton() }
          );
          return jsonResponse({ ok: true });
        }
        
        const { oderId, deviceId } = JSON.parse(codeData);
        
        // \u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u0441\u0432\u044f\u0437\u043a\u0443
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
        
        // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
        await logToChat(env,
          `\u{1f517} *\u041f\u0440\u0438\u0432\u044f\u0437\u0430\u043d*`,
          'connect',
          {
            telegramId,
            username: username || '\u043d\u0435\u0442',
            action: '\u041a\u043e\u0434 \u0442\u0435\u043a\u0441\u0442\u043e\u043c',
            details: `${param.substring(0, 3)}***`
          }
        );
        
        const result = await getLicenseData(env, oderId);
        
        let message = `\u2705 *\u041f\u0440\u0438\u0432\u044f\u0437\u0430\u043d\u043e!*\n\n`;
        
        if (result.license) {
          const timeLeft = result.license.expiresAt - Date.now();
          const hoursLeft = Math.ceil(timeLeft / (60 * 60 * 1000));
          const daysLeft = Math.ceil(timeLeft / (24 * 60 * 60 * 1000));
          
          if (result.license.type === 'trial') {
            message += `\u{1f381} \u041f\u0440\u043e\u0431\u043d\u044b\u0439: *${hoursLeft} \u0447.*`;
          } else {
            message += `\u{1f48e} \u041f\u0440\u0435\u043c\u0438\u0443\u043c: *${daysLeft} \u0434\u043d.*`;
          }
        }
        
        await sendTelegramMessage(env, chatId, message, {
          reply_markup: {
            inline_keyboard: [
              [
                { text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' },
                { text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }
              ]
            ]
          }
        });
      } else {
        // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0443
        const existingOderId = await env.USERS.get(`tg:${telegramId}`);
        
        let welcomeMsg;
        let keyboard;
        
        if (existingOderId) {
          // \u0415\u0441\u0442\u044c \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0430 - \u043f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e \u0447\u0435\u0440\u0435\u0437 getLicenseData
          // \u042d\u0442\u043e \u0442\u0430\u043a\u0436\u0435 \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438 \u0441\u043e\u0437\u0434\u0430\u0441\u0442 \u043f\u0440\u043e\u0431\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434 \u0435\u0441\u043b\u0438 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u0438 \u043d\u0435\u0442
          const result = await getLicenseData(env, existingOderId);
          const stats = await getUserStats(env, telegramId);
          const canUseGroups = result.license && LICENSE_TYPES[result.license.type]?.groupChats;
          
          if (result.license) {
            const license = result.license;
            const isValid = license.expiresAt > Date.now();
            const timeLeft = license.expiresAt - Date.now();
            const daysLeft = Math.ceil(timeLeft / (24 * 60 * 60 * 1000));
            const hoursLeft = Math.ceil(timeLeft / (60 * 60 * 1000));
            
            // \u041f\u0440\u043e\u0433\u0440\u0435\u0441\u0441-\u0431\u0430\u0440
            const maxTime = license.type === 'trial' ? 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
            const percent = isValid ? Math.min(100, Math.round((timeLeft / maxTime) * 100)) : 0;
            const bar = getProgressBar(percent);
            
            const emoji = isValid ? getStatusEmoji(daysLeft, license.type) : '\u274c';
            const timeDisplay = license.type === 'trial' ? `${hoursLeft} \u0447.` : `${daysLeft} \u0434\u043d.`;
            const typeName = LICENSE_TYPES[license.type]?.name || license.type;
            
            welcomeMsg = `\u{1f44b} *${firstName || '\u041f\u0440\u0438\u0432\u0435\u0442'}!*\n\n`;
            
            if (isValid) {
              welcomeMsg += `${emoji} *${typeName}* \u2014 ${timeDisplay}\n`;
              welcomeMsg += `${bar} ${percent}%\n\n`;
              welcomeMsg += `\u{1f4ca} \u041a\u043b\u0438\u043a\u043e\u0432 \u0441\u0435\u0433\u043e\u0434\u043d\u044f: *${stats.todayClicks || 0}*`;
            } else {
              welcomeMsg += `\u274c \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u0438\u0441\u0442\u0435\u043a\u043b\u0430\n\n`;
              welcomeMsg += `\u041f\u0440\u043e\u0434\u043b\u0438\u0442\u0435 \u0434\u043b\u044f \u043f\u0440\u043e\u0434\u043e\u043b\u0436\u0435\u043d\u0438\u044f \u0440\u0430\u0431\u043e\u0442\u044b`;
            }
            
            keyboard = getUserMenuKeyboard(isValid, canUseGroups);
          } else {
            // \u041f\u0440\u0438\u0432\u044f\u0437\u043a\u0430 \u0435\u0441\u0442\u044c, \u043d\u043e \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044f \u043d\u0435 \u0441\u043e\u0437\u0434\u0430\u043b\u0430\u0441\u044c (\u0441\u0442\u0440\u0430\u043d\u043d\u044b\u0439 \u0441\u043b\u0443\u0447\u0430\u0439)
            welcomeMsg = `\u{1f44b} *${firstName || '\u041f\u0440\u0438\u0432\u0435\u0442'}!*\n\n\u2705 \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n\n\u26a0\ufe0f \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430`;
            keyboard = {
              inline_keyboard: [
                [{ text: '\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
                [{ text: '\u{1f4ac} \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430', callback_data: 'support' }]
              ]
            };
          }
        } else {
          // \u041d\u043e\u0432\u044b\u0439 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c - \u043d\u0435 \u043f\u0440\u0438\u0432\u044f\u0437\u0430\u043d
          welcomeMsg = `\u{1f44b} *${firstName || '\u041f\u0440\u0438\u0432\u0435\u0442'}!*

*Exotic Assistant v${BOT_VERSION}* \u{1f916}

\u{1f5b1}\ufe0f \u0410\u0432\u0442\u043e\u043a\u043b\u0438\u043a \u043f\u043e \u0437\u0430\u044f\u0432\u043a\u0430\u043c
\u{1f4ca} \u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u0438 \u0430\u043d\u0430\u043b\u0438\u0442\u0438\u043a\u0430
\u{1f514} \u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0432 Telegram
\u{1f381} 24\u0447 \u0431\u0435\u0441\u043f\u043b\u0430\u0442\u043d\u043e \u043f\u0440\u0438 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u0438

*\u041a\u0430\u043a \u043d\u0430\u0447\u0430\u0442\u044c:*
1\ufe0f\u20e3 \u0423\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435
2\ufe0f\u20e3 \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 \u043a\u043e\u0434 \u0432 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430\u0445
3\ufe0f\u20e3 \u041e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u043a\u043e\u0434 \u0441\u044e\u0434\u0430`;
          
          keyboard = getMainMenuKeyboard();
        }
        
        await sendTelegramMessage(env, chatId, welcomeMsg, {
          reply_markup: keyboard
        });
        
        // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
        await logToChat(env,
          `\u{1f680} *${existingOderId ? '\u0412\u0445\u043e\u0434' : '\u041d\u043e\u0432\u044b\u0439'}*`,
          'user',
          {
            telegramId,
            username: username || '\u043d\u0435\u0442',
            action: '/start',
            details: existingOderId ? '\u041f\u0440\u0438\u0432\u044f\u0437\u0430\u043d' : '\u041d\u043e\u0432\u044b\u0439'
          }
        );
      }
    }
    
    // /status
    else if (update.message?.text === '/status') {
      const chatId = update.message.chat.id;
      const telegramId = String(update.message.from.id);
      
      // \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0443
      const existingOderId = await env.USERS.get(`tg:${telegramId}`);
      
      if (!existingOderId) {
        await sendTelegramMessage(env, chatId, 
          `\u{1f4ca} *\u0421\u0442\u0430\u0442\u0443\u0441*\n\n\u274c \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n\n\u041f\u0440\u0438\u0432\u044f\u0436\u0438\u0442\u0435 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u0447\u0435\u0440\u0435\u0437 \u043a\u043e\u0434`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '\u{1f517} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c', callback_data: 'how_to_connect' }],
              [{ text: ' \u0422\u0430\u0440\u0438\u0444\u044b', callback_data: 'buy' }]
            ]
          }}
        );
        return jsonResponse({ ok: true });
      }
      
      // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e \u0447\u0435\u0440\u0435\u0437 getLicenseData
      const result = await getLicenseData(env, existingOderId);
      
      if (!result.license) {
        await sendTelegramMessage(env, chatId, 
          `\u{1f4ca} *\u0421\u0442\u0430\u0442\u0443\u0441*\n\n\u2705 \u0420\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043e\n\u274c \u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430`,
          { reply_markup: {
            inline_keyboard: [
              [{ text: '\u{1f4b3} \u041a\u0443\u043f\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0443', callback_data: 'buy' }],
              [{ text: '\u{1f4ac} \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430', callback_data: 'support' }]
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
        const typeText = LICENSE_TYPES[license.type]?.name || '\u041f\u0440\u0435\u043c\u0438\u0443\u043c';
        const timeDisplay = license.type === 'trial' ? `${hoursLeft} \u0447.` : `${daysLeft} \u0434\u043d.`;
        
        // \u041f\u0440\u043e\u0433\u0440\u0435\u0441\u0441-\u0431\u0430\u0440 (\u0434\u043b\u044f trial 24\u0447, \u0434\u043b\u044f \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438 30\u0434\u043d)
        const maxTime = license.type === 'trial' ? 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
        const percent = Math.min(100, Math.round((timeLeft / maxTime) * 100));
        const bar = getProgressBar(percent);
        
        message = `\u{1f4ca} *\u0421\u0442\u0430\u0442\u0443\u0441*\n\n`;
        message += `${emoji} ${typeText}\n`;
        message += `${bar} ${percent}%\n\n`;
        message += `\u23f0 ${timeDisplay} \u2022 ${expiresDate}`;
        
        if (daysLeft <= 3 && license.type !== 'trial') {
          message += `\n\n\u26a0\ufe0f _\u0421\u043a\u043e\u0440\u043e \u0438\u0441\u0442\u0435\u0447\u0451\u0442_`;
        }
      } else {
        message = `\u{1f4ca} *\u0421\u0442\u0430\u0442\u0443\u0441*\n\n`;
        message += `\u274c \u0418\u0441\u0442\u0435\u043a\u043b\u0430 ${expiresDate}`;
      }
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: getStatusKeyboard(isValid, license.type)
      });
    }
    
    // /help  
    else if (update.message?.text === '/help') {
      const chatId = update.message.chat.id;
      
      const helpMsg = `\u{1f4d6} *Exotic Assistant v${BOT_VERSION}*

\u{1f5b1}\ufe0f *\u0410\u0432\u0442\u043e\u043a\u043b\u0438\u043a\u0435\u0440*
\u0410\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438 \u0431\u0435\u0440\u0451\u0442 \u0437\u0430\u044f\u0432\u043a\u0438 "\u0412 \u0440\u0430\u0431\u043e\u0442\u0443"

\u{1f4ca} *\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430*
\u041a\u043b\u0438\u043a\u0438 \u0437\u0430 \u0434\u0435\u043d\u044c \u2022 \u0412\u0441\u0435\u0433\u043e \u2022 \u0413\u0440\u0430\u0444\u0438\u043a\u0438

\u{1f514} *\u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f*
Push \u2022 Telegram \u2022 \u0417\u0432\u0443\u043a

\u2699\ufe0f *\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438*
\u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u2022 \u0417\u0430\u0434\u0435\u0440\u0436\u043a\u0430 \u2022 \u0413\u0440\u043e\u043c\u043a\u043e\u0441\u0442\u044c

\u{1f39f}\ufe0f *\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u0430\u044f \u043f\u0440\u043e\u0433\u0440\u0430\u043c\u043c\u0430*
\u041f\u0440\u0438\u0433\u043b\u0430\u0441\u0438 \u0434\u0440\u0443\u0433\u0430 \u2014 \u043f\u043e\u043b\u0443\u0447\u0438 +1 \u0434\u0435\u043d\u044c!`;
      
      await sendTelegramMessage(env, chatId, helpMsg, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: '\u{1f517} \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u044c', callback_data: 'how_to_connect' },
              { text: '\u2753 FAQ', callback_data: 'faq' }
            ],
            [{ text: ' \u0422\u0430\u0440\u0438\u0444\u044b', callback_data: 'buy' }],
            [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
          ]
        }
      });
    }
    
    // /menu
    else if (update.message?.text === '/menu') {
      const chatId = update.message.chat.id;
      
      await sendTelegramMessage(env, chatId, `\u{1f3e0} *\u041c\u0435\u043d\u044e*`, {
        reply_markup: getMainMenuKeyboard()
      });
    }
    
    // /id \u2014 \u043f\u043e\u043a\u0430\u0437\u0430\u0442\u044c ID \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f \u0438 \u0447\u0430\u0442\u0430
    else if (update.message?.text === '/id') {
      const chat = update.message.chat;
      const chatId = chat.id;
      const telegramId = String(update.message.from.id);
      const username = update.message.from.username;
      const firstName = update.message.from.first_name;
      
      let message = `\u{1f194} *\u0418\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044f \u043e\u0431 ID*\n\n`;
      message += `\u{1f464} *\u0412\u044b:*\n`;
      message += `\u251c ID: \`${telegramId}\`\n`;
      if (username) message += `\u251c Username: @${username}\n`;
      if (firstName) message += `\u2514 \u0418\u043c\u044f: ${firstName}\n`;
      
      if (chat.type === 'group' || chat.type === 'supergroup') {
        message += `\n\u{1f4ac} *\u042d\u0442\u043e\u0442 \u0447\u0430\u0442:*\n`;
        message += `\u251c ID: \`${chatId}\`\n`;
        message += `\u251c \u0422\u0438\u043f: ${chat.type === 'supergroup' ? '\u0421\u0443\u043f\u0435\u0440\u0433\u0440\u0443\u043f\u043f\u0430' : '\u0413\u0440\u0443\u043f\u043f\u0430'}\n`;
        if (chat.title) message += `\u2514 \u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435: ${chat.title}\n`;
      } else if (chat.type === 'private') {
        message += `\n\u{1f4ac} *\u0422\u0438\u043f \u0447\u0430\u0442\u0430:* \u041b\u0438\u0447\u043d\u044b\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f`;
      }
      
      await sendTelegramMessage(env, chatId, message, {
        reply_markup: {
          inline_keyboard: [
            [{ text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }]
          ]
        }
      });
    }
    
    // \u041a\u043e\u0434 \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0438 (8 \u0441\u0438\u043c\u0432\u043e\u043b\u043e\u0432)
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
        
        // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c
        await logToChat(env,
          `\u{1f517} *\u041f\u0440\u0438\u0432\u044f\u0437\u0430\u043d*`,
          'connect',
          {
            telegramId,
            username: username || '\u043d\u0435\u0442',
            action: '\u041a\u043e\u0434 \u0442\u0435\u043a\u0441\u0442\u043e\u043c',
            details: `${code.substring(0, 3)}***`
          }
        );
        
        const result = await getLicenseData(env, oderId);
        
        let message = `\u2705 *\u041f\u0440\u0438\u0432\u044f\u0437\u0430\u043d\u043e!*\n\n`;
        
        if (result.license) {
          const timeLeft = result.license.expiresAt - Date.now();
          const hoursLeft = Math.ceil(timeLeft / (60 * 60 * 1000));
          const daysLeft = Math.ceil(timeLeft / (24 * 60 * 60 * 1000));
          
          if (result.license.type === 'trial') {
            message += `\u{1f381} \u041f\u0440\u043e\u0431\u043d\u044b\u0439: *${hoursLeft} \u0447.*`;
          } else {
            message += `\u{1f48e} \u041f\u0440\u0435\u043c\u0438\u0443\u043c: *${daysLeft} \u0434\u043d.*`;
          }
        }
        
        await sendTelegramMessage(env, chatId, message, {
          reply_markup: {
            inline_keyboard: [
              [
                { text: '\u{1f4ca} \u0421\u0442\u0430\u0442\u0443\u0441', callback_data: 'status' },
                { text: '\u2190 \u041c\u0435\u043d\u044e', callback_data: 'menu' }
              ]
            ]
          }
        });
      } else {
        await sendTelegramMessage(env, chatId, 
          `\u274c *\u041a\u043e\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d*\n\n\u041f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 \u043d\u043e\u0432\u044b\u0439 \u0432 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430\u0445`,
          { reply_markup: getMainMenuKeyboard() }
        );
      }
    }
    
    // \u041d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e\u0435
    else if (update.message?.text) {
      const chatId = update.message.chat.id;
      
      await sendTelegramMessage(env, chatId, 
        `\u{1f914} \u041d\u0435 \u043f\u043e\u043d\u0438\u043c\u0430\u044e.\n\n\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0437 \u043c\u0435\u043d\u044e:`,
        { reply_markup: getMainMenuKeyboard() }
      );
    }
    
    return jsonResponse({ ok: true });
    
  } catch (error) {
    console.error('Webhook error:', error);
    
    await logToChat(env,
      `\u{1f6a8} *\u041a\u0440\u0438\u0442\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u043e\u0448\u0438\u0431\u043a\u0430*\n\n` +
      `\u{1f4cd} Path: \`${path}\`\n` +
      `\u274c Error: ${error.message}`,
      'error'
    );
    
    return jsonResponse({ error: 'Internal server error', message: error.message }, 500);
  }
}

export default {
  // Cron handler \u0434\u043b\u044f \u043d\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u043d\u0438\u0439 \u043e\u0431 \u0438\u0441\u0442\u0435\u0447\u0435\u043d\u0438\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438
  async scheduled(event, env, ctx) {
    ctx.waitUntil(checkAndSendExpirationReminders(env));
  },
  
  async fetch(request, env) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const path = url.pathname;
    
    // HTTPS \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 - \u0440\u0435\u0434\u0438\u0440\u0435\u043a\u0442 \u0441 HTTP
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
      // Health check \u044d\u043d\u0434\u043f\u043e\u0438\u043d\u0442 \u0434\u043b\u044f \u043c\u043e\u043d\u0438\u0442\u043e\u0440\u0438\u043d\u0433\u0430
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
      
      // \u041e\u0441\u043d\u043e\u0432\u043d\u044b\u0435 \u044d\u043d\u0434\u043f\u043e\u0438\u043d\u0442\u044b
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
      
      // Telegram \u0431\u043e\u0442 \u044d\u043d\u0434\u043f\u043e\u0438\u043d\u0442\u044b  
      if (path === '/api/telegram/connect' && request.method === 'POST') {
        return await handleTelegramConnect(request, env);
      }
      
      // ========== \u0421\u0418\u041d\u0425\u0420\u041e\u041d\u0418\u0417\u0410\u0426\u0418\u042f \u041d\u0410\u0421\u0422\u0420\u041e\u0415\u041a \u0420\u0410\u0421\u0428\u0418\u0420\u0415\u041d\u0418\u042f ==========
      // \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u0438\u0437 \u0431\u043e\u0442\u0430 \u0434\u043b\u044f \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f
      if (path === '/api/settings/get' && request.method === 'POST') {
        const { data, error } = await safeParseJson(request);
        if (error) return jsonResponse({ error: 'Invalid JSON' }, 400);
        
        const { oderId, telegramId } = data;
        
        if (!oderId && !telegramId) {
          return jsonResponse({ error: 'oderId or telegramId required' }, 400);
        }
        
        let tgId = telegramId;
        
        // \u0415\u0441\u043b\u0438 \u043f\u0435\u0440\u0435\u0434\u0430\u043d oderId, \u043f\u043e\u043b\u0443\u0447\u0430\u0435\u043c telegramId
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
        
        // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u0438\u0437 \u0431\u043e\u0442\u0430
        const botSettings = await getUserSettings(env, tgId);
        
        // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e
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
      
      // \u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f \u0432 \u0431\u043e\u0442
      if (path === '/api/settings/sync' && request.method === 'POST') {
        const { data, error } = await safeParseJson(request);
        if (error) return jsonResponse({ error: 'Invalid JSON' }, 400);
        
        const { oderId, telegramId, settings } = data;
        
        if (!oderId && !telegramId) {
          return jsonResponse({ error: 'oderId or telegramId required' }, 400);
        }
        
        let tgId = telegramId;
        
        // \u0415\u0441\u043b\u0438 \u043f\u0435\u0440\u0435\u0434\u0430\u043d oderId, \u043f\u043e\u043b\u0443\u0447\u0430\u0435\u043c telegramId
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
        
        // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0442\u0435\u043a\u0443\u0449\u0438\u0435 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438
        const currentSettings = await getUserSettings(env, tgId);
        
        // \u041e\u0431\u043d\u043e\u0432\u043b\u044f\u0435\u043c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 (\u0442\u043e\u043b\u044c\u043a\u043e \u0440\u0430\u0437\u0440\u0435\u0448\u0451\u043d\u043d\u044b\u0435 \u043f\u043e\u043b\u044f)
        const updatedSettings = {
          ...currentSettings,
          notifications: {
            ...currentSettings.notifications,
            // \u0421\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0438\u0440\u0443\u0435\u043c \u0442\u043e\u043b\u044c\u043a\u043e \u044d\u0442\u0438 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u0438\u0437 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f
            clicks: settings?.clickNotifications ?? currentSettings.notifications.clicks,
            sound: settings?.soundEnabled ?? currentSettings.notifications.sound
          },
          // \u0414\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u0435 \u0434\u0430\u043d\u043d\u044b\u0435 \u0438\u0437 \u0440\u0430\u0441\u0448\u0438\u0440\u0435\u043d\u0438\u044f
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
      
      // \u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0441\u0442\u0430\u0442\u0443\u0441 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438
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
        
        // \u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u0438 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u044e
        const botSettings = await getUserSettings(env, tgId);
        const licenseData = await env.USERS.get(`license:tg:${tgId}`);
        let license = null;
        if (licenseData) {
          license = JSON.parse(licenseData);
        }
        
        // \u0413\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0435 \u0447\u0430\u0442\u044b
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
      
      // \u0410\u0434\u043c\u0438\u043d\u0441\u043a\u0438\u0435 \u044d\u043d\u0434\u043f\u043e\u0438\u043d\u0442\u044b
      if (path === '/api/admin/grant-license' && request.method === 'POST') {
        return await handleGrantLicense(request, env);
      }
      if (path === '/api/license/info' && request.method === 'POST') {
        return await handleLicenseInfo(request, env);
      }
      
      // Health check \u0441 \u043c\u0435\u0442\u0440\u0438\u043a\u0430\u043c\u0438
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
      
      // \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u044b\u0435 \u043c\u0435\u0442\u0440\u0438\u043a\u0438 (\u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f \u0430\u0434\u043c\u0438\u043d\u0430)
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
      
      // Webhook info \u0434\u043b\u044f \u043e\u0442\u043b\u0430\u0434\u043a\u0438
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
      
      // \u0423\u0441\u0442\u0430\u043d\u043e\u0432\u043a\u0430 webhook
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
          
          // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u043a\u0443 webhook
          await logToChat(env, 
            `\u{1f527} Webhook \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d\n\n` +
            `\u{1f4e1} URL: \`${webhookUrl}\`\n` +
            `\u2705 \u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442: ${data.ok ? '\u0423\u0441\u043f\u0435\u0448\u043d\u043e' : '\u041e\u0448\u0438\u0431\u043a\u0430'}`,
            'system'
          );
          
          return jsonResponse({ ...data, webhookUrl });
        } catch (e) {
          return jsonResponse({ error: e.message }, 500);
        }
      }
      
      // \u0422\u0435\u0441\u0442\u043e\u0432\u044b\u0439 \u043b\u043e\u0433 - \u0434\u043b\u044f \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438 \u0440\u0430\u0431\u043e\u0442\u044b \u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u044f
      if (path === '/api/test-log') {
        await logToChat(env, 
          `\u{1f9ea} *\u0422\u0435\u0441\u0442\u043e\u0432\u043e\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435*\n\n` +
          `\u0415\u0441\u043b\u0438 \u0432\u044b \u0432\u0438\u0434\u0438\u0442\u0435 \u044d\u0442\u043e \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435, \u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442 \u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u043e!`,
          'success',
          { 
            action: '\u0422\u0435\u0441\u0442 \u0441\u0438\u0441\u0442\u0435\u043c\u044b \u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u044f',
            details: `\u0417\u0430\u043f\u0440\u043e\u0441 \u043e\u0442: ${request.headers.get('CF-Connecting-IP') || 'unknown'}`
          }
        );
        recordMetric(path, true, Date.now() - startTime);
        return jsonResponse({ ok: true, message: 'Test log sent to configured chat' });
      }
      
      recordMetric(path, false, Date.now() - startTime, 'Not found');
      return jsonResponse({ error: 'Not found' }, 404);
      
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // \u0421\u0442\u0440\u0443\u043a\u0442\u0443\u0440\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u043e\u0435 \u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u043e\u0448\u0438\u0431\u043a\u0438
      structuredLog('error', error.message, {
        path,
        duration,
        stack: error.stack?.substring(0, 500),
        ip: request.headers.get('CF-Connecting-IP')
      });
      
      // \u0417\u0430\u043f\u0438\u0441\u044b\u0432\u0430\u0435\u043c \u043c\u0435\u0442\u0440\u0438\u043a\u0443
      recordMetric(path, false, duration, error.message);
      
      // \u041b\u043e\u0433\u0438\u0440\u0443\u0435\u043c \u043a\u0440\u0438\u0442\u0438\u0447\u0435\u0441\u043a\u0438\u0435 \u043e\u0448\u0438\u0431\u043a\u0438 \u0432 Telegram
      await logToChat(env,
        `\u{1f6a8} *\u041a\u0440\u0438\u0442\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u043e\u0448\u0438\u0431\u043a\u0430*\n\n` +
        `\u{1f4cd} Path: \`${path}\`\n` +
        `\u274c Error: ${error.message}`,
        'error'
      );
      
      return jsonResponse({ error: 'Internal server error', message: error.message }, 500);
    }
  }
};

