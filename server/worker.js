


const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
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

function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 8; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}

async function handleLicenseCheck(request, env) {
  const { oderId, deviceId } = await request.json();
  if (!oderId) {
    return jsonResponse({ valid: false, error: 'oderId is required' });
  }
  // Получаем данные пользователя
  const userData = await env.USERS.get(`user:${oderId}`);
  if (!userData) {
    return jsonResponse({ valid: false, error: 'NOT_CONNECTED', message: 'Пользователь не найден' });
  }
  // ...дополнительная логика проверки лицензии, если нужно...
  return jsonResponse({ valid: true });
}

async function handleGenerateCode(request, env) {
  const { oderId, deviceId } = await request.json();
  if (!oderId) {
    return jsonResponse({ error: 'oderId is required' }, 400);
  }
  const code = generateCode();
  await env.USERS.put(`code:${code}`, JSON.stringify({ oderId, deviceId }), {
    expirationTtl: 600
  });
  return jsonResponse({ 
    success: true, 
    code,
    expiresIn: 600
  });
}

async function handleStatus(request, env) {
  const { oderId } = await request.json();
  if (!oderId) {
    return jsonResponse({ error: 'oderId is required' }, 400);
  }
  const userData = await env.USERS.get(`user:${oderId}`);
  if (!userData) {
    return jsonResponse({ connected: false });
  }
    const { username } = JSON.parse(userData);
    const license = await env.USERS.get(`license:${username}`);
  if (!license) {
    return jsonResponse({ connected: true, username, licensed: false });
  }
  const licenseData = JSON.parse(license);
  const isValid = licenseData.expiresAt > Date.now();
  return jsonResponse({ 
    connected: true, 
    username,
    licensed: isValid,
    licenseType: licenseData.type,
    expiresAt: licenseData.expiresAt,
    daysLeft: Math.ceil((licenseData.expiresAt - Date.now()) / (24 * 60 * 60 * 1000))
  });
}

async function handleNotify(request, env) {
  const { oderId, transaction } = await request.json();
  if (!oderId) {
    return jsonResponse({ error: 'oderId is required' }, 400);
  }
  const userData = await env.USERS.get(`user:${oderId}`);
  if (!userData) {
    return jsonResponse({ error: 'User not connected', code: 'NOT_CONNECTED' }, 404);
  }
    const { username } = JSON.parse(userData);
    const license = await env.USERS.get(`license:${username}`);
  if (!license) {
    return jsonResponse({ error: 'No license', code: 'NO_LICENSE' }, 403);
  }
  const licenseData = JSON.parse(license);
  if (licenseData.expiresAt < Date.now()) {
    return jsonResponse({ error: 'License expired', code: 'EXPIRED' }, 403);
  }
  // Формируем сообщение
  let message = `✅ *Заявка взята в работу!*\n\n`;
  if (transaction) {
    if (transaction.transactionId) {
      message += `🆔 *ID:* \`${transaction.transactionId}\`\n`;
    }
    if (transaction.amount) {
      message += `💰 *Сумма:* ${transaction.amount}\n`;
    }
    if (transaction.method) {
      message += `💳 *Способ:* ${transaction.method}\n`;
    }
    if (transaction.created) {
      message += `📅 *Создана:* ${transaction.created}\n`;
    }
    if (transaction.requisites) {
      message += `🔢 *Реквизиты:* \`${transaction.requisites}\`\n`;
    }
    if (transaction.bank) {
      message += `🏦 *Банк:* ${transaction.bank}\n`;
    }
  }
  const time = new Date().toLocaleString('ru-RU', { 
    timeZone: 'Europe/Moscow',
    hour: '2-digit', 
    minute: '2-digit',
    second: '2-digit'
  });
  message += `\n⏰ *Время:* ${time} (МСК)`;
    // Здесь могла бы быть отправка уведомления
  return jsonResponse({ success: true, message });
}

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }
    const url = new URL(request.url);
    const path = url.pathname;
    try {
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
      return jsonResponse({ error: 'Not found' }, 404);
    } catch (error) {
      console.error('Error:', error);
      return jsonResponse({ error: 'Internal server error' }, 500);
    }
  }
};
