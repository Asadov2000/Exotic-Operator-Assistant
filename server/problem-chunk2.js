export default {};
async function test(env, chatId, messageId) {
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
}
