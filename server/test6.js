export default {};
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