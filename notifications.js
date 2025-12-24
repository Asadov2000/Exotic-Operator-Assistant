class NotificationManager {
  constructor() {
    this.permission = null;
    this.notifications = [];
    this.init();
  }

  async init() {
    await this.checkPermission();
    await this.loadNotifications();
  }

  async checkPermission() {
    if (typeof Notification === 'undefined') return false;
    if (Notification.permission === 'default') {
      this.permission = await Notification.requestPermission();
    } else {
      this.permission = Notification.permission;
    }
    return this.permission === 'granted';
  }

  async loadNotifications() {
    const data = await chrome.storage.local.get(['notifications']);
    this.notifications = data.notifications || [];
  }

  async saveNotifications() {
    await chrome.storage.local.set({ notifications: this.notifications });
  }

  async sendNotification(title, message, options = {}) {
    if (this.permission !== 'granted') {
      const granted = await this.checkPermission();
      if (!granted) return null;
    }

    const notification = {
      id: `exotic_${Date.now()}`,
      title,
      message,
      timestamp: Date.now(),
      read: false,
      ...options
    };

    if (chrome.notifications) {
      await chrome.notifications.create(notification.id, {
        type: 'basic',
        iconUrl: 'icons/icon48.png',
        title: notification.title,
        message: notification.message,
        priority: 2,
        requireInteraction: notification.requireInteraction || false
      });
    }

    this.notifications.push(notification);
    if (this.notifications.length > 100) {
      this.notifications.splice(0, this.notifications.length - 100);
    }

    await this.saveNotifications();
    return notification.id;
  }

  async markAsRead(notificationId) {
    const notification = this.notifications.find(n => n.id === notificationId);
    if (notification) {
      notification.read = true;
      await this.saveNotifications();
    }
  }

  async markAllAsRead() {
    this.notifications.forEach(n => n.read = true);
    await this.saveNotifications();
  }

  async deleteNotification(notificationId) {
    this.notifications = this.notifications.filter(n => n.id !== notificationId);
    await this.saveNotifications();
  }

  async deleteAllNotifications() {
    this.notifications = [];
    await this.saveNotifications();
  }

  getUnreadCount() {
    return this.notifications.filter(n => !n.read).length;
  }

  getRecentNotifications(limit = 10) {
    return this.notifications
      .sort((a, b) => b.timestamp - a.timestamp)
      .slice(0, limit);
  }
}

const notificationManager = new NotificationManager();