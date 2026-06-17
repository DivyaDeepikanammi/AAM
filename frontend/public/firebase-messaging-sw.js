importScripts('https://www.gstatic.com/firebasejs/12.15.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/12.15.0/firebase-messaging-compat.js');

firebase.initializeApp({
  apiKey: 'AIzaSyBuWP-Rl-P2C7HUL2TNeC1wI8zHPsKkbqo',
  authDomain: 'aamdb-840c9.firebaseapp.com',
  databaseURL: 'https://aamdb-840c9-default-rtdb.firebaseio.com',
  projectId: 'aamdb-840c9',
  storageBucket: 'aamdb-840c9.firebasestorage.app',
  messagingSenderId: '462538425059',
  appId: '1:462538425059:web:7147b0a3ddbe7c55b07bd6',
  measurementId: 'G-YNLX3HD77Z',
});

const messaging = firebase.messaging();

messaging.onBackgroundMessage((payload) => {
  const notification = payload.notification || {};
  self.registration.showNotification(notification.title || 'AAM reminder', {
    body: notification.body || 'Please check your meal or medicine reminder.',
    icon: '/icons/icon-192x192.png',
    badge: '/icons/icon-192x192.png',
    data: payload.data || {},
  });
});
