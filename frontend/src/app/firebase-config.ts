export const firebaseConfig = {
  apiKey: 'AIzaSyBuWP-Rl-P2C7HUL2TNeC1wI8zHPsKkbqo',
  authDomain: 'aamdb-840c9.firebaseapp.com',
  databaseURL: 'https://aamdb-840c9-default-rtdb.firebaseio.com',
  projectId: 'aamdb-840c9',
  storageBucket: 'aamdb-840c9.firebasestorage.app',
  messagingSenderId: '462538425059',
  appId: '1:462538425059:web:7147b0a3ddbe7c55b07bd6',
  measurementId: 'G-YNLX3HD77Z',
};

export const firebaseVapidKey = 'BDhVeagkCrLX2MXsnWeByHJ6BhW9-Q4nJnkH3AI6G79nOvGGacfTolvCG9hStjYmQpaPIloPkzdDoK0ezo5CjqU';

export function hasFirebaseMessagingConfig() {
  return !Object.values(firebaseConfig).some((value) => value.startsWith('PASTE_'))
    && !firebaseVapidKey.startsWith('PASTE_');
}
