import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import './index.css'

// This app does not use a service worker. If a stale one is registered on
// this origin (e.g. left over from a previous app served on localhost),
// it intercepts fetches and throws "Failed to fetch" (sw.js). Unregister
// any such service worker and drop its caches so it can't interfere.
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.getRegistrations()
    .then((regs) => regs.forEach((r) => r.unregister()))
    .catch(() => { /* nothing to clean up */ })
  if (window.caches) {
    caches.keys().then((keys) => keys.forEach((k) => caches.delete(k))).catch(() => {})
  }
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
