// Service Worker for PWA
const CACHE_NAME = 'sunset-bot-v6';

// 只缓存 manifest.json，不缓存 HTML（HTML 需要动态 token）
const PRECACHE_URLS = [
  '/manifest.json'
];

// 安装时预缓存
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(PRECACHE_URLS))
      .then(() => self.skipWaiting())
  );
});

// 激活时清理旧缓存
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((cacheName) => {
          if (cacheName !== CACHE_NAME) {
            return caches.delete(cacheName);
          }
        })
      );
    }).then(() => self.clients.claim())
  );
});

// 请求拦截
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // HTML 页面请求：始终网络，不缓存（含动态 token）
  if (event.request.headers.get('accept') === 'text/html' || url.pathname === '/') {
    return;
  }

  // API 请求：始终网络，不缓存
  if (url.pathname.startsWith('/api/')) {
    return;
  }

  // SSE 请求：不缓存，不拦截
  if (event.request.headers.get('accept') === 'text/event-stream') {
    return;
  }

  // 其他静态资源：网络优先 + 缓存回退
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response.status === 200) {
          const responseClone = response.clone();
          caches.open(CACHE_NAME).then((cache) => {
            cache.put(event.request, responseClone);
          });
        }
        return response;
      })
      .catch(() => {
        return caches.match(event.request);
      })
  );
});
