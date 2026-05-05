const CACHE = 'play-v6';

// Arquivos essenciais do próprio app
const SHELL = [
    '/index.html',
    '/login.html',
    '/atendimento_nivel_1.html',
    '/clientes.html',
    '/pedidos.html',
    '/verificar_envios.html',
    '/media_de_vendas.html',
    '/auth.js',
    '/database.js',
    '/offline-sync.js',
    '/assets/js/play-dialogs.js',
    '/assets/img/logo.png',
    '/assets/img/logomenor.png',
];

// Arquivos do Firebase SDK (CDN) — necessários para o app funcionar offline
const FIREBASE_SDK = [
    'https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js',
    'https://www.gstatic.com/firebasejs/10.8.0/firebase-auth.js',
    'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js',
    'https://www.gstatic.com/firebasejs/10.8.0/firebase-storage.js',
];

self.addEventListener('install', (e) => {
    e.waitUntil(
        caches.open(CACHE).then(cache =>
            // SHELL com addAll (falha se algum falhar) + SDK com add individual (tolerante a falhas)
            cache.addAll(SHELL).then(() =>
                Promise.allSettled(FIREBASE_SDK.map(url => cache.add(url)))
            )
        )
    );
    self.skipWaiting();
});

self.addEventListener('activate', (e) => {
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
        )
    );
    self.clients.claim();
});

self.addEventListener('fetch', (e) => {
    const url = e.request.url;

    // Chamadas de API do Firebase (dados em tempo real, auth tokens, storage uploads)
    // NÃO devem ser cacheadas — passam direto para a rede
    if (url.includes('firebaseio.com') ||
        url.includes('firebasestorage.googleapis.com') ||
        url.includes('googleapis.com') ||
        url.includes('flaticon.com')) {
        return;
    }

    // Todo o resto (próprio app + Firebase SDK do gstatic.com):
    // rede primeiro, cache como fallback
    e.respondWith(
        fetch(e.request)
            .then(response => {
                if (response.ok) {
                    const clone = response.clone();
                    caches.open(CACHE).then(cache => cache.put(e.request, clone));
                }
                return response;
            })
            .catch(() =>
                caches.match(e.request).then(cached => cached || caches.match('/index.html'))
            )
    );
});
