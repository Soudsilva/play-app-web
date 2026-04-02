const CACHE = 'play-v1';

// Arquivos do shell do app que ficam em cache
const SHELL = [
    '/login.html',
    '/index.html',
    '/auth.js',
    '/database.js',
    '/assets/img/logo.png',
    '/assets/js/play-dialogs.js'
];

// Instala e faz cache dos arquivos do shell
self.addEventListener('install', (e) => {
    e.waitUntil(
        caches.open(CACHE).then(cache => cache.addAll(SHELL))
    );
    self.skipWaiting();
});

// Remove caches antigos ao ativar nova versão
self.addEventListener('activate', (e) => {
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
        )
    );
    self.clients.claim();
});

// Estratégia: rede primeiro, cache como fallback
self.addEventListener('fetch', (e) => {
    // Deixa Firebase e CDN externo passarem direto (sem interceptar)
    const url = e.request.url;
    if (url.includes('firebase') || url.includes('googleapis') || url.includes('gstatic') || url.includes('flaticon')) {
        return;
    }

    e.respondWith(
        fetch(e.request)
            .then(response => {
                // Guarda no cache se for do próprio domínio
                if (response.ok && url.startsWith(self.location.origin)) {
                    const clone = response.clone();
                    caches.open(CACHE).then(cache => cache.put(e.request, clone));
                }
                return response;
            })
            .catch(() =>
                caches.match(e.request).then(cached => cached || caches.match('/login.html'))
            )
    );
});
