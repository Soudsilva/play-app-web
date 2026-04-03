const CACHE = 'play-v3';

// Arquivos essenciais do app — disponíveis sem internet
const SHELL = [
    '/index.html',
    '/login.html',
    '/atendimento_nivel_1.html',
    '/clientes.html',
    '/pedidos.html',
    '/auth.js',
    '/database.js',
    '/offline-sync.js',
    '/assets/js/play-dialogs.js',
    '/assets/img/logo.png',
    '/assets/img/logomenor.png',
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
    const url = e.request.url;

    // Firebase, CDN externo e WebSockets passam direto — não interceptar
    if (url.includes('firebase') || url.includes('googleapis') ||
        url.includes('gstatic')  || url.includes('flaticon')) {
        return;
    }

    e.respondWith(
        fetch(e.request)
            .then(response => {
                // Atualiza o cache se for do próprio domínio
                if (response.ok && url.startsWith(self.location.origin)) {
                    const clone = response.clone();
                    caches.open(CACHE).then(cache => cache.put(e.request, clone));
                }
                return response;
            })
            .catch(() =>
                // Sem internet: serve do cache, ou index.html como fallback final
                caches.match(e.request).then(cached => cached || caches.match('/index.html'))
            )
    );
});
