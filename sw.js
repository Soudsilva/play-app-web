const CACHE = 'play-v54';

// Arquivos essenciais do próprio app
const SHELL = [
    '/index.html',
    '/login.html',
    '/instalar.html',
    '/permissoes-service.js',
    '/permissoes-rules.js',
    '/atendimento_nivel_1.html',
    '/clientes.html',
    '/pedidos.html',
    '/verificar_envios.html',
    '/media_de_vendas.html',
    '/importar_cobrancas_dataverse.html',
    '/configuracoes_automaticas.html',
    '/arquivos_para_impressao.html',
    '/arquivos-impressao.css?v=48',
    '/arquivos-impressao.js?v=48',
    '/arquivos-impressao-service.js?v=48',
    '/arquivos-impressao-rules.js?v=48',
    '/arquivos-impressao-thumbnail.js?v=48',
    '/arquivos-impressao-order.js?v=48',
    '/auth.js',
    '/pwa-guard.js',
    '/pwa-install.js',
    '/database.js',
    '/firebase-app.js',
    '/maquina-posse-rules.mjs',
    '/maquina-posse-service.js',
    '/pix-posse-rules.mjs',
    '/pix-posse-service.js',
    '/offline-sync.js',
    '/atendimento-rascunho-service.js',
    '/atendimento-contador-rules.mjs',
    '/atendimento-contador-service.js',
    '/atendimento-fila-sync.mjs',
    '/atendimento-sync-confirmacao-service.js',
    '/atendimento-sync-upload-service.js',
    '/atendimento-sync-runner.js',
    '/verificar-envios-sync-rules.mjs',
    '/assets/js/play-dialogs.js',
    '/assets/img/logo.png',
    '/assets/img/logomenor-192.png',
    '/assets/img/logomenor.png',
    '/manifest.json',
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

    // O gerador de miniaturas é pesado e carregado somente sob demanda.
    // Depois do primeiro uso, reutiliza a cópia local sem novo consumo de dados.
    if (url.includes('/assets/vendor/pdfjs/')) {
        e.respondWith(
            caches.match(e.request).then(cached => cached || fetch(e.request).then(response => {
                if (response.ok) {
                    const clone = response.clone();
                    caches.open(CACHE).then(cache => cache.put(e.request, clone));
                }
                return response;
            }))
        );
        return;
    }

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
    const ehArquivoDoApp = new URL(e.request.url).origin === self.location.origin;

    e.respondWith(
        fetch(e.request, ehArquivoDoApp ? { cache: 'no-cache' } : undefined)
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
