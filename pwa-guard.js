(function protegerAcessoPwa() {
    'use strict';

    function estaEmModoAplicativo() {
        const displayStandalone = window.matchMedia('(display-mode: standalone)').matches;
        const displayWindowControls = window.matchMedia('(display-mode: window-controls-overlay)').matches;
        const iosStandalone = navigator.standalone === true;

        return displayStandalone || displayWindowControls || iosStandalone;
    }

    function estaEmAmbienteLocal() {
        const hostname = window.location.hostname;
        const hostLocal = ['localhost', '127.0.0.1', '[::1]'].includes(hostname);
        const ipv4Privado = /^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(hostname);
        const portaGoLive = window.location.protocol === 'http:' && window.location.port === '5500';

        return hostLocal || (ipv4Privado && portaGoLive);
    }

    window.PlayPwaAccess = Object.freeze({
        isRunningAsApp: estaEmModoAplicativo
    });

    if (!estaEmModoAplicativo() && !estaEmAmbienteLocal()) {
        document.documentElement.style.visibility = 'hidden';
        window.location.replace('/instalar.html');
        return;
    }

    document.documentElement.dataset.pwaAccess = 'allowed';

    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.register('/sw.js').catch((error) => {
            console.warn('Não foi possível registrar o service worker.', error);
        });
    }
}());
