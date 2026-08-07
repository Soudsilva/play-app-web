(function protegerAcessoPwa() {
    'use strict';

    function estaEmModoAplicativo() {
        const displayStandalone = window.matchMedia('(display-mode: standalone)').matches;
        const displayWindowControls = window.matchMedia('(display-mode: window-controls-overlay)').matches;
        const iosStandalone = navigator.standalone === true;

        return displayStandalone || displayWindowControls || iosStandalone;
    }

    window.PlayPwaAccess = Object.freeze({
        isRunningAsApp: estaEmModoAplicativo
    });

    if (!estaEmModoAplicativo()) {
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
