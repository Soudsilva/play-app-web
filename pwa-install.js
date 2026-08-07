(function prepararInstalacaoPwa() {
    'use strict';

    let eventoDeInstalacao = null;
    let instalacaoConfirmada = false;
    let interfacePronta = false;
    let verificacaoConcluida = false;

    function estaEmModoAplicativo() {
        const displayStandalone = window.matchMedia('(display-mode: standalone)').matches;
        const displayWindowControls = window.matchMedia('(display-mode: window-controls-overlay)').matches;
        const iosStandalone = navigator.standalone === true;

        return displayStandalone || displayWindowControls || iosStandalone;
    }

    function dispositivoAppleMovel() {
        const userAgentApple = /iphone|ipad|ipod/i.test(navigator.userAgent);
        const ipadComUserAgentDesktop = navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1;
        return userAgentApple || ipadComUserAgentDesktop;
    }

    function elementos() {
        return {
            botao: document.getElementById('installButton'),
            aguardando: document.getElementById('waitingInstructions'),
            ios: document.getElementById('iosInstructions'),
            alternativa: document.getElementById('manualInstructions'),
            mensagem: document.getElementById('installMessage')
        };
    }

    function atualizarInterface() {
        if (!interfacePronta) return;

        const ui = elementos();
        const isApple = dispositivoAppleMovel();

        ui.botao.hidden = instalacaoConfirmada || isApple || !eventoDeInstalacao;
        ui.aguardando.hidden = instalacaoConfirmada || isApple || Boolean(eventoDeInstalacao) || verificacaoConcluida;
        ui.ios.hidden = instalacaoConfirmada || !isApple;
        ui.alternativa.hidden = instalacaoConfirmada || isApple || Boolean(eventoDeInstalacao) || !verificacaoConcluida;

        if (instalacaoConfirmada) {
            ui.mensagem.hidden = false;
            ui.mensagem.textContent = 'Aplicativo instalado. Feche esta página e abra o aplicativo pelo ícone criado no seu dispositivo.';
        }
    }

    async function solicitarInstalacao() {
        if (!eventoDeInstalacao || instalacaoConfirmada) return;

        const ui = elementos();
        const promptDisponivel = eventoDeInstalacao;
        eventoDeInstalacao = null;
        ui.botao.disabled = true;
        ui.botao.textContent = 'Abrindo instalação...';

        try {
            await promptDisponivel.prompt();
            const escolha = await promptDisponivel.userChoice;

            if (escolha.outcome === 'accepted') {
                ui.mensagem.hidden = false;
                ui.mensagem.textContent = 'Instalação solicitada. Aguarde a confirmação do navegador.';
            } else {
                ui.mensagem.hidden = false;
                ui.mensagem.textContent = 'Instalação cancelada. O sistema continua bloqueado no navegador.';
            }
        } catch (error) {
            ui.mensagem.hidden = false;
            ui.mensagem.textContent = 'Não foi possível abrir a instalação. Use a opção de instalar no menu do navegador.';
            console.warn('Falha ao solicitar a instalação do aplicativo.', error);
        } finally {
            ui.botao.disabled = false;
            ui.botao.textContent = 'Clique aqui para instalar o aplicativo';
            atualizarInterface();
        }
    }

    if (estaEmModoAplicativo()) {
        window.location.replace('/index.html');
        return;
    }

    window.addEventListener('beforeinstallprompt', (event) => {
        event.preventDefault();
        eventoDeInstalacao = event;
        verificacaoConcluida = true;
        atualizarInterface();
    });

    window.addEventListener('appinstalled', () => {
        instalacaoConfirmada = true;
        eventoDeInstalacao = null;
        atualizarInterface();
    });

    document.addEventListener('DOMContentLoaded', () => {
        interfacePronta = true;
        elementos().botao.addEventListener('click', solicitarInstalacao);
        atualizarInterface();

        window.setTimeout(() => {
            verificacaoConcluida = true;
            atualizarInterface();
        }, 1200);

        if ('serviceWorker' in navigator) {
            navigator.serviceWorker.register('/sw.js').catch((error) => {
                console.warn('Não foi possível registrar o service worker.', error);
            });
        }
    });
}());
