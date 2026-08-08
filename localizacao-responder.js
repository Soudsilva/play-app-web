import {
    escutarSolicitacaoDoUsuario,
    estaEmPreviaLocal,
    marcarPermissaoLocalizacaoNegada,
    responderSolicitacaoComLocalizacao
} from './localizacao-service.js';
import { registrarIdentidadeSegura } from './permissoes-service.js';

let usuarioAtivoUid = '';
let solicitacaoAtual = null;
let solicitacaoEmProcessamento = '';
let cancelarSolicitacao = null;
let eventosConectados = false;

function solicitarPosicao(opcoes) {
    return new Promise((resolve, reject) => {
        if (!navigator.geolocation) {
            reject({ code: 2, message: 'Geolocalização indisponível.' });
            return;
        }
        navigator.geolocation.getCurrentPosition(resolve, reject, opcoes);
    });
}

async function obterPosicaoAtual() {
    try {
        return await solicitarPosicao({
            enableHighAccuracy: true,
            timeout: 8000,
            maximumAge: 60000
        });
    } catch (erroAltaPrecisao) {
        if (Number(erroAltaPrecisao?.code) === 1) throw erroAltaPrecisao;
        return solicitarPosicao({
            enableHighAccuracy: false,
            timeout: 8000,
            maximumAge: 120000
        });
    }
}

async function tentarResponderSolicitacao() {
    const pedido = solicitacaoAtual;
    const usuarioUid = String(usuarioAtivoUid || '').trim();
    if (!pedido || pedido.status !== 'pendente' || !pedido.id || !usuarioUid) return;
    if (!navigator.onLine || solicitacaoEmProcessamento) return;

    solicitacaoEmProcessamento = pedido.id;
    try {
        const posicao = await obterPosicaoAtual();
        await responderSolicitacaoComLocalizacao({
            usuarioUid,
            solicitacao: pedido,
            posicao
        });
    } catch (error) {
        if (Number(error?.code) === 1) {
            await marcarPermissaoLocalizacaoNegada({
                usuarioUid,
                solicitacaoId: pedido.id
            }).catch(() => {});
        }
    } finally {
        solicitacaoEmProcessamento = '';
        if (solicitacaoAtual?.status === 'pendente' && solicitacaoAtual.id !== pedido.id) {
            tentarResponderSolicitacao();
        }
    }
}

function conectarEventosDeRetomada() {
    if (eventosConectados) return;
    eventosConectados = true;
    window.addEventListener('online', tentarResponderSolicitacao);
    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') tentarResponderSolicitacao();
    });
    window.addEventListener('pagehide', () => {
        if (typeof cancelarSolicitacao === 'function') cancelarSolicitacao();
        cancelarSolicitacao = null;
    }, { once: true });
}

export async function iniciarRespondedorLocalizacao(usuario) {
    if (!usuario?.uid || estaEmPreviaLocal()) return;
    if (usuarioAtivoUid === usuario.uid && typeof cancelarSolicitacao === 'function') return;

    if (typeof cancelarSolicitacao === 'function') cancelarSolicitacao();
    cancelarSolicitacao = null;
    usuarioAtivoUid = usuario.uid;

    await registrarIdentidadeSegura(usuario).catch(() => {});
    conectarEventosDeRetomada();
    cancelarSolicitacao = escutarSolicitacaoDoUsuario(
        usuario.uid,
        (solicitacao) => {
            solicitacaoAtual = solicitacao;
            tentarResponderSolicitacao();
        },
        () => {}
    );
}
