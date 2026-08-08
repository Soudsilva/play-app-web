import {
    equalTo,
    get,
    limitToFirst,
    onValue,
    orderByChild,
    orderByKey,
    push,
    query,
    ref,
    runTransaction,
    serverTimestamp,
    set,
    startAfter
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import { db, localizacaoDb } from './firebase-app.js';
import {
    aplicarRespostaPendente,
    classificarPrecisao,
    coordenadasValidas,
    idFirebaseValido,
    textoNormalizado
} from './localizacao-rules.js';

const LOCALIZACAO_ROOT = 'modulos/localizacao';
const COLABORADORES_ROOT = 'colaboradores';

export function estaEmPreviaLocal() {
    const hostname = window.location.hostname;
    const hostLocal = ['localhost', '127.0.0.1', '[::1]'].includes(hostname);
    const ipv4Privado = /^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(hostname);
    return hostLocal || ipv4Privado;
}

function garantirEscritaPermitida() {
    if (estaEmPreviaLocal()) {
        throw new Error('A gravação de localização está desativada na prévia local.');
    }
}

function validarUid(uid) {
    const valor = String(uid || '').trim();
    if (!idFirebaseValido(valor)) throw new Error('Usuário autenticado inválido.');
    return valor;
}

export async function listarColaboradoresPagina({ cursor = '', limite = 12 } = {}) {
    const quantidade = Math.min(30, Math.max(1, Number(limite) || 12));
    const base = ref(db, COLABORADORES_ROOT);
    const consulta = cursor
        ? query(base, orderByKey(), startAfter(String(cursor)), limitToFirst(quantidade))
        : query(base, orderByKey(), limitToFirst(quantidade));
    const snapshot = await get(consulta);
    const dados = snapshot.val() || {};
    const itens = Object.entries(dados).map(([firebaseUrl, colaborador]) => ({
        firebaseUrl,
        ...(colaborador || {})
    }));
    return {
        itens,
        proximoCursor: itens.length === quantidade ? itens[itens.length - 1].firebaseUrl : '',
        temMais: itens.length === quantidade
    };
}

export async function buscarColaboradorPorNome(nome) {
    const nomeLimpo = String(nome || '').trim();
    if (!nomeLimpo) return null;
    const consulta = query(
        ref(db, COLABORADORES_ROOT),
        orderByChild('nome'),
        equalTo(nomeLimpo),
        limitToFirst(2)
    );
    const snapshot = await get(consulta);
    const dados = snapshot.val() || {};
    const entradaExata = Object.entries(dados).find(([, colaborador]) =>
        textoNormalizado(colaborador?.nome) === textoNormalizado(nomeLimpo));
    return entradaExata
        ? { firebaseUrl: entradaExata[0], ...(entradaExata[1] || {}) }
        : null;
}

export function escutarSolicitacaoDoUsuario(uid, callback, onError) {
    const usuarioUid = validarUid(uid);
    return onValue(
        ref(localizacaoDb, `${LOCALIZACAO_ROOT}/por_usuario/${usuarioUid}/solicitacao_atual`),
        (snapshot) => callback(snapshot.val() || null),
        (error) => onError?.(error)
    );
}

export function escutarLocalizacoesDoUsuario(uid, callback, onError) {
    const usuarioUid = validarUid(uid);
    return onValue(
        ref(localizacaoDb, `${LOCALIZACAO_ROOT}/por_usuario/${usuarioUid}`),
        (snapshot) => callback(snapshot.val() || {}),
        (error) => onError?.(error)
    );
}

export async function solicitarLocalizacao({ colaborador, gestor }) {
    garantirEscritaPermitida();
    const usuarioUid = validarUid(colaborador?.localizacaoUid);
    const colaboradorId = String(colaborador?.firebaseUrl || '').trim();
    if (!idFirebaseValido(colaboradorId)) throw new Error('Colaborador inválido.');
    const solicitacaoRef = ref(
        localizacaoDb,
        `${LOCALIZACAO_ROOT}/por_usuario/${usuarioUid}/solicitacao_atual`
    );
    const solicitacaoId = push(solicitacaoRef).key;
    if (!solicitacaoId) throw new Error('Não foi possível criar a solicitação.');

    const registro = {
        id: solicitacaoId,
        status: 'pendente',
        usuarioUid,
        colaboradorId,
        colaboradorNome: String(colaborador?.nome || '').trim(),
        solicitadoPorUid: String(gestor?.uid || '').trim(),
        solicitadoPorNome: String(gestor?.displayName || '').trim(),
        solicitadoEmCliente: Date.now(),
        solicitadoEmServidor: serverTimestamp()
    };
    await set(solicitacaoRef, registro);
    return registro;
}

export async function responderSolicitacaoComLocalizacao({ usuarioUid, solicitacao, posicao }) {
    garantirEscritaPermitida();
    const uid = validarUid(usuarioUid);
    const solicitacaoId = String(solicitacao?.id || '').trim();
    const latitude = Number(posicao?.coords?.latitude);
    const longitude = Number(posicao?.coords?.longitude);
    const precisaoMetros = Number(posicao?.coords?.accuracy);
    if (!idFirebaseValido(solicitacaoId)
        || !coordenadasValidas(latitude, longitude, precisaoMetros)) {
        throw new Error('Localização inválida.');
    }

    const recebidaEmCliente = Date.now();
    const registro = {
        solicitacaoId,
        usuarioUid: uid,
        colaboradorId: String(solicitacao?.colaboradorId || '').trim(),
        colaboradorNome: String(solicitacao?.colaboradorNome || '').trim(),
        latitude,
        longitude,
        precisaoMetros: Math.round(precisaoMetros),
        qualidade: classificarPrecisao(precisaoMetros),
        origem: 'dispositivo',
        capturadaEm: Number(posicao?.timestamp || recebidaEmCliente),
        recebidaEmCliente,
        recebidaEmServidor: serverTimestamp()
    };

    const caminho = `${LOCALIZACAO_ROOT}/por_usuario/${uid}`;
    const resultado = await runTransaction(ref(localizacaoDb, caminho), (atual) => {
        return aplicarRespostaPendente(atual, solicitacaoId, registro, {
            respondidaEmCliente: recebidaEmCliente,
            respondidaEmServidor: serverTimestamp()
        });
    });
    return resultado.committed;
}

export async function marcarPermissaoLocalizacaoNegada({ usuarioUid, solicitacaoId }) {
    garantirEscritaPermitida();
    const uid = validarUid(usuarioUid);
    const pedidoId = String(solicitacaoId || '').trim();
    if (!idFirebaseValido(pedidoId)) return false;
    const caminho = `${LOCALIZACAO_ROOT}/por_usuario/${uid}/solicitacao_atual`;
    const resultado = await runTransaction(ref(localizacaoDb, caminho), (pedidoAtual) => {
        if (!pedidoAtual || pedidoAtual.id !== pedidoId || pedidoAtual.status !== 'pendente') return;
        return {
            ...pedidoAtual,
            status: 'permissao_negada',
            respondidaEmCliente: Date.now(),
            respondidaEmServidor: serverTimestamp()
        };
    });
    return resultado.committed;
}
