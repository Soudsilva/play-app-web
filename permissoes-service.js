import {
    equalTo,
    get,
    limitToFirst,
    orderByChild,
    query,
    ref,
    serverTimestamp,
    set,
    update
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import { dadosSegurosDb } from './firebase-app.js';
import { idFirebaseValido, perfilPodeGerenciarLocalizacao } from './localizacao-rules.js';

const USUARIOS_ROOT = 'usuarios';
const PERMISSOES_ROOT = 'permissoes';

function validarUid(uid) {
    const valor = String(uid || '').trim();
    if (!idFirebaseValido(valor)) throw new Error('Usuário autenticado inválido.');
    return valor;
}

function garantirEscritaRemota() {
    const hostname = window.location.hostname;
    const hostLocal = ['localhost', '127.0.0.1', '[::1]'].includes(hostname);
    const ipv4Privado = /^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(hostname);
    if (hostLocal || ipv4Privado) {
        throw new Error('A gravação de permissões está desativada na prévia local.');
    }
}

export async function buscarUsuarioSeguroPorEmail(email) {
    const valor = String(email || '').trim().toLowerCase();
    if (!valor) return null;
    const consulta = query(
        ref(dadosSegurosDb, USUARIOS_ROOT),
        orderByChild('email'),
        equalTo(valor),
        limitToFirst(1)
    );
    const snapshot = await get(consulta);
    const dados = snapshot.val() || {};
    const entrada = Object.entries(dados)[0];
    return entrada ? { uid: entrada[0], ...(entrada[1] || {}) } : null;
}

export async function usuarioTemPermissao(uid, permissao) {
    const usuarioUid = validarUid(uid);
    const chave = String(permissao || '').trim();
    if (!idFirebaseValido(chave)) return false;
    const snapshot = await get(ref(dadosSegurosDb, `${PERMISSOES_ROOT}/${usuarioUid}/${chave}`));
    return snapshot.val() === true;
}

export async function registrarIdentidadeSegura(usuario) {
    garantirEscritaRemota();
    const uid = validarUid(usuario?.uid);
    const email = String(usuario?.email || '').trim().toLowerCase();
    const nome = String(usuario?.displayName || '').trim();
    if (!email || !nome) throw new Error('Identidade do usuário incompleta.');
    const chaveSessao = `play_identidade_segura_${uid}`;
    const assinatura = `${email}\n${nome}`;
    try {
        if (sessionStorage.getItem(chaveSessao) === assinatura) return uid;
    } catch (_) {}
    await set(ref(dadosSegurosDb, `${USUARIOS_ROOT}/${uid}`), {
        uid,
        nome,
        email,
        atualizadoEm: serverTimestamp()
    });
    try {
        sessionStorage.setItem(chaveSessao, assinatura);
    } catch (_) {}
    return uid;
}

export async function sincronizarAcessoSeguroColaborador({ uid = '', nome, email, perfil }) {
    garantirEscritaRemota();
    const emailLimpo = String(email || '').trim().toLowerCase();
    const nomeLimpo = String(nome || '').trim();
    let usuarioUid = String(uid || '').trim();
    if (!usuarioUid) {
        const existente = await buscarUsuarioSeguroPorEmail(emailLimpo);
        usuarioUid = String(existente?.uid || '').trim();
    }
    usuarioUid = validarUid(usuarioUid);
    const podeGerenciar = perfilPodeGerenciarLocalizacao(perfil);
    const atualizacoes = {};
    atualizacoes[`${USUARIOS_ROOT}/${usuarioUid}`] = {
        uid: usuarioUid,
        nome: nomeLimpo,
        email: emailLimpo,
        atualizadoEm: serverTimestamp()
    };
    atualizacoes[`${PERMISSOES_ROOT}/${usuarioUid}/administrador`] = podeGerenciar;
    atualizacoes[`${PERMISSOES_ROOT}/${usuarioUid}/localizacao_gestor`] = podeGerenciar;
    await update(ref(dadosSegurosDb), atualizacoes);
    return usuarioUid;
}

export async function removerAcessoSeguroColaborador({ uid = '', email = '' }) {
    garantirEscritaRemota();
    let usuarioUid = String(uid || '').trim();
    if (!usuarioUid && email) {
        const existente = await buscarUsuarioSeguroPorEmail(email);
        usuarioUid = String(existente?.uid || '').trim();
    }
    if (!usuarioUid) return false;
    usuarioUid = validarUid(usuarioUid);
    await update(ref(dadosSegurosDb), {
        [`${USUARIOS_ROOT}/${usuarioUid}`]: null,
        [`${PERMISSOES_ROOT}/${usuarioUid}`]: null
    });
    return true;
}
