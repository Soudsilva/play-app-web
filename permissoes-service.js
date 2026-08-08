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
import { idFirebaseValido, perfilPodeAdministrarDadosSeguros } from './permissoes-rules.js';

const USUARIOS_ROOT = 'usuarios';
const PERMISSOES_ROOT = 'permissoes';

function validarUid(uid) {
    const valor = String(uid || '').trim();
    if (!idFirebaseValido(valor)) throw new Error('Usuário autenticado inválido.');
    return valor;
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
    const emailLimpo = String(email || '').trim().toLowerCase();
    const nomeLimpo = String(nome || '').trim();
    let usuarioUid = String(uid || '').trim();
    if (!usuarioUid) {
        const existente = await buscarUsuarioSeguroPorEmail(emailLimpo);
        usuarioUid = String(existente?.uid || '').trim();
    }
    usuarioUid = validarUid(usuarioUid);
    const podeAdministrar = perfilPodeAdministrarDadosSeguros(perfil);
    const atualizacoes = {};
    atualizacoes[`${USUARIOS_ROOT}/${usuarioUid}`] = {
        uid: usuarioUid,
        nome: nomeLimpo,
        email: emailLimpo,
        atualizadoEm: serverTimestamp()
    };
    atualizacoes[`${PERMISSOES_ROOT}/${usuarioUid}/administrador`] = podeAdministrar;
    await update(ref(dadosSegurosDb), atualizacoes);
    return usuarioUid;
}

export async function removerAcessoSeguroColaborador({ uid = '', email = '' }) {
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
