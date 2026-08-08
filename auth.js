import { initializeApp, deleteApp } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js";
import {
    getAuth,
    signInWithEmailAndPassword,
    signOut,
    onAuthStateChanged,
    createUserWithEmailAndPassword,
    updateProfile,
    updatePassword,
    updateEmail,
    deleteUser
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-auth.js";
import { app, auth } from './firebase-app.js';

export { auth, createUserWithEmailAndPassword, updateProfile };

function normalizarNomeUsuario(nome) {
    return String(nome || '').trim();
}

// Converte nome do colaborador em email interno para o Firebase Auth
// Ex: "João Silva" → "joao.silva@play.internal"
export function nomeParaEmail(nome) {
    return normalizarNomeUsuario(nome).toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')   // remove acentos
        .replace(/[^a-z0-9]/g, '.')                          // espaços e especiais → ponto
        .replace(/\.+/g, '.')                                 // pontos duplos → um
        .replace(/^\.|\.$/g, '')                              // remove ponto no início/fim
        + '@play.internal';
}

// Firebase exige mínimo 6 caracteres. Completa com '@' se necessário.
// Usado tanto no login quanto na criação de contas para manter consistência.
export function normalizarSenha(senha) {
    let s = senha;
    while (s.length < 6) s += '@';
    return s;
}

function criarAuthSecundario(prefixo) {
    const sufixo = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const appSec = initializeApp(app.options, `${prefixo}-${sufixo}`);
    const authSec = getAuth(appSec);
    return {
        authSec,
        encerrar: async () => {
            await signOut(authSec).catch(() => {});
            await deleteApp(appSec).catch(() => {});
        }
    };
}

// Faz login com nome + senha. Salva o nome no localStorage para o site reconhecer.
export async function fazerLogin(nome, senha) {
    const email = nomeParaEmail(nome);
    const cred = await signInWithEmailAndPassword(auth, email, normalizarSenha(senha));
    const nomeExibicao = normalizarNomeUsuario(cred.user.displayName || nome);
    localStorage.setItem('usuarioLogado', nomeExibicao);
    localStorage.setItem('usuario_selecionado', nomeExibicao);
    return cred.user;
}

// Faz logout e redireciona para o login
export async function fazerLogout() {
    localStorage.removeItem('usuarioLogado');
    localStorage.removeItem('usuario_selecionado');
    await signOut(auth);
    window.location.replace('login.html');
}

// Verifica autenticação. Se não estiver logado, redireciona para login.html.
// Chame no topo de cada página protegida.
export function verificarAutenticacao() {
    return new Promise((resolve) => {
        const unsubscribe = onAuthStateChanged(auth, (user) => {
            unsubscribe();
            if (user) {
                const nomeExibicao = normalizarNomeUsuario(user.displayName);
                if (nomeExibicao) {
                    localStorage.setItem('usuarioLogado', nomeExibicao);
                    localStorage.setItem('usuario_selecionado', nomeExibicao);
                }
                document.body.style.visibility = 'visible';
                resolve(user);
            } else {
                window.location.replace('login.html');
                resolve(null);
            }
        });
    });
}

// Cria conta Firebase para novo colaborador sem derrubar a sessão do admin.
export async function criarContaColaborador(nome, senha) {
    const { authSec, encerrar } = criarAuthSecundario('cad');
    try {
        const nomeNormalizado = normalizarNomeUsuario(nome);
        const email = nomeParaEmail(nomeNormalizado);
        const cred = await createUserWithEmailAndPassword(authSec, email, normalizarSenha(senha));
        await updateProfile(cred.user, { displayName: nomeNormalizado });
        return cred.user.uid;
    } finally {
        await encerrar();
    }
}

// Atualiza a senha do Firebase Auth de um colaborador existente sem derrubar a sessão do admin.
export async function atualizarSenhaColaborador(nome, senhaAntiga, senhaNova) {
    const { authSec, encerrar } = criarAuthSecundario('pwd');
    try {
        const email = nomeParaEmail(normalizarNomeUsuario(nome));
        const cred = await signInWithEmailAndPassword(authSec, email, normalizarSenha(senhaAntiga));
        await updatePassword(cred.user, normalizarSenha(senhaNova));
    } finally {
        await encerrar();
    }
}

async function atualizarContaColaboradorComLogin({ nomeLogin, senhaLogin, nomeAtual, senhaAtual }) {
    const nomeLoginNormalizado = normalizarNomeUsuario(nomeLogin);
    const nomeAtualNormalizado = normalizarNomeUsuario(nomeAtual);
    const emailLogin = nomeParaEmail(nomeLoginNormalizado);
    const emailAtual = nomeParaEmail(nomeAtualNormalizado);
    const { authSec, encerrar } = criarAuthSecundario('sync');

    try {
        const cred = await signInWithEmailAndPassword(authSec, emailLogin, normalizarSenha(senhaLogin));
        if (emailLogin !== emailAtual) {
            await updateEmail(cred.user, emailAtual);
        }
        await updateProfile(cred.user, { displayName: nomeAtualNormalizado });
        await updatePassword(cred.user, normalizarSenha(senhaAtual));
        return cred.user.uid;
    } finally {
        await encerrar();
    }
}

export async function sincronizarContaColaborador({ nomeAnterior = '', senhaAnterior = '', nomeAtual, senhaAtual }) {
    const nomeAtualNormalizado = normalizarNomeUsuario(nomeAtual);
    const senhaAtualNormalizada = String(senhaAtual || '').trim();
    const nomeAnteriorNormalizado = normalizarNomeUsuario(nomeAnterior);
    const senhaAnteriorNormalizada = String(senhaAnterior || '').trim();

    if (!nomeAtualNormalizado || !senhaAtualNormalizada) {
        throw new Error('Nome e senha do colaborador são obrigatórios para criar o login.');
    }

    if (!nomeAnteriorNormalizado) {
        try {
            return await criarContaColaborador(nomeAtualNormalizado, senhaAtualNormalizada);
        } catch (error) {
            if (error?.code !== 'auth/email-already-in-use') throw error;
            return atualizarContaColaboradorComLogin({
                nomeLogin: nomeAtualNormalizado,
                senhaLogin: senhaAtualNormalizada,
                nomeAtual: nomeAtualNormalizado,
                senhaAtual: senhaAtualNormalizada
            });
        }
    }

    if (!senhaAnteriorNormalizada) {
        throw new Error('Senha anterior do colaborador não encontrada para atualizar o login.');
    }

    return atualizarContaColaboradorComLogin({
        nomeLogin: nomeAnteriorNormalizado,
        senhaLogin: senhaAnteriorNormalizada,
        nomeAtual: nomeAtualNormalizado,
        senhaAtual: senhaAtualNormalizada
    });
}

export async function excluirContaColaborador(nome, senha) {
    const nomeNormalizado = normalizarNomeUsuario(nome);
    const senhaNormalizada = String(senha || '').trim();

    if (!nomeNormalizado || !senhaNormalizada) {
        throw new Error('Nome e senha do colaborador são obrigatórios para excluir o login.');
    }

    const { authSec, encerrar } = criarAuthSecundario('del');
    try {
        const email = nomeParaEmail(nomeNormalizado);
        const cred = await signInWithEmailAndPassword(authSec, email, normalizarSenha(senhaNormalizada));
        const uid = cred.user.uid;
        await deleteUser(cred.user);
        return uid;
    } finally {
        await encerrar();
    }
}
