import { initializeApp } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js";
import {
    getAuth,
    signInWithEmailAndPassword,
    signOut,
    onAuthStateChanged,
    createUserWithEmailAndPassword,
    updateProfile,
    updatePassword
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-auth.js";
import { app } from './database.js';

export const auth = getAuth(app);
export { createUserWithEmailAndPassword, updateProfile };

// Converte nome do colaborador em email interno para o Firebase Auth
// Ex: "João Silva" → "joao.silva@play.internal"
export function nomeParaEmail(nome) {
    return nome.trim().toLowerCase()
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

// Faz login com nome + senha. Salva o nome no localStorage para o site reconhecer.
export async function fazerLogin(nome, senha) {
    const email = nomeParaEmail(nome.trim());
    const cred = await signInWithEmailAndPassword(auth, email, normalizarSenha(senha));
    const nomeExibicao = cred.user.displayName || nome.trim();
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
                if (user.displayName) {
                    localStorage.setItem('usuarioLogado', user.displayName);
                    localStorage.setItem('usuario_selecionado', user.displayName);
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
    const appSec = initializeApp(app.options, 'cad-' + Date.now());
    const authSec = getAuth(appSec);
    try {
        const email = nomeParaEmail(nome);
        const cred = await createUserWithEmailAndPassword(authSec, email, normalizarSenha(senha));
        await updateProfile(cred.user, { displayName: nome });
    } finally {
        await signOut(authSec).catch(() => {});
    }
}

// Atualiza a senha do Firebase Auth de um colaborador existente sem derrubar a sessão do admin.
export async function atualizarSenhaColaborador(nome, senhaAntiga, senhaNova) {
    const appSec = initializeApp(app.options, 'pwd-' + Date.now());
    const authSec = getAuth(appSec);
    try {
        const email = nomeParaEmail(nome);
        const cred = await signInWithEmailAndPassword(authSec, email, normalizarSenha(senhaAntiga));
        await updatePassword(cred.user, normalizarSenha(senhaNova));
    } finally {
        await signOut(authSec).catch(() => {});
    }
}
