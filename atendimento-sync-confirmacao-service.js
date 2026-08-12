import { ref, get } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import { db } from './firebase-app.js';
import {
    validarConfirmacaoAtendimentoRemoto,
    validarConfirmacaoProdutosAtendimento
} from './atendimento-fila-sync.mjs';

export async function confirmarAtendimentoNoFirebase(atendimentoId, dadosEsperados = {}) {
    const id = String(atendimentoId || '').trim();
    if (!id) throw new Error('ID do atendimento nao informado para confirmacao.');

    const snapshot = await get(ref(db, `atendimentos/${id}`));
    const remoto = snapshot.exists() ? snapshot.val() : null;
    validarConfirmacaoAtendimentoRemoto(remoto, dadosEsperados);
    return remoto;
}

export async function confirmarResultadoRotaNoFirebase(numeroRota, resultado = {}) {
    if (resultado?.liberada !== true) return true;
    const rota = String(numeroRota || '').trim();
    if (!rota) return true;

    const snapshot = await get(ref(db, `selecao_rotas/ativa/rotas/${rota}/selecionada_por`));
    if (snapshot.exists() && String(snapshot.val() || '').trim()) {
        throw new Error('Liberacao da rota ainda nao foi confirmada no Firebase.');
    }
    return true;
}

export async function confirmarProdutosAtendimentoNoFirebase(resultado = {}, dadosEsperados = {}) {
    const caminhos = Array.isArray(resultado?.caminhos) ? resultado.caminhos : [];
    const registros = await Promise.all(caminhos.map(async caminho => {
        const caminhoLimpo = String(caminho || '').replace(/^\/+|\/+$/g, '');
        if (!caminhoLimpo.startsWith('movimentacao_balanco_historico/')) {
            throw new Error('Caminho de confirmacao de produto invalido.');
        }
        const snapshot = await get(ref(db, caminhoLimpo));
        if (!snapshot.exists()) throw new Error('Movimentacao do produto ainda nao foi confirmada no Firebase.');
        return snapshot.val();
    }));

    validarConfirmacaoProdutosAtendimento(dadosEsperados, registros);
    return true;
}
