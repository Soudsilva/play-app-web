import {
    get,
    push,
    ref,
    update
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import { db } from './firebase-app.js';

const CLIENTES_ROOT = 'clientes';
const HISTORICO_ROOT = 'notas_proxima_visita_historico';
const VERSAO_CLIENTES_PATH = 'metadata/clientes_versao';

function textoObrigatorio(valor, mensagem) {
    const texto = String(valor || '').trim();
    if (!texto) throw new Error(mensagem);
    return texto;
}

function caminhoCliente(clienteId) {
    const id = textoObrigatorio(clienteId, 'ID do cliente e obrigatorio.');
    return `${CLIENTES_ROOT}/${id}`;
}

async function obterCliente(clienteId) {
    const id = textoObrigatorio(clienteId, 'ID do cliente e obrigatorio.');
    const snapshot = await get(ref(db, caminhoCliente(id)));
    if (!snapshot.exists()) throw new Error('Cliente nao encontrado.');
    return { id, dados: snapshot.val() || {} };
}

export async function salvarNotaProximaVisita(clienteId, atendimentoOrigemId, textoNota, nomeGestor) {
    const atendimentoId = textoObrigatorio(atendimentoOrigemId, 'Atendimento de origem nao encontrado.');
    const texto = textoObrigatorio(textoNota, 'Digite a nota para a proxima visita.');
    const gestor = textoObrigatorio(nomeGestor, 'Nome do gestor nao encontrado.');
    if (texto.length > 500) throw new Error('A nota deve ter no maximo 500 caracteres.');

    const { id, dados } = await obterCliente(clienteId);
    const notaAtual = dados?.notaProximaVisitaAtiva || null;

    const agora = new Date().toISOString();
    const notaId = String(notaAtual?.id || '').trim()
        || push(ref(db, `${HISTORICO_ROOT}/${id}`)).key;
    if (!notaId) throw new Error('Nao foi possivel preparar a nota.');

    const nota = {
        id: notaId,
        texto,
        status: 'ativa',
        criadaEm: notaAtual?.criadaEm || agora,
        criadaPor: notaAtual?.criadaPor || gestor,
        atualizadaEm: agora,
        atualizadaPor: gestor,
        atendimentoOrigemId: notaAtual?.atendimentoOrigemId || atendimentoId
    };

    await update(ref(db), {
        [`${caminhoCliente(id)}/notaProximaVisitaAtiva`]: nota,
        [`${HISTORICO_ROOT}/${id}/${notaId}`]: nota,
        [VERSAO_CLIENTES_PATH]: Date.now()
    });

    return { notaProximaVisitaAtiva: nota };
}

export async function concluirNotaProximaVisita(clienteId, notaId, atendimentoConclusaoId, nomeGestor) {
    const idNota = textoObrigatorio(notaId, 'Nota nao encontrada.');
    const atendimentoId = textoObrigatorio(atendimentoConclusaoId, 'Atendimento de conclusao nao encontrado.');
    const gestor = textoObrigatorio(nomeGestor, 'Nome do gestor nao encontrado.');
    const { id, dados } = await obterCliente(clienteId);
    const notaAtual = dados?.notaProximaVisitaAtiva || null;

    if (!notaAtual || String(notaAtual.id || '').trim() !== idNota) {
        throw new Error('Esta nota ja foi concluida ou substituida.');
    }
    if (String(notaAtual.atendimentoOrigemId || '').trim() === atendimentoId) {
        throw new Error('A nota so pode ser concluida em uma visita posterior.');
    }

    const agora = new Date().toISOString();
    const notaConcluida = {
        ...notaAtual,
        status: 'concluida',
        concluidaEm: agora,
        concluidaPor: gestor,
        atendimentoConclusaoId: atendimentoId
    };

    await update(ref(db), {
        [`${caminhoCliente(id)}/notaProximaVisitaAtiva`]: null,
        [`${HISTORICO_ROOT}/${id}/${idNota}`]: notaConcluida,
        [VERSAO_CLIENTES_PATH]: Date.now()
    });

    return { notaProximaVisitaAtiva: null };
}
