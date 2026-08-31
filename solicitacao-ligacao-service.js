import {
    get,
    ref,
    update
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import { db } from './firebase-app.js';

const TIPOS_REGISTRO = new Set(['atendimento', 'cliente']);
const CONTESTACOES_USUARIO_ROOT = 'contestacao_atendimento';
const VERSAO_CLIENTES_PATH = 'metadata/clientes_versao';

function textoObrigatorio(valor, mensagem) {
    const texto = String(valor || '').trim();
    if (!texto) throw new Error(mensagem);
    return texto;
}

function normalizarTipo(tipoRegistro) {
    const tipo = String(tipoRegistro || '').trim().toLowerCase();
    if (!TIPOS_REGISTRO.has(tipo)) throw new Error('Tipo de servico invalido.');
    return tipo;
}

function caminhoRegistro(tipo, id) {
    return tipo === 'cliente' ? `clientes/${id}` : `atendimentos/${id}`;
}

function obterResponsavel(tipo, registro) {
    const nome = tipo === 'cliente' ? registro?.cadastradoPor : registro?.atendente;
    return textoObrigatorio(nome, 'Responsavel pelo servico nao encontrado.');
}

function caminhoPendencia(nomeUsuario, tipo, id) {
    return `${CONTESTACOES_USUARIO_ROOT}/${nomeUsuario}/pendentes/ligacao_${tipo}_${id}`;
}

async function obterRegistro(tipoRegistro, registroId) {
    const tipo = normalizarTipo(tipoRegistro);
    const id = textoObrigatorio(registroId, 'ID do servico e obrigatorio.');
    const path = caminhoRegistro(tipo, id);
    const snapshot = await get(ref(db, path));
    if (!snapshot.exists()) throw new Error('Servico nao encontrado.');
    return { tipo, id, path, dados: snapshot.val() || {} };
}

async function atualizarResumoUsuario(nomeUsuario) {
    const pendentesPath = `${CONTESTACOES_USUARIO_ROOT}/${nomeUsuario}/pendentes`;
    const snapshot = await get(ref(db, pendentesPath));
    const totalPendentes = snapshot.exists()
        ? Object.keys(snapshot.val() || {}).length
        : 0;

    await update(ref(db), {
        [`${CONTESTACOES_USUARIO_ROOT}/${nomeUsuario}/resumo/statusContestacao`]: totalPendentes > 0 ? 'pendente' : 'ok',
        [`${CONTESTACOES_USUARIO_ROOT}/${nomeUsuario}/resumo/totalPendentes`]: totalPendentes,
        [`${CONTESTACOES_USUARIO_ROOT}/${nomeUsuario}/resumo/atualizadoEm`]: new Date().toISOString()
    });
}

export async function solicitarLigacaoServico(tipoRegistro, registroId, nomeGestor) {
    const gestor = textoObrigatorio(nomeGestor, 'Nome do gestor nao encontrado.');
    const { tipo, id, path, dados } = await obterRegistro(tipoRegistro, registroId);
    if (dados.solicitacaoLigacaoPendente === true) {
        throw new Error('A ligacao ja foi solicitada.');
    }

    const responsavel = obterResponsavel(tipo, dados);
    const solicitacaoLigacaoEm = new Date().toISOString();
    const patchRaiz = {
        [`${path}/solicitacaoLigacaoPendente`]: true,
        [`${path}/solicitacaoLigacaoEm`]: solicitacaoLigacaoEm,
        [`${path}/solicitacaoLigacaoPor`]: gestor,
        [caminhoPendencia(responsavel, tipo, id)]: true
    };
    if (tipo === 'cliente') patchRaiz[VERSAO_CLIENTES_PATH] = Date.now();

    await update(ref(db), patchRaiz);
    await atualizarResumoUsuario(responsavel);

    return {
        solicitacaoLigacaoPendente: true,
        solicitacaoLigacaoEm,
        solicitacaoLigacaoPor: gestor
    };
}

export async function confirmarLigacaoRecebidaServico(tipoRegistro, registroId, nomeGestor) {
    const gestor = textoObrigatorio(nomeGestor, 'Nome do gestor nao encontrado.');
    const { tipo, id, path, dados } = await obterRegistro(tipoRegistro, registroId);
    if (dados.solicitacaoLigacaoPendente !== true) {
        throw new Error('Esta solicitacao de ligacao ja foi encerrada.');
    }

    const responsavel = obterResponsavel(tipo, dados);
    const ligacaoRecebidaEm = new Date().toISOString();
    const patchRaiz = {
        [`${path}/solicitacaoLigacaoPendente`]: null,
        [`${path}/solicitacaoLigacaoEm`]: null,
        [`${path}/solicitacaoLigacaoPor`]: null,
        [`${path}/ligacaoRecebidaEm`]: ligacaoRecebidaEm,
        [`${path}/ligacaoRecebidaPor`]: gestor,
        [caminhoPendencia(responsavel, tipo, id)]: null
    };
    if (tipo === 'cliente') patchRaiz[VERSAO_CLIENTES_PATH] = Date.now();

    await update(ref(db), patchRaiz);
    await atualizarResumoUsuario(responsavel);

    return {
        solicitacaoLigacaoPendente: null,
        solicitacaoLigacaoEm: null,
        solicitacaoLigacaoPor: null,
        ligacaoRecebidaEm,
        ligacaoRecebidaPor: gestor
    };
}
