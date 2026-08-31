import {
    get,
    push,
    ref,
    update
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import { db } from './firebase-app.js';

const CLIENTES_ROOT = 'clientes';
const VERSAO_CLIENTES_PATH = 'metadata/clientes_versao';

// Mantem o indicador ja usado no index para reunir todas as duvidas do gestor.
// O prefixo impede colisao entre IDs de clientes e de atendimentos.
const CONTESTACOES_USUARIO_ROOT = 'contestacao_atendimento';
const PREFIXO_PENDENCIA_CLIENTE = 'cliente_';

function textoObrigatorio(valor, mensagem) {
    const texto = String(valor || '').trim();
    if (!texto) throw new Error(mensagem);
    return texto;
}

function caminhoCliente(clienteId) {
    const id = textoObrigatorio(clienteId, 'ID do cliente e obrigatorio.');
    return `${CLIENTES_ROOT}/${id}`;
}

function chaveUsuario(nomeUsuario) {
    return textoObrigatorio(nomeUsuario, 'Usuario responsavel nao encontrado.');
}

function obterMensagensAnteriores(valor) {
    if (valor && typeof valor === 'object' && valor.mensagens) {
        return { ...valor.mensagens };
    }
    if (typeof valor !== 'string' || !valor.trim()) return {};

    return {
        legado: {
            autor: 'Gestor',
            papel: 'gestor',
            texto: valor.trim(),
            timestamp: new Date(0).toISOString()
        }
    };
}

function caminhoPendencia(nomeUsuario, clienteId) {
    return `${CONTESTACOES_USUARIO_ROOT}/${nomeUsuario}/pendentes/${PREFIXO_PENDENCIA_CLIENTE}${clienteId}`;
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

async function obterCliente(clienteId) {
    const path = caminhoCliente(clienteId);
    const snapshot = await get(ref(db, path));
    if (!snapshot.exists()) throw new Error('Cliente nao encontrado.');
    return { id: String(clienteId).trim(), dados: snapshot.val() || {} };
}

export async function temDuvidaClientePendente(clienteId) {
    const { dados } = await obterCliente(clienteId);
    return dados.contestado === true;
}

export async function enviarDuvidaCliente(clienteId, nomeGestor, observacao) {
    const gestor = textoObrigatorio(nomeGestor, 'Nome do gestor nao encontrado.');
    const texto = textoObrigatorio(observacao, 'Digite a duvida.');
    const { id, dados } = await obterCliente(clienteId);
    if (dados.contestado === true) {
        throw new Error('Ja existe uma duvida pendente para este cliente.');
    }
    const responsavel = chaveUsuario(dados.cadastradoPor);
    const mensagens = obterMensagensAnteriores(dados.contestacaoObservacao);
    const agora = new Date().toISOString();
    const mensagemId = push(ref(db, `${caminhoCliente(id)}/contestacaoObservacao/mensagens`)).key;
    if (!mensagemId) throw new Error('Nao foi possivel preparar a duvida.');

    mensagens[mensagemId] = {
        autor: gestor,
        papel: 'gestor',
        texto,
        timestamp: agora
    };

    const contestacaoObservacao = {
        tipo: 'conversa_contestacao_cliente',
        mensagens
    };
    const versao = Date.now();
    const contestadoAte = new Date(versao + 24 * 60 * 60 * 1000).toISOString();
    await update(ref(db), {
        [`${caminhoCliente(id)}/contestado`]: true,
        [`${caminhoCliente(id)}/contestadoAte`]: contestadoAte,
        [`${caminhoCliente(id)}/contestadoPor`]: gestor,
        [`${caminhoCliente(id)}/contestacaoObservacao`]: contestacaoObservacao,
        [caminhoPendencia(responsavel, id)]: true,
        [VERSAO_CLIENTES_PATH]: versao
    });
    await atualizarResumoUsuario(responsavel);

    return {
        contestado: true,
        contestadoAte,
        contestadoPor: gestor,
        contestacaoObservacao
    };
}

export async function responderDuvidaCliente(clienteId, nomeUsuario, resposta) {
    const usuario = chaveUsuario(nomeUsuario);
    const texto = textoObrigatorio(resposta, 'Digite uma resposta.');
    const { id, dados } = await obterCliente(clienteId);
    if (dados.contestado !== true) {
        throw new Error('Esta duvida ja foi respondida ou removida.');
    }
    const responsavel = chaveUsuario(dados.cadastradoPor || usuario);
    const mensagens = obterMensagensAnteriores(dados.contestacaoObservacao);
    const mensagemId = push(ref(db, `${caminhoCliente(id)}/contestacaoObservacao/mensagens`)).key;
    if (!mensagemId) throw new Error('Nao foi possivel preparar a resposta.');

    mensagens[mensagemId] = {
        autor: usuario,
        papel: 'usuario',
        texto,
        timestamp: new Date().toISOString()
    };

    const contestacaoObservacao = {
        tipo: 'conversa_contestacao_cliente',
        mensagens
    };
    await update(ref(db), {
        [`${caminhoCliente(id)}/contestado`]: null,
        [`${caminhoCliente(id)}/contestadoAte`]: null,
        [`${caminhoCliente(id)}/contestadoPor`]: null,
        [`${caminhoCliente(id)}/contestacaoObservacao`]: contestacaoObservacao,
        [caminhoPendencia(responsavel, id)]: null,
        [VERSAO_CLIENTES_PATH]: Date.now()
    });
    await atualizarResumoUsuario(responsavel);

    return {
        contestado: null,
        contestadoAte: null,
        contestadoPor: null,
        contestacaoObservacao
    };
}

export async function removerDuvidaCliente(clienteId) {
    const { id, dados } = await obterCliente(clienteId);
    if (dados.contestado !== true) {
        throw new Error('Esta duvida ja foi respondida ou removida.');
    }
    const responsavel = chaveUsuario(dados.cadastradoPor);
    await update(ref(db), {
        [`${caminhoCliente(id)}/contestado`]: null,
        [`${caminhoCliente(id)}/contestadoAte`]: null,
        [`${caminhoCliente(id)}/contestadoPor`]: null,
        [`${caminhoCliente(id)}/contestacaoObservacao`]: null,
        [caminhoPendencia(responsavel, id)]: null,
        [VERSAO_CLIENTES_PATH]: Date.now()
    });
    await atualizarResumoUsuario(responsavel);

    return {
        contestado: null,
        contestadoAte: null,
        contestadoPor: null,
        contestacaoObservacao: null
    };
}
