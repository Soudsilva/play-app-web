import {
    ref,
    runTransaction,
    update
} from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';
import { db } from './firebase-app.js';

const REGRAS_ROOT = 'contratos_sociedade/regras';

function validarId(valor, rotulo) {
    const id = String(valor || '').trim();
    if (!id || /[.#$/\[\]]/.test(id)) throw new Error(`${rotulo} inválido.`);
    return id;
}

export async function criarSolicitacaoRegra(dados) {
    const regraId = validarId(dados?.regraId, 'Regra');
    const propostaRef = ref(db, `${REGRAS_ROOT}/pendentes/${regraId}`);
    const agora = Date.now();
    const proposta = {
        regraId,
        acao: dados.acao,
        titulo: String(dados.titulo || '').trim(),
        propostoPor: String(dados.propostoPor || '').trim(),
        propostoPorNome: String(dados.propostoPorNome || '').trim(),
        aprovadores: Array.isArray(dados.aprovadores) ? dados.aprovadores : [],
        aprovacoes: dados.aprovacoes || {},
        valorAnterior: dados.valorAnterior || null,
        valorNovo: dados.valorNovo || null,
        criadoEm: new Date(agora).toISOString(),
        expiraEm: Number(dados.expiraEm)
    };

    const resultado = await runTransaction(propostaRef, atual => {
        if (atual && Number(atual.expiraEm) > agora) return;
        return proposta;
    });
    if (!resultado.committed) throw new Error('Já existe uma alteração aguardando aprovação para esta regra.');
}

export async function responderSolicitacaoRegra(regraIdValor, socioIdValor, decisao) {
    const regraId = validarId(regraIdValor, 'Regra');
    const socioId = validarId(socioIdValor, 'Sócio');
    const propostaRef = ref(db, `${REGRAS_ROOT}/pendentes/${regraId}`);
    let motivoInterrupcao = '';

    const resultado = await runTransaction(propostaRef, atual => {
        if (!atual) {
            motivoInterrupcao = 'ausente';
            return;
        }
        if (Number(atual.expiraEm) <= Date.now()) {
            motivoInterrupcao = 'expirada';
            return null;
        }

        const ids = (Array.isArray(atual.aprovadores) ? atual.aprovadores : [])
            .map(item => String(item?.id || '').trim())
            .filter(Boolean);
        if (!ids.includes(socioId)) {
            motivoInterrupcao = 'nao_autorizado';
            return;
        }

        const aprovacoes = { ...(atual.aprovacoes || {}), [socioId]: decisao === true };
        const status = ids.some(id => aprovacoes[id] === false)
            ? 'negada'
            : ids.every(id => aprovacoes[id] === true)
                ? 'aprovada'
                : 'pendente';
        return { ...atual, aprovacoes, status };
    });

    const proposta = resultado.snapshot.val();
    if (!resultado.committed || !proposta) return { status: motivoInterrupcao || 'ausente' };
    if (proposta.status === 'pendente') return { status: 'pendente' };

    const atualizacoes = {
        [`${REGRAS_ROOT}/pendentes/${regraId}`]: null
    };
    if (proposta.status === 'aprovada') {
        atualizacoes[`${REGRAS_ROOT}/ativos/${regraId}`] = proposta.acao === 'excluir'
            ? {
                excluida: true,
                atualizadoEm: new Date().toISOString(),
                atualizadoPor: socioId
            }
            : {
                ...proposta.valorNovo,
                excluida: false,
                atualizadoEm: new Date().toISOString(),
                atualizadoPor: socioId
            };
    }
    await update(ref(db), atualizacoes);
    return { status: proposta.status };
}

export async function descartarSolicitacaoRegraExpirada(regraIdValor, expiraEmEsperado) {
    const regraId = validarId(regraIdValor, 'Regra');
    const propostaRef = ref(db, `${REGRAS_ROOT}/pendentes/${regraId}`);
    await runTransaction(propostaRef, atual => {
        if (!atual) return;
        if (Number(atual.expiraEm) !== Number(expiraEmEsperado)) return;
        return Number(atual.expiraEm) <= Date.now() ? null : atual;
    });
}
