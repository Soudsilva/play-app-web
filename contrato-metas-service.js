import {
    get,
    ref,
    runTransaction,
    update
} from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';
import { db } from './firebase-app.js';

const META_ROOT = 'contratos_sociedade/metas/faturamento_mensal';
const RESUMO_FATURAMENTO_ROOT = 'resumo_faturamento_mensal';

export async function carregarResumoFaturamentoMensal(competencia) {
    const chave = String(competencia || '').trim();
    if (!/^\d{4}-\d{2}$/.test(chave)) throw new Error('Competência mensal inválida.');
    const snapshot = await get(ref(db, `${RESUMO_FATURAMENTO_ROOT}/${chave}/totalGeral`));
    if (!snapshot.exists()) return { disponivel: false, totalGeral: 0 };
    const totalGeral = Number(snapshot.val());
    return {
        disponivel: Number.isFinite(totalGeral),
        totalGeral: Number.isFinite(totalGeral) ? totalGeral : 0
    };
}

export async function criarSolicitacaoMetaFaturamento(dados) {
    const propostaRef = ref(db, `${META_ROOT}/pendente`);
    const agora = Date.now();
    const proposta = {
        tipo: 'editar_meta_faturamento',
        propostoPor: String(dados?.propostoPor || '').trim(),
        propostoPorNome: String(dados?.propostoPorNome || '').trim(),
        aprovadores: Array.isArray(dados?.aprovadores) ? dados.aprovadores : [],
        aprovacoes: dados?.aprovacoes || {},
        valorAnterior: Number(dados?.valorAnterior || 0),
        valorNovo: Number(dados?.valorNovo || 0),
        criadoEm: new Date(agora).toISOString(),
        expiraEm: Number(dados?.expiraEm)
    };

    const resultado = await runTransaction(propostaRef, atual => {
        if (atual && Number(atual.expiraEm) > agora) return;
        return proposta;
    });
    if (!resultado.committed) throw new Error('Já existe uma alteração da meta aguardando aprovação.');
}

export async function responderSolicitacaoMetaFaturamento(socioIdValor, decisao) {
    const socioId = String(socioIdValor || '').trim();
    if (!socioId || /[.#$/\[\]]/.test(socioId)) throw new Error('Sócio inválido.');
    const propostaRef = ref(db, `${META_ROOT}/pendente`);
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

    const atualizacoes = { [`${META_ROOT}/pendente`]: null };
    if (proposta.status === 'aprovada') {
        atualizacoes[`${META_ROOT}/ativo`] = {
            valor: Number(proposta.valorNovo),
            atualizadoEm: new Date().toISOString(),
            atualizadoPor: socioId
        };
    }
    await update(ref(db), atualizacoes);
    return { status: proposta.status };
}

export async function descartarSolicitacaoMetaExpirada(expiraEmEsperado) {
    const propostaRef = ref(db, `${META_ROOT}/pendente`);
    await runTransaction(propostaRef, atual => {
        if (!atual) return;
        if (Number(atual.expiraEm) !== Number(expiraEmEsperado)) return;
        return Number(atual.expiraEm) <= Date.now() ? null : atual;
    });
}
