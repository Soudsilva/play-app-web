import {
    get,
    increment,
    limitToLast,
    onValue,
    orderByChild,
    push,
    query,
    ref,
    runTransaction,
    serverTimestamp,
    update
} from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';
import { db } from './firebase-app.js';

const RESERVA_ROOT = 'reserva_emergencia';
const META_ROOT = 'contratos_sociedade/metas/reserva_emergencia';

export function escutarSaldoReserva(callback) {
    return onValue(ref(db, `${RESERVA_ROOT}/saldo`), snapshot => {
        const valor = Number(snapshot.val() || 0);
        callback(Number.isFinite(valor) && valor >= 0 ? valor : 0);
    });
}

export async function carregarMovimentacoesReserva(limite = 100) {
    const quantidade = Math.max(1, Math.min(200, Number(limite) || 100));
    const snapshot = await get(query(
        ref(db, `${RESERVA_ROOT}/movimentacoes`),
        orderByChild('criadoEm'),
        limitToLast(quantidade)
    ));
    return Object.entries(snapshot.val() || {})
        .map(([id, movimento]) => ({ id, ...movimento }))
        .sort((a, b) => Number(b.criadoEm || 0) - Number(a.criadoEm || 0));
}

export async function registrarMovimentoReserva({ tipo, valor, motivo, registradoPor }) {
    const tipoNormalizado = String(tipo || '').trim();
    const valorNormalizado = Number(valor);
    const motivoNormalizado = String(motivo || '').trim();
    const responsavel = String(registradoPor || '').trim();

    if (!['entrada', 'retirada'].includes(tipoNormalizado)) throw new Error('TIPO_INVALIDO');
    if (!Number.isFinite(valorNormalizado) || valorNormalizado <= 0) throw new Error('VALOR_INVALIDO');
    if (!responsavel) throw new Error('RESPONSAVEL_INVALIDO');
    if (tipoNormalizado === 'retirada' && !motivoNormalizado) throw new Error('MOTIVO_OBRIGATORIO');

    if (tipoNormalizado === 'retirada') {
        const saldoSnapshot = await get(ref(db, `${RESERVA_ROOT}/saldo`));
        const saldo = Number(saldoSnapshot.val() || 0);
        if (!Number.isFinite(saldo) || valorNormalizado > saldo) throw new Error('SALDO_INSUFICIENTE');
    }

    const movimentoRef = push(ref(db, `${RESERVA_ROOT}/movimentacoes`));
    const movimento = {
        tipo: tipoNormalizado,
        valor: valorNormalizado,
        registradoPor: responsavel,
        criadoEm: serverTimestamp()
    };
    if (motivoNormalizado) movimento.motivo = motivoNormalizado;

    const delta = tipoNormalizado === 'entrada' ? valorNormalizado : -valorNormalizado;
    await update(ref(db), {
        [`${RESERVA_ROOT}/saldo`]: increment(delta),
        [`${RESERVA_ROOT}/movimentacoes/${movimentoRef.key}`]: movimento
    });
    return movimentoRef.key;
}

export async function criarSolicitacaoMetaReserva(dados) {
    const propostaRef = ref(db, `${META_ROOT}/pendente`);
    const agora = Date.now();
    const proposta = {
        tipo: 'editar_meta_reserva_emergencia',
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

export async function responderSolicitacaoMetaReserva(socioIdValor, decisao) {
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

export async function descartarSolicitacaoMetaReservaExpirada(expiraEmEsperado) {
    const propostaRef = ref(db, `${META_ROOT}/pendente`);
    await runTransaction(propostaRef, atual => {
        if (!atual) return;
        if (Number(atual.expiraEm) !== Number(expiraEmEsperado)) return;
        return Number(atual.expiraEm) <= Date.now() ? null : atual;
    });
}
