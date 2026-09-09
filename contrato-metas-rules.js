export const META_FATURAMENTO_PADRAO = 200000;
export const PRAZO_APROVACAO_META_MS = 24 * 60 * 60 * 1000;

function partesDataBrasilia(data) {
    const partes = new Intl.DateTimeFormat('pt-BR', {
        timeZone: 'America/Sao_Paulo',
        year: 'numeric',
        month: '2-digit'
    }).formatToParts(data);
    return Object.fromEntries(partes.map(parte => [parte.type, parte.value]));
}

export function obterPeriodoMesAtualBrasilia(agora = new Date()) {
    const partes = partesDataBrasilia(agora);
    const ano = Number(partes.year);
    const mes = Number(partes.month);
    const proximoMes = mes === 12 ? 1 : mes + 1;
    const proximoAno = mes === 12 ? ano + 1 : ano;
    const ultimoDia = new Date(Date.UTC(ano, mes, 0)).getUTCDate();
    return {
        ano,
        mes,
        inicioIso: `${ano}-${String(mes).padStart(2, '0')}-01T00:00:00-03:00`,
        fimIso: `${proximoAno}-${String(proximoMes).padStart(2, '0')}-01T00:00:00-03:00`,
        ultimoDia
    };
}

export function obterValorMetaFaturamento(metaDados = {}) {
    const valor = Number(metaDados?.ativo?.valor);
    return Number.isFinite(valor) && valor > 0 ? valor : META_FATURAMENTO_PADRAO;
}

export function calcularPercentualMeta(valorAtual, valorMeta) {
    const meta = Number(valorMeta);
    if (!Number.isFinite(meta) || meta <= 0) return 0;
    return Math.max(0, Math.round((Number(valorAtual || 0) / meta) * 100));
}

export function propostaMetaExpirada(proposta, agora = Date.now()) {
    const expiraEm = Number(proposta?.expiraEm);
    return !Number.isFinite(expiraEm) || expiraEm <= agora;
}
