export const META_RESERVA_PADRAO = 60000;

export function obterMetaReserva(metaDados = {}) {
    const valor = Number(metaDados?.ativo?.valor);
    return Number.isFinite(valor) && valor > 0 ? valor : META_RESERVA_PADRAO;
}

export function calcularPercentualReserva(saldo, meta) {
    const valorMeta = Number(meta);
    if (!Number.isFinite(valorMeta) || valorMeta <= 0) return 0;
    return Math.max(0, Math.round((Number(saldo || 0) / valorMeta) * 100));
}
