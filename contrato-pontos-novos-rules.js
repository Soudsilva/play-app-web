export const META_PONTOS_NOVOS_PADRAO = 10;

export function obterMetaPontosNovos(metaDados = {}) {
    const quantidade = Number(metaDados?.ativo?.quantidade);
    return Number.isInteger(quantidade) && quantidade > 0
        ? quantidade
        : META_PONTOS_NOVOS_PADRAO;
}
