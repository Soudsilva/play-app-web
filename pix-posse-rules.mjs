export function normalizarNumeroPix(valor) {
    return String(valor || '').trim();
}

export function listarPixUnicos(itens = []) {
    const unicos = new Map();

    (Array.isArray(itens) ? itens : []).forEach(item => {
        const numero = normalizarNumeroPix(
            typeof item === 'string' || typeof item === 'number'
                ? item
                : item?.pix ?? item?.numero_pix
        );
        if (!numero || unicos.has(numero)) return;
        unicos.set(numero, numero);
    });

    return [...unicos.values()];
}

export function calcularPixAdicionados(itensAtuais = [], itensAnteriores = []) {
    const anteriores = new Set(listarPixUnicos(itensAnteriores));
    return listarPixUnicos(itensAtuais).filter(numero => !anteriores.has(numero));
}

export function formatarErroPixSemPosse(numeros = []) {
    const lista = listarPixUnicos(numeros);
    if (lista.length === 1) {
        return `O Pix ${lista[0]} não está no seu balanço. Você só pode entregá-lo a um cliente depois de recebê-lo.`;
    }

    return `Estes Pix não estão no seu balanço: ${lista.join(', ')}. Você só pode entregá-los a um cliente depois de recebê-los.`;
}
