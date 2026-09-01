export function normalizarNumeroPix(valor) {
    const texto = String(valor ?? '').trim();
    if (!/^\d+$/.test(texto)) return texto;
    return texto.replace(/^0+(?=\d)/, '');
}

export function listarVariantesNumeroPix(valor, tamanhoMaximo = 3) {
    const original = String(valor ?? '').trim();
    const normalizado = normalizarNumeroPix(original);
    const variantes = new Set([original, normalizado].filter(Boolean));

    if (/^\d+$/.test(normalizado)) {
        const limite = Math.max(normalizado.length, Number(tamanhoMaximo) || 0);
        for (let tamanho = normalizado.length; tamanho <= limite; tamanho += 1) {
            variantes.add(normalizado.padStart(tamanho, '0'));
        }
    }

    return [...variantes];
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
