function limparTextoEndereco(valor) {
    return String(valor ?? '').replace(/\s+/g, ' ').trim();
}

function enderecoTemNumero(endereco) {
    const textoSemCep = limparTextoEndereco(endereco)
        .replace(/\b\d{5}-?\d{3}\b/g, '');

    return textoSemCep
        .split(',')
        .map(limparTextoEndereco)
        .filter(Boolean)
        .some((segmento) => {
            const numeroComoSegmento = /^(?:n(?:[\u00ba\u00b0o]|umero)?\.?\s*)?\d+[a-z]?(?:\s*[-/]\s*[^\d].*)?$/i;
            const numeroNoFimDaRua = /(?:^|\s)(?:n(?:[\u00ba\u00b0o]|umero)?\.?\s*)?\d+[a-z]?$/i;
            return numeroComoSegmento.test(segmento) || numeroNoFimDaRua.test(segmento);
        });
}

export function montarEnderecoCliente(cliente) {
    const endereco = limparTextoEndereco(cliente?.endereco);
    if (!endereco) return '';
    return enderecoTemNumero(endereco) ? endereco : `${endereco}, 1`;
}
