function normalizarTexto(valor) {
    return String(valor || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, ' ');
}

const MODELOS_TROCA_FICHAS = Object.freeze([
    Object.freeze({ codigo: 'troca_fichas_4p', nome: 'Troca Fichas', resumo: '4 P', itens: [{ tipo: 'P', quantidade: 4 }] }),
    Object.freeze({ codigo: 'troca_fichas_3p_1g', nome: 'Troca Fichas', resumo: '3 P + 1 G', itens: [{ tipo: 'P', quantidade: 3 }, { tipo: 'G', quantidade: 1 }] }),
    Object.freeze({ codigo: 'troca_fichas_2p_2g', nome: 'Troca Fichas', resumo: '2 P + 2 G', itens: [{ tipo: 'P', quantidade: 2 }, { tipo: 'G', quantidade: 2 }] })
]);

export function ehTrocaFichas(itemOuNome) {
    const nome = normalizarTexto(typeof itemOuNome === 'object' ? itemOuNome?.nome : itemOuNome);
    return nome === 'troca fichas';
}

export function listarModelosTrocaFichas(itemOuNome) {
    if (!ehTrocaFichas(itemOuNome)) return [];
    return MODELOS_TROCA_FICHAS.map(modelo => ({
        ...modelo,
        itens: modelo.itens.map(item => ({ ...item }))
    }));
}

export function obterModeloTrocaFichas(codigo, itemOuNome) {
    return listarModelosTrocaFichas(itemOuNome).find(modelo => modelo.codigo === String(codigo || '').trim()) || null;
}
