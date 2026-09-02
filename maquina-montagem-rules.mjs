function normalizarTexto(valor) {
    return String(valor || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, ' ');
}

const MODELOS_MONTAGEM_PG = Object.freeze([
    Object.freeze({ codigo: 'p_simples', nome: 'Máquina P Simples', resumo: '1 P', origens: ['P'], itens: [{ tipo: 'P', quantidade: 1 }] }),
    Object.freeze({ codigo: 'g_simples', nome: 'Máquina G Simples', resumo: '1 G', origens: ['G'], itens: [{ tipo: 'G', quantidade: 1 }] }),
    Object.freeze({ codigo: 'dupla_p', nome: 'Dupla P', resumo: '2 P', origens: ['P'], itens: [{ tipo: 'P', quantidade: 2 }] }),
    Object.freeze({ codigo: 'rack_4p', nome: 'Rack', resumo: '4 P', origens: ['P'], itens: [{ tipo: 'P', quantidade: 4 }] }),
    Object.freeze({ codigo: 'rack_3p_1g', nome: 'Rack', resumo: '3 P + 1 G', origens: ['P', 'G'], itens: [{ tipo: 'P', quantidade: 3 }, { tipo: 'G', quantidade: 1 }] }),
    Object.freeze({ codigo: 'rack_2p_2g', nome: 'Rack', resumo: '2 P + 2 G', origens: ['P', 'G'], itens: [{ tipo: 'P', quantidade: 2 }, { tipo: 'G', quantidade: 2 }] })
]);

export function obterTipoMaquinaPG(itemOuNome) {
    const nome = normalizarTexto(typeof itemOuNome === 'object' ? itemOuNome?.nome : itemOuNome);
    if (nome === 'maquina p') return 'P';
    if (nome === 'maquina g') return 'G';
    return '';
}

export function listarModelosMontagemPG(itemOuNome) {
    const tipoOrigem = obterTipoMaquinaPG(itemOuNome);
    if (!tipoOrigem) return [];
    return MODELOS_MONTAGEM_PG
        .filter(modelo => modelo.origens.includes(tipoOrigem))
        .map(modelo => ({
            ...modelo,
            origens: [...modelo.origens],
            itens: modelo.itens.map(item => ({ ...item }))
        }));
}

export function obterModeloMontagemPG(codigo, itemOuNome) {
    return listarModelosMontagemPG(itemOuNome).find(modelo => modelo.codigo === String(codigo || '').trim()) || null;
}
