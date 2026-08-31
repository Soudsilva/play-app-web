export function normalizarChaveUsuarioBalanco(valor) {
    return String(valor || '').trim().replace(/[.#$/[\]]/g, '_');
}

export function normalizarChaveItemBalanco(valor) {
    return String(valor || '').trim().replace(/[.#$/[\]]/g, '_');
}

export function normalizarTipoMovimentacaoMaquina(movimento) {
    const tipo = String(movimento?.tipo || movimento?.origemRegistro || '').trim();
    const origem = String(movimento?.origemRegistro || '').trim();
    const categoria = String(movimento?.categoria || movimento?.itemCategoria || '').trim();
    const quantidade = Number(movimento?.movimento || 0);
    const ehAdicaoLegadaEmManutencao = tipo === 'manutencao'
        && origem === 'manutencao'
        && categoria === 'maquina'
        && quantidade < 0;
    return ehAdicaoLegadaEmManutencao ? 'manutencao_adicao' : tipo;
}

export function perfilPodeAparecerNaAuditoriaMaquinas(colaborador) {
    const perfil = String(colaborador?.nivel_completo || '').trim().toLowerCase();
    return perfil.includes('atendimento_2');
}

export function listarUsuariosAuditaveis(colaboradores) {
    const nomes = new Set();
    (Array.isArray(colaboradores) ? colaboradores : []).forEach(colaborador => {
        if (!perfilPodeAparecerNaAuditoriaMaquinas(colaborador)) return;
        const nome = String(colaborador?.nome || colaborador?.usuario || '').trim();
        if (nome) nomes.add(nome);
    });
    return [...nomes].sort((a, b) => a.localeCompare(b, 'pt-BR'));
}

export function extrairMaquinasDaPosse(dados) {
    return Object.entries(dados && typeof dados === 'object' ? dados : {})
        .map(([chave, valor]) => ({
            itemChave: String(valor?.itemChave || valor?.itemId || chave || '').trim(),
            nome: String(valor?.itemNome || '').trim(),
            categoria: String(valor?.categoria || '').trim(),
            quantidade: Number(valor?.quantidade || 0),
            atualizadoEm: String(valor?.atualizadoEm || '').trim()
        }))
        .filter(item => item.itemChave && item.nome && item.categoria === 'maquina')
        .sort((a, b) => {
            const grupoA = a.quantidade > 0 ? 0 : 1;
            const grupoB = b.quantidade > 0 ? 0 : 1;
            return grupoA - grupoB
                || a.nome.localeCompare(b.nome, 'pt-BR')
                || a.itemChave.localeCompare(b.itemChave);
        });
}

export function montarPaginaMovimentacoes(dados, limite, cursorId = '') {
    const tamanho = Math.max(1, Math.min(Number(limite) || 10, 30));
    const cursor = String(cursorId || '').trim();
    const registros = Object.entries(dados && typeof dados === 'object' ? dados : {})
        .map(([id, valor]) => ({ id, firebaseUrl: id, ...valor }))
        .filter(item => !cursor || item.id !== cursor)
        .sort((a, b) => String(b.id).localeCompare(String(a.id)));
    const temMais = registros.length > tamanho;
    const movimentos = registros.slice(0, tamanho);
    return {
        movimentos,
        temMais,
        proximoCursor: movimentos.length ? String(movimentos[movimentos.length - 1].id) : ''
    };
}
