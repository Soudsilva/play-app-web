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

export function extrairClienteDaDescricaoHistorico(movimento) {
    const descricao = String(movimento?.descricao || '').trim();
    const prefixos = ['Serviço realizado - ', 'Cadastro de cliente - ', 'Atendimento - '];
    const prefixo = prefixos.find(item => descricao.toLocaleLowerCase('pt-BR').startsWith(item.toLocaleLowerCase('pt-BR')));
    return prefixo ? descricao.slice(prefixo.length).trim() : '';
}

export function formatarRotuloHistoricoMaquina(movimento) {
    const tipo = normalizarTipoMovimentacaoMaquina(movimento);
    const cliente = extrairClienteDaDescricaoHistorico(movimento);
    const rotulos = {
        saida_estoque: 'Retirada - Estoque',
        entrada_estoque: 'Devolução ao estoque',
        atendimento: `Entregue - ${cliente || 'Cliente'}`,
        cadastro_cliente: `Entregue - ${cliente || 'Cliente'}`,
        manutencao: `Reposição${cliente ? ` - ${cliente}` : ''}`,
        manutencao_adicao: `Entregue - ${cliente || 'Cliente'}`,
        manutencao_retirada: `Retirada - ${cliente || 'Cliente'}`,
        ajuste_balanco_gestor: 'Ajuste do gestor',
        balanco_aprovado: 'Conferido',
        cancelamento: 'Cancelamento'
    };
    return rotulos[tipo] || String(movimento?.descricao || tipo || 'Movimentação').trim();
}

export function formatarDescricaoComplementarHistorico(movimento) {
    const tipo = normalizarTipoMovimentacaoMaquina(movimento);
    const descricao = String(movimento?.descricao || '').trim();
    if (tipo === 'balanco_aprovado') return '';
    if (tipo === 'cancelamento') {
        const prefixo = 'Cancelamento:';
        if (descricao.toLocaleLowerCase('pt-BR').startsWith(prefixo.toLocaleLowerCase('pt-BR'))) {
            return descricao.slice(prefixo.length).trim();
        }
    }
    if (['manutencao', 'manutencao_adicao', 'manutencao_retirada'].includes(tipo)
        && extrairClienteDaDescricaoHistorico(movimento)) {
        return '';
    }
    if (['atendimento', 'cadastro_cliente'].includes(tipo)
        && extrairClienteDaDescricaoHistorico(movimento)) {
        return '';
    }
    if (tipo === 'saida_estoque' && normalizarTextoComparacao(descricao) === 'retirada de estoque') return '';
    return descricao;
}

function normalizarTextoComparacao(valor) {
    return String(valor || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .trim()
        .toLowerCase();
}

export function ordenarMovimentacoesMaisRecentes(movimentos) {
    return [...(Array.isArray(movimentos) ? movimentos : [])].sort((a, b) => {
        const dataA = Date.parse(a?.timestamp || a?.data || '');
        const dataB = Date.parse(b?.timestamp || b?.data || '');
        const tempoA = Number.isFinite(dataA) ? dataA : 0;
        const tempoB = Number.isFinite(dataB) ? dataB : 0;
        return tempoB - tempoA
            || String(b?.id || b?.firebaseUrl || '').localeCompare(String(a?.id || a?.firebaseUrl || ''));
    });
}

export function selecionarMovimentacoesDoCard(movimentos, expandido, limiteExpandido = 10) {
    const lista = Array.isArray(movimentos) ? movimentos : [];
    const limite = expandido
        ? Math.max(2, Math.min(Number(limiteExpandido) || 10, 30))
        : 2;
    return lista.slice(0, limite);
}

export function movimentoEhConferenciaDeTotal(movimento) {
    return normalizarTipoMovimentacaoMaquina(movimento) === 'balanco_aprovado';
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
