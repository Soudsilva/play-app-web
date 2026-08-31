function normalizarTexto(valor) {
    return String(valor || '').trim().toLowerCase();
}

function obterItemChave(item) {
    return String(item?.itemId || item?.itemChave || item?.refId || '').trim();
}

function obterQuantidade(item) {
    const quantidade = Number(item?.quantidade ?? item?.qtd ?? 0);
    return Number.isFinite(quantidade) && quantidade > 0 ? quantidade : 0;
}

export function consolidarQuantidadesMaquinas(itens) {
    const consolidadas = new Map();
    (Array.isArray(itens) ? itens : []).forEach(item => {
        if (normalizarTexto(item?.categoria) !== 'maquina') return;
        const itemChave = obterItemChave(item);
        const quantidade = obterQuantidade(item);
        if (!itemChave || quantidade <= 0) return;

        const atual = consolidadas.get(itemChave) || {
            itemChave,
            nome: String(item?.nome || item?.itemNome || 'Máquina').trim() || 'Máquina',
            quantidade: 0
        };
        atual.quantidade += quantidade;
        consolidadas.set(itemChave, atual);
    });
    return [...consolidadas.values()];
}

export function listarMaquinasSemIdentificador(itens) {
    return (Array.isArray(itens) ? itens : [])
        .filter(item => normalizarTexto(item?.categoria) === 'maquina')
        .filter(item => obterQuantidade(item) > 0 && !obterItemChave(item))
        .map(item => String(item?.nome || item?.itemNome || 'Máquina').trim() || 'Máquina');
}

export function calcularAdicoesMaquinas(itensAtuais, itensAnteriores = []) {
    const atuais = consolidarQuantidadesMaquinas(itensAtuais);
    const anteriores = new Map(
        consolidarQuantidadesMaquinas(itensAnteriores)
            .map(item => [item.itemChave, item.quantidade])
    );

    return atuais
        .map(item => ({
            ...item,
            quantidade: Math.max(item.quantidade - Number(anteriores.get(item.itemChave) || 0), 0)
        }))
        .filter(item => item.quantidade > 0);
}

export function listarMaquinasComSaldoInsuficiente(solicitadas, saldosPorItem = {}) {
    return (Array.isArray(solicitadas) ? solicitadas : [])
        .map(item => {
            const disponivel = Number(saldosPorItem?.[item.itemChave] || 0);
            return {
                ...item,
                disponivel: Number.isFinite(disponivel) ? disponivel : 0
            };
        })
        .filter(item => item.disponivel < Number(item.quantidade || 0));
}

export function movimentoExigeSaldoDisponivelMaquina(entrada, movimento) {
    if (normalizarTexto(entrada?.categoria) !== 'maquina' || Number(movimento || 0) >= 0) return false;
    if (normalizarTexto(entrada?.tipo) === 'cancelamento') return false;

    const origem = normalizarTexto(entrada?.origemRegistro || entrada?.tipo);
    return ['atendimento', 'manutencao', 'cadastro_cliente', 'reposicao_cliente'].includes(origem);
}

export function formatarErroSaldoMaquinas(faltas) {
    const itens = Array.isArray(faltas) ? faltas : [];
    if (itens.length === 0) return '';
    if (itens.length === 1) {
        const item = itens[0];
        return `Você não possui saldo suficiente de ${item.nome} no seu balanço. Disponível: ${item.disponivel}. Necessário: ${item.quantidade}.`;
    }
    const detalhes = itens
        .map(item => `${item.nome} (disponível: ${item.disponivel}; necessário: ${item.quantidade})`)
        .join(', ');
    return `Você não possui saldo suficiente destas máquinas no seu balanço: ${detalhes}.`;
}
