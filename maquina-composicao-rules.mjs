import { listarModelosMontagemPG, obterTipoMaquinaPG } from './maquina-montagem-rules.mjs';
import { ehTrocaFichas, listarModelosTrocaFichas } from './troca-fichas-rules.mjs';

function texto(valor) {
    return String(valor || '').trim();
}

function numeroPositivo(valor, padrao = 0) {
    const numero = Number(valor);
    return Number.isFinite(numero) && numero > 0 ? numero : padrao;
}

function obterItemMaquinaPorTipo(estoque, tipo) {
    return (Array.isArray(estoque) ? estoque : []).find(item =>
        texto(item?.categoria).toLowerCase() === 'maquina' && obterTipoMaquinaPG(item) === tipo
    ) || null;
}

export function maquinaExigeComposicao(itemOuNome) {
    return Boolean(obterTipoMaquinaPG(itemOuNome) || ehTrocaFichas(itemOuNome));
}

export function listarConfiguracoesMaquina(itemOrigem, estoque, opcoes = {}) {
    const modelos = obterTipoMaquinaPG(itemOrigem)
        ? listarModelosMontagemPG(itemOrigem)
        : listarModelosTrocaFichas(itemOrigem);
    const exigirValor = opcoes?.exigirValor === true;
    const exigirEstoque = opcoes?.exigirEstoque === true;

    return modelos.map(modelo => {
        const componentes = modelo.itens.map(({ tipo, quantidade }) => {
            const item = obterItemMaquinaPorTipo(estoque, tipo);
            const valorUnitario = Number(item?.valorEquipamento || 0);
            return {
                tipo,
                itemId: texto(item?.firebaseUrl),
                equipamentoId: texto(item?.firebaseUrl),
                nome: texto(item?.nome) || `Máquina ${tipo}`,
                quantidade,
                valorUnitario,
                valorTotal: valorUnitario * quantidade,
                quantidadeEstoque: Number.parseInt(item?.quantidade || 0, 10) || 0
            };
        });
        const componentesCadastrados = componentes.every(item => item.itemId);
        const valoresValidos = componentes.every(item => item.valorUnitario > 0);
        const estoqueDisponivel = componentes.every(item => item.quantidadeEstoque >= item.quantidade);
        const disponivel = componentesCadastrados
            && (!exigirValor || valoresValidos)
            && (!exigirEstoque || estoqueDisponivel);

        return {
            ...modelo,
            componentes,
            valorTotal: componentes.reduce((total, item) => total + item.valorTotal, 0),
            disponivel,
            motivoIndisponivel: !componentesCadastrados
                ? 'Máquina não cadastrada'
                : (exigirValor && !valoresValidos)
                    ? 'Valor não cadastrado'
                    : (exigirEstoque && !estoqueDisponivel ? 'Estoque insuficiente' : '')
        };
    });
}

export function criarRegistroMaquinaComposta({ itemOrigem, configuracao, quantidade = 1 }) {
    if (!itemOrigem || !configuracao?.codigo || !Array.isArray(configuracao?.componentes)) return null;
    const qtdMontagens = numeroPositivo(quantidade, 1);
    return {
        itemId: texto(itemOrigem?.firebaseUrl || itemOrigem?.itemId),
        maquinaBaseItemId: texto(itemOrigem?.firebaseUrl || itemOrigem?.itemId),
        maquinaBaseNome: texto(itemOrigem?.nome),
        categoria: 'maquina',
        nome: texto(configuracao.nome),
        qtd: String(qtdMontagens),
        codigoMontagem: texto(configuracao.codigo),
        composicaoResumo: texto(configuracao.resumo),
        composicao: configuracao.componentes.map(item => ({
            itemId: texto(item?.itemId || item?.equipamentoId),
            equipamentoId: texto(item?.itemId || item?.equipamentoId),
            nome: texto(item?.nome),
            quantidade: numeroPositivo(item?.quantidade),
            valorUnitario: Number(item?.valorUnitario || 0)
        })).filter(item => item.itemId && item.quantidade > 0)
    };
}

export function expandirEquipamentosEmComponentes(itens) {
    const expandidos = [];
    (Array.isArray(itens) ? itens : []).forEach(item => {
        const quantidadeMontagens = numeroPositivo(item?.qtd ?? item?.quantidade);
        const composicao = Array.isArray(item?.composicao) ? item.composicao : [];
        if (composicao.length === 0) {
            expandidos.push({ ...item });
            return;
        }

        composicao.forEach(componente => {
            const quantidadeComponente = numeroPositivo(componente?.quantidade);
            const itemId = texto(componente?.itemId || componente?.equipamentoId);
            if (quantidadeMontagens <= 0 || quantidadeComponente <= 0) return;
            expandidos.push({
                ...componente,
                itemId,
                itemChave: itemId,
                refId: itemId,
                categoria: 'maquina',
                nome: texto(componente?.nome) || 'Máquina',
                qtd: String(quantidadeComponente * quantidadeMontagens),
                quantidade: quantidadeComponente * quantidadeMontagens,
                codigoMontagemOrigem: texto(item?.codigoMontagem),
                composicaoResumoOrigem: texto(item?.composicaoResumo)
            });
        });
    });
    return expandidos;
}

export function obterCamposComposicaoPersistidos(item) {
    const composicao = Array.isArray(item?.composicao)
        ? item.composicao.map(componente => ({
            itemId: texto(componente?.itemId || componente?.equipamentoId),
            equipamentoId: texto(componente?.itemId || componente?.equipamentoId),
            nome: texto(componente?.nome),
            quantidade: numeroPositivo(componente?.quantidade),
            valorUnitario: Number(componente?.valorUnitario || 0)
        })).filter(componente => componente.itemId && componente.quantidade > 0)
        : [];
    if (!texto(item?.codigoMontagem) || composicao.length === 0) return {};
    return {
        maquinaBaseItemId: texto(item?.maquinaBaseItemId || item?.itemId),
        maquinaBaseNome: texto(item?.maquinaBaseNome),
        codigoMontagem: texto(item?.codigoMontagem),
        composicaoResumo: texto(item?.composicaoResumo),
        composicao
    };
}

export function formatarNomeMaquinaComposta(item) {
    const nome = texto(item?.nome);
    const resumo = texto(item?.composicaoResumo);
    return resumo ? `${nome} — ${resumo}` : nome;
}
