import test from 'node:test';
import assert from 'node:assert/strict';

import {
    extrairMaquinasDaPosse,
    formatarDescricaoComplementarHistorico,
    formatarRotuloHistoricoMaquina,
    listarUsuariosAuditaveis,
    montarPaginaMovimentacoes,
    movimentoEhConferenciaDeTotal,
    normalizarTipoMovimentacaoMaquina,
    ordenarMovimentacoesMaisRecentes,
    selecionarMovimentacoesDoCard
} from '../balanco-maquinas-gestor-rules.mjs';

test('lista somente Atendimento 2 sem duplicar usuários', () => {
    const lista = listarUsuariosAuditaveis([
        { nome: 'Usuário Teste B', nivel_completo: 'atendimento_2' },
        { nome: 'Usuário Teste A', nivel_completo: 'atendimento_1' },
        { nome: 'Usuário Teste A', nivel_completo: 'atendimento_1' },
        { nome: 'Usuário Teste C', nivel_completo: 'atendimento_3' },
        { nome: 'Usuário Teste D', nivel_completo: 'gestao_2' }
    ]);
    assert.deepEqual(lista, ['Usuário Teste B']);
});

test('coloca saldos positivos primeiro, preserva saldo zero e exclui outras categorias', () => {
    const maquinas = extrairMaquinasDaPosse({
        maquina_a: { itemNome: 'Máquina Teste A', categoria: 'maquina', quantidade: 0 },
        produto_a: { itemNome: 'Produto Teste', categoria: 'produtos', quantidade: 3 },
        maquina_z: { itemNome: 'Máquina Teste Z', categoria: 'maquina', quantidade: 2 }
    });
    assert.deepEqual(maquinas.map(item => [item.itemChave, item.quantidade]), [
        ['maquina_z', 2],
        ['maquina_a', 0]
    ]);
});

test('monta página estável, remove o cursor repetido e informa próxima página', () => {
    const dados = {
        id_05: { itemChave: 'maquina_a' },
        id_04: { itemChave: 'maquina_a' },
        id_03: { itemChave: 'maquina_a' },
        id_02: { itemChave: 'maquina_a' }
    };
    const primeira = montarPaginaMovimentacoes(dados, 2);
    assert.deepEqual(primeira.movimentos.map(item => item.id), ['id_05', 'id_04']);
    assert.equal(primeira.temMais, true);
    assert.equal(primeira.proximoCursor, 'id_04');

    const seguinte = montarPaginaMovimentacoes({
        id_04: dados.id_04,
        id_03: dados.id_03,
        id_02: dados.id_02
    }, 2, 'id_04');
    assert.deepEqual(seguinte.movimentos.map(item => item.id), ['id_03', 'id_02']);
    assert.equal(seguinte.temMais, false);
});

test('interpreta máquina adicionada em manutenção antiga como adição', () => {
    assert.equal(normalizarTipoMovimentacaoMaquina({
        tipo: 'manutencao',
        origemRegistro: 'manutencao',
        categoria: 'maquina',
        movimento: -4
    }), 'manutencao_adicao');
});

test('preserva reposição de produto e retirada de máquina', () => {
    assert.equal(normalizarTipoMovimentacaoMaquina({
        tipo: 'manutencao',
        origemRegistro: 'manutencao',
        categoria: 'produto',
        movimento: -4
    }), 'manutencao');
    assert.equal(normalizarTipoMovimentacaoMaquina({
        tipo: 'manutencao_retirada',
        origemRegistro: 'manutencao',
        categoria: 'maquina',
        movimento: 1
    }), 'manutencao_retirada');
});

test('ordena as duas movimentações do resumo da mais recente para a mais antiga', () => {
    const movimentos = ordenarMovimentacoesMaisRecentes([
        { id: 'cadastro_julho', timestamp: '2026-07-13T14:42:00.000Z' },
        { id: 'cancelamento', timestamp: '2026-08-20T18:54:00.000Z' },
        { id: 'conferido', timestamp: '2026-08-21T11:22:00.000Z' },
        { id: 'movimento_junho', timestamp: '2026-06-10T12:00:00.000Z' }
    ]).slice(0, 2);

    assert.deepEqual(movimentos.map(item => item.id), ['conferido', 'cancelamento']);
});

test('valor conferido representa o total e não uma movimentação zero', () => {
    assert.equal(movimentoEhConferenciaDeTotal({ tipo: 'balanco_aprovado', movimento: 0, totalApos: 1 }), true);
    assert.equal(movimentoEhConferenciaDeTotal({ tipo: 'cancelamento', movimento: -1, totalApos: 1 }), false);
    assert.equal(formatarRotuloHistoricoMaquina({ tipo: 'balanco_aprovado' }), 'Conferido');
    assert.equal(formatarDescricaoComplementarHistorico({
        tipo: 'balanco_aprovado',
        descricao: 'Valor conferido'
    }), '');
});

test('cancelamento não repete o nome no complemento', () => {
    assert.equal(formatarDescricaoComplementarHistorico({
        tipo: 'cancelamento',
        descricao: 'Cancelamento: Retirada de estoque'
    }), 'Retirada de estoque');
});

test('card limita o histórico recolhido e expandido', () => {
    const movimentos = Array.from({ length: 12 }, (_, indice) => ({ id: `mov_${indice + 1}` }));
    assert.deepEqual(
        selecionarMovimentacoesDoCard(movimentos, false).map(item => item.id),
        ['mov_1', 'mov_2']
    );
    assert.equal(selecionarMovimentacoesDoCard(movimentos, true).length, 10);
});

test('retirada mostra a origem física em vez da tela utilizada', () => {
    const retiradaCliente = {
        tipo: 'manutencao_retirada',
        descricao: 'Serviço realizado - Cliente Teste'
    };
    assert.equal(formatarRotuloHistoricoMaquina(retiradaCliente), 'Retirada - Cliente Teste');
    assert.equal(formatarDescricaoComplementarHistorico(retiradaCliente), '');
    assert.equal(formatarRotuloHistoricoMaquina({ tipo: 'manutencao_retirada' }), 'Retirada - Cliente');
    assert.equal(formatarRotuloHistoricoMaquina({ tipo: 'saida_estoque' }), 'Retirada - Estoque');
    assert.equal(formatarDescricaoComplementarHistorico({
        tipo: 'saida_estoque',
        descricao: 'Retirada de estoque'
    }), '');
});

test('cadastro e atendimento mostram o cliente como destino da entrega', () => {
    const cadastro = {
        tipo: 'cadastro_cliente',
        descricao: 'Cadastro de cliente - Cliente Teste'
    };
    const atendimento = {
        tipo: 'atendimento',
        descricao: 'Atendimento - Cliente Teste'
    };
    assert.equal(formatarRotuloHistoricoMaquina(cadastro), 'Entregue - Cliente Teste');
    assert.equal(formatarDescricaoComplementarHistorico(cadastro), '');
    assert.equal(formatarRotuloHistoricoMaquina(atendimento), 'Entregue - Cliente Teste');
    assert.equal(formatarDescricaoComplementarHistorico(atendimento), '');
});

test('máquina adicionada em manutenção também aparece como entregue ao cliente', () => {
    const movimento = {
        tipo: 'manutencao_adicao',
        descricao: 'Serviço realizado - Cliente Teste'
    };
    assert.equal(formatarRotuloHistoricoMaquina(movimento), 'Entregue - Cliente Teste');
    assert.equal(formatarDescricaoComplementarHistorico(movimento), '');
});
