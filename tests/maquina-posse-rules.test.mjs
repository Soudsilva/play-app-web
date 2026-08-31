import test from 'node:test';
import assert from 'node:assert/strict';

import {
    calcularAdicoesMaquinas,
    formatarErroSaldoMaquinas,
    listarMaquinasComSaldoInsuficiente,
    listarMaquinasSemIdentificador,
    movimentoExigeSaldoDisponivelMaquina
} from '../maquina-posse-rules.mjs';

test('consolida máquinas repetidas e valida somente a quantidade acrescentada', () => {
    const adicoes = calcularAdicoesMaquinas([
        { itemId: 'maq_a', categoria: 'maquina', nome: 'Máquina A', quantidade: 2 },
        { itemId: 'maq_a', categoria: 'maquina', nome: 'Máquina A', qtd: 1 },
        { itemId: 'produto_a', categoria: 'produtos', nome: 'Produto A', quantidade: 20 }
    ], [
        { itemId: 'maq_a', categoria: 'maquina', nome: 'Máquina A', quantidade: 1 }
    ]);

    assert.deepEqual(adicoes, [{ itemChave: 'maq_a', nome: 'Máquina A', quantidade: 2 }]);
});

test('bloqueia saldo zero e saldo menor que a quantidade solicitada', () => {
    const solicitadas = [
        { itemChave: 'maq_a', nome: 'Máquina A', quantidade: 1 },
        { itemChave: 'maq_b', nome: 'Máquina B', quantidade: 2 }
    ];
    const faltas = listarMaquinasComSaldoInsuficiente(solicitadas, { maq_a: 0, maq_b: 1 });

    assert.deepEqual(faltas.map(item => [item.itemChave, item.disponivel]), [
        ['maq_a', 0],
        ['maq_b', 1]
    ]);
    assert.match(formatarErroSaldoMaquinas(faltas), /saldo suficiente/i);
});

test('permite saldo positivo suficiente e ignora produtos', () => {
    const adicoes = calcularAdicoesMaquinas([
        { itemId: 'maq_a', categoria: 'maquina', nome: 'Máquina A', quantidade: 1 },
        { itemId: 'produto_a', categoria: 'produtos', nome: 'Produto A', quantidade: 50 }
    ]);

    assert.equal(listarMaquinasComSaldoInsuficiente(adicoes, { maq_a: 1 }).length, 0);
});

test('edição sem aumento de quantidade não exige novo saldo', () => {
    const itens = [{ itemId: 'maq_a', categoria: 'maquina', nome: 'Máquina A', quantidade: 1 }];
    assert.deepEqual(calcularAdicoesMaquinas(itens, itens), []);
});

test('não permite validar máquina sem identificador técnico', () => {
    assert.deepEqual(listarMaquinasSemIdentificador([
        { categoria: 'maquina', nome: 'Máquina sem ID', quantidade: 1 },
        { categoria: 'produtos', nome: 'Produto sem ID', quantidade: 10 }
    ]), ['Máquina sem ID']);
});

test('proteção final vale para entregas de máquina e preserva retiradas e cancelamentos', () => {
    assert.equal(movimentoExigeSaldoDisponivelMaquina({
        categoria: 'maquina',
        tipo: 'manutencao_adicao',
        origemRegistro: 'manutencao'
    }, -1), true);
    assert.equal(movimentoExigeSaldoDisponivelMaquina({
        categoria: 'maquina',
        tipo: 'manutencao_retirada',
        origemRegistro: 'manutencao'
    }, 1), false);
    assert.equal(movimentoExigeSaldoDisponivelMaquina({
        categoria: 'maquina',
        tipo: 'cancelamento',
        origemRegistro: 'manutencao'
    }, -1), false);
    assert.equal(movimentoExigeSaldoDisponivelMaquina({
        categoria: 'produtos',
        tipo: 'atendimento',
        origemRegistro: 'atendimento'
    }, -10), false);
});
