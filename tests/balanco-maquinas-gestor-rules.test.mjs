import test from 'node:test';
import assert from 'node:assert/strict';

import {
    extrairMaquinasDaPosse,
    listarUsuariosAuditaveis,
    montarPaginaMovimentacoes
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
