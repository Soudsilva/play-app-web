import test from 'node:test';
import assert from 'node:assert/strict';

import { montarEnderecoCliente } from '../selecionar-rota-rules.mjs';

test('preserva endereço completo sem repetir número nem cidade', () => {
    const endereco = 'Av. Exemplo, 451, Centro, Cidade Exemplo - RJ, 26210-220';
    assert.equal(montarEnderecoCliente({
        endereco,
        numero_endereco: '451',
        cidade: 'Outra Cidade'
    }), endereco);
});

test('acrescenta somente o número 1 quando o endereço salvo não possui número', () => {
    assert.equal(montarEnderecoCliente({
        endereco: 'Rua das Flores',
        numero_endereco: '123',
        cidade: 'Cidade Exemplo'
    }), 'Rua das Flores, 1');
});

test('nunca acrescenta a cidade armazenada em campo separado', () => {
    assert.equal(montarEnderecoCliente({
        endereco: 'Avenida Nilo Peçanha',
        cidade: 'Cidade Incorreta'
    }), 'Avenida Nilo Peçanha, 1');
});

test('reconhece número no fim do nome da rua mesmo sem vírgula', () => {
    const endereco = 'Rua Modelo 20';
    assert.equal(montarEnderecoCliente({ endereco }), endereco);
});

test('considera número com zero à esquerda existente', () => {
    const endereco = 'Rua Modelo, 06, Bairro, Cidade Exemplo - RJ, 26000-000';
    assert.equal(montarEnderecoCliente({ endereco }), endereco);
});

test('reconhece número acompanhado de complemento', () => {
    const endereco = 'Rua Modelo, 451 - Fundos, Cidade Exemplo - RJ';
    assert.equal(montarEnderecoCliente({ endereco }), endereco);
});

test('reconhece prefixos nº e n° antes do número', () => {
    for (const prefixo of ['nº', 'n°']) {
        const endereco = `Rua Modelo, ${prefixo} 451, Cidade Exemplo - RJ`;
        assert.equal(montarEnderecoCliente({ endereco }), endereco);
    }
});

test('não confunde CEP com número do imóvel', () => {
    assert.equal(
        montarEnderecoCliente({ endereco: 'Rua Modelo, Bairro, Cidade Exemplo - RJ, 12345-451' }),
        'Rua Modelo, Bairro, Cidade Exemplo - RJ, 12345-451, 1'
    );
});

test('não cria endereço a partir de número ou cidade separados', () => {
    assert.equal(montarEnderecoCliente({ numero_endereco: '10', cidade: 'Cidade Exemplo' }), '');
    assert.equal(montarEnderecoCliente(null), '');
});
