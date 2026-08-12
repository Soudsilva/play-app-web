import test from 'node:test';
import assert from 'node:assert/strict';
import {
    processarAtendimentoPendente,
    ordenarPendenciasAtendimento,
    validarConfirmacaoAtendimentoRemoto,
    validarConfirmacaoProdutosAtendimento
} from '../atendimento-fila-sync.mjs';
import { obterStatusVisualAtendimento } from '../verificar-envios-sync-rules.mjs';

const dadosBase = {
    atendente: 'Usuario Teste',
    cliente: { id: 'cliente-1', rota: '18' },
    produtos: [{ nome: 'Amoeba', quantidade: 2 }],
    fotos: {
        ficha: 'https://storage/ficha.jpg',
        maquinas: [{ nome: 'Maquina P', url: 'https://storage/maquina.jpg' }],
        pix: []
    },
    fotosPendentes: false
};

test('confirma atendimento somente quando registro e fotos estão remotos', () => {
    assert.equal(validarConfirmacaoAtendimentoRemoto({ ...dadosBase }, dadosBase), true);
    assert.throws(() => validarConfirmacaoAtendimentoRemoto({
        ...dadosBase,
        fotos: { ...dadosBase.fotos, ficha: 'data:image/jpeg;base64,abc' }
    }, dadosBase), /Foto da ficha ainda nao foi confirmada/);
});

test('confirma efeitos somente quando quantidades dos produtos correspondem', () => {
    assert.equal(validarConfirmacaoProdutosAtendimento(dadosBase, [
        { nome: 'Amoeba', movimento: -2 }
    ]), true);
    assert.throws(() => validarConfirmacaoProdutosAtendimento(dadosBase, [
        { nome: 'Amoeba', movimento: 1 }
    ]), /Efeito do produto ainda nao confirmado/);
});

test('retomada após efeitos confirmados executa somente rota e confirmação remota', async () => {
    const chamadas = [];
    let itemAtual = {
        id: 'fila-1',
        atendimentoServidorId: 'atendimento-1',
        fase: 'efeitos_confirmados',
        dados: structuredClone(dadosBase)
    };

    await processarAtendimentoPendente({
        item: itemAtual,
        atualizarItem: async patch => {
            itemAtual = { ...itemAtual, ...patch };
            chamadas.push(`fase:${patch.fase}`);
            return itemAtual;
        },
        gerarAtendimentoId: async () => { chamadas.push('gerar-id'); return 'outro-id'; },
        salvarFoto: async () => { chamadas.push('foto'); return {}; },
        salvarAtendimento: async () => chamadas.push('atendimento'),
        sincronizarProdutos: async () => chamadas.push('produtos'),
        confirmarProdutos: async () => chamadas.push('confirmar-produtos'),
        verificarRota: async () => { chamadas.push('rota'); return { liberada: false }; },
        confirmarRota: async () => chamadas.push('confirmar-rota'),
        confirmarAtendimento: async () => chamadas.push('confirmar-atendimento')
    });

    assert.deepEqual(chamadas, [
        'rota',
        'confirmar-rota',
        'fase:rota_confirmada',
        'confirmar-atendimento',
        'fase:confirmacao_remota'
    ]);
});

test('fluxo completo confirma fotos, atendimento, efeitos, rota e leitura final', async () => {
    const fases = [];
    let itemAtual = {
        id: 'fila-completa',
        atendimentoServidorId: 'atendimento-completo',
        fase: 'salvo_localmente',
        dados: {
            ...structuredClone(dadosBase),
            fotos: {
                ficha: 'data:image/jpeg;base64,ficha',
                maquinas: [{ nome: 'Maquina P', url: 'data:image/jpeg;base64,maquina' }],
                pix: []
            }
        }
    };
    let numeroFoto = 0;

    const resultado = await processarAtendimentoPendente({
        item: itemAtual,
        atualizarItem: async patch => {
            itemAtual = { ...itemAtual, ...patch };
            if (patch.fase) fases.push(patch.fase);
            return itemAtual;
        },
        salvarFoto: async () => {
            numeroFoto += 1;
            return {
                url: `https://storage/foto-${numeroFoto}.jpg`,
                thumbUrl: `https://storage/thumb-${numeroFoto}.jpg`
            };
        },
        salvarAtendimento: async () => {},
        sincronizarProdutos: async () => ({ caminhos: ['movimentacao_balanco_historico/usuario/movimento-1'] }),
        confirmarProdutos: async (_resultado, dados) => validarConfirmacaoProdutosAtendimento(dados, [
            { nome: 'Amoeba', movimento: 2 }
        ]),
        verificarRota: async () => ({ liberada: false }),
        confirmarRota: async () => true,
        confirmarAtendimento: async (_id, dados) => validarConfirmacaoAtendimentoRemoto(dados, dados)
    });

    assert.equal(resultado.atendimentoId, 'atendimento-completo');
    assert.deepEqual(fases, [
        'fotos_enviando',
        'fotos_enviando',
        'fotos_confirmadas',
        'atendimento_confirmado',
        'efeitos_confirmados',
        'rota_confirmada',
        'confirmacao_remota'
    ]);
});

test('falha na leitura final mantém a fase pronta para nova confirmação', async () => {
    let itemAtual = {
        id: 'fila-2',
        atendimentoServidorId: 'atendimento-2',
        fase: 'rota_confirmada',
        dados: structuredClone(dadosBase)
    };

    await assert.rejects(() => processarAtendimentoPendente({
        item: itemAtual,
        atualizarItem: async patch => {
            itemAtual = { ...itemAtual, ...patch };
            return itemAtual;
        },
        salvarFoto: async () => ({}),
        salvarAtendimento: async () => {},
        confirmarAtendimento: async () => { throw new Error('leitura indisponivel'); }
    }), /leitura indisponivel/);

    assert.equal(itemAtual.fase, 'rota_confirmada');
});

test('status visual alterna entre tique, spinner e confirmação', () => {
    const pendente = {
        _statusSincronizacao: 'pendente',
        _sincronizacaoLocal: { estado: 'pendente', leaseAte: 0, prioridadeEnvio: false }
    };
    const enviando = {
        _statusSincronizacao: 'pendente',
        _sincronizacaoLocal: { estado: 'enviando', leaseAte: 2000, prioridadeEnvio: false }
    };
    const prioritario = {
        _statusSincronizacao: 'pendente',
        _sincronizacaoLocal: { estado: 'pendente', leaseAte: 0, prioridadeEnvio: true }
    };

    assert.equal(obterStatusVisualAtendimento(pendente, 1000).classe, 'pendente');
    assert.equal(obterStatusVisualAtendimento(enviando, 1000).classe, 'enviando');
    assert.equal(obterStatusVisualAtendimento(enviando, 3000).classe, 'pendente');
    assert.equal(obterStatusVisualAtendimento(prioritario, 1000).classe, 'enviando');
    assert.equal(obterStatusVisualAtendimento({}, 1000).classe, 'confirmado');
});

test('atendimento priorizado passa à frente sem alterar a ordem dos demais', () => {
    const fila = ordenarPendenciasAtendimento([
        { id: 'primeiro', criadoEm: '2026-08-12T10:00:00.000Z' },
        { id: 'prioritario', criadoEm: '2026-08-12T12:00:00.000Z', prioridadeEnvio: true },
        { id: 'segundo', criadoEm: '2026-08-12T11:00:00.000Z' }
    ]);
    assert.deepEqual(fila.map(item => item.id), ['prioritario', 'primeiro', 'segundo']);
});
