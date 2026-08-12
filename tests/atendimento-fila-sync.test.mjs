import test from 'node:test';
import assert from 'node:assert/strict';
import {
    processarAtendimentoPendente,
    ordenarPendenciasAtendimento,
    prepararAtendimentoComFotosPendentes,
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

test('prepara o texto para o Firebase sem incluir Base64 e marca somente as fotos pendentes', () => {
    const preparado = prepararAtendimentoComFotosPendentes({
        ...structuredClone(dadosBase),
        fotos: {
            ficha: 'data:image/jpeg;base64,ficha',
            maquinas: [
                { nome: 'Maquina P', url: 'data:image/jpeg;base64,maquina' },
                { nome: 'Maquina G', url: 'https://storage/existente.jpg' }
            ],
            pix: []
        }
    }, { id: 'fila-texto' });

    assert.equal(preparado.fotosPendentes, true);
    assert.equal(preparado.fotos.ficha, null);
    assert.equal(preparado.fotos.fichaPendente, true);
    assert.equal(preparado.fotos.maquinas[0].url, null);
    assert.equal(preparado.fotos.maquinas[0].uploadPendente, true);
    assert.equal(preparado.fotos.maquinas[1].url, 'https://storage/existente.jpg');
    assert.equal(JSON.stringify(preparado).includes('data:image'), false);
});

test('envio inicial acionado pelo formulario grava somente o texto e conserva a fila', async () => {
    const chamadas = [];
    let itemAtual = {
        id: 'fila-envio-inicial',
        atendimentoServidorId: 'atendimento-envio-inicial',
        fase: 'salvo_localmente',
        dados: {
            ...structuredClone(dadosBase),
            fotos: {
                ficha: 'data:image/jpeg;base64,ficha',
                maquinas: [],
                pix: []
            }
        }
    };

    const resultado = await processarAtendimentoPendente({
        item: itemAtual,
        pararAposRegistroInicial: true,
        atualizarItem: async patch => {
            itemAtual = { ...itemAtual, ...patch };
            chamadas.push(`fase:${patch.fase}`);
            return itemAtual;
        },
        salvarAtendimento: async dados => {
            chamadas.push('texto');
            assert.equal(dados.fotosPendentes, true);
            assert.equal(JSON.stringify(dados).includes('data:image'), false);
        },
        salvarFoto: async () => chamadas.push('foto'),
        atualizarFotoAtendimento: async () => chamadas.push('foto-remota'),
        sincronizarProdutos: async () => chamadas.push('produtos'),
        confirmarAtendimento: async () => chamadas.push('confirmacao')
    });

    assert.equal(resultado.parcial, true);
    assert.equal(itemAtual.registroInicialConfirmado, true);
    assert.match(itemAtual.dados.fotos.ficha, /^data:/);
    assert.deepEqual(chamadas, ['texto', 'fase:atendimento_pendente_confirmado']);
});

test('retomada após efeitos confirmados executa somente rota e confirmação remota', async () => {
    const chamadas = [];
    let itemAtual = {
        id: 'fila-1',
        atendimentoServidorId: 'atendimento-1',
        fase: 'efeitos_confirmados',
        registroInicialConfirmado: true,
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
    const ordem = [];

    const resultado = await processarAtendimentoPendente({
        item: itemAtual,
        atualizarItem: async patch => {
            itemAtual = { ...itemAtual, ...patch };
            if (patch.fase) fases.push(patch.fase);
            return itemAtual;
        },
        salvarFoto: async () => {
            ordem.push('upload');
            numeroFoto += 1;
            return {
                url: `https://storage/foto-${numeroFoto}.jpg`,
                thumbUrl: `https://storage/thumb-${numeroFoto}.jpg`
            };
        },
        salvarAtendimento: async (dados, id, opcoes) => {
            ordem.push('texto');
            assert.equal(id, 'atendimento-completo');
            assert.equal(opcoes.recalcularRemuneracoes, false);
            assert.equal(dados.fotosPendentes, true);
            assert.equal(JSON.stringify(dados).includes('data:image'), false);
        },
        atualizarFotoAtendimento: async referencia => ordem.push(`foto-remota:${referencia.tipo}`),
        sincronizarProdutos: async () => ({ caminhos: ['movimentacao_balanco_historico/usuario/movimento-1'] }),
        confirmarProdutos: async (_resultado, dados) => validarConfirmacaoProdutosAtendimento(dados, [
            { nome: 'Amoeba', movimento: 2 }
        ]),
        verificarRota: async () => ({ liberada: false }),
        confirmarRota: async () => true,
        confirmarAtendimento: async (_id, dados) => {
            ordem.push('confirmar-atendimento');
            return validarConfirmacaoAtendimentoRemoto(dados, dados);
        }
    });

    assert.equal(resultado.atendimentoId, 'atendimento-completo');
    assert.deepEqual(fases, [
        'atendimento_pendente_confirmado',
        'fotos_enviando',
        'fotos_enviando',
        'fotos_confirmadas',
        'atendimento_confirmado',
        'efeitos_confirmados',
        'rota_confirmada',
        'confirmacao_remota'
    ]);
    assert.deepEqual(ordem.slice(0, 5), [
        'texto',
        'upload',
        'foto-remota:ficha',
        'upload',
        'foto-remota:maquina'
    ]);
});

test('falha na segunda foto preserva a primeira e a retomada envia somente o que falta', async () => {
    let itemAtual = {
        id: 'fila-retomada-foto',
        atendimentoServidorId: 'atendimento-retomada-foto',
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
    let uploads = 0;
    const atualizarItem = async patch => {
        itemAtual = { ...itemAtual, ...patch };
        return itemAtual;
    };
    const servicos = {
        atualizarItem,
        salvarAtendimento: async () => {},
        atualizarFotoAtendimento: async () => {},
        sincronizarProdutos: async () => ({ caminhos: [] }),
        confirmarProdutos: async () => true,
        confirmarAtendimento: async () => true
    };

    await assert.rejects(() => processarAtendimentoPendente({
        item: itemAtual,
        ...servicos,
        salvarFoto: async () => {
            uploads += 1;
            if (uploads === 2) throw new Error('rede interrompida');
            return { url: 'https://storage/ficha.jpg', thumbUrl: 'https://storage/thumb-ficha.jpg' };
        }
    }), /rede interrompida/);

    assert.equal(itemAtual.dados.fotos.ficha, 'https://storage/ficha.jpg');
    assert.match(itemAtual.dados.fotos.maquinas[0].url, /^data:/);

    await processarAtendimentoPendente({
        item: itemAtual,
        ...servicos,
        salvarFoto: async () => {
            uploads += 1;
            return { url: 'https://storage/maquina.jpg', thumbUrl: 'https://storage/thumb-maquina.jpg' };
        }
    });

    assert.equal(uploads, 3);
    assert.equal(itemAtual.dados.fotos.maquinas[0].url, 'https://storage/maquina.jpg');
});

test('falha na leitura final mantém a fase pronta para nova confirmação', async () => {
    let itemAtual = {
        id: 'fila-2',
        atendimentoServidorId: 'atendimento-2',
        fase: 'rota_confirmada',
        registroInicialConfirmado: true,
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
