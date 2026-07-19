import {
    getDatabase,
    ref,
    get,
    onValue,
    push,
    update,
    runTransaction,
    increment,
    serverTimestamp
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import {
    getStorage,
    ref as storageRef,
    uploadBytes,
    getDownloadURL,
    deleteObject
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-storage.js";
import { app } from './database.js';

const db = getDatabase(app);
const storage = getStorage(app);

// O ambiente fica isolado durante os testes. Na liberação oficial, somente este
// módulo será ajustado para apontar para o ambiente oficial.
export const PRODUCAO_EQUIPAMENTOS_AMBIENTE = 'teste';
const PRODUCAO_ROOT = `producao_equipamentos/ambientes/${PRODUCAO_EQUIPAMENTOS_AMBIENTE}`;
const TEMPO_BLOQUEIO_RATEIO_MS = 30 * 1000;

// Resumo operacional leve: saldos_testados/{equipamentoId}/{AAAA-MM}
// O mês atual acompanha somente o fechamento do mês imediatamente anterior.
// Meses mais antigos já estão consolidados dentro desse fechamento anterior.
// Fotos e detalhes completos continuam exclusivamente em registros/{validacaoId}.

function limparTexto(valor) {
    return String(valor || '').trim();
}

function limparId(valor) {
    return limparTexto(valor).replace(/[.#$/[\]]/g, '_');
}

function numeroMonetario(valor) {
    const numero = typeof valor === 'number'
        ? valor
        : Number(String(valor || '').replace(/\./g, '').replace(',', '.'));
    return Number.isFinite(numero) ? Math.round(numero * 100) / 100 : 0;
}

function normalizarComposicaoEquipamento(valor) {
    if (!Array.isArray(valor) || valor.length === 0) return [];
    return valor.map((item) => {
        const equipamentoId = limparId(item?.equipamentoId || item?.id);
        const nome = limparTexto(item?.nome);
        const quantidade = Math.max(0, Math.trunc(Number(item?.quantidade || 0)));
        const valorUnitario = numeroMonetario(item?.valorUnitario);
        if (!equipamentoId || !nome || quantidade <= 0 || valorUnitario <= 0) {
            throw new Error('A composição da montagem possui uma máquina inválida. Atualize o estoque e tente novamente.');
        }
        return {
            equipamentoId,
            nome,
            quantidade,
            valorUnitario,
            valorTotal: numeroMonetario(quantidade * valorUnitario)
        };
    });
}

function resumoComposicaoEquipamento(composicao, resumoInformado = '') {
    const informado = limparTexto(resumoInformado);
    if (informado) return informado;
    return composicao.map((item) => `${item.quantidade} ${item.nome}`).join(' + ');
}

function competenciaSaoPaulo(data = new Date()) {
    const partes = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'America/Sao_Paulo',
        year: 'numeric',
        month: '2-digit'
    }).formatToParts(data);
    const mapa = {};
    partes.forEach((parte) => {
        if (parte.type !== 'literal') mapa[parte.type] = parte.value;
    });
    return mapa.year && mapa.month ? `${mapa.year}-${mapa.month}` : '';
}

function validarCompetencia(competencia) {
    const valor = limparTexto(competencia);
    return /^\d{4}-\d{2}$/.test(valor) ? valor : '';
}

function competenciaAnterior(competencia) {
    const valor = validarCompetencia(competencia);
    if (!valor) return '';
    const [ano, mes] = valor.split('-').map(Number);
    const data = new Date(Date.UTC(ano, mes - 2, 15));
    return `${data.getUTCFullYear()}-${String(data.getUTCMonth() + 1).padStart(2, '0')}`;
}

function quantidadeInteira(valor) {
    return Math.max(0, Math.trunc(Number(valor || 0)));
}

function componentesParaSaldoTestado(equipamentoId, equipamentoNome, composicao, quantidade) {
    const itens = composicao.length > 0
        ? composicao
        : [{ equipamentoId, nome: equipamentoNome, quantidade }];
    const porEquipamento = new Map();
    itens.forEach((item) => {
        const id = limparId(item?.equipamentoId || item?.id);
        const qtd = quantidadeInteira(item?.quantidade);
        if (!id || qtd <= 0) return;
        porEquipamento.set(id, quantidadeInteira(porEquipamento.get(id)) + qtd);
    });
    return [...porEquipamento.entries()].map(([id, qtd]) => ({ id, quantidade: qtd }));
}

function componentesParaSaldoTestadoDoRegistro(registro) {
    const equipamento = registro?.equipamento || {};
    return componentesParaSaldoTestado(
        equipamento.id,
        equipamento.nome,
        Array.isArray(equipamento.composicao) ? equipamento.composicao : [],
        equipamento.quantidade
    );
}

function normalizarSaldoTestado(valor) {
    const saldoInicial = quantidadeInteira(valor?.saldoInicial);
    const testadas = quantidadeInteira(valor?.testadas);
    const saidas = quantidadeInteira(valor?.saidas);
    return {
        saldoInicial,
        testadas,
        saidas,
        saldo: Math.max(0, saldoInicial + testadas - saidas)
    };
}

function chaveDiaSaoPaulo(valor) {
    const data = new Date(Number(valor || 0));
    if (Number.isNaN(data.getTime())) return '';
    const partes = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'America/Sao_Paulo',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).formatToParts(data);
    const mapa = {};
    partes.forEach((parte) => {
        if (parte.type !== 'literal') mapa[parte.type] = parte.value;
    });
    return mapa.year && mapa.month && mapa.day ? `${mapa.year}-${mapa.month}-${mapa.day}` : '';
}

function calcularSaldoTestado(saldoInicial, testadas, saidas) {
    return Math.max(
        0,
        quantidadeInteira(saldoInicial) + quantidadeInteira(testadas) - quantidadeInteira(saidas)
    );
}

async function sincronizarSaldoTestadoMes(equipamentoId, competencia = competenciaSaoPaulo(), saldoAnteriorConhecido = null) {
    const id = limparId(equipamentoId);
    const mes = validarCompetencia(competencia);
    if (!id || !mes) return normalizarSaldoTestado({});

    const saldoRef = ref(db, `${PRODUCAO_ROOT}/saldos_testados/${id}/${mes}`);
    const mesAnterior = competenciaAnterior(mes);
    let saldoTransportado = saldoAnteriorConhecido === null
        ? null
        : quantidadeInteira(saldoAnteriorConhecido);
    if (saldoTransportado === null) {
        const snapshotAnterior = mesAnterior
            ? await get(ref(db, `${PRODUCAO_ROOT}/saldos_testados/${id}/${mesAnterior}`))
            : null;
        saldoTransportado = snapshotAnterior?.exists()
            ? normalizarSaldoTestado(snapshotAnterior.val() || {}).saldo
            : 0;
    }
    const agora = Date.now();
    const resultado = await runTransaction(
        saldoRef,
        (atual) => {
            const resumoAtual = normalizarSaldoTestado(atual || {});
            const saldoCalculado = calcularSaldoTestado(
                saldoTransportado,
                resumoAtual.testadas,
                resumoAtual.saidas
            );
            const jaSincronizado = atual
                && typeof atual === 'object'
                && quantidadeInteira(atual.saldoInicial) === saldoTransportado
                && quantidadeInteira(atual.testadas) === resumoAtual.testadas
                && quantidadeInteira(atual.saidas) === resumoAtual.saidas
                && quantidadeInteira(atual.saldo) === saldoCalculado
                && limparTexto(atual.transportadoDe) === mesAnterior;
            if (jaSincronizado) return;
            return {
                ...(atual && typeof atual === 'object' ? atual : {}),
                saldoInicial: saldoTransportado,
                testadas: resumoAtual.testadas,
                saidas: resumoAtual.saidas,
                saldo: saldoCalculado,
                transportadoDe: mesAnterior,
                criadoEm: atual?.criadoEm || agora,
                atualizadoEm: agora
            };
        },
        { applyLocally: false }
    );
    return normalizarSaldoTestado(resultado.snapshot?.val?.() || {});
}

function criarTokenOperacao() {
    if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
    return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

async function redimensionarImagem(arquivo, tamanhoMaximo, qualidade) {
    const bitmap = await createImageBitmap(arquivo);
    try {
        const proporcao = Math.min(tamanhoMaximo / bitmap.width, tamanhoMaximo / bitmap.height, 1);
        const largura = Math.max(1, Math.round(bitmap.width * proporcao));
        const altura = Math.max(1, Math.round(bitmap.height * proporcao));
        const canvas = document.createElement('canvas');
        canvas.width = largura;
        canvas.height = altura;
        const contexto = canvas.getContext('2d', { alpha: false });
        contexto.fillStyle = '#ffffff';
        contexto.fillRect(0, 0, largura, altura);
        contexto.drawImage(bitmap, 0, 0, largura, altura);
        return await new Promise((resolve, reject) => {
            canvas.toBlob((blob) => {
                if (blob) resolve(blob);
                else reject(new Error('Não foi possível preparar a foto da máquina.'));
            }, 'image/jpeg', qualidade);
        });
    } finally {
        bitmap.close?.();
    }
}

async function salvarFotosValidacao(validacaoId, arquivo) {
    if (!(arquivo instanceof Blob) || !String(arquivo.type || '').startsWith('image/')) {
        throw new Error('Selecione uma foto válida da máquina.');
    }
    if (Number(arquivo.size || 0) > 25 * 1024 * 1024) {
        throw new Error('A foto ultrapassa o limite de 25 MB.');
    }

    const [fotoBlob, thumbBlob] = await Promise.all([
        redimensionarImagem(arquivo, 1600, 0.84),
        redimensionarImagem(arquivo, 280, 0.76)
    ]);
    const pasta = `producao_equipamentos/${PRODUCAO_EQUIPAMENTOS_AMBIENTE}/${validacaoId}`;
    const fotoPath = `${pasta}/foto.jpg`;
    const thumbPath = `${pasta}/thumb.jpg`;
    const fotoRef = storageRef(storage, fotoPath);
    const thumbRef = storageRef(storage, thumbPath);

    try {
        const [fotoSnapshot, thumbSnapshot] = await Promise.all([
            uploadBytes(fotoRef, fotoBlob, { contentType: 'image/jpeg' }),
            uploadBytes(thumbRef, thumbBlob, { contentType: 'image/jpeg' })
        ]);
        const [url, thumbUrl] = await Promise.all([
            getDownloadURL(fotoSnapshot.ref),
            getDownloadURL(thumbSnapshot.ref)
        ]);
        return { url, thumbUrl, fotoPath, thumbPath };
    } catch (erro) {
        await Promise.allSettled([deleteObject(fotoRef), deleteObject(thumbRef)]);
        throw erro;
    }
}

function resumoIndiceDoRegistro(registro, valorParticipante) {
    const resumo = {
        validacaoId: registro.id,
        competencia: registro.competencia,
        status: registro.status,
        criadoEm: registro.criadoEm,
        atualizadoEm: registro.atualizadoEm,
        equipamentoId: registro.equipamento.id,
        equipamentoNome: registro.equipamento.nome,
        quantidade: registro.equipamento.quantidade,
        valorUnitario: registro.equipamento.valorUnitario,
        valorTotalEquipamento: registro.equipamento.valorTotal,
        valor: numeroMonetario(valorParticipante),
        fotoThumbUrl: registro.foto.thumbUrl,
        produtorOriginalId: registro.produtorOriginal.colaboradorId,
        produtorOriginalNome: registro.produtorOriginal.nome,
        validadoPorId: registro.validadoPor.colaboradorId,
        validadoPorNome: registro.validadoPor.nome
    };
    if (registro.equipamento?.codigoMontagem) resumo.codigoMontagem = registro.equipamento.codigoMontagem;
    if (registro.equipamento?.composicaoResumo) resumo.composicaoResumo = registro.equipamento.composicaoResumo;
    return resumo;
}

export async function dbCriarValidacaoProducaoEquipamento({
    equipamento,
    produtor,
    validador,
    fotoArquivo,
    quantidade = 1
}) {
    const equipamentoId = limparId(equipamento?.id);
    const equipamentoNome = limparTexto(equipamento?.nome);
    const codigoMontagem = limparId(equipamento?.codigoMontagem).toLowerCase();
    const composicao = normalizarComposicaoEquipamento(equipamento?.composicao);
    const composicaoResumo = resumoComposicaoEquipamento(composicao, equipamento?.composicaoResumo);
    const produtorId = limparId(produtor?.id);
    const produtorNome = limparTexto(produtor?.nome);
    const validadorId = limparId(validador?.id);
    const validadorNome = limparTexto(validador?.nome);
    const qtd = composicao.length > 0 ? 1 : Math.max(1, Math.trunc(Number(quantidade || 1)));
    const valorComposicao = numeroMonetario(composicao.reduce((total, item) => total + item.valorTotal, 0));
    const valorUnitario = composicao.length > 0 ? valorComposicao : numeroMonetario(equipamento?.valorUnitario);
    const valorTotal = composicao.length > 0 ? valorComposicao : numeroMonetario(valorUnitario * qtd);
    const competencia = competenciaSaoPaulo();
    const componentesSaldoTestado = componentesParaSaldoTestado(equipamentoId, equipamentoNome, composicao, qtd);

    if (!equipamentoId || !equipamentoNome) throw new Error('Selecione a máquina testada.');
    if (valorUnitario <= 0) throw new Error('A máquina selecionada não possui valor de remuneração.');
    if (composicao.length > 0 && !codigoMontagem) throw new Error('Não foi possível identificar a configuração da montagem.');
    if (!produtorId || !produtorNome) throw new Error('Selecione quem produziu a máquina.');
    if (!validadorId || !validadorNome) throw new Error('Não foi possível identificar quem realizou o teste.');
    if (!competencia) throw new Error('Não foi possível identificar o mês da validação.');

    // Cria o resumo mensal antes da foto e transporta somente o fechamento do
    // mês imediatamente anterior. Nenhum histórico completo é percorrido.
    await Promise.all(
        componentesSaldoTestado.map((item) => sincronizarSaldoTestadoMes(item.id, competencia))
    );

    const validacaoId = push(ref(db, `${PRODUCAO_ROOT}/registros`)).key;
    if (!validacaoId) throw new Error('Não foi possível gerar o identificador da validação.');

    const foto = await salvarFotosValidacao(validacaoId, fotoArquivo);
    const agoraServidor = serverTimestamp();
    const registro = {
        id: validacaoId,
        ambiente: PRODUCAO_EQUIPAMENTOS_AMBIENTE,
        competencia,
        status: 'confirmada',
        criadoEm: agoraServidor,
        atualizadoEm: agoraServidor,
        equipamento: {
            id: equipamentoId,
            nome: equipamentoNome,
            quantidade: qtd,
            valorUnitario,
            valorTotal
        },
        foto: {
            url: foto.url,
            thumbUrl: foto.thumbUrl,
            storagePath: foto.fotoPath,
            thumbStoragePath: foto.thumbPath
        },
        validadoPor: {
            colaboradorId: validadorId,
            nome: validadorNome
        },
        produtorOriginal: {
            colaboradorId: produtorId,
            nome: produtorNome
        },
        participantes: {
            [produtorId]: {
                nome: produtorNome,
                valor: valorTotal,
                atualizadoEm: agoraServidor
            }
        }
    };
    if (codigoMontagem) registro.equipamento.codigoMontagem = codigoMontagem;
    if (composicaoResumo) registro.equipamento.composicaoResumo = composicaoResumo;
    if (composicao.length > 0) registro.equipamento.composicao = composicao;
    const resumoProdutor = resumoIndiceDoRegistro(registro, valorTotal);
    const resumoMes = {
        validacaoId,
        competencia,
        status: 'confirmada',
        criadoEm: agoraServidor,
        atualizadoEm: agoraServidor,
        equipamentoId,
        equipamentoNome,
        quantidade: qtd,
        valorTotal,
        fotoThumbUrl: foto.thumbUrl,
        produtorOriginalId: produtorId,
        produtorOriginalNome: produtorNome,
        validadoPorId: validadorId,
        validadoPorNome: validadorNome
    };
    if (codigoMontagem) resumoMes.codigoMontagem = codigoMontagem;
    if (composicaoResumo) resumoMes.composicaoResumo = composicaoResumo;
    const updates = {};
    updates[`${PRODUCAO_ROOT}/registros/${validacaoId}`] = registro;
    updates[`${PRODUCAO_ROOT}/por_usuario/${produtorId}/${competencia}/${validacaoId}`] = resumoProdutor;
    updates[`${PRODUCAO_ROOT}/por_mes/${competencia}/${validacaoId}`] = resumoMes;
    updates[`${PRODUCAO_ROOT}/meses_por_usuario/${produtorId}/${competencia}`] = true;
    updates[`${PRODUCAO_ROOT}/totais_mensais/${produtorId}/${competencia}/valor`] = increment(valorTotal);
    updates[`${PRODUCAO_ROOT}/totais_mensais/${produtorId}/${competencia}/atualizadoEm`] = agoraServidor;
    componentesSaldoTestado.forEach((item) => {
        const caminhoSaldo = `${PRODUCAO_ROOT}/saldos_testados/${item.id}/${competencia}`;
        updates[`${caminhoSaldo}/testadas`] = increment(item.quantidade);
        updates[`${caminhoSaldo}/saidas`] = increment(0);
        updates[`${caminhoSaldo}/saldo`] = increment(item.quantidade);
        updates[`${caminhoSaldo}/atualizadoEm`] = agoraServidor;
    });

    try {
        await update(ref(db), updates);
        return { id: validacaoId, ...registro };
    } catch (erro) {
        await Promise.allSettled([
            deleteObject(storageRef(storage, foto.fotoPath)),
            deleteObject(storageRef(storage, foto.thumbPath))
        ]);
        throw erro;
    }
}

async function adicionarReversaoComponentesSaldoTestado(competencia, componentes, updates, atualizadoEm) {
    const mes = validarCompetencia(competencia);
    if (!mes || !Array.isArray(componentes) || componentes.length === 0) return;

    await Promise.all(componentes.map(async (item) => {
        const id = limparId(item?.id);
        const quantidade = quantidadeInteira(item?.quantidade);
        if (!id || quantidade <= 0) return;

        await sincronizarSaldoTestadoMes(id, mes);
        const caminhoSaldo = `${PRODUCAO_ROOT}/saldos_testados/${id}/${mes}`;
        const snapshotSaldo = await get(ref(db, caminhoSaldo));
        const saldoAtual = normalizarSaldoTestado(snapshotSaldo.val() || {});
        if (saldoAtual.saldo < quantidade) {
            throw new Error('Esta validação não pode ser excluída porque o saldo testado já foi consumido em saída de estoque.');
        }
        updates[`${caminhoSaldo}/testadas`] = increment(-quantidade);
        updates[`${caminhoSaldo}/saldo`] = increment(-quantidade);
        updates[`${caminhoSaldo}/atualizadoEm`] = atualizadoEm;
    }));
}

export async function dbExcluirValidacaoProducaoEquipamentoHoje(validacaoId, solicitante) {
    const id = limparId(validacaoId);
    const solicitanteId = limparId(solicitante?.id);
    if (!id) throw new Error('Não foi possível identificar a validação.');
    if (!solicitanteId) throw new Error('Não foi possível identificar o usuário logado.');

    const snapshot = await get(ref(db, `${PRODUCAO_ROOT}/registros/${id}`));
    if (!snapshot.exists()) throw new Error('A validação selecionada não foi encontrada.');

    const registro = snapshot.val() || {};
    const competencia = validarCompetencia(registro?.competencia);
    const validadoPorId = limparId(registro?.validadoPor?.colaboradorId);
    const criadoEm = Number(registro?.criadoEm || 0);
    if (!competencia) throw new Error('A validação está sem competência mensal válida.');
    if (registro?.status !== 'confirmada') throw new Error('Esta validação não está disponível para exclusão.');
    if (validadoPorId !== solicitanteId) throw new Error('Somente quem confirmou esta máquina pode excluir a validação.');
    if (chaveDiaSaoPaulo(criadoEm) !== chaveDiaSaoPaulo(Date.now())) {
        throw new Error('A exclusão só fica disponível nas validações de hoje.');
    }

    const participantes = registro?.participantes && typeof registro.participantes === 'object'
        ? registro.participantes
        : {};
    const agoraServidor = serverTimestamp();
    const updates = {};
    updates[`${PRODUCAO_ROOT}/registros/${id}`] = null;
    updates[`${PRODUCAO_ROOT}/por_mes/${competencia}/${id}`] = null;
    updates[`${PRODUCAO_ROOT}/bloqueios_rateio/${id}`] = null;
    Object.entries(participantes).forEach(([participanteIdOriginal, participante]) => {
        const participanteId = limparId(participanteIdOriginal);
        const valor = numeroMonetario(participante?.valor);
        if (!participanteId) return;
        updates[`${PRODUCAO_ROOT}/por_usuario/${participanteId}/${competencia}/${id}`] = null;
        if (valor !== 0) {
            updates[`${PRODUCAO_ROOT}/totais_mensais/${participanteId}/${competencia}/valor`] = increment(-valor);
            updates[`${PRODUCAO_ROOT}/totais_mensais/${participanteId}/${competencia}/atualizadoEm`] = agoraServidor;
        }
    });
    await adicionarReversaoComponentesSaldoTestado(
        competencia,
        componentesParaSaldoTestadoDoRegistro(registro),
        updates,
        agoraServidor
    );

    await update(ref(db), updates);

    const fotoPath = limparTexto(registro?.foto?.storagePath);
    const thumbPath = limparTexto(registro?.foto?.thumbStoragePath);
    await Promise.allSettled([
        fotoPath ? deleteObject(storageRef(storage, fotoPath)) : Promise.resolve(),
        thumbPath ? deleteObject(storageRef(storage, thumbPath)) : Promise.resolve()
    ]);
}

function listaOrdenada(snapshot) {
    if (!snapshot.exists()) return [];
    return Object.entries(snapshot.val() || {})
        .map(([id, valor]) => ({ id, ...valor }))
        .sort((a, b) => Number(b?.criadoEm || 0) - Number(a?.criadoEm || 0));
}

export function dbEscutarValidacoesProducaoMes(competencia, callback, callbackErro = null) {
    const mes = validarCompetencia(competencia);
    if (!mes) {
        callback([]);
        return () => {};
    }
    return onValue(ref(db, `${PRODUCAO_ROOT}/por_mes/${mes}`), (snapshot) => {
        callback(listaOrdenada(snapshot));
    }, (erro) => {
        callback([]);
        callbackErro?.(erro);
    });
}

// Leitura pontual da foto completa de uma validação. Os índices mensais
// carregam somente o thumb; a foto em alta permanece em registros/{id}.
export async function dbObterFotoValidacaoProducao(validacaoId) {
    const id = limparId(validacaoId);
    if (!id) return '';
    const snapshot = await get(ref(db, `${PRODUCAO_ROOT}/registros/${id}/foto/url`));
    return limparTexto(snapshot.val());
}

export function dbEscutarMesesProducaoUsuario(colaboradorId, callback, callbackErro = null) {
    const id = limparId(colaboradorId);
    if (!id) {
        callback([]);
        return () => {};
    }
    return onValue(ref(db, `${PRODUCAO_ROOT}/meses_por_usuario/${id}`), (snapshot) => {
        const meses = snapshot.exists()
            ? Object.keys(snapshot.val() || {}).filter(validarCompetencia).sort((a, b) => b.localeCompare(a))
            : [];
        callback(meses);
    }, (erro) => {
        callback([]);
        callbackErro?.(erro);
    });
}

export function dbEscutarProducoesUsuarioMes(colaboradorId, competencia, callback, callbackErro = null) {
    const id = limparId(colaboradorId);
    const mes = validarCompetencia(competencia);
    if (!id || !mes) {
        callback([]);
        return () => {};
    }
    return onValue(ref(db, `${PRODUCAO_ROOT}/por_usuario/${id}/${mes}`), (snapshot) => {
        callback(listaOrdenada(snapshot));
    }, (erro) => {
        callback([]);
        callbackErro?.(erro);
    });
}

export function dbEscutarTotalProducaoUsuarioMes(colaboradorId, competencia, callback, callbackErro = null) {
    const id = limparId(colaboradorId);
    const mes = validarCompetencia(competencia);
    if (!id || !mes) {
        callback({ valor: 0 });
        return () => {};
    }
    return onValue(ref(db, `${PRODUCAO_ROOT}/totais_mensais/${id}/${mes}`), (snapshot) => {
        callback(snapshot.exists() ? (snapshot.val() || { valor: 0 }) : { valor: 0 });
    }, (erro) => {
        callback({ valor: 0 });
        callbackErro?.(erro);
    });
}

export function dbEscutarSaldoTestadoEquipamentoMesAtual(equipamentoId, callback, callbackErro = null) {
    const id = limparId(equipamentoId);
    const atual = competenciaSaoPaulo();
    const anterior = competenciaAnterior(atual);
    if (!id || !atual || !anterior) {
        callback({ competenciaAtual: atual, competenciaAnterior: anterior, saldoInicial: 0, testadas: 0, saidas: 0, saldoAtual: 0, saldoExibido: 0 });
        return () => {};
    }

    let encerrada = false;
    const valores = new Map();
    const carregados = new Set();
    const falhas = new Set();
    let ultimoSaldoAnteriorSincronizado = null;
    const publicar = () => {
        if (carregados.size < 2 || encerrada) return;
        const resumoAtual = normalizarSaldoTestado(valores.get(atual) || {});
        const saldoAnterior = normalizarSaldoTestado(valores.get(anterior) || {}).saldo;
        const saldoAtual = calcularSaldoTestado(saldoAnterior, resumoAtual.testadas, resumoAtual.saidas);
        callback({
            competenciaAtual: atual,
            competenciaAnterior: anterior,
            saldoInicial: saldoAnterior,
            testadas: resumoAtual.testadas,
            saidas: resumoAtual.saidas,
            saldoAtual,
            saldoExibido: saldoAtual
        });

        if (falhas.size === 0 && ultimoSaldoAnteriorSincronizado !== saldoAnterior) {
            ultimoSaldoAnteriorSincronizado = saldoAnterior;
            sincronizarSaldoTestadoMes(id, atual, saldoAnterior).catch((erro) => {
                ultimoSaldoAnteriorSincronizado = null;
                if (!encerrada) callbackErro?.(erro);
            });
        }
    };
    const escutarMes = (competencia) => onValue(
        ref(db, `${PRODUCAO_ROOT}/saldos_testados/${id}/${competencia}`),
        (snapshot) => {
            valores.set(competencia, snapshot.val() || {});
            carregados.add(competencia);
            falhas.delete(competencia);
            if (competencia === anterior) ultimoSaldoAnteriorSincronizado = null;
            publicar();
        },
        (erro) => {
            valores.set(competencia, {});
            carregados.add(competencia);
            falhas.add(competencia);
            publicar();
            callbackErro?.(erro);
        }
    );
    const pararAtual = escutarMes(atual);
    const pararAnterior = escutarMes(anterior);
    return () => {
        encerrada = true;
        pararAtual();
        pararAnterior();
    };
}

// Compatibilidade temporária para qualquer tela ainda usando o nome anterior.
export function dbEscutarSaldoTestadoEquipamentoMesAtualEAnterior(equipamentoId, callback, callbackErro = null) {
    return dbEscutarSaldoTestadoEquipamentoMesAtual(equipamentoId, callback, callbackErro);
}

async function consumirSaldoTestadoDoMes(equipamentoId, competencia, quantidade) {
    const id = limparId(equipamentoId);
    const mes = validarCompetencia(competencia);
    const solicitada = quantidadeInteira(quantidade);
    if (!id || !mes || solicitada <= 0) return 0;

    const saldoRef = ref(db, `${PRODUCAO_ROOT}/saldos_testados/${id}/${mes}`);
    const snapshotInicial = await get(saldoRef);
    if (!snapshotInicial.exists()) return 0;
    let consumida = 0;
    const resultado = await runTransaction(
        saldoRef,
        (atual) => {
            if (!atual || typeof atual !== 'object') {
                consumida = 0;
                return;
            }
            const saldoAtual = normalizarSaldoTestado(atual);
            consumida = Math.min(solicitada, saldoAtual.saldo);
            if (consumida <= 0) return;
            return {
                ...atual,
                testadas: saldoAtual.testadas,
                saidas: saldoAtual.saidas + consumida,
                saldo: saldoAtual.saldo - consumida,
                atualizadoEm: Date.now()
            };
        },
        { applyLocally: false }
    );
    return resultado.committed ? consumida : 0;
}

export async function dbConsumirSaldosTestadosSaida(equipamentoId, quantidade) {
    const id = limparId(equipamentoId);
    const solicitada = quantidadeInteira(quantidade);
    if (!id || solicitada <= 0) return {};

    const atual = competenciaSaoPaulo();
    if (!atual) return {};
    await sincronizarSaldoTestadoMes(id, atual);
    const consumida = await consumirSaldoTestadoDoMes(id, atual, solicitada);
    return consumida > 0 ? { [atual]: consumida } : {};
}

export async function dbReverterSaldosTestadosSaida(equipamentoId, consumosPorMes) {
    const id = limparId(equipamentoId);
    if (!id || !consumosPorMes || typeof consumosPorMes !== 'object') return;

    const entradas = Object.entries(consumosPorMes)
        .map(([competencia, quantidade]) => [validarCompetencia(competencia), quantidadeInteira(quantidade)])
        .filter(([competencia, quantidade]) => competencia && quantidade > 0);
    if (entradas.length === 0) return;

    const atual = competenciaSaoPaulo();
    if (!atual) return;
    const devolucaoMesAtual = entradas
        .filter(([competencia]) => competencia === atual)
        .reduce((total, [, quantidade]) => total + quantidade, 0);
    const devolucaoMesesFechados = entradas
        .filter(([competencia]) => competencia !== atual)
        .reduce((total, [, quantidade]) => total + quantidade, 0);

    await sincronizarSaldoTestadoMes(id, atual);
    const saldoRef = ref(db, `${PRODUCAO_ROOT}/saldos_testados/${id}/${atual}`);
    await runTransaction(
        saldoRef,
        (valorAtual) => {
            if (!valorAtual || typeof valorAtual !== 'object') return;
            const saldoAtual = normalizarSaldoTestado(valorAtual);
            const saidaDevolvida = Math.min(devolucaoMesAtual, saldoAtual.saidas);
            const totalDevolvido = saidaDevolvida + devolucaoMesesFechados;
            if (totalDevolvido <= 0) return;
            return {
                ...valorAtual,
                saldoInicial: saldoAtual.saldoInicial,
                // Cancelamento de uma saída antiga entra como retorno no mês
                // atual, sem reabrir nem recalcular o fechamento antigo.
                testadas: saldoAtual.testadas + devolucaoMesesFechados,
                saidas: saldoAtual.saidas - saidaDevolvida,
                saldo: saldoAtual.saldo + totalDevolvido,
                atualizadoEm: Date.now()
            };
        },
        { applyLocally: false }
    );
}

export async function dbObterValidacaoProducaoEquipamento(validacaoId) {
    const id = limparId(validacaoId);
    if (!id) return null;
    const snapshot = await get(ref(db, `${PRODUCAO_ROOT}/registros/${id}`));
    return snapshot.exists() ? (snapshot.val() || null) : null;
}

async function adquirirBloqueioRateio(validacaoId, realizadoPor) {
    const token = criarTokenOperacao();
    const agora = Date.now();
    const bloqueioRef = ref(db, `${PRODUCAO_ROOT}/bloqueios_rateio/${validacaoId}`);
    const resultado = await runTransaction(bloqueioRef, (atual) => {
        const expiraEm = Number(atual?.expiraEm || 0);
        if (atual && expiraEm > agora) return;
        return {
            token,
            realizadoPor: limparTexto(realizadoPor),
            criadoEm: agora,
            expiraEm: agora + TEMPO_BLOQUEIO_RATEIO_MS
        };
    }, { applyLocally: false });
    if (!resultado.committed || resultado.snapshot.val()?.token !== token) {
        throw new Error('Este equipamento está sendo atualizado em outro aparelho. Tente novamente.');
    }
    return token;
}

async function liberarBloqueioRateio(validacaoId, token) {
    const bloqueioRef = ref(db, `${PRODUCAO_ROOT}/bloqueios_rateio/${validacaoId}`);
    await runTransaction(bloqueioRef, (atual) => {
        if (!atual || atual?.token === token) return null;
        return atual;
    }, { applyLocally: false }).catch(() => {});
}

export async function dbCompartilharValorProducaoEquipamento({
    validacaoId,
    produtorId,
    destinatario,
    valorCompartilhado,
    realizadoPor
}) {
    const id = limparId(validacaoId);
    const origemId = limparId(produtorId);
    const destinoId = limparId(destinatario?.id);
    const destinoNome = limparTexto(destinatario?.nome);
    const realizadoPorId = limparId(realizadoPor?.id);
    const realizadoPorNome = limparTexto(realizadoPor?.nome);
    const valorTransferido = numeroMonetario(valorCompartilhado);

    if (!id || !origemId) throw new Error('Não foi possível identificar a produção.');
    if (!destinoId || !destinoNome || destinoId === origemId) throw new Error('Selecione outro colaborador.');
    if (!realizadoPorId || realizadoPorId !== origemId) throw new Error('Somente o produtor indicado pode compartilhar o valor.');
    if (valorTransferido <= 0) throw new Error('Informe um valor maior que zero para compartilhar.');

    const token = await adquirirBloqueioRateio(id, realizadoPorNome);
    try {
        const snapshot = await get(ref(db, `${PRODUCAO_ROOT}/registros/${id}`));
        if (!snapshot.exists()) throw new Error('A produção selecionada não foi encontrada.');
        const registro = snapshot.val() || {};
        const competencia = validarCompetencia(registro?.competencia);
        const produtorOriginalId = limparId(registro?.produtorOriginal?.colaboradorId);
        if (!competencia) throw new Error('A produção está sem competência mensal válida.');
        if (registro?.status !== 'confirmada') throw new Error('Esta produção não está disponível para compartilhamento.');
        if (produtorOriginalId !== origemId) throw new Error('Somente o produtor original pode compartilhar o valor.');

        const participantes = registro?.participantes || {};
        const valorOrigemAtual = numeroMonetario(participantes?.[origemId]?.valor);
        const valorDestinoAtual = numeroMonetario(participantes?.[destinoId]?.valor);
        const valorDestinoNovo = numeroMonetario(valorDestinoAtual + valorTransferido);
        const valorOrigemNovo = numeroMonetario(valorOrigemAtual - valorTransferido);
        if (valorOrigemNovo < 0) {
            throw new Error(`O produtor possui somente R$ ${valorOrigemAtual.toLocaleString('pt-BR', { minimumFractionDigits: 2 })} disponível neste equipamento.`);
        }

        const movimentacaoId = push(ref(db, `${PRODUCAO_ROOT}/registros/${id}/movimentacoes`)).key;
        if (!movimentacaoId) throw new Error('Não foi possível gerar o histórico do compartilhamento.');
        const agoraServidor = serverTimestamp();
        const resumoBase = {
            ...registro,
            id,
            atualizadoEm: agoraServidor
        };
        const resumoOrigem = resumoIndiceDoRegistro(resumoBase, valorOrigemNovo);
        const resumoDestino = resumoIndiceDoRegistro(resumoBase, valorDestinoNovo);
        const updates = {};

        updates[`${PRODUCAO_ROOT}/registros/${id}/participantes/${origemId}/nome`] = limparTexto(registro?.produtorOriginal?.nome || participantes?.[origemId]?.nome);
        updates[`${PRODUCAO_ROOT}/registros/${id}/participantes/${origemId}/valor`] = valorOrigemNovo;
        updates[`${PRODUCAO_ROOT}/registros/${id}/participantes/${origemId}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/registros/${id}/participantes/${destinoId}`] = {
            nome: destinoNome,
            valor: valorDestinoNovo,
            atualizadoEm: agoraServidor
        };
        updates[`${PRODUCAO_ROOT}/registros/${id}/movimentacoes/${movimentacaoId}`] = {
            tipo: 'compartilhamento_valor',
            produtorId: origemId,
            destinatarioId: destinoId,
            destinatarioNome: destinoNome,
            valorTransferido,
            valorProdutorAnterior: valorOrigemAtual,
            valorProdutorNovo: valorOrigemNovo,
            valorDestinatarioAnterior: valorDestinoAtual,
            valorDestinatarioNovo: valorDestinoNovo,
            realizadoPorId,
            realizadoPorNome,
            realizadoEm: agoraServidor
        };
        updates[`${PRODUCAO_ROOT}/registros/${id}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/por_usuario/${origemId}/${competencia}/${id}`] = resumoOrigem;
        updates[`${PRODUCAO_ROOT}/por_usuario/${destinoId}/${competencia}/${id}`] = resumoDestino;
        updates[`${PRODUCAO_ROOT}/meses_por_usuario/${destinoId}/${competencia}`] = true;
        updates[`${PRODUCAO_ROOT}/totais_mensais/${origemId}/${competencia}/valor`] = increment(-valorTransferido);
        updates[`${PRODUCAO_ROOT}/totais_mensais/${origemId}/${competencia}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/totais_mensais/${destinoId}/${competencia}/valor`] = increment(valorTransferido);
        updates[`${PRODUCAO_ROOT}/totais_mensais/${destinoId}/${competencia}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/por_mes/${competencia}/${id}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/bloqueios_rateio/${id}`] = null;

        await update(ref(db), updates);
        return {
            valorProdutor: valorOrigemNovo,
            valorDestinatario: valorDestinoNovo,
            valorTransferido
        };
    } finally {
        await liberarBloqueioRateio(id, token);
    }
}

export async function dbEstornarRateioProducaoEquipamento({
    validacaoId,
    destinatario,
    valor,
    realizadoPor
}) {
    const id = limparId(validacaoId);
    const destinatarioId = limparId(destinatario?.id);
    const destinatarioNomeInformado = limparTexto(destinatario?.nome);
    const realizadoPorId = limparId(realizadoPor?.id);
    const realizadoPorNome = limparTexto(realizadoPor?.nome);
    const valorDevolvido = numeroMonetario(valor);

    if (!id) throw new Error('Não foi possível identificar a produção.');
    if (!destinatarioId || !realizadoPorId || realizadoPorId !== destinatarioId) {
        throw new Error('Somente quem recebeu o valor pode fazer a devolução.');
    }
    if (valorDevolvido <= 0) throw new Error('Informe um valor maior que zero para devolver.');

    const token = await adquirirBloqueioRateio(id, realizadoPorNome);
    try {
        const snapshot = await get(ref(db, `${PRODUCAO_ROOT}/registros/${id}`));
        if (!snapshot.exists()) throw new Error('A produção selecionada não foi encontrada.');

        const registro = snapshot.val() || {};
        const competencia = validarCompetencia(registro?.competencia);
        const competenciaAtual = competenciaSaoPaulo();
        const produtorOriginalId = limparId(registro?.produtorOriginal?.colaboradorId);
        const produtorOriginalNome = limparTexto(registro?.produtorOriginal?.nome);
        if (!competencia) throw new Error('A produção está sem competência mensal válida.');
        if (competencia !== competenciaAtual) {
            throw new Error('Só é possível devolver valores de produções do mês vigente.');
        }
        if (registro?.status !== 'confirmada') throw new Error('Esta produção não está disponível para devolução.');
        if (!produtorOriginalId || produtorOriginalId === destinatarioId) {
            throw new Error('O produtor original não pode devolver valor para si mesmo.');
        }

        const participantes = registro?.participantes || {};
        const valorDestinatarioAtual = numeroMonetario(participantes?.[destinatarioId]?.valor);
        const valorProdutorAtual = numeroMonetario(participantes?.[produtorOriginalId]?.valor);
        if (valorDestinatarioAtual <= 0) {
            throw new Error('Você não possui valor recebido neste equipamento para devolver.');
        }
        if (valorDevolvido > valorDestinatarioAtual) {
            throw new Error(`Você pode devolver no máximo ${valorDestinatarioAtual.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}.`);
        }

        const valorDestinatarioNovo = numeroMonetario(valorDestinatarioAtual - valorDevolvido);
        const valorProdutorNovo = numeroMonetario(valorProdutorAtual + valorDevolvido);
        const movimentacaoId = push(ref(db, `${PRODUCAO_ROOT}/registros/${id}/movimentacoes`)).key;
        if (!movimentacaoId) throw new Error('Não foi possível gerar o histórico da devolução.');

        const agoraServidor = serverTimestamp();
        const resumoBase = {
            ...registro,
            id,
            atualizadoEm: agoraServidor
        };
        const resumoProdutor = resumoIndiceDoRegistro(resumoBase, valorProdutorNovo);
        const resumoDestinatario = resumoIndiceDoRegistro(resumoBase, valorDestinatarioNovo);
        const destinatarioNome = limparTexto(participantes?.[destinatarioId]?.nome || destinatarioNomeInformado || realizadoPorNome);
        const updates = {};

        updates[`${PRODUCAO_ROOT}/registros/${id}/participantes/${produtorOriginalId}/nome`] = produtorOriginalNome || limparTexto(participantes?.[produtorOriginalId]?.nome);
        updates[`${PRODUCAO_ROOT}/registros/${id}/participantes/${produtorOriginalId}/valor`] = valorProdutorNovo;
        updates[`${PRODUCAO_ROOT}/registros/${id}/participantes/${produtorOriginalId}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/registros/${id}/participantes/${destinatarioId}`] = valorDestinatarioNovo > 0
            ? { nome: destinatarioNome, valor: valorDestinatarioNovo, atualizadoEm: agoraServidor }
            : null;
        updates[`${PRODUCAO_ROOT}/registros/${id}/movimentacoes/${movimentacaoId}`] = {
            tipo: 'devolucao_rateio',
            remetenteId: produtorOriginalId,
            remetenteNome: produtorOriginalNome,
            destinatarioId,
            destinatarioNome,
            valorDevolvido,
            valorDestinatarioAnterior: valorDestinatarioAtual,
            valorDestinatarioNovo,
            valorRemetenteAnterior: valorProdutorAtual,
            valorRemetenteNovo: valorProdutorNovo,
            realizadoPorId,
            realizadoPorNome,
            realizadoEm: agoraServidor
        };
        updates[`${PRODUCAO_ROOT}/registros/${id}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/por_usuario/${produtorOriginalId}/${competencia}/${id}`] = resumoProdutor;
        updates[`${PRODUCAO_ROOT}/por_usuario/${destinatarioId}/${competencia}/${id}`] = valorDestinatarioNovo > 0
            ? resumoDestinatario
            : null;
        updates[`${PRODUCAO_ROOT}/totais_mensais/${produtorOriginalId}/${competencia}/valor`] = increment(valorDevolvido);
        updates[`${PRODUCAO_ROOT}/totais_mensais/${produtorOriginalId}/${competencia}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/totais_mensais/${destinatarioId}/${competencia}/valor`] = increment(-valorDevolvido);
        updates[`${PRODUCAO_ROOT}/totais_mensais/${destinatarioId}/${competencia}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/por_mes/${competencia}/${id}/atualizadoEm`] = agoraServidor;
        updates[`${PRODUCAO_ROOT}/bloqueios_rateio/${id}`] = null;

        await update(ref(db), updates);
        return {
            valorDevolvido,
            valorProdutor: valorProdutorNovo,
            valorDestinatario: valorDestinatarioNovo
        };
    } finally {
        await liberarBloqueioRateio(id, token);
    }
}

export function obterCompetenciaAtualProducaoEquipamentos() {
    return competenciaSaoPaulo();
}
