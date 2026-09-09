import {
    get,
    push,
    ref,
    runTransaction,
    update
} from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';
import {
    deleteObject,
    getDownloadURL,
    ref as storageRef,
    uploadBytesResumable
} from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-storage.js';
import { db, storage } from './firebase-app.js';
import { limparNomeParaCaminho, obterExtensaoMiniatura } from './arquivos-impressao-rules.js?v=48';
import {
    criarNomeBuscaDocumento,
    obterNomeDocumento,
    validarArquivoDocumento,
    validarMiniaturaDocumento
} from './contrato-documentos-rules.js';

const DOCUMENTOS_ROOT = 'contratos_sociedade/documentos';

function validarId(valor, rotulo) {
    const id = String(valor || '').trim();
    if (!id || /[.#$/\[\]]/.test(id)) throw new Error(`${rotulo} inválido.`);
    return id;
}

function documentoParaBanco(documento = {}) {
    const resultado = {};
    [
        'nome', 'nomeBusca', 'formato', 'detalhePadrao', 'icone', 'storagePath',
        'miniaturaPath', 'atualizadoEm', 'atualizadoPor', 'criadoEm', 'criadoPor'
    ].forEach(campo => {
        if (documento[campo] !== undefined && documento[campo] !== null) resultado[campo] = documento[campo];
    });
    ['tamanhoBytes', 'versaoAtual', 'ordem'].forEach(campo => {
        const numero = Number(documento[campo]);
        if (Number.isFinite(numero)) resultado[campo] = numero;
    });
    if (documento.excluido === true) resultado.excluido = true;
    return resultado;
}

function enviarComProgresso(referencia, arquivo, aoProgresso) {
    return new Promise((resolve, reject) => {
        const tarefa = uploadBytesResumable(referencia, arquivo, { contentType: arquivo.type });
        tarefa.on('state_changed', snapshot => {
            const total = Number(snapshot.totalBytes || 0);
            if (typeof aoProgresso === 'function' && total > 0) {
                aoProgresso(Math.round((snapshot.bytesTransferred / total) * 100));
            }
        }, reject, () => resolve(tarefa.snapshot));
    });
}

async function removerArquivoSeExistir(caminho) {
    const valor = String(caminho || '').trim();
    if (!valor) return;
    try {
        await deleteObject(storageRef(storage, valor));
    } catch (erro) {
        const codigo = String(erro?.code || '');
        if (!codigo.includes('object-not-found')) console.warn('Não foi possível limpar arquivo pendente:', erro);
    }
}

async function limparArquivosDaProposta(proposta) {
    if (!['atualizar', 'adicionar'].includes(proposta?.acao)) return;
    await Promise.all([
        removerArquivoSeExistir(proposta?.valorNovo?.storagePath),
        removerArquivoSeExistir(proposta?.valorNovo?.miniaturaPath)
    ]);
}

function montarPropostaBase({ documento, acao, propostoPor, propostoPorNome, aprovadores, aprovacoes, expiraEm, solicitacaoId }) {
    const agora = Date.now();
    return {
        solicitacaoId,
        documentoId: validarId(documento?.id, 'Documento'),
        acao,
        nome: String(documento?.nome || 'Documento').trim(),
        propostoPor: validarId(propostoPor, 'Sócio'),
        propostoPorNome: String(propostoPorNome || '').trim(),
        aprovadores: Array.isArray(aprovadores) ? aprovadores : [],
        aprovacoes: aprovacoes || {},
        valorAnterior: documentoParaBanco(documento),
        criadoEm: new Date(agora).toISOString(),
        expiraEm: Number(expiraEm),
        status: acao === 'atualizar' ? 'enviando' : 'pendente'
    };
}

export async function criarSolicitacaoAtualizacaoDocumento(dados) {
    const arquivo = validarArquivoDocumento(dados?.arquivo);
    const miniatura = validarMiniaturaDocumento(dados?.miniatura);
    const documentoId = validarId(dados?.documento?.id, 'Documento');
    const solicitacaoId = validarId(push(ref(db, `${DOCUMENTOS_ROOT}/ids`)).key, 'Solicitação');
    const propostaRef = ref(db, `${DOCUMENTOS_ROOT}/pendentes/${documentoId}`);
    const agora = Date.now();
    const propostaBase = montarPropostaBase({ ...dados, acao: 'atualizar', solicitacaoId });
    const pendenciaAnterior = (await get(propostaRef)).val();
    if (pendenciaAnterior && Number(pendenciaAnterior.expiraEm) <= agora) {
        await descartarSolicitacaoDocumentoExpirada(documentoId, pendenciaAnterior.expiraEm);
    }

    const reserva = await runTransaction(propostaRef, atual => {
        if (atual && Number(atual.expiraEm) > agora) return;
        return propostaBase;
    });
    if (!reserva.committed) throw new Error('Já existe uma mudança aguardando aprovação para este documento.');

    const nome = obterNomeDocumento(arquivo);
    const nomeCaminho = limparNomeParaCaminho(arquivo.name);
    const pastaVersao = `contratos-documentos/${documentoId}/${solicitacaoId}`;
    const arquivoPath = `${pastaVersao}/${nomeCaminho}.pdf`;
    const miniaturaPath = miniatura
        ? `${pastaVersao}/miniatura.${obterExtensaoMiniatura(miniatura)}`
        : '';

    try {
        await enviarComProgresso(storageRef(storage, arquivoPath), arquivo, dados?.aoProgresso);
        if (miniatura) await enviarComProgresso(storageRef(storage, miniaturaPath), miniatura);
        const valorNovo = {
            ...documentoParaBanco(dados.documento),
            nome,
            nomeBusca: criarNomeBuscaDocumento(nome),
            formato: 'pdf',
            tamanhoBytes: arquivo.size,
            storagePath: arquivoPath,
            miniaturaPath,
            versaoAtual: Number(dados?.documento?.versaoAtual || 0) + 1,
            excluido: false,
            atualizadoEm: new Date().toISOString(),
            atualizadoPor: String(dados?.propostoPorNome || '').trim()
        };
        const finalizacao = await runTransaction(propostaRef, atual => {
            if (!atual || atual.solicitacaoId !== solicitacaoId) return;
            return { ...atual, nome, valorNovo, status: 'pendente' };
        });
        if (!finalizacao.committed) throw new Error('A solicitação deixou de estar disponível durante o envio.');
        return finalizacao.snapshot.val();
    } catch (erro) {
        await runTransaction(propostaRef, atual => {
            if (atual?.solicitacaoId === solicitacaoId) return null;
            return atual;
        });
        await Promise.all([removerArquivoSeExistir(arquivoPath), removerArquivoSeExistir(miniaturaPath)]);
        throw erro;
    }
}

export async function criarSolicitacaoInclusaoDocumento(dados) {
    const arquivo = validarArquivoDocumento(dados?.arquivo);
    const miniatura = validarMiniaturaDocumento(dados?.miniatura);
    const documentoId = validarId(push(ref(db, `${DOCUMENTOS_ROOT}/ativos`)).key, 'Documento');
    const solicitacaoId = validarId(push(ref(db, `${DOCUMENTOS_ROOT}/ids`)).key, 'Solicitação');
    const propostaRef = ref(db, `${DOCUMENTOS_ROOT}/pendentes/${documentoId}`);
    const nome = obterNomeDocumento(arquivo);
    const agora = Date.now();
    const documentoInicial = {
        id: documentoId,
        nome,
        formato: 'pdf',
        icone: '📄',
        ordem: agora
    };
    const propostaBase = montarPropostaBase({
        ...dados,
        documento: documentoInicial,
        acao: 'adicionar',
        solicitacaoId
    });
    const reserva = await runTransaction(propostaRef, atual => atual ? undefined : propostaBase);
    if (!reserva.committed) throw new Error('Não foi possível reservar a inclusão deste documento.');

    const nomeCaminho = limparNomeParaCaminho(arquivo.name);
    const pastaVersao = `contratos-documentos/${documentoId}/${solicitacaoId}`;
    const arquivoPath = `${pastaVersao}/${nomeCaminho}.pdf`;
    const miniaturaPath = miniatura
        ? `${pastaVersao}/miniatura.${obterExtensaoMiniatura(miniatura)}`
        : '';

    try {
        await enviarComProgresso(storageRef(storage, arquivoPath), arquivo, dados?.aoProgresso);
        if (miniatura) await enviarComProgresso(storageRef(storage, miniaturaPath), miniatura);
        const criadoEm = new Date().toISOString();
        const valorNovo = {
            nome,
            nomeBusca: criarNomeBuscaDocumento(nome),
            formato: 'pdf',
            tamanhoBytes: arquivo.size,
            storagePath: arquivoPath,
            miniaturaPath,
            versaoAtual: 1,
            ordem: agora,
            icone: '📄',
            excluido: false,
            criadoEm,
            criadoPor: String(dados?.propostoPorNome || '').trim(),
            atualizadoEm: criadoEm,
            atualizadoPor: String(dados?.propostoPorNome || '').trim()
        };
        const finalizacao = await runTransaction(propostaRef, atual => {
            if (!atual || atual.solicitacaoId !== solicitacaoId) return;
            return { ...atual, nome, valorNovo, status: 'pendente' };
        });
        if (!finalizacao.committed) throw new Error('A solicitação deixou de estar disponível durante o envio.');
        return { documentoId, proposta: finalizacao.snapshot.val() };
    } catch (erro) {
        await runTransaction(propostaRef, atual => {
            if (atual?.solicitacaoId === solicitacaoId) return null;
            return atual;
        });
        await Promise.all([removerArquivoSeExistir(arquivoPath), removerArquivoSeExistir(miniaturaPath)]);
        throw erro;
    }
}

export async function criarSolicitacaoExclusaoDocumento(dados) {
    const documentoId = validarId(dados?.documento?.id, 'Documento');
    const solicitacaoId = validarId(push(ref(db, `${DOCUMENTOS_ROOT}/ids`)).key, 'Solicitação');
    const propostaRef = ref(db, `${DOCUMENTOS_ROOT}/pendentes/${documentoId}`);
    const agora = Date.now();
    const proposta = montarPropostaBase({ ...dados, acao: 'excluir', solicitacaoId });
    const pendenciaAnterior = (await get(propostaRef)).val();
    if (pendenciaAnterior && Number(pendenciaAnterior.expiraEm) <= agora) {
        await descartarSolicitacaoDocumentoExpirada(documentoId, pendenciaAnterior.expiraEm);
    }
    const resultado = await runTransaction(propostaRef, atual => {
        if (atual && Number(atual.expiraEm) > agora) return;
        return proposta;
    });
    if (!resultado.committed) throw new Error('Já existe uma mudança aguardando aprovação para este documento.');
    return resultado.snapshot.val();
}

export async function responderSolicitacaoDocumento(documentoIdValor, socioIdValor, decisao) {
    const documentoId = validarId(documentoIdValor, 'Documento');
    const socioId = validarId(socioIdValor, 'Sócio');
    const propostaRef = ref(db, `${DOCUMENTOS_ROOT}/pendentes/${documentoId}`);
    let motivoInterrupcao = '';

    const resultado = await runTransaction(propostaRef, atual => {
        if (!atual) {
            motivoInterrupcao = 'ausente';
            return;
        }
        if (atual.status !== 'pendente') {
            motivoInterrupcao = 'em_envio';
            return;
        }
        if (Number(atual.expiraEm) <= Date.now()) {
            motivoInterrupcao = 'expirada';
            return atual;
        }
        const ids = (Array.isArray(atual.aprovadores) ? atual.aprovadores : [])
            .map(item => String(item?.id || '').trim())
            .filter(Boolean);
        if (!ids.includes(socioId)) {
            motivoInterrupcao = 'nao_autorizado';
            return;
        }
        const aprovacoes = { ...(atual.aprovacoes || {}), [socioId]: decisao === true };
        const status = ids.some(id => aprovacoes[id] === false)
            ? 'negada'
            : ids.every(id => aprovacoes[id] === true)
                ? 'aprovada'
                : 'pendente';
        return { ...atual, aprovacoes, status };
    });

    const proposta = resultado.snapshot.val();
    if (!resultado.committed || !proposta) return { status: motivoInterrupcao || 'ausente' };
    if (motivoInterrupcao === 'expirada') {
        await descartarSolicitacaoDocumentoExpirada(documentoId, proposta.expiraEm);
        return { status: 'expirada' };
    }
    if (proposta.status === 'pendente') return { status: 'pendente' };

    const atualizacoes = { [`${DOCUMENTOS_ROOT}/pendentes/${documentoId}`]: null };
    if (proposta.status === 'aprovada') {
        atualizacoes[`${DOCUMENTOS_ROOT}/ativos/${documentoId}`] = proposta.acao === 'excluir'
            ? {
                ...proposta.valorAnterior,
                excluido: true,
                atualizadoEm: new Date().toISOString(),
                atualizadoPor: socioId
            }
            : {
                ...proposta.valorNovo,
                excluido: false,
                atualizadoEm: new Date().toISOString(),
                atualizadoPor: socioId
            };
    }
    await update(ref(db), atualizacoes);
    if (proposta.status === 'negada') await limparArquivosDaProposta(proposta);
    return { status: proposta.status };
}

export async function descartarSolicitacaoDocumentoExpirada(documentoIdValor, expiraEmEsperado) {
    const documentoId = validarId(documentoIdValor, 'Documento');
    const propostaRef = ref(db, `${DOCUMENTOS_ROOT}/pendentes/${documentoId}`);
    const snapshot = await get(propostaRef);
    const descartada = snapshot.val();
    if (!descartada || Number(descartada.expiraEm) !== Number(expiraEmEsperado)) return;
    if (Number(descartada.expiraEm) > Date.now()) return;
    const resultado = await runTransaction(propostaRef, atual => {
        if (!atual || atual.solicitacaoId !== descartada.solicitacaoId) return;
        if (Number(atual.expiraEm) !== Number(expiraEmEsperado) || Number(atual.expiraEm) > Date.now()) return;
        return null;
    });
    if (resultado.committed && !resultado.snapshot.exists()) await limparArquivosDaProposta(descartada);
}

export async function obterUrlDocumento(storagePath) {
    const caminho = String(storagePath || '').trim();
    if (!caminho) throw new Error('Este documento ainda não possui um arquivo enviado.');
    return getDownloadURL(storageRef(storage, caminho));
}
