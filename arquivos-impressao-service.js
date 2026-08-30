import {
    get,
    push,
    ref,
    update,
    query,
    orderByChild,
    startAt,
    endAt,
    startAfter,
    limitToFirst,
    limitToLast
} from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';
import {
    ref as storageRef,
    uploadBytesResumable,
    getDownloadURL,
    getBlob
} from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-storage.js';
import { db, storage } from './firebase-app.js';
import {
    criarChaveOrdenacaoArquivo,
    limparNomeParaCaminho,
    normalizarNomeArquivo,
    obterExtensaoMiniatura,
    validarMiniaturaArquivo,
    validarPdfParaImpressao
} from './arquivos-impressao-rules.js';

const ROOT = 'arquivos_impressao';
const PAGE_SIZE = 20;
const ORDEM_INTERVALO = 1000;

function validarId(id) {
    const valor = String(id || '').trim();
    if (!valor || /[.#$\[\]/]/.test(valor)) throw new Error('Identificador de arquivo inválido.');
    return valor;
}

function normalizarTitulo(titulo, arquivo) {
    const valor = String(titulo || '').replace(/\s+/g, ' ').trim();
    const fallback = String(arquivo?.name || '').replace(/\.pdf$/i, '').trim();
    const resultado = valor || fallback;
    if (resultado.length < 2 || resultado.length > 120) {
        throw new Error('Informe um nome entre 2 e 120 caracteres.');
    }
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

function montarIndice(documento) {
    return {
        nome: documento.nome,
        nomeBusca: documento.nomeBusca,
        formato: documento.formato,
        tamanhoBytes: documento.tamanhoBytes,
        versaoAtual: documento.versaoAtual,
        ordem: Number(documento.ordem || 0),
        miniaturaPath: documento.miniaturaPath || '',
        atualizadoEm: documento.atualizadoEm,
        ativo: true
    };
}

async function consultarPaginaArquivos({ termo, campoOrdenacao, cursor }) {
    const restricoes = [orderByChild(campoOrdenacao)];
    if (cursor?.campo === campoOrdenacao && cursor?.valor !== undefined && cursor?.id) {
        restricoes.push(startAfter(cursor.valor, cursor.id));
    } else if (termo) {
        restricoes.push(startAt(termo));
    }
    if (termo) restricoes.push(endAt(`${termo}\uf8ff`));
    restricoes.push(limitToFirst(PAGE_SIZE));

    const snapshot = await get(query(ref(db, `${ROOT}/listagem`), ...restricoes));
    const itens = [];
    snapshot.forEach(itemSnapshot => {
        const item = itemSnapshot.val() || {};
        if (item.ativo !== false) itens.push({ id: itemSnapshot.key, ...item });
    });
    const ultimo = itens.at(-1);

    return {
        itens,
        proximoCursor: itens.length === PAGE_SIZE && ultimo ? {
            valor: campoOrdenacao === 'ordem'
                ? (ultimo.ordem == null ? null : Number(ultimo.ordem))
                : String(ultimo.nomeBusca || ''),
            id: ultimo.id,
            campo: campoOrdenacao
        } : null,
        ordenacaoDisponivel: campoOrdenacao === 'ordem'
    };
}

export async function dbListarArquivosImpressao({ busca = '', cursor = null } = {}) {
    const termo = normalizarNomeArquivo(busca);
    const campoOrdenacao = termo ? 'nomeBusca' : (cursor?.campo || 'ordem');
    try {
        return await consultarPaginaArquivos({ termo, campoOrdenacao, cursor });
    } catch (erro) {
        const indiceOrdemAusente = campoOrdenacao === 'ordem'
            && String(erro?.message || '').includes('Index not defined');
        if (!indiceOrdemAusente) throw erro;
        return consultarPaginaArquivos({
            termo: '',
            campoOrdenacao: 'nomeBusca',
            cursor: null
        });
    }
}

async function obterProximaOrdemArquivo() {
    const snapshot = await get(query(
        ref(db, `${ROOT}/listagem`),
        orderByChild('ordem'),
        limitToLast(1)
    ));
    let maiorOrdem = 0;
    snapshot.forEach(itemSnapshot => {
        const ordem = Number(itemSnapshot.val()?.ordem || 0);
        if (Number.isFinite(ordem)) maiorOrdem = Math.max(maiorOrdem, ordem);
    });
    return Math.max(maiorOrdem + ORDEM_INTERVALO, Date.now());
}

export async function dbObterArquivoImpressao(id) {
    const arquivoId = validarId(id);
    const snapshot = await get(ref(db, `${ROOT}/documentos/${arquivoId}`));
    const dados = snapshot.val();
    if (!dados || dados.ativo === false) throw new Error('Este arquivo não está mais disponível.');
    return { id: arquivoId, ...dados };
}

export async function dbObterUrlArquivoImpressao(storagePath) {
    const caminho = String(storagePath || '').trim();
    if (!caminho) throw new Error('Caminho do arquivo não encontrado.');
    return getDownloadURL(storageRef(storage, caminho));
}

export async function dbBaixarArquivoImpressao(storagePath) {
    const caminho = String(storagePath || '').trim();
    if (!caminho) throw new Error('Caminho do arquivo não encontrado.');
    return getBlob(storageRef(storage, caminho));
}

export async function dbCadastrarArquivoImpressao({ arquivo, miniatura, titulo, usuario, aoProgresso }) {
    validarPdfParaImpressao(arquivo);
    validarMiniaturaArquivo(miniatura);

    const novaReferencia = push(ref(db, `${ROOT}/documentos`));
    const id = validarId(novaReferencia.key);
    const nome = normalizarTitulo(titulo, arquivo);
    const agora = new Date().toISOString();
    const ordem = await obterProximaOrdemArquivo();
    const nomeCaminho = limparNomeParaCaminho(arquivo.name);
    const pdfPath = `arquivos-impressao/${id}/v001/${nomeCaminho}.pdf`;
    const miniaturaPath = miniatura
        ? `arquivos-impressao/${id}/v001/miniatura.${obterExtensaoMiniatura(miniatura)}`
        : '';

    await enviarComProgresso(storageRef(storage, pdfPath), arquivo, aoProgresso);
    if (miniatura) await enviarComProgresso(storageRef(storage, miniaturaPath), miniatura);

    const documento = {
        nome,
        nomeBusca: criarChaveOrdenacaoArquivo(nome, id),
        formato: 'pdf',
        tamanhoBytes: arquivo.size,
        storagePath: pdfPath,
        miniaturaPath,
        ordem,
        versaoAtual: 1,
        ativo: true,
        criadoEm: agora,
        criadoPor: String(usuario || '').trim(),
        atualizadoEm: agora,
        atualizadoPor: String(usuario || '').trim()
    };
    const atualizacoes = {
        [`${ROOT}/documentos/${id}`]: documento,
        [`${ROOT}/versoes/${id}/1`]: {
            storagePath: pdfPath,
            miniaturaPath,
            tamanhoBytes: arquivo.size,
            criadoEm: agora,
            criadoPor: String(usuario || '').trim()
        },
        [`${ROOT}/listagem/${id}`]: montarIndice(documento)
    };
    await update(ref(db), atualizacoes);
    return { id, ...documento };
}

export async function dbAtualizarOrdemArquivosImpressao(arquivosIdsOrdenados) {
    const ids = Array.isArray(arquivosIdsOrdenados)
        ? arquivosIdsOrdenados.map(validarId)
        : [];
    if (ids.length < 2) return;
    if (new Set(ids).size !== ids.length) {
        throw new Error('A nova ordem contém arquivos duplicados.');
    }

    const atualizacoes = {};
    ids.forEach((arquivoId, indice) => {
        const ordem = (indice + 1) * ORDEM_INTERVALO;
        atualizacoes[`${ROOT}/documentos/${arquivoId}/ordem`] = ordem;
        atualizacoes[`${ROOT}/listagem/${arquivoId}/ordem`] = ordem;
    });
    await update(ref(db), atualizacoes);
}

export async function dbRenomearArquivoImpressao(id, novoNome, usuario) {
    const documento = await dbObterArquivoImpressao(id);
    const nome = normalizarTitulo(novoNome);
    const nomeBusca = criarChaveOrdenacaoArquivo(nome, documento.id);
    const agora = new Date().toISOString();
    const atualizacoes = {
        [`${ROOT}/documentos/${documento.id}/nome`]: nome,
        [`${ROOT}/documentos/${documento.id}/nomeBusca`]: nomeBusca,
        [`${ROOT}/documentos/${documento.id}/atualizadoEm`]: agora,
        [`${ROOT}/documentos/${documento.id}/atualizadoPor`]: String(usuario || '').trim(),
        [`${ROOT}/listagem/${documento.id}/nome`]: nome,
        [`${ROOT}/listagem/${documento.id}/nomeBusca`]: nomeBusca,
        [`${ROOT}/listagem/${documento.id}/atualizadoEm`]: agora
    };
    await update(ref(db), atualizacoes);
    return { ...documento, nome, nomeBusca, atualizadoEm: agora };
}

export async function dbDesativarArquivoImpressao(id, usuario) {
    const documento = await dbObterArquivoImpressao(id);
    const agora = new Date().toISOString();
    await update(ref(db), {
        [`${ROOT}/documentos/${documento.id}/ativo`]: false,
        [`${ROOT}/documentos/${documento.id}/desativadoEm`]: agora,
        [`${ROOT}/documentos/${documento.id}/desativadoPor`]: String(usuario || '').trim(),
        [`${ROOT}/listagem/${documento.id}`]: null
    });
}
