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
    limitToFirst
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
        miniaturaPath: documento.miniaturaPath || '',
        atualizadoEm: documento.atualizadoEm,
        ativo: true
    };
}

export async function dbListarArquivosImpressao({ busca = '', cursor = '' } = {}) {
    const termo = normalizarNomeArquivo(busca);
    const restricoes = [orderByChild('nomeBusca')];
    if (cursor) {
        restricoes.push(startAfter(cursor));
    } else if (termo) {
        restricoes.push(startAt(termo));
    }
    if (termo) restricoes.push(endAt(`${termo}\uf8ff`));
    restricoes.push(limitToFirst(PAGE_SIZE));

    const snapshot = await get(query(ref(db, `${ROOT}/listagem`), ...restricoes));
    const itens = Object.entries(snapshot.val() || {})
        .map(([id, item]) => ({ id, ...(item || {}) }))
        .filter(item => item.ativo !== false)
        .sort((a, b) => String(a.nomeBusca || '').localeCompare(String(b.nomeBusca || ''), 'pt-BR'));

    return {
        itens,
        proximoCursor: itens.length === PAGE_SIZE ? String(itens.at(-1)?.nomeBusca || '') : ''
    };
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

export async function dbSubstituirArquivoImpressao({ id, arquivo, miniatura, titulo, usuario, aoProgresso }) {
    validarPdfParaImpressao(arquivo);
    validarMiniaturaArquivo(miniatura);
    const documentoAtual = await dbObterArquivoImpressao(id);
    const arquivoId = documentoAtual.id;
    const versao = Number(documentoAtual.versaoAtual || 0) + 1;
    const nome = normalizarTitulo(titulo, arquivo);
    const agora = new Date().toISOString();
    const pastaVersao = `v${String(versao).padStart(3, '0')}`;
    const pdfPath = `arquivos-impressao/${arquivoId}/${pastaVersao}/${limparNomeParaCaminho(arquivo.name)}.pdf`;
    const miniaturaPath = miniatura
        ? `arquivos-impressao/${arquivoId}/${pastaVersao}/miniatura.${obterExtensaoMiniatura(miniatura)}`
        : String(documentoAtual.miniaturaPath || '');

    await enviarComProgresso(storageRef(storage, pdfPath), arquivo, aoProgresso);
    if (miniatura) await enviarComProgresso(storageRef(storage, miniaturaPath), miniatura);

    const documento = {
        ...documentoAtual,
        nome,
        nomeBusca: criarChaveOrdenacaoArquivo(nome, arquivoId),
        tamanhoBytes: arquivo.size,
        storagePath: pdfPath,
        miniaturaPath,
        versaoAtual: versao,
        atualizadoEm: agora,
        atualizadoPor: String(usuario || '').trim()
    };
    const atualizacoes = {
        [`${ROOT}/documentos/${arquivoId}`]: documento,
        [`${ROOT}/versoes/${arquivoId}/${versao}`]: {
            storagePath: pdfPath,
            miniaturaPath,
            tamanhoBytes: arquivo.size,
            criadoEm: agora,
            criadoPor: String(usuario || '').trim()
        },
        [`${ROOT}/listagem/${arquivoId}`]: montarIndice(documento)
    };
    await update(ref(db), atualizacoes);
    return documento;
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
