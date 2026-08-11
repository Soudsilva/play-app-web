const DB_NAME = 'play-atendimento-rascunho';
const DB_VERSION = 1;
const STORE_NAME = 'rascunhos';

const cacheFotos = new Map();

function abrirBancoRascunho() {
    return new Promise((resolve, reject) => {
        if (!globalThis.indexedDB) {
            reject(new Error('IndexedDB indisponivel neste navegador.'));
            return;
        }

        const requisicao = indexedDB.open(DB_NAME, DB_VERSION);
        requisicao.onupgradeneeded = () => {
            const banco = requisicao.result;
            if (!banco.objectStoreNames.contains(STORE_NAME)) {
                banco.createObjectStore(STORE_NAME, { keyPath: 'id' });
            }
        };
        requisicao.onsuccess = () => resolve(requisicao.result);
        requisicao.onerror = () => reject(requisicao.error || new Error('Falha ao abrir o rascunho local.'));
        requisicao.onblocked = () => reject(new Error('Atualizacao do rascunho local bloqueada.'));
    });
}

function aguardarTransacao(transacao) {
    return new Promise((resolve, reject) => {
        transacao.oncomplete = () => resolve();
        transacao.onerror = () => reject(transacao.error || new Error('Falha na transacao do rascunho.'));
        transacao.onabort = () => reject(transacao.error || new Error('Transacao do rascunho cancelada.'));
    });
}

function normalizarParteChave(valor) {
    return String(valor || '').trim().toLowerCase();
}

function criarChave(usuario, visitaId) {
    const usuarioNormalizado = normalizarParteChave(usuario);
    const visitaNormalizada = String(visitaId || '').trim();
    if (!usuarioNormalizado || !visitaNormalizada) {
        throw new Error('Usuario e visita sao obrigatorios para o rascunho.');
    }
    return `${usuarioNormalizado}::${visitaNormalizada}`;
}

export function gerarIdVisitaAtendimento() {
    return globalThis.crypto?.randomUUID?.()
        || `visita_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

export function deveRestaurarVisitaAtendimento(tipoNavegacao, visitaId) {
    const tipo = String(tipoNavegacao || '').trim().toLowerCase();
    return !!String(visitaId || '').trim() && (tipo === 'reload' || tipo === 'back_forward');
}

async function origemParaBlob(origem, chaveCache) {
    const valor = String(origem || '').trim();
    if (!valor) return null;

    const cache = cacheFotos.get(chaveCache);
    if (cache?.origem === valor && cache?.blob instanceof Blob) return { blob: cache.blob };

    if (!valor.startsWith('data:') && !valor.startsWith('blob:')) {
        return { url: valor };
    }

    const resposta = await fetch(valor);
    if (!resposta.ok) throw new Error('Nao foi possivel preparar uma foto do rascunho.');
    const blob = await resposta.blob();
    cacheFotos.set(chaveCache, { origem: valor, blob });
    return { blob };
}

function blobParaDataUrl(blob) {
    return new Promise((resolve, reject) => {
        const leitor = new FileReader();
        leitor.onload = () => resolve(String(leitor.result || ''));
        leitor.onerror = () => reject(leitor.error || new Error('Nao foi possivel restaurar uma foto.'));
        leitor.readAsDataURL(blob);
    });
}

async function fotoArmazenadaParaOrigem(foto, chaveCache) {
    if (!foto) return '';
    if (foto.url) return String(foto.url);
    if (!(foto.blob instanceof Blob)) return '';

    const origem = await blobParaDataUrl(foto.blob);
    cacheFotos.set(chaveCache, { origem, blob: foto.blob });
    return origem;
}

async function prepararDadosParaArmazenar(dados, chaveRegistro) {
    const fotosPix = await Promise.all((Array.isArray(dados?.fotosPix) ? dados.fotosPix : []).map(async foto => ({
        index: Number(foto?.index || 0),
        arquivo: await origemParaBlob(foto?.src, `${chaveRegistro}:pix:${foto?.index || 0}`)
    })));
    const fotosMaquinas = await Promise.all((Array.isArray(dados?.fotosMaquinas) ? dados.fotosMaquinas : []).map(async foto => ({
        index: Number(foto?.index || 0),
        arquivo: await origemParaBlob(foto?.src, `${chaveRegistro}:maquina:${foto?.index || 0}`)
    })));

    return {
        ...dados,
        fotoFichaBase64: undefined,
        fotoFichaArquivo: await origemParaBlob(dados?.fotoFichaBase64, `${chaveRegistro}:ficha`),
        fotosPix,
        fotosMaquinas
    };
}

async function prepararDadosParaTela(dados, chaveRegistro) {
    if (!dados) return null;
    const fotosPix = await Promise.all((Array.isArray(dados?.fotosPix) ? dados.fotosPix : []).map(async foto => ({
        index: Number(foto?.index || 0),
        src: await fotoArmazenadaParaOrigem(foto?.arquivo, `${chaveRegistro}:pix:${foto?.index || 0}`)
    })));
    const fotosMaquinas = await Promise.all((Array.isArray(dados?.fotosMaquinas) ? dados.fotosMaquinas : []).map(async foto => ({
        index: Number(foto?.index || 0),
        src: await fotoArmazenadaParaOrigem(foto?.arquivo, `${chaveRegistro}:maquina:${foto?.index || 0}`)
    })));

    return {
        ...dados,
        fotoFichaArquivo: undefined,
        fotoFichaBase64: await fotoArmazenadaParaOrigem(dados?.fotoFichaArquivo, `${chaveRegistro}:ficha`),
        fotosPix,
        fotosMaquinas
    };
}

export async function salvarRascunhoAtendimento({ usuario, visitaId, dados }) {
    const id = criarChave(usuario, visitaId);
    const dadosArmazenados = await prepararDadosParaArmazenar(dados || {}, id);
    const banco = await abrirBancoRascunho();
    const transacao = banco.transaction(STORE_NAME, 'readwrite');
    transacao.objectStore(STORE_NAME).put({
        id,
        usuario: normalizarParteChave(usuario),
        visitaId: String(visitaId),
        atualizadoEm: new Date().toISOString(),
        dados: dadosArmazenados
    });
    await aguardarTransacao(transacao);
    banco.close();
}

export async function lerRascunhoAtendimento({ usuario, visitaId }) {
    const id = criarChave(usuario, visitaId);
    const banco = await abrirBancoRascunho();
    const registro = await new Promise((resolve, reject) => {
        const requisicao = banco.transaction(STORE_NAME, 'readonly').objectStore(STORE_NAME).get(id);
        requisicao.onsuccess = () => resolve(requisicao.result || null);
        requisicao.onerror = () => reject(requisicao.error || new Error('Falha ao ler o rascunho.'));
    });
    banco.close();
    return registro ? prepararDadosParaTela(registro.dados, id) : null;
}

export async function removerRascunhoAtendimento({ usuario, visitaId }) {
    const id = criarChave(usuario, visitaId);
    const banco = await abrirBancoRascunho();
    const transacao = banco.transaction(STORE_NAME, 'readwrite');
    transacao.objectStore(STORE_NAME).delete(id);
    await aguardarTransacao(transacao);
    banco.close();
    for (const chave of cacheFotos.keys()) {
        if (chave.startsWith(`${id}:`)) cacheFotos.delete(chave);
    }
}

export async function limparRascunhosAtendimentoDoUsuario(usuario) {
    const usuarioNormalizado = normalizarParteChave(usuario);
    if (!usuarioNormalizado) return;

    const banco = await abrirBancoRascunho();
    const transacao = banco.transaction(STORE_NAME, 'readwrite');
    const store = transacao.objectStore(STORE_NAME);
    const requisicao = store.openCursor();
    requisicao.onsuccess = () => {
        const cursor = requisicao.result;
        if (!cursor) return;
        if (cursor.value?.usuario === usuarioNormalizado) cursor.delete();
        cursor.continue();
    };
    await aguardarTransacao(transacao);
    banco.close();
    for (const chave of cacheFotos.keys()) {
        if (chave.startsWith(`${usuarioNormalizado}::`)) cacheFotos.delete(chave);
    }
}
