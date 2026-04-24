/**
 * offline-sync.js
 * Gerencia dados offline usando IndexedDB:
 *  - Cache da lista de clientes (disponível mesmo após recarregar a página sem internet)
 *  - Fila de atendimentos pendentes (com fotos em base64, enviados quando a internet voltar)
 */

const DB_NAME   = 'play-offline';
const DB_VER    = 2;
const CLIENTES  = 'clientes_cache';
const ESTOQUE   = 'estoque_cache';
const PENDENTES = 'atendimentos_pendentes';

function abrirDB() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, DB_VER);
        req.onupgradeneeded = e => {
            const db = e.target.result;
            if (!db.objectStoreNames.contains(CLIENTES))
                db.createObjectStore(CLIENTES, { keyPath: 'id' });
            if (!db.objectStoreNames.contains(ESTOQUE))
                db.createObjectStore(ESTOQUE, { keyPath: 'id' });
            if (!db.objectStoreNames.contains(PENDENTES))
                db.createObjectStore(PENDENTES, { keyPath: 'id', autoIncrement: true });
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror  = () => reject(req.error);
    });
}

// ─── CACHE DE CLIENTES ────────────────────────────────────────────────────────
// Salva a lista no IndexedDB toda vez que o Firebase atualizar.
export async function salvarCacheClientes(lista, versao = Date.now()) {
    try {
        const db = await abrirDB();
        const tx = db.transaction(CLIENTES, 'readwrite');
        tx.objectStore(CLIENTES).put({ id: 'lista', dados: lista, versao, ts: Date.now() });
        return new Promise((res, rej) => { tx.oncomplete = res; tx.onerror = () => rej(tx.error); });
    } catch(e) { console.warn('offline-sync: salvarCacheClientes falhou:', e); }
}

// Lê a lista do IndexedDB (usado quando a página abre offline).
export async function lerCacheClientes() {
    try {
        const db = await abrirDB();
        return new Promise((resolve, reject) => {
            const req = db.transaction(CLIENTES, 'readonly').objectStore(CLIENTES).get('lista');
            req.onsuccess = () => resolve(req.result?.dados || []);
            req.onerror  = () => reject(req.error);
        });
    } catch(e) { return []; }
}

export async function lerCacheClientesCompleto() {
    try {
        const db = await abrirDB();
        return new Promise((resolve, reject) => {
            const req = db.transaction(CLIENTES, 'readonly').objectStore(CLIENTES).get('lista');
            req.onsuccess = () => {
                const registro = req.result || null;
                resolve({
                    dados: registro?.dados || [],
                    versao: Number(registro?.versao || 0),
                    ts: Number(registro?.ts || 0)
                });
            };
            req.onerror  = () => reject(req.error);
        });
    } catch(e) {
        return { dados: [], versao: 0, ts: 0 };
    }
}

// ─── CACHE DE ESTOQUE (PRODUTOS) ─────────────────────────────────────────────
export async function salvarCacheEstoque(lista) {
    try {
        const db = await abrirDB();
        const tx = db.transaction(ESTOQUE, 'readwrite');
        tx.objectStore(ESTOQUE).put({ id: 'lista', dados: lista, ts: Date.now() });
        return new Promise((res, rej) => { tx.oncomplete = res; tx.onerror = () => rej(tx.error); });
    } catch(e) { console.warn('offline-sync: salvarCacheEstoque falhou:', e); }
}

export async function lerCacheEstoque() {
    try {
        const db = await abrirDB();
        return new Promise((resolve, reject) => {
            const req = db.transaction(ESTOQUE, 'readonly').objectStore(ESTOQUE).get('lista');
            req.onsuccess = () => resolve(req.result?.dados || []);
            req.onerror  = () => reject(req.error);
        });
    } catch(e) { return []; }
}

// ─── FILA DE ATENDIMENTOS PENDENTES ──────────────────────────────────────────
// Enfileira um atendimento (as fotos ficam como base64 até ter internet).
export async function enfileirarAtendimento(dadosAtendimento) {
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const tx  = db.transaction(PENDENTES, 'readwrite');
        const req = tx.objectStore(PENDENTES).add({
            dados:     dadosAtendimento,
            criadoEm:  new Date().toISOString()
        });
        req.onsuccess = () => resolve(req.result); // retorna o id gerado
        req.onerror   = () => reject(req.error);
    });
}

export async function listarPendentes() {
    try {
        const db = await abrirDB();
        return new Promise((resolve, reject) => {
            const req = db.transaction(PENDENTES, 'readonly').objectStore(PENDENTES).getAll();
            req.onsuccess = () => resolve(req.result);
            req.onerror   = () => reject(req.error);
        });
    } catch(e) { return []; }
}

export async function removerPendente(id) {
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const req = db.transaction(PENDENTES, 'readwrite').objectStore(PENDENTES).delete(id);
        req.onsuccess = () => resolve();
        req.onerror   = () => reject(req.error);
    });
}

export async function contarPendentes() {
    try {
        const db = await abrirDB();
        return new Promise((resolve, reject) => {
            const req = db.transaction(PENDENTES, 'readonly').objectStore(PENDENTES).count();
            req.onsuccess = () => resolve(req.result);
            req.onerror   = () => reject(req.error);
        });
    } catch(e) { return 0; }
}

// ─── SINCRONIZAÇÃO ────────────────────────────────────────────────────────────
// Pega tudo da fila, faz upload das fotos (ainda base64), salva no Firebase e limpa.
// Retorna quantos atendimentos foram sincronizados com sucesso.
export async function sincronizarPendentes(storageSalvarFoto, dbSalvarAtendimento, dbSincronizarProdutosAtendimentoNoHistorico = null) {
    if (!navigator.onLine) return 0;
    const pendentes = await listarPendentes();
    let ok = 0;

    for (const item of pendentes) {
        try {
            const d = JSON.parse(JSON.stringify(item.dados)); // cópia para não mutar

            // Upload das fotos que ainda estão em base64
            if (d.fotos?.ficha?.startsWith('data:'))
                d.fotos.ficha = await storageSalvarFoto(d.fotos.ficha);

            for (const f of (d.fotos?.maquinas || []))
                if (f.url?.startsWith('data:')) f.url = await storageSalvarFoto(f.url);

            for (const f of (d.fotos?.pix || []))
                if (f.url?.startsWith('data:')) f.url = await storageSalvarFoto(f.url);

            const atendimentoId = await dbSalvarAtendimento(d);
            if (typeof dbSincronizarProdutosAtendimentoNoHistorico === 'function') {
                await dbSincronizarProdutosAtendimentoNoHistorico(atendimentoId, d);
            }
            await removerPendente(item.id);
            ok++;
        } catch(e) {
            console.warn('offline-sync: falha ao sincronizar item', item.id, e);
            break; // Para na primeira falha — provavelmente sem internet ainda
        }
    }

    return ok;
}
