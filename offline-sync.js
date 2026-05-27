/**
 * offline-sync.js
 * Gerencia dados offline usando IndexedDB:
 *  - Cache da lista de clientes (disponível mesmo após recarregar a página sem internet)
 *  - Fila de atendimentos pendentes (com fotos em base64, enviados quando a internet voltar)
 */

const DB_NAME   = 'play-offline';
const DB_VER    = 3;
const CLIENTES  = 'clientes_cache';
const ESTOQUE   = 'estoque_cache';
const PENDENTES = 'atendimentos_pendentes';
const FOTOS_PENDENTES = 'fotos_upload_pendentes';

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
            if (!db.objectStoreNames.contains(FOTOS_PENDENTES))
                db.createObjectStore(FOTOS_PENDENTES, { keyPath: 'id' });
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

export async function salvarFotoPendenteUpload(foto) {
    const id = String(foto?.id || '').trim();
    if (!id) throw new Error('Foto pendente sem id local.');
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(FOTOS_PENDENTES, 'readwrite');
        tx.objectStore(FOTOS_PENDENTES).put({
            ...foto,
            id,
            criadoEm: foto?.criadoEm || new Date().toISOString(),
            tentativas: Number(foto?.tentativas || 0)
        });
        tx.oncomplete = () => resolve(id);
        tx.onerror = () => reject(tx.error);
    });
}

export async function salvarFotosPendentesUpload(fotos = []) {
    const lista = Array.isArray(fotos) ? fotos.filter(f => f?.id) : [];
    if (lista.length === 0) return 0;
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(FOTOS_PENDENTES, 'readwrite');
        const store = tx.objectStore(FOTOS_PENDENTES);
        lista.forEach(foto => {
            store.put({
                ...foto,
                criadoEm: foto?.criadoEm || new Date().toISOString(),
                tentativas: Number(foto?.tentativas || 0)
            });
        });
        tx.oncomplete = () => resolve(lista.length);
        tx.onerror = () => reject(tx.error);
    });
}

export async function listarFotosPendentesUpload() {
    try {
        const db = await abrirDB();
        return new Promise((resolve, reject) => {
            const req = db.transaction(FOTOS_PENDENTES, 'readonly').objectStore(FOTOS_PENDENTES).getAll();
            req.onsuccess = () => resolve(req.result || []);
            req.onerror = () => reject(req.error);
        });
    } catch(e) { return []; }
}

export async function contarFotosPendentesUpload() {
    try {
        const db = await abrirDB();
        return new Promise((resolve, reject) => {
            const req = db.transaction(FOTOS_PENDENTES, 'readonly').objectStore(FOTOS_PENDENTES).count();
            req.onsuccess = () => resolve(req.result || 0);
            req.onerror = () => reject(req.error);
        });
    } catch(e) { return 0; }
}

export async function removerFotoPendenteUpload(id) {
    const chave = String(id || '').trim();
    if (!chave) return;
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const req = db.transaction(FOTOS_PENDENTES, 'readwrite').objectStore(FOTOS_PENDENTES).delete(chave);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
    });
}

async function incrementarTentativaFotoPendente(id) {
    const chave = String(id || '').trim();
    if (!chave) return;
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(FOTOS_PENDENTES, 'readwrite');
        const store = tx.objectStore(FOTOS_PENDENTES);
        const req = store.get(chave);
        req.onsuccess = () => {
            const atual = req.result;
            if (atual) {
                store.put({
                    ...atual,
                    tentativas: Number(atual.tentativas || 0) + 1,
                    ultimaTentativaEm: new Date().toISOString()
                });
            }
        };
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
}

// ─── SINCRONIZAÇÃO ────────────────────────────────────────────────────────────
// Pega tudo da fila, faz upload das fotos (ainda base64), salva no Firebase e limpa.
// Retorna quantos atendimentos foram sincronizados com sucesso.
export async function sincronizarPendentes(storageSalvarFotoComThumb, dbSalvarAtendimento, dbSincronizarProdutosAtendimentoNoHistorico = null) {
    if (!navigator.onLine) return 0;
    const pendentes = await listarPendentes();
    let ok = 0;

    async function salvarFotoCompleta(base64) {
        const r = await storageSalvarFotoComThumb(base64);
        if (typeof r === 'string') return { url: r, thumbUrl: r };
        return { url: r?.url || '', thumbUrl: r?.thumbUrl || r?.url || '' };
    }

    for (const item of pendentes) {
        try {
            const d = JSON.parse(JSON.stringify(item.dados)); // cópia para não mutar

            // Upload das fotos que ainda estão em base64
            if (d.fotos?.ficha?.startsWith('data:')) {
                const r = await salvarFotoCompleta(d.fotos.ficha);
                d.fotos.ficha = r.url;
                d.fotos.fichaThumb = r.thumbUrl;
            }

            for (const f of (d.fotos?.maquinas || [])) {
                if (f.url?.startsWith('data:')) {
                    const r = await salvarFotoCompleta(f.url);
                    f.url = r.url;
                    f.thumbUrl = r.thumbUrl;
                }
            }

            for (const f of (d.fotos?.pix || [])) {
                if (f.url?.startsWith('data:')) {
                    const r = await salvarFotoCompleta(f.url);
                    f.url = r.url;
                    f.thumbUrl = r.thumbUrl;
                }
            }

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

export async function sincronizarFotosPendentes(storageSalvarFotoComThumb, dbAtualizarFotoAtendimentoPendente) {
    if (!navigator.onLine) return { ok: 0, falhas: 0, total: 0 };
    const pendentes = await listarFotosPendentesUpload();
    let ok = 0;
    let falhas = 0;

    for (const foto of pendentes) {
        try {
            const base64 = String(foto?.base64 || '').trim();
            const atendimentoId = String(foto?.atendimentoId || '').trim();
            if (!base64.startsWith('data:') || !atendimentoId) {
                await removerFotoPendenteUpload(foto.id);
                continue;
            }
            const r = await storageSalvarFotoComThumb(base64, foto?.pasta || 'atendimentos');
            await dbAtualizarFotoAtendimentoPendente(foto, r.url, r.thumbUrl || r.url);
            await removerFotoPendenteUpload(foto.id);
            ok++;
        } catch(e) {
            falhas++;
            await incrementarTentativaFotoPendente(foto?.id).catch(() => {});
            console.warn('offline-sync: falha ao sincronizar foto pendente', foto?.id, e);
            break;
        }
    }

    return { ok, falhas, total: pendentes.length };
}
