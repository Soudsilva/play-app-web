import { ordenarPendenciasAtendimento, processarAtendimentoPendente } from './atendimento-fila-sync.mjs';

/**
 * offline-sync.js
 * Gerencia dados offline usando IndexedDB:
 *  - Cache da lista de clientes (disponível mesmo após recarregar a página sem internet)
 *  - Fila de atendimentos pendentes (com fotos em base64, enviados quando a internet voltar)
 */

const DB_NAME   = 'play-offline';
const DB_VER    = 4;
const CLIENTES  = 'clientes_cache';
const ESTOQUE   = 'estoque_cache';
const PENDENTES = 'atendimentos_pendentes';
const FOTOS_PENDENTES = 'fotos_upload_pendentes';
const PIX_POSSE_PENDENTES = 'pix_posse_pendentes';
const DURACAO_RESERVA_ENVIO_MS = 45 * 1000;
const INTERVALO_RENOVACAO_RESERVA_MS = 10 * 1000;

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
            if (!db.objectStoreNames.contains(PIX_POSSE_PENDENTES))
                db.createObjectStore(PIX_POSSE_PENDENTES, { keyPath: 'id' });
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
export async function enfileirarAtendimento(dadosAtendimento, opcoes = {}) {
    const idInformado = opcoes?.id ?? null;
    const atendimentoServidorId = String(opcoes?.atendimentoServidorId || '').trim();
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const tx  = db.transaction(PENDENTES, 'readwrite');
        const registro = {
            ...(idInformado !== null && idInformado !== undefined ? { id: idInformado } : {}),
            dados:     dadosAtendimento,
            criadoEm:  opcoes?.criadoEm || new Date().toISOString(),
            atualizadoEm: new Date().toISOString(),
            estado: 'pendente',
            fase: 'salvo_localmente',
            tentativas: Number(opcoes?.tentativas || 0),
            ...(atendimentoServidorId ? { atendimentoServidorId } : {})
        };
        const req = idInformado !== null && idInformado !== undefined
            ? tx.objectStore(PENDENTES).put(registro)
            : tx.objectStore(PENDENTES).add(registro);
        let idSalvo = idInformado;
        req.onsuccess = () => { idSalvo = req.result; };
        tx.oncomplete = () => resolve(idSalvo);
        tx.onerror = () => reject(tx.error || req.error);
        tx.onabort = () => reject(tx.error || new Error('Falha ao confirmar a fila local.'));
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

export async function removerPendente(id, tokenSincronizacao = '') {
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(PENDENTES, 'readwrite');
        const store = tx.objectStore(PENDENTES);
        const req = store.get(id);
        req.onsuccess = () => {
            const atual = req.result;
            if (!atual) return;
            if (tokenSincronizacao && atual.tokenSincronizacao !== tokenSincronizacao) return;
            store.delete(id);
        };
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
}

async function atualizarPendente(id, patch = {}, tokenSincronizacao = '') {
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(PENDENTES, 'readwrite');
        const store = tx.objectStore(PENDENTES);
        const req = store.get(id);
        let atualizado = null;
        req.onsuccess = () => {
            const atual = req.result;
            if (!atual) return;
            if (tokenSincronizacao && atual.tokenSincronizacao !== tokenSincronizacao) return;
            atualizado = {
                ...atual,
                ...patch,
                atualizadoEm: new Date().toISOString()
            };
            store.put(atualizado);
        };
        tx.oncomplete = () => resolve(atualizado);
        tx.onerror = () => reject(tx.error);
    });
}

async function reservarPendente(id, tokenSincronizacao) {
    const agora = Date.now();
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(PENDENTES, 'readwrite');
        const store = tx.objectStore(PENDENTES);
        const req = store.get(id);
        let reservado = null;
        req.onsuccess = () => {
            const atual = req.result;
            if (!atual) return;
            const leaseAtivo = atual.estado === 'enviando'
                && Number(atual.leaseAte || 0) > agora
                && atual.tokenSincronizacao !== tokenSincronizacao;
            if (leaseAtivo) return;
            reservado = {
                ...atual,
                estado: 'enviando',
                fase: atual.fase || 'iniciando_envio',
                tentativas: Number(atual.tentativas || 0) + 1,
                ultimaTentativaEm: new Date(agora).toISOString(),
                tokenSincronizacao,
                leaseAte: agora + DURACAO_RESERVA_ENVIO_MS,
                prioridadeEnvio: false,
                prioridadeSolicitadaEm: null,
                ultimoErro: null,
                atualizadoEm: new Date(agora).toISOString()
            };
            store.put(reservado);
        };
        tx.oncomplete = () => resolve(reservado);
        tx.onerror = () => reject(tx.error);
    });
}

export async function priorizarPendente(id) {
    const idLimpo = String(id || '').trim();
    if (!idLimpo) return null;
    return atualizarPendente(idLimpo, {
        prioridadeEnvio: true,
        prioridadeSolicitadaEm: new Date().toISOString()
    });
}

function iniciarRenovacaoReserva(id, tokenSincronizacao) {
    let encerrada = false;
    const renovar = () => atualizarPendente(id, {
        estado: 'enviando',
        leaseAte: Date.now() + DURACAO_RESERVA_ENVIO_MS
    }, tokenSincronizacao).catch(() => null);
    const timer = globalThis.setInterval(renovar, INTERVALO_RENOVACAO_RESERVA_MS);
    return async () => {
        if (encerrada) return;
        encerrada = true;
        globalThis.clearInterval(timer);
    };
}

async function marcarFalhaPendente(id, tokenSincronizacao, erro) {
    return atualizarPendente(id, {
        estado: 'pendente',
        ultimoErro: String(erro?.message || erro || 'Falha de sincronizacao').slice(0, 500),
        ultimaFalhaEm: new Date().toISOString(),
        tokenSincronizacao: null,
        leaseAte: 0,
        prioridadeEnvio: false,
        prioridadeSolicitadaEm: null
    }, tokenSincronizacao);
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

export async function enfileirarPixPossePendente(pendencia) {
    const id = String(pendencia?.id || '').trim();
    if (!id) throw new Error('Pendencia de Pix em posse sem id local.');
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(PIX_POSSE_PENDENTES, 'readwrite');
        tx.objectStore(PIX_POSSE_PENDENTES).put({
            ...pendencia,
            id,
            criadoEm: pendencia?.criadoEm || new Date().toISOString(),
            tentativas: Number(pendencia?.tentativas || 0)
        });
        tx.oncomplete = () => resolve(id);
        tx.onerror = () => reject(tx.error);
    });
}

export async function listarPixPossePendentes() {
    try {
        const db = await abrirDB();
        return new Promise((resolve, reject) => {
            const req = db.transaction(PIX_POSSE_PENDENTES, 'readonly').objectStore(PIX_POSSE_PENDENTES).getAll();
            req.onsuccess = () => resolve(req.result || []);
            req.onerror = () => reject(req.error);
        });
    } catch(e) { return []; }
}

export async function removerPixPossePendente(id) {
    const chave = String(id || '').trim();
    if (!chave) return;
    const db = await abrirDB();
    return new Promise((resolve, reject) => {
        const req = db.transaction(PIX_POSSE_PENDENTES, 'readwrite').objectStore(PIX_POSSE_PENDENTES).delete(chave);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
    });
}

async function incrementarTentativaPixPossePendente(item) {
    const id = String(item?.id || '').trim();
    if (!id) return;
    await enfileirarPixPossePendente({
        ...item,
        tentativas: Number(item?.tentativas || 0) + 1,
        ultimaTentativaEm: new Date().toISOString()
    });
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
let sincronizacaoAtendimentosEmAndamento = null;

async function executarComTravaGlobalSincronizacao(tarefa) {
    if (!globalThis.navigator?.locks?.request) return tarefa();
    return navigator.locks.request('play-atendimentos-sync', { ifAvailable: true }, async (lock) => {
        if (!lock) return 0;
        return tarefa();
    });
}

export async function sincronizarPendentes(storageSalvarFotoComThumb, dbSalvarAtendimento, dbSincronizarProdutosAtendimentoNoHistorico = null, opcoes = {}) {
    if (!navigator.onLine) return 0;
    if (sincronizacaoAtendimentosEmAndamento) return sincronizacaoAtendimentosEmAndamento;

    sincronizacaoAtendimentosEmAndamento = executarComTravaGlobalSincronizacao(async () => {
        const idPendente = String(opcoes?.idPendente || '').trim();
        let ok = 0;
        const processados = new Set();
        const tokenSincronizacao = globalThis.crypto?.randomUUID?.()
            || `sync_${Date.now()}_${Math.random().toString(36).slice(2)}`;

        while (true) {
            const pendentes = ordenarPendenciasAtendimento((await listarPendentes())
                .filter(item => !idPendente || String(item?.id || '').trim() === idPendente)
                .filter(item => !processados.has(String(item?.id || ''))));
            const resumo = pendentes[0];
            if (!resumo) break;
            processados.add(String(resumo.id));
            const item = await reservarPendente(resumo.id, tokenSincronizacao);
            if (!item) continue;
            const encerrarRenovacao = iniciarRenovacaoReserva(item.id, tokenSincronizacao);
            try {
                const resultadoProcessamento = await processarAtendimentoPendente({
                    item,
                    gerarAtendimentoId: opcoes?.gerarAtendimentoId,
                    salvarFoto: storageSalvarFotoComThumb,
                    salvarAtendimento: dbSalvarAtendimento,
                    atualizarFotoAtendimento: opcoes?.atualizarFotoAtendimento,
                    sincronizarProdutos: dbSincronizarProdutosAtendimentoNoHistorico,
                    verificarRota: opcoes?.verificarRota,
                    confirmarAtendimento: opcoes?.confirmarAtendimento,
                    confirmarProdutos: opcoes?.confirmarProdutos,
                    confirmarRota: opcoes?.confirmarRota,
                    pararAposRegistroInicial: opcoes?.somenteRegistroInicial === true,
                    atualizarItem: async (patch) => {
                        const atualizado = await atualizarPendente(item.id, {
                            ...patch,
                            estado: 'enviando',
                            leaseAte: Date.now() + DURACAO_RESERVA_ENVIO_MS
                        }, tokenSincronizacao);
                        if (!atualizado) throw new Error('A reserva local do atendimento expirou.');
                        return atualizado;
                    }
                });
                if (resultadoProcessamento?.parcial === true) {
                    const liberado = await atualizarPendente(item.id, {
                        estado: 'pendente',
                        tokenSincronizacao: null,
                        leaseAte: 0
                    }, tokenSincronizacao);
                    if (!liberado) throw new Error('Nao foi possivel liberar a fila apos o envio inicial.');
                    break;
                }
                const concluido = await atualizarPendente(item.id, {
                    estado: 'enviado',
                    fase: 'concluido',
                    concluidoEm: new Date().toISOString()
                }, tokenSincronizacao);
                if (!concluido) throw new Error('A conclusao local do atendimento nao foi confirmada.');
                await removerPendente(item.id, tokenSincronizacao);
                ok++;
            } catch(e) {
                await marcarFalhaPendente(item.id, tokenSincronizacao, e).catch(() => {});
                console.warn('offline-sync: falha ao sincronizar item', item.id, e);
                break;
            } finally {
                await encerrarRenovacao();
            }
        }
        return ok;
    });

    try {
        return await sincronizacaoAtendimentosEmAndamento;
    } finally {
        sincronizacaoAtendimentosEmAndamento = null;
    }
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
            const nomeEstavel = `pendente-${String(foto?.id || atendimentoId).replace(/[^a-zA-Z0-9_-]/g, '_')}`;
            const r = await storageSalvarFotoComThumb(base64, foto?.pasta || 'atendimentos', 200, nomeEstavel);
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

export async function sincronizarPixPossePendentes(dbAdicionarPixEmPosse) {
    if (!navigator.onLine || typeof dbAdicionarPixEmPosse !== 'function') {
        return { ok: 0, falhas: 0, total: 0 };
    }

    const pendentes = await listarPixPossePendentes();
    let ok = 0;
    let falhas = 0;

    for (const item of pendentes) {
        try {
            const usuario = String(item?.usuario || item?.responsavel || '').trim();
            const numeroPix = String(item?.numeroPix || item?.numero_pix || '').trim();
            if (!usuario || !numeroPix) {
                await removerPixPossePendente(item.id);
                continue;
            }

            await dbAdicionarPixEmPosse(usuario, numeroPix, item?.dadosExtras || {});
            await removerPixPossePendente(item.id);
            ok++;
        } catch(e) {
            falhas++;
            await incrementarTentativaPixPossePendente(item).catch(() => {});
            console.warn('offline-sync: falha ao sincronizar Pix em posse pendente', item?.id, e);
            break;
        }
    }

    return { ok, falhas, total: pendentes.length };
}
