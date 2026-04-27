/* =========================================================================
   PROJETO: PLAY NA WEB
   BANCO DE DADOS: FIREBASE REALTIME DATABASE
   OBJETIVO: Este arquivo é o "mensageiro". Ele leva dados do site para o servidor e traz de volta.
   ========================================================================= */

// --- 1. IMPORTAÇÃO DAS FERRAMENTAS ---
// Aqui estamos "pegando emprestado" as funções prontas do Google (Firebase) para não ter que criar tudo do zero.
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js";
import {
    getDatabase,
    ref,
    set,
    get,
    push,
    remove,
    onValue,
    update,
    query,
    orderByChild,
    equalTo,
    startAt,
    limitToLast,
    runTransaction
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import { getStorage, ref as storageRef, uploadBytes, getDownloadURL } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-storage.js";
import { salvarCacheClientes, lerCacheClientes, lerCacheClientesCompleto, salvarCacheEstoque, lerCacheEstoque } from './offline-sync.js';

// --- 2. CONFIGURAÇÃO (AS CHAVES DO COFRE) ---
// Estas são as credenciais que permitem que seu site converse especificamente com o SEU banco de dados.
const firebaseConfig = {
    apiKey: "AIzaSyAog2lzvvWkOSvr8BqPgtGCZpSM4VQ2b3E",
    authDomain: "play-na-web.firebaseapp.com",
    databaseURL: "https://play-na-web-default-rtdb.firebaseio.com", 
    projectId: "play-na-web",
    storageBucket: "play-na-web.firebasestorage.app",
    messagingSenderId: "278404685529",
    appId: "1:278404685529:web:c8e7dc89eeb660173ae8c8"
};

// --- 3. INICIALIZAÇÃO ---
// Aqui ligamos o "motor" do Firebase usando as configurações acima.
export const app = initializeApp(firebaseConfig);
const db = getDatabase(app);
const storage = getStorage(app);

/* --- FUNÇÕES PARA CLIENTES --- */

// FUNÇÃO: ESCUTAR CLIENTES (Em Tempo Real)
// O que faz: Fica "de ouvidos abertos". Sempre que alguém mudar algo no banco de dados,
// essa função avisa o site instantaneamente para atualizar a tela sem precisar recarregar (F5).
let clientesCacheMemoria = null;
let clientesCacheVersaoMemoria = 0;

function _normalizarListaClientes(data) {
    if (!data) return [];
    return Object.keys(data).map(key => ({ ...data[key], firebaseUrl: key }));
}

async function _lerRegistroCacheClientes() {
    const registro = await lerCacheClientesCompleto();
    const dados = Array.isArray(registro?.dados) ? registro.dados : [];
    const versao = Number(registro?.versao || 0);

    clientesCacheMemoria = dados;
    clientesCacheVersaoMemoria = versao;

    return { dados, versao };
}

async function _obterVersaoClientesRemota() {
    try {
        const snap = await get(ref(db, 'metadata/clientes_versao'));
        return Number(snap.val() || 0);
    } catch (error) {
        console.warn("NÃ£o foi possÃ­vel ler versÃ£o dos clientes:", error);
        return 0;
    }
}

async function _baixarListaClientesDoServidor(versaoRemota = 0) {
    const snapshot = await get(ref(db, 'clientes'));
    const lista = snapshot.exists() ? _normalizarListaClientes(snapshot.val()) : [];
    const versaoParaCache = Number(versaoRemota || Date.now());

    clientesCacheMemoria = lista;
    clientesCacheVersaoMemoria = versaoParaCache;
    salvarCacheClientes(lista, versaoParaCache).catch(() => {});

    return lista;
}

async function _obterListaClientesAtualizada({ forcarServidor = false } = {}) {
    const cache = await _lerRegistroCacheClientes();
    if (!navigator.onLine) return cache.dados;

    const versaoRemota = await _obterVersaoClientesRemota();
    if (!forcarServidor && cache.versao > 0 && versaoRemota > 0 && cache.versao === versaoRemota) {
        return cache.dados;
    }

    return _baixarListaClientesDoServidor(versaoRemota);
}

function dbEscutarClientesLegacy(callback) {
    // Serve o cache do IndexedDB imediatamente (útil ao abrir offline após um reload)
    lerCacheClientes().then(cached => {
        if (cached.length > 0) callback(cached);
    }).catch(() => {});

    const clientesRef = ref(db, 'clientes');
    onValue(clientesRef, (snapshot) => {
        const data = snapshot.val();
        if (data) {
            const lista = Object.keys(data).map(key => ({ ...data[key], firebaseUrl: key }));
            callback(lista);
            salvarCacheClientes(lista).catch(() => {}); // espelha no IndexedDB
        } else if (navigator.onLine) {
            callback([]); // só limpa se realmente não há dados (e temos internet)
        }
    });
}

// FUNÇÃO INTERNA: Atualiza o timestamp de versão dos clientes no banco.
// Usada após qualquer alteração na lista de clientes para avisar todos os dispositivos.
export function dbEscutarClientes(callback) {
    let ativo = true;
    let ultimoPayload = null;

    const emitir = (lista) => {
        if (!ativo) return;
        const payload = JSON.stringify(lista || []);
        if (payload === ultimoPayload) return;
        ultimoPayload = payload;
        callback(lista || []);
    };

    _lerRegistroCacheClientes()
        .then(({ dados }) => {
            if (dados.length > 0) emitir(dados);
        })
        .catch(() => {});

    const cancelarVersao = onValue(ref(db, 'metadata/clientes_versao'), async (snapshot) => {
        if (!ativo) return;
        const versaoRemota = Number(snapshot.val() || 0);

        try {
            if (!navigator.onLine) {
                if (clientesCacheMemoria && clientesCacheMemoria.length > 0) emitir(clientesCacheMemoria);
                return;
            }

            if (clientesCacheMemoria !== null && clientesCacheVersaoMemoria > 0 && versaoRemota > 0 && clientesCacheVersaoMemoria === versaoRemota) {
                emitir(clientesCacheMemoria);
                return;
            }

            const lista = await _obterListaClientesAtualizada({
                forcarServidor: versaoRemota > 0 && clientesCacheVersaoMemoria > 0 && clientesCacheVersaoMemoria !== versaoRemota
            });
            emitir(lista);
        } catch (error) {
            console.error("ERRO AO SINCRONIZAR CLIENTES:", error);
            if (clientesCacheMemoria && clientesCacheMemoria.length > 0) emitir(clientesCacheMemoria);
        }
    });

    return () => {
        ativo = false;
        if (typeof cancelarVersao === 'function') cancelarVersao();
    };
}

async function _atualizarVersaoClientes() {
    try {
        await set(ref(db, 'metadata/clientes_versao'), Date.now());
    } catch (e) {
        // Não bloquear a operação principal se isso falhar
        console.warn("Não foi possível atualizar versão dos clientes:", e);
    }
}

// FUNÇÃO: ESCUTAR VERSÃO DOS CLIENTES
// O que faz: Escuta APENAS um número (timestamp) no banco. Quando ele muda,
// significa que alguém alterou a lista de clientes. Economiza dados pois não
// baixa a lista inteira — só avisa que ela mudou.
async function _atualizarVersaoMediaDeVendas() {
    try {
        await set(ref(db, 'metadata/media_de_vendas_versao'), Date.now());
    } catch (e) {
        console.warn("NÃ£o foi possÃ­vel atualizar versÃ£o da Media_de_Vendas:", e);
    }
}

export function dbEscutarVersaoClientes(callback) {
    onValue(ref(db, 'metadata/clientes_versao'), (snap) => {
        callback(snap.val());
    });
}

export function dbEscutarVersaoMediaDeVendas(callback) {
    onValue(ref(db, 'metadata/media_de_vendas_versao'), (snap) => {
        callback(snap.val());
    });
}

// FUNÇÃO: SALVAR CLIENTE (Criar ou Editar)
// O que faz: Verifica se é um cliente novo ou antigo.
// Se tiver ID (idExistente), ele atualiza os dados. Se não, cria um novo registro.
export async function dbSalvarCliente(cliente, idExistente = null) {
    try {
        let clienteId = String(idExistente || '').trim();
        if (idExistente) {
            // Modo Edição: Atualiza o cliente específico
            const clienteRef = ref(db, `clientes/${idExistente}`);
            const snapAnt = await get(clienteRef);
            const dadosAnt = snapAnt.exists() ? snapAnt.val() : null;
            const numeroAntigo = dadosAnt?.numero ?? null;
            // Usa update para preservar campos calculados/gerados pelo sistema (ex: venda_por_dia)
            await update(clienteRef, cliente);
            await _upsertMediaDeVendasPorCliente(cliente, idExistente, numeroAntigo);
            await _atualizarVersaoMediaDeVendas();
        } else {
            // Modo Criação: Cria um novo cliente com chave única
            const clientesRef = ref(db, 'clientes');
            const novoRef = await push(clientesRef, cliente);
            clienteId = novoRef.key || '';
            await _upsertMediaDeVendasPorCliente(cliente, novoRef.key || null, null);
            await _atualizarVersaoMediaDeVendas();
        }
        // Avisa todos os dispositivos que a lista mudou
        await _atualizarVersaoClientes();
        await _tentarRecalcularRemuneracoes();
        return clienteId || null;
    } catch (error) {
        console.error("ERRO AO SALVAR CLIENTE:", error);
        throw error;
    }
}

// [BLOCO: EMERGÊNCIA - LIMPAR TUDO]
export async function dbLimparHistoricoCompleto() {
    try {
        const histRef = ref(db, 'historico_estoque');
        await remove(histRef);
        await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("Erro ao limpar histórico completo:", error);
        throw error;
    }
}

// [BLOCO: HISTÓRICO - EXCLUIR]
export async function dbExcluirHistorico(id) {
    try {
        const histRef = ref(db, `historico_estoque/${id}`);
        await remove(histRef);
        await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("Erro ao excluir histórico:", error);
        throw error;
    }
}

// [BLOCO: HISTÓRICO DE MOVIMENTAÇÃO]
// Grava um registro eterno de cada entrada ou saída
export async function dbSalvarHistorico(movimento) {
    try {
        const histRef = ref(db, 'historico_estoque');
        await push(histRef, movimento);
        await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("Erro ao salvar histórico:", error);
    }
}

// Escuta apenas os últimos 20 movimentos para exibir na tela
export async function dbListarHistorico() {
    try {
        const snapshot = await get(ref(db, 'historico_estoque'));
        if (!snapshot.exists()) return [];
        const data = snapshot.val();
        return Object.keys(data).map(key => ({ firebaseUrl: key, ...data[key] }));
    } catch (error) {
        console.error("Erro ao listar histÃ³rico:", error);
        return [];
    }
}

export function dbEscutarHistoricoCompleto(callback) {
    onValue(ref(db, 'historico_estoque'), (snapshot) => {
        const data = snapshot.val() || {};
        callback(Object.keys(data).map(key => ({ firebaseUrl: key, ...data[key] })));
    });
}

export function dbEscutarHistorico(callback) {
    const histRef = ref(db, 'historico_estoque');
    const ultimosQuery = query(histRef, limitToLast(20));

    onValue(ultimosQuery, (snapshot) => {
        const data = snapshot.val();
        const lista = [];
        if (data) {
            // Mapeia adicionando o ID (firebaseUrl)
            Object.keys(data).forEach(key => {
                lista.push({ ...data[key], firebaseUrl: key });
            });
        }
        // O Firebase devolve na ordem cronológica (antigo -> novo), vamos inverter na tela depois
        callback(lista);
    });
}

// FUNÇÃO: EXCLUIR CLIENTE
// O que faz: Remove permanentemente o cliente do banco de dados baseado no ID.
export async function dbExcluirCliente(id) {
    try {
        // Remove também da base Media_de_Vendas (id = número do cliente)
        try {
            const snap = await get(ref(db, `clientes/${id}`));
            const numero = snap.exists() ? (snap.val()?.numero ?? null) : null;
            const key = _normalizarNumeroCliente(numero);
            if (key) await remove(ref(db, `Media_de_Vendas/${key}`));
        } catch (e) {
            console.warn("Não foi possível remover cliente de Media_de_Vendas:", e);
        }
        const clienteRef = ref(db, `clientes/${id}`);
        await remove(clienteRef);
        // Avisa todos os dispositivos que a lista mudou
        await _atualizarVersaoClientes();
    } catch (error) {
        console.error("ERRO AO EXCLUIR CLIENTE:", error);
        throw error;
    }
}

// FUNÇÃO: LISTAR CLIENTES (Uma única vez)
// O que faz: Tira uma "foto" (snapshot) do banco naquele momento.
// Diferente do "Escutar", este não fica vigiando alterações futuras.
export async function dbListarClientes() {
    try {
        return await _obterListaClientesAtualizada();
    } catch (error) {
        console.error("ERRO AO LISTAR CLIENTES:", error);
    }
    return [];
}

/* --- FUNÇÕES PARA COLABORADORES --- */

// FUNÇÃO: ESCUTAR COLABORADORES (Com Ordem)
// O que faz: Igual ao de clientes, mas com um passo extra: ORDENAÇÃO.
// Garante que a lista apareça na ordem que você definiu (arrastar e soltar).
export function dbEscutarColaboradores(callback) {
    const colabRef = ref(db, 'colaboradores'); 
    onValue(colabRef, (snapshot) => {
        const data = snapshot.val();
        if (data) {
            const lista = Object.keys(data).map(key => ({
                ...data[key],
                firebaseUrl: key
            })).sort((a, b) => {
                // CORREÇÃO: Agora ordena pelo campo 'ordem' para respeitar o arrastar e soltar
                return (a.ordem || 0) - (b.ordem || 0); 
            });
            callback(lista);
        } else {
            callback([]);
        }
    });
}

export async function dbListarColaboradores() {
    const snapshot = await get(ref(db, 'colaboradores'));
    const data = snapshot.val();
    if (!data) return [];
    return Object.keys(data).map(key => ({ ...data[key], firebaseUrl: key }));
}

// FUNÇÃO: SALVAR COLABORADOR
// O que faz: Salva dados do funcionário.
// Truque especial: Usa um número negativo (-Date.now()) para que novos cadastros
// apareçam automaticamente no topo da lista antes de você reordenar.
export async function dbSalvarColaborador(colaborador, idExistente = null) {
    try {
        const colaboradorNormalizado = {
            ...colaborador,
            nome: String(colaborador?.nome || '').trim()
        };
        if (idExistente) {
            const colabRef = ref(db, `colaboradores/${idExistente}`);
            const snapshot = await get(colabRef);
            const dadosAntigos = snapshot.val();
            // Mantém a posição na fila se já existir (não joga pro final)
            if (dadosAntigos && dadosAntigos.ordem !== undefined) {
                colaboradorNormalizado.ordem = dadosAntigos.ordem;
            }
            await set(colabRef, colaboradorNormalizado);
        } else {
            // USANDO O TIMESTAMP NEGATIVO: 
            // Quanto mais recente o cadastro, menor o número, logo, fica no topo.
            colaboradorNormalizado.ordem = -Date.now();
            const colabRef = ref(db, 'colaboradores');
            await push(colabRef, colaboradorNormalizado);
        }
        await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("Erro:", error);
        throw error;
    }
}

// NOVA FUNÇÃO: Necessária para gravar a posição após o arraste
// O que faz: Atualiza APENAS o número da ordem, sem mexer no nome ou foto.
// É usada quando você solta o card na tela de gestão.
export async function dbAtualizarOrdemColaborador(id, novaOrdem) {
    try {
        const colabRef = ref(db, `colaboradores/${id}`);
        await update(colabRef, { ordem: novaOrdem });
    } catch (error) {
        console.error("ERRO AO ATUALIZAR ORDEM:", error);
    }
}

// FUNÇÃO: EXCLUIR COLABORADOR
export async function dbExcluirColaborador(id) {
    try {
        const colabRef = ref(db, `colaboradores/${id}`);
        await remove(colabRef);
    } catch (error) {
        console.error("ERRO AO EXCLUIR COLABORADOR:", error);
        throw error;
    }
}

// [BLOCO: ESTOQUE - SALVAR ITEM]
export async function dbSalvarItemEstoque(item, id = null, opcoes = {}) {
    try {
        const recalcular = opcoes?.recalcular !== false;
        if (id) {
            // Se tem ID, atualiza o item existente
            const itemRef = ref(db, `estoque/${id}`);
            await update(itemRef, item);
        } else {
            // Se não tem ID, cria um novo
            const estoqueRef = ref(db, 'estoque');
            await push(estoqueRef, item);
        }
        if (recalcular) await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("Erro ao salvar no estoque:", error);
        throw error;
    }
}

// [BLOCO: ESTOQUE - LER ITENS]
export function dbEscutarEstoque(callback) {
    // Serve o cache do IndexedDB imediatamente quando offline
    lerCacheEstoque().then(cached => {
        if (cached.length > 0) callback(cached);
    }).catch(() => {});

    const estoqueRef = ref(db, 'estoque');
    onValue(estoqueRef, (snapshot) => {
        const dados = snapshot.val();
        const lista = [];
        if (dados) {
            for (let key in dados) {
                lista.push({ firebaseUrl: key, ...dados[key] });
            }
        }
        lista.sort((a, b) => (a.nome || "").toUpperCase().localeCompare((b.nome || "").toUpperCase()));
        callback(lista);
        if (lista.length) salvarCacheEstoque(lista).catch(() => {}); // espelha no IndexedDB
    });
}

// [BLOCO: ESTOQUE - LISTAR ITENS]
// Leitura pontual usada em telas que precisam cruzar categoria sem abrir um listener adicional.
export async function dbListarEstoque() {
    try {
        const snapshot = await get(ref(db, 'estoque'));
        if (!snapshot.exists()) return [];
        const dados = snapshot.val();
        const lista = Object.keys(dados).map(key => ({ firebaseUrl: key, ...dados[key] }));
        lista.sort((a, b) => (a.nome || "").toUpperCase().localeCompare((b.nome || "").toUpperCase()));
        return lista;
    } catch (error) {
        console.error("Erro ao listar estoque:", error);
        return [];
    }
}

// [BLOCO: ESTOQUE - EXCLUIR ITEM]
export async function dbExcluirItemEstoque(id) {
    try {
        const itemRef = ref(db, `estoque/${id}`);
        await remove(itemRef);
        await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("Erro ao excluir do estoque:", error);
        throw error;
    }
}

// [BLOCO: PEDIDOS DE COMPRA]

// Salvar ou Atualizar Pedido
export async function dbSalvarPedido(pedido, id = null) {
    try {
        if (id) {
            await update(ref(db, `pedidos/${id}`), pedido);
        } else {
            await push(ref(db, 'pedidos'), pedido);
        }
    } catch (error) {
        console.error("Erro ao salvar pedido:", error);
        throw error;
    }
}

// Ler Pedidos
export function dbEscutarPedidos(callback) {
    onValue(ref(db, 'pedidos'), (snapshot) => {
        const dados = snapshot.val();
        const lista = dados ? Object.keys(dados).map(key => ({ firebaseUrl: key, ...dados[key] })) : [];
        callback(lista);
    });
}

// Excluir Pedido (Confirmar Compra)
export async function dbExcluirPedido(id) {
    try {
        await remove(ref(db, `pedidos/${id}`));
    } catch (error) {
        console.error("Erro ao excluir pedido:", error);
    }
}

/* --- FUNÇÕES PARA ATENDIMENTO --- */

// [NOVO] Helper para converter Base64 em Blob para upload
function base64ToBlob(base64, contentType = 'image/jpeg') {
    const byteCharacters = atob(base64.split(',')[1]);
    const byteArrays = [];
    for (let offset = 0; offset < byteCharacters.length; offset += 512) {
        const slice = byteCharacters.slice(offset, offset + 512);
        const byteNumbers = new Array(slice.length);
        for (let i = 0; i < slice.length; i++) {
            byteNumbers[i] = slice.charCodeAt(i);
        }
        const byteArray = new Uint8Array(byteNumbers);
        byteArrays.push(byteArray);
    }
    return new Blob(byteArrays, { type: contentType });
}

// [NOVO] Salvar uma foto no Firebase Storage
export async function storageSalvarFoto(base64String, pasta = 'atendimentos') {
    // Offline: devolve o base64 como está — será enviado quando a internet voltar
    if (!navigator.onLine) return base64String;
    try {
        // 1. Converte a string base64 para um formato de arquivo (Blob)
        const blob = base64ToBlob(base64String);
        
        // 2. Cria um nome de arquivo único para evitar sobreposições
        const nomeArquivo = `${String(pasta || 'atendimentos').replace(/^\/+|\/+$/g, '')}/${Date.now()}-${Math.round(Math.random() * 1E9)}.jpg`;
        const fotoRef = storageRef(storage, nomeArquivo);
        
        // 3. Faz o upload do arquivo (Com limite de 15 segundos para não travar)
        const uploadPromise = uploadBytes(fotoRef, blob);
        const timeoutPromise = new Promise((_, reject) => setTimeout(() => reject(new Error("Tempo limite de upload excedido. A internet pode estar instável.")), 15000));
        
        const snapshot = await Promise.race([uploadPromise, timeoutPromise]);
        
        // 4. Pega a URL pública do arquivo que acabamos de subir
        const downloadURL = await getDownloadURL(snapshot.ref);
        
        return downloadURL;
    } catch (error) {
        console.error("Erro ao fazer upload da foto:", error);
        throw error; // Re-lança o erro para ser tratado na tela de atendimento
    }
}

// Redimensiona uma imagem base64 para miniatura usando Canvas
async function _redimensionarParaThumb(base64, maxPx = 200) {
    return new Promise((resolve) => {
        const img = new Image();
        img.onload = () => {
            const ratio = Math.min(maxPx / img.width, maxPx / img.height, 1);
            if (ratio >= 1) { resolve(base64); return; }
            const w = Math.round(img.width * ratio);
            const h = Math.round(img.height * ratio);
            const canvas = document.createElement('canvas');
            canvas.width = w;
            canvas.height = h;
            canvas.getContext('2d').drawImage(img, 0, 0, w, h);
            resolve(canvas.toDataURL('image/jpeg', 0.75));
        };
        img.onerror = () => resolve(base64);
        img.src = base64;
    });
}

// Salva foto original + miniatura. Retorna { url, thumbUrl }.
// Offline: retorna base64 de ambas para sync posterior.
export async function storageSalvarFotoComThumb(base64String, pasta = 'atendimentos', thumbMaxPx = 200) {
    const thumbBase64 = await _redimensionarParaThumb(base64String, thumbMaxPx).catch(() => base64String);
    if (!navigator.onLine) return { url: base64String, thumbUrl: thumbBase64 };
    try {
        const uploads = [storageSalvarFoto(base64String, pasta)];
        if (thumbBase64 !== base64String) {
            uploads.push(storageSalvarFoto(thumbBase64, pasta + '/thumbs'));
        }
        const [url, thumbUrl] = await Promise.all(uploads);
        return { url, thumbUrl: thumbUrl || url };
    } catch (error) {
        console.error("Erro ao salvar foto com miniatura:", error);
        throw error;
    }
}

// [NOVO] Salvar o registro completo do atendimento
function _obterTimestampOrdenacaoAtendimento(atendimento) {
    const dataBase = atendimento?.ultimaEdicao || atendimento?.data || "";
    const timestamp = Date.parse(dataBase);
    return Number.isNaN(timestamp) ? 0 : timestamp;
}

function _extrairPixValidosAtendimento(atendimento) {
    return (Array.isArray(atendimento?.financeiro?.pix) ? atendimento.financeiro.pix : [])
        .map(item => {
            const numero = _normalizarNumeroPix(item?.numero);
            const bruto = item?.contAtual;
            if (!numero || bruto === null || bruto === undefined || bruto === "") return null;

            const contAtual = typeof bruto === "number"
                ? bruto
                : Number(String(bruto).replace(/\D/g, ""));

            if (!Number.isFinite(contAtual)) return null;
            return { numero, contAtual: String(contAtual) };
        })
        .filter(Boolean);
}

async function _listarAtendimentosDoCliente(clienteRef = {}) {
    const resultados = new Map();
    const clienteId = String(clienteRef?.id || clienteRef?.firebaseUrl || "").trim();
    const numeroCliente = _normalizarNumeroCliente(clienteRef?.numero);

    const adicionarSnapshot = (snapshot) => {
        if (!snapshot.exists()) return;
        const data = snapshot.val() || {};
        Object.keys(data).forEach(key => {
            resultados.set(key, { firebaseUrl: key, ...data[key] });
        });
    };

    if (clienteId) {
        const snapPorId = await get(query(ref(db, 'atendimentos'), orderByChild('cliente/id'), equalTo(clienteId)));
        adicionarSnapshot(snapPorId);
    }

    if (numeroCliente) {
        const valoresTentativa = [numeroCliente];
        const numeroComoNumero = Number(numeroCliente);
        if (Number.isFinite(numeroComoNumero)) valoresTentativa.unshift(numeroComoNumero);

        for (const valor of valoresTentativa) {
            const snapPorNumero = await get(query(ref(db, 'atendimentos'), orderByChild('cliente/numero'), equalTo(valor)));
            adicionarSnapshot(snapPorNumero);
        }
    }

    return Array.from(resultados.values());
}

async function _sincronizarContadorAtualClientePorAtendimento(atendimento) {
    try {
        if (atendimento?._teste) return;

        const pixValidos = _extrairPixValidosAtendimento(atendimento);
        if (pixValidos.length === 0) return;

        const clienteRef = atendimento?.cliente || {};
        const clienteId = String(clienteRef?.id || clienteRef?.firebaseUrl || "").trim();
        let cliente = clienteId
            ? await dbBuscarClientePorId(clienteId)
            : await dbBuscarClientePorNumero(clienteRef?.numero);

        if (!cliente && clienteRef?.numero != null) {
            cliente = await dbBuscarClientePorNumero(clienteRef.numero);
        }
        if (!cliente?.firebaseUrl) return;

        const equipamentos = _normalizarEquipDetalhesCliente(cliente);
        if (equipamentos.length === 0) return;

        const numerosAlvo = new Set(pixValidos.map(item => item.numero));
        if (!equipamentos.some(item => numerosAlvo.has(_normalizarNumeroPix(item?.pix)))) return;

        let ultimosContadores = {};
        try {
            const atendimentosCliente = await _listarAtendimentosDoCliente({
                id: cliente.firebaseUrl,
                numero: cliente.numero
            });

            atendimentosCliente
                .sort((a, b) => _obterTimestampOrdenacaoAtendimento(b) - _obterTimestampOrdenacaoAtendimento(a))
                .forEach(item => {
                    _extrairPixValidosAtendimento(item).forEach(pix => {
                        if (!numerosAlvo.has(pix.numero) || ultimosContadores[pix.numero] != null) return;
                        ultimosContadores[pix.numero] = pix.contAtual;
                    });
                });
        } catch (erroBusca) {
            console.warn("NÃO FOI POSSÍVEL REPROCESSAR CONTADORES DO CLIENTE:", erroBusca);
        }

        pixValidos.forEach(item => {
            if (ultimosContadores[item.numero] == null) {
                ultimosContadores[item.numero] = item.contAtual;
            }
        });

        let houveMudanca = false;
        const equipamentosAtualizados = equipamentos.map(item => {
            const numeroPix = _normalizarNumeroPix(item?.pix);
            if (!numeroPix || ultimosContadores[numeroPix] == null) return item;

            const contadorAtualizado = String(ultimosContadores[numeroPix]).trim();
            if (String(item?.contador || "").trim() === contadorAtualizado) return item;

            houveMudanca = true;
            return { ...item, contador: contadorAtualizado };
        });

        if (!houveMudanca) return;

        await update(ref(db, `clientes/${cliente.firebaseUrl}`), {
            equipDetalhes: equipamentosAtualizados,
            equip: _serializarEquipTextoCliente(equipamentosAtualizados)
        });
        await _atualizarVersaoClientes();
    } catch (erro) {
        console.error("ERRO AO SINCRONIZAR CONTADOR DO CLIENTE PELO ATENDIMENTO:", erro);
    }
}

export async function dbContestarAtendimento(id, nomeGestor) {
    await update(ref(db, `atendimentos/${id}`), {
        contestado: true,
        contestadoAte: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(),
        contestadoPor: nomeGestor
    });
}

export async function dbRemoverContestacao(id) {
    await update(ref(db, `atendimentos/${id}`), {
        contestado: null,
        contestadoAte: null,
        contestadoPor: null
    });
}

export async function dbSalvarAtendimento(atendimento, idExistente = null) {
    try {
        let atendimentoId = String(idExistente || '').trim();
        const payloadAtendimento = { ...(atendimento || {}) };
        const ehAtendimentoComum = !payloadAtendimento?.origemRegistro || payloadAtendimento.origemRegistro === 'atendimento';
        if (ehAtendimentoComum) delete payloadAtendimento.produtos;
        if (idExistente) {
            await set(ref(db, `atendimentos/${idExistente}`), payloadAtendimento);
        } else {
            // Cria uma nova entrada na coleção 'atendimentos'
            const atendimentosRef = ref(db, 'atendimentos');
            const novoRef = await push(atendimentosRef, payloadAtendimento);
            atendimentoId = String(novoRef?.key || '').trim();
        }
        await _sincronizarContadorAtualClientePorAtendimento(atendimento);
        // Atualiza a base "Media_de_Vendas" (tempo real, sem depender do cache offline)
        await _upsertMediaDeVendasPorAtendimento(atendimento);
        await _atualizarVersaoMediaDeVendas();
        await _tentarRecalcularRemuneracoes();
        return atendimentoId || null;
    } catch (error) {
        console.error("ERRO AO SALVAR ATENDIMENTO:", error);
        throw error;
    }
}

export async function dbListarAtendimentosRecentes(limite = 800) {
    try {
        const n = Math.max(1, Math.min(Number(limite) || 800, 5000));
        const atendRef = ref(db, 'atendimentos');
        const snap = await get(query(atendRef, limitToLast(n)));
        if (!snap.exists()) return [];
        const data = snap.val();
        return Object.keys(data).map(key => ({
            ...data[key],
            firebaseUrl: key
        }));
    } catch (e) {
        console.error("ERRO AO LISTAR ATENDIMENTOS RECENTES:", e);
        return [];
    }
}

// [NOVO] Escutar todos os atendimentos salvos
export function dbEscutarAtendimentos(callback) {
    const atendimentosRef = ref(db, 'atendimentos');
    onValue(atendimentosRef, (snapshot) => {
        const data = snapshot.val();
        const lista = [];
        if (data) {
            Object.keys(data).forEach(key => {
                lista.push({ firebaseUrl: key, ...data[key] });
            });
        }
        callback(lista);
    });
}

export function dbEscutarHistoricoDoUsuario(nomeUsuario, callback) {
    const histRef = ref(db, 'historico_estoque');
    const nomeNormalizado = String(nomeUsuario || '').trim();

    return onValue(histRef, (snapshot) => {
        const data = snapshot.val();
        const lista = [];
        if (data) {
            Object.keys(data).forEach(key => {
                const registro = { ...data[key], firebaseUrl: key };
                if (String(registro?.responsavel || '').trim() === nomeNormalizado) {
                    lista.push(registro);
                }
            });
        }
        callback(lista);
    });
}

export function dbEscutarAtendimentosDoUsuario(nomeUsuario, callback) {
    const atendimentosRef = ref(db, 'atendimentos');
    const nomeNormalizado = String(nomeUsuario || '').trim();
    return onValue(atendimentosRef, (snapshot) => {
        const data = snapshot.val();
        const lista = [];
        if (data) {
            Object.keys(data).forEach(key => {
                const registro = { firebaseUrl: key, ...data[key] };
                if (String(registro?.atendente || '').trim() === nomeNormalizado) {
                    lista.push(registro);
                }
            });
        }
        callback(lista);
    });
}

export async function dbListarAtendimentos() {
    try {
        const snapshot = await get(ref(db, 'atendimentos'));
        if (snapshot.exists()) {
            const data = snapshot.val();
            return Object.keys(data).map(key => ({
                ...data[key],
                firebaseUrl: key
            }));
        }
    } catch (error) {
        console.error("ERRO AO LISTAR ATENDIMENTOS:", error);
    }
    return [];
}

async function _reverterContadorClientePorAtendimentoExcluido(atendimento) {
    try {
        if (atendimento?._teste) return;

        const pixValidos = _extrairPixValidosAtendimento(atendimento);
        if (pixValidos.length === 0) return;

        const clienteRef = atendimento?.cliente || {};
        const clienteId = String(clienteRef?.id || clienteRef?.firebaseUrl || "").trim();
        let cliente = clienteId
            ? await dbBuscarClientePorId(clienteId)
            : await dbBuscarClientePorNumero(clienteRef?.numero);

        if (!cliente && clienteRef?.numero != null) {
            cliente = await dbBuscarClientePorNumero(clienteRef.numero);
        }
        if (!cliente?.firebaseUrl) return;

        const equipamentos = _normalizarEquipDetalhesCliente(cliente);
        if (equipamentos.length === 0) return;

        const numerosAlvo = new Set(pixValidos.map(item => item.numero));
        if (!equipamentos.some(item => numerosAlvo.has(_normalizarNumeroPix(item?.pix)))) return;

        // Busca atendimentos restantes (o excluído já foi removido antes desta chamada)
        const ultimosContadores = {};
        try {
            const atendimentosCliente = await _listarAtendimentosDoCliente({
                id: cliente.firebaseUrl,
                numero: cliente.numero
            });

            atendimentosCliente
                .sort((a, b) => _obterTimestampOrdenacaoAtendimento(b) - _obterTimestampOrdenacaoAtendimento(a))
                .forEach(item => {
                    _extrairPixValidosAtendimento(item).forEach(pix => {
                        if (!numerosAlvo.has(pix.numero) || ultimosContadores[pix.numero] != null) return;
                        ultimosContadores[pix.numero] = pix.contAtual;
                    });
                });
        } catch (erroBusca) {
            console.warn("NÃO FOI POSSÍVEL REPROCESSAR CONTADORES DO CLIENTE AO EXCLUIR:", erroBusca);
        }

        // Sem fallback: se nenhum atendimento restante tiver contador para esse PIX, zera o campo
        let houveMudanca = false;
        const equipamentosAtualizados = equipamentos.map(item => {
            const numeroPix = _normalizarNumeroPix(item?.pix);
            if (!numeroPix || !numerosAlvo.has(numeroPix)) return item;

            const contadorAtualizado = ultimosContadores[numeroPix] != null
                ? String(ultimosContadores[numeroPix]).trim()
                : "";

            if (String(item?.contador || "").trim() === contadorAtualizado) return item;

            houveMudanca = true;
            return { ...item, contador: contadorAtualizado };
        });

        if (!houveMudanca) return;

        await update(ref(db, `clientes/${cliente.firebaseUrl}`), {
            equipDetalhes: equipamentosAtualizados,
            equip: _serializarEquipTextoCliente(equipamentosAtualizados)
        });
        await _atualizarVersaoClientes();
    } catch (erro) {
        console.error("ERRO AO REVERTER CONTADOR DO CLIENTE POR ATENDIMENTO EXCLUÍDO:", erro);
    }
}

export async function dbExcluirAtendimento(id) {
    try {
        // Captura dados antes de excluir para poder reverter o contador PIX
        const snap = await get(ref(db, `atendimentos/${id}`));
        const atendimento = snap.exists() ? { ...snap.val(), firebaseUrl: id } : null;

        await remove(ref(db, `atendimentos/${id}`));

        if (atendimento) {
            await _reverterContadorClientePorAtendimentoExcluido(atendimento);
            await _cancelarMovimentacoesHistoricoBalancoPorRefId(
                atendimento.firebaseUrl,
                atendimento.atendente
            );
        }
        await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("ERRO AO EXCLUIR ATENDIMENTO:", error);
        throw error;
    }
}

export async function dbAtualizarAtendimento(id, patch) {
    try {
        if (!id) throw new Error("ID do atendimento é obrigatório.");
        if (!patch || typeof patch !== 'object') throw new Error("Patch inválido.");
        await update(ref(db, `atendimentos/${id}`), patch);
        await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("ERRO AO ATUALIZAR ATENDIMENTO:", error);
        throw error;
    }
}

/* --- FUNÇÕES: MEDIA DE VENDAS (SAÚDE FINANCEIRA) --- */

function _normalizarNumeroCliente(numero) {
    const k = String(numero ?? '').trim();
    return k ? k : null;
}

function _hojeLocalYYYYMMDD() {
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
}

function _inicioDiaLocalFromIso(iso) {
    if (!iso) return null;
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return null;
    d.setHours(0, 0, 0, 0);
    return d;
}

function _inicioDiaLocalFromYMD(ymd) {
    if (!ymd) return null;
    const d = new Date(`${ymd}T00:00:00`);
    if (Number.isNaN(d.getTime())) return null;
    d.setHours(0, 0, 0, 0);
    return d;
}

function _diasDesdeUltimoAtendimento(ultimoIso, ymdAlvo) {
    const dUlt = _inicioDiaLocalFromIso(ultimoIso);
    const dAlvo = _inicioDiaLocalFromYMD(ymdAlvo);
    if (!dUlt || !dAlvo) return null;
    const diff = Math.round((dAlvo - dUlt) / 86400000);
    return Math.max(0, diff);
}

function _calcularEstimativa(vendaPorDia, ultimoIso, ymdAlvo) {
    if (typeof vendaPorDia !== 'number') return null;
    const dias = _diasDesdeUltimoAtendimento(ultimoIso, ymdAlvo);
    if (dias === null) return null;
    return Math.round(((vendaPorDia * dias) + Number.EPSILON) * 100) / 100;
}

async function _buscarClienteBasePorReferencia(clienteRef) {
    const id = String(clienteRef?.id ?? clienteRef?.firebaseUrl ?? '').trim();
    if (id) {
        const snapPorId = await get(ref(db, `clientes/${id}`));
        if (snapPorId.exists()) {
            return { firebaseUrl: id, ...snapPorId.val() };
        }
    }

    const numero = _normalizarNumeroCliente(clienteRef?.numero);
    if (!numero) return null;

    const valoresBusca = [numero];
    const numeroComoNumero = Number(numero);
    if (Number.isFinite(numeroComoNumero) && String(numeroComoNumero) === numero) {
        valoresBusca.unshift(numeroComoNumero);
    }

    for (const valor of valoresBusca) {
        const snapPorNumero = await get(query(ref(db, 'clientes'), orderByChild('numero'), equalTo(valor)));
        if (!snapPorNumero.exists()) continue;
        const data = snapPorNumero.val() || {};
        const primeiroId = Object.keys(data)[0];
        if (primeiroId) {
            return { firebaseUrl: primeiroId, ...data[primeiroId] };
        }
    }

    return null;
}

async function _upsertMediaDeVendasPorCliente(cliente, clienteId = null, numeroAntigo = null) {
    const numeroNovoKey = _normalizarNumeroCliente(cliente?.numero);
    if (!numeroNovoKey) return;

    const agoraIso = new Date().toISOString();
    const hojeYmd = _hojeLocalYYYYMMDD();

    const oldKey = _normalizarNumeroCliente(numeroAntigo);
    if (oldKey && oldKey !== numeroNovoKey) {
        try {
            const oldRef = ref(db, `Media_de_Vendas/${oldKey}`);
            const snapOld = await get(oldRef);
            const oldData = snapOld.exists() ? snapOld.val() : null;
            if (oldData) {
                await runTransaction(ref(db, `Media_de_Vendas/${numeroNovoKey}`), (current) => {
                    const cur = current || {};
                    return {
                        ...oldData,
                        ...cur,
                        cliente: { ...(oldData?.cliente || {}), ...(cur?.cliente || {}) },
                    };
                });
            }
            await remove(oldRef);
        } catch (e) {
            console.warn("Falha ao mover Media_de_Vendas para novo número:", e);
        }
    }

    await runTransaction(ref(db, `Media_de_Vendas/${numeroNovoKey}`), (current) => {
        const cur = current || {};
        const clienteAtual = {
            ...(cur?.cliente || {}),
            id: clienteId ?? cur?.cliente?.id ?? null,
            numero: cliente?.numero ?? cur?.cliente?.numero ?? null,
            nome: cliente?.nome ?? cur?.cliente?.nome ?? "",
            rota: cliente?.rota ?? cur?.cliente?.rota ?? "",
        };

        const next = {
            ...cur,
            cliente: clienteAtual,
            rota: clienteAtual.rota || cur?.rota || "",
            atualizado_em: agoraIso,
            hoje_dia: hojeYmd,
        };

        const vendaDia = (typeof next.venda_por_dia === 'number') ? next.venda_por_dia : null;
        next.estimativa_hoje = _calcularEstimativa(vendaDia, next.ultimo_atendimento_em, hojeYmd);

        return next;
    });
}

async function _upsertMediaDeVendasPorAtendimento(atendimento) {
    const clienteAtendimento = atendimento?.cliente || {};
    const clienteBase = await _buscarClienteBasePorReferencia(clienteAtendimento);
    const c = clienteBase || clienteAtendimento;
    const numeroKey = _normalizarNumeroCliente(c?.numero) || _normalizarNumeroCliente(clienteAtendimento?.numero);
    if (!numeroKey) return;

    const dataAt = atendimento?.data || null;
    const tAt = Date.parse(dataAt || "");
    if (Number.isNaN(tAt)) return;

    const agoraIso = new Date().toISOString();
    const hojeYmd = _hojeLocalYYYYMMDD();

    const ind = atendimento?.indicadores_venda_dia || null;
    const novoVendaDia = (typeof ind?.venda_por_dia === 'number')
        ? ind.venda_por_dia
        : (typeof c?.venda_por_dia === 'number' ? c.venda_por_dia : null);
    const novoMsg = (ind?.msg ?? c?.venda_por_dia_msg ?? null);
    const novoBaseTotal = (typeof ind?.baseTotal === 'number')
        ? ind.baseTotal
        : (Number(c?.venda_por_dia_base_total) || 0);
    const novoDiasEntre = (typeof ind?.diasEntre === 'number')
        ? ind.diasEntre
        : (Number(c?.venda_por_dia_intervalo_dias) || 0);

    const temIndicador = (novoVendaDia !== null) || Boolean(novoMsg);

    await runTransaction(ref(db, `Media_de_Vendas/${numeroKey}`), (current) => {
        const cur = current || {};

        const curUlt = Date.parse(cur?.ultimo_atendimento_em || "");
        const atualizarUltimo = Number.isNaN(curUlt) || tAt >= curUlt;

        const clienteAtual = {
            ...(cur?.cliente || {}),
            id: clienteBase?.firebaseUrl ?? c?.id ?? clienteAtendimento?.id ?? cur?.cliente?.id ?? null,
            numero: c?.numero ?? cur?.cliente?.numero ?? null,
            nome: c?.nome ?? cur?.cliente?.nome ?? "",
            rota: c?.rota ?? cur?.cliente?.rota ?? "",
        };

        const next = {
            ...cur,
            cliente: clienteAtual,
            rota: clienteAtual.rota || cur?.rota || "",
            atualizado_em: agoraIso,
            hoje_dia: hojeYmd,
        };

        if (atualizarUltimo) {
            next.ultimo_atendimento_em = dataAt;

            // Só sobrescreve o indicador se este atendimento tiver indicador (ou se ainda não houver nenhum no registro)
            const jaTemVenda = (typeof next.venda_por_dia === 'number') || Boolean(next.venda_por_dia_msg);
            if (temIndicador || !jaTemVenda) {
                next.venda_por_dia = novoVendaDia;
                next.venda_por_dia_msg = novoMsg || null;
                next.venda_por_dia_base_total = novoBaseTotal;
                next.venda_por_dia_intervalo_dias = novoDiasEntre;
                next.venda_por_dia_atualizado_em = ind?.calculadoEm ?? c?.venda_por_dia_atualizado_em ?? agoraIso;
            }
        }

        const vendaDia = (typeof next.venda_por_dia === 'number') ? next.venda_por_dia : null;
        next.estimativa_hoje = _calcularEstimativa(vendaDia, next.ultimo_atendimento_em, hojeYmd);

        return next;
    });
}

export function dbEscutarMediaDeVendas(callback) {
    const refBase = ref(db, 'Media_de_Vendas');
    onValue(refBase, (snapshot) => {
        const data = snapshot.val();
        if (!data) {
            callback([]);
            return;
        }
        const lista = Object.keys(data).map(key => ({ firebaseUrl: key, ...data[key] }));
        callback(lista);
    });
}

export async function dbListarMediaDeVendas() {
    try {
        const snapshot = await get(ref(db, 'Media_de_Vendas'));
        if (!snapshot.exists()) return [];
        const data = snapshot.val() || {};
        return Object.keys(data).map(key => ({
            firebaseUrl: key,
            ...data[key]
        }));
    } catch (error) {
        console.error("ERRO AO LISTAR MEDIA_DE_VENDAS:", error);
        return [];
    }
}

export async function dbSincronizarMediaDeVendasComClientes() {
    const [snapClientes, snapBase, snapAtendimentos] = await Promise.all([
        get(ref(db, 'clientes')),
        get(ref(db, 'Media_de_Vendas')),
        get(ref(db, 'atendimentos'))
    ]);
    if (!snapClientes.exists()) return 0;

    const data = snapClientes.val();
    const baseAtual = snapBase.exists() ? (snapBase.val() || {}) : {};
    const atendimentos = snapAtendimentos.exists() ? (snapAtendimentos.val() || {}) : {};

    const agoraIso = new Date().toISOString();
    const hojeYmd = _hojeLocalYYYYMMDD();

    const updates = {};
    let total = 0;

    const ultimosAtendimentos = {};
    const ultimosIndicadores = {};

    Object.keys(atendimentos).forEach((id) => {
        const at = atendimentos[id] || {};
        const numeroKey = _normalizarNumeroCliente(at?.cliente?.numero);
        const dataAt = at?.data || null;
        const ts = Date.parse(dataAt || "");
        if (!numeroKey || Number.isNaN(ts)) return;

        const atualUltimo = ultimosAtendimentos[numeroKey];
        if (!atualUltimo || ts > atualUltimo.ts) {
            ultimosAtendimentos[numeroKey] = { ts, at };
        }

        const ind = at?.indicadores_venda_dia || null;
        const temIndicador =
            (typeof ind?.venda_por_dia === 'number') ||
            Boolean(ind?.msg) ||
            (typeof at?.cliente?.venda_por_dia === 'number') ||
            Boolean(at?.cliente?.venda_por_dia_msg);

        if (temIndicador) {
            const atualComIndicador = ultimosIndicadores[numeroKey];
            if (!atualComIndicador || ts > atualComIndicador.ts) {
                ultimosIndicadores[numeroKey] = { ts, at };
            }
        }
    });

    const chavesClientes = new Set();

    Object.keys(data).forEach(id => {
        const c = data[id] || {};
        const key = _normalizarNumeroCliente(c?.numero);
        if (!key) return;
        chavesClientes.add(key);
        total++;

        const reg = baseAtual[key] || {};
        const ultimoAt = ultimosAtendimentos[key]?.at || null;
        const ultimoAtComIndicador = ultimosIndicadores[key]?.at || null;

        const origemIndicador = ultimoAtComIndicador || ultimoAt || null;
        const ind = origemIndicador?.indicadores_venda_dia || null;

        const vendaDia = (typeof ind?.venda_por_dia === 'number')
            ? ind.venda_por_dia
            : (typeof origemIndicador?.cliente?.venda_por_dia === 'number'
                ? origemIndicador.cliente.venda_por_dia
                : (typeof reg?.venda_por_dia === 'number' ? reg.venda_por_dia : null));
        const vendaMsg = (ind?.msg ?? origemIndicador?.cliente?.venda_por_dia_msg ?? reg?.venda_por_dia_msg ?? null);
        const vendaBaseTotal = (typeof ind?.baseTotal === 'number')
            ? ind.baseTotal
            : (Number(origemIndicador?.cliente?.venda_por_dia_base_total) || Number(reg?.venda_por_dia_base_total) || 0);
        const vendaDiasEntre = (typeof ind?.diasEntre === 'number')
            ? ind.diasEntre
            : (Number(origemIndicador?.cliente?.venda_por_dia_intervalo_dias) || Number(reg?.venda_por_dia_intervalo_dias) || 0);
        const ultimo = ultimoAt?.data || reg?.ultimo_atendimento_em || null;

        updates[`Media_de_Vendas/${key}/cliente/id`] = id;
        updates[`Media_de_Vendas/${key}/cliente/numero`] = c.numero ?? null;
        updates[`Media_de_Vendas/${key}/cliente/nome`] = c.nome || "";
        updates[`Media_de_Vendas/${key}/cliente/rota`] = c.rota || "";
        updates[`Media_de_Vendas/${key}/rota`] = c.rota || "";
        updates[`Media_de_Vendas/${key}/ultimo_atendimento_em`] = ultimo;
        updates[`Media_de_Vendas/${key}/venda_por_dia`] = vendaDia;
        updates[`Media_de_Vendas/${key}/venda_por_dia_msg`] = vendaMsg;
        updates[`Media_de_Vendas/${key}/venda_por_dia_base_total`] = vendaBaseTotal;
        updates[`Media_de_Vendas/${key}/venda_por_dia_intervalo_dias`] = vendaDiasEntre;
        updates[`Media_de_Vendas/${key}/venda_por_dia_atualizado_em`] =
            ind?.calculadoEm ??
            origemIndicador?.cliente?.venda_por_dia_atualizado_em ??
            reg?.venda_por_dia_atualizado_em ??
            agoraIso;
        updates[`Media_de_Vendas/${key}/hoje_dia`] = hojeYmd;
        updates[`Media_de_Vendas/${key}/estimativa_hoje`] = _calcularEstimativa(vendaDia, ultimo, hojeYmd);
        updates[`Media_de_Vendas/${key}/atualizado_em`] = agoraIso;
    });

    Object.keys(baseAtual).forEach((key) => {
        if (!chavesClientes.has(String(key))) {
            updates[`Media_de_Vendas/${key}`] = null;
        }
    });

    if (Object.keys(updates).length > 0) {
        await update(ref(db), updates);
        await _atualizarVersaoMediaDeVendas();
    }

    return total;
}

/* --- FUNCOES PARA MANUTENCAO --- */

/**
 * Busca os dados completos de um cliente diretamente pelo ID do Firebase (firebaseUrl).
 * Mais rápido e não exige índice no banco — use este sempre que tiver o ID disponível.
 */
export async function dbBuscarClientePorId(firebaseId) {
    try {
        if (!firebaseId) return null;
        const snapshot = await get(ref(db, `clientes/${firebaseId}`));
        if (!snapshot.exists()) return null;
        return { firebaseUrl: firebaseId, ...snapshot.val() };
    } catch (erro) {
        console.error("ERRO AO BUSCAR CLIENTE POR ID:", erro);
        return null;
    }
}

/**
 * Busca os dados completos de um cliente usando o número único dele.
 * Ex: dbBuscarClientePorNumero(42) retorna o objeto do cliente número 42.
 * Retorna o objeto do cliente com a chave "firebaseUrl" (ID no banco), ou null se não encontrar.
 */
export async function dbBuscarClientePorNumero(numero) {
    try {
        const numeroNormalizado = String(numero || '').trim();
        if (!numeroNormalizado) return null;

        // Tenta buscar tanto como número quanto como string (o banco pode ter salvo de qualquer forma)
        const valoresTentativa = [numeroNormalizado];
        const comoNumero = Number(numeroNormalizado);
        if (Number.isFinite(comoNumero)) valoresTentativa.unshift(comoNumero);

        for (const valor of valoresTentativa) {
            const resultado = await get(query(ref(db, 'clientes'), orderByChild('numero'), equalTo(valor)));
            if (!resultado.exists()) continue;
            const dados = resultado.val() || {};
            const primeiroId = Object.keys(dados)[0];
            if (primeiroId) return { firebaseUrl: primeiroId, ...dados[primeiroId] };
        }
        return null;
    } catch (erro) {
        console.error("ERRO AO BUSCAR CLIENTE POR NÚMERO:", erro);
        return null;
    }
}

function _normalizarEquipDetalhesCliente(cliente) {
    if (Array.isArray(cliente?.equipDetalhes) && cliente.equipDetalhes.length > 0) {
        return cliente.equipDetalhes
            .map((item, index) => ({
                rowId: String(item?.rowId || `equip_${index + 1}`),
                itemId: String(item?.itemId || "").trim(),
                nome: String(item?.nome || "").trim(),
                qtd: String(item?.qtd || item?.quantidade || "1").trim() || "1",
                categoria: String(item?.categoria || "maquina").trim() || "maquina",
                pix: String(item?.pix || "").trim(),
                contador: String(item?.contador || "").trim(),
                pixRetiradoPendente: String(item?.pixRetiradoPendente || "").trim(),
                contadorRetiradoPendente: String(item?.contadorRetiradoPendente || "").trim(),
                retiradaPendenteEm: item?.retiradaPendenteEm || "",
                retiradaPendentePor: item?.retiradaPendentePor || "",
                tecnico: String(item?.tecnico || "").trim(),
                manutencaoPendente: String(item?.manutencaoPendente || "").trim(),
                aguardandoConfirmacao: item?.aguardandoConfirmacao === true,
                manutencaoPendenteEm: item?.manutencaoPendenteEm || "",
                manutencaoPendentePor: item?.manutencaoPendentePor || ""
            }))
            .filter(item => item.nome);
    }

    if (!cliente?.equip) return [];

    const textoEquip = String(cliente.equip || "").trim();
    const partes = textoEquip.includes('|')
        ? textoEquip.split(/\s*\|\s*/)
        : textoEquip.split(/\s*,\s*/);

    return partes
        .map((item, index) => {
            let nome = String(item || "").trim();
            if (!nome) return null;
            let qtd = "1";
            let pix = "";

            const regexPixLegado = /\s*\[Pix:\s*([^\]]+)\]\s*$/i;
            const matchPixLegado = nome.match(regexPixLegado);
            if (matchPixLegado) {
                pix = String(matchPixLegado[1] || "").trim();
                nome = nome.replace(regexPixLegado, "").trim();
            } else {
                const regexPixTexto = /\s+Pix\s+([0-9]+)\s*$/i;
                const matchPixTexto = nome.match(regexPixTexto);
                if (matchPixTexto) {
                    pix = String(matchPixTexto[1] || "").trim();
                    nome = nome.replace(regexPixTexto, "").trim();
                }
            }

            const regexQtd = /\s*\(([^)]+)\)\s*$/;
            const matchQtd = nome.match(regexQtd);
            if (matchQtd) {
                qtd = String(matchQtd[1] || "1").trim() || "1";
                nome = nome.replace(regexQtd, "").trim();
            }

            return {
                rowId: `equip_${index + 1}`,
                itemId: "",
                nome: String(nome || "").trim(),
                qtd: String(qtd || "1").trim() || "1",
                categoria: "maquina",
                pix: String(pix || "").trim(),
                contador: "",
                pixRetiradoPendente: "",
                contadorRetiradoPendente: "",
                retiradaPendenteEm: "",
                retiradaPendentePor: "",
                manutencaoPendente: "",
                aguardandoConfirmacao: false,
                manutencaoPendenteEm: "",
                manutencaoPendentePor: ""
            };
        })
        .filter(item => item && item.nome);
}

function _serializarEquipTextoCliente(equipDetalhes) {
    return (Array.isArray(equipDetalhes) ? equipDetalhes : [])
        .filter(item => String(item?.nome || "").trim())
        .map(item => {
            const nome = String(item?.nome || "").trim();
            const qtd = String(item?.qtd || "1").trim() || "1";
            const pix = String(item?.pix || "").trim();
            const base = `${nome}${qtd !== '1' ? ` (${qtd})` : ''}`;
            return pix ? `${base} [Pix: ${pix}]` : base;
        })
        .join(', ');
}

export async function dbPadronizarEquipamentosClientes() {
    try {
        const clientes = await _obterListaClientesAtualizada();
        let analisados = 0;
        let atualizados = 0;

        for (const cliente of (Array.isArray(clientes) ? clientes : [])) {
            analisados++;
            if (!cliente?.firebaseUrl) continue;

            const equipDetalhesPadrao = _normalizarEquipDetalhesCliente(cliente);
            const equipTextoPadrao = _serializarEquipTextoCliente(equipDetalhesPadrao);
            const equipDetalhesAtuais = Array.isArray(cliente?.equipDetalhes)
                ? _normalizarEquipDetalhesCliente({ equipDetalhes: cliente.equipDetalhes })
                : [];
            const equipTextoAtual = String(cliente?.equip || "").trim();

            const mudouEquipDetalhes = JSON.stringify(equipDetalhesAtuais) !== JSON.stringify(equipDetalhesPadrao);
            const mudouEquipTexto = equipTextoAtual !== equipTextoPadrao;

            if (!mudouEquipDetalhes && !mudouEquipTexto) continue;

            await update(ref(db, `clientes/${cliente.firebaseUrl}`), {
                equipDetalhes: equipDetalhesPadrao,
                equip: equipTextoPadrao
            });
            atualizados++;
        }

        if (atualizados > 0) {
            await _atualizarVersaoClientes();
        }

        return { analisados, atualizados };
    } catch (error) {
        console.error("ERRO AO PADRONIZAR EQUIPAMENTOS DOS CLIENTES:", error);
        throw error;
    }
}

function _normalizarNumeroPix(valor) {
    return String(valor || "").trim();
}

function _obterTimestampRegistroPix(registro) {
    const dataBase = registro?.data_retirada || registro?.data || registro?.criado_em || registro?.atualizado_em || "";
    const t = Date.parse(dataBase);
    return Number.isNaN(t) ? 0 : t;
}

function _formatarIdentificacaoPixCliente(cliente) {
    const numeroBruto = String(cliente?.numero || "").trim();
    const nome = String(cliente?.nome || "").trim();
    if (numeroBruto && nome) {
        const numeroFmt = /^\d+$/.test(numeroBruto) ? numeroBruto.padStart(2, "0") : numeroBruto;
        return `N. ${numeroFmt} - ${nome}`;
    }
    return nome || numeroBruto || "";
}

function _normalizarTextoPixLookup(valor) {
    return String(valor || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim();
}

function _encontrarRegistroPixPorNumero(registros, numeroPix, opcoes = {}) {
    const alvo = _normalizarNumeroPix(numeroPix);
    if (!alvo) return null;

    const equipamento = _normalizarTextoPixLookup(opcoes?.equipamento);
    const clienteNome = _normalizarTextoPixLookup(opcoes?.cliente);
    const identificacao = _normalizarTextoPixLookup(opcoes?.identificacao);

    let candidatos = Object.entries(registros || {})
        .filter(([, registro]) => _normalizarNumeroPix(registro?.numero_pix) === alvo)
        .map(([key, registro]) => ({ key, registro }));

    if (candidatos.length === 0) return null;

    if (equipamento) {
        candidatos = candidatos.filter(({ registro }) =>
            _normalizarTextoPixLookup(registro?.equipamento) === equipamento
        );
    }

    if ((clienteNome || identificacao) && candidatos.length > 0) {
        const filtradosPorCliente = candidatos.filter(({ registro }) => {
            const itemCliente = _normalizarTextoPixLookup(registro?.cliente);
            const itemIdent = _normalizarTextoPixLookup(registro?.identificacao);
            return (clienteNome && itemCliente === clienteNome)
                || (identificacao && itemIdent === identificacao);
        });
        if (filtradosPorCliente.length > 0) candidatos = filtradosPorCliente;
    }

    candidatos.sort((a, b) => {
        const aInstalado = a.registro?.situacao === "instalado" ? 1 : 0;
        const bInstalado = b.registro?.situacao === "instalado" ? 1 : 0;
        if (aInstalado !== bInstalado) return bInstalado - aInstalado;
        return _obterTimestampRegistroPix(b.registro) - _obterTimestampRegistroPix(a.registro);
    });

    return candidatos[0];
}

export async function dbSincronizarCadastrosPixPorManutencao(cliente, payload = {}) {
    try {
        const clienteBase = cliente || {};
        const atendente = String(payload?.atendente || "").trim();
        const observacoes = String(payload?.observacoes || "").trim();
        const dataEvento = payload?.data || new Date().toISOString();
        const identificacao = _formatarIdentificacaoPixCliente(clienteBase);
        const clienteNome = String(clienteBase?.nome || "").trim();

        const equipamentosAdicionados = Array.isArray(payload?.equipamentosAdicionados) ? payload.equipamentosAdicionados : [];
        const equipamentosRetirados = Array.isArray(payload?.equipamentosRetiradosDetalhes) ? payload.equipamentosRetiradosDetalhes : [];
        const alteracoes = [
            ...equipamentosAdicionados.filter(item => _normalizarNumeroPix(item?.pix)).map(item => ({ tipo: "instalado", item })),
            ...equipamentosRetirados.filter(item => _normalizarNumeroPix(item?.pix)).map(item => ({ tipo: "retirado", item }))
        ];

        if (alteracoes.length === 0) return;

        const cadastrosRef = ref(db, "cadastros_pix");
        const snapshot = await get(cadastrosRef);
        const registrosAtuais = snapshot.exists() ? (snapshot.val() || {}) : {};

        for (const alteracao of alteracoes) {
            const numeroPix = _normalizarNumeroPix(alteracao?.item?.pix);
            if (!numeroPix) continue;

            const encontrado = _encontrarRegistroPixPorNumero(registrosAtuais, numeroPix, {
                equipamento: alteracao?.item?.nome,
                cliente: clienteNome,
                identificacao
            });
            const keyExistente = encontrado?.key || null;
            const registroBase = encontrado?.registro || {};
            const agoraIso = new Date().toISOString();

            if (alteracao.tipo === "instalado") {
                const patchInstalacao = {
                    numero_pix: numeroPix,
                    cliente: clienteNome || String(registroBase?.cliente || "").trim(),
                    equipamento: String(alteracao.item?.nome || registroBase?.equipamento || "").trim(),
                    identificacao: identificacao || String(registroBase?.identificacao || "").trim(),
                    tecnico: String(alteracao.item?.tecnico || atendente || registroBase?.tecnico || "").trim(),
                    data: dataEvento,
                    obs: observacoes || String(registroBase?.obs || "").trim(),
                    situacao: "instalado",
                    tecnico_retirada: "",
                    data_retirada: "",
                    motivo_retirada: "",
                    condicao_retirada: "",
                    destino_retirada: "",
                    obs_retirada: "",
                    origem: "manutencao",
                    atualizado_em: agoraIso
                };

                if (keyExistente) {
                    await update(ref(db, `cadastros_pix/${keyExistente}`), patchInstalacao);
                    registrosAtuais[keyExistente] = { ...registroBase, ...patchInstalacao };
                } else {
                    const novoRegistro = { ...patchInstalacao, criado_em: agoraIso };
                    const novoRef = await push(cadastrosRef, novoRegistro);
                    registrosAtuais[novoRef.key] = novoRegistro;
                }
                continue;
            }

            const patchRetirada = {
                numero_pix: numeroPix,
                cliente: clienteNome || String(registroBase?.cliente || "").trim(),
                equipamento: String(alteracao.item?.nome || registroBase?.equipamento || "").trim(),
                identificacao: String(registroBase?.identificacao || identificacao || "").trim(),
                tecnico: String(registroBase?.tecnico || "").trim(),
                data: registroBase?.data || dataEvento,
                situacao: "retirado",
                tecnico_retirada: atendente || String(registroBase?.tecnico_retirada || "").trim(),
                data_retirada: dataEvento,
                motivo_retirada: payload?.pontoEncerrado ? "encerrado" : "manutencao",
                condicao_retirada: payload?.pontoEncerrado ? "analise" : String(registroBase?.condicao_retirada || "").trim(),
                destino_retirada: payload?.pontoEncerrado ? "analise" : String(registroBase?.destino_retirada || "").trim(),
                obs_retirada: observacoes || String(registroBase?.obs_retirada || "").trim(),
                origem: String(registroBase?.origem || "manutencao"),
                atualizado_em: agoraIso,
                retirou_trocou_contador: alteracao.item?.retirou_trocou_contador === true,
                contador_anterior_retirada: alteracao.item?.retirou_trocou_contador === true ? Number(alteracao.item?.contador_anterior_retirada || 0) : "",
                contador_atual_retirada: alteracao.item?.retirou_trocou_contador === true ? Number(alteracao.item?.contador_atual_retirada || 0) : "",
                percentual_comissao_cliente: alteracao.item?.retirou_trocou_contador === true ? Number(alteracao.item?.percentual_comissao_cliente || 0) : "",
                valor_bruto_cliente: alteracao.item?.retirou_trocou_contador === true ? Number(alteracao.item?.valor_bruto_cliente || 0) : "",
                valor_comissao_cliente: alteracao.item?.retirou_trocou_contador === true ? Number(alteracao.item?.valor_comissao_cliente || 0) : "",
                valor_pagar_cliente: alteracao.item?.retirou_trocou_contador === true ? Number(alteracao.item?.valor_pagar_cliente || 0) : "",
                foto_contador_retirada: alteracao.item?.retirou_trocou_contador === true ? String(alteracao.item?.foto_contador_retirada || "").trim() : "",
                foto_contador_retirada_thumb: alteracao.item?.retirou_trocou_contador === true ? String(alteracao.item?.foto_contador_retirada_thumb || "").trim() : ""
            };

            if (keyExistente) {
                await update(ref(db, `cadastros_pix/${keyExistente}`), patchRetirada);
                registrosAtuais[keyExistente] = { ...registroBase, ...patchRetirada };
            } else {
                const novoRegistro = { ...patchRetirada, criado_em: agoraIso };
                const novoRef = await push(cadastrosRef, novoRegistro);
                registrosAtuais[novoRef.key] = novoRegistro;
            }
        }
    } catch (erro) {
        console.error("ERRO AO SINCRONIZAR CADASTROS PIX POR MANUTENCAO:", erro);
        throw erro;
    }
}

export async function dbAplicarPendenciasManutencaoCliente(firebaseUrlCliente, payload = {}) {
    try {
        if (!firebaseUrlCliente) return null;

        const clienteRef = ref(db, `clientes/${firebaseUrlCliente}`);
        const snapshot = await get(clienteRef);
        if (!snapshot.exists()) return null;

        const clienteAtual = snapshot.val() || {};
        const agoraIso = new Date().toISOString();
        const atendente = String(payload?.atendente || "").trim();
        const equipamentosBase = _normalizarEquipDetalhesCliente(clienteAtual);
        const equipamentosAtualizados = equipamentosBase.map(item => ({ ...item }));

        const adicionados = Array.isArray(payload?.equipamentosAdicionados) ? payload.equipamentosAdicionados : [];
        adicionados.forEach((item, index) => {
            const nome = String(item?.nome || "").trim();
            if (!nome) return;
            equipamentosAtualizados.push({
                rowId: String(item?.rowId || `pend_add_${Date.now()}_${index + 1}`),
                itemId: String(item?.itemId || "").trim(),
                nome,
                qtd: String(item?.qtd || "1").trim() || "1",
                categoria: String(item?.categoria || "maquina").trim() || "maquina",
                pix: String(item?.pix || "").trim(),
                contador: String(item?.contador || "").trim(),
                tecnico: String(item?.tecnico || "").trim(),
                manutencaoPendente: 'adicao',
                aguardandoConfirmacao: true,
                manutencaoPendenteEm: agoraIso,
                manutencaoPendentePor: atendente
            });
        });

        const retirados = Array.isArray(payload?.equipamentosRetiradosDetalhes) ? payload.equipamentosRetiradosDetalhes : [];
        retirados.forEach((item) => {
            const nome = String(item?.nome || "").trim();
            if (!nome) return;

            const alvo = equipamentosAtualizados.find(equip => {
                if (String(equip?.nome || "").trim() !== nome) return false;
                const pixAlvo = String(item?.pix || "").trim();
                if (pixAlvo && String(equip?.pix || "").trim() !== pixAlvo) return false;
                return true;
            });

            if (!alvo) return;
            const pixAtual = String(alvo?.pix || "").trim();
            if (pixAtual && !String(alvo?.pixRetiradoPendente || "").trim()) {
                alvo.pixRetiradoPendente = pixAtual;
                alvo.contadorRetiradoPendente = String(alvo?.contador || "").trim();
                alvo.retiradaPendenteEm = agoraIso;
                alvo.retiradaPendentePor = atendente;
                alvo.pix = "";
                alvo.contador = "";
            }
            alvo.manutencaoPendente = 'retirada';
            alvo.aguardandoConfirmacao = true;
            alvo.manutencaoPendenteEm = agoraIso;
            alvo.manutencaoPendentePor = atendente;
        });

        const patch = {
            equipDetalhes: equipamentosAtualizados,
            equip: _serializarEquipTextoCliente(equipamentosAtualizados),
            aguardandoRevisao: payload?.pontoEncerrado ? true : (clienteAtual?.aguardandoRevisao === true),
            encerrado: payload?.pontoEncerrado ? true : (clienteAtual?.encerrado === true),
            valorEncerramento: payload?.pontoEncerrado ? String(payload?.valorEncerramento || '') : (clienteAtual?.valorEncerramento || ''),
            dataEncerramento: payload?.pontoEncerrado ? agoraIso : (clienteAtual?.dataEncerramento || '')
        };

        await update(clienteRef, patch);
        await _atualizarVersaoClientes();
        return { firebaseUrl: firebaseUrlCliente, ...clienteAtual, ...patch };
    } catch (erro) {
        console.error("ERRO AO APLICAR PENDENCIAS DE MANUTENCAO NO CLIENTE:", erro);
        throw erro;
    }
}

/**
 * Reverte o encerramento de um ponto: limpa flags do cliente e restaura PIX dos equipamentos.
 */
export async function dbReverterEncerramentoCliente(firebaseUrlCliente, equipamentosRetiradosDetalhes = []) {
    try {
        if (!firebaseUrlCliente) return;
        const clienteRef = ref(db, `clientes/${firebaseUrlCliente}`);
        const snapshot = await get(clienteRef);
        if (!snapshot.exists()) return;

        const clienteAtual = snapshot.val() || {};
        const equipamentos = _normalizarEquipDetalhesCliente(clienteAtual).map(item => ({ ...item }));

        // Restaura PIX e contador de cada equipamento retirado pendente
        equipamentosRetiradosDetalhes.forEach(retirado => {
            const alvo = equipamentos.find(e =>
                String(e?.nome || '').trim() === String(retirado?.nome || '').trim() &&
                (String(retirado?.pix || '').trim() === '' ||
                 String(e?.pixRetiradoPendente || '').trim() === String(retirado?.pix || '').trim())
            );
            if (!alvo) return;
            if (alvo.pixRetiradoPendente) alvo.pix = alvo.pixRetiradoPendente;
            if (alvo.contadorRetiradoPendente) alvo.contador = alvo.contadorRetiradoPendente;
            delete alvo.pixRetiradoPendente;
            delete alvo.contadorRetiradoPendente;
            delete alvo.retiradaPendenteEm;
            delete alvo.retiradaPendentePor;
            delete alvo.manutencaoPendente;
            delete alvo.aguardandoConfirmacao;
            delete alvo.manutencaoPendenteEm;
            delete alvo.manutencaoPendentePor;
        });

        await update(clienteRef, {
            encerrado: false,
            aguardandoRevisao: false,
            valorEncerramento: '',
            dataEncerramento: '',
            equipDetalhes: equipamentos,
            equip: _serializarEquipTextoCliente(equipamentos)
        });
        await _atualizarVersaoClientes();
    } catch (erro) {
        console.error("ERRO AO REVERTER ENCERRAMENTO DO CLIENTE:", erro);
        throw erro;
    }
}

/**
 * Marca um cliente como encerrado no banco.
 * Usado quando o técnico retira todos os equipamentos e confirma que o ponto foi encerrado.
 * O card do cliente vai aparecer em vermelho na lista, aguardando revisão do gestor.
 */
export async function dbMarcarClienteEncerrado(firebaseUrlCliente, valorCobrado) {
    try {
        await update(ref(db, `clientes/${firebaseUrlCliente}`), {
            encerrado: true,
            aguardandoRevisao: true,
            valorEncerramento: valorCobrado || '',
            dataEncerramento: new Date().toISOString()
        });
        await _atualizarVersaoClientes();
    } catch (erro) {
        console.error("ERRO AO MARCAR CLIENTE ENCERRADO:", erro);
        throw erro;
    }
}



export async function dbSalvarManutencao(manutencao, idExistente = null) {
    try {
        if (idExistente) {
            await set(ref(db, `manutencoes/${idExistente}`), manutencao);
        } else {
            const manutencoesRef = ref(db, 'manutencoes');
            await push(manutencoesRef, manutencao);
        }
    } catch (error) {
        console.error("ERRO AO SALVAR MANUTENCAO:", error);
        throw error;
    }
}

export async function dbReverterConclusaoManutencao(manutencaoId) {
    try {
        if (!manutencaoId) return;
        await update(ref(db, `manutencoes/${manutencaoId}`), {
            status: 'pendente',
            dataConclusao: null
        });
    } catch (erro) {
        console.error("ERRO AO REVERTER CONCLUSAO DA MANUTENCAO:", erro);
        throw erro;
    }
}

export async function dbConfirmarGestorManutencao(id, nomeGestor) {
    try {
        if (!id) return;

        // Busca o registro de manutenção para obter cliente e equipamentos
        const manutSnap = await get(ref(db, `manutencoes/${id}`));
        const manut = manutSnap.exists() ? manutSnap.val() : null;

        const clienteId = String(manut?.clienteId || manut?.clienteFirebaseUrl || '').trim();
        const adicionados = Array.isArray(manut?.equipamentosAdicionados) ? manut.equipamentosAdicionados : [];
        const retirados = Array.isArray(manut?.equipamentosRetiradosDetalhes) ? manut.equipamentosRetiradosDetalhes : [];

        if (clienteId && (adicionados.length > 0 || retirados.length > 0)) {
            const clienteSnap = await get(ref(db, `clientes/${clienteId}`));
            if (clienteSnap.exists()) {
                let equipamentos = _normalizarEquipDetalhesCliente(clienteSnap.val() || {});

                // Máquinas ADICIONADAS confirmadas: limpa as flags de pendência
                if (adicionados.length > 0) {
                    const nomesAdd = new Set(adicionados.map(i => String(i?.nome || '').trim()).filter(Boolean));
                    equipamentos = equipamentos.map(equip => {
                        if (equip.manutencaoPendente === 'adicao' && nomesAdd.has(equip.nome)) {
                            return {
                                ...equip,
                                manutencaoPendente: '',
                                aguardandoConfirmacao: false,
                                manutencaoPendenteEm: '',
                                manutencaoPendentePor: ''
                            };
                        }
                        return equip;
                    });
                }

                // Máquinas RETIRADAS confirmadas: remove da lista do cliente
                if (retirados.length > 0) {
                    const rowIdsRet = new Set(retirados.map(i => String(i?.rowId || '')).filter(Boolean));
                    const nomesRet  = new Set(retirados.map(i => String(i?.nome  || '').trim()).filter(Boolean));
                    equipamentos = equipamentos.filter(equip => {
                        const porRowId = equip.rowId && rowIdsRet.has(equip.rowId);
                        const porNome  = !porRowId && nomesRet.has(equip.nome) && equip.manutencaoPendente === 'retirada';
                        return !(porRowId || porNome);
                    });
                }

                await update(ref(db, `clientes/${clienteId}`), {
                    equipDetalhes: equipamentos,
                    equip: _serializarEquipTextoCliente(equipamentos)
                });
                await _atualizarVersaoClientes();
            }
        }

        await update(ref(db, `manutencoes/${id}`), {
            confirmadoGestor: true,
            confirmedBy: nomeGestor || '',
            dataConfirmacaoGestor: new Date().toISOString()
        });

        if (manut.pontoEncerrado === true && clienteId) {
            await dbExcluirCliente(clienteId);
        }
    } catch (erro) {
        console.error("ERRO AO CONFIRMAR SERVICO:", erro);
        throw erro;
    }
}

function _normalizarChaveUsuario(nomeUsuario) {
    return String(nomeUsuario || '').trim().replace(/[.#$/[\]]/g, '_');
}

function _normalizarListaFluxoCaixa(data) {
    if (!data) return [];
    return Object.keys(data).map(key => ({ firebaseKey: key, ...data[key] }));
}

function _obterInicioJanelaFluxoCaixa(mesesRecentes = 3) {
    const totalMeses = Math.max(1, Number(mesesRecentes || 3));
    const agora = new Date();
    return new Date(agora.getFullYear(), agora.getMonth() - (totalMeses - 1), 1).getTime();
}

export function dbEscutarFluxoCaixa(nomeUsuario, callback) {
    const chave = _normalizarChaveUsuario(nomeUsuario);
    if (!chave) {
        callback([]);
        return () => {};
    }

    return onValue(ref(db, `fluxo_caixa/${chave}/movimentacoes`), (snapshot) => {
        callback(_normalizarListaFluxoCaixa(snapshot.val()));
    });
}

export async function dbListarFluxoCaixa(nomeUsuario, { mesesRecentes = 3 } = {}) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        if (!chave) return [];
        let snapshot;

        try {
            const inicioJanela = _obterInicioJanelaFluxoCaixa(mesesRecentes);
            const movimentosRef = query(
                ref(db, `fluxo_caixa/${chave}/movimentacoes`),
                orderByChild('criado_em'),
                startAt(inicioJanela)
            );
            snapshot = await get(movimentosRef);
        } catch (erroConsulta) {
            console.warn("Consulta filtrada do fluxo de caixa falhou. Usando leitura completa.", erroConsulta);
            snapshot = await get(ref(db, `fluxo_caixa/${chave}/movimentacoes`));
        }

        if (!snapshot.exists()) return [];
        return _normalizarListaFluxoCaixa(snapshot.val());
    } catch (error) {
        console.error("ERRO AO LISTAR MOVIMENTOS DO FLUXO DE CAIXA:", error);
        throw error;
    }
}

export async function dbSalvarMovimentoFluxoCaixa(nomeUsuario, movimento) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        if (!chave) throw new Error('USUARIO_INVALIDO_FLUXO_CAIXA');
        const novoRef = await push(ref(db, `fluxo_caixa/${chave}/movimentacoes`), movimento);
        await _tentarRecalcularRemuneracoes();
        return { firebaseKey: novoRef.key, ...movimento };
    } catch (error) {
        console.error("ERRO AO SALVAR MOVIMENTO DO FLUXO DE CAIXA:", error);
        throw error;
    }
}

export async function dbExcluirMovimentoFluxoCaixa(nomeUsuario, firebaseKey) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        if (!chave) throw new Error('USUARIO_INVALIDO_FLUXO_CAIXA');
        await remove(ref(db, `fluxo_caixa/${chave}/movimentacoes/${firebaseKey}`));
        await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("ERRO AO EXCLUIR MOVIMENTO DO FLUXO DE CAIXA:", error);
        throw error;
    }
}

export async function dbExcluirTodosMovimentosFluxoCaixa(nomeUsuario) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        if (!chave) throw new Error('USUARIO_INVALIDO_FLUXO_CAIXA');
        await remove(ref(db, `fluxo_caixa/${chave}/movimentacoes`));
        await _tentarRecalcularRemuneracoes();
    } catch (error) {
        console.error("ERRO AO EXCLUIR TODOS OS MOVIMENTOS DO FLUXO DE CAIXA:", error);
        throw error;
    }
}

function _normalizarNomeRemuneracao(nome) {
    return String(nome || '').trim().toLowerCase();
}

function _parseNumeroRemuneracao(valor) {
    const num = Number(valor || 0);
    return Number.isFinite(num) ? num : 0;
}

function _parsePercentualRemuneracao(valor) {
    if (typeof valor === 'number') return Number.isFinite(valor) ? valor : 0;
    const texto = String(valor || '').trim().replace('%', '').replace(',', '.');
    const num = Number(texto);
    return Number.isFinite(num) ? num : 0;
}

function _obterMesAnoRemuneracao(valor) {
    if (!valor && valor !== 0) return '';
    if (typeof valor === 'number') {
        const data = new Date(valor);
        if (Number.isNaN(data.getTime())) return '';
        return `${data.getFullYear()}-${String(data.getMonth() + 1).padStart(2, '0')}`;
    }
    const texto = String(valor).trim();
    if (/^\d{4}-\d{2}/.test(texto)) return texto.slice(0, 7);
    const data = new Date(texto);
    if (Number.isNaN(data.getTime())) return '';
    return `${data.getFullYear()}-${String(data.getMonth() + 1).padStart(2, '0')}`;
}

function _mesAnoAtualRemuneracao() {
    const data = new Date();
    return `${data.getFullYear()}-${String(data.getMonth() + 1).padStart(2, '0')}`;
}

function _obterSaldoParcialRemuneracao(financeiro) {
    const fin = financeiro || {};
    return _parseNumeroRemuneracao(fin.saldoFinal ?? (_parseNumeroRemuneracao(fin.totalGeral) - _parseNumeroRemuneracao(fin.comissaoValor)));
}

function _obterComissaoRemuneracaoParaMes(colab, mesAno) {
    const historico = Array.isArray(colab?.comissaoHistorico) ? [...colab.comissaoHistorico] : [];
    if (historico.length === 0) return _parsePercentualRemuneracao(colab?.comissao);
    const sorted = historico.sort((a, b) => String(a?.inicio || '').localeCompare(String(b?.inicio || '')));
    let taxa = _parsePercentualRemuneracao(sorted[0]?.valor);
    for (const item of sorted) {
        if (String(item?.inicio || '') <= mesAno) taxa = _parsePercentualRemuneracao(item?.valor);
        else break;
    }
    return taxa;
}

function _obterResponsaveisItemRemuneracao(item) {
    const lista = [
        ...(Array.isArray(item?.responsaveis) ? item.responsaveis : []),
        item?.tecnico
    ];
    const mapa = new Map();
    lista.forEach(nome => {
        const limpo = String(nome || '').trim();
        const chave = _normalizarNomeRemuneracao(limpo);
        if (!limpo || mapa.has(chave)) return;
        mapa.set(chave, limpo);
    });
    return Array.from(mapa.values());
}

function _criarMapaClientesRepresentante(clientes) {
    const porId = {};
    const porNumero = {};
    (clientes || []).forEach(cliente => {
        const representante = String(cliente?.representante || '').trim();
        if (!representante) return;
        if (cliente?.firebaseUrl) porId[String(cliente.firebaseUrl).trim()] = representante;
        const numero = _normalizarNumeroCliente(cliente?.numero);
        if (numero) porNumero[String(numero)] = representante;
    });
    return { porId, porNumero };
}

function _obterRepresentanteAtendimentoRemuneracao(atendimento, mapaClientes) {
    const direto = String(
        atendimento?.representante
        || atendimento?.cliente?.representante
        || ''
    ).trim();
    if (direto) return direto;

    const clienteId = String(
        atendimento?.cliente?.id
        || atendimento?.cliente?.firebaseUrl
        || atendimento?.clienteId
        || atendimento?.clienteFirebaseUrl
        || ''
    ).trim();
    if (clienteId && mapaClientes?.porId?.[clienteId]) return String(mapaClientes.porId[clienteId]).trim();

    const numero = _normalizarNumeroCliente(
        atendimento?.cliente?.numero
        || atendimento?.clienteNumero
    );
    if (numero && mapaClientes?.porNumero?.[numero]) return String(mapaClientes.porNumero[numero]).trim();

    return '';
}

function _registrarPerfilRemuneracao(perfis, id, dados) {
    if (!perfis || !id || !dados) return;
    perfis[id] = dados;
}

function _somarPerfisRemuneracao(perfis) {
    return Object.values(perfis || {}).reduce((soma, item) => soma + _parseNumeroRemuneracao(item?.valor), 0);
}

async function _tentarRecalcularRemuneracoes() {
    try {
        await dbRecalcularRemuneracoes();
    } catch (erro) {
        console.warn("NÃO FOI POSSÍVEL RECALCULAR REMUNERAÇÕES:", erro);
    }
}

export async function dbRecalcularRemuneracoes() {
    try {
        const [
            colaboradoresSnap,
            clientesSnap,
            atendimentosSnap,
            historicoSnap,
            historicoBalancoSnap,
            estoqueSnap,
            fluxoSnap
        ] = await Promise.all([
            get(ref(db, 'colaboradores')),
            get(ref(db, 'clientes')),
            get(ref(db, 'atendimentos')),
            get(ref(db, 'historico_estoque')),
            get(ref(db, 'movimentacao_balanco_historico')),
            get(ref(db, 'estoque')),
            get(ref(db, 'fluxo_caixa'))
        ]);

        const colaboradoresData = colaboradoresSnap.exists() ? (colaboradoresSnap.val() || {}) : {};
        const clientesData = clientesSnap.exists() ? (clientesSnap.val() || {}) : {};
        const atendimentosData = atendimentosSnap.exists() ? (atendimentosSnap.val() || {}) : {};
        const historicoData = historicoSnap.exists() ? (historicoSnap.val() || {}) : {};
        const historicoBalancoData = historicoBalancoSnap.exists() ? (historicoBalancoSnap.val() || {}) : {};
        const estoqueData = estoqueSnap.exists() ? (estoqueSnap.val() || {}) : {};
        const fluxoCaixa = fluxoSnap.exists() ? (fluxoSnap.val() || {}) : {};

        const colaboradores = Object.keys(colaboradoresData).map(key => ({ firebaseUrl: key, ...colaboradoresData[key] }));
        const clientes = Object.keys(clientesData).map(key => ({ firebaseUrl: key, ...clientesData[key] }));
        const atendimentos = Object.keys(atendimentosData).map(key => ({ firebaseUrl: key, ...atendimentosData[key] }));
        const historico = Object.keys(historicoData).map(key => ({ firebaseUrl: key, ...historicoData[key] }));
        const historicoBalanco = Object.entries(historicoBalancoData).flatMap(([usuarioKey, movimentos]) =>
            Object.entries(movimentos || {}).map(([firebaseUrl, mov]) => ({ usuarioKey, firebaseUrl, ...mov }))
        );
        const estoque = Object.keys(estoqueData).map(key => ({ firebaseUrl: key, ...estoqueData[key] }));

        const mapaClientesRepresentante = _criarMapaClientesRepresentante(clientes);
        const mapaValoresEstoque = {};
        const mapaValoresProdutos = {};
        const mapaValoresProdutosPorChave = {};
        const mapaValoresPecas = {};

        estoque.forEach(item => {
            const nome = String(item?.nome || '').trim();
            if (!nome) return;
            const valor = _parseNumeroRemuneracao(item?.valorEquipamento);
            mapaValoresEstoque[_normalizarNomeRemuneracao(nome)] = valor;
            if (item?.categoria === 'produtos') {
                mapaValoresProdutos[nome] = valor;
                const chaveProduto = String(item?.firebaseUrl || '').trim();
                if (chaveProduto) mapaValoresProdutosPorChave[chaveProduto] = valor;
            }
            if (item?.categoria === 'peca') mapaValoresPecas[nome] = valor;
        });

        const meses = new Set([_mesAnoAtualRemuneracao()]);
        atendimentos.forEach(item => {
            const mesAno = _obterMesAnoRemuneracao(item?.data);
            if (mesAno) meses.add(mesAno);
        });
        historico.forEach(item => {
            const mesAno = _obterMesAnoRemuneracao(item?.data || item?.criado_em);
            if (mesAno) meses.add(mesAno);
        });

        const mesesOrdenados = [...meses].sort();
        const remuneracaoMensal = {};
        const atualizadoEm = Date.now();

        for (const mesAno of mesesOrdenados) {
            const financeirosMes = atendimentos.filter(item =>
                _obterMesAnoRemuneracao(item?.data) === mesAno &&
                item?.origemRegistro !== 'retirada_estoque' &&
                item?.origemRegistro !== 'entrada_estoque' &&
                _parseNumeroRemuneracao(item?.financeiro?.totalGeral) > 0
            );
            const manutencoesMes = atendimentos.filter(item =>
                _obterMesAnoRemuneracao(item?.data) === mesAno &&
                Array.isArray(item?.manutencao?.equipamentosAdicionados) &&
                item.manutencao.equipamentosAdicionados.length > 0
            );
            const historicoMes = historico.filter(item =>
                _obterMesAnoRemuneracao(item?.data || item?.criado_em) === mesAno
            );

            const baseGlobal = financeirosMes.reduce((soma, item) => soma + _obterSaldoParcialRemuneracao(item?.financeiro), 0);
            const registrosMes = {};

            for (const colab of colaboradores) {
                const nome = String(colab?.nome || '').trim();
                if (!nome) continue;

                const nomeNorm = _normalizarNomeRemuneracao(nome);
                const usuarioKey = _normalizarChaveUsuario(nome);
                const perfis = {};

                const taxaComissao = _obterComissaoRemuneracaoParaMes(colab, mesAno);
                if (taxaComissao > 0) {
                    const baseIndividual = financeirosMes
                        .filter(item => _normalizarNomeRemuneracao(item?.atendente) === nomeNorm)
                        .reduce((soma, item) => soma + _obterSaldoParcialRemuneracao(item?.financeiro), 0);
                    const valor = Math.round(baseIndividual * taxaComissao / 100);
                    if (valor > 0) {
                        _registrarPerfilRemuneracao(perfis, 'comissao_producao', {
                            valor,
                            percentual: taxaComissao,
                            base: Math.round(baseIndividual),
                            atualizadoEm
                        });
                    }
                }

                const taxaGlobal = _parsePercentualRemuneracao(colab?.comissaoGlobal);
                if (taxaGlobal > 0 && baseGlobal > 0) {
                    _registrarPerfilRemuneracao(perfis, 'comissao_global', {
                        valor: Math.round(baseGlobal * taxaGlobal / 100),
                        percentual: taxaGlobal,
                        base: Math.round(baseGlobal),
                        atualizadoEm
                    });
                }

                const taxaRepresentante = _parsePercentualRemuneracao(colab?.representanteComissao);
                if (taxaRepresentante > 0) {
                    const baseRepresentante = financeirosMes
                        .filter(item => _normalizarNomeRemuneracao(_obterRepresentanteAtendimentoRemuneracao(item, mapaClientesRepresentante)) === nomeNorm)
                        .reduce((soma, item) => soma + _obterSaldoParcialRemuneracao(item?.financeiro), 0);
                    const valor = Math.round(baseRepresentante * taxaRepresentante / 100);
                    if (valor > 0) {
                        _registrarPerfilRemuneracao(perfis, 'representante', {
                            valor,
                            percentual: taxaRepresentante,
                            base: Math.round(baseRepresentante),
                            atualizadoEm
                        });
                    }
                }

                const fixo = _parseNumeroRemuneracao(colab?.fixo);
                if (fixo > 0) {
                    _registrarPerfilRemuneracao(perfis, 'fixo', {
                        valor: Math.round(fixo),
                        atualizadoEm
                    });
                }

                const bonusFixo = _parseNumeroRemuneracao(colab?.bonusFixo);
                if (bonusFixo > 0) {
                    _registrarPerfilRemuneracao(perfis, 'bonus_fixo', {
                        valor: 0,
                        configurado: Math.round(bonusFixo),
                        status: 'meta_pendente',
                        atualizadoEm
                    });
                }

                const bonusProducao = _parseNumeroRemuneracao(colab?.bonusManutencao);
                if (bonusProducao > 0) {
                    _registrarPerfilRemuneracao(perfis, 'bonus_producao', {
                        valor: 0,
                        configurado: Math.round(bonusProducao),
                        status: 'meta_pendente',
                        atualizadoEm
                    });
                }

                if (colab?.producaoEquipamentos === true) {
                    let totalEquip = 0;
                    manutencoesMes.forEach(item => {
                        const equipamentos = Array.isArray(item?.manutencao?.equipamentosAdicionados)
                            ? item.manutencao.equipamentosAdicionados
                            : [];
                        equipamentos.forEach(equip => {
                            const responsaveis = _obterResponsaveisItemRemuneracao(equip);
                            if (!responsaveis.some(resp => _normalizarNomeRemuneracao(resp) === nomeNorm)) return;
                            const chave = _normalizarNomeRemuneracao(equip?.nome || '');
                            const valorUnit = _parseNumeroRemuneracao(mapaValoresEstoque[chave]);
                            const qtd = parseInt(equip?.qtd, 10) || 0;
                            totalEquip += (qtd * valorUnit) / Math.max(1, responsaveis.length);
                        });
                    });
                    totalEquip = Math.round(totalEquip);
                    if (totalEquip > 0) {
                        _registrarPerfilRemuneracao(perfis, 'producao_equipamentos', {
                            valor: totalEquip,
                            atualizadoEm
                        });
                    }
                }

                if (colab?.producaoPecas === true) {
                    let totalPecas = 0;
                    historicoMes.forEach(item => {
                        if (item?.tipo !== 'entrada') return;
                        if (String(item?.itemCategoria || '').trim() !== 'peca') return;
                        if (_normalizarNomeRemuneracao(item?.responsavel) !== nomeNorm) return;
                        const nomePeca = String(item?.itemNome || '').trim();
                        const valorUnit = _parseNumeroRemuneracao(item?.valorUnitario) || _parseNumeroRemuneracao(mapaValoresPecas[nomePeca]);
                        totalPecas += valorUnit * _parseNumeroRemuneracao(item?.qtd);
                    });
                    totalPecas = Math.round(totalPecas);
                    if (totalPecas > 0) {
                        _registrarPerfilRemuneracao(perfis, 'producao_pecas', {
                            valor: totalPecas,
                            atualizadoEm
                        });
                    }
                }

                if (colab?.prestacaoServico === true) {
                    _registrarPerfilRemuneracao(perfis, 'prestacao_servico', {
                        valor: 0,
                        status: 'fonte_pendente',
                        atualizadoEm
                    });
                }

                const total = Math.round(_somarPerfisRemuneracao(perfis));
                if (total > 0 || Object.keys(perfis).length > 0) {
                    registrosMes[usuarioKey] = {
                        nome,
                        total,
                        perfis,
                        atualizadoEm
                    };
                }
            }

            remuneracaoMensal[mesAno] = registrosMes;
        }

        const acumuladoProdutos = {};
        colaboradores
            .filter(colab => colab?.producaoProdutos === true)
            .forEach(colab => {
                const nome = String(colab?.nome || '').trim();
                if (!nome) return;

                const usuarioKey = _normalizarChaveUsuario(nome);
                const nomeNorm = _normalizarNomeRemuneracao(nome);
                let valorProducao = 0;

                historicoBalanco.forEach(item => {
                    if (item?.cancelado) return;
                    if (String(item?.tipo || '').trim() !== 'entrada_estoque') return;
                    if (String(item?.origemRegistro || '').trim() !== 'entrada_estoque') return;
                    if (String(item?.categoria || '').trim() !== 'produtos') return;
                    if (_normalizarNomeRemuneracao(item?.responsavel) !== nomeNorm) return;
                    const nomeProduto = String(item?.itemNome || '').trim();
                    const itemChave = String(item?.itemChave || item?.refId || '').trim();
                    const valorUnit = _parseNumeroRemuneracao(item?.valorUnitario)
                        || _parseNumeroRemuneracao(mapaValoresProdutosPorChave[itemChave])
                        || _parseNumeroRemuneracao(mapaValoresProdutos[nomeProduto]);
                    valorProducao += valorUnit * Math.abs(_parseNumeroRemuneracao(item?.movimento));
                });

                const fluxoUsuario = fluxoCaixa?.[usuarioKey]?.movimentacoes || {};
                const valorFluxo = Object.values(fluxoUsuario).reduce((soma, mov) => {
                    const valor = _parseNumeroRemuneracao(mov?.valor);
                    return soma + (mov?.tipo === 'entrada' ? valor : -valor);
                }, 0);

                acumuladoProdutos[usuarioKey] = {
                    nome,
                    valor: Math.round(valorProducao + valorFluxo),
                    atualizadoEm
                };
            });

        await Promise.all([
            set(ref(db, 'remuneracao/mensal'), remuneracaoMensal),
            set(ref(db, 'remuneracao/meses'), mesesOrdenados),
            set(ref(db, 'remuneracao/acumulado/producao_produtos'), acumuladoProdutos),
            set(ref(db, 'remuneracao/atualizadoEm'), atualizadoEm)
        ]);

        return {
            mensal: remuneracaoMensal,
            meses: mesesOrdenados,
            acumulado: acumuladoProdutos
        };
    } catch (error) {
        console.error("ERRO AO RECALCULAR REMUNERAÇÕES:", error);
        throw error;
    }
}

export async function dbListarMesesRemuneracao() {
    try {
        const snapshot = await get(ref(db, 'remuneracao/meses'));
        return snapshot.exists() ? (snapshot.val() || []) : [];
    } catch (error) {
        console.error("ERRO AO LISTAR MESES DE REMUNERAÇÃO:", error);
        return [];
    }
}

export function dbEscutarMesesRemuneracao(callback) {
    return onValue(ref(db, 'remuneracao/meses'), (snapshot) => {
        callback(snapshot.exists() ? (snapshot.val() || []) : []);
    });
}

export async function dbListarRemuneracaoMensal(mesAno) {
    try {
        if (!mesAno) return [];
        const snapshot = await get(ref(db, `remuneracao/mensal/${mesAno}`));
        const data = snapshot.exists() ? (snapshot.val() || {}) : {};
        return Object.keys(data).map(key => ({ usuarioKey: key, ...data[key] }));
    } catch (error) {
        console.error("ERRO AO LISTAR REMUNERAÇÃO MENSAL:", error);
        return [];
    }
}

export function dbEscutarRemuneracaoMensal(mesAno, callback) {
    if (!mesAno) {
        callback([]);
        return () => {};
    }
    return onValue(ref(db, `remuneracao/mensal/${mesAno}`), (snapshot) => {
        const data = snapshot.exists() ? (snapshot.val() || {}) : {};
        callback(Object.keys(data).map(key => ({ usuarioKey: key, ...data[key] })));
    });
}

export async function dbListarRemuneracaoAcumuladaProdutos() {
    try {
        const snapshot = await get(ref(db, 'remuneracao/acumulado/producao_produtos'));
        const data = snapshot.exists() ? (snapshot.val() || {}) : {};
        return Object.keys(data).map(key => ({ usuarioKey: key, ...data[key] }));
    } catch (error) {
        console.error("ERRO AO LISTAR REMUNERAÇÃO ACUMULADA DE PRODUTOS:", error);
        return [];
    }
}

export function dbEscutarRemuneracaoAcumuladaProdutos(callback) {
    return onValue(ref(db, 'remuneracao/acumulado/producao_produtos'), (snapshot) => {
        const data = snapshot.exists() ? (snapshot.val() || {}) : {};
        callback(Object.keys(data).map(key => ({ usuarioKey: key, ...data[key] })));
    });
}

function _normalizarChavePix(numeroPix) {
    return String(numeroPix || '').trim().replace(/[.#$/[\]]/g, '_');
}

export async function dbAdicionarPixEmPosse(nomeUsuario, numeroPix, dadosExtras = {}) {
    try {
        const chaveUsuario = _normalizarChaveUsuario(nomeUsuario);
        const pixNumero = String(numeroPix || '').trim();
        const chavePix = _normalizarChavePix(pixNumero);
        if (!chaveUsuario || !pixNumero || !chavePix) return;

        await set(ref(db, `pix_em_posse/${chaveUsuario}/${chavePix}`), {
            numero_pix: pixNumero,
            atualizadoEm: new Date().toISOString(),
            ...dadosExtras
        });
    } catch (error) {
        console.error("ERRO AO ADICIONAR PIX EM POSSE:", error);
        throw error;
    }
}

export async function dbRemoverPixEmPosse(nomeUsuario, numeroPix) {
    try {
        const chaveUsuario = _normalizarChaveUsuario(nomeUsuario);
        const chavePix = _normalizarChavePix(numeroPix);
        if (!chaveUsuario || !chavePix) return;

        await remove(ref(db, `pix_em_posse/${chaveUsuario}/${chavePix}`));
    } catch (error) {
        console.error("ERRO AO REMOVER PIX EM POSSE:", error);
        throw error;
    }
}

export async function dbLerPixEmPosse(nomeUsuario) {
    try {
        const chaveUsuario = _normalizarChaveUsuario(nomeUsuario);
        if (!chaveUsuario) return [];

        const snap = await get(ref(db, `pix_em_posse/${chaveUsuario}`));
        if (!snap.exists()) return [];

        const data = snap.val() || {};
        return Object.keys(data).map(key => ({ firebaseUrl: key, ...data[key] }));
    } catch (error) {
        console.error("ERRO AO LER PIX EM POSSE:", error);
        return [];
    }
}

export function dbEscutarPixEmPosse(nomeUsuario, callback) {
    const chaveUsuario = _normalizarChaveUsuario(nomeUsuario);
    if (!chaveUsuario) {
        callback([]);
        return () => {};
    }

    return onValue(ref(db, `pix_em_posse/${chaveUsuario}`), (snapshot) => {
        const data = snapshot.val() || {};
        const lista = Object.keys(data).map(key => ({ firebaseUrl: key, ...data[key] }));
        callback(lista);
    });
}

export function dbEscutarTodosPixEmPosse(callback) {
    return onValue(ref(db, 'pix_em_posse'), (snapshot) => {
        const data = snapshot.val() || {};
        const numeros = new Set();
        Object.values(data).forEach(usuario => {
            if (usuario && typeof usuario === 'object') {
                Object.values(usuario).forEach(registro => {
                    const pix = String(registro?.numero_pix || '').trim();
                    if (pix) numeros.add(pix);
                });
            }
        });
        callback(numeros);
    });
}

export function dbEscutarCadastrosPix(callback) {
    return onValue(ref(db, 'cadastros_pix'), (snapshot) => {
        const data = snapshot.val() || {};
        const lista = Object.keys(data).map(key => ({ firebaseUrl: key, ...data[key] }));
        callback(lista);
    });
}

/* =============================================================================
   FUNÇÕES PARA POSSE ACUMULADA (PRODUTOS E MÁQUINAS)
   =============================================================================
   CONTEXTO / POR QUE EXISTE:
   Quando um atendimento é EXCLUÍDO do Firebase (ex: atendimentos antigos removidos
   para poupar espaço ou corrigir erro), os produtos e máquinas que faziam parte
   daquele atendimento seriam perdidos para sempre do balanço.

   Para resolver isso, existe este "acumulador": antes de excluir qualquer atendimento,
   o sistema chama `dbAcumularPosse` para somar os produtos e máquinas daquele
   atendimento a um contador permanente no banco, no nó `posse_acumulada/{usuario}`.

   Assim, o balanço em `balanco.html` pode mostrar:
     - Produtos em posse = soma dos atendimentos ATIVOS + acumulado dos excluídos
     - Máquinas em posse = idem

   ESTRUTURA NO FIREBASE:
   posse_acumulada/
     {nomeUsuario}/
       produtos/
         "Nome do Produto": <quantidade total>
       maquinas/
         "Nome da Máquina": <quantidade total>

   ONDE É USADO:
   - `balanco.html` lê via `dbLerPosseAcumulada` na inicialização
   - `[tela de exclusão de atendimento]` deve chamar `dbAcumularPosse` ANTES de excluir
   ============================================================================= */

export async function dbLerPosseAcumulada(nomeUsuario) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        const snap = await get(ref(db, `posse_acumulada/${chave}`));
        return snap.val() || { produtos: {}, maquinas: {} };
    } catch (error) {
        console.error("ERRO AO LER POSSE ACUMULADA:", error);
        return { produtos: {}, maquinas: {} };
    }
}

export function dbEscutarPosseAcumulada(nomeUsuario, callback) {
    const chave = _normalizarChaveUsuario(nomeUsuario);
    if (!chave) {
        callback({ produtos: {}, maquinas: {} });
        return () => {};
    }

    return onValue(ref(db, `posse_acumulada/${chave}`), (snapshot) => {
        callback(snapshot.val() || { produtos: {}, maquinas: {} });
    });
}

export async function dbAcumularPosse(nomeUsuario, atendimento) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        const posseRef = ref(db, `posse_acumulada/${chave}`);
        const snap = await get(posseRef);
        const atual = snap.val() || { produtos: {}, maquinas: {} };

        const deltaProdutos = atendimento?.origemRegistro === 'retirada_estoque' ? 1 : -1;

        (atendimento.produtos || []).forEach(p => {
            if (!p.nome) return;
            const proximoTotal = (atual.produtos[p.nome] || 0) + (Number(p.quantidade || 0) * deltaProdutos);
            if (proximoTotal === 0) delete atual.produtos[p.nome];
            else atual.produtos[p.nome] = proximoTotal;
        });

        if (atendimento?.origemRegistro === 'manutencao') {
            (atendimento?.manutencao?.equipamentosRetiradosDetalhes || []).forEach(item => {
                if (!item?.nome) return;
                const proximoTotal = (atual.maquinas[item.nome] || 0) + Number(item?.qtd || 1);
                if (proximoTotal === 0) delete atual.maquinas[item.nome];
                else atual.maquinas[item.nome] = proximoTotal;
            });
            (atendimento?.manutencao?.equipamentosAdicionados || []).forEach(item => {
                if (!item?.nome) return;
                const proximoTotal = (atual.maquinas[item.nome] || 0) - Number(item?.qtd || 1);
                if (proximoTotal === 0) delete atual.maquinas[item.nome];
                else atual.maquinas[item.nome] = proximoTotal;
            });
        } else {
            (atendimento.fotos?.maquinas || []).forEach(m => {
                if (!m.nome) return;
                const proximoTotal = (atual.maquinas[m.nome] || 0) - 1;
                if (proximoTotal === 0) delete atual.maquinas[m.nome];
                else atual.maquinas[m.nome] = proximoTotal;
            });
        }

        await set(posseRef, atual);
    } catch (error) {
        console.error("ERRO AO ACUMULAR POSSE:", error);
        throw error;
    }
}

// =============================================================================
// MOVIMENTAÇÃO BALANÇO HISTÓRICO — nó único para todos os movimentos de produtos/máquinas/peças
// Path: movimentacao_balanco_historico/{chaveUsuario}/{autoId}

function _normalizarNomeHistoricoBalancoItem(itemNome) {
    return String(itemNome || '').trim().toLowerCase();
}

function _obterMovimentacaoHistoricoBalanco(entrada) {
    const valor = Number(entrada?.movimento ?? 0);
    return Number.isFinite(valor) ? valor : 0;
}

function _normalizarChavePosseItem(itemChave, itemNome) {
    const base = String(itemChave || _normalizarNomeHistoricoBalancoItem(itemNome) || '').trim();
    return base.replace(/[.#$/[\]]/g, '_');
}

async function _listarHistoricoBalancoDoUsuario(chaveUsuario) {
    if (!chaveUsuario) return [];
    const snap = await get(ref(db, `movimentacao_balanco_historico/${chaveUsuario}`));
    if (!snap.exists()) return [];
    return Object.entries(snap.val() || {}).map(([id, valor]) => ({ id, ...valor }));
}

function _obterTotalAtualHistoricoBalancoDaLista(lista, itemNome, itemChave = '') {
    const chaveAlvo = String(itemChave || '').trim();
    if (!chaveAlvo) return 0;

    return (Array.isArray(lista) ? lista : []).reduce((soma, item) => {
        const chaveItem = String(item?.itemChave || '').trim();
        if (chaveItem !== chaveAlvo) return soma;
        return soma + _obterMovimentacaoHistoricoBalanco(item);
    }, 0);
}

async function _obterTotalAtualHistoricoBalanco(chaveUsuario, itemNome, itemChave = '') {
    try {
        const chaveItem = String(itemChave || '').trim();
        if (!chaveUsuario || !chaveItem) return 0;

        const lista = await _listarHistoricoBalancoDoUsuario(chaveUsuario);
        return _obterTotalAtualHistoricoBalancoDaLista(lista, '', chaveItem);
    } catch (e) {
        console.warn('ERRO AO LER TOTAL ATUAL DA MOVIMENTAÇÃO BALANÇO HISTÓRICO:', e);
        return 0;
    }
}

async function _resolverChavePosseItemUsuario(chaveUsuario, itemChavePreferida, entrada, movimento = 0) {
    const chavePreferida = String(itemChavePreferida || '').trim();
    return chavePreferida;
}

async function _atualizarPosseItensUsuario(entrada, movimento) {
    const controlarPosse = entrada?.controlarPosse !== false;
    const itemChaveBase = _normalizarChavePosseItem(
        entrada?.itemChave || entrada?.itemId || (String(entrada?.tipo || '').includes('_estoque') ? entrada?.refId : ''),
        entrada?.itemNome
    );

    if (!controlarPosse) {
        return { itemChave: itemChaveBase, totalAntes: null, totalApos: null };
    }

    const chaveUsuario = _normalizarChaveUsuario(entrada?.responsavel);
    const itemChave = await _resolverChavePosseItemUsuario(chaveUsuario, itemChaveBase, entrada, movimento);
    if (!chaveUsuario || !itemChave) {
        throw new Error('Não foi possível identificar a posse do item.');
    }

    const posseRef = ref(db, `posse_itens_usuario/${chaveUsuario}/${itemChave}`);
    const posseAtualSnap = await get(posseRef);
    const quantidadeInicialFallback = posseAtualSnap.exists()
        ? null
        : await _obterTotalAtualHistoricoBalanco(chaveUsuario, entrada?.itemNome, itemChave);
    const atualizadoEm = new Date().toISOString();
    const resultado = await runTransaction(posseRef, (atual) => {
        const estadoAtual = atual && typeof atual === 'object' ? atual : {};
        const categoriaAtual = String(entrada?.categoria || estadoAtual?.categoria || 'produto').trim();
        const origemMovimento = String(entrada?.origemRegistro || entrada?.tipo || '').trim();
        const ignorarValidacaoPosse = entrada?.ignorarValidacaoPosse === true;
        const permitirSaldoNegativo = origemMovimento === 'atendimento'
            || origemMovimento === 'manutencao'
            || origemMovimento === 'manutencao_adicao'
            || origemMovimento === 'entrada_estoque';
        const quantidadeAtualBase = estadoAtual?.quantidade;
        const quantidadeAtual = Number(
            quantidadeAtualBase != null
                ? quantidadeAtualBase
                : (quantidadeInicialFallback != null ? quantidadeInicialFallback : 0)
        );
        const quantidadeNova = quantidadeAtual + Number(movimento || 0);

        if (quantidadeNova < 0 && !permitirSaldoNegativo && !ignorarValidacaoPosse) return;

        return {
            itemNome: String(entrada?.itemNome || estadoAtual?.itemNome || '').trim(),
            categoria: categoriaAtual,
            quantidade: quantidadeNova,
            atualizadoEm,
            itemChave,
            itemId: String(
                entrada?.itemId
                || (String(entrada?.tipo || '').includes('_estoque') ? entrada?.refId : '')
                || estadoAtual?.itemId
                || ''
            ).trim()
        };
    });

    if (!resultado?.committed || !resultado?.snapshot?.exists()) {
        throw new Error(`Posse insuficiente para ${String(entrada?.responsavel || 'o usuário').trim()} em ${String(entrada?.itemNome || 'o item').trim()}.`);
    }

    const totalApos = Number(resultado.snapshot.val()?.quantidade || 0);
    return {
        itemChave,
        totalAntes: totalApos - Number(movimento || 0),
        totalApos
    };
}

async function _salvarEntradaHistoricoBalanco(chaveUsuario, entrada, totais = {}) {
    const movimento = _obterMovimentacaoHistoricoBalanco(entrada);
    return push(ref(db, `movimentacao_balanco_historico/${chaveUsuario}`), {
        timestamp: new Date().toISOString(),
        tipo: String(entrada.tipo || ''),
        origemRegistro: String(entrada.origemRegistro || entrada.tipo || '').trim(),
        categoria: String(entrada.categoria || 'produto'),
        itemNome: String(entrada.itemNome).trim(),
        itemNomeNormalizado: _normalizarNomeHistoricoBalancoItem(entrada.itemNome),
        itemChave: String(totais?.itemChave || entrada?.itemChave || '').trim(),
        movimento,
        totalAntes: totais?.totalAntes ?? null,
        totalApos: totais?.totalApos ?? null,
        responsavel: String(entrada.responsavel).trim(),
        registradoPor: String(entrada.registradoPor || entrada.responsavel).trim(),
        descricao: String(entrada.descricao || ''),
        refId: String(entrada.refId || ''),
        ...(entrada?.atendimentoRefId ? { atendimentoRefId: String(entrada.atendimentoRefId).trim() } : {}),
        controlarPosse: entrada?.controlarPosse !== false,
        ...(entrada?.ignorarValidacaoPosse === true ? { ignorarValidacaoPosse: true } : {}),
        isDefeitoEntry: Boolean(entrada.isDefeitoEntry),
        qtdDefeitoConsumida: Number(entrada.qtdDefeitoConsumida || 0),
        ...(entrada?.estoqueAntes != null ? { estoqueAntes: Number(entrada.estoqueAntes) } : {}),
        ...(entrada?.estoqueDepois != null ? { estoqueDepois: Number(entrada.estoqueDepois) } : {})
    });
}

export async function dbSalvarHistoricoBalanco(entrada, opcoes = {}) {
    // entrada: { responsavel, registradoPor?, itemNome, categoria, tipo, origemRegistro?, movimento, descricao?, refId?, itemChave?, controlarPosse?, isDefeitoEntry?, qtdDefeitoConsumida? }
    try {
        const recalcular = opcoes?.recalcular !== false;
        const chaveU = _normalizarChaveUsuario(entrada.responsavel);
        const movimento = _obterMovimentacaoHistoricoBalanco(entrada);
        if (!chaveU || !entrada.itemNome || !movimento) return;
        const totais = await _atualizarPosseItensUsuario(entrada, movimento);
        const novoRef = await _salvarEntradaHistoricoBalanco(chaveU, {
            ...entrada,
            movimento,
            itemChave: totais?.itemChave || entrada?.itemChave || ''
        }, totais);
        if (recalcular) await _tentarRecalcularRemuneracoes();
        return novoRef?.key || null;
    } catch (e) {
        console.error('ERRO AO SALVAR MOVIMENTAÇÃO BALANÇO HISTÓRICO:', e);
        throw e;
    }
}

async function _cancelarMovimentacoesHistoricoBalancoPorRefId(refId, responsavel) {
    const chaveU = _normalizarChaveUsuario(responsavel);
    const refIdLimpo = String(refId || '').trim();
    if (!chaveU || !refIdLimpo) return;

    // Busca tudo e filtra no cliente para não depender de índice no Firebase
    const snap = await get(ref(db, `movimentacao_balanco_historico/${chaveU}`));

    if (!snap.exists()) return;

    const entradas = Object.entries(snap.val() || {})
        .map(([id, valor]) => ({ id, ...valor }))
        .filter(item => {
            if (item?.cancelado) return false;
            const refHistorico = String(item?.refId || '').trim();
            const refAtendimento = String(item?.atendimentoRefId || '').trim();
            return refHistorico === refIdLimpo || refAtendimento === refIdLimpo;
        });

    for (const entrada of entradas) {
        await dbExcluirHistoricoBalanco(entrada.id, responsavel);
    }
}

export async function dbSincronizarProdutosAtendimentoNoHistorico(atendimentoId, atendimento) {
    try {
        const idRef = String(atendimentoId || '').trim();
        const responsavel = String(atendimento?.atendente || '').trim();
        const nomeCliente = String(atendimento?.cliente?.nome || '').trim();
        const descricaoAtendimento = nomeCliente ? `Atendimento - ${nomeCliente}` : 'Atendimento';
        if (!idRef || !responsavel) return;

        await _cancelarMovimentacoesHistoricoBalancoPorRefId(idRef, responsavel);

        if (atendimento?.origemRegistro && atendimento.origemRegistro !== 'atendimento') return;

        const produtos = Array.isArray(atendimento?.produtos) ? atendimento.produtos : [];
        const itensValidos = produtos
            .map(item => ({
                itemId: String(item?.itemId || item?.itemChave || item?.refId || '').trim(),
                categoria: String(item?.categoria || 'produtos').trim() || 'produtos',
                nome: String(item?.nome || '').trim(),
                quantidade: Number(item?.quantidade || 0)
            }))
            .filter(item => item.nome && item.quantidade > 0);

        const itemSemId = itensValidos.find(item => !item.itemId);
        if (itemSemId) {
            throw new Error(`Item sem ID técnico no atendimento: ${itemSemId.nome}. Limpe a base antiga e use apenas itens do estoque atual.`);
        }

        if (itensValidos.length === 0) return;

        for (const produto of itensValidos) {
            await dbSalvarHistoricoBalanco({
                responsavel,
                registradoPor: atendimento?.atendente || responsavel,
                itemNome: produto.nome,
                categoria: produto.categoria,
                tipo: 'atendimento',
                origemRegistro: 'atendimento',
                itemChave: produto.itemId,
                movimento: -Number(produto.quantidade || 0),
                descricao: descricaoAtendimento,
                refId: produto.itemId,
                atendimentoRefId: idRef,
                controlarPosse: true,
                isDefeitoEntry: false,
                qtdDefeitoConsumida: 0
            });
        }
    } catch (e) {
        console.error('ERRO AO SINCRONIZAR PRODUTOS DO ATENDIMENTO NO HISTORICO:', e);
        throw e;
    }
}

export async function dbSincronizarItensManutencaoNoHistorico(atendimentoId, atendimento) {
    try {
        const idRef = String(atendimentoId || '').trim();
        const responsavel = String(atendimento?.atendente || '').trim();
        const nomeCliente = String(atendimento?.cliente?.nome || '').trim();
        const descricaoManutencao = nomeCliente ? `Serviço realizado - ${nomeCliente}` : 'Serviço realizado';
        if (!idRef || !responsavel) return;

        await _cancelarMovimentacoesHistoricoBalancoPorRefId(idRef, responsavel);

        if (String(atendimento?.origemRegistro || '').trim() !== 'manutencao') return;

        const produtos = Array.isArray(atendimento?.produtos) ? atendimento.produtos : [];
        const equipamentosAdicionados = Array.isArray(atendimento?.manutencao?.equipamentosAdicionados)
            ? atendimento.manutencao.equipamentosAdicionados
            : [];
        const equipamentosRetirados = Array.isArray(atendimento?.manutencao?.equipamentosRetiradosDetalhes)
            ? atendimento.manutencao.equipamentosRetiradosDetalhes
            : [];

        const produtosValidos = produtos
            .map(item => ({
                itemId: String(item?.itemId || item?.itemChave || item?.refId || '').trim(),
                categoria: String(item?.categoria || 'produtos').trim() || 'produtos',
                nome: String(item?.nome || '').trim(),
                quantidade: Number(item?.quantidade || 0)
            }))
            .filter(item => item.nome && item.quantidade > 0);

        const produtoSemId = produtosValidos.find(item => !item.itemId);
        if (produtoSemId) {
            throw new Error(`Produto sem ID técnico na manutenção: ${produtoSemId.nome}. Limpe a base antiga e use apenas itens do estoque atual.`);
        }

        for (const produto of produtosValidos) {
            await dbSalvarHistoricoBalanco({
                responsavel,
                registradoPor: atendimento?.atendente || responsavel,
                itemNome: produto.nome,
                categoria: produto.categoria,
                tipo: 'manutencao',
                origemRegistro: 'manutencao',
                itemChave: produto.itemId,
                movimento: -Number(produto.quantidade || 0),
                descricao: descricaoManutencao,
                refId: produto.itemId,
                atendimentoRefId: idRef,
                controlarPosse: true,
                isDefeitoEntry: false,
                qtdDefeitoConsumida: 0
            });
        }

        const adicionadosValidos = equipamentosAdicionados
            .map(item => ({
                itemId: String(item?.itemId || item?.itemChave || item?.refId || '').trim(),
                categoria: String(item?.categoria || 'maquina').trim() || 'maquina',
                nome: String(item?.nome || '').trim(),
                quantidade: Number(item?.qtd || item?.quantidade || 0)
            }))
            .filter(item => item.nome && item.quantidade > 0);

        const maquinaAdicionadaSemId = adicionadosValidos.find(item => !item.itemId);
        if (maquinaAdicionadaSemId) {
            throw new Error(`Máquina sem ID técnico na manutenção: ${maquinaAdicionadaSemId.nome}. Limpe a base antiga e use apenas itens do estoque atual.`);
        }

        for (const maquina of adicionadosValidos) {
            await dbSalvarHistoricoBalanco({
                responsavel,
                registradoPor: atendimento?.atendente || responsavel,
                itemNome: maquina.nome,
                categoria: maquina.categoria,
                tipo: 'manutencao_adicao',
                origemRegistro: 'manutencao',
                itemChave: maquina.itemId,
                movimento: -Number(maquina.quantidade || 0),
                descricao: descricaoManutencao,
                refId: maquina.itemId,
                atendimentoRefId: idRef,
                controlarPosse: true,
                isDefeitoEntry: false,
                qtdDefeitoConsumida: 0
            });
        }

        const retiradosValidos = equipamentosRetirados
            .map(item => ({
                itemId: String(item?.itemId || item?.itemChave || '').trim(),
                categoria: String(item?.categoria || 'maquina').trim() || 'maquina',
                nome: String(item?.nome || '').trim(),
                quantidade: Number(item?.qtd || item?.quantidade || 0)
            }))
            .filter(item => item.nome && item.quantidade > 0);

        const maquinaRetiradaSemId = retiradosValidos.find(item => !item.itemId);
        if (maquinaRetiradaSemId) {
            throw new Error(`Máquina retirada sem ID técnico na manutenção: ${maquinaRetiradaSemId.nome}. Limpe a base antiga e use apenas itens do estoque atual.`);
        }

        for (const maquina of retiradosValidos) {
            await dbSalvarHistoricoBalanco({
                responsavel,
                registradoPor: atendimento?.atendente || responsavel,
                itemNome: maquina.nome,
                categoria: maquina.categoria,
                tipo: 'manutencao_retirada',
                origemRegistro: 'manutencao',
                itemChave: maquina.itemId,
                movimento: Number(maquina.quantidade || 0),
                descricao: descricaoManutencao,
                refId: maquina.itemId,
                atendimentoRefId: idRef,
                controlarPosse: true,
                isDefeitoEntry: false,
                qtdDefeitoConsumida: 0
            });
        }
    } catch (e) {
        console.error('ERRO AO SINCRONIZAR ITENS DA MANUTENCAO NO HISTORICO:', e);
        throw e;
    }
}

export async function dbExcluirHistoricoBalanco(id, responsavel, opcoes = {}) {
    try {
        const recalcular = opcoes?.recalcular !== false;
        const chaveU = _normalizarChaveUsuario(responsavel);
        const entradaRef = ref(db, `movimentacao_balanco_historico/${chaveU}/${id}`);
        const snap = await get(entradaRef);
        if (!snap.exists()) return;
        const entrada = snap.val();
        if (entrada?.cancelado) return;
        const deveReverterPosse = entrada?.controlarPosse !== false;

        // Mantém o evento original como trilha e adiciona o reverso para corrigir o saldo atual.
        await dbSalvarHistoricoBalanco({
            responsavel:   entrada.responsavel,
            registradoPor: entrada.registradoPor,
            itemNome:      entrada.itemNome,
            categoria:     entrada.categoria,
            tipo:          'cancelamento',
            origemRegistro: entrada?.origemRegistro || entrada?.tipo || '',
            itemChave:     entrada.itemChave || '',
            movimento:     -Number(_obterMovimentacaoHistoricoBalanco(entrada) || 0),
            descricao:     `Cancelamento: ${entrada.descricao || entrada.tipo}`,
            refId:         id,
            controlarPosse: deveReverterPosse,
            ignorarValidacaoPosse: true
        }, { recalcular: false });

        await update(entradaRef, {
            cancelado: true,
            canceladoEm: new Date().toISOString()
        });
        if (recalcular) await _tentarRecalcularRemuneracoes();
    } catch (e) {
        console.error('ERRO AO EXCLUIR MOVIMENTAÇÃO BALANÇO HISTÓRICO:', e);
        throw e;
    }
}

export function dbEscutarPosseItensUsuario(responsavel, callback) {
    const chaveU = _normalizarChaveUsuario(responsavel);
    if (!chaveU) { callback([]); return () => {}; }
    return onValue(ref(db, `posse_itens_usuario/${chaveU}`), (snap) => {
        if (!snap.exists()) { callback([]); return; }
        const lista = Object.entries(snap.val() || {})
            .map(([chave, v]) => ({
                chave,
                itemNome:    String(v?.itemNome || '').trim(),
                categoria:   String(v?.categoria || 'produto').trim(),
                quantidade:  Number(v?.quantidade || 0),
                atualizadoEm: String(v?.atualizadoEm || '')
            }))
            .filter(item => item.itemNome && item.quantidade !== 0);
        callback(lista);
    });
}

export function dbEscutarHistoricoBalancoDoUsuario(responsavel, callback) {
    const chaveU = _normalizarChaveUsuario(responsavel);
    if (!chaveU) { callback([]); return () => {}; }
    return onValue(ref(db, `movimentacao_balanco_historico/${chaveU}`), (snap) => {
        if (!snap.exists()) { callback([]); return; }
        callback(
            Object.entries(snap.val())
                .map(([id, v]) => ({ id, firebaseUrl: id, ...v }))
                .sort((a, b) => b.timestamp.localeCompare(a.timestamp))
        );
    });
}

export function dbEscutarHistoricoBalanco(callback) {
    return onValue(ref(db, 'movimentacao_balanco_historico'), (snap) => {
        if (!snap.exists()) { callback([]); return; }

        const lista = [];
        const data = snap.val() || {};

        Object.entries(data).forEach(([chaveUsuario, registros]) => {
            Object.entries(registros || {}).forEach(([id, valor]) => {
                lista.push({ firebaseUrl: id, chaveUsuario, ...valor });
            });
        });

        callback(
            lista
                .sort((a, b) => String(b.timestamp || '').localeCompare(String(a.timestamp || '')))
                .slice(0, 60)
        );
    });
}

export async function dbSalvarContestacaoBalanco(dados) {
    try {
        const chaveU = _normalizarChaveUsuario(dados?.responsavel);
        if (!chaveU) throw new Error('Usuário inválido para contestação do balanço.');

        const itemChave = String(dados?.itemChave || '').trim();
        const itemNome = String(dados?.itemNome || '').trim();
        const quantidadeAtual = Number(dados?.quantidadeAtual);
        const quantidadeInformada = Number(dados?.quantidadeInformada);

        if (!itemChave || !itemNome) throw new Error('Item inválido para contestação do balanço.');
        if (!Number.isFinite(quantidadeAtual) || !Number.isFinite(quantidadeInformada)) {
            throw new Error('Quantidade inválida para contestação do balanço.');
        }

        const timestamp = new Date().toISOString();
        const status = String(dados?.status || '').trim();
        const registro = {
            timestamp,
            responsavel: String(dados.responsavel).trim(),
            registradoPor: String(dados?.registradoPor || dados.responsavel).trim(),
            categoria: String(dados?.categoria || 'produto').trim(),
            itemNome,
            itemNomeNormalizado: _normalizarNomeHistoricoBalancoItem(itemNome),
            itemChave,
            quantidadeAtual,
            quantidadeInformada,
            diferenca: quantidadeInformada - quantidadeAtual,
            origemRegistro: 'balanco',
            tipo: 'contestacao_balanco',
            ...(status ? { status } : {}),
            ...(status === 'aprovado' ? {
                aprovadoEm: timestamp,
                aprovadoPor: String(dados?.aprovadoPor || dados?.registradoPor || dados.responsavel).trim()
            } : {})
        };

        const novoRef = await push(ref(db, `contestacao_balanco/${chaveU}`), registro);
        return { id: novoRef?.key || null, ...registro };
    } catch (e) {
        console.error('ERRO AO SALVAR CONTESTAÇÃO DO BALANÇO:', e);
        throw e;
    }
}

export function dbEscutarContestacoesBalanco(responsavel, callback) {
    const chaveU = _normalizarChaveUsuario(responsavel);
    if (!chaveU) { callback([]); return () => {}; }

    return onValue(ref(db, `contestacao_balanco/${chaveU}`), (snap) => {
        if (!snap.exists()) { callback([]); return; }

        const lista = Object.entries(snap.val() || {})
            .map(([id, valor]) => ({ id, firebaseUrl: id, ...valor }))
            .sort((a, b) => String(b.timestamp || '').localeCompare(String(a.timestamp || '')));

        callback(lista);
    });
}

export async function dbAprovarContestacaoBalanco(responsavel, contestacaoId, aprovadoPor) {
    try {
        const chaveU = _normalizarChaveUsuario(responsavel);
        const id = String(contestacaoId || '').trim();
        if (!chaveU || !id) throw new Error('Contestação inválida para aprovação.');

        await update(ref(db, `contestacao_balanco/${chaveU}/${id}`), {
            status: 'aprovado',
            aprovadoEm: new Date().toISOString(),
            aprovadoPor: String(aprovadoPor || responsavel).trim()
        });
    } catch (e) {
        console.error('ERRO AO APROVAR CONTESTAÇÃO DO BALANÇO:', e);
        throw e;
    }
}

export async function dbCancelarContestacaoBalanco(responsavel, contestacaoId) {
    try {
        const chaveU = _normalizarChaveUsuario(responsavel);
        const id = String(contestacaoId || '').trim();
        if (!chaveU || !id) throw new Error('Contestação inválida para cancelamento.');

        await remove(ref(db, `contestacao_balanco/${chaveU}/${id}`));
    } catch (e) {
        console.error('ERRO AO CANCELAR CONTESTAÇÃO DO BALANÇO:', e);
        throw e;
    }
}

export async function dbBuscarUltimosPorItem(responsavel, itemNome, limite = 10) {
    try {
        const chaveU = _normalizarChaveUsuario(responsavel);
        const nomeItem = String(itemNome || '').trim().toLowerCase();
        const limiteFinal = Math.max(1, Math.min(Number(limite) || 10, 100));
        if (!chaveU || !nomeItem) return [];

        const snap = await get(ref(db, `movimentacao_balanco_historico/${chaveU}`));
        if (!snap.exists()) return [];
        return Object.entries(snap.val())
            .map(([id, v]) => ({ id, ...v }))
            .filter(v => String(v.itemNome || '').trim().toLowerCase() === nomeItem)
            .sort((a, b) => String(b.timestamp || '').localeCompare(String(a.timestamp || '')))
            .slice(0, limiteFinal);
    } catch (e) {
        console.warn('Erro ao buscar últimos por item:', e);
        return [];
    }
}

export function dbEscutarUltimosPorItem(responsavel, itemNome, limite = 20, callback) {
    const chaveU = _normalizarChaveUsuario(responsavel);
    const nomeItem = String(itemNome || '').trim().toLowerCase();
    const limiteFinal = Math.max(1, Math.min(Number(limite) || 20, 100));
    if (!chaveU || !nomeItem) {
        callback([]);
        return () => {};
    }

    return onValue(ref(db, `movimentacao_balanco_historico/${chaveU}`), (snap) => {
        if (!snap.exists()) {
            callback([]);
            return;
        }

        const lista = Object.entries(snap.val() || {})
            .map(([id, v]) => ({ id, ...v }))
            .filter(v => String(v.itemNome || '').trim().toLowerCase() === nomeItem)
            .sort((a, b) => String(b.timestamp || '').localeCompare(String(a.timestamp || '')))
            .slice(0, limiteFinal);

        callback(lista);
    });
}

export async function dbBuscarProdutosDoAtendimento(atendente, atendimentoRefId) {
    try {
        const chaveU = _normalizarChaveUsuario(atendente);
        const refId = String(atendimentoRefId || '').trim();
        if (!chaveU || !refId) return [];
        const snap = await get(ref(db, `movimentacao_balanco_historico/${chaveU}`));
        if (!snap.exists()) return [];
        return Object.entries(snap.val())
            .map(([id, v]) => ({ id, ...v }))
            .filter(v =>
                (String(v.atendimentoRefId || '').trim() === refId || String(v.refId || '').trim() === refId) &&
                String(v.origemRegistro || '').trim() === 'atendimento' &&
                !v.cancelado
            )
            .map(v => ({
                nome: v.itemNome || '—',
                movimento: Math.abs(Number(v.movimento || 0)),
                totalApos: v.totalApos != null ? Number(v.totalApos) : null
            }));
    } catch (e) {
        console.warn('Erro ao buscar produtos do atendimento:', e);
        return [];
    }
}

/* --- FUNÇÕES PARA DEPÓSITOS --- */

export async function dbSalvarDeposito(deposito) {
    try {
        const chave = _normalizarChaveUsuario(deposito.usuario);
        await push(ref(db, `depositos/${chave}`), deposito);
    } catch (error) {
        console.error("ERRO AO SALVAR DEPOSITO:", error);
        throw error;
    }
}

export async function dbExcluirDeposito(nomeUsuario, firebaseKey) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        await remove(ref(db, `depositos/${chave}/${firebaseKey}`));
    } catch (error) {
        console.error("ERRO AO EXCLUIR DEPOSITO:", error);
        throw error;
    }
}

export async function dbAtualizarDeposito(nomeUsuario, firebaseKey, dados) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        await set(ref(db, `depositos/${chave}/${firebaseKey}`), dados);
    } catch (error) {
        console.error("ERRO AO ATUALIZAR DEPOSITO:", error);
        throw error;
    }
}

export async function dbListarDepositos(nomeUsuario) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        const snapshot = await get(ref(db, `depositos/${chave}`));
        if (snapshot.exists()) {
            const data = snapshot.val();
            return Object.keys(data).map(key => ({ ...data[key], firebaseUrl: key }));
        }
    } catch (error) {
        console.error("ERRO AO LISTAR DEPOSITOS:", error);
    }
    return [];
}

export function dbEscutarDepositos(nomeUsuario, callback) {
    const chave = _normalizarChaveUsuario(nomeUsuario);
    if (!chave) {
        callback([]);
        return () => {};
    }

    return onValue(ref(db, `depositos/${chave}`), (snapshot) => {
        const data = snapshot.val();
        const lista = data
            ? Object.keys(data).map(key => ({ ...data[key], firebaseUrl: key }))
            : [];
        callback(lista);
    });
}

export async function dbListarManutencoes() {
    try {
        const snapshot = await get(ref(db, 'manutencoes'));
        if (snapshot.exists()) {
            const data = snapshot.val();
            return Object.keys(data).map(key => ({ ...data[key], firebaseUrl: key }));
        }
    } catch (error) {
        console.error("ERRO AO LISTAR MANUTENCOES:", error);
    }
    return [];
}

export function dbEscutarManutencoes(callback) {
    const manutencoesRef = ref(db, 'manutencoes');
    onValue(manutencoesRef, (snapshot) => {
        const data = snapshot.val();
        const lista = [];
        if (data) {
            Object.keys(data).forEach(key => {
                lista.push({ firebaseUrl: key, ...data[key] });
            });
        }
        callback(lista);
    });
}

/* --- FUNÇÕES PARA SELEÇÃO DE ROTAS --- */

// Cria ou sobrescreve a sessão ativa de seleção de rotas
export async function dbCriarSessaoRotas(sessao) {
    try {
        await set(ref(db, 'selecao_rotas/ativa'), sessao);
    } catch (error) {
        console.error("ERRO AO CRIAR SESSÃO DE ROTAS:", error);
        throw error;
    }
}

// Escuta em tempo real a sessão ativa de seleção de rotas
export function dbEscutarSessaoRotas(callback) {
    onValue(ref(db, 'selecao_rotas/ativa'), (snapshot) => {
        callback(snapshot.val());
    });
}

// Lê uma vez a sessão ativa
export async function dbObterSessaoRotas() {
    const snapshot = await get(ref(db, 'selecao_rotas/ativa'));
    return snapshot.val();
}

// Encerra a sessão ativa de seleção de rotas
export async function dbEncerrarSessaoRotas() {
    try {
        await remove(ref(db, 'selecao_rotas/ativa'));
    } catch (error) {
        console.error("ERRO AO ENCERRAR SESSÃO:", error);
        throw error;
    }
}

// Tenta atomicamente reivindicar a rota de maior valor disponível para o usuário.
// Retorna { numeroRota, ...dadosRota } se conseguir, ou null se não houver rotas disponíveis.
export async function dbSelecionarRota(nomeUsuario) {
    const sessionRef = ref(db, 'selecao_rotas/ativa');
    const snapshot = await get(sessionRef);
    const sessao = snapshot.val();
    if (!sessao || !sessao.rotas) return null;

    const rotas = sessao.rotas;

    // Ordena as disponíveis pelo maior valor estimado
    const disponiveis = Object.entries(rotas)
        .filter(([_, r]) => !r.selecionada_por)
        .sort(([_, a], [__, b]) => (b.valor_estimado || 0) - (a.valor_estimado || 0));

    if (disponiveis.length === 0) return null;

    const [numeroRota] = disponiveis[0];
    const rotaRef = ref(db, `selecao_rotas/ativa/rotas/${numeroRota}`);

    let tentativa = null;

    const result = await runTransaction(rotaRef, (dadosAtuais) => {
        tentativa = null; // reseta a cada invocação do callback (Firebase pode chamar várias vezes)
        if (dadosAtuais && !dadosAtuais.selecionada_por) {
            tentativa = { ...dadosAtuais, numeroRota };
            return {
                ...dadosAtuais,
                selecionada_por: nomeUsuario,
                timestamp_selecao: new Date().toISOString()
            };
        }
        return undefined; // aborta: rota já foi pega
    });

    if (result.committed && tentativa) {
        return { ...tentativa, selecionada_por: nomeUsuario };
    }

    // Rota foi pega por outra pessoa no mesmo instante: tenta a próxima
    return dbSelecionarRota(nomeUsuario);
}

/* --- FUNÇÕES PARA CHECK-IN DE ROTAS / JUSTIFICATIVAS --- */

// Salva (ou substitui) a justificativa de um cliente em uma rota
export async function dbSalvarJustificativaCheckin(routeNumber, clienteKey, dados) {
    try {
        await set(ref(db, `justificativas_rotas/${routeNumber}/${clienteKey}`), dados);
    } catch (error) {
        console.error("ERRO AO SALVAR JUSTIFICATIVA:", error);
        throw error;
    }
}

// Lê todas as justificativas de uma rota (retorna objeto { clienteKey: {...} })
export async function dbListarJustificativasCheckin(routeNumber) {
    try {
        const snap = await get(ref(db, `justificativas_rotas/${routeNumber}`));
        return snap.val() || {};
    } catch (error) {
        console.error("ERRO AO LISTAR JUSTIFICATIVAS:", error);
        return {};
    }
}

// Remove a justificativa de um cliente em uma rota
export async function dbRemoverJustificativaCheckin(routeNumber, clienteKey) {
    try {
        await remove(ref(db, `justificativas_rotas/${routeNumber}/${clienteKey}`));
    } catch (error) {
        console.error("ERRO AO REMOVER JUSTIFICATIVA:", error);
        throw error;
    }
}

// Escuta em tempo real todas as justificativas de uma rota
export function dbEscutarJustificativasCheckin(routeNumber, callback) {
    onValue(ref(db, `justificativas_rotas/${routeNumber}`), (snap) => {
        callback(snap.val() || {});
    });
}

function _normalizarTextoRotaUsuario(valor) {
    return String(valor || '').trim().toLowerCase();
}

function _normalizarNumeroClienteRota(valor) {
    return String(valor || '').replace(/\D/g, '').trim();
}

function _obterTimestampRota(valor) {
    if (typeof valor === 'number' && Number.isFinite(valor)) return valor;
    const convertido = Date.parse(valor || '');
    return Number.isNaN(convertido) ? null : convertido;
}

function _compararClienteRota(clienteRota, clienteAtendimento) {
    const idRota = String(clienteRota?.firebaseUrl || clienteRota?.id || '').trim();
    const idAtendimento = String(clienteAtendimento?.id || clienteAtendimento?.firebaseUrl || '').trim();
    if (idRota && idAtendimento && idRota === idAtendimento) return true;

    const numeroRota = _normalizarNumeroClienteRota(clienteRota?.numero);
    const numeroAtendimento = _normalizarNumeroClienteRota(clienteAtendimento?.numero);
    return Boolean(numeroRota && numeroAtendimento && numeroRota === numeroAtendimento);
}

function _obterJustificativaCliente(justificativas, cliente) {
    const chaves = [
        String(cliente?.firebaseUrl || '').trim(),
        String(cliente?.id || '').trim(),
        _normalizarNumeroClienteRota(cliente?.numero)
    ].filter(Boolean);

    for (const chave of chaves) {
        if (justificativas?.[chave]) return justificativas[chave];
    }
    return null;
}

function _obterTotalAtendimentoRota(atendimento) {
    const total = Number(
        atendimento?.financeiro?.totalGeral
        ?? atendimento?.financeiro?.total
        ?? atendimento?.totalGeral
        ?? atendimento?.total
        ?? 0
    );
    return Number.isFinite(total) ? total : 0;
}

export function calcularStatusLiberacaoRota({
    rotaNumero = '',
    rotaDados = {},
    clientesDaRota = [],
    atendimentos = [],
    justificativas = {},
    nomeUsuario = '',
    agora = Date.now()
} = {}) {
    const DOIS_DIAS = 2 * 24 * 60 * 60 * 1000;
    const SEIS_DIAS = 6 * 24 * 60 * 60 * 1000;
    const CINCO_DIAS = 5 * 24 * 60 * 60 * 1000;
    const inicioRotaMs = _obterTimestampRota(rotaDados?.timestamp_selecao);
    const prazoMaximoMs = inicioRotaMs == null ? null : (inicioRotaMs + CINCO_DIAS);
    const usuarioNorm = _normalizarTextoRotaUsuario(nomeUsuario || rotaDados?.selecionada_por);

    const atendidos = [];
    const justificados = [];
    const pendentes = [];
    let primeiroAtendimentoValidoMs = null;

    (Array.isArray(clientesDaRota) ? clientesDaRota : []).forEach((cliente) => {
        const atendimentosValidosCliente = (Array.isArray(atendimentos) ? atendimentos : []).filter((atendimento) => {
            if (!_compararClienteRota(cliente, atendimento?.cliente)) return false;

            const atendenteNorm = _normalizarTextoRotaUsuario(atendimento?.atendente);
            if (usuarioNorm && atendenteNorm !== usuarioNorm) return false;

            const dataAtendimentoMs = _obterTimestampRota(atendimento?.ultimaEdicao || atendimento?.data);
            if (dataAtendimentoMs == null) return false;
            if (!(_obterTotalAtendimentoRota(atendimento) > 0)) return false;
            if ((agora - dataAtendimentoMs) > SEIS_DIAS) return false;
            if (inicioRotaMs != null && dataAtendimentoMs < inicioRotaMs) return false;
            return true;
        });

        const foiAtendido = atendimentosValidosCliente.length > 0;

        if (foiAtendido) {
            const primeiroAtendimentoClienteMs = atendimentosValidosCliente.reduce((menor, atendimento) => {
                const ts = _obterTimestampRota(atendimento?.ultimaEdicao || atendimento?.data);
                return ts != null && (menor == null || ts < menor) ? ts : menor;
            }, null);
            if (primeiroAtendimentoClienteMs != null && (primeiroAtendimentoValidoMs == null || primeiroAtendimentoClienteMs < primeiroAtendimentoValidoMs)) {
                primeiroAtendimentoValidoMs = primeiroAtendimentoClienteMs;
            }
            atendidos.push(cliente);
            return;
        }

        const justif = _obterJustificativaCliente(justificativas, cliente);
        const justifMs = _obterTimestampRota(justif?.timestamp);
        if (justif && justifMs != null && (inicioRotaMs == null || justifMs >= inicioRotaMs)) {
            justificados.push({
                ...cliente,
                justif,
                liberarEm: justifMs + DOIS_DIAS,
                expirou: agora >= (justifMs + DOIS_DIAS)
            });
            return;
        }

        pendentes.push(cliente);
    });

    const todosResolvidos = pendentes.length === 0;
    const ultimaJustificativaMs = justificados.reduce((maior, cliente) => {
        const ts = _obterTimestampRota(cliente?.justif?.timestamp);
        return ts != null && ts > maior ? ts : maior;
    }, 0);

    let motivo = 'aguardando_prazo_maximo';
    let liberarAgora = false;
    let liberarEmMs = prazoMaximoMs;
    let referenciaLiberacaoMs = null;
    let referenciaLiberacaoTipo = null;

    if (prazoMaximoMs != null && agora >= prazoMaximoMs) {
        motivo = 'prazo_maximo';
        liberarAgora = true;
        liberarEmMs = prazoMaximoMs;
    } else if (todosResolvidos && justificados.length === 0) {
        motivo = 'todos_atendidos';
        liberarAgora = true;
        liberarEmMs = agora;
    } else if (todosResolvidos && justificados.length > 0) {
        if (primeiroAtendimentoValidoMs == null) {
            motivo = 'aguardando_primeiro_atendimento';
        } else {
            referenciaLiberacaoMs = Math.max(primeiroAtendimentoValidoMs, ultimaJustificativaMs || 0);
            referenciaLiberacaoTipo = referenciaLiberacaoMs === primeiroAtendimentoValidoMs && primeiroAtendimentoValidoMs > (ultimaJustificativaMs || 0)
                ? 'primeiro_atendimento'
                : 'ultima_justificativa';
            liberarEmMs = referenciaLiberacaoMs + DOIS_DIAS;
            if (agora >= liberarEmMs) {
                motivo = 'ultima_justificativa_expirada';
                liberarAgora = true;
            } else {
                motivo = 'aguardando_ultima_justificativa';
            }
        }
    }

    return {
        rotaNumero: String(rotaNumero || rotaDados?.numero || '').trim(),
        atendidos,
        justificados,
        pendentes,
        todosResolvidos,
        inicioRotaMs,
        primeiroAtendimentoValidoMs,
        prazoMaximoMs,
        ultimaJustificativaMs: ultimaJustificativaMs || null,
        liberarAgora,
        liberarEmMs,
        motivo,
        referenciaLiberacaoMs,
        referenciaLiberacaoTipo,
        tempoRestanteMs: liberarEmMs == null ? null : Math.max(0, liberarEmMs - agora)
    };
}

export async function dbVerificarELiberarRota(numeroRota, nomeUsuario, dados = {}) {
    try {
        const sessao = dados?.sessao || (await get(ref(db, 'selecao_rotas/ativa'))).val();
        const rotaDados = sessao?.rotas?.[numeroRota];
        if (!rotaDados) return { liberada: false, status: null };

        const usuarioSelecionado = _normalizarTextoRotaUsuario(rotaDados?.selecionada_por);
        const usuarioInformado = _normalizarTextoRotaUsuario(nomeUsuario);
        if (usuarioInformado && usuarioSelecionado && usuarioSelecionado !== usuarioInformado) {
            return { liberada: false, status: null };
        }

        const clientesDaRota = dados?.clientesDaRota || (sessao?.clientes_por_rota?.[numeroRota] || []);
        const atendimentos = dados?.atendimentos || await dbListarAtendimentos();
        const justificativas = dados?.justificativas || await dbListarJustificativasCheckin(numeroRota);

        const status = calcularStatusLiberacaoRota({
            rotaNumero: numeroRota,
            rotaDados,
            clientesDaRota,
            atendimentos,
            justificativas,
            nomeUsuario: rotaDados?.selecionada_por || nomeUsuario
        });

        if (!status?.liberarAgora) {
            return { liberada: false, status };
        }

        await dbLiberarRota(numeroRota);
        return { liberada: true, status };
    } catch (error) {
        console.error("ERRO AO VERIFICAR LIBERACAO DA ROTA:", error);
        throw error;
    }
}

/**
 * Libera uma rota específica da sessão ativa, zerando o campo selecionada_por.
 *
 * QUANDO USAR:
 * Critérios de liberação:
 *   1. Todos os clientes atendidos nos últimos 6 dias -> libera no mesmo instante.
 *   2. Havendo justificativa, libera 2 dias após a última justificativa,
 *      mas essa contagem só começa depois do primeiro atendimento válido da rota.
 *   3. Rota selecionada há mais de 5 dias (timestamp_selecao) -> libera pelo prazo máximo.
 * Ao liberar, a rota volta a ficar disponível para outro usuário selecionar.
 *
 * QUEM CHAMA:
 * - checkin_rotas.html -> ao abrir e ao atualizar as rotas do usuário
 * - atendimento_nivel_1.html -> após salvar um atendimento, para liberar na hora quando a rota termina
 *
 * @param {string} numeroRota - O número/chave da rota no Firebase (ex: "3", "12")
 */
export async function dbLiberarRota(numeroRota) {
    try {
        const rotaRef = ref(db, `selecao_rotas/ativa/rotas/${numeroRota}`);
        await update(rotaRef, {
            selecionada_por: null,
            timestamp_selecao: null
        });
    } catch (error) {
        console.error("ERRO AO LIBERAR ROTA:", error);
        throw error;
    }
}

// Limpa todas as seleções da sessão ativa (apenas para testes)
export async function dbLimparSelecoes(numerosRota) {
    const updates = {};
    numerosRota.forEach(n => {
        updates[`selecao_rotas/ativa/rotas/${n}/selecionada_por`] = null;
        updates[`selecao_rotas/ativa/rotas/${n}/timestamp_selecao`] = null;
    });
    await update(ref(db, '/'), updates);
}

