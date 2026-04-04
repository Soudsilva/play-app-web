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
    limitToLast,
    runTransaction
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import { getStorage, ref as storageRef, uploadBytes, getDownloadURL } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-storage.js";
import { salvarCacheClientes, lerCacheClientes, salvarCacheEstoque, lerCacheEstoque } from './offline-sync.js';

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
export function dbEscutarClientes(callback) {
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
            await _upsertMediaDeVendasPorCliente(cliente, novoRef.key || null, null);
            await _atualizarVersaoMediaDeVendas();
        }
        // Avisa todos os dispositivos que a lista mudou
        await _atualizarVersaoClientes();
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
    } catch (error) {
        console.error("Erro ao salvar histórico:", error);
    }
}

// Escuta apenas os últimos 20 movimentos para exibir na tela
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
        const snapshot = await get(ref(db, 'clientes'));
        if (snapshot.exists()) {
            const data = snapshot.val();
            return Object.keys(data).map(key => ({ 
                ...data[key], 
                firebaseUrl: key 
            }));
        }
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
        if (idExistente) {
            const colabRef = ref(db, `colaboradores/${idExistente}`);
            const snapshot = await get(colabRef);
            const dadosAntigos = snapshot.val();
            // Mantém a posição na fila se já existir (não joga pro final)
            if (dadosAntigos && dadosAntigos.ordem !== undefined) {
                colaborador.ordem = dadosAntigos.ordem;
            }
            await set(colabRef, colaborador);
        } else {
            // USANDO O TIMESTAMP NEGATIVO: 
            // Quanto mais recente o cadastro, menor o número, logo, fica no topo.
            colaborador.ordem = -Date.now();
            const colabRef = ref(db, 'colaboradores');
            await push(colabRef, colaborador);
        }
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
export async function dbSalvarItemEstoque(item, id = null) {
    try {
        if (id) {
            // Se tem ID, atualiza o item existente
            const itemRef = ref(db, `estoque/${id}`);
            await update(itemRef, item);
        } else {
            // Se não tem ID, cria um novo
            const estoqueRef = ref(db, 'estoque');
            await push(estoqueRef, item);
        }
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

// [BLOCO: ESTOQUE - EXCLUIR ITEM]
export async function dbExcluirItemEstoque(id) {
    try {
        const itemRef = ref(db, `estoque/${id}`);
        await remove(itemRef);
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

// [NOVO] Salvar o registro completo do atendimento
export async function dbSalvarAtendimento(atendimento, idExistente = null) {
    try {
        if (idExistente) {
            await set(ref(db, `atendimentos/${idExistente}`), atendimento);
        } else {
            // Cria uma nova entrada na coleção 'atendimentos'
            const atendimentosRef = ref(db, 'atendimentos');
            await push(atendimentosRef, atendimento);
        }
        // Atualiza a base "Media_de_Vendas" (tempo real, sem depender do cache offline)
        await _upsertMediaDeVendasPorAtendimento(atendimento);
        await _atualizarVersaoMediaDeVendas();
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

export async function dbExcluirAtendimento(id) {
    try {
        await remove(ref(db, `atendimentos/${id}`));
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

function _normalizarChaveUsuario(nomeUsuario) {
    return String(nomeUsuario || '').replace(/[.#$/[\]]/g, '_');
}

export function dbEscutarFluxoCaixa(nomeUsuario, callback) {
    const chave = _normalizarChaveUsuario(nomeUsuario);
    if (!chave) {
        callback([]);
        return;
    }

    onValue(ref(db, `fluxo_caixa/${chave}/movimentacoes`), (snapshot) => {
        const data = snapshot.val();
        const lista = data
            ? Object.keys(data).map(key => ({ firebaseKey: key, ...data[key] }))
            : [];
        callback(lista);
    });
}

export async function dbListarFluxoCaixa(nomeUsuario) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        if (!chave) return [];
        const snapshot = await get(ref(db, `fluxo_caixa/${chave}/movimentacoes`));
        if (!snapshot.exists()) return [];
        const data = snapshot.val();
        return Object.keys(data).map(key => ({ firebaseKey: key, ...data[key] }));
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
    } catch (error) {
        console.error("ERRO AO EXCLUIR TODOS OS MOVIMENTOS DO FLUXO DE CAIXA:", error);
        throw error;
    }
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

/**
 * Lê o acumulador de posse de um usuário no Firebase.
 * Retorna { produtos: { "NomeProduto": quantidade, ... }, maquinas: { "NomeMaquina": contagem, ... } }
 * Se não houver nada salvo, retorna objetos vazios.
 */
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

/**
 * Incrementa o acumulador de posse com os dados de UM atendimento.
 * DEVE ser chamada ANTES de excluir um atendimento para não perder os dados.
 *
 * @param {string} nomeUsuario - Nome do colaborador dono do atendimento
 * @param {object} atendimento - Objeto completo do atendimento que será excluído
 */
export async function dbAcumularPosse(nomeUsuario, atendimento) {
    try {
        const chave = _normalizarChaveUsuario(nomeUsuario);
        const posseRef = ref(db, `posse_acumulada/${chave}`);
        const snap = await get(posseRef);
        const atual = snap.val() || { produtos: {}, maquinas: {} };

        // Acumula produtos do atendimento
        (atendimento.produtos || []).forEach(p => {
            if (!p.nome) return;
            atual.produtos[p.nome] = (atual.produtos[p.nome] || 0) + Number(p.quantidade || 0);
        });

        // Acumula máquinas do atendimento (salvas dentro de fotos.maquinas)
        (atendimento.fotos?.maquinas || []).forEach(m => {
            if (!m.nome) return;
            atual.maquinas[m.nome] = (atual.maquinas[m.nome] || 0) + 1;
        });

        await set(posseRef, atual);
    } catch (error) {
        console.error("ERRO AO ACUMULAR POSSE:", error);
        throw error;
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

/**
 * Libera uma rota específica da sessão ativa, zerando o campo selecionada_por.
 *
 * QUANDO USAR:
 * Critérios de liberação (verificados no checkin_rotas.html ao abrir):
 *   1. Todos os clientes atendidos (pelo mesmo usuário nos últimos 2 dias)
 *      OU com justificativa expirada (mais de 2 dias) — zero pendentes.
 *   2. Rota selecionada há mais de 5 dias (timestamp_selecao).
 * Ao liberar, a rota volta a ficar disponível para outro usuário selecionar.
 *
 * QUEM CHAMA:
 * - atendimento_nivel_1.html → após salvar um atendimento, verifica se a rota ficou completa
 * - checkin_rotas.html → na inicialização, verifica se alguma rota já expirou e pode ser liberada
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

