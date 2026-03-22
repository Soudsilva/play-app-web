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
    update // ADICIONADO: Necessário para atualizar apenas a ordem
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";

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
const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

/* --- FUNÇÕES PARA CLIENTES --- */

// FUNÇÃO: ESCUTAR CLIENTES (Em Tempo Real)
// O que faz: Fica "de ouvidos abertos". Sempre que alguém mudar algo no banco de dados,
// essa função avisa o site instantaneamente para atualizar a tela sem precisar recarregar (F5).
export function dbEscutarClientes(callback) {
    const clientesRef = ref(db, 'clientes');
    onValue(clientesRef, (snapshot) => {
        const data = snapshot.val();
        if (data) {
            // Transforma os dados brutos do banco em uma lista organizada
            const lista = Object.keys(data).map(key => ({ 
                ...data[key], 
                firebaseUrl: key 
            }));
            // Devolve a lista pronta para quem chamou a função
            callback(lista);
        } else {
            callback([]);
        }
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
            await set(clienteRef, cliente);
        } else {
            // Modo Criação: Cria um novo cliente com chave única
            const clientesRef = ref(db, 'clientes');
            await push(clientesRef, cliente);
        }
    } catch (error) {
        console.error("ERRO AO SALVAR CLIENTE:", error);
        throw error;
    }
}

// FUNÇÃO: EXCLUIR CLIENTE
// O que faz: Remove permanentemente o cliente do banco de dados baseado no ID.
export async function dbExcluirCliente(id) {
    try {
        const clienteRef = ref(db, `clientes/${id}`);
        await remove(clienteRef);
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
export async function dbSalvarItemEstoque(item) {
    try {
        const estoqueRef = ref(db, 'estoque');
        await push(estoqueRef, item);
    } catch (error) {
        console.error("Erro ao salvar no estoque:", error);
        throw error;
    }
}

// [BLOCO: ESTOQUE - LER ITENS]
export function dbEscutarEstoque(callback) {
    const estoqueRef = ref(db, 'estoque');
    onValue(estoqueRef, (snapshot) => {
        const dados = snapshot.val();
        const lista = [];
        if (dados) {
            for (let key in dados) {
                lista.push({ firebaseUrl: key, ...dados[key] });
            }
        }
        // Ordena por ordem alfabética
        lista.sort((a, b) => a.nome.localeCompare(b.nome));
        callback(lista);
    });
}