/* =========================================================================
   PROJETO: PLAY NA WEB
   BANCO DE DADOS: FIREBASE REALTIME DATABASE
   FUNÇÃO: Este arquivo centraliza todas as operações de leitura, escrita e 
           exclusão. Nenhuma outra tela toca no Firebase diretamente;
           elas pedem permissão para este arquivo.
   ========================================================================= */

// 1. IMPORTAÇÕES DAS BIBLIOTECAS OFICIAIS DO GOOGLE
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js";
import { 
    getDatabase, 
    ref, 
    set, 
    get, 
    push, 
    remove, 
    onValue 
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";

// 2. CONFIGURAÇÕES DE ACESSO (O "Endereço" do seu banco)
const firebaseConfig = {
    apiKey: "AIzaSyAog2lzvvWkOSvr8BqPgtGCZpSM4VQ2b3E",
    authDomain: "play-na-web.firebaseapp.com",
    databaseURL: "https://play-na-web-default-rtdb.firebaseio.com", 
    projectId: "play-na-web",
    storageBucket: "play-na-web.firebasestorage.app",
    messagingSenderId: "278404685529",
    appId: "1:278404685529:web:c8e7dc89eeb660173ae8c8"
};

// 3. INICIALIZAÇÃO
// app: Inicia a conexão com o Google
// db: Cria o túnel para o Realtime Database
const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

/* =========================================================================
   FUNÇÕES EXPORTADAS (As que as telas HTML utilizam)
   ========================================================================= */

/**
 * [ESCUTA EM TEMPO REAL]
 * Esta função é "fofoqueira". Se qualquer pessoa mudar um dado no Firebase,
 * ela avisa a tela Clientes.html instantaneamente sem precisar dar Refresh.
 * @param {Function} callback - A função que será executada quando os dados chegarem.
 */
export function dbEscutarClientes(callback) {
    const clientesRef = ref(db, 'clientes'); // Aponta para a pasta 'clientes'

    onValue(clientesRef, (snapshot) => {
        const data = snapshot.val();
        if (data) {
            // O Firebase entrega um Objeto {ID: {dados}}. 
            // Nós convertemos para uma Lista (Array) para facilitar o uso no HTML.
            // O campo 'firebaseUrl' guarda o ID único gerado pelo Google.
            const lista = Object.keys(data).map(key => ({ 
                ...data[key], 
                firebaseUrl: key 
            }));
            callback(lista);
        } else {
            callback([]); // Se não houver dados, retorna lista vazia
        }
    });
}

/**
 * [SALVAR OU ATUALIZAR]
 * Se receber um 'idExistente', ele sobrepõe os dados (Edição).
 * Se não receber, ele cria um novo registro com ID aleatório (Novo).
 */
export async function dbSalvarCliente(cliente, idExistente = null) {
    try {
        if (idExistente) {
            // MODO EDIÇÃO: O caminho é 'clientes/ID_DO_CLIENTE'
            const clienteRef = ref(db, `clientes/${idExistente}`);
            await set(clienteRef, cliente); // 'set' substitui tudo o que tem lá
        } else {
            // MODO NOVO: Cria uma chave única automática no caminho 'clientes'
            const clientesRef = ref(db, 'clientes');
            await push(clientesRef, cliente); // 'push' gera o ID tipo "-NoXyZ..."
        }
    } catch (error) {
        console.error("ERRO CRÍTICO AO SALVAR NO FIREBASE:", error);
        throw error; // Repassa o erro para a tela avisar o usuário
    }
}

/**
 * [EXCLUIR REGISTRO]
 * Remove permanentemente o cliente do banco de dados pelo ID.
 */
export async function dbExcluirCliente(id) {
    try {
        const clienteRef = ref(db, `clientes/${id}`);
        await remove(clienteRef);
    } catch (error) {
        console.error("ERRO AO EXCLUIR REGISTRO:", error);
        throw error;
    }
}

/**
 * [LISTAGEM ÚNICA (SEM ESCUTA)]
 * Diferente da dbEscutarClientes, esta função busca os dados uma única vez
 * e encerra a conexão. Útil para relatórios ou buscas específicas.
 */
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
        console.error("ERRO AO LISTAR DADOS:", error);
    }
    return [];
}