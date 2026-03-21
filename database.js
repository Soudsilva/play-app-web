/* =========================================================================
   PROJETO: PLAY NA WEB
   BANCO DE DADOS: FIREBASE REALTIME DATABASE
   ========================================================================= */

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

const firebaseConfig = {
    apiKey: "AIzaSyAog2lzvvWkOSvr8BqPgtGCZpSM4VQ2b3E",
    authDomain: "play-na-web.firebaseapp.com",
    databaseURL: "https://play-na-web-default-rtdb.firebaseio.com", 
    projectId: "play-na-web",
    storageBucket: "play-na-web.firebasestorage.app",
    messagingSenderId: "278404685529",
    appId: "1:278404685529:web:c8e7dc89eeb660173ae8c8"
};

const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

/* --- FUNÇÕES PARA CLIENTES --- */

export function dbEscutarClientes(callback) {
    const clientesRef = ref(db, 'clientes');
    onValue(clientesRef, (snapshot) => {
        const data = snapshot.val();
        if (data) {
            const lista = Object.keys(data).map(key => ({ 
                ...data[key], 
                firebaseUrl: key 
            }));
            callback(lista);
        } else {
            callback([]);
        }
    });
}

export async function dbSalvarCliente(cliente, idExistente = null) {
    try {
        if (idExistente) {
            const clienteRef = ref(db, `clientes/${idExistente}`);
            await set(clienteRef, cliente);
        } else {
            const clientesRef = ref(db, 'clientes');
            await push(clientesRef, cliente);
        }
    } catch (error) {
        console.error("ERRO AO SALVAR CLIENTE:", error);
        throw error;
    }
}

export async function dbExcluirCliente(id) {
    try {
        const clienteRef = ref(db, `clientes/${id}`);
        await remove(clienteRef);
    } catch (error) {
        console.error("ERRO AO EXCLUIR CLIENTE:", error);
        throw error;
    }
}

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

export function dbEscutarColaboradores(callback) {
    const colabRef = ref(db, 'colaboradores'); 
    onValue(colabRef, (snapshot) => {
        const data = snapshot.val();
        if (data) {
            const lista = Object.keys(data).map(key => ({ 
                ...data[key], 
                firebaseUrl: key 
            })).sort((a, b) => {
                // Ordena: Mais recente (Data maior) primeiro
                const dataA = a.dataCadastro ? new Date(a.dataCadastro) : new Date(0);
                const dataB = b.dataCadastro ? new Date(b.dataCadastro) : new Date(0);
                return dataB - dataA; 
            });
            callback(lista);
        } else {
            callback([]);
        }
    });
}

export async function dbSalvarColaborador(colaborador, idExistente = null) {
    try {
        if (idExistente) {
            const colabRef = ref(db, `colaboradores/${idExistente}`);
            await set(colabRef, colaborador);
        } else {
            const colabRef = ref(db, 'colaboradores');
            await push(colabRef, colaborador);
        }
    } catch (error) {
        console.error("ERRO AO SALVAR COLABORADOR:", error);
        throw error;
    }
}

export async function dbExcluirColaborador(id) {
    try {
        const colabRef = ref(db, `colaboradores/${id}`);
        await remove(colabRef);
    } catch (error) {
        console.error("ERRO AO EXCLUIR COLABORADOR:", error);
        throw error;
    }
}