/* =========================================
   BANCO DE DADOS NA NUVEM (FIREBASE)
   Projeto: Play na Web - MODO REALTIME
   ========================================= */

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js";
import { getDatabase, ref, set, get, push, remove, onValue } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";

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

/* =========================================
   FUNÇÕES EXPORTADAS
   ========================================= */

// ESCUTA EM TEMPO REAL: Esta é a função mágica do delay zero
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

// SALVAR
export async function dbSalvarCliente(cliente) {
    const clientesRef = ref(db, 'clientes');
    const novoClienteRef = push(clientesRef);
    await set(novoClienteRef, cliente);
}

// EXCLUIR
export async function dbExcluirCliente(id) {
    const clienteRef = ref(db, `clientes/${id}`);
    await remove(clienteRef);
}

// LISTAR (Apenas para cálculos internos se necessário)
export async function dbListarClientes() {
    const snapshot = await get(ref(db, 'clientes'));
    if (snapshot.exists()) {
        const data = snapshot.val();
        return Object.keys(data).map(key => ({ ...data[key], firebaseUrl: key }));
    }
    return [];
}