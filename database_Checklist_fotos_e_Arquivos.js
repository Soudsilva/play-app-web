/* =========================================================================
   CHECKLIST, FOTOS E ARQUIVOS
   Camada de dados isolada para a tela arquivos_para_impressao.html.
   Caminho principal: checklists/maquinas/{maquinaId}
   ========================================================================= */

import {
    getDatabase,
    ref,
    set,
    get,
    push,
    remove,
    onValue,
    update
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js";
import { app } from './database.js';

const db = getDatabase(app);
const CHECKLISTS_ROOT = 'checklists';

function _normalizarNomeMaquinaChecklist(valor) {
    return String(valor || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, ' ')
        .trim();
}

function _validarChaveChecklist(chave, nomeCampo) {
    const valor = String(chave || '').trim();
    if (!valor || /[.#$\[\]\/]/.test(valor)) {
        throw new Error(`${nomeCampo} inválido.`);
    }
    return valor;
}

export function dbEscutarMaquinasChecklist(callback) {
    const maquinasRef = ref(db, `${CHECKLISTS_ROOT}/maquinas`);
    return onValue(maquinasRef, (snapshot) => {
        const dados = snapshot.val() || {};
        const lista = Object.entries(dados)
            .map(([firebaseUrl, maquina]) => ({ firebaseUrl, ...(maquina || {}) }))
            .filter(maquina => maquina.ativo !== false)
            .sort((a, b) => {
                const ordemA = Number(a.ordem || Number.MAX_SAFE_INTEGER);
                const ordemB = Number(b.ordem || Number.MAX_SAFE_INTEGER);
                if (ordemA !== ordemB) return ordemA - ordemB;
                return String(a.nome || '').localeCompare(String(b.nome || ''), 'pt-BR');
            });
        callback(lista);
    });
}

export async function dbAdicionarMaquinaChecklist(nome, criadoPor = '') {
    const nomeLimpo = String(nome || '').replace(/\s+/g, ' ').trim();
    if (nomeLimpo.length < 2 || nomeLimpo.length > 80) {
        throw new Error('Informe um nome de máquina entre 2 e 80 caracteres.');
    }

    const maquinasRef = ref(db, `${CHECKLISTS_ROOT}/maquinas`);
    const snapshot = await get(maquinasRef);
    const maquinas = snapshot.val() || {};
    const nomeNormalizado = _normalizarNomeMaquinaChecklist(nomeLimpo);
    const maquinaDuplicada = Object.values(maquinas).some(maquina =>
        _normalizarNomeMaquinaChecklist(maquina?.nome) === nomeNormalizado
    );

    if (maquinaDuplicada) {
        throw new Error('Essa máquina já está cadastrada nos checklists.');
    }

    const ordens = Object.values(maquinas)
        .map(maquina => Number(maquina?.ordem || 0))
        .filter(Number.isFinite);
    const novaRef = push(maquinasRef);
    const dados = {
        nome: nomeLimpo,
        ativo: true,
        ordem: (ordens.length ? Math.max(...ordens) : 0) + 1,
        itens: {},
        criadoEm: new Date().toISOString(),
        criadoPor: String(criadoPor || '').trim(),
        origem: 'cadastro_manual'
    };

    await set(novaRef, dados);
    return { firebaseUrl: novaRef.key, ...dados };
}

export async function dbExcluirMaquinaChecklist(maquinaId) {
    const idMaquina = _validarChaveChecklist(maquinaId, 'Identificador da máquina');
    await set(ref(db, `${CHECKLISTS_ROOT}/maquinas/${idMaquina}/ativo`), false);
}

export async function dbRenomearMaquinaChecklist(maquinaId, nome) {
    const idMaquina = _validarChaveChecklist(maquinaId, 'Identificador da máquina');
    const nomeLimpo = String(nome || '').replace(/\s+/g, ' ').trim();
    if (nomeLimpo.length < 2 || nomeLimpo.length > 80) {
        throw new Error('Informe um nome de máquina entre 2 e 80 caracteres.');
    }

    const maquinasRef = ref(db, `${CHECKLISTS_ROOT}/maquinas`);
    const snapshot = await get(maquinasRef);
    const maquinas = snapshot.val() || {};
    if (!maquinas[idMaquina]) {
        throw new Error('Essa máquina não foi encontrada.');
    }

    const nomeNormalizado = _normalizarNomeMaquinaChecklist(nomeLimpo);
    const maquinaDuplicada = Object.entries(maquinas).some(([id, maquina]) =>
        id !== idMaquina && _normalizarNomeMaquinaChecklist(maquina?.nome) === nomeNormalizado
    );
    if (maquinaDuplicada) {
        throw new Error('Essa máquina já está cadastrada nos checklists.');
    }

    await update(ref(db, `${CHECKLISTS_ROOT}/maquinas/${idMaquina}`), { nome: nomeLimpo });
}

export async function dbAdicionarItemChecklist(maquinaId, descricao, criadoPor = '') {
    const idMaquina = _validarChaveChecklist(maquinaId, 'Identificador da máquina');
    const descricaoLimpa = String(descricao || '').replace(/\s+/g, ' ').trim();
    if (descricaoLimpa.length < 2 || descricaoLimpa.length > 240) {
        throw new Error('Informe uma descrição entre 2 e 240 caracteres.');
    }

    const itensRef = ref(db, `${CHECKLISTS_ROOT}/maquinas/${idMaquina}/itens`);
    const snapshot = await get(itensRef);
    const itens = snapshot.val() || {};
    const descricaoNormalizada = _normalizarNomeMaquinaChecklist(descricaoLimpa);
    const itemDuplicado = Object.values(itens).some(item =>
        _normalizarNomeMaquinaChecklist(item?.descricao) === descricaoNormalizada
    );
    if (itemDuplicado) {
        throw new Error('Essa informação já existe no checklist.');
    }

    const ordens = Object.values(itens)
        .map(item => Number(item?.ordem || 0))
        .filter(Number.isFinite);
    const novoRef = push(itensRef);
    const dados = {
        descricao: descricaoLimpa,
        ordem: (ordens.length ? Math.max(...ordens) : 0) + 1,
        criadoEm: new Date().toISOString(),
        criadoPor: String(criadoPor || '').trim(),
        origem: 'cadastro_manual'
    };
    await set(novoRef, dados);
    return { firebaseUrl: novoRef.key, ...dados };
}

export async function dbEditarItemChecklist(maquinaId, itemId, descricao) {
    const idMaquina = _validarChaveChecklist(maquinaId, 'Identificador da máquina');
    const idItem = _validarChaveChecklist(itemId, 'Identificador da informação');
    const descricaoLimpa = String(descricao || '').replace(/\s+/g, ' ').trim();
    if (descricaoLimpa.length < 2 || descricaoLimpa.length > 240) {
        throw new Error('Informe uma descrição entre 2 e 240 caracteres.');
    }

    const itensRef = ref(db, `${CHECKLISTS_ROOT}/maquinas/${idMaquina}/itens`);
    const snapshot = await get(itensRef);
    const itens = snapshot.val() || {};
    if (!itens[idItem]) {
        throw new Error('Essa informação não foi encontrada no checklist.');
    }

    const descricaoNormalizada = _normalizarNomeMaquinaChecklist(descricaoLimpa);
    const itemDuplicado = Object.entries(itens).some(([id, item]) =>
        id !== idItem && _normalizarNomeMaquinaChecklist(item?.descricao) === descricaoNormalizada
    );
    if (itemDuplicado) {
        throw new Error('Essa informação já existe no checklist.');
    }

    await update(ref(db, `${CHECKLISTS_ROOT}/maquinas/${idMaquina}/itens/${idItem}`), {
        descricao: descricaoLimpa
    });
}

export async function dbAtualizarOrdemItensChecklist(maquinaId, itensIdsOrdenados) {
    const idMaquina = _validarChaveChecklist(maquinaId, 'Identificador da máquina');
    const ids = Array.isArray(itensIdsOrdenados)
        ? itensIdsOrdenados.map(itemId => _validarChaveChecklist(itemId, 'Identificador da informação'))
        : [];

    if (ids.length === 0) return;
    if (new Set(ids).size !== ids.length) {
        throw new Error('A nova ordem contém informações duplicadas.');
    }

    const atualizacoes = {};
    ids.forEach((itemId, indice) => {
        atualizacoes[`${itemId}/ordem`] = indice + 1;
    });

    await update(ref(db, `${CHECKLISTS_ROOT}/maquinas/${idMaquina}/itens`), atualizacoes);
}

export async function dbExcluirItemChecklist(maquinaId, itemId) {
    const idMaquina = _validarChaveChecklist(maquinaId, 'Identificador da máquina');
    const idItem = _validarChaveChecklist(itemId, 'Identificador da informação');
    await remove(ref(db, `${CHECKLISTS_ROOT}/maquinas/${idMaquina}/itens/${idItem}`));
}
