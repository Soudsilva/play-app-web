import {
    ref,
    get,
    onValue,
    query,
    orderByChild,
    equalTo,
    startAt,
    endAt,
    limitToLast
} from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';
import { db } from './firebase-app.js';
import {
    extrairMaquinasDaPosse,
    montarPaginaMovimentacoes,
    normalizarChaveItemBalanco,
    normalizarChaveUsuarioBalanco
} from './balanco-maquinas-gestor-rules.mjs';

const HISTORICO_ROOT = 'movimentacao_balanco_historico';
const POSSE_ROOT = 'posse_itens_usuario';

function criarConsultaCampo(responsavel, campo, valorCampo, limite, cursorId = '') {
    const usuario = normalizarChaveUsuarioBalanco(responsavel);
    const valor = campo === 'itemChave'
        ? normalizarChaveItemBalanco(valorCampo)
        : String(valorCampo || '').trim();
    if (!usuario || !valor) return null;

    const referencia = ref(db, `${HISTORICO_ROOT}/${usuario}`);
    const tamanho = Math.max(1, Math.min(Number(limite) || 10, 30));
    const cursor = String(cursorId || '').trim();
    if (!cursor) {
        return query(referencia, orderByChild(campo), equalTo(valor), limitToLast(tamanho + 1));
    }
    return query(
        referencia,
        orderByChild(campo),
        startAt(valor),
        endAt(valor, cursor),
        limitToLast(tamanho + 2)
    );
}

export function escutarMaquinasDoUsuario(responsavel, callback, tratarErro = () => {}) {
    const usuario = normalizarChaveUsuarioBalanco(responsavel);
    if (!usuario) {
        callback([]);
        return () => {};
    }
    return onValue(
        ref(db, `${POSSE_ROOT}/${usuario}`),
        snapshot => callback(extrairMaquinasDaPosse(snapshot.exists() ? snapshot.val() : {})),
        tratarErro
    );
}

export function escutarUltimasMovimentacoesDaMaquina(
    responsavel,
    itemChave,
    itemNome,
    limite,
    callback,
    tratarErro = () => {}
) {
    const consultaPrincipal = criarConsultaCampo(responsavel, 'itemChave', itemChave, limite);
    const consultaLegada = criarConsultaCampo(responsavel, 'itemNome', itemNome, limite);
    if (!consultaPrincipal && !consultaLegada) {
        callback([]);
        return () => {};
    }
    let dadosPrincipais = {};
    let dadosLegados = {};
    let principalCarregado = !consultaPrincipal;
    let legadoCarregado = !consultaLegada;
    const emitir = () => {
        if (!principalCarregado || !legadoCarregado) return;
        callback(montarPaginaMovimentacoes({ ...dadosLegados, ...dadosPrincipais }, limite).movimentos);
    };
    const cancelarPrincipal = consultaPrincipal
        ? onValue(consultaPrincipal, snapshot => {
            dadosPrincipais = snapshot.exists() ? snapshot.val() : {};
            principalCarregado = true;
            emitir();
        }, tratarErro)
        : () => {};
    const cancelarLegado = consultaLegada
        ? onValue(consultaLegada, snapshot => {
            dadosLegados = snapshot.exists() ? snapshot.val() : {};
            legadoCarregado = true;
            emitir();
        }, tratarErro)
        : () => {};
    return () => {
        cancelarPrincipal();
        cancelarLegado();
    };
}

export async function listarPaginaMovimentacoesDaMaquina(
    responsavel,
    itemChave,
    { limite = 10, cursorId = '', itemNome = '', mesclarLegado = true } = {}
) {
    const consultaPrincipal = criarConsultaCampo(responsavel, 'itemChave', itemChave, limite, cursorId);
    const consultaLegada = criarConsultaCampo(responsavel, 'itemNome', itemNome, limite, cursorId);
    if (!consultaPrincipal && !consultaLegada) return { movimentos: [], temMais: false, proximoCursor: '' };
    if (!mesclarLegado) {
        const snapshotPrincipal = consultaPrincipal ? await get(consultaPrincipal) : null;
        if (snapshotPrincipal?.exists()) {
            return montarPaginaMovimentacoes(snapshotPrincipal.val(), limite, cursorId);
        }
        const snapshotLegado = consultaLegada ? await get(consultaLegada) : null;
        return montarPaginaMovimentacoes(snapshotLegado?.exists() ? snapshotLegado.val() : {}, limite, cursorId);
    }
    const [snapshotPrincipal, snapshotLegado] = await Promise.all([
        consultaPrincipal ? get(consultaPrincipal) : Promise.resolve(null),
        consultaLegada ? get(consultaLegada) : Promise.resolve(null)
    ]);
    const dadosPrincipais = snapshotPrincipal?.exists() ? snapshotPrincipal.val() : {};
    const dadosLegados = snapshotLegado?.exists() ? snapshotLegado.val() : {};
    return montarPaginaMovimentacoes({ ...dadosLegados, ...dadosPrincipais }, limite, cursorId);
}
