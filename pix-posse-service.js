import { get, ref } from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';
import { db } from './firebase-app.js';
import {
    calcularPixAdicionados,
    formatarErroPixSemPosse,
    normalizarNumeroPix
} from './pix-posse-rules.mjs';

const PIX_EM_POSSE_ROOT = 'pix_em_posse';

function normalizarChaveFirebase(valor) {
    return String(valor || '').trim().replace(/[.#$/[\]]/g, '_');
}

function criarErroValidacao(mensagem, codigo, detalhes = []) {
    const erro = new Error(mensagem);
    erro.code = codigo;
    erro.detalhes = detalhes;
    return erro;
}

function registroCorrespondeAoPix(registro, numeroPix) {
    if (registro === null || registro === undefined) return false;
    if (typeof registro !== 'object') return true;

    const numeroRegistrado = normalizarNumeroPix(registro.numero_pix);
    return !numeroRegistrado || numeroRegistrado === numeroPix;
}

export async function validarPixParaCadastrarNoCliente({
    responsavel,
    itensAtuais,
    itensAnteriores = []
} = {}) {
    const pixAdicionados = calcularPixAdicionados(itensAtuais, itensAnteriores);
    if (pixAdicionados.length === 0) {
        return { ok: true, pixAdicionados: [] };
    }

    const chaveUsuario = normalizarChaveFirebase(responsavel);
    if (!chaveUsuario) {
        throw criarErroValidacao(
            'Não foi possível identificar o usuário responsável pelo Pix.',
            'USUARIO_PIX_NAO_IDENTIFICADO'
        );
    }

    const indisponiveis = [];
    try {
        for (const numeroPix of pixAdicionados) {
            const chavePix = normalizarChaveFirebase(numeroPix);
            const snapshot = await get(ref(db, `${PIX_EM_POSSE_ROOT}/${chaveUsuario}/${chavePix}`));
            if (!snapshot.exists() || !registroCorrespondeAoPix(snapshot.val(), numeroPix)) {
                indisponiveis.push(numeroPix);
            }
        }
    } catch (erro) {
        console.error('ERRO AO CONFERIR PIX EM POSSE:', erro);
        throw criarErroValidacao(
            'Não foi possível conferir seu balanço de Pix agora. Verifique a conexão e tente novamente.',
            'POSSE_PIX_INDISPONIVEL'
        );
    }

    if (indisponiveis.length > 0) {
        throw criarErroValidacao(
            formatarErroPixSemPosse(indisponiveis),
            'PIX_NAO_ESTA_EM_POSSE',
            indisponiveis
        );
    }

    return { ok: true, pixAdicionados };
}
