import { get, ref } from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';
import { db } from './firebase-app.js';
import {
    calcularAdicoesMaquinas,
    formatarErroSaldoMaquinas,
    listarMaquinasComSaldoInsuficiente,
    listarMaquinasSemIdentificador
} from './maquina-posse-rules.mjs';

const POSSE_ITENS_ROOT = 'posse_itens_usuario';

function normalizarChaveFirebase(valor) {
    return String(valor || '').trim().replace(/[.#$/[\]]/g, '_');
}

function criarErroValidacao(mensagem, codigo, detalhes = []) {
    const erro = new Error(mensagem);
    erro.code = codigo;
    erro.detalhes = detalhes;
    return erro;
}

export async function validarSaldoParaAdicionarMaquinas({
    responsavel,
    itensAtuais,
    itensAnteriores = []
} = {}) {
    const semIdentificador = listarMaquinasSemIdentificador(itensAtuais);
    if (semIdentificador.length > 0) {
        throw criarErroValidacao(
            `Não foi possível identificar esta máquina no balanço: ${semIdentificador.join(', ')}. Selecione-a novamente.`,
            'MAQUINA_SEM_IDENTIFICADOR',
            semIdentificador
        );
    }

    const solicitadas = calcularAdicoesMaquinas(itensAtuais, itensAnteriores);
    if (solicitadas.length === 0) return { ok: true, solicitadas: [], saldos: {} };

    const chaveUsuario = normalizarChaveFirebase(responsavel);
    if (!chaveUsuario) {
        throw criarErroValidacao(
            'Não foi possível identificar o usuário responsável pelas máquinas.',
            'USUARIO_MAQUINA_NAO_IDENTIFICADO'
        );
    }

    const saldos = {};
    try {
        for (const item of solicitadas) {
            const chaveItem = normalizarChaveFirebase(item.itemChave);
            const snapshot = await get(ref(db, `${POSSE_ITENS_ROOT}/${chaveUsuario}/${chaveItem}`));
            saldos[item.itemChave] = snapshot.exists()
                ? Number(snapshot.val()?.quantidade || 0)
                : 0;
        }
    } catch (erro) {
        console.error('ERRO AO CONFERIR SALDO DE MÁQUINAS:', erro);
        throw criarErroValidacao(
            'Não foi possível conferir seu balanço agora. Verifique a conexão e tente novamente.',
            'SALDO_MAQUINA_INDISPONIVEL'
        );
    }

    const faltas = listarMaquinasComSaldoInsuficiente(solicitadas, saldos);
    if (faltas.length > 0) {
        throw criarErroValidacao(
            formatarErroSaldoMaquinas(faltas),
            'SALDO_MAQUINA_INSUFICIENTE',
            faltas
        );
    }

    return { ok: true, solicitadas, saldos };
}
