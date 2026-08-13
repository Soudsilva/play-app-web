import {
    sincronizarPendentes,
    sincronizarFotosPendentes
} from './offline-sync.js';
import {
    dbSalvarAtendimento,
    dbSincronizarProdutosAtendimentoNoHistorico,
    dbGerarIdAtendimento,
    dbVerificarELiberarRota,
    dbAtualizarFotoAtendimentoPendente,
    storageSalvarFotoComThumb
} from './database.js';
import {
    confirmarAtendimentoNoFirebase,
    confirmarProdutosAtendimentoNoFirebase,
    confirmarResultadoRotaNoFirebase
} from './atendimento-sync-confirmacao-service.js';
import { salvarFotoAtendimentoRetomavel } from './atendimento-sync-upload-service.js';

function executarSincronizacaoAtendimentos(salvarFoto, opcoes = {}) {
    return sincronizarPendentes(
        salvarFoto,
        dbSalvarAtendimento,
        dbSincronizarProdutosAtendimentoNoHistorico,
        {
            gerarAtendimentoId: dbGerarIdAtendimento,
            atualizarFotoAtendimento: dbAtualizarFotoAtendimentoPendente,
            verificarRota: dbVerificarELiberarRota,
            confirmarAtendimento: confirmarAtendimentoNoFirebase,
            confirmarProdutos: confirmarProdutosAtendimentoNoFirebase,
            confirmarRota: confirmarResultadoRotaNoFirebase,
            ...(String(opcoes?.idPendente || '').trim()
                ? { idPendente: String(opcoes.idPendente).trim() }
                : {}),
            ...(opcoes?.somenteRegistroInicial === true
                ? { somenteRegistroInicial: true }
                : {})
        }
    );
}

export function sincronizarFilaAtendimentos(opcoes = {}) {
    return executarSincronizacaoAtendimentos(salvarFotoAtendimentoRetomavel, opcoes);
}

// Usado somente enquanto a tela de atendimento permanece aberta aguardando
// a conclusao. Reaproveita o uploader original, que envia foto e miniatura
// em paralelo, mantendo a fila local como protecao contra interrupcoes.
export function sincronizarFilaAtendimentosDireto(opcoes = {}) {
    return executarSincronizacaoAtendimentos(storageSalvarFotoComThumb, {
        ...opcoes,
        envioDireto: true
    });
}

export function sincronizarFotosPendentesLegadas() {
    return sincronizarFotosPendentes(
        salvarFotoAtendimentoRetomavel,
        dbAtualizarFotoAtendimentoPendente
    );
}
