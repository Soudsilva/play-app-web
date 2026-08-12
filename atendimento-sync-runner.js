import {
    sincronizarPendentes,
    sincronizarFotosPendentes
} from './offline-sync.js';
import {
    dbSalvarAtendimento,
    dbSincronizarProdutosAtendimentoNoHistorico,
    dbGerarIdAtendimento,
    dbVerificarELiberarRota,
    dbAtualizarFotoAtendimentoPendente
} from './database.js';
import {
    confirmarAtendimentoNoFirebase,
    confirmarProdutosAtendimentoNoFirebase,
    confirmarResultadoRotaNoFirebase
} from './atendimento-sync-confirmacao-service.js';
import { salvarFotoAtendimentoRetomavel } from './atendimento-sync-upload-service.js';

export function sincronizarFilaAtendimentos(opcoes = {}) {
    return sincronizarPendentes(
        salvarFotoAtendimentoRetomavel,
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

export function sincronizarFotosPendentesLegadas() {
    return sincronizarFotosPendentes(
        salvarFotoAtendimentoRetomavel,
        dbAtualizarFotoAtendimentoPendente
    );
}
