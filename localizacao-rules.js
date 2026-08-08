export const LIMITE_LOCALIZACOES = 2;

export function textoNormalizado(valor) {
    return String(valor || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();
}

export function idFirebaseValido(valor) {
    const id = String(valor || '').trim();
    return !!id && !/[.#$\[\]\/]/.test(id);
}

export function perfilEhGestor(perfil) {
    const valor = textoNormalizado(perfil).replace(/\s+/g, '_');
    return valor.includes('gestao') || valor.includes('gestor');
}

export function perfilPodeGerenciarLocalizacao(perfil) {
    const valor = textoNormalizado(perfil).replace(/\s+/g, '_');
    return perfilEhGestor(valor) && !valor.includes('gestao_1');
}

export function perfilEhOperador(perfil) {
    const valor = textoNormalizado(perfil).replace(/\s+/g, '_');
    return !!valor && !perfilEhGestor(valor);
}

export function coordenadasValidas(latitude, longitude, precisaoMetros) {
    const lat = Number(latitude);
    const lng = Number(longitude);
    const precisao = Number(precisaoMetros);
    return Number.isFinite(lat) && lat >= -90 && lat <= 90
        && Number.isFinite(lng) && lng >= -180 && lng <= 180
        && Number.isFinite(precisao) && precisao >= 0 && precisao <= 100000;
}

export function classificarPrecisao(precisaoMetros) {
    const precisao = Number(precisaoMetros);
    if (!Number.isFinite(precisao)) return 'indisponivel';
    if (precisao <= 100) return 'alta';
    if (precisao <= 1000) return 'media';
    return 'aproximada';
}

export function limitarHistoricoLocalizacoes(historicoAtual, novoId, novaLocalizacao) {
    const historico = historicoAtual || {};
    const novoRegistro = { ...(novaLocalizacao || {}), solicitacaoId: novoId };
    const maisRecente = historico.mais_recente || null;
    if (maisRecente?.solicitacaoId === novoId) {
        return {
            ...(historico.anterior ? { anterior: historico.anterior } : {}),
            mais_recente: novoRegistro
        };
    }
    return {
        ...(maisRecente ? { anterior: maisRecente } : {}),
        mais_recente: novoRegistro
    };
}

export function aplicarRespostaPendente(estadoAtual, solicitacaoId, registro, respostaSolicitacao) {
    const estado = estadoAtual || {};
    const pedidoAtual = estado.solicitacao_atual || {};
    if (pedidoAtual.id !== solicitacaoId || pedidoAtual.status !== 'pendente') return undefined;

    return {
        ...estado,
        solicitacao_atual: {
            ...pedidoAtual,
            ...(respostaSolicitacao || {}),
            status: 'respondida'
        },
        historico: limitarHistoricoLocalizacoes(
            estado.historico,
            solicitacaoId,
            registro
        )
    };
}

export function historicoComoLista(historico) {
    return Object.entries(historico || {})
        .map(([id, registro]) => ({ id, ...(registro || {}) }))
        .sort((a, b) => Number(b.recebidaEmCliente || b.capturadaEm || 0)
            - Number(a.recebidaEmCliente || a.capturadaEm || 0))
        .slice(0, LIMITE_LOCALIZACOES);
}
