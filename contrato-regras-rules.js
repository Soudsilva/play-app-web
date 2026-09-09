export const PRAZO_APROVACAO_REGRA_MS = 24 * 60 * 60 * 1000;

export const REGRAS_PADRAO = Object.freeze([
    {
        id: 'registro_atendimento',
        titulo: 'Registro obrigatório de atendimento',
        descricao: 'Todo atendimento em máquina deve ser registrado no app antes de sair do local. Sem exceções.'
    },
    {
        id: 'pagamentos_pix',
        titulo: 'Pagamentos apenas via Pix',
        descricao: 'Nenhum pagamento de cliente deve ser aceito em dinheiro vivo. Todo recebimento deve gerar registro no sistema.'
    },
    {
        id: 'retirada_socios',
        titulo: 'Retirada de sócios somente com saldo positivo',
        descricao: 'Não é permitido retirar lucro se o caixa do mês ainda estiver negativo ou em aberto.'
    },
    {
        id: 'manutencao_preventiva',
        titulo: 'Manutenção preventiva mensal',
        descricao: 'Cada máquina deve receber revisão completa uma vez por mês, independente de chamado.'
    },
    {
        id: 'comunicacao_problemas',
        titulo: 'Comunicação de problemas em até 2h',
        descricao: 'Qualquer falha em máquina comunicada por cliente deve ter resposta registrada em até 2 horas úteis.'
    },
    {
        id: 'estoque_minimo',
        titulo: 'Estoque mínimo sempre abastecido',
        descricao: 'O estoque de produto não pode ficar abaixo de 30% sem que um pedido de reposição já esteja feito.'
    }
]);

export function obterRegrasExibidas(ativos = {}) {
    const idsPadrao = new Set(REGRAS_PADRAO.map(regra => regra.id));
    const regrasPadrao = REGRAS_PADRAO
        .map(regra => ({ ...regra, ...(ativos?.[regra.id] || {}) }))
        .filter(regra => regra.excluida !== true);
    const regrasCriadas = Object.entries(ativos || {})
        .filter(([id, regra]) => !idsPadrao.has(id) && regra?.excluida !== true)
        .map(([id, regra]) => ({ id, ...regra }))
        .sort((a, b) => Number(a.criadoEmMs || 0) - Number(b.criadoEmMs || 0));
    return [...regrasPadrao, ...regrasCriadas];
}

export function obterAprovacoesIniciais(aprovadores = []) {
    return Object.fromEntries(
        aprovadores
            .map(item => String(item?.id || '').trim())
            .filter(Boolean)
            .map(id => [id, null])
    );
}

export function propostaRegraExpirada(proposta, agora = Date.now()) {
    const expiraEm = Number(proposta?.expiraEm);
    return !Number.isFinite(expiraEm) || expiraEm <= agora;
}
