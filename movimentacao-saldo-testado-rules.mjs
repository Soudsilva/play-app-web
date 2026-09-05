export function movimentoUsaSaldoTestado(item, modo) {
    return String(modo || '').trim() === 'saida'
        && String(item?.categoria || '').trim() === 'maquina';
}

export function obterSaldoTestadoDisponivel(resumo) {
    if (!resumo || resumo.carregado === false) return null;
    const valor = Number(resumo.saldoAtual ?? resumo.saldoExibido ?? resumo.saldo);
    return Number.isFinite(valor) ? Math.max(0, Math.trunc(valor)) : null;
}

export function validarSaldosTestadosParaSaida(linhas = [], saldosPorEquipamento = new Map(), modo = 'saida') {
    if (String(modo || '').trim() !== 'saida') return { valido: true, consumos: [] };

    const consumos = new Map();
    (Array.isArray(linhas) ? linhas : []).forEach(linha => {
        if (!movimentoUsaSaldoTestado(linha?.itemOrigem, modo)) return;
        const equipamentoId = String(linha?.idItem || linha?.itemOrigem?.firebaseUrl || '').trim();
        const quantidade = Math.max(0, Math.trunc(Number(linha?.qtdMovimento || 0)));
        if (!equipamentoId || quantidade <= 0) return;

        const atual = consumos.get(equipamentoId) || {
            equipamentoId,
            nome: String(linha?.itemOrigem?.nome || 'máquina').trim() || 'máquina',
            quantidade: 0
        };
        atual.quantidade += quantidade;
        consumos.set(equipamentoId, atual);
    });

    for (const consumo of consumos.values()) {
        const saldo = obterSaldoTestadoDisponivel(saldosPorEquipamento.get(consumo.equipamentoId));
        if (saldo === null) {
            return {
                valido: false,
                motivo: 'carregando',
                mensagem: `Aguarde o saldo testado de ${consumo.nome} carregar.`
            };
        }
        if (consumo.quantidade > saldo) {
            return {
                valido: false,
                motivo: 'insuficiente',
                equipamentoId: consumo.equipamentoId,
                disponivel: saldo,
                necessario: consumo.quantidade,
                mensagem: `Saldo testado insuficiente de ${consumo.nome}. Disponível: ${saldo}. Necessário: ${consumo.quantidade}.`
            };
        }
    }

    return { valido: true, consumos: [...consumos.values()] };
}
