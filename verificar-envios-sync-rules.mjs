function normalizarTexto(valor) {
    return String(valor || '').trim().toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '');
}

function pertenceAoUsuario(item, usuarioLogado) {
    const usuario = normalizarTexto(usuarioLogado);
    if (!usuario) return false;
    const dados = item?.dados || {};
    return [
        dados?.atendente,
        dados?.responsavel,
        dados?.enviadoPor,
        dados?.autor,
        dados?.autorizadoPor
    ].some(nome => normalizarTexto(nome) === usuario);
}

function criarMetadadosPendencia(item) {
    return {
        estado: String(item?.estado || 'pendente'),
        fase: String(item?.fase || 'salvo_localmente'),
        tentativas: Number(item?.tentativas || 0),
        ultimoErro: String(item?.ultimoErro || '')
    };
}

export function combinarAtendimentosComFila(atendimentosRemotos = [], pendenciasLocais = [], usuarioLogado = '') {
    const remotos = Array.isArray(atendimentosRemotos) ? atendimentosRemotos : [];
    const pendencias = (Array.isArray(pendenciasLocais) ? pendenciasLocais : [])
        .filter(item => pertenceAoUsuario(item, usuarioLogado));
    const pendenciasPorServidor = new Map();

    pendencias.forEach(item => {
        const atendimentoServidorId = String(item?.atendimentoServidorId || '').trim();
        if (atendimentoServidorId) pendenciasPorServidor.set(atendimentoServidorId, item);
    });

    const idsRemotos = new Set();
    const combinados = remotos.map(remoto => {
        const firebaseUrl = String(remoto?.firebaseUrl || '').trim();
        if (firebaseUrl) idsRemotos.add(firebaseUrl);
        const pendencia = firebaseUrl ? pendenciasPorServidor.get(firebaseUrl) : null;
        if (!pendencia) {
            return { ...remoto, _statusSincronizacao: 'confirmado' };
        }

        return {
            ...remoto,
            ...(pendencia?.dados || {}),
            firebaseUrl,
            _statusSincronizacao: 'pendente',
            _registroSomenteLocal: false,
            _sincronizacaoLocal: criarMetadadosPendencia(pendencia),
            _produtosFilaLocal: Array.isArray(pendencia?.dados?.produtos) ? pendencia.dados.produtos : []
        };
    });

    pendencias.forEach(item => {
        const atendimentoServidorId = String(item?.atendimentoServidorId || '').trim();
        if (atendimentoServidorId && idsRemotos.has(atendimentoServidorId)) return;
        const idLocal = String(item?.id || '').trim();
        const dados = item?.dados || {};
        combinados.push({
            ...dados,
            data: dados?.data || item?.criadoEm || new Date(0).toISOString(),
            firebaseUrl: atendimentoServidorId || `local_${idLocal}`,
            _statusSincronizacao: 'pendente',
            _registroSomenteLocal: true,
            _sincronizacaoLocal: criarMetadadosPendencia(item),
            _produtosFilaLocal: Array.isArray(dados?.produtos) ? dados.produtos : []
        });
    });

    return combinados;
}

export function obterStatusVisualAtendimento(envio = {}) {
    if (envio?._statusSincronizacao === 'pendente') {
        return {
            classe: 'pendente',
            simbolo: '✓',
            titulo: 'Salvo no aparelho. Aguardando confirmação do Firebase.'
        };
    }
    return {
        classe: 'confirmado',
        simbolo: '✓✓',
        titulo: 'Dados e fotos confirmados no Firebase.'
    };
}
