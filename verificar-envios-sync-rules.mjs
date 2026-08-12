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

function limitarPercentual(valor) {
    const numero = Number(valor);
    if (!Number.isFinite(numero)) return null;
    return Math.max(0, Math.min(100, Math.round(numero)));
}

function calcularProgressoFotosLocal(dados = {}) {
    const origens = [];
    if (String(dados?.fotos?.ficha || '').trim()) origens.push(String(dados.fotos.ficha));
    (Array.isArray(dados?.fotos?.maquinas) ? dados.fotos.maquinas : []).forEach(foto => {
        if (String(foto?.url || '').trim()) origens.push(String(foto.url));
    });
    (Array.isArray(dados?.fotos?.pix) ? dados.fotos.pix : []).forEach(foto => {
        if (String(foto?.url || '').trim()) origens.push(String(foto.url));
    });
    if (origens.length === 0) return 0;
    const confirmadas = origens.filter(origem => !origem.startsWith('data:') && !origem.startsWith('blob:')).length;
    return Math.max(0, Math.min(70, Math.round((confirmadas / origens.length) * 70)));
}

function obterProgressoPendencia(item = {}) {
    const progressoRegistrado = limitarPercentual(item?.progressoSincronizacao);
    if (progressoRegistrado !== null) return progressoRegistrado;

    const fase = String(item?.fase || 'salvo_localmente').trim();
    if (fase === 'concluido' || item?.estado === 'enviado') return 100;
    if (fase === 'rota_confirmada') return 99;
    if (fase === 'efeitos_confirmados') return 95;
    if (fase === 'atendimento_confirmado') return 85;
    if (fase === 'fotos_enviando') return calcularProgressoFotosLocal(item?.dados || {});
    return 0;
}

function criarMetadadosPendencia(item) {
    return {
        id: String(item?.id || ''),
        estado: String(item?.estado || 'pendente'),
        fase: String(item?.fase || 'salvo_localmente'),
        progresso: obterProgressoPendencia(item),
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
        const percentual = limitarPercentual(envio?._sincronizacaoLocal?.progresso) ?? 0;
        return {
            classe: 'pendente',
            simbolo: `${percentual}%`,
            percentual,
            titulo: percentual > 0
                ? `Envio em andamento: ${percentual}% confirmado.`
                : 'Salvo no aparelho. Nenhum dado confirmado no Firebase.'
        };
    }
    return {
        classe: 'confirmado',
        simbolo: '✓✓',
        percentual: 100,
        titulo: 'Dados e fotos confirmados no Firebase.'
    };
}
