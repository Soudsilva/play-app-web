function clonar(valor) {
    return JSON.parse(JSON.stringify(valor));
}

function normalizarResultadoUpload(resultado) {
    if (typeof resultado === 'string') {
        return { url: resultado, thumbUrl: resultado };
    }
    const url = String(resultado?.url || '').trim();
    const thumbUrl = String(resultado?.thumbUrl || url).trim();
    if (!url) throw new Error('Upload da foto nao retornou URL.');
    return { url, thumbUrl };
}

export function listarFotosBase64Atendimento(dados = {}) {
    const alvos = [];
    if (String(dados?.fotos?.ficha || '').startsWith('data:')) {
        alvos.push({ tipo: 'ficha', indice: null, base64: dados.fotos.ficha });
    }

    (Array.isArray(dados?.fotos?.maquinas) ? dados.fotos.maquinas : []).forEach((foto, indice) => {
        if (String(foto?.url || '').startsWith('data:')) {
            alvos.push({ tipo: 'maquina', indice, base64: foto.url });
        }
    });

    (Array.isArray(dados?.fotos?.pix) ? dados.fotos.pix : []).forEach((foto, indice) => {
        if (String(foto?.url || '').startsWith('data:')) {
            alvos.push({ tipo: 'pix', indice, base64: foto.url });
        }
    });
    return alvos;
}

export function aplicarUploadNaFotoAtendimento(dados, alvo, resultadoUpload) {
    const atualizado = clonar(dados || {});
    const resultado = normalizarResultadoUpload(resultadoUpload);
    if (!atualizado.fotos) atualizado.fotos = {};

    if (alvo?.tipo === 'ficha') {
        atualizado.fotos.ficha = resultado.url;
        atualizado.fotos.fichaThumb = resultado.thumbUrl;
        return atualizado;
    }

    const indice = Number(alvo?.indice);
    const colecao = alvo?.tipo === 'maquina' ? 'maquinas' : alvo?.tipo === 'pix' ? 'pix' : '';
    if (!colecao || !Number.isInteger(indice) || indice < 0) {
        throw new Error('Destino de foto pendente invalido.');
    }
    if (!Array.isArray(atualizado.fotos[colecao])) atualizado.fotos[colecao] = [];
    atualizado.fotos[colecao][indice] = {
        ...(atualizado.fotos[colecao][indice] || {}),
        url: resultado.url,
        thumbUrl: resultado.thumbUrl
    };
    return atualizado;
}

export function criarNomeEstavelFoto(item, atendimentoId, alvo) {
    const operacao = String(item?.id || 'operacao').replace(/[^a-zA-Z0-9_-]/g, '_');
    const servidor = String(atendimentoId || 'atendimento').replace(/[^a-zA-Z0-9_-]/g, '_');
    const sufixo = alvo?.tipo === 'ficha' ? 'ficha' : `${alvo?.tipo || 'foto'}-${Number(alvo?.indice) || 0}`;
    return `${servidor}-${operacao}-${sufixo}`;
}

export async function processarAtendimentoPendente({
    item,
    atualizarItem,
    gerarAtendimentoId,
    salvarFoto,
    salvarAtendimento,
    sincronizarProdutos,
    verificarRota
}) {
    if (!item?.id) throw new Error('Item da fila sem id local.');
    if (typeof atualizarItem !== 'function') throw new Error('Atualizador da fila nao informado.');
    if (typeof salvarFoto !== 'function' || typeof salvarAtendimento !== 'function') {
        throw new Error('Servicos obrigatorios de sincronizacao nao informados.');
    }

    let atendimentoId = String(item?.atendimentoServidorId || '').trim();
    if (!atendimentoId) {
        if (typeof gerarAtendimentoId !== 'function') {
            throw new Error('Nao foi possivel reservar um ID estavel para o atendimento.');
        }
        atendimentoId = String(await gerarAtendimentoId()).trim();
        if (!atendimentoId) throw new Error('ID estavel do atendimento nao foi gerado.');
        item = await atualizarItem({
            atendimentoServidorId: atendimentoId,
            fase: 'id_reservado'
        });
    }

    let dados = clonar(item?.dados || {});
    for (const alvo of listarFotosBase64Atendimento(dados)) {
        const resultado = await salvarFoto(
            alvo.base64,
            'atendimentos',
            200,
            criarNomeEstavelFoto(item, atendimentoId, alvo)
        );
        dados = aplicarUploadNaFotoAtendimento(dados, alvo, resultado);
        item = await atualizarItem({
            dados,
            fase: 'fotos_enviando'
        });
    }

    if (listarFotosBase64Atendimento(dados).length > 0) {
        throw new Error('Ainda existem fotos locais sem confirmacao de upload.');
    }

    await salvarAtendimento(dados, atendimentoId);
    item = await atualizarItem({ dados, fase: 'atendimento_confirmado' });

    if (typeof sincronizarProdutos === 'function') {
        await sincronizarProdutos(atendimentoId, dados);
        item = await atualizarItem({ fase: 'efeitos_confirmados' });
    }

    if (typeof verificarRota === 'function') {
        const numeroRota = String(dados?.cliente?.rota || '').trim();
        const atendente = String(dados?.atendente || '').trim();
        if (numeroRota) await verificarRota(numeroRota, atendente);
    }

    return { atendimentoId, dados, item };
}
