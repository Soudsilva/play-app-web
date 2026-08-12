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

const ORDEM_FASES = Object.freeze({
    salvo_localmente: 0,
    id_reservado: 1,
    atendimento_pendente_confirmado: 2,
    fotos_enviando: 3,
    fotos_confirmadas: 4,
    atendimento_confirmado: 5,
    efeitos_confirmados: 6,
    rota_confirmada: 7,
    confirmacao_remota: 8,
    concluido: 9
});

function faseAtingida(item, fase) {
    const atual = ORDEM_FASES[String(item?.fase || 'salvo_localmente')] ?? 0;
    const alvo = ORDEM_FASES[String(fase || 'salvo_localmente')] ?? 0;
    return atual >= alvo;
}

function normalizarNomeProduto(valor) {
    return String(valor || '').trim().toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '');
}

export function ordenarPendenciasAtendimento(pendencias = []) {
    return [...(Array.isArray(pendencias) ? pendencias : [])].sort((a, b) => {
        const prioridadeA = a?.prioridadeEnvio === true ? 1 : 0;
        const prioridadeB = b?.prioridadeEnvio === true ? 1 : 0;
        if (prioridadeA !== prioridadeB) return prioridadeB - prioridadeA;
        return String(a?.criadoEm || '').localeCompare(String(b?.criadoEm || ''));
    });
}

export function validarConfirmacaoProdutosAtendimento(dados = {}, registros = []) {
    if (dados?.origemRegistro && dados.origemRegistro !== 'atendimento') return true;

    const esperados = new Map();
    (Array.isArray(dados?.produtos) ? dados.produtos : []).forEach(item => {
        const nome = normalizarNomeProduto(item?.nome);
        const quantidade = Number(item?.quantidade || 0);
        if (!nome || !(quantidade > 0)) return;
        esperados.set(nome, (esperados.get(nome) || 0) + quantidade);
    });
    if (esperados.size === 0) return true;

    const confirmados = new Map();
    (Array.isArray(registros) ? registros : []).forEach(item => {
        const nome = normalizarNomeProduto(item?.nome || item?.itemNome);
        const quantidade = Math.abs(Number(item?.movimento || item?.quantidade || 0));
        if (!nome || !(quantidade > 0)) return;
        confirmados.set(nome, (confirmados.get(nome) || 0) + quantidade);
    });

    for (const [nome, quantidade] of esperados) {
        if (Number(confirmados.get(nome) || 0) !== quantidade) {
            throw new Error(`Efeito do produto ainda nao confirmado no Firebase: ${nome}.`);
        }
    }
    return true;
}

function validarUrlFotoConfirmada(valor, descricao) {
    const url = String(valor || '').trim();
    if (!url || url.startsWith('data:') || url.startsWith('blob:')) {
        throw new Error(`${descricao} ainda nao foi confirmada no Firebase.`);
    }
    return url;
}

export function validarConfirmacaoAtendimentoRemoto(remoto = null, esperado = {}) {
    if (!remoto || typeof remoto !== 'object') {
        throw new Error('Atendimento ainda nao foi confirmado no Firebase.');
    }

    const atendenteEsperado = String(esperado?.atendente || '').trim();
    const clienteIdEsperado = String(esperado?.cliente?.id || '').trim();
    if (atendenteEsperado && String(remoto?.atendente || '').trim() !== atendenteEsperado) {
        throw new Error('Atendente do registro remoto nao corresponde ao atendimento local.');
    }
    if (clienteIdEsperado && String(remoto?.cliente?.id || '').trim() !== clienteIdEsperado) {
        throw new Error('Cliente do registro remoto nao corresponde ao atendimento local.');
    }

    const fichaEsperada = String(esperado?.fotos?.ficha || '').trim();
    if (fichaEsperada) {
        const fichaRemota = validarUrlFotoConfirmada(remoto?.fotos?.ficha, 'Foto da ficha');
        if (fichaRemota !== fichaEsperada) {
            throw new Error('Foto da ficha remota nao corresponde ao atendimento local.');
        }
        const fichaThumbEsperada = String(esperado?.fotos?.fichaThumb || '').trim();
        if (fichaThumbEsperada) {
            const fichaThumbRemota = validarUrlFotoConfirmada(remoto?.fotos?.fichaThumb, 'Miniatura da ficha');
            if (fichaThumbRemota !== fichaThumbEsperada) {
                throw new Error('Miniatura da ficha remota nao corresponde ao atendimento local.');
            }
        }
    }

    for (const [colecao, descricao] of [['maquinas', 'Foto da maquina'], ['pix', 'Foto do Pix']]) {
        const fotosEsperadas = Array.isArray(esperado?.fotos?.[colecao]) ? esperado.fotos[colecao] : [];
        const fotosRemotas = Array.isArray(remoto?.fotos?.[colecao]) ? remoto.fotos[colecao] : [];
        if (fotosRemotas.length < fotosEsperadas.length) {
            throw new Error(`${descricao} ainda nao foi confirmada no Firebase.`);
        }
        fotosEsperadas.forEach((fotoEsperada, indice) => {
            const urlEsperada = validarUrlFotoConfirmada(fotoEsperada?.url, descricao);
            const urlRemota = validarUrlFotoConfirmada(fotosRemotas[indice]?.url, descricao);
            if (urlRemota !== urlEsperada) {
                throw new Error(`${descricao} remota nao corresponde ao atendimento local.`);
            }
            const thumbEsperada = String(fotoEsperada?.thumbUrl || '').trim();
            if (thumbEsperada) {
                const thumbRemota = validarUrlFotoConfirmada(fotosRemotas[indice]?.thumbUrl, `Miniatura - ${descricao}`);
                if (thumbRemota !== thumbEsperada) {
                    throw new Error(`Miniatura remota nao corresponde ao atendimento local: ${descricao}.`);
                }
            }
        });
    }

    if (remoto?.fotosPendentes === true) {
        throw new Error('O Firebase ainda informa que existem fotos pendentes.');
    }
    return true;
}

export function aplicarUploadNaFotoAtendimento(dados, alvo, resultadoUpload) {
    const atualizado = clonar(dados || {});
    const resultado = normalizarResultadoUpload(resultadoUpload);
    if (!atualizado.fotos) atualizado.fotos = {};

    if (alvo?.tipo === 'ficha') {
        atualizado.fotos.ficha = resultado.url;
        atualizado.fotos.fichaThumb = resultado.thumbUrl;
        atualizado.fotos.fichaPendente = false;
        atualizado.fotos.fichaLocalId = null;
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
        thumbUrl: resultado.thumbUrl,
        uploadPendente: false,
        fotoLocalId: null
    };
    return atualizado;
}

function criarIdLocalFoto(item, alvo) {
    const operacao = String(item?.id || 'operacao').replace(/[^a-zA-Z0-9_-]/g, '_');
    const sufixo = alvo?.tipo === 'ficha' ? 'ficha' : `${alvo?.tipo || 'foto'}-${Number(alvo?.indice) || 0}`;
    return `${operacao}-${sufixo}`;
}

export function prepararAtendimentoComFotosPendentes(dados = {}, item = {}) {
    const preparado = clonar(dados || {});
    if (!preparado.fotos) preparado.fotos = {};
    const alvos = listarFotosBase64Atendimento(preparado);

    alvos.forEach(alvo => {
        const idLocal = criarIdLocalFoto(item, alvo);
        if (alvo.tipo === 'ficha') {
            preparado.fotos.ficha = null;
            preparado.fotos.fichaThumb = null;
            preparado.fotos.fichaPendente = true;
            preparado.fotos.fichaLocalId = idLocal;
            return;
        }

        const colecao = alvo.tipo === 'maquina' ? 'maquinas' : 'pix';
        preparado.fotos[colecao][alvo.indice] = {
            ...(preparado.fotos[colecao][alvo.indice] || {}),
            url: null,
            thumbUrl: null,
            uploadPendente: true,
            fotoLocalId: idLocal
        };
    });

    preparado.fotosPendentes = alvos.length > 0;
    return preparado;
}

export function criarReferenciaFotoPendente(item, atendimentoId, alvo) {
    return {
        atendimentoId,
        tipo: alvo?.tipo,
        indice: alvo?.indice,
        fotoLocalId: criarIdLocalFoto(item, alvo)
    };
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
    atualizarFotoAtendimento,
    sincronizarProdutos,
    verificarRota,
    confirmarAtendimento,
    confirmarProdutos,
    confirmarRota
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
    if (item?.registroInicialConfirmado !== true) {
        const faseAnterior = String(item?.fase || 'salvo_localmente');
        const dadosIniciais = prepararAtendimentoComFotosPendentes(dados, item);
        await salvarAtendimento(dadosIniciais, atendimentoId, { recalcularRemuneracoes: false });
        item = await atualizarItem({
            registroInicialConfirmado: true,
            fase: faseAtingida(item, 'atendimento_confirmado')
                ? faseAnterior
                : 'atendimento_pendente_confirmado'
        });
    }

    for (const alvo of listarFotosBase64Atendimento(dados)) {
        const resultado = await salvarFoto(
            alvo.base64,
            'atendimentos',
            200,
            criarNomeEstavelFoto(item, atendimentoId, alvo)
        );
        if (typeof atualizarFotoAtendimento !== 'function') {
            throw new Error('Atualizador remoto de foto pendente nao informado.');
        }
        const uploadNormalizado = normalizarResultadoUpload(resultado);
        await atualizarFotoAtendimento(
            criarReferenciaFotoPendente(item, atendimentoId, alvo),
            uploadNormalizado.url,
            uploadNormalizado.thumbUrl
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

    dados.fotosPendentes = false;

    if (!faseAtingida(item, 'fotos_confirmadas')) {
        item = await atualizarItem({ dados, fase: 'fotos_confirmadas' });
    }

    if (!faseAtingida(item, 'atendimento_confirmado')) {
        if (typeof confirmarAtendimento !== 'function') {
            throw new Error('Confirmacao remota do atendimento nao informada.');
        }
        await confirmarAtendimento(atendimentoId, dados);
        item = await atualizarItem({ dados, fase: 'atendimento_confirmado' });
    }

    if (!faseAtingida(item, 'efeitos_confirmados') && typeof sincronizarProdutos === 'function') {
        const resultadoProdutos = await sincronizarProdutos(atendimentoId, dados);
        if (typeof confirmarProdutos === 'function') {
            await confirmarProdutos(resultadoProdutos, dados);
        }
        item = await atualizarItem({ fase: 'efeitos_confirmados' });
    }

    if (!faseAtingida(item, 'rota_confirmada') && typeof verificarRota === 'function') {
        const numeroRota = String(dados?.cliente?.rota || '').trim();
        const atendente = String(dados?.atendente || '').trim();
        if (numeroRota) {
            const resultadoRota = await verificarRota(numeroRota, atendente);
            if (typeof confirmarRota === 'function') {
                await confirmarRota(numeroRota, resultadoRota);
            }
        }
        item = await atualizarItem({ fase: 'rota_confirmada' });
    }

    if (typeof confirmarAtendimento !== 'function') {
        throw new Error('Confirmacao remota do atendimento nao informada.');
    }
    await confirmarAtendimento(atendimentoId, dados);
    item = await atualizarItem({ fase: 'confirmacao_remota' });

    return { atendimentoId, dados, item };
}
