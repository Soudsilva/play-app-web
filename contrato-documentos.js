import {
    PRAZO_APROVACAO_DOCUMENTO_MS,
    obterDocumentosExibidos,
    propostaDocumentoExpirada
} from './contrato-documentos-rules.js';
import {
    criarSolicitacaoInclusaoDocumento,
    criarSolicitacaoAtualizacaoDocumento,
    criarSolicitacaoExclusaoDocumento,
    descartarSolicitacaoDocumentoExpirada,
    obterUrlDocumento,
    responderSolicitacaoDocumento
} from './contrato-documentos-service.js';

function criarElemento(documento, tag, classe, texto = '') {
    const elemento = documento.createElement(tag);
    if (classe) elemento.className = classe;
    if (texto) elemento.textContent = texto;
    return elemento;
}

function formatarData(valor) {
    const data = new Date(valor || '');
    if (Number.isNaN(data.getTime())) return '';
    return data.toLocaleDateString('pt-BR', {
        timeZone: 'America/Sao_Paulo',
        day: '2-digit',
        month: '2-digit',
        year: 'numeric'
    });
}

function formatarPrazo(valor) {
    const data = new Date(Number(valor));
    if (Number.isNaN(data.getTime())) return 'Prazo indisponível';
    return `Até ${data.toLocaleString('pt-BR', {
        timeZone: 'America/Sao_Paulo',
        day: '2-digit',
        month: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    })}`;
}

export function criarControleDocumentos(opcoes) {
    const {
        listaContainer,
        pendentesContainer,
        botaoAdicionar,
        obterSociosAprovadores,
        obterAprovadorLogado
    } = opcoes;
    const documentoPagina = listaContainer.ownerDocument;
    const seletorArquivo = criarElemento(documentoPagina, 'input', 'documento-seletor-arquivo');
    seletorArquivo.type = 'file';
    seletorArquivo.accept = 'application/pdf,.pdf';
    seletorArquivo.hidden = true;
    documentoPagina.body.append(seletorArquivo);

    let dados = { ativos: {}, pendentes: {} };
    let documentoParaAtualizar = null;
    let modoSelecaoArquivo = '';
    let documentoEmEnvio = '';
    let salvandoNovoDocumento = false;
    let temporizadorExpiracao = null;

    const alertar = mensagem => window.playAlert
        ? window.playAlert(mensagem)
        : Promise.resolve(window.alert(mensagem));

    function obterSocios() {
        return typeof obterSociosAprovadores === 'function' ? obterSociosAprovadores() : [];
    }

    function obterSocioLogadoAtivo() {
        const atual = typeof obterAprovadorLogado === 'function' ? obterAprovadorLogado() : null;
        if (!atual?.id) return null;
        return obterSocios().some(item => item.id === atual.id) ? atual : null;
    }

    function aprovacoesIniciais(aprovadores) {
        return Object.fromEntries((aprovadores || []).map(item => [item.id, null]));
    }

    function obterMetaDocumento(item) {
        const formato = String(item.formato || 'pdf').toUpperCase();
        const data = formatarData(item.atualizadoEm);
        if (data) return `${formato} · Atualizado em ${data}`;
        return `${formato} · ${item.detalhePadrao || 'Arquivo ainda não enviado'}`;
    }

    async function abrirDocumento(item) {
        if (!item.storagePath) {
            await alertar('Este documento ainda não possui um arquivo enviado. Use Atualizar para selecionar o PDF.');
            return;
        }
        const novaAba = window.open('about:blank', '_blank');
        try {
            const url = await obterUrlDocumento(item.storagePath);
            if (novaAba) novaAba.location.replace(url);
            else window.open(url, '_blank');
        } catch (erro) {
            if (novaAba) novaAba.close();
            await alertar(erro?.message || 'Não foi possível abrir este documento.');
        }
    }

    function criarBotaoAcao(item, tipo, desabilitado) {
        const rotulo = tipo === 'atualizar' ? `Atualizar ${item.nome}` : `Excluir ${item.nome}`;
        const botao = criarElemento(documentoPagina, 'button', `doc-acao-discreta doc-acao-${tipo}`);
        botao.type = 'button';
        botao.innerHTML = tipo === 'atualizar'
            ? '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M12 20h9"></path><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L8 18l-4 1 1-4Z"></path></svg>'
            : '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M4 7h16"></path><path d="M9 7V4h6v3"></path><path d="m6.5 7 .8 13h9.4l.8-13"></path><path d="M10 11v5M14 11v5"></path></svg>';
        botao.title = rotulo;
        botao.setAttribute('aria-label', rotulo);
        botao.disabled = desabilitado;
        botao.addEventListener('click', evento => {
            evento.stopPropagation();
            evento.currentTarget.closest('details')?.removeAttribute('open');
            if (tipo === 'atualizar') selecionarAtualizacao(item);
            else solicitarExclusao(item);
        });
        return botao;
    }

    function criarMenuAcoes(item, desabilitado) {
        const menu = criarElemento(documentoPagina, 'details', 'doc-menu');
        const resumo = criarElemento(documentoPagina, 'summary', '', '⋮');
        resumo.title = 'Ações';
        resumo.setAttribute('aria-label', `Ações de ${item.nome}`);
        const opcoesMenu = criarElemento(documentoPagina, 'div', 'doc-menu-opcoes');
        opcoesMenu.append(
            criarBotaoAcao(item, 'atualizar', desabilitado),
            criarBotaoAcao(item, 'excluir', desabilitado)
        );
        menu.append(resumo, opcoesMenu);
        return menu;
    }

    function criarCardDocumento(item, podeAlterar) {
        const possuiPendencia = !!dados.pendentes?.[item.id]
            && !propostaDocumentoExpirada(dados.pendentes[item.id]);
        const card = criarElemento(documentoPagina, 'div', 'doc-item');
        card.dataset.documentoId = item.id;
        const areaAbrir = criarElemento(documentoPagina, 'div', 'doc-abrir');
        areaAbrir.setAttribute('role', 'button');
        areaAbrir.tabIndex = 0;
        areaAbrir.setAttribute('aria-label', `Abrir ${item.nome}`);
        areaAbrir.addEventListener('click', () => abrirDocumento(item));
        areaAbrir.addEventListener('keydown', evento => {
            if (evento.key !== 'Enter' && evento.key !== ' ') return;
            evento.preventDefault();
            abrirDocumento(item);
        });

        const info = criarElemento(documentoPagina, 'div', 'doc-info');
        info.append(criarElemento(documentoPagina, 'div', 'doc-name', item.nome));
        const enviando = documentoEmEnvio === item.id;
        info.append(criarElemento(
            documentoPagina,
            'div',
            enviando ? 'doc-meta doc-enviando' : 'doc-meta',
            enviando ? 'Preparando envio...' : obterMetaDocumento(item)
        ));
        areaAbrir.append(info);
        card.append(areaAbrir, criarMenuAcoes(item, !podeAlterar || possuiPendencia || enviando));
        return card;
    }

    function renderizarDocumentos() {
        const fragmento = documentoPagina.createDocumentFragment();
        const itens = obterDocumentosExibidos(dados.ativos || {});
        const podeAlterar = !!obterSocioLogadoAtivo();
        itens.forEach(item => fragmento.append(criarCardDocumento(item, podeAlterar)));
        if (!itens.length) fragmento.append(criarElemento(documentoPagina, 'div', 'loading', 'Nenhum documento ativo.'));
        listaContainer.replaceChildren(fragmento);
    }

    function criarVoto(proposta, socio) {
        const voto = proposta.aprovacoes?.[socio.id];
        const classe = voto === true ? ' documento-voto-sim' : voto === false ? ' documento-voto-nao' : '';
        const estado = voto === true ? ' concordou' : voto === false ? ' negou' : ' aguardando';
        return criarElemento(documentoPagina, 'div', `documento-voto${classe}`, `${socio.nome}${estado}`);
    }

    async function responder(documentoId, decisao, botao) {
        const socio = obterSocioLogadoAtivo();
        if (!socio) return alertar('Somente usuários com cargo Sócio podem responder.');
        botao.disabled = true;
        try {
            const resultado = await responderSolicitacaoDocumento(documentoId, socio.id, decisao);
            if (resultado.status === 'expirada' || resultado.status === 'ausente') {
                await alertar('Esta solicitação expirou e foi descartada.');
            } else if (resultado.status === 'nao_autorizado') {
                await alertar('Seu usuário não faz parte desta solicitação.');
            }
        } catch (erro) {
            console.error('Não foi possível responder à mudança do documento:', erro);
            botao.disabled = false;
            await alertar(erro?.message || 'Não foi possível registrar sua resposta.');
        }
    }

    function renderizarPendencias() {
        const fragmento = documentoPagina.createDocumentFragment();
        const entradas = Object.entries(dados.pendentes || {})
            .filter(([, proposta]) => !propostaDocumentoExpirada(proposta));
        if (entradas.length) {
            fragmento.append(criarElemento(documentoPagina, 'div', 'documentos-pendentes-titulo', 'Mudanças aguardando aprovação'));
        }
        const socioAtual = obterSocioLogadoAtivo();

        entradas.forEach(([documentoId, proposta]) => {
            const card = criarElemento(documentoPagina, 'article', 'card documento-pendente-card');
            const topo = criarElemento(documentoPagina, 'div', 'documento-pendente-topo');
            const identificacao = criarElemento(documentoPagina, 'div', '');
            const tipo = proposta.status === 'enviando'
                ? 'ARQUIVO SENDO ENVIADO'
                : proposta.acao === 'excluir'
                    ? 'EXCLUSÃO PENDENTE'
                    : proposta.acao === 'adicionar' ? 'NOVO DOCUMENTO PENDENTE' : 'ATUALIZAÇÃO PENDENTE';
            identificacao.append(
                criarElemento(documentoPagina, 'div', 'documento-pendente-tipo', tipo),
                criarElemento(documentoPagina, 'div', 'documento-pendente-nome', proposta.nome || 'Documento')
            );
            topo.append(identificacao, criarElemento(documentoPagina, 'div', 'documento-pendente-prazo', formatarPrazo(proposta.expiraEm)));
            card.append(topo);

            const detalhe = proposta.status === 'enviando'
                ? 'Aguarde a conclusão do envio antes de responder.'
                : proposta.acao === 'excluir'
                    ? 'O documento será retirado da lista somente se todos os sócios concordarem.'
                    : `Novo arquivo: ${proposta.valorNovo?.nome || proposta.nome || 'PDF selecionado'}`;
            card.append(criarElemento(documentoPagina, 'div', 'documento-pendente-detalhe', detalhe));

            const votos = criarElemento(documentoPagina, 'div', 'documento-votos');
            (proposta.aprovadores || []).forEach(socio => votos.append(criarVoto(proposta, socio)));
            card.append(votos);

            const ids = (proposta.aprovadores || []).map(item => item.id);
            const meuVoto = socioAtual ? proposta.aprovacoes?.[socioAtual.id] : undefined;
            if (proposta.status === 'pendente' && socioAtual && ids.includes(socioAtual.id) && meuVoto == null) {
                const respostas = criarElemento(documentoPagina, 'div', 'documento-respostas');
                const concordar = criarElemento(documentoPagina, 'button', 'documento-resposta documento-concordar', '✓ Concordar');
                const negar = criarElemento(documentoPagina, 'button', 'documento-resposta documento-negar', '✕ Negar');
                concordar.type = negar.type = 'button';
                concordar.addEventListener('click', () => responder(documentoId, true, concordar));
                negar.addEventListener('click', () => responder(documentoId, false, negar));
                respostas.append(concordar, negar);
                card.append(respostas);
            }
            fragmento.append(card);
        });
        pendentesContainer.replaceChildren(fragmento);
    }

    function renderizar() {
        renderizarDocumentos();
        renderizarPendencias();
        botaoAdicionar.disabled = !obterSocioLogadoAtivo() || salvandoNovoDocumento;
        botaoAdicionar.textContent = salvandoNovoDocumento ? 'Preparando arquivo...' : '+ Adicionar novo arquivo';
    }

    function selecionarAtualizacao(item) {
        if (!obterSocioLogadoAtivo()) return alertar('Somente usuários com cargo Sócio podem atualizar documentos.');
        documentoParaAtualizar = item;
        modoSelecaoArquivo = 'atualizar';
        seletorArquivo.value = '';
        seletorArquivo.click();
    }

    function selecionarInclusao() {
        if (!obterSocioLogadoAtivo()) return alertar('Somente usuários com cargo Sócio podem adicionar documentos.');
        documentoParaAtualizar = null;
        modoSelecaoArquivo = 'adicionar';
        seletorArquivo.value = '';
        seletorArquivo.click();
    }

    async function solicitarExclusao(item) {
        const socio = obterSocioLogadoAtivo();
        if (!socio) return alertar('Somente usuários com cargo Sócio podem excluir documentos.');
        const confirmou = window.playConfirm
            ? await window.playConfirm(`Enviar a exclusão de “${item.nome}” para aprovação?`, {
                primaryLabel: 'Enviar',
                secondaryLabel: 'Cancelar'
            })
            : window.confirm('Enviar esta exclusão para aprovação?');
        if (!confirmou) return;
        const aprovadores = obterSocios();
        if (!aprovadores.length) return alertar('Nenhum sócio foi encontrado para aprovar esta mudança.');
        try {
            await criarSolicitacaoExclusaoDocumento({
                documento: item,
                propostoPor: socio.id,
                propostoPorNome: socio.nome,
                aprovadores,
                aprovacoes: aprovacoesIniciais(aprovadores),
                expiraEm: Date.now() + PRAZO_APROVACAO_DOCUMENTO_MS
            });
            await alertar('Exclusão enviada para aprovação dos sócios.');
        } catch (erro) {
            await alertar(erro?.message || 'Não foi possível solicitar a exclusão.');
        }
    }

    botaoAdicionar.addEventListener('click', selecionarInclusao);

    seletorArquivo.addEventListener('change', async () => {
        const modo = modoSelecaoArquivo;
        const item = documentoParaAtualizar;
        const arquivo = seletorArquivo.files?.[0];
        documentoParaAtualizar = null;
        modoSelecaoArquivo = '';
        if (!arquivo || !['atualizar', 'adicionar'].includes(modo)) return;
        if (modo === 'atualizar' && !item) return;
        const socio = obterSocioLogadoAtivo();
        const aprovadores = obterSocios();
        if (!socio || !aprovadores.length) return alertar('Nenhum sócio foi encontrado para aprovar esta mudança.');
        if (modo === 'atualizar') {
            documentoEmEnvio = item.id;
            renderizarDocumentos();
        } else {
            salvandoNovoDocumento = true;
            renderizar();
        }
        try {
            const dadosEnvio = {
                arquivo,
                propostoPor: socio.id,
                propostoPorNome: socio.nome,
                aprovadores,
                aprovacoes: aprovacoesIniciais(aprovadores),
                expiraEm: Date.now() + PRAZO_APROVACAO_DOCUMENTO_MS,
                aoProgresso: percentual => {
                    if (modo === 'atualizar') {
                        const meta = listaContainer.querySelector(`[data-documento-id="${CSS.escape(item.id)}"] .doc-meta`);
                        if (meta) meta.textContent = `Enviando PDF: ${percentual}%`;
                    } else {
                        botaoAdicionar.textContent = `Enviando PDF: ${percentual}%`;
                    }
                }
            };
            if (modo === 'atualizar') {
                await criarSolicitacaoAtualizacaoDocumento({ ...dadosEnvio, documento: item });
                await alertar('Atualização enviada para aprovação dos sócios.');
            } else {
                await criarSolicitacaoInclusaoDocumento(dadosEnvio);
                await alertar('Novo arquivo enviado para aprovação dos sócios.');
            }
        } catch (erro) {
            console.error('Não foi possível enviar o documento:', erro);
            await alertar(erro?.message || 'Não foi possível enviar o documento.');
        } finally {
            documentoEmEnvio = '';
            salvandoNovoDocumento = false;
            renderizar();
        }
    });

    async function descartarExpiradas() {
        const expiradas = Object.entries(dados.pendentes || {})
            .filter(([, proposta]) => propostaDocumentoExpirada(proposta));
        await Promise.all(expiradas.map(([id, proposta]) =>
            descartarSolicitacaoDocumentoExpirada(id, proposta.expiraEm)
        ));
    }

    function agendarExpiracao() {
        if (temporizadorExpiracao) window.clearTimeout(temporizadorExpiracao);
        const agora = Date.now();
        const prazos = Object.values(dados.pendentes || {})
            .map(proposta => Number(proposta?.expiraEm))
            .filter(prazo => Number.isFinite(prazo) && prazo > agora);
        if (!prazos.length) return;
        const espera = Math.min(2147483647, Math.max(0, Math.min(...prazos) - agora) + 50);
        temporizadorExpiracao = window.setTimeout(() => {
            descartarExpiradas().catch(erro => console.warn('Não foi possível limpar documentos expirados:', erro));
        }, espera);
    }

    return {
        atualizar(novosDados = {}) {
            dados = {
                ativos: novosDados?.ativos || {},
                pendentes: novosDados?.pendentes || {}
            };
            renderizar();
            descartarExpiradas().catch(erro => console.warn('Não foi possível limpar documentos expirados:', erro));
            agendarExpiracao();
        },
        renderizar
    };
}
