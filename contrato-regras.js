import {
    PRAZO_APROVACAO_REGRA_MS,
    obterAprovacoesIniciais,
    obterRegrasExibidas,
    propostaRegraExpirada
} from './contrato-regras-rules.js';
import {
    criarSolicitacaoRegra,
    descartarSolicitacaoRegraExpirada,
    responderSolicitacaoRegra
} from './contrato-regras-service.js';

function criarElemento(documento, tag, classe, texto = '') {
    const elemento = documento.createElement(tag);
    if (classe) elemento.className = classe;
    if (texto) elemento.textContent = texto;
    return elemento;
}

function horaExpiracao(timestamp) {
    return new Date(Number(timestamp)).toLocaleTimeString('pt-BR', {
        timeZone: 'America/Sao_Paulo',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function criarModalEdicao(documento) {
    const overlay = criarElemento(documento, 'div', 'regra-modal-overlay');
    overlay.innerHTML = `
        <form class="regra-modal">
            <div class="regra-modal-titulo">Editar regra</div>
            <div class="regra-modal-aviso">A mudança precisará da aprovação de todos os sócios em até 24 horas.</div>
            <div class="regra-modal-campo">
                <label for="regraEdicaoTitulo">Nome da regra</label>
                <input id="regraEdicaoTitulo" maxlength="100" required>
            </div>
            <div class="regra-modal-campo">
                <label for="regraEdicaoDescricao">Descrição</label>
                <textarea id="regraEdicaoDescricao" maxlength="1000" required></textarea>
            </div>
            <div class="regra-modal-acoes">
                <button type="button" class="regra-modal-cancelar">Cancelar</button>
                <button type="submit" class="regra-modal-enviar">Enviar para aprovação</button>
            </div>
        </form>`;
    documento.body.append(overlay);
    return {
        overlay,
        form: overlay.querySelector('form'),
        cabecalho: overlay.querySelector('.regra-modal-titulo'),
        titulo: overlay.querySelector('#regraEdicaoTitulo'),
        descricao: overlay.querySelector('#regraEdicaoDescricao'),
        cancelar: overlay.querySelector('.regra-modal-cancelar'),
        enviar: overlay.querySelector('.regra-modal-enviar')
    };
}

export function criarControleRegrasEmpresa(opcoes) {
    const { listaContainer, pendentesContainer, obterSociosAprovadores, obterAprovadorLogado } = opcoes;
    const documento = listaContainer.ownerDocument;
    const modal = criarModalEdicao(documento);
    let dados = { ativos: {}, pendentes: {} };
    let regraEmEdicao = null;
    let modoModal = 'editar';
    let temporizadorExpiracao = null;

    const alertar = mensagem => window.playAlert
        ? window.playAlert(mensagem)
        : Promise.resolve(window.alert(mensagem));

    function obterSocios() {
        return typeof obterSociosAprovadores === 'function' ? obterSociosAprovadores() : [];
    }

    function obterSocioLogado() {
        return typeof obterAprovadorLogado === 'function' ? obterAprovadorLogado() : { id: '', nome: '' };
    }

    function socioLogadoAtivo() {
        const logado = obterSocioLogado();
        return obterSocios().some(socio => socio.id === logado.id) ? logado : null;
    }

    async function criarProposta(regra, acao, valorNovo = null) {
        const socioLogado = socioLogadoAtivo();
        if (!socioLogado) {
            await alertar('Somente usuários com cargo Sócio podem solicitar mudanças nas regras.');
            return false;
        }
        const aprovadores = obterSocios();
        if (!aprovadores.length) {
            await alertar('Nenhum sócio foi encontrado para aprovar esta mudança.');
            return false;
        }

        try {
            await criarSolicitacaoRegra({
                regraId: regra.id,
                acao,
                titulo: regra.titulo,
                propostoPor: socioLogado.id,
                propostoPorNome: socioLogado.nome,
                aprovadores,
                aprovacoes: obterAprovacoesIniciais(aprovadores),
                valorAnterior: { titulo: regra.titulo, descricao: regra.descricao },
                valorNovo,
                expiraEm: Date.now() + PRAZO_APROVACAO_REGRA_MS
            });
            const mensagens = {
                criar: 'Nova regra enviada para aprovação dos sócios.',
                editar: 'Alteração enviada para aprovação dos sócios.',
                excluir: 'Exclusão enviada para aprovação dos sócios.'
            };
            await alertar(mensagens[acao] || 'Mudança enviada para aprovação dos sócios.');
            return true;
        } catch (erro) {
            await alertar(erro?.message || 'Não foi possível enviar a mudança para aprovação.');
            return false;
        }
    }

    function fecharModal() {
        modal.overlay.classList.remove('aberto');
        regraEmEdicao = null;
        modoModal = 'editar';
        modal.form.reset();
    }

    function abrirModal(regra) {
        modoModal = 'editar';
        regraEmEdicao = regra;
        modal.cabecalho.textContent = 'Editar regra';
        modal.titulo.value = regra.titulo;
        modal.descricao.value = regra.descricao;
        modal.overlay.classList.add('aberto');
        modal.titulo.focus();
    }

    function abrirModalNovaRegra() {
        modoModal = 'criar';
        regraEmEdicao = null;
        modal.form.reset();
        modal.cabecalho.textContent = 'Adicionar nova regra';
        modal.overlay.classList.add('aberto');
        modal.titulo.focus();
    }

    async function solicitarExclusao(regra) {
        const confirmou = window.playConfirm
            ? await window.playConfirm(`Enviar a exclusão da regra “${regra.titulo}” para aprovação?`, {
                primaryLabel: 'Enviar',
                secondaryLabel: 'Cancelar'
            })
            : window.confirm('Enviar esta exclusão para aprovação?');
        if (confirmou) await criarProposta(regra, 'excluir');
    }

    function criarBotaoAcao(classe, simbolo, descricao, aoClicar, desabilitado) {
        const botao = criarElemento(documento, 'button', `regra-acao ${classe}`, simbolo);
        botao.type = 'button';
        botao.title = descricao;
        botao.setAttribute('aria-label', descricao);
        botao.disabled = desabilitado;
        if (!desabilitado) botao.addEventListener('click', aoClicar);
        return botao;
    }

    function renderizarRegras() {
        const fragmento = documento.createDocumentFragment();
        const regras = obterRegrasExibidas(dados.ativos);
        const pendentesAtivas = dados.pendentes || {};
        const podeAlterar = !!socioLogadoAtivo();

        regras.forEach((regra, indice) => {
            const possuiPendencia = !!pendentesAtivas[regra.id] && !propostaRegraExpirada(pendentesAtivas[regra.id]);
            const card = criarElemento(documento, 'article', 'card regra-card');
            const item = criarElemento(documento, 'div', 'regra-item');
            const conteudo = criarElemento(documento, 'div', 'regra-conteudo');
            const tituloLinha = criarElemento(documento, 'div', 'regra-titulo-linha');
            tituloLinha.append(
                criarElemento(documento, 'div', 'regra-num', String(indice + 1)),
                criarElemento(documento, 'strong', 'regra-titulo', regra.titulo)
            );
            conteudo.append(
                tituloLinha,
                criarElemento(documento, 'div', 'regra-descricao', regra.descricao)
            );
            if (possuiPendencia) conteudo.append(criarElemento(documento, 'div', 'regra-aguardando', 'Aguardando aprovação'));

            const acoes = criarElemento(documento, 'div', 'regra-acoes');
            const desabilitado = !podeAlterar || possuiPendencia;
            acoes.append(
                criarBotaoAcao('regra-acao-editar', '✎', `Editar ${regra.titulo}`, () => abrirModal(regra), desabilitado),
                criarBotaoAcao('regra-acao-excluir', '×', `Excluir ${regra.titulo}`, () => solicitarExclusao(regra), desabilitado)
            );
            item.append(conteudo, acoes);
            card.append(item);
            fragmento.append(card);
        });

        if (!regras.length) fragmento.append(criarElemento(documento, 'div', 'loading', 'Nenhuma regra ativa.'));
        const botaoAdicionar = criarElemento(documento, 'button', 'regra-adicionar', '+ Adicionar nova regra');
        botaoAdicionar.type = 'button';
        botaoAdicionar.disabled = !podeAlterar;
        botaoAdicionar.addEventListener('click', abrirModalNovaRegra);
        fragmento.append(botaoAdicionar);
        listaContainer.replaceChildren(fragmento);
    }

    function criarVoto(proposta, socio) {
        const voto = proposta.aprovacoes?.[socio.id];
        const classe = voto === true ? ' regra-voto-sim' : voto === false ? ' regra-voto-nao' : '';
        const estado = voto === true ? ' concordou' : voto === false ? ' negou' : ' aguardando';
        return criarElemento(documento, 'div', `regra-voto${classe}`, `${socio.nome}${estado}`);
    }

    async function responder(regraId, decisao) {
        const socio = socioLogadoAtivo();
        if (!socio) return alertar('Somente usuários com cargo Sócio podem responder.');
        try {
            const resultado = await responderSolicitacaoRegra(regraId, socio.id, decisao);
            if (resultado.status === 'expirada' || resultado.status === 'ausente') {
                await alertar('Esta solicitação expirou e foi descartada.');
            } else if (resultado.status === 'nao_autorizado') {
                await alertar('Seu usuário não faz parte desta solicitação.');
            }
        } catch (erro) {
            await alertar(erro?.message || 'Não foi possível registrar sua resposta.');
        }
    }

    function renderizarPendencias() {
        const fragmento = documento.createDocumentFragment();
        const agora = Date.now();
        const entries = Object.entries(dados.pendentes || {}).filter(([, proposta]) => !propostaRegraExpirada(proposta, agora));
        if (entries.length) fragmento.append(criarElemento(documento, 'div', 'regras-pendentes-titulo', 'Mudanças aguardando aprovação'));

        const socioAtual = socioLogadoAtivo();
        entries.forEach(([regraId, proposta]) => {
            const card = criarElemento(documento, 'article', 'card regra-pendente-card');
            const topo = criarElemento(documento, 'div', 'regra-pendente-topo');
            const identificacao = criarElemento(documento, 'div', '');
            const rotulosPendencia = {
                criar: 'NOVA REGRA PENDENTE',
                editar: 'EDIÇÃO PENDENTE',
                excluir: 'EXCLUSÃO PENDENTE'
            };
            identificacao.append(
                criarElemento(documento, 'div', 'regra-pendente-tipo', rotulosPendencia[proposta.acao] || 'MUDANÇA PENDENTE'),
                criarElemento(documento, 'div', 'regra-pendente-nome', proposta.titulo || 'Regra')
            );
            topo.append(identificacao, criarElemento(documento, 'div', 'regra-pendente-prazo', `Expira às ${horaExpiracao(proposta.expiraEm)}`));
            card.append(topo);

            const comparacao = criarElemento(documento, 'div', 'regra-pendente-comparacao');
            if (proposta.acao === 'editar') {
                const antes = criarElemento(documento, 'div', 'regra-pendente-bloco');
                antes.append(
                    criarElemento(documento, 'div', 'regra-pendente-label', 'ANTES'),
                    criarElemento(documento, 'div', 'regra-pendente-texto', `${proposta.valorAnterior?.titulo || ''} — ${proposta.valorAnterior?.descricao || ''}`)
                );
                const depois = criarElemento(documento, 'div', 'regra-pendente-bloco');
                depois.append(
                    criarElemento(documento, 'div', 'regra-pendente-label', 'DEPOIS'),
                    criarElemento(documento, 'div', 'regra-pendente-texto', `${proposta.valorNovo?.titulo || ''} — ${proposta.valorNovo?.descricao || ''}`)
                );
                comparacao.append(antes, depois);
            } else if (proposta.acao === 'criar') {
                const novaRegra = criarElemento(documento, 'div', 'regra-pendente-bloco');
                novaRegra.append(
                    criarElemento(documento, 'div', 'regra-pendente-label', 'NOVA REGRA'),
                    criarElemento(documento, 'div', 'regra-pendente-texto', `${proposta.valorNovo?.titulo || ''} — ${proposta.valorNovo?.descricao || ''}`)
                );
                comparacao.append(novaRegra);
            } else {
                const aviso = criarElemento(documento, 'div', 'regra-pendente-bloco');
                aviso.append(
                    criarElemento(documento, 'div', 'regra-pendente-label', 'REGRA A EXCLUIR'),
                    criarElemento(documento, 'div', 'regra-pendente-texto', proposta.valorAnterior?.descricao || '')
                );
                comparacao.append(aviso);
            }
            card.append(comparacao);

            const votos = criarElemento(documento, 'div', 'regra-votos');
            (proposta.aprovadores || []).forEach(socio => votos.append(criarVoto(proposta, socio)));
            card.append(votos);

            const idsAprovadores = (proposta.aprovadores || []).map(item => item.id);
            const meuVoto = socioAtual ? proposta.aprovacoes?.[socioAtual.id] : undefined;
            if (socioAtual && idsAprovadores.includes(socioAtual.id) && meuVoto == null) {
                const respostas = criarElemento(documento, 'div', 'regra-respostas');
                const concordar = criarElemento(documento, 'button', 'regra-resposta regra-concordar', '✓ Concordar');
                const negar = criarElemento(documento, 'button', 'regra-resposta regra-negar', '✕ Negar');
                concordar.type = negar.type = 'button';
                concordar.addEventListener('click', () => responder(regraId, true));
                negar.addEventListener('click', () => responder(regraId, false));
                respostas.append(concordar, negar);
                card.append(respostas);
            }
            fragmento.append(card);
        });
        pendentesContainer.replaceChildren(fragmento);
    }

    async function descartarExpiradas() {
        const expiradas = Object.entries(dados.pendentes || {})
            .filter(([, proposta]) => propostaRegraExpirada(proposta));
        await Promise.all(expiradas.map(([regraId, proposta]) =>
            descartarSolicitacaoRegraExpirada(regraId, proposta.expiraEm)
        ));
    }

    function agendarExpiracao() {
        if (temporizadorExpiracao) window.clearTimeout(temporizadorExpiracao);
        const agora = Date.now();
        const prazos = Object.values(dados.pendentes || {})
            .map(proposta => Number(proposta?.expiraEm))
            .filter(prazo => Number.isFinite(prazo) && prazo > agora);
        if (!prazos.length) return;
        const espera = Math.max(0, Math.min(...prazos) - agora) + 50;
        temporizadorExpiracao = window.setTimeout(() => {
            renderizarPendencias();
            renderizarRegras();
            descartarExpiradas().catch(erro => console.warn('Não foi possível descartar alteração de regra expirada:', erro));
            agendarExpiracao();
        }, espera);
    }

    function renderizar() {
        renderizarPendencias();
        renderizarRegras();
        descartarExpiradas().catch(erro => console.warn('Não foi possível limpar alterações de regras expiradas:', erro));
        agendarExpiracao();
    }

    modal.cancelar.addEventListener('click', fecharModal);
    modal.overlay.addEventListener('click', evento => {
        if (evento.target === modal.overlay) fecharModal();
    });
    modal.form.addEventListener('submit', async evento => {
        evento.preventDefault();
        const titulo = modal.titulo.value.trim();
        const descricao = modal.descricao.value.trim();
        if (!titulo || !descricao) return alertar('Preencha o nome e a descrição da regra.');
        if (modoModal === 'criar') {
            const agora = Date.now();
            const regraNova = {
                id: `regra_${agora}_${Math.random().toString(36).slice(2, 8)}`,
                titulo,
                descricao
            };
            modal.enviar.disabled = true;
            const enviado = await criarProposta(regraNova, 'criar', {
                titulo,
                descricao,
                criadoEmMs: agora
            });
            modal.enviar.disabled = false;
            if (enviado) fecharModal();
            return;
        }
        if (!regraEmEdicao) return;
        if (titulo === regraEmEdicao.titulo && descricao === regraEmEdicao.descricao) {
            fecharModal();
            return;
        }
        modal.enviar.disabled = true;
        const enviado = await criarProposta(regraEmEdicao, 'editar', { titulo, descricao });
        modal.enviar.disabled = false;
        if (enviado) fecharModal();
    });

    return {
        atualizar(novosDados = {}) {
            dados = {
                ativos: novosDados.ativos || {},
                pendentes: novosDados.pendentes || {}
            };
            renderizar();
        },
        renderizar
    };
}
