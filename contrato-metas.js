import {
    META_FATURAMENTO_PADRAO,
    PRAZO_APROVACAO_META_MS,
    calcularPercentualMeta,
    obterPeriodoMesAtualBrasilia,
    obterValorMetaFaturamento,
    propostaMetaExpirada
} from './contrato-metas-rules.js';
import {
    carregarResumoFaturamentoMensal,
    criarSolicitacaoMetaFaturamento,
    descartarSolicitacaoMetaExpirada,
    responderSolicitacaoMetaFaturamento
} from './contrato-metas-service.js';

const formatadorMoeda = new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    maximumFractionDigits: 0
});

function criarElemento(documento, tag, classe, texto = '') {
    const elemento = documento.createElement(tag);
    if (classe) elemento.className = classe;
    if (texto) elemento.textContent = texto;
    return elemento;
}

function obterNomeMes(ano, mes) {
    const texto = new Intl.DateTimeFormat('pt-BR', {
        timeZone: 'America/Sao_Paulo',
        month: 'long'
    }).format(new Date(Date.UTC(ano, mes - 1, 15, 12)));
    return texto.charAt(0).toUpperCase() + texto.slice(1);
}

function formatarExpiracao(timestamp) {
    return new Date(Number(timestamp)).toLocaleString('pt-BR', {
        timeZone: 'America/Sao_Paulo',
        day: '2-digit',
        month: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function criarModal(documento) {
    const overlay = criarElemento(documento, 'div', 'meta-modal-overlay');
    overlay.innerHTML = `
        <form class="meta-modal">
            <div class="meta-modal-titulo">Editar meta mensal</div>
            <div class="meta-modal-aviso">O novo valor precisará da aprovação de todos os sócios em até 24 horas.</div>
            <label for="metaFaturamentoNovoValor">Valor da meta</label>
            <div class="meta-modal-valor"><span>R$</span><input id="metaFaturamentoNovoValor" inputmode="numeric" maxlength="12" required></div>
            <div class="meta-modal-acoes">
                <button type="button" class="meta-modal-cancelar">Cancelar</button>
                <button type="submit" class="meta-modal-enviar">Enviar para aprovação</button>
            </div>
        </form>`;
    documento.body.append(overlay);
    return {
        overlay,
        form: overlay.querySelector('form'),
        input: overlay.querySelector('input'),
        cancelar: overlay.querySelector('.meta-modal-cancelar'),
        enviar: overlay.querySelector('.meta-modal-enviar')
    };
}

export function criarControleMetaFaturamento(opcoes) {
    const {
        card,
        dataEl,
        prazoEl,
        barraEl,
        valoresEl,
        percentualEl,
        editarEl,
        pendenciaEl,
        obterSociosAprovadores,
        obterAprovadorLogado
    } = opcoes;
    const documento = card.ownerDocument;
    const modal = criarModal(documento);
    let metaDados = {};
    let faturamentoAtual = 0;
    let faturamentoDisponivel = false;
    let carregandoFaturamento = true;
    let carregamentoEmAndamento = null;
    let temporizadorExpiracao = null;

    const alertar = mensagem => window.playAlert
        ? window.playAlert(mensagem)
        : Promise.resolve(window.alert(mensagem));

    function obterSocios() {
        return typeof obterSociosAprovadores === 'function' ? obterSociosAprovadores() : [];
    }

    function obterSocioLogadoAtivo() {
        const logado = typeof obterAprovadorLogado === 'function'
            ? obterAprovadorLogado()
            : { id: '', nome: '' };
        return obterSocios().some(socio => socio.id === logado.id) ? logado : null;
    }

    function fecharModal() {
        modal.overlay.classList.remove('aberto');
        modal.form.reset();
    }

    function abrirModal() {
        if (!obterSocioLogadoAtivo()) return alertar('Somente usuários com cargo Sócio podem alterar a meta.');
        modal.input.value = Math.round(obterValorMetaFaturamento(metaDados)).toLocaleString('pt-BR');
        modal.overlay.classList.add('aberto');
        modal.input.focus();
    }

    function criarVoto(proposta, socio) {
        const voto = proposta.aprovacoes?.[socio.id];
        const classe = voto === true ? ' meta-voto-sim' : voto === false ? ' meta-voto-nao' : '';
        const estado = voto === true ? ' concordou' : voto === false ? ' negou' : ' aguardando';
        return criarElemento(documento, 'div', `meta-voto${classe}`, `${socio.nome}${estado}`);
    }

    async function responder(decisao) {
        const socio = obterSocioLogadoAtivo();
        if (!socio) return alertar('Somente usuários com cargo Sócio podem responder.');
        try {
            const resultado = await responderSolicitacaoMetaFaturamento(socio.id, decisao);
            if (resultado.status === 'expirada' || resultado.status === 'ausente') {
                await alertar('Esta solicitação expirou e foi descartada.');
            } else if (resultado.status === 'nao_autorizado') {
                await alertar('Seu usuário não faz parte desta solicitação.');
            }
        } catch (erro) {
            await alertar(erro?.message || 'Não foi possível registrar sua resposta.');
        }
    }

    function renderizarPendencia() {
        const proposta = metaDados?.pendente;
        if (!proposta || propostaMetaExpirada(proposta)) {
            pendenciaEl.replaceChildren();
            pendenciaEl.hidden = true;
            return;
        }
        const socioAtual = obterSocioLogadoAtivo();
        const bloco = criarElemento(documento, 'div', 'meta-pendente');
        bloco.append(
            criarElemento(documento, 'div', 'meta-pendente-titulo', 'Alteração aguardando aprovação'),
            criarElemento(documento, 'div', 'meta-pendente-valores', `${formatadorMoeda.format(proposta.valorAnterior || META_FATURAMENTO_PADRAO)} → ${formatadorMoeda.format(proposta.valorNovo || 0)}`),
            criarElemento(documento, 'div', 'meta-pendente-prazo', `Expira em ${formatarExpiracao(proposta.expiraEm)}`)
        );
        const votos = criarElemento(documento, 'div', 'meta-votos');
        (proposta.aprovadores || []).forEach(socio => votos.append(criarVoto(proposta, socio)));
        bloco.append(votos);

        const ids = (proposta.aprovadores || []).map(item => item.id);
        const meuVoto = socioAtual ? proposta.aprovacoes?.[socioAtual.id] : undefined;
        if (socioAtual && ids.includes(socioAtual.id) && meuVoto == null) {
            const acoes = criarElemento(documento, 'div', 'meta-pendente-acoes');
            const concordar = criarElemento(documento, 'button', 'meta-concordar', '✓ Concordar');
            const negar = criarElemento(documento, 'button', 'meta-negar', '✕ Negar');
            concordar.type = negar.type = 'button';
            concordar.addEventListener('click', () => responder(true));
            negar.addEventListener('click', () => responder(false));
            acoes.append(concordar, negar);
            bloco.append(acoes);
        }
        pendenciaEl.replaceChildren(bloco);
        pendenciaEl.hidden = false;
    }

    function renderizar() {
        const periodo = obterPeriodoMesAtualBrasilia();
        const meta = obterValorMetaFaturamento(metaDados);
        const percentual = calcularPercentualMeta(faturamentoAtual, meta);
        dataEl.textContent = `Meta para ${obterNomeMes(periodo.ano, periodo.mes)}/${periodo.ano}`;
        prazoEl.textContent = `${String(periodo.ultimoDia).padStart(2, '0')}/${String(periodo.mes).padStart(2, '0')}/${periodo.ano}`;
        barraEl.style.width = `${Math.min(percentual, 100)}%`;
        valoresEl.textContent = carregandoFaturamento
            ? `Carregando de ${formatadorMoeda.format(meta)}`
            : faturamentoDisponivel
                ? `${formatadorMoeda.format(faturamentoAtual)} de ${formatadorMoeda.format(meta)}`
                : `Resumo indisponível de ${formatadorMoeda.format(meta)}`;
        percentualEl.textContent = carregandoFaturamento || !faturamentoDisponivel ? '—' : `${percentual}%`;
        editarEl.hidden = !obterSocioLogadoAtivo();
        editarEl.disabled = !!metaDados?.pendente && !propostaMetaExpirada(metaDados.pendente);
        renderizarPendencia();
    }

    async function recarregarFaturamento() {
        if (carregamentoEmAndamento) return carregamentoEmAndamento;
        carregandoFaturamento = true;
        renderizar();
        const periodo = obterPeriodoMesAtualBrasilia();
        carregamentoEmAndamento = (async () => {
            try {
                const competencia = `${periodo.ano}-${String(periodo.mes).padStart(2, '0')}`;
                const resumo = await carregarResumoFaturamentoMensal(competencia);
                faturamentoDisponivel = resumo.disponivel;
                faturamentoAtual = resumo.totalGeral;
            } catch (erro) {
                console.warn('Não foi possível carregar o faturamento mensal:', erro);
                faturamentoDisponivel = false;
                await alertar('Não foi possível atualizar o faturamento mensal agora.');
            } finally {
                carregandoFaturamento = false;
                carregamentoEmAndamento = null;
                renderizar();
            }
        })();
        return carregamentoEmAndamento;
    }

    function agendarExpiracao() {
        if (temporizadorExpiracao) window.clearTimeout(temporizadorExpiracao);
        const proposta = metaDados?.pendente;
        if (!proposta || propostaMetaExpirada(proposta)) return;
        const espera = Math.max(0, Number(proposta.expiraEm) - Date.now()) + 50;
        temporizadorExpiracao = window.setTimeout(() => {
            renderizar();
            descartarSolicitacaoMetaExpirada(proposta.expiraEm)
                .catch(erro => console.warn('Não foi possível descartar a meta expirada:', erro));
        }, espera);
    }

    editarEl.addEventListener('click', abrirModal);
    modal.cancelar.addEventListener('click', fecharModal);
    modal.overlay.addEventListener('click', evento => {
        if (evento.target === modal.overlay) fecharModal();
    });
    modal.input.addEventListener('input', () => {
        const numero = modal.input.value.replace(/\D/g, '').slice(0, 9);
        modal.input.value = numero ? Number(numero).toLocaleString('pt-BR') : '';
    });
    modal.form.addEventListener('submit', async evento => {
        evento.preventDefault();
        const valorNovo = Number(modal.input.value.replace(/\D/g, ''));
        const valorAtual = obterValorMetaFaturamento(metaDados);
        if (!Number.isFinite(valorNovo) || valorNovo <= 0) return alertar('Informe um valor de meta válido.');
        if (valorNovo === valorAtual) return fecharModal();
        const socio = obterSocioLogadoAtivo();
        const aprovadores = obterSocios();
        if (!socio || !aprovadores.length) return alertar('Nenhum sócio foi encontrado para aprovar esta alteração.');
        const aprovacoes = Object.fromEntries(aprovadores.map(item => [item.id, null]));
        modal.enviar.disabled = true;
        try {
            await criarSolicitacaoMetaFaturamento({
                propostoPor: socio.id,
                propostoPorNome: socio.nome,
                aprovadores,
                aprovacoes,
                valorAnterior: valorAtual,
                valorNovo,
                expiraEm: Date.now() + PRAZO_APROVACAO_META_MS
            });
            fecharModal();
            await alertar('Alteração da meta enviada para aprovação dos sócios.');
        } catch (erro) {
            await alertar(erro?.message || 'Não foi possível enviar a alteração da meta.');
        } finally {
            modal.enviar.disabled = false;
        }
    });

    return {
        atualizarDados(novosDados = {}) {
            metaDados = novosDados || {};
            renderizar();
            const proposta = metaDados?.pendente;
            if (proposta && propostaMetaExpirada(proposta)) {
                descartarSolicitacaoMetaExpirada(proposta.expiraEm)
                    .catch(erro => console.warn('Não foi possível limpar a meta expirada:', erro));
            }
            agendarExpiracao();
        },
        recarregarFaturamento,
        renderizar
    };
}
