import {
    PRAZO_APROVACAO_META_MS,
    propostaMetaExpirada
} from './contrato-metas-rules.js';
import {
    META_RESERVA_PADRAO,
    calcularPercentualReserva,
    obterMetaReserva
} from './contrato-reserva-rules.js';
import {
    carregarMovimentacoesReserva,
    criarSolicitacaoMetaReserva,
    descartarSolicitacaoMetaReservaExpirada,
    escutarSaldoReserva,
    registrarMovimentoReserva,
    responderSolicitacaoMetaReserva
} from './contrato-reserva-service.js';

const formatadorMoeda = new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    maximumFractionDigits: 0
});

const MOVIMENTOS_POR_PAGINA = 3;

function criarElemento(documento, tag, classe, texto = '') {
    const elemento = documento.createElement(tag);
    if (classe) elemento.className = classe;
    if (texto) elemento.textContent = texto;
    return elemento;
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

function obterDataISO(timestamp = Date.now()) {
    const data = new Date(Number(timestamp));
    if (Number.isNaN(data.getTime())) return '';
    const partes = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'America/Sao_Paulo',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).formatToParts(data);
    const valores = Object.fromEntries(partes.map(item => [item.type, item.value]));
    return `${valores.year}-${valores.month}-${valores.day}`;
}

function formatarHora(timestamp) {
    const data = new Date(Number(timestamp));
    if (Number.isNaN(data.getTime())) return '';
    return data.toLocaleTimeString('pt-BR', {
        timeZone: 'America/Sao_Paulo',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function rotuloData(dataISO) {
    if (!dataISO) return 'Sem data';
    const hoje = obterDataISO();
    const ontem = obterDataISO(Date.now() - 86400000);
    if (dataISO === hoje) return 'Hoje';
    if (dataISO === ontem) return 'Ontem';
    const [ano, mes, dia] = dataISO.split('-');
    return `${dia}/${mes}/${ano}`;
}

function criarModalMeta(documento) {
    const overlay = criarElemento(documento, 'div', 'meta-modal-overlay');
    overlay.innerHTML = `
        <form class="meta-modal">
            <div class="meta-modal-titulo">Editar meta da reserva</div>
            <div class="meta-modal-aviso">O novo valor precisará da aprovação de todos os sócios em até 24 horas.</div>
            <label for="metaReservaNovoValor">Valor da meta</label>
            <div class="meta-modal-valor"><span>R$</span><input id="metaReservaNovoValor" inputmode="numeric" maxlength="12" required></div>
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

export function criarControleReservaEmergencia(opcoes) {
    const {
        card,
        resumoEl,
        prazoEl,
        barraEl,
        valoresEl,
        percentualEl,
        editarEl,
        pendenciaEl,
        detalhesEl,
        obterSociosAprovadores,
        obterAprovadorLogado
    } = opcoes;
    const documento = card.ownerDocument;
    const modal = criarModalMeta(documento);
    let metaDados = {};
    let saldo = 0;
    let expandido = false;
    let carregandoMovimentacoes = false;
    let movimentosCarregados = false;
    let movimentos = [];
    let quantidadeMovimentosVisiveis = MOVIMENTOS_POR_PAGINA;
    let ignorarRolagemAte = 0;
    let erroMovimentacoes = '';
    let tipoMovimento = '';
    let salvandoMovimento = false;
    let temporizadorExpiracao = null;

    detalhesEl.innerHTML = `
        <form class="reserva-form" hidden>
            <div class="reserva-form-titulo"></div>
            <label class="reserva-campo-label" for="reservaValor">Valor</label>
            <div class="reserva-valor-wrap"><span>R$</span><input id="reservaValor" inputmode="numeric" maxlength="12" required></div>
            <label class="reserva-campo-label" for="reservaMotivo">Motivo</label>
            <input id="reservaMotivo" class="reserva-motivo" maxlength="160">
            <div class="reserva-form-acoes">
                <button type="button" class="reserva-cancelar">Cancelar</button>
                <button type="submit" class="reserva-confirmar">Confirmar</button>
            </div>
        </form>
        <div class="reserva-feedback" aria-live="polite"></div>
        <div class="reserva-extrato" aria-live="polite"></div>`;

    const adicionarEl = card.querySelector('.reserva-adicionar');
    const retirarEl = card.querySelector('.reserva-retirar');
    const rotuloExtratoEl = card.querySelector('#reservaRotuloExtrato');
    const formEl = detalhesEl.querySelector('.reserva-form');
    const formTituloEl = detalhesEl.querySelector('.reserva-form-titulo');
    const valorEl = detalhesEl.querySelector('#reservaValor');
    const motivoLabelEl = detalhesEl.querySelector('label[for="reservaMotivo"]');
    const motivoEl = detalhesEl.querySelector('#reservaMotivo');
    const cancelarMovimentoEl = detalhesEl.querySelector('.reserva-cancelar');
    const confirmarMovimentoEl = detalhesEl.querySelector('.reserva-confirmar');
    const feedbackEl = detalhesEl.querySelector('.reserva-feedback');
    const extratoEl = detalhesEl.querySelector('.reserva-extrato');

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

    function fecharModalMeta() {
        modal.overlay.classList.remove('aberto');
        modal.form.reset();
    }

    function abrirModalMeta() {
        if (!obterSocioLogadoAtivo()) return alertar('Somente usuários com cargo Sócio podem alterar a meta.');
        modal.input.value = Math.round(obterMetaReserva(metaDados)).toLocaleString('pt-BR');
        modal.overlay.classList.add('aberto');
        modal.input.focus();
    }

    function criarVoto(proposta, socio) {
        const voto = proposta.aprovacoes?.[socio.id];
        const classe = voto === true ? ' meta-voto-sim' : voto === false ? ' meta-voto-nao' : '';
        const estado = voto === true ? ' concordou' : voto === false ? ' negou' : ' aguardando';
        return criarElemento(documento, 'div', `meta-voto${classe}`, `${socio.nome}${estado}`);
    }

    async function responderMeta(decisao) {
        const socio = obterSocioLogadoAtivo();
        if (!socio) return alertar('Somente usuários com cargo Sócio podem responder.');
        try {
            const resultado = await responderSolicitacaoMetaReserva(socio.id, decisao);
            if (resultado.status === 'expirada' || resultado.status === 'ausente') {
                await alertar('Esta solicitação expirou e foi descartada.');
            } else if (resultado.status === 'nao_autorizado') {
                await alertar('Seu usuário não faz parte desta solicitação.');
            }
        } catch (erro) {
            await alertar(erro?.message || 'Não foi possível registrar sua resposta.');
        }
    }

    function renderizarPendenciaMeta() {
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
            criarElemento(documento, 'div', 'meta-pendente-valores', `${formatadorMoeda.format(proposta.valorAnterior || META_RESERVA_PADRAO)} → ${formatadorMoeda.format(proposta.valorNovo || 0)}`),
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
            concordar.addEventListener('click', () => responderMeta(true));
            negar.addEventListener('click', () => responderMeta(false));
            acoes.append(concordar, negar);
            bloco.append(acoes);
        }
        pendenciaEl.replaceChildren(bloco);
        pendenciaEl.hidden = false;
    }

    function renderizarResumo() {
        const ano = new Intl.DateTimeFormat('en', {
            timeZone: 'America/Sao_Paulo',
            year: 'numeric'
        }).format(new Date());
        const meta = obterMetaReserva(metaDados);
        const percentual = calcularPercentualReserva(saldo, meta);
        prazoEl.textContent = `31/12/${ano}`;
        barraEl.style.width = `${Math.min(percentual, 100)}%`;
        valoresEl.textContent = `${formatadorMoeda.format(saldo)} de ${formatadorMoeda.format(meta)}`;
        percentualEl.textContent = `${percentual}%`;
        editarEl.hidden = !obterSocioLogadoAtivo();
        editarEl.disabled = !!metaDados?.pendente && !propostaMetaExpirada(metaDados.pendente);
        resumoEl.setAttribute('aria-expanded', String(expandido));
        rotuloExtratoEl.hidden = !expandido;
        detalhesEl.hidden = !expandido;
        renderizarPendenciaMeta();
        renderizarControlesMovimento();
    }

    function renderizarControlesMovimento() {
        const podeMovimentar = !!obterSocioLogadoAtivo();
        adicionarEl.hidden = !podeMovimentar;
        retirarEl.hidden = !podeMovimentar;
        if (!podeMovimentar) {
            tipoMovimento = '';
            formEl.hidden = true;
        }
    }

    function renderizarExtrato() {
        extratoEl.replaceChildren();
        if (carregandoMovimentacoes) {
            extratoEl.append(criarElemento(documento, 'div', 'reserva-extrato-vazio', 'Carregando extrato...'));
            return;
        }
        if (erroMovimentacoes) {
            extratoEl.append(criarElemento(documento, 'div', 'reserva-extrato-erro', erroMovimentacoes));
            return;
        }
        if (!movimentos.length) {
            extratoEl.append(criarElemento(documento, 'div', 'reserva-extrato-vazio', 'Nenhuma movimentação registrada.'));
            return;
        }

        const ordenados = [...movimentos].sort((a, b) => Number(b.criadoEm || 0) - Number(a.criadoEm || 0));
        const saldoApos = new Map();
        let saldoRetroativo = Number(saldo || 0);
        ordenados.forEach(movimento => {
            saldoApos.set(movimento.id, saldoRetroativo);
            const valor = Number(movimento.valor || 0);
            saldoRetroativo -= movimento.tipo === 'retirada' ? -valor : valor;
        });

        const movimentosVisiveis = ordenados.slice(0, quantidadeMovimentosVisiveis);
        const grupos = new Map();
        movimentosVisiveis.forEach(movimento => {
            const data = obterDataISO(movimento.criadoEm) || 'sem-data';
            if (!grupos.has(data)) grupos.set(data, []);
            grupos.get(data).push(movimento);
        });

        const fragmento = documento.createDocumentFragment();
        grupos.forEach((itens, data) => {
            const grupo = criarElemento(documento, 'div', 'reserva-grupo-data');
            grupo.append(criarElemento(documento, 'div', 'reserva-grupo-data-label', rotuloData(data)));

            itens.forEach(movimento => {
                const retirada = movimento.tipo === 'retirada';
                const tipoVisual = retirada ? 'gasto' : 'entrada';
                const item = criarElemento(documento, 'div', `reserva-mov-card ${tipoVisual}`);
                const icone = criarElemento(documento, 'div', `reserva-mov-icon ${tipoVisual}`, retirada ? '⬇️' : '💰');
                const info = criarElemento(documento, 'div', 'reserva-mov-info');
                info.append(criarElemento(documento, 'div', `reserva-mov-cat ${tipoVisual}`, retirada ? 'Retirada' : 'Entrada'));
                if (movimento.motivo) {
                    info.append(criarElemento(documento, 'div', 'reserva-mov-desc', movimento.motivo));
                }
                const hora = formatarHora(movimento.criadoEm);
                if (hora) info.append(criarElemento(documento, 'div', 'reserva-mov-hora', hora));
                info.append(criarElemento(
                    documento,
                    'div',
                    'reserva-mov-registrado-por',
                    `Inserido por ${movimento.registradoPor || 'Não informado'}`
                ));

                const valores = criarElemento(documento, 'div', 'reserva-mov-valores');
                valores.append(
                    criarElemento(documento, 'div', `reserva-mov-valor ${tipoVisual}`, `${retirada ? '−' : '+'}${formatadorMoeda.format(movimento.valor || 0)}`),
                    criarElemento(documento, 'div', 'reserva-mov-saldo-apos', `saldo ${formatadorMoeda.format(saldoApos.get(movimento.id) || 0)}`)
                );
                item.append(icone, info, valores);
                grupo.append(item);
            });
            fragmento.append(grupo);
        });
        extratoEl.append(fragmento);
    }

    async function carregarExtrato() {
        if (carregandoMovimentacoes) return;
        carregandoMovimentacoes = true;
        quantidadeMovimentosVisiveis = MOVIMENTOS_POR_PAGINA;
        erroMovimentacoes = '';
        renderizarExtrato();
        try {
            movimentos = await carregarMovimentacoesReserva(100);
            movimentosCarregados = true;
        } catch (erro) {
            console.warn('Não foi possível carregar o extrato da reserva:', erro);
            erroMovimentacoes = 'Extrato temporariamente indisponível.';
        } finally {
            carregandoMovimentacoes = false;
            renderizarExtrato();
        }
    }

    function posicionarCardAberto() {
        ignorarRolagemAte = Date.now() + 900;
        window.requestAnimationFrame(() => {
            const cabecalho = documento.querySelector('.header');
            const margemTopo = Number(cabecalho?.getBoundingClientRect().height || 0) + 10;
            const destino = window.scrollY + card.getBoundingClientRect().top - margemTopo;
            window.scrollTo({ top: Math.max(0, destino), behavior: 'smooth' });
        });
    }

    function alternarExpandido() {
        expandido = !expandido;
        if (expandido) quantidadeMovimentosVisiveis = MOVIMENTOS_POR_PAGINA;
        renderizarResumo();
        if (expandido) {
            posicionarCardAberto();
            if (!movimentosCarregados) carregarExtrato();
            else renderizarExtrato();
        }
    }

    function exibirProximosMovimentosAoRolar() {
        if (!expandido || carregandoMovimentacoes || erroMovimentacoes) return;
        if (Date.now() < ignorarRolagemAte) return;
        if (quantidadeMovimentosVisiveis >= movimentos.length) return;
        const alturaTela = window.innerHeight || documento.documentElement.clientHeight;
        if (extratoEl.getBoundingClientRect().bottom > alturaTela + 48) return;
        quantidadeMovimentosVisiveis = Math.min(
            movimentos.length,
            quantidadeMovimentosVisiveis + MOVIMENTOS_POR_PAGINA
        );
        renderizarExtrato();
    }

    function selecionarTipoMovimento(tipo) {
        if (!obterSocioLogadoAtivo()) return;
        tipoMovimento = tipo;
        formEl.hidden = false;
        formTituloEl.textContent = tipo === 'retirada' ? 'Retirar da reserva' : 'Adicionar à reserva';
        motivoLabelEl.textContent = tipo === 'retirada' ? 'Motivo da retirada' : 'Motivo (opcional)';
        motivoEl.placeholder = tipo === 'retirada' ? 'Informe por que o valor será retirado' : 'Se desejar, descreva a entrada';
        motivoEl.required = tipo === 'retirada';
        confirmarMovimentoEl.textContent = tipo === 'retirada' ? 'Confirmar retirada' : 'Confirmar entrada';
        adicionarEl.classList.toggle('ativo', tipo === 'entrada');
        retirarEl.classList.toggle('ativo', tipo === 'retirada');
        valorEl.focus();
    }

    function cancelarMovimento() {
        tipoMovimento = '';
        formEl.reset();
        formEl.hidden = true;
        adicionarEl.classList.remove('ativo');
        retirarEl.classList.remove('ativo');
    }

    function mensagemErroMovimento(erro) {
        const codigo = String(erro?.message || '');
        if (codigo.includes('SALDO_INSUFICIENTE') || codigo.includes('PERMISSION_DENIED')) {
            return 'O valor da retirada é maior que o saldo disponível.';
        }
        if (codigo.includes('MOTIVO_OBRIGATORIO')) return 'Informe o motivo da retirada.';
        if (codigo.includes('VALOR_INVALIDO')) return 'Informe um valor válido.';
        return navigator.onLine
            ? 'Não foi possível registrar a movimentação. Tente novamente.'
            : 'Sem internet. Conecte-se e tente novamente.';
    }

    async function registrarMovimento(evento) {
        evento.preventDefault();
        if (!tipoMovimento || salvandoMovimento) return;
        const socio = obterSocioLogadoAtivo();
        if (!socio) return alertar('Somente usuários com cargo Sócio podem movimentar a reserva.');
        const valor = Number(valorEl.value.replace(/\D/g, ''));
        const motivo = motivoEl.value.trim();
        if (!Number.isFinite(valor) || valor <= 0) return alertar('Informe um valor válido.');
        if (tipoMovimento === 'retirada' && !motivo) return alertar('Informe o motivo da retirada.');

        salvandoMovimento = true;
        confirmarMovimentoEl.disabled = true;
        const textoOriginal = confirmarMovimentoEl.textContent;
        confirmarMovimentoEl.textContent = 'Salvando...';
        try {
            await registrarMovimentoReserva({
                tipo: tipoMovimento,
                valor,
                motivo,
                registradoPor: socio.nome
            });
            feedbackEl.textContent = tipoMovimento === 'retirada' ? 'Retirada registrada.' : 'Entrada registrada.';
            cancelarMovimento();
            await carregarExtrato();
            window.setTimeout(() => { feedbackEl.textContent = ''; }, 2500);
        } catch (erro) {
            console.error('Não foi possível movimentar a reserva:', erro);
            await alertar(mensagemErroMovimento(erro));
        } finally {
            salvandoMovimento = false;
            confirmarMovimentoEl.disabled = false;
            confirmarMovimentoEl.textContent = textoOriginal;
        }
    }

    function agendarExpiracaoMeta() {
        if (temporizadorExpiracao) window.clearTimeout(temporizadorExpiracao);
        const proposta = metaDados?.pendente;
        if (!proposta || propostaMetaExpirada(proposta)) return;
        const espera = Math.max(0, Number(proposta.expiraEm) - Date.now()) + 50;
        temporizadorExpiracao = window.setTimeout(() => {
            renderizarResumo();
            descartarSolicitacaoMetaReservaExpirada(proposta.expiraEm)
                .catch(erro => console.warn('Não foi possível descartar a meta expirada:', erro));
        }, espera);
    }

    resumoEl.addEventListener('click', evento => {
        if (evento.target.closest('.reserva-atalho')) return;
        alternarExpandido();
    });
    resumoEl.addEventListener('keydown', evento => {
        if (evento.target.closest('.reserva-atalho')) return;
        if (evento.key !== 'Enter' && evento.key !== ' ') return;
        evento.preventDefault();
        alternarExpandido();
    });
    editarEl.addEventListener('click', evento => {
        evento.stopPropagation();
        abrirModalMeta();
    });
    adicionarEl.addEventListener('click', evento => {
        evento.stopPropagation();
        if (!expandido) alternarExpandido();
        selecionarTipoMovimento('entrada');
    });
    retirarEl.addEventListener('click', evento => {
        evento.stopPropagation();
        if (!expandido) alternarExpandido();
        selecionarTipoMovimento('retirada');
    });
    cancelarMovimentoEl.addEventListener('click', cancelarMovimento);
    formEl.addEventListener('submit', registrarMovimento);
    valorEl.addEventListener('input', () => {
        const digitos = valorEl.value.replace(/\D/g, '').slice(0, 12);
        valorEl.value = digitos ? Number(digitos).toLocaleString('pt-BR') : '';
    });
    window.addEventListener('scroll', exibirProximosMovimentosAoRolar, { passive: true });

    modal.cancelar.addEventListener('click', fecharModalMeta);
    modal.overlay.addEventListener('click', evento => {
        if (evento.target === modal.overlay) fecharModalMeta();
    });
    modal.input.addEventListener('input', () => {
        const digitos = modal.input.value.replace(/\D/g, '').slice(0, 12);
        modal.input.value = digitos ? Number(digitos).toLocaleString('pt-BR') : '';
    });
    modal.form.addEventListener('submit', async evento => {
        evento.preventDefault();
        const valorNovo = Number(modal.input.value.replace(/\D/g, ''));
        const valorAtual = obterMetaReserva(metaDados);
        if (!Number.isFinite(valorNovo) || valorNovo <= 0) return alertar('Informe um valor de meta válido.');
        if (valorNovo === valorAtual) return fecharModalMeta();
        const socio = obterSocioLogadoAtivo();
        const aprovadores = obterSocios();
        if (!socio || !aprovadores.length) return alertar('Nenhum sócio foi encontrado para aprovar esta alteração.');
        const aprovacoes = Object.fromEntries(aprovadores.map(item => [item.id, null]));
        modal.enviar.disabled = true;
        try {
            await criarSolicitacaoMetaReserva({
                propostoPor: socio.id,
                propostoPorNome: socio.nome,
                aprovadores,
                aprovacoes,
                valorAnterior: valorAtual,
                valorNovo,
                expiraEm: Date.now() + PRAZO_APROVACAO_META_MS
            });
            fecharModalMeta();
            await alertar('Alteração da meta enviada para aprovação dos sócios.');
        } catch (erro) {
            await alertar(erro?.message || 'Não foi possível enviar a alteração da meta.');
        } finally {
            modal.enviar.disabled = false;
        }
    });

    return {
        atualizarDadosMeta(novosDados = {}) {
            metaDados = novosDados || {};
            renderizarResumo();
            const proposta = metaDados?.pendente;
            if (proposta && propostaMetaExpirada(proposta)) {
                descartarSolicitacaoMetaReservaExpirada(proposta.expiraEm)
                    .catch(erro => console.warn('Não foi possível limpar a meta expirada:', erro));
            }
            agendarExpiracaoMeta();
        },
        inicializar() {
            renderizarResumo();
            renderizarExtrato();
            return escutarSaldoReserva(novoSaldo => {
                saldo = novoSaldo;
                renderizarResumo();
            });
        },
        renderizar: renderizarResumo
    };
}
