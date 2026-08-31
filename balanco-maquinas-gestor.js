import {
    escutarMaquinasDoUsuario,
    listarPaginaMovimentacoesDaMaquina
} from './balanco-maquinas-gestor-service.js';
import {
    listarUsuariosAuditaveis,
    movimentoEhConferenciaDeTotal,
    normalizarTipoMovimentacaoMaquina,
    ordenarMovimentacoesMaisRecentes
} from './balanco-maquinas-gestor-rules.mjs';

const ITENS_POR_PAGINA = 6;
const CONCORRENCIA_RESUMOS = 3;
const CANDIDATOS_POR_RESUMO = 10;

function escaparHtml(valor) {
    return String(valor ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function formatarDataHora(valor) {
    const data = new Date(valor || '');
    if (!Number.isFinite(data.getTime())) return '';
    return data.toLocaleDateString('pt-BR') + ' ' + data.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
}

function rotuloMovimento(movimento) {
    const rotulos = {
        saida_estoque: 'Retirada na loja',
        entrada_estoque: 'Devolução ao estoque',
        atendimento: 'Entregue ao cliente',
        cadastro_cliente: 'Cadastro em cliente',
        manutencao: 'Reposição',
        manutencao_retirada: 'Retirada em manutenção',
        manutencao_adicao: 'Adicionou',
        ajuste_balanco_gestor: 'Ajuste do gestor',
        balanco_aprovado: 'Valor conferido',
        cancelamento: 'Cancelamento'
    };
    const tipo = normalizarTipoMovimentacaoMaquina(movimento);
    return rotulos[tipo] || String(movimento?.descricao || tipo || 'Movimentação').trim();
}

function htmlMovimentoResumo(movimento) {
    const quantidade = Number(movimento?.movimento || 0);
    const sinal = quantidade > 0 ? '+' : '';
    const ehConferencia = movimentoEhConferenciaDeTotal(movimento);
    const totalBase = movimento?.totalApos ?? (ehConferencia ? movimento?.valorConferido : null);
    const totalNumero = totalBase != null ? Number(totalBase) : null;
    const totalTexto = totalNumero != null && Number.isFinite(totalNumero) ? totalNumero : '—';
    const descricao = String(movimento?.descricao || '').trim();
    const rotulo = rotuloMovimento(movimento);
    return `<div class="audit-maq-mov">
        <div class="audit-maq-mov-topo">
            <span>${escaparHtml(rotulo)}</span>
            <span class="audit-maq-mov-valores${ehConferencia ? ' somente-total' : ''}">
                ${ehConferencia ? '' : `<strong class="${quantidade < 0 ? 'negativo' : 'positivo'}">${sinal}${quantidade}</strong>`}
                <span class="audit-maq-mov-total">Total: <strong class="${totalNumero != null && totalNumero < 0 ? 'negativo' : 'positivo'}">${totalTexto}</strong></span>
            </span>
        </div>
        ${descricao && descricao !== rotulo ? `<div class="audit-maq-mov-desc">${escaparHtml(descricao)}</div>` : ''}
        <div class="audit-maq-mov-data">${escaparHtml(formatarDataHora(movimento?.timestamp || movimento?.data))}</div>
    </div>`;
}

export function criarAuditoriaMaquinasGestor({ aoAbrirHistorico, aoAlterarAbertura, servico = {} } = {}) {
    const escutarMaquinas = servico.escutarMaquinasDoUsuario || escutarMaquinasDoUsuario;
    const listarMovimentos = servico.listarPaginaMovimentacoesDaMaquina || listarPaginaMovimentacoesDaMaquina;
    let raiz = null;
    let usuarios = [];
    let aberto = false;
    let usuarioSelecionado = '';
    let maquinas = [];
    let pagina = 0;
    let carregandoMaquinas = false;
    let erroMaquinas = '';
    let cancelarPosse = null;
    let geracao = 0;
    const resumos = new Map();

    function maquinasVisiveis() {
        const inicio = pagina * ITENS_POR_PAGINA;
        return maquinas.slice(inicio, inicio + ITENS_POR_PAGINA);
    }

    function totalPaginas() {
        return Math.max(1, Math.ceil(maquinas.length / ITENS_POR_PAGINA));
    }

    function htmlResumo(maquina) {
        const resumo = resumos.get(maquina.itemChave);
        if (!resumo || resumo.status === 'carregando') {
            return '<div class="audit-maq-carregando">Buscando últimas movimentações...</div>';
        }
        if (resumo.status === 'erro') {
            return '<div class="audit-maq-erro">Não foi possível carregar o histórico.</div>';
        }
        if (!resumo.movimentos.length) {
            return '<div class="audit-maq-vazio">Nenhuma movimentação encontrada.</div>';
        }
        return resumo.movimentos.map(htmlMovimentoResumo).join('');
    }

    function renderizar() {
        if (!raiz) return;
        const opcoes = usuarios.map(nome => `<option value="${escaparHtml(nome)}"${nome === usuarioSelecionado ? ' selected' : ''}>${escaparHtml(nome)}</option>`).join('');
        let corpo = '';
        if (usuarioSelecionado && carregandoMaquinas) {
            corpo = '<div class="audit-maq-instrucao">Carregando máquinas...</div>';
        } else if (usuarioSelecionado && erroMaquinas) {
            corpo = `<div class="audit-maq-erro">${escaparHtml(erroMaquinas)}</div>`;
        } else if (usuarioSelecionado && !maquinas.length) {
            corpo = '<div class="audit-maq-instrucao">Nenhuma máquina relacionada a este usuário.</div>';
        } else if (usuarioSelecionado) {
            corpo = `<div class="audit-maq-lista">${maquinasVisiveis().map(maquina => `
                <button type="button" class="audit-maq-card" data-item-chave="${escaparHtml(maquina.itemChave)}">
                    <div class="audit-maq-card-topo">
                        <span class="audit-maq-nome">${escaparHtml(maquina.nome)}</span>
                        <span class="audit-maq-total${maquina.quantidade < 0 ? ' negativo' : ''}">Total: ${maquina.quantidade}</span>
                    </div>
                    <div class="audit-maq-resumo">${htmlResumo(maquina)}</div>
                    <div class="audit-maq-abrir">Toque para ver o histórico</div>
                </button>`).join('')}</div>
                ${totalPaginas() > 1 ? `<div class="audit-maq-paginacao">
                    <button type="button" data-pagina="anterior"${pagina === 0 ? ' disabled' : ''}>&lsaquo;</button>
                    <span>${pagina + 1} de ${totalPaginas()}</span>
                    <button type="button" data-pagina="proxima"${pagina >= totalPaginas() - 1 ? ' disabled' : ''}>&rsaquo;</button>
                </div>` : ''}`;
        }

        raiz.innerHTML = `<section class="audit-maq-secao">
            <button type="button" class="gestor-mov-header audit-maq-header${aberto ? ' aberto' : ''}" id="auditMaquinasCabecalho">
                <span class="gestor-mov-icone">&#128377;&#65039;</span>
                <span class="gestor-mov-info"><span class="gestor-mov-label">Máquinas</span></span>
                <span class="gestor-mov-seta${aberto ? ' girada' : ''}">&#9660;</span>
            </button>
            <div class="gestor-mov-corpo audit-maq-corpo${aberto ? ' aberto' : ''}">
                ${aberto ? `<div class="audit-maq-controle">
                    <label for="auditMaquinasUsuario">Usuário</label>
                    <select id="auditMaquinasUsuario">
                        <option value="">Selecione</option>
                        ${opcoes}
                    </select>
                </div>
                ${corpo}` : ''}
            </div>
        </section>`;

        raiz.querySelector('#auditMaquinasCabecalho')?.addEventListener('click', () => {
            aberto = !aberto;
            renderizar();
            if (typeof aoAlterarAbertura === 'function') aoAlterarAbertura(aberto);
        });
        raiz.querySelector('#auditMaquinasUsuario')?.addEventListener('change', evento => selecionarUsuario(evento.target.value));
        raiz.querySelector('[data-pagina="anterior"]')?.addEventListener('click', () => alterarPagina(-1));
        raiz.querySelector('[data-pagina="proxima"]')?.addEventListener('click', () => alterarPagina(1));
        raiz.querySelectorAll('.audit-maq-card').forEach(card => card.addEventListener('click', () => {
            const maquina = maquinas.find(item => item.itemChave === card.dataset.itemChave);
            if (maquina && typeof aoAbrirHistorico === 'function') aoAbrirHistorico({ ...maquina, responsavel: usuarioSelecionado });
        }));
        if (aberto) carregarResumosVisiveis();
    }

    async function carregarResumosVisiveis() {
        const geracaoAtual = geracao;
        const pendentes = maquinasVisiveis().filter(maquina => !resumos.has(maquina.itemChave));
        pendentes.forEach(maquina => resumos.set(maquina.itemChave, { status: 'carregando', movimentos: [] }));
        if (pendentes.length) renderizar();

        for (let inicio = 0; inicio < pendentes.length; inicio += CONCORRENCIA_RESUMOS) {
            const lote = pendentes.slice(inicio, inicio + CONCORRENCIA_RESUMOS);
            await Promise.all(lote.map(async maquina => {
                try {
                    const paginaMovimentos = await listarMovimentos(usuarioSelecionado, maquina.itemChave, {
                        limite: CANDIDATOS_POR_RESUMO,
                        itemNome: maquina.nome,
                        mesclarLegado: true
                    });
                    if (geracaoAtual !== geracao) return;
                    resumos.set(maquina.itemChave, {
                        status: 'pronto',
                        movimentos: ordenarMovimentacoesMaisRecentes(paginaMovimentos.movimentos).slice(0, 2)
                    });
                } catch (erro) {
                    console.error('Erro ao carregar resumo da máquina:', erro);
                    if (geracaoAtual !== geracao) return;
                    resumos.set(maquina.itemChave, { status: 'erro', movimentos: [] });
                }
            }));
            if (geracaoAtual !== geracao) return;
            renderizar();
        }
    }

    function selecionarUsuario(nome) {
        const novoUsuario = String(nome || '').trim();
        if (novoUsuario === usuarioSelecionado) return;
        geracao += 1;
        if (typeof cancelarPosse === 'function') cancelarPosse();
        cancelarPosse = null;
        usuarioSelecionado = novoUsuario;
        maquinas = [];
        pagina = 0;
        erroMaquinas = '';
        carregandoMaquinas = Boolean(novoUsuario);
        resumos.clear();
        renderizar();
        if (!novoUsuario) return;

        const geracaoAtual = geracao;
        cancelarPosse = escutarMaquinas(novoUsuario, lista => {
            if (geracaoAtual !== geracao) return;
            maquinas = Array.isArray(lista) ? lista : [];
            pagina = Math.min(pagina, totalPaginas() - 1);
            carregandoMaquinas = false;
            erroMaquinas = '';
            renderizar();
        }, erro => {
            console.error('Erro ao carregar máquinas do usuário:', erro);
            if (geracaoAtual !== geracao) return;
            carregandoMaquinas = false;
            erroMaquinas = 'Não foi possível carregar as máquinas.';
            renderizar();
        });
    }

    function alterarPagina(delta) {
        const proxima = Math.max(0, Math.min(totalPaginas() - 1, pagina + Number(delta || 0)));
        if (proxima === pagina) return;
        pagina = proxima;
        renderizar();
    }

    return {
        atualizarColaboradores(colaboradores) {
            usuarios = listarUsuariosAuditaveis(colaboradores);
            if (usuarioSelecionado && !usuarios.includes(usuarioSelecionado)) selecionarUsuario('');
            else renderizar();
        },
        montar(elemento) {
            raiz = elemento || null;
            renderizar();
        },
        fechar() {
            if (!aberto) return;
            aberto = false;
            renderizar();
        },
        destruir() {
            geracao += 1;
            if (typeof cancelarPosse === 'function') cancelarPosse();
            cancelarPosse = null;
            raiz = null;
        }
    };
}
