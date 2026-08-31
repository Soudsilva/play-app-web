import {
    concluirNotaProximaVisita,
    salvarNotaProximaVisita
} from './nota-proxima-visita-service.js';

const containersInstalados = new WeakSet();

function escHtml(valor) {
    return String(valor || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function formatarData(valor) {
    if (!valor) return '';
    const data = new Date(valor);
    if (Number.isNaN(data.getTime())) return '';
    return data.toLocaleString('pt-BR', {
        day: '2-digit',
        month: '2-digit',
        year: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function atendimentoEhPosteriorNota(atendimentoData, notaCriadaEm) {
    const atendimentoMs = new Date(atendimentoData || '').getTime();
    const notaMs = new Date(notaCriadaEm || '').getTime();
    if (!Number.isFinite(atendimentoMs) || !Number.isFinite(notaMs)) return true;
    return atendimentoMs > notaMs;
}

function obterContextoNota({ cliente, atendimentoId, atendimentoData }) {
    const clienteId = String(cliente?.firebaseUrl || cliente?.id || '').trim();
    const visitaId = String(atendimentoId || '').trim();
    const nota = cliente?.notaProximaVisitaAtiva || null;
    const origemId = String(nota?.atendimentoOrigemId || '').trim();

    let estado = 'nova';
    if (nota && origemId === visitaId) {
        estado = 'agendada';
    } else if (nota && atendimentoEhPosteriorNota(atendimentoData, nota?.criadaEm)) {
        estado = 'pendente';
    } else if (nota) {
        estado = 'indisponivel';
    }

    return { clienteId, visitaId, nota, estado };
}

export function montarBotaoNotaProximaVisitaHtml(dados) {
    const { clienteId, visitaId, estado } = obterContextoNota(dados);
    if (!clienteId || !visitaId) return '';

    const temNota = estado !== 'nova';
    const podeEditar = estado !== 'indisponivel';
    const rotulo = temNota && podeEditar
        ? 'Alterar nota para a pr\u00f3xima visita'
        : !temNota
            ? 'Criar nota para a pr\u00f3xima visita'
            : 'J\u00e1 existe uma nota para este cliente';
    const icone = temNota
        ? `<svg viewBox="0 0 24 24" aria-hidden="true">
                <path d="m4 20 4.2-1 10.9-10.9a2.1 2.1 0 0 0-3-3L5.2 16 4 20Z"></path>
                <path d="m14.7 6.5 3 3M4 20h5"></path>
           </svg>`
        : `<svg viewBox="0 0 24 24" aria-hidden="true">
                <path d="M7 3.5h11a2 2 0 0 1 2 2v13a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2v-13a2 2 0 0 1 2-2Z"></path>
                <path d="M3.5 7h3M3.5 12h3M3.5 17h3M10 8h6M10 12h6M10 16h4"></path>
           </svg>`;

    return `
        <button class="btn-nota-proxima-icone${temNota ? ' tem-nota' : ''}" type="button"
            data-nota-acao="abrir"
            data-nota-cliente-id="${escHtml(clienteId)}"
            data-nota-atendimento-id="${escHtml(visitaId)}"
            aria-label="${rotulo}" title="${rotulo}" ${podeEditar ? '' : 'disabled'}>
            ${icone}
        </button>`;
}

export function montarNotaProximaVisitaHtml({ cliente, atendimentoId, atendimentoData }) {
    const { clienteId, visitaId, nota, estado } = obterContextoNota({
        cliente,
        atendimentoId,
        atendimentoData
    });
    if (!clienteId || !visitaId) return '';

    if (estado === 'nova' || estado === 'indisponivel') {
        return `
            <div class="nota-proxima-visita nota-vazia" data-nota-cliente-id="${escHtml(clienteId)}" data-nota-atendimento-id="${escHtml(visitaId)}" hidden></div>`;
    }

    const criadaNestaVisita = estado === 'agendada';

    const titulo = criadaNestaVisita
        ? 'Nota salva para a pr&oacute;xima visita'
        : 'Nota para verificar nesta visita';
    const meta = [nota?.criadaPor, formatarData(nota?.criadaEm)]
        .filter(Boolean)
        .map(escHtml)
        .join(' &middot; ');
    const acao = criadaNestaVisita
        ? ''
        : `<div class="nota-proxima-acoes"><button class="nota-proxima-concluir" type="button" data-nota-acao="concluir" data-nota-id="${escHtml(nota?.id || '')}" aria-label="Concluir nota" title="Concluir nota"><span aria-hidden="true">&#10003;</span> Concluir</button></div>`;

    return `
        <div class="nota-proxima-visita ${criadaNestaVisita ? 'agendada' : 'pendente'}" data-nota-cliente-id="${escHtml(clienteId)}" data-nota-atendimento-id="${escHtml(visitaId)}">
            <div class="nota-proxima-titulo">${titulo}</div>
            <div class="nota-proxima-texto">${escHtml(nota?.texto || '')}</div>
            ${meta ? `<div class="nota-proxima-meta">${meta}</div>` : ''}
            ${acao}
        </div>`;
}

function mostrarMensagem(mensagem) {
    if (window.playAlert) return window.playAlert(mensagem);
    alert(mensagem);
    return Promise.resolve();
}

function abrirFormulario(wrapper, cliente) {
    if (!wrapper) return;
    const formularioExistente = wrapper.querySelector('.nota-proxima-form');
    if (formularioExistente) {
        fecharFormulario(wrapper);
        return;
    }
    wrapper.hidden = false;
    const nota = cliente?.notaProximaVisitaAtiva || null;
    const formulario = document.createElement('div');
    formulario.className = 'nota-proxima-form';
    formulario.innerHTML = `
        <label>Nota para a pr&oacute;xima visita</label>
        <textarea maxlength="500" rows="3" placeholder="O que o gestor precisa verificar?">${escHtml(nota?.texto || '')}</textarea>
        <div class="nota-proxima-form-acoes">
            <button class="nota-proxima-cancelar" type="button" data-nota-acao="cancelar" aria-label="Cancelar edi&ccedil;&atilde;o" title="Cancelar edi&ccedil;&atilde;o">
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6 6 18"></path></svg>
            </button>
            <button class="nota-proxima-salvar" type="button" data-nota-acao="salvar" aria-label="Salvar nota" title="Salvar nota">
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 4h12l2 2v14H5V4Z"></path><path d="M8 4v6h8V4M8 20v-6h8v6"></path></svg>
            </button>
        </div>`;
    wrapper.querySelectorAll(':scope > button, :scope > .nota-proxima-acoes').forEach(elemento => {
        elemento.hidden = true;
    });
    wrapper.appendChild(formulario);
    formulario.querySelector('textarea')?.focus();
}

function fecharFormulario(wrapper) {
    wrapper?.querySelector('.nota-proxima-form')?.remove();
    wrapper?.querySelectorAll(':scope > button, :scope > .nota-proxima-acoes').forEach(elemento => {
        elemento.hidden = false;
    });
    if (wrapper?.classList.contains('nota-vazia')) wrapper.hidden = true;
}

export function instalarNotasProximaVisita(opcoes) {
    const container = opcoes?.container;
    if (!container || containersInstalados.has(container)) return;
    containersInstalados.add(container);

    container.addEventListener('click', async (event) => {
        const botao = event.target.closest('[data-nota-acao]');
        if (!botao || !container.contains(botao)) return;
        const wrapper = botao.closest('.nota-proxima-visita')
            || botao.closest('.card')?.querySelector('.nota-proxima-visita');
        if (!wrapper) return;
        event.preventDefault();
        event.stopPropagation();

        const clienteId = String(botao.dataset.notaClienteId || wrapper.dataset.notaClienteId || '').trim();
        const atendimentoId = String(botao.dataset.notaAtendimentoId || wrapper.dataset.notaAtendimentoId || '').trim();
        const cliente = opcoes.obterCliente(clienteId);
        const acao = botao.dataset.notaAcao;

        if (acao === 'abrir') {
            abrirFormulario(wrapper, cliente);
            return;
        }
        if (acao === 'cancelar') {
            fecharFormulario(wrapper);
            return;
        }

        if (acao === 'salvar') {
            const textarea = wrapper.querySelector('.nota-proxima-form textarea');
            const texto = String(textarea?.value || '').trim();
            if (!texto) {
                await mostrarMensagem('Digite a nota para a proxima visita.');
                textarea?.focus();
                return;
            }

            botao.disabled = true;
            botao.classList.add('carregando');
            botao.setAttribute('aria-label', 'Salvando nota');
            try {
                const patch = await salvarNotaProximaVisita(
                    clienteId,
                    atendimentoId,
                    texto,
                    opcoes.obterGestor()
                );
                opcoes.atualizarCliente(clienteId, patch);
                opcoes.renderizar();
                await mostrarMensagem('Nota salva para a proxima visita.');
            } catch (erro) {
                await mostrarMensagem('Erro ao salvar nota: ' + (erro?.message || erro));
                botao.disabled = false;
                botao.classList.remove('carregando');
                botao.setAttribute('aria-label', 'Salvar nota');
            }
            return;
        }

        if (acao === 'concluir') {
            const confirmar = window.playConfirm
                ? await window.playConfirm('Marcar esta nota como concluida?', {
                    primaryLabel: 'Concluir',
                    secondaryLabel: 'Cancelar'
                })
                : confirm('Marcar esta nota como concluida?');
            if (!confirmar) return;

            botao.disabled = true;
            botao.textContent = 'Concluindo...';
            try {
                const patch = await concluirNotaProximaVisita(
                    clienteId,
                    botao.dataset.notaId,
                    atendimentoId,
                    opcoes.obterGestor()
                );
                opcoes.atualizarCliente(clienteId, patch);
                opcoes.renderizar();
            } catch (erro) {
                await mostrarMensagem('Erro ao concluir nota: ' + (erro?.message || erro));
                botao.disabled = false;
                botao.innerHTML = '<span aria-hidden="true">&#10003;</span> Concluir';
            }
        }
    });
}
