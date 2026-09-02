import { listarConfiguracoesMaquina, maquinaExigeComposicao } from './maquina-composicao-rules.mjs';

function escaparHtml(valor) {
    return String(valor || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function garantirEstilos() {
    if (document.getElementById('estilosMaquinaComposicao')) return;
    const estilo = document.createElement('style');
    estilo.id = 'estilosMaquinaComposicao';
    estilo.textContent = `
        .maquina-composicao-host { width: 100%; flex: 0 0 100%; box-sizing: border-box; }
        .maquina-composicao-card { width: 100%; margin-top: 8px; padding: 10px; border: 1px solid rgba(33,150,243,.42); border-radius: 12px; background: rgba(5,22,48,.72); box-sizing: border-box; }
        .maquina-composicao-opcoes { display: grid; grid-template-columns: 1fr; gap: 9px; }
        .maquina-composicao-opcao { position: relative; overflow: hidden; width: 100%; min-height: 62px; padding: 11px 13px 11px 16px; display: grid; grid-template-columns: 22px minmax(0,1fr); align-items: center; gap: 11px; border: 1px solid rgba(255,255,255,.18); border-radius: 13px; background: linear-gradient(180deg,rgba(11,40,94,.88),rgba(5,26,66,.92)); color: #fff; text-align: left; cursor: pointer; font: inherit; transition: border-color .15s ease,background .15s ease,box-shadow .15s ease; }
        .maquina-composicao-opcao::before { content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 3px; background: rgba(255,255,255,.12); transition: background .15s ease; }
        .maquina-composicao-opcao:hover, .maquina-composicao-opcao:focus-visible { border-color: var(--amarelo,#ffeb00); outline: none; }
        .maquina-composicao-opcao.selecionada { border-color: var(--verde,#36c66b); background: linear-gradient(180deg,rgba(20,74,102,.92),rgba(11,48,74,.95)); box-shadow: 0 0 0 2px rgba(54,198,107,.18),0 6px 16px rgba(0,0,0,.26); }
        .maquina-composicao-opcao.selecionada::before { background: linear-gradient(180deg,#3ee07f,rgba(54,198,107,.2)); }
        .maquina-composicao-opcao.selecionada::after { content: ''; position: absolute; inset: 0; pointer-events: none; background: linear-gradient(105deg,transparent 32%,rgba(255,255,255,.14) 50%,transparent 68%); transform: translateX(-120%); animation: maquina-composicao-brilho .7s ease-out; }
        @keyframes maquina-composicao-brilho { to { transform: translateX(120%); } }
        .maquina-composicao-marca { width: 22px; height: 22px; border-radius: 50%; border: 2px solid rgba(255,255,255,.32); display: grid; place-items: center; font-size: 12px; font-weight: 900; line-height: 1; color: transparent; transition: background .18s ease,border-color .18s ease,color .18s ease,box-shadow .18s ease; box-sizing: border-box; }
        .maquina-composicao-opcao.selecionada .maquina-composicao-marca { border-color: var(--verde,#36c66b); background: var(--verde,#36c66b); color: #06301a; box-shadow: 0 0 0 4px rgba(54,198,107,.18); }
        .maquina-composicao-info { min-width: 0; min-height: 28px; display: flex; align-items: center; justify-content: flex-start; gap: 10px; }
        .maquina-composicao-nome { display: block; min-width: 0; font-size: 16px; font-weight: 800; line-height: 1.15; white-space: nowrap; }
        .maquina-composicao-resumo { flex: 0 0 auto; display: inline-flex; align-items: center; justify-content: center; margin: 0; padding: 4px 11px; border-radius: 999px; background: var(--amarelo,#ffeb00); color: #09245a; font-size: 16px; font-weight: 900; letter-spacing: .3px; white-space: nowrap; }
        .maquina-composicao-opcao:disabled { opacity: .45; cursor: not-allowed; }
        .maquina-composicao-aviso { margin: 0; color: #ffd27d; text-align: center; font-size: 12px; }
        @media (prefers-reduced-motion: reduce) { .maquina-composicao-opcao.selecionada::after { animation: none; } }
    `;
    document.head.appendChild(estilo);
}

export function renderizarCartaoComposicao({
    host,
    itemOrigem,
    estoque,
    codigoSelecionado = '',
    onSelecionar,
    exigirValor = false,
    exigirEstoque = false
} = {}) {
    if (!host) return { configuracoes: [], selecionada: null };
    garantirEstilos();
    host.innerHTML = '';
    host.hidden = !maquinaExigeComposicao(itemOrigem);
    if (host.hidden) return { configuracoes: [], selecionada: null };

    const configuracoes = listarConfiguracoesMaquina(itemOrigem, estoque, { exigirValor, exigirEstoque });
    const selecionada = configuracoes.find(item => item.codigo === codigoSelecionado && item.disponivel) || null;
    const disponiveis = configuracoes.filter(item => item.disponivel);
    if (disponiveis.length === 0) {
        host.innerHTML = '<div class="maquina-composicao-card"><p class="maquina-composicao-aviso">Cadastre Máquina P e Máquina G no estoque para configurar esta montagem.</p></div>';
        return { configuracoes, selecionada: null };
    }

    const card = document.createElement('div');
    card.className = 'maquina-composicao-card';
    card.innerHTML = `
        <div class="maquina-composicao-opcoes" role="radiogroup">
            ${disponiveis.map(configuracao => `
                <button type="button" class="maquina-composicao-opcao${selecionada?.codigo === configuracao.codigo ? ' selecionada' : ''}"
                    data-codigo-montagem="${escaparHtml(configuracao.codigo)}" role="radio"
                    aria-checked="${selecionada?.codigo === configuracao.codigo ? 'true' : 'false'}"
                    aria-label="${escaparHtml(configuracao.nome)}, ${escaparHtml(configuracao.resumo)}">
                    <span class="maquina-composicao-marca" aria-hidden="true">✓</span>
                    <span class="maquina-composicao-info">
                        <span class="maquina-composicao-resumo">${escaparHtml(configuracao.resumo)}</span>
                        <strong class="maquina-composicao-nome">${escaparHtml(configuracao.nome)}</strong>
                    </span>
                </button>
            `).join('')}
        </div>
    `;
    card.addEventListener('click', evento => {
        const botao = evento.target.closest('[data-codigo-montagem]');
        if (!botao) return;
        const configuracao = configuracoes.find(item => item.codigo === botao.dataset.codigoMontagem && item.disponivel);
        if (configuracao && typeof onSelecionar === 'function') onSelecionar(configuracao);
    });
    host.appendChild(card);
    return { configuracoes, selecionada };
}
