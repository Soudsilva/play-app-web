const TEMPO_PARA_ARRASTAR = 320;
const DISTANCIA_PARA_CANCELAR = 9;

export function ativarOrdenacaoArquivos({ container, habilitado, aoSalvar, aoFalhar }) {
    if (!container || !habilitado) return () => {};
    const cards = container.querySelectorAll('[data-card-arquivo-id]');
    if (cards.length < 2) return () => {};

    let movimento = null;
    let salvando = false;
    let suprimirCliqueAte = 0;

    const obterIdsNaTela = () => Array.from(
        container.querySelectorAll('[data-card-arquivo-id]')
    ).map(card => card.dataset.cardArquivoId).filter(Boolean);

    const restaurarOrdem = ids => {
        const porId = new Map(Array.from(
            container.querySelectorAll('[data-card-arquivo-id]')
        ).map(card => [card.dataset.cardArquivoId, card]));
        ids.forEach(id => {
            const card = porId.get(id);
            if (card) container.appendChild(card);
        });
    };

    const posicionarFlutuante = () => {
        if (!movimento?.flutuante) return;
        const margem = 10;
        const largura = movimento.flutuante.offsetWidth;
        const altura = movimento.flutuante.offsetHeight;
        const esquerda = Math.max(
            margem,
            Math.min(movimento.xAtual - movimento.deslocamentoX, window.innerWidth - largura - margem)
        );
        const topo = Math.max(
            margem,
            Math.min(movimento.yAtual - movimento.deslocamentoY, window.innerHeight - altura - margem)
        );
        movimento.flutuante.style.left = `${esquerda}px`;
        movimento.flutuante.style.top = `${topo}px`;
    };

    const criarFlutuante = () => {
        if (!movimento || movimento.flutuante) return;
        const flutuante = document.createElement('div');
        flutuante.className = 'arquivo-card-flutuante';
        flutuante.textContent = movimento.texto;
        flutuante.style.width = `${movimento.largura}px`;
        document.body.appendChild(flutuante);
        movimento.flutuante = flutuante;
        posicionarFlutuante();
    };

    const limparMovimento = () => {
        if (!movimento) return;
        clearTimeout(movimento.temporizador);
        movimento.flutuante?.remove();
        movimento.card.classList.remove('item-em-movimento');
        document.body.classList.remove('reordenando-arquivos');
        movimento = null;
    };

    const iniciarMovimento = (evento, ponto) => {
        if (salvando || evento.target.closest('.arquivo-acao-icone')) return;
        const card = evento.target.closest('[data-card-arquivo-id]');
        if (!card) return;
        const limites = card.getBoundingClientRect();

        limparMovimento();
        movimento = {
            card,
            xInicial: ponto.clientX,
            yInicial: ponto.clientY,
            xAtual: ponto.clientX,
            yAtual: ponto.clientY,
            deslocamentoX: ponto.clientX - limites.left,
            deslocamentoY: ponto.clientY - limites.top,
            texto: card.querySelector('.arquivo-nome')?.textContent?.trim() || 'Arquivo',
            largura: Math.min(limites.width, window.innerWidth - 20),
            flutuante: null,
            ativo: false,
            ordemInicial: obterIdsNaTela(),
            temporizador: window.setTimeout(() => {
                if (!movimento || movimento.card !== card) return;
                movimento.ativo = true;
                card.classList.add('item-em-movimento');
                document.body.classList.add('reordenando-arquivos');
                criarFlutuante();
            }, TEMPO_PARA_ARRASTAR)
        };
    };

    const moverCard = (evento, ponto) => {
        if (!movimento) return;
        if (!movimento.ativo) {
            const distanciaX = Math.abs(ponto.clientX - movimento.xInicial);
            const distanciaY = Math.abs(ponto.clientY - movimento.yInicial);
            if (distanciaX > DISTANCIA_PARA_CANCELAR || distanciaY > DISTANCIA_PARA_CANCELAR) {
                limparMovimento();
            }
            return;
        }

        if (evento.cancelable) evento.preventDefault();
        movimento.xAtual = ponto.clientX;
        movimento.yAtual = ponto.clientY;
        posicionarFlutuante();

        const elementoAlvo = document.elementFromPoint(ponto.clientX, ponto.clientY);
        const cardAlvo = elementoAlvo?.closest('[data-card-arquivo-id]');
        if (cardAlvo && cardAlvo !== movimento.card) {
            const limites = cardAlvo.getBoundingClientRect();
            if (ponto.clientY < limites.top + (limites.height / 2)) {
                container.insertBefore(movimento.card, cardAlvo);
            } else {
                container.insertBefore(movimento.card, cardAlvo.nextSibling);
            }
        }

        if (ponto.clientY < 70) window.scrollBy(0, -14);
        if (ponto.clientY > window.innerHeight - 70) window.scrollBy(0, 14);
    };

    const finalizarMovimento = async () => {
        if (!movimento) return;
        const estavaAtivo = movimento.ativo;
        const ordemInicial = movimento.ordemInicial;
        if (estavaAtivo) suprimirCliqueAte = Date.now() + 700;
        limparMovimento();
        if (!estavaAtivo) return;

        const ids = obterIdsNaTela();
        if (ordemInicial.join('|') === ids.join('|')) return;

        salvando = true;
        container.classList.add('ordenacao-salvando');
        try {
            await aoSalvar(ids);
        } catch (erro) {
            restaurarOrdem(ordemInicial);
            await aoFalhar(erro);
        } finally {
            salvando = false;
            container.classList.remove('ordenacao-salvando');
        }
    };

    const cancelarMovimento = () => {
        if (!movimento) return;
        const ordemInicial = movimento.ordemInicial;
        const estavaAtivo = movimento.ativo;
        limparMovimento();
        if (estavaAtivo) restaurarOrdem(ordemInicial);
    };

    const impedirCliqueAposArrastar = evento => {
        if (Date.now() >= suprimirCliqueAte) return;
        evento.preventDefault();
        evento.stopPropagation();
    };
    const aoTocar = evento => iniciarMovimento(evento, evento.touches[0]);
    const aoMoverToque = evento => {
        if (evento.touches[0]) moverCard(evento, evento.touches[0]);
    };
    const aoPressionarMouse = evento => {
        if (evento.button !== 0) return;
        iniciarMovimento(evento, evento);
    };
    const aoMoverMouse = evento => moverCard(evento, evento);
    const impedirMenuContexto = evento => {
        if (evento.target.closest('[data-card-arquivo-id]')) evento.preventDefault();
    };

    container.addEventListener('click', impedirCliqueAposArrastar, true);
    container.addEventListener('touchstart', aoTocar, { passive: true });
    container.addEventListener('touchmove', aoMoverToque, { passive: false });
    container.addEventListener('touchend', finalizarMovimento);
    container.addEventListener('touchcancel', cancelarMovimento);
    container.addEventListener('mousedown', aoPressionarMouse);
    container.addEventListener('contextmenu', impedirMenuContexto);
    document.addEventListener('mousemove', aoMoverMouse);
    document.addEventListener('mouseup', finalizarMovimento);

    return () => {
        cancelarMovimento();
        container.removeEventListener('click', impedirCliqueAposArrastar, true);
        container.removeEventListener('touchstart', aoTocar);
        container.removeEventListener('touchmove', aoMoverToque);
        container.removeEventListener('touchend', finalizarMovimento);
        container.removeEventListener('touchcancel', cancelarMovimento);
        container.removeEventListener('mousedown', aoPressionarMouse);
        container.removeEventListener('contextmenu', impedirMenuContexto);
        document.removeEventListener('mousemove', aoMoverMouse);
        document.removeEventListener('mouseup', finalizarMovimento);
    };
}
