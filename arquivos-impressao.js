import {
    dbCadastrarArquivoImpressao,
    dbAtualizarOrdemArquivosImpressao,
    dbBaixarArquivoImpressao,
    dbDesativarArquivoImpressao,
    dbListarArquivosImpressao,
    dbObterArquivoImpressao,
    dbObterUrlArquivoImpressao,
    dbRenomearArquivoImpressao
} from './arquivos-impressao-service.js';
import { gerarMiniaturaPdf } from './arquivos-impressao-thumbnail.js';
import { ativarOrdenacaoArquivos } from './arquivos-impressao-order.js';

function escaparHtml(valor) {
    return String(valor || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

export function inicializarArquivosImpressao({ nomeUsuario, podeAdministrar }) {
    const elementos = {
        painel: document.getElementById('painelArquivos'),
        lista: document.getElementById('listaArquivos'),
        busca: document.getElementById('buscaArquivos'),
        carregarMais: document.getElementById('btnCarregarMaisArquivos'),
        adicionar: document.getElementById('btnAdicionarArquivo'),
        modal: document.getElementById('modalArquivo'),
        form: document.getElementById('formArquivo'),
        tituloModal: document.getElementById('tituloModalArquivo'),
        pdf: document.getElementById('pdfArquivo'),
        salvar: document.getElementById('btnSalvarArquivo'),
        cancelar: document.getElementById('btnCancelarArquivo'),
        progresso: document.getElementById('progressoArquivo'),
        barraProgresso: document.getElementById('barraProgressoArquivo'),
        progressoTexto: document.getElementById('textoProgressoArquivo'),
        modalRenomear: document.getElementById('modalRenomearArquivo'),
        formRenomear: document.getElementById('formRenomearArquivo'),
        nomeRenomear: document.getElementById('novoNomeArquivo'),
        salvarRenomear: document.getElementById('btnSalvarNomeArquivo'),
        cancelarRenomear: document.getElementById('btnCancelarNomeArquivo')
    };
    const estado = {
        itens: [],
        busca: '',
        cursor: null,
        ordenacaoDisponivel: true,
        carregando: false,
        erroLista: '',
        observador: null,
        temporizadorBusca: null,
        documentosCompartilhamento: new Map(),
        documentosComFalha: new Set(),
        documentosEmPreparacao: new Set(),
        arquivosPreparados: new Map(),
        encerrarOrdenacao: null,
        arquivoRenomeando: null
    };

    elementos.adicionar.hidden = !podeAdministrar;

    function exibirMensagem(mensagem) {
        elementos.lista.innerHTML = `<div class="estado">${escaparHtml(mensagem)}</div>`;
    }

    function limparObservador() {
        estado.observador?.disconnect();
        estado.observador = null;
    }

    function encerrarOrdenacao() {
        if (typeof estado.encerrarOrdenacao === 'function') estado.encerrarOrdenacao();
        estado.encerrarOrdenacao = null;
    }

    function atualizarBotaoMais() {
        elementos.carregarMais.classList.toggle('oculto', !estado.cursor);
        elementos.carregarMais.disabled = estado.carregando;
        elementos.carregarMais.textContent = estado.carregando ? 'Carregando...' : 'Carregar mais';
    }

    function montarCard(arquivo) {
        const miniatura = arquivo.miniaturaPath
            ? `<div class="arquivo-miniatura" data-miniatura-path="${escaparHtml(arquivo.miniaturaPath)}" aria-label="Miniatura de ${escaparHtml(arquivo.nome)}">PDF</div>`
            : '<div class="arquivo-miniatura" aria-label="Arquivo PDF">PDF</div>';
        const acaoRenomear = `
            <button type="button" class="arquivo-acao-icone acao-editar" data-acao="renomear" data-arquivo-id="${escaparHtml(arquivo.id)}" aria-label="Editar nome de ${escaparHtml(arquivo.nome)}" title="Editar nome">
                <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                    <path d="M12 20h9"></path>
                    <path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L8 18l-4 1 1-4Z"></path>
                </svg>
            </button>`;
        const acaoRemover = podeAdministrar ? `
            <button type="button" class="arquivo-acao-icone acao-perigosa" data-acao="desativar" data-arquivo-id="${escaparHtml(arquivo.id)}" aria-label="Remover ${escaparHtml(arquivo.nome)}" title="Remover arquivo">
                <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                    <path d="M4 7h16"></path>
                    <path d="M9 7V4h6v3"></path>
                    <path d="m6.5 7 .8 13h9.4l.8-13"></path>
                    <path d="M10 11v5M14 11v5"></path>
                </svg>
            </button>` : '';
        const compartilhamentoPronto = estado.documentosCompartilhamento.has(arquivo.id);
        const compartilhamentoFalhou = estado.documentosComFalha.has(arquivo.id);
        const rotuloCompartilhar = compartilhamentoPronto
            ? `Compartilhar ${arquivo.nome}`
            : compartilhamentoFalhou ? 'Arquivo indisponível' : 'Preparando arquivo';
        const classeCard = compartilhamentoPronto
            ? ' compartilhavel'
            : compartilhamentoFalhou ? ' indisponivel' : ' preparando';

        return `
            <article class="arquivo-card${classeCard}" data-card-arquivo-id="${escaparHtml(arquivo.id)}">
                <button type="button" class="arquivo-card-clique" data-acao="compartilhar" data-arquivo-id="${escaparHtml(arquivo.id)}" aria-label="${escaparHtml(rotuloCompartilhar)}" title="${escaparHtml(rotuloCompartilhar)}" ${compartilhamentoPronto ? '' : 'disabled'}></button>
                ${miniatura}
                <div class="arquivo-conteudo">
                    <div class="arquivo-cabecalho">
                        <h3 class="arquivo-nome">${escaparHtml(arquivo.nome)}</h3>
                        ${acaoRemover}
                    </div>
                </div>
                ${acaoRenomear}
            </article>`;
    }

    function ativarOrdenacao() {
        encerrarOrdenacao();
        const haLegadosEmOutraPagina = estado.cursor && estado.cursor.valor === null;
        estado.encerrarOrdenacao = ativarOrdenacaoArquivos({
            container: elementos.lista,
            habilitado: estado.ordenacaoDisponivel
                && !estado.busca
                && !estado.carregando
                && !haLegadosEmOutraPagina,
            aoSalvar: async ids => {
                await dbAtualizarOrdemArquivosImpressao(ids);
                const itensPorId = new Map(estado.itens.map(item => [item.id, item]));
                estado.itens = ids.map((id, indice) => ({
                    ...itensPorId.get(id),
                    ordem: (indice + 1) * 1000
                })).filter(item => item.id);
                if (estado.cursor) {
                    estado.cursor = {
                        valor: ids.length * 1000,
                        id: ids.at(-1)
                    };
                }
            },
            aoFalhar: async erro => {
                console.error('Erro ao atualizar ordem dos arquivos:', erro);
                renderizar();
                await window.playAlert('Não foi possível salvar a nova ordem dos arquivos.');
            }
        });
    }

    function ativarMiniaturas() {
        limparObservador();
        const miniaturas = elementos.lista.querySelectorAll('[data-miniatura-path]');
        if (!miniaturas.length) return;

        const carregarMiniatura = async elemento => {
            const caminho = elemento.dataset.miniaturaPath;
            if (!caminho || elemento.dataset.carregada) return;
            elemento.dataset.carregada = 'true';
            try {
                const url = await dbObterUrlArquivoImpressao(caminho);
                const imagem = document.createElement('img');
                imagem.src = url;
                imagem.alt = '';
                elemento.replaceChildren(imagem);
            } catch (_) {
                // A miniatura é opcional; o cartão continua utilizável com o marcador PDF.
            }
        };

        if (!('IntersectionObserver' in window)) {
            miniaturas.forEach(carregarMiniatura);
            return;
        }
        estado.observador = new IntersectionObserver(entradas => {
            entradas.forEach(entrada => {
                if (!entrada.isIntersecting) return;
                estado.observador?.unobserve(entrada.target);
                carregarMiniatura(entrada.target);
            });
        }, { rootMargin: '180px 0px' });
        miniaturas.forEach(miniatura => estado.observador.observe(miniatura));
    }

    async function prepararDocumentosCompartilhamento() {
        const pendentes = estado.itens.filter(arquivo =>
            !estado.documentosCompartilhamento.has(arquivo.id)
            && !estado.documentosComFalha.has(arquivo.id)
            && !estado.documentosEmPreparacao.has(arquivo.id)
        );
        if (!pendentes.length) return;

        pendentes.forEach(arquivo => estado.documentosEmPreparacao.add(arquivo.id));
        const limite = 3;
        let proximoIndice = 0;
        const trabalhador = async () => {
            while (proximoIndice < pendentes.length) {
                const arquivo = pendentes[proximoIndice++];
                try {
                    const documento = await dbObterArquivoImpressao(arquivo.id);
                    estado.documentosCompartilhamento.set(arquivo.id, documento);
                } catch (erro) {
                    console.warn('Arquivo indisponível para compartilhamento:', erro);
                    estado.documentosComFalha.add(arquivo.id);
                } finally {
                    estado.documentosEmPreparacao.delete(arquivo.id);
                }
            }
        };
        await Promise.all(Array.from({ length: Math.min(limite, pendentes.length) }, trabalhador));
        renderizar();
    }

    function renderizar() {
        encerrarOrdenacao();
        if (estado.erroLista) {
            exibirMensagem(estado.erroLista);
            atualizarBotaoMais();
            return;
        }
        if (!estado.itens.length) {
            exibirMensagem(estado.carregando ? 'Carregando arquivos...' : 'Nenhum arquivo encontrado.');
            atualizarBotaoMais();
            return;
        }
        elementos.lista.innerHTML = estado.itens.map(montarCard).join('');
        ativarMiniaturas();
        ativarOrdenacao();
        atualizarBotaoMais();
        prepararDocumentosCompartilhamento();
    }

    async function carregar({ acrescentar = false } = {}) {
        if (estado.carregando) return;
        estado.carregando = true;
        estado.erroLista = '';
        if (!acrescentar) {
            estado.cursor = null;
            estado.itens = [];
            renderizar();
        } else {
            atualizarBotaoMais();
        }

        try {
            const pagina = await dbListarArquivosImpressao({
                busca: estado.busca,
                cursor: acrescentar ? estado.cursor : null
            });
            estado.itens = acrescentar ? [...estado.itens, ...pagina.itens] : pagina.itens;
            estado.cursor = pagina.proximoCursor;
            estado.ordenacaoDisponivel = pagina.ordenacaoDisponivel;
        } catch (erro) {
            console.error('Erro ao listar arquivos para impressão:', erro);
            estado.erroLista = 'Não foi possível carregar os arquivos. Verifique a internet e tente novamente.';
            estado.cursor = null;
        } finally {
            estado.carregando = false;
            renderizar();
        }
    }

    async function compartilharArquivo(id, botao) {
        if (botao?.disabled) return;
        const documento = estado.documentosCompartilhamento.get(id);
        if (!documento) return;
        const rotuloPadrao = `Compartilhar ${documento.nome || 'arquivo'}`;
        const card = botao.closest('.arquivo-card');
        const atualizarEstadoBotao = (rotulo, carregando = false) => {
            botao.setAttribute('aria-label', rotulo);
            botao.title = rotulo;
            botao.setAttribute('aria-busy', String(carregando));
            card?.classList.toggle('compartilhando', carregando);
        };
        if (!navigator.share) {
            await window.playAlert('O compartilhamento nativo não é suportado neste navegador.');
            return;
        }
        try {
            let arquivo = estado.arquivosPreparados.get(id);
            if (!arquivo) {
                botao.disabled = true;
                atualizarEstadoBotao('Preparando PDF', true);
                const blob = await dbBaixarArquivoImpressao(documento.storagePath);
                arquivo = new File([blob], `${documento.nome}.pdf`, {
                    type: 'application/pdf'
                });
                estado.arquivosPreparados.set(id, arquivo);
            }

            if (typeof navigator.canShare === 'function' && !navigator.canShare({ files: [arquivo] })) {
                throw new Error('Este navegador não permite compartilhar PDF como arquivo.');
            }
            atualizarEstadoBotao('Abrindo opções de compartilhamento', true);
            await navigator.share({ files: [arquivo] });
        } catch (erro) {
            if (erro?.name === 'AbortError') return;
            if (erro?.name === 'NotAllowedError' && estado.arquivosPreparados.has(id)) {
                await window.playAlert('O PDF foi preparado. Toque em Compartilhar novamente para abrir as opções do celular.');
                return;
            }
            console.error('Erro ao compartilhar arquivo para impressão:', erro);
            await window.playAlert(erro?.message || 'Não foi possível compartilhar este arquivo.');
        } finally {
            botao.disabled = false;
            atualizarEstadoBotao(rotuloPadrao);
        }
    }

    function abrirModal() {
        elementos.form.reset();
        elementos.tituloModal.textContent = 'Adicionar arquivo';
        elementos.pdf.required = true;
        elementos.salvar.textContent = 'Enviar';
        elementos.progresso.classList.add('oculto');
        elementos.modal.classList.add('aberto');
        document.body.style.overflow = 'hidden';
        setTimeout(() => elementos.pdf.focus(), 0);
    }

    function fecharModal() {
        elementos.modal.classList.remove('aberto');
        document.body.style.overflow = '';
        elementos.form.reset();
    }

    function abrirModalRenomear(id) {
        const arquivo = estado.itens.find(item => item.id === id);
        if (!arquivo) return;
        estado.arquivoRenomeando = arquivo;
        elementos.formRenomear.reset();
        elementos.nomeRenomear.value = arquivo.nome || '';
        elementos.modalRenomear.classList.add('aberto');
        document.body.style.overflow = 'hidden';
        setTimeout(() => {
            elementos.nomeRenomear.focus();
            elementos.nomeRenomear.select();
        }, 0);
    }

    function fecharModalRenomear() {
        elementos.modalRenomear.classList.remove('aberto');
        estado.arquivoRenomeando = null;
        elementos.formRenomear.reset();
        if (!elementos.modal.classList.contains('aberto')) document.body.style.overflow = '';
    }

    async function desativar(id) {
        const confirmar = await window.playConfirm('Remover este arquivo da lista? As versões salvas não serão apagadas.', {
            primaryLabel: 'Remover',
            secondaryLabel: 'Cancelar'
        });
        if (!confirmar) return;
        try {
            await dbDesativarArquivoImpressao(id, nomeUsuario);
            await carregar();
        } catch (erro) {
            console.error('Erro ao desativar arquivo para impressão:', erro);
            await window.playAlert(erro?.message || 'Não foi possível remover o arquivo.');
        }
    }

    elementos.busca.addEventListener('input', () => {
        clearTimeout(estado.temporizadorBusca);
        estado.temporizadorBusca = window.setTimeout(() => {
            estado.busca = elementos.busca.value;
            carregar();
        }, 350);
    });
    elementos.carregarMais.addEventListener('click', () => carregar({ acrescentar: true }));
    elementos.adicionar.addEventListener('click', () => abrirModal());
    elementos.cancelar.addEventListener('click', fecharModal);
    elementos.modal.addEventListener('click', evento => {
        if (evento.target === elementos.modal) fecharModal();
    });
    elementos.cancelarRenomear.addEventListener('click', fecharModalRenomear);
    elementos.modalRenomear.addEventListener('click', evento => {
        if (evento.target === elementos.modalRenomear) fecharModalRenomear();
    });
    elementos.lista.addEventListener('click', async evento => {
        const botao = evento.target.closest('[data-acao][data-arquivo-id]');
        if (!botao) return;
        const { acao, arquivoId } = botao.dataset;
        if (acao === 'desativar' && podeAdministrar) return desativar(arquivoId);
        if (acao === 'renomear') return abrirModalRenomear(arquivoId);
        if (acao === 'compartilhar') return compartilharArquivo(arquivoId, botao);
    });
    elementos.formRenomear.addEventListener('submit', async evento => {
        evento.preventDefault();
        const arquivo = estado.arquivoRenomeando;
        if (!arquivo || elementos.salvarRenomear.disabled) return;
        elementos.salvarRenomear.disabled = true;
        elementos.salvarRenomear.textContent = 'Salvando...';
        try {
            await dbRenomearArquivoImpressao(
                arquivo.id,
                elementos.nomeRenomear.value,
                nomeUsuario
            );
            estado.documentosCompartilhamento.delete(arquivo.id);
            estado.documentosComFalha.delete(arquivo.id);
            estado.arquivosPreparados.delete(arquivo.id);
            fecharModalRenomear();
            await carregar();
            await window.playAlert('Nome do arquivo atualizado.');
        } catch (erro) {
            console.error('Erro ao renomear arquivo para impressão:', erro);
            await window.playAlert(erro?.message || 'Não foi possível atualizar o nome do arquivo.');
        } finally {
            elementos.salvarRenomear.disabled = false;
            elementos.salvarRenomear.textContent = 'Salvar';
        }
    });
    elementos.form.addEventListener('submit', async evento => {
        evento.preventDefault();
        if (elementos.salvar.disabled) return;
        const arquivo = elementos.pdf.files?.[0];
        elementos.salvar.disabled = true;
        elementos.salvar.textContent = 'Enviando...';
        elementos.progresso.classList.remove('oculto');
        elementos.barraProgresso.removeAttribute('value');
        elementos.progressoTexto.textContent = 'Criando miniatura da primeira página...';
        let miniatura = null;
        let miniaturaFalhou = false;
        try {
            try {
                miniatura = await gerarMiniaturaPdf(arquivo);
            } catch (erroMiniatura) {
                miniaturaFalhou = true;
                console.warn('Não foi possível gerar a miniatura automática:', erroMiniatura);
            }
            elementos.barraProgresso.value = 0;
            elementos.progressoTexto.textContent = 'Preparando envio...';
            const dados = {
                arquivo,
                miniatura,
                titulo: String(arquivo?.name || '').replace(/\.pdf$/i, ''),
                usuario: nomeUsuario,
                aoProgresso: percentual => {
                    elementos.barraProgresso.value = percentual;
                    elementos.progressoTexto.textContent = `Enviando PDF: ${percentual}%`;
                }
            };
            await dbCadastrarArquivoImpressao(dados);
            fecharModal();
            await carregar();
            await window.playAlert(miniaturaFalhou
                ? 'Arquivo salvo. Não foi possível criar a miniatura; ele será exibido com o ícone PDF.'
                : 'Arquivo salvo com sucesso.');
        } catch (erro) {
            console.error('Erro ao salvar arquivo para impressão:', erro);
            await window.playAlert(erro?.message || 'Não foi possível salvar o arquivo.');
        } finally {
            elementos.salvar.disabled = false;
            elementos.salvar.textContent = 'Enviar';
            elementos.progresso.classList.add('oculto');
        }
    });

    return {
        abrir({ registrarHistorico = true } = {}) {
            elementos.painel.classList.remove('oculto');
            if (registrarHistorico) {
                history.pushState({ ...history.state, telaArquivosImpressao: 'arquivos' }, '');
            }
            carregar();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        },
        fechar() {
            clearTimeout(estado.temporizadorBusca);
            limparObservador();
            encerrarOrdenacao();
            fecharModal();
            fecharModalRenomear();
            elementos.painel.classList.add('oculto');
        }
    };
}
