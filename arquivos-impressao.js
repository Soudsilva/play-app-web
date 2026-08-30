import {
    dbCadastrarArquivoImpressao,
    dbDesativarArquivoImpressao,
    dbListarArquivosImpressao,
    dbObterArquivoImpressao,
    dbObterUrlArquivoImpressao,
    dbSubstituirArquivoImpressao
} from './arquivos-impressao-service.js';
import { formatarDataArquivo, formatarTamanhoArquivo } from './arquivos-impressao-rules.js';

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
        nome: document.getElementById('nomeArquivo'),
        pdf: document.getElementById('pdfArquivo'),
        miniatura: document.getElementById('miniaturaArquivo'),
        salvar: document.getElementById('btnSalvarArquivo'),
        cancelar: document.getElementById('btnCancelarArquivo'),
        progresso: document.getElementById('progressoArquivo'),
        progressoTexto: document.getElementById('textoProgressoArquivo')
    };
    const estado = {
        itens: [],
        busca: '',
        cursor: '',
        carregando: false,
        erroLista: '',
        arquivoEmEdicao: null,
        observador: null,
        temporizadorBusca: null,
        urlsCompartilhamento: new Map(),
        urlsComFalha: new Set(),
        urlsEmPreparacao: new Set(),
        arquivosPreparados: new Map()
    };

    elementos.adicionar.hidden = !podeAdministrar;

    function exibirMensagem(mensagem) {
        elementos.lista.innerHTML = `<div class="estado">${escaparHtml(mensagem)}</div>`;
    }

    function limparObservador() {
        estado.observador?.disconnect();
        estado.observador = null;
    }

    function atualizarBotaoMais() {
        elementos.carregarMais.classList.toggle('oculto', !estado.cursor);
        elementos.carregarMais.disabled = estado.carregando;
        elementos.carregarMais.textContent = estado.carregando ? 'Carregando...' : 'Carregar mais';
    }

    function montarCard(arquivo) {
        const detalhes = [
            'PDF',
            formatarTamanhoArquivo(arquivo.tamanhoBytes),
            arquivo.versaoAtual ? `Versão ${arquivo.versaoAtual}` : '',
            arquivo.atualizadoEm ? `Atualizado em ${formatarDataArquivo(arquivo.atualizadoEm)}` : ''
        ].filter(Boolean).join(' · ');
        const miniatura = arquivo.miniaturaPath
            ? `<div class="arquivo-miniatura" data-miniatura-path="${escaparHtml(arquivo.miniaturaPath)}" aria-label="Miniatura de ${escaparHtml(arquivo.nome)}">PDF</div>`
            : '<div class="arquivo-miniatura" aria-label="Arquivo PDF">PDF</div>';
        const acoesGestor = podeAdministrar ? `
            <button type="button" data-acao="substituir" data-arquivo-id="${escaparHtml(arquivo.id)}">Substituir</button>
            <button type="button" class="acao-perigosa" data-acao="desativar" data-arquivo-id="${escaparHtml(arquivo.id)}">Remover</button>` : '';
        const compartilhamentoPronto = estado.urlsCompartilhamento.has(arquivo.id);
        const compartilhamentoFalhou = estado.urlsComFalha.has(arquivo.id);
        const textoCompartilhar = compartilhamentoPronto
            ? 'Compartilhar'
            : compartilhamentoFalhou ? 'Arquivo indisponível' : 'Preparando...';

        return `
            <article class="arquivo-card">
                ${miniatura}
                <div class="arquivo-conteudo">
                    <h3 class="arquivo-nome">${escaparHtml(arquivo.nome)}</h3>
                    <p class="arquivo-detalhes">${escaparHtml(detalhes)}</p>
                    <div class="arquivo-acoes">
                        <button type="button" class="acao-principal" data-acao="compartilhar" data-arquivo-id="${escaparHtml(arquivo.id)}" ${compartilhamentoPronto ? '' : 'disabled'}>${textoCompartilhar}</button>
                        ${acoesGestor}
                    </div>
                </div>
            </article>`;
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

    async function prepararUrlsCompartilhamento() {
        const pendentes = estado.itens.filter(arquivo =>
            !estado.urlsCompartilhamento.has(arquivo.id)
            && !estado.urlsComFalha.has(arquivo.id)
            && !estado.urlsEmPreparacao.has(arquivo.id)
        );
        if (!pendentes.length) return;

        pendentes.forEach(arquivo => estado.urlsEmPreparacao.add(arquivo.id));
        const limite = 3;
        let proximoIndice = 0;
        const trabalhador = async () => {
            while (proximoIndice < pendentes.length) {
                const arquivo = pendentes[proximoIndice++];
                try {
                    const documento = await dbObterArquivoImpressao(arquivo.id);
                    const url = await dbObterUrlArquivoImpressao(documento.storagePath);
                    estado.urlsCompartilhamento.set(arquivo.id, { documento, url });
                } catch (erro) {
                    console.warn('Arquivo indisponível para compartilhamento:', erro);
                    estado.urlsComFalha.add(arquivo.id);
                } finally {
                    estado.urlsEmPreparacao.delete(arquivo.id);
                }
            }
        };
        await Promise.all(Array.from({ length: Math.min(limite, pendentes.length) }, trabalhador));
        renderizar();
    }

    function renderizar() {
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
        atualizarBotaoMais();
        prepararUrlsCompartilhamento();
    }

    async function carregar({ acrescentar = false } = {}) {
        if (estado.carregando) return;
        estado.carregando = true;
        estado.erroLista = '';
        if (!acrescentar) {
            estado.cursor = '';
            estado.itens = [];
            renderizar();
        } else {
            atualizarBotaoMais();
        }

        try {
            const pagina = await dbListarArquivosImpressao({
                busca: estado.busca,
                cursor: acrescentar ? estado.cursor : ''
            });
            estado.itens = acrescentar ? [...estado.itens, ...pagina.itens] : pagina.itens;
            estado.cursor = pagina.proximoCursor;
        } catch (erro) {
            console.error('Erro ao listar arquivos para impressão:', erro);
            estado.erroLista = 'Não foi possível carregar os arquivos. Verifique a internet e tente novamente.';
            estado.cursor = '';
        } finally {
            estado.carregando = false;
            renderizar();
        }
    }

    async function compartilharArquivo(id, botao) {
        if (botao?.disabled) return;
        const compartilhamento = estado.urlsCompartilhamento.get(id);
        if (!compartilhamento) return;
        const { documento, url } = compartilhamento;
        const textoOriginal = botao?.textContent || 'Compartilhar';
        if (!navigator.share) {
            await window.playAlert('O compartilhamento nativo não é suportado neste navegador.');
            return;
        }
        try {
            let arquivo = estado.arquivosPreparados.get(id);
            if (!arquivo) {
                botao.disabled = true;
                botao.textContent = 'Preparando PDF...';
                const resposta = await fetch(url);
                if (!resposta.ok) throw new Error('Não foi possível baixar o PDF para compartilhar.');
                const blob = await resposta.blob();
                arquivo = new File([blob], `${documento.nome}.pdf`, {
                    type: blob.type || 'application/pdf'
                });
                estado.arquivosPreparados.set(id, arquivo);
            }

            if (typeof navigator.canShare === 'function' && !navigator.canShare({ files: [arquivo] })) {
                throw new Error('Este navegador não permite compartilhar PDF como arquivo.');
            }
            await navigator.share({ title: documento.nome, files: [arquivo] });
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
            botao.textContent = textoOriginal;
        }
    }

    function abrirModal(arquivo = null) {
        estado.arquivoEmEdicao = arquivo;
        elementos.form.reset();
        elementos.tituloModal.textContent = arquivo ? 'Substituir arquivo' : 'Adicionar arquivo';
        elementos.nome.value = arquivo?.nome || '';
        elementos.pdf.required = true;
        elementos.salvar.textContent = arquivo ? 'Substituir' : 'Enviar';
        elementos.progresso.classList.add('oculto');
        elementos.modal.classList.add('aberto');
        document.body.style.overflow = 'hidden';
        setTimeout(() => elementos.nome.focus(), 0);
    }

    function fecharModal() {
        elementos.modal.classList.remove('aberto');
        document.body.style.overflow = '';
        estado.arquivoEmEdicao = null;
        elementos.form.reset();
    }

    async function substituir(id) {
        try {
            const arquivo = await dbObterArquivoImpressao(id);
            abrirModal(arquivo);
        } catch (erro) {
            await window.playAlert(erro?.message || 'Não foi possível preparar a substituição.');
        }
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
    elementos.lista.addEventListener('click', async evento => {
        const botao = evento.target.closest('[data-acao][data-arquivo-id]');
        if (!botao) return;
        const { acao, arquivoId } = botao.dataset;
        if (acao === 'substituir' && podeAdministrar) return substituir(arquivoId);
        if (acao === 'desativar' && podeAdministrar) return desativar(arquivoId);
        if (acao === 'compartilhar') return compartilharArquivo(arquivoId, botao);
    });
    elementos.pdf.addEventListener('change', () => {
        if (estado.arquivoEmEdicao || elementos.nome.value.trim()) return;
        const arquivo = elementos.pdf.files?.[0];
        if (arquivo) elementos.nome.value = arquivo.name.replace(/\.pdf$/i, '');
    });
    elementos.form.addEventListener('submit', async evento => {
        evento.preventDefault();
        if (elementos.salvar.disabled) return;
        const arquivo = elementos.pdf.files?.[0];
        const miniatura = elementos.miniatura.files?.[0];
        elementos.salvar.disabled = true;
        elementos.salvar.textContent = estado.arquivoEmEdicao ? 'Substituindo...' : 'Enviando...';
        elementos.progresso.classList.remove('oculto');
        elementos.progresso.value = 0;
        elementos.progressoTexto.textContent = 'Preparando envio...';
        try {
            const dados = {
                arquivo,
                miniatura,
                titulo: elementos.nome.value,
                usuario: nomeUsuario,
                aoProgresso: percentual => {
                    elementos.progresso.value = percentual;
                    elementos.progressoTexto.textContent = `Enviando PDF: ${percentual}%`;
                }
            };
            if (estado.arquivoEmEdicao) {
                await dbSubstituirArquivoImpressao({ ...dados, id: estado.arquivoEmEdicao.id });
                estado.arquivosPreparados.delete(estado.arquivoEmEdicao.id);
            } else {
                await dbCadastrarArquivoImpressao(dados);
            }
            fecharModal();
            await carregar();
            await window.playAlert('Arquivo salvo com sucesso.');
        } catch (erro) {
            console.error('Erro ao salvar arquivo para impressão:', erro);
            await window.playAlert(erro?.message || 'Não foi possível salvar o arquivo.');
        } finally {
            elementos.salvar.disabled = false;
            elementos.salvar.textContent = estado.arquivoEmEdicao ? 'Substituir' : 'Enviar';
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
            fecharModal();
            elementos.painel.classList.add('oculto');
        }
    };
}
