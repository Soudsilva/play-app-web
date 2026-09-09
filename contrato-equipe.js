// A Equipe reutiliza o cadastro já carregado pela tela, sem novas consultas ou gravações.

function obterNumero(valor) {
    if (typeof valor === 'number') return Number.isFinite(valor) ? valor : null;

    const texto = String(valor ?? '')
        .replace(/%/g, '')
        .replace(/\s+/g, '')
        .replace(/\.(?=\d{3}(?:\D|$))/g, '')
        .replace(',', '.');
    if (!texto) return null;

    const numero = Number(texto);
    return Number.isFinite(numero) ? numero : null;
}

function formatarPercentual(valor) {
    if (valor === true) return 'Ativo';
    const numero = obterNumero(valor);
    if (numero === null || numero <= 0) return '';
    return `${numero.toLocaleString('pt-BR', { maximumFractionDigits: 2 })}%`;
}

function formatarMoeda(valor) {
    const numero = obterNumero(valor);
    if (numero === null || numero <= 0) return '';
    return numero.toLocaleString('pt-BR', {
        style: 'currency',
        currency: 'BRL',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    });
}

function criarTexto(documento, classe, texto) {
    const elemento = documento.createElement('div');
    elemento.className = classe;
    elemento.textContent = texto;
    return elemento;
}

function criarLinhaDetalhe(documento, rotulo, valor) {
    const item = documento.createElement('div');
    item.className = 'detalhe-row';
    const label = documento.createElement('span');
    label.className = 'label';
    label.textContent = rotulo;
    const dado = documento.createElement('strong');
    dado.className = 'valor';
    dado.textContent = valor;
    item.append(label, dado);
    return item;
}

function obterRemuneracoes(colaborador) {
    return [
        ['Comissão por produção', formatarPercentual(colaborador.comissao)],
        ['Comissão global', formatarPercentual(colaborador.comissaoGlobal)],
        ['Representante', formatarPercentual(colaborador.representanteComissao)],
        ['Fixo', formatarMoeda(colaborador.fixo)],
        ['Bônus fixo', formatarMoeda(colaborador.bonusFixo)],
        ['Bônus produção', formatarMoeda(colaborador.bonusManutencao)]
    ].filter(([, valor]) => valor);
}

function obterModalidades(colaborador) {
    return [
        [colaborador.producaoProdutos, 'Prod. Produtos'],
        [colaborador.producaoEquipamentos, 'Prod. Equipamentos'],
        [colaborador.producaoPecas, 'Prod. Peças'],
        [colaborador.prestacaoServico, 'Prest. Serviço']
    ].filter(([ativo]) => ativo === true).map(([, rotulo]) => rotulo);
}

export function renderizarEquipe(container, colaboradores) {
    const documento = container.ownerDocument;
    const fragmento = documento.createDocumentFragment();
    const lista = Array.isArray(colaboradores) ? colaboradores : [];

    let indiceCard = 0;
    for (const colaborador of lista) {
        const nome = typeof colaborador?.nome === 'string' ? colaborador.nome.trim() : '';
        if (!nome) continue;

        const card = documento.createElement('div');
        card.className = 'card equipe-card';

        const cabecalho = documento.createElement('button');
        cabecalho.type = 'button';
        cabecalho.className = 'colab-card equipe-card-toggle';
        cabecalho.setAttribute('aria-expanded', 'false');

        const info = documento.createElement('div');
        info.className = 'colab-info';
        info.append(criarTexto(documento, 'colab-nome', nome));

        const cargo = typeof colaborador.cargo === 'string' ? colaborador.cargo.trim() : '';
        if (cargo) info.append(criarTexto(documento, 'colab-cargo', cargo));

        const remuneracoes = obterRemuneracoes(colaborador);
        const modalidades = obterModalidades(colaborador);
        const indicador = documento.createElement('span');
        indicador.className = 'equipe-expandir-indicador';
        indicador.setAttribute('aria-hidden', 'true');
        indicador.textContent = '▾';
        cabecalho.append(info, indicador);
        card.append(cabecalho);

        const detalhe = documento.createElement('div');
        detalhe.className = 'colab-detalhe';
        detalhe.id = `equipe-detalhe-${indiceCard++}`;
        detalhe.hidden = true;
        cabecalho.setAttribute('aria-controls', detalhe.id);
        remuneracoes.forEach(([rotulo, valor]) => detalhe.append(criarLinhaDetalhe(documento, rotulo, valor)));
        if (modalidades.length) {
            const atividades = documento.createElement('div');
            atividades.className = 'equipe-atividades';
            atividades.append(criarTexto(documento, 'equipe-atividades-label', 'Remuneração'));
            const listaAtividades = documento.createElement('div');
            listaAtividades.className = 'equipe-atividades-lista';
            modalidades.forEach(rotulo => listaAtividades.append(criarTexto(documento, 'equipe-atividade', rotulo)));
            atividades.append(listaAtividades);
            detalhe.append(atividades);
        }
        if (!remuneracoes.length && !modalidades.length) {
            detalhe.append(criarTexto(documento, 'equipe-sem-remuneracao', 'Remuneração não cadastrada'));
        }
        cabecalho.addEventListener('click', () => {
            const deveExpandir = cabecalho.getAttribute('aria-expanded') !== 'true';
            cabecalho.setAttribute('aria-expanded', String(deveExpandir));
            indicador.textContent = deveExpandir ? '▴' : '▾';
            detalhe.hidden = !deveExpandir;
        });
        if (detalhe.childNodes.length) card.append(detalhe);
        fragmento.append(card);
    }

    if (!fragmento.childNodes.length) {
        const aviso = documento.createElement('div');
        aviso.className = 'loading';
        aviso.textContent = 'Nenhum colaborador disponível. Verifique a conexão para atualizar a equipe.';
        fragmento.append(aviso);
    }
    container.replaceChildren(fragmento);
}
