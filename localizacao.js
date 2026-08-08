import { nomeParaEmail, verificarAutenticacao } from './auth.js';
import {
    buscarColaboradorPorNome,
    escutarLocalizacoesDoUsuario,
    estaEmPreviaLocal,
    listarColaboradoresPagina,
    solicitarLocalizacao
} from './localizacao-service.js';
import { buscarUsuarioSeguroPorEmail, usuarioTemPermissao } from './permissoes-service.js';
import {
    historicoComoLista,
    perfilPodeGerenciarLocalizacao,
    perfilEhOperador,
    textoNormalizado
} from './localizacao-rules.js';

const TAMANHO_PAGINA = 12;
const estado = {
    gestor: null,
    cursor: '',
    temMais: true,
    carregando: false,
    operadores: new Map(),
    listeners: new Map()
};

const app = document.getElementById('appLocalizacao');
const lista = document.getElementById('listaOperadores');
const carregando = document.getElementById('estadoCarregando');
const vazio = document.getElementById('estadoVazio');
const erro = document.getElementById('estadoErro');
const btnCarregar = document.getElementById('btnCarregarMais');

function colaboradorNoCache(nome) {
    try {
        const colaboradores = JSON.parse(localStorage.getItem('cache_colaboradores') || '[]');
        const procurado = textoNormalizado(nome);
        return Array.isArray(colaboradores)
            ? colaboradores.find((item) => textoNormalizado(item?.nome) === procurado) || null
            : null;
    } catch (_) {
        return null;
    }
}

function formatarDataHora(timestamp) {
    const numero = Number(timestamp || 0);
    if (!numero) return 'Data não informada';
    return new Intl.DateTimeFormat('pt-BR', {
        timeZone: 'America/Sao_Paulo',
        dateStyle: 'short',
        timeStyle: 'short'
    }).format(new Date(numero));
}

function textoStatus(dados) {
    const pedido = dados?.solicitacao_atual || null;
    if (!pedido) return { classe: '', texto: 'Nenhuma solicitação realizada' };
    if (pedido.status === 'pendente') {
        return { classe: 'pendente', texto: 'Solicitação pendente' };
    }
    if (pedido.status === 'permissao_negada') {
        return { classe: 'negada', texto: 'Permissão de localização negada' };
    }
    if (pedido.status === 'respondida') {
        return { classe: 'recebida', texto: 'Localização recebida' };
    }
    return { classe: '', texto: 'Aguardando nova solicitação' };
}

function criarCard(operador) {
    const card = document.createElement('article');
    card.className = 'card-operador';
    card.dataset.colaboradorId = operador.firebaseUrl;

    const topo = document.createElement('div');
    topo.className = 'card-topo';
    const identidade = document.createElement('div');
    const nome = document.createElement('h2');
    nome.className = 'nome-operador';
    nome.textContent = String(operador.nome || 'Operador');
    const status = document.createElement('p');
    status.className = 'status-operador';
    status.textContent = operador.localizacaoUid
        ? 'Carregando localizações...'
        : 'Aguardando vínculo seguro';
    identidade.append(nome, status);

    const atualizar = document.createElement('button');
    atualizar.type = 'button';
    atualizar.className = 'btn-atualizar';
    atualizar.textContent = operador.localizacaoUid ? 'Atualizar' : 'Indisponível';
    atualizar.disabled = !operador.localizacaoUid;
    atualizar.addEventListener('click', () => solicitarNovamente(operador, atualizar, status));

    topo.append(identidade, atualizar);
    const historico = document.createElement('div');
    historico.className = 'historico';
    card.append(topo, historico);
    lista.appendChild(card);

    return { card, status, historico };
}

function renderizarHistorico(elementos, dados) {
    const statusAtual = textoStatus(dados);
    elementos.status.className = `status-operador ${statusAtual.classe}`.trim();
    elementos.status.textContent = statusAtual.texto;
    elementos.historico.replaceChildren();

    const registros = historicoComoLista(dados?.historico);
    if (!registros.length) {
        const semDados = document.createElement('p');
        semDados.className = 'sem-localizacao';
        semDados.textContent = 'Nenhuma localização recebida.';
        elementos.historico.appendChild(semDados);
        return;
    }

    registros.forEach((registro) => {
        const item = document.createElement('div');
        item.className = 'localizacao-item';
        const info = document.createElement('div');
        const data = document.createElement('span');
        data.className = 'localizacao-data';
        data.textContent = formatarDataHora(registro.recebidaEmCliente || registro.capturadaEm);
        const detalhe = document.createElement('span');
        detalhe.className = 'localizacao-detalhe';
        detalhe.textContent = `Precisão aproximada: ${Math.round(Number(registro.precisaoMetros || 0))} m`;
        info.append(data, detalhe);

        const mapa = document.createElement('a');
        mapa.className = 'btn-mapa';
        mapa.textContent = 'Mapa';
        mapa.target = '_blank';
        mapa.rel = 'noopener noreferrer';
        mapa.href = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${registro.latitude},${registro.longitude}`)}`;
        item.append(info, mapa);
        elementos.historico.appendChild(item);
    });
}

function renderizarSemVinculo(elementos) {
    const aviso = document.createElement('p');
    aviso.className = 'sem-localizacao';
    aviso.textContent = 'O acesso será liberado após a vinculação segura da conta.';
    elementos.historico.appendChild(aviso);
}

function acompanharOperador(operador) {
    if (estado.operadores.has(operador.firebaseUrl)) return;
    estado.operadores.set(operador.firebaseUrl, operador);
    const elementos = criarCard(operador);
    if (!operador.localizacaoUid) {
        renderizarSemVinculo(elementos);
        return;
    }
    if (estado.listeners.has(operador.localizacaoUid)) return;
    const cancelar = escutarLocalizacoesDoUsuario(
        operador.localizacaoUid,
        (dados) => renderizarHistorico(elementos, dados),
        () => {
            elementos.status.className = 'status-operador erro';
            elementos.status.textContent = 'Não foi possível carregar';
        }
    );
    estado.listeners.set(operador.localizacaoUid, cancelar);
}

async function solicitarNovamente(operador, botao, status) {
    if (botao.disabled || !operador.localizacaoUid) return;
    botao.disabled = true;
    botao.textContent = 'Solicitando...';
    erro.hidden = true;
    try {
        await solicitarLocalizacao({ colaborador: operador, gestor: estado.gestor });
        status.className = 'status-operador pendente';
        status.textContent = 'Solicitação pendente';
    } catch (_) {
        status.className = 'status-operador erro';
        status.textContent = estaEmPreviaLocal()
            ? 'Solicitação desativada na prévia local'
            : 'Falha ao solicitar localização';
    } finally {
        botao.disabled = false;
        botao.textContent = 'Atualizar';
    }
}

async function vincularOperadorComUid(operador) {
    const usuarioSeguro = await buscarUsuarioSeguroPorEmail(nomeParaEmail(operador?.nome));
    return {
        ...operador,
        localizacaoUid: String(usuarioSeguro?.uid || '').trim()
    };
}

async function carregarMaisOperadores() {
    if (estado.carregando || !estado.temMais) return;
    estado.carregando = true;
    btnCarregar.disabled = true;
    btnCarregar.textContent = 'Carregando...';
    erro.hidden = true;
    try {
        const pagina = await listarColaboradoresPagina({
            cursor: estado.cursor,
            limite: TAMANHO_PAGINA
        });
        estado.cursor = pagina.proximoCursor;
        estado.temMais = pagina.temMais;
        const operadoresBase = pagina.itens.filter((item) => perfilEhOperador(item?.nivel_completo));
        const operadores = await Promise.all(operadoresBase.map(vincularOperadorComUid));
        operadores.forEach(acompanharOperador);
        vazio.hidden = estado.operadores.size > 0 || estado.temMais;
        btnCarregar.hidden = !estado.temMais;
    } catch (_) {
        erro.textContent = 'Não foi possível carregar os operadores.';
        erro.hidden = false;
    } finally {
        carregando.hidden = true;
        estado.carregando = false;
        btnCarregar.disabled = false;
        btnCarregar.textContent = 'Carregar mais';
    }
}

async function iniciarTela() {
    const usuario = await verificarAutenticacao();
    if (!usuario) return;
    const nome = String(usuario.displayName || localStorage.getItem('usuarioLogado') || '').trim();
    const colaboradorGestor = colaboradorNoCache(nome) || await buscarColaboradorPorNome(nome);
    const autorizadoNaTela = perfilPodeGerenciarLocalizacao(colaboradorGestor?.nivel_completo);
    const autorizadoNaBase = autorizadoNaTela
        ? await usuarioTemPermissao(usuario.uid, 'localizacao_gestor').catch(() => false)
        : false;
    if (!autorizadoNaTela || !autorizadoNaBase) {
        window.location.replace('index.html');
        return;
    }
    estado.gestor = usuario;
    app.hidden = false;
    document.getElementById('btnHome').addEventListener('click', () => {
        window.location.href = 'index.html';
    });
    btnCarregar.addEventListener('click', carregarMaisOperadores);
    await carregarMaisOperadores();
}

window.addEventListener('pagehide', () => {
    estado.listeners.forEach((cancelar) => {
        if (typeof cancelar === 'function') cancelar();
    });
    estado.listeners.clear();
}, { once: true });

iniciarTela().catch(() => {
    app.hidden = false;
    carregando.hidden = true;
    erro.textContent = 'Não foi possível abrir a tela de localização.';
    erro.hidden = false;
});
