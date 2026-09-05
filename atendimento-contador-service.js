import { get, ref } from 'https://www.gstatic.com/firebasejs/10.8.0/firebase-database.js';
import { db } from './firebase-app.js';
import { extrairContadoresEquipDetalhes } from './atendimento-contador-rules.mjs';

const DB_NOME = 'play-contadores-rotas';
const DB_VERSAO = 1;
const STORE_ROTAS = 'rotas';
const LIMITE_CONCORRENCIA = 8;
const PRAZO_TOTAL_MS = 15000;
const carregamentosEmAndamento = new Map();

function normalizarTextoChave(valor) {
    return String(valor ?? '').trim().toLocaleLowerCase('pt-BR');
}

function normalizarNumeroRota(valor) {
    return String(valor ?? '').trim();
}

function criarChaveRota(usuario, numeroRota) {
    return `${normalizarTextoChave(usuario)}::${normalizarNumeroRota(numeroRota)}`;
}

function abrirBanco() {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NOME, DB_VERSAO);
        request.onupgradeneeded = () => {
            const banco = request.result;
            if (!banco.objectStoreNames.contains(STORE_ROTAS)) {
                const store = banco.createObjectStore(STORE_ROTAS, { keyPath: 'chave' });
                store.createIndex('usuario', 'usuario', { unique: false });
            }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error || new Error('Falha ao abrir o cache de contadores.'));
    });
}

async function executarStore(modo, executar) {
    const banco = await abrirBanco();
    try {
        return await new Promise((resolve, reject) => {
            const transacao = banco.transaction(STORE_ROTAS, modo);
            const store = transacao.objectStore(STORE_ROTAS);
            let resultado;

            transacao.oncomplete = () => resolve(resultado);
            transacao.onerror = () => reject(transacao.error || new Error('Falha no cache de contadores.'));
            transacao.onabort = () => reject(transacao.error || new Error('Operação cancelada no cache de contadores.'));

            resultado = executar(store);
        });
    } finally {
        banco.close();
    }
}

function aguardarRequest(request) {
    return new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error || new Error('Falha ao consultar o cache de contadores.'));
    });
}

async function lerRegistroRota(usuario, numeroRota) {
    const chave = criarChaveRota(usuario, numeroRota);
    return executarStore('readonly', store => aguardarRequest(store.get(chave)));
}

async function salvarRegistroRota(registro) {
    await executarStore('readwrite', store => store.put(registro));
    return registro;
}

async function listarRegistrosUsuario(usuario) {
    const usuarioNormalizado = normalizarTextoChave(usuario);
    return executarStore('readonly', store => {
        const indice = store.index('usuario');
        return aguardarRequest(indice.getAll(usuarioNormalizado));
    });
}

function obterTimestamp(valor) {
    if (typeof valor === 'number' && Number.isFinite(valor)) return valor;
    const timestamp = Date.parse(String(valor || ''));
    return Number.isNaN(timestamp) ? 0 : timestamp;
}

function registroPertenceSelecao(registro, selecionadaEm) {
    const inicioSelecao = obterTimestamp(selecionadaEm);
    if (!(inicioSelecao > 0)) return true;
    return Number(registro?.iniciadoEm || 0) >= inicioSelecao;
}

function normalizarClientes(clientes) {
    const unicos = new Map();
    (Array.isArray(clientes) ? clientes : []).forEach(cliente => {
        const id = String(cliente?.id || cliente?.firebaseUrl || '').trim();
        if (!id || /[.#$\[\]\/]/.test(id) || unicos.has(id)) return;
        unicos.set(id, { id });
    });
    return [...unicos.values()];
}

function comPrazo(promise, prazoMs) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Tempo limite ao consultar contador.')), prazoMs);
        promise.then(
            valor => {
                clearTimeout(timer);
                resolve(valor);
            },
            erro => {
                clearTimeout(timer);
                reject(erro);
            }
        );
    });
}

export async function lerContadoresOficiaisCliente(clienteId, prazoMs = 10000) {
    const id = String(clienteId || '').trim();
    if (!id || /[.#$\[\]\/]/.test(id)) throw new Error('Cliente inválido para consultar contadores.');

    const snapshot = await comPrazo(
        get(ref(db, `clientes/${id}/equipDetalhes`)),
        Math.max(1000, Number(prazoMs) || 10000)
    );

    return extrairContadoresEquipDetalhes(snapshot.val());
}

function criarResumo(registro, total) {
    const carregados = Object.keys(registro?.clientes || {}).length;
    const falhas = Array.isArray(registro?.falhas) ? registro.falhas.length : 0;
    return {
        status: registro?.status || 'erro',
        total,
        carregados,
        falhas,
        completo: falhas === 0 && carregados === total
    };
}

export async function carregarContadoresRota({
    usuario,
    numeroRota,
    selecionadaEm = null,
    clientes = [],
    forcar = false,
    onProgresso = null
}) {
    const usuarioNormalizado = normalizarTextoChave(usuario);
    const rotaNormalizada = normalizarNumeroRota(numeroRota);
    if (!usuarioNormalizado || !rotaNormalizada) throw new Error('Usuário e rota são obrigatórios.');

    const chave = criarChaveRota(usuarioNormalizado, rotaNormalizada);
    if (carregamentosEmAndamento.has(chave)) return carregamentosEmAndamento.get(chave);

    const operacao = (async () => {
        const listaClientes = normalizarClientes(clientes);
        const existente = await lerRegistroRota(usuarioNormalizado, rotaNormalizada).catch(() => null);
        const existenteValido = existente && registroPertenceSelecao(existente, selecionadaEm);

        if (!forcar && existenteValido && existente.status === 'pronto') {
            return criarResumo(existente, listaClientes.length);
        }

        const iniciadoEm = Date.now();
        const registro = {
            chave,
            usuario: usuarioNormalizado,
            numeroRota: rotaNormalizada,
            selecionadaEm: selecionadaEm || null,
            iniciadoEm,
            atualizadoEm: iniciadoEm,
            status: 'carregando',
            clientes: !forcar && existenteValido ? { ...(existente.clientes || {}) } : {},
            falhas: []
        };
        await salvarRegistroRota(registro);

        if (listaClientes.length === 0) {
            registro.status = 'pronto';
            registro.atualizadoEm = Date.now();
            await salvarRegistroRota(registro);
            return criarResumo(registro, 0);
        }

        const prazoFinal = Date.now() + PRAZO_TOTAL_MS;
        let proximoIndice = 0;
        let concluidos = 0;

        const trabalhador = async () => {
            while (proximoIndice < listaClientes.length) {
                const indice = proximoIndice++;
                const cliente = listaClientes[indice];
                const restante = prazoFinal - Date.now();

                if (restante <= 0) {
                    registro.falhas.push(cliente.id);
                    concluidos += 1;
                    if (typeof onProgresso === 'function') onProgresso(concluidos, listaClientes.length);
                    continue;
                }

                try {
                    const contadores = await lerContadoresOficiaisCliente(cliente.id, Math.min(10000, restante));
                    registro.clientes[cliente.id] = {
                        contadores,
                        capturadoEm: Date.now()
                    };
                } catch {
                    registro.falhas.push(cliente.id);
                } finally {
                    concluidos += 1;
                    if (typeof onProgresso === 'function') onProgresso(concluidos, listaClientes.length);
                }
            }
        };

        const quantidadeTrabalhadores = Math.min(LIMITE_CONCORRENCIA, listaClientes.length);
        await Promise.all(Array.from({ length: quantidadeTrabalhadores }, () => trabalhador()));

        registro.atualizadoEm = Date.now();
        registro.status = registro.falhas.length === 0 ? 'pronto' : (Object.keys(registro.clientes).length > 0 ? 'parcial' : 'erro');
        await salvarRegistroRota(registro);
        return criarResumo(registro, listaClientes.length);
    })().finally(() => carregamentosEmAndamento.delete(chave));

    carregamentosEmAndamento.set(chave, operacao);
    return operacao;
}

export async function lerContadoresUsuario({ usuario, rotas = [] }) {
    const registros = await listarRegistrosUsuario(usuario).catch(() => []);
    const rotasAtuais = new Map(
        (Array.isArray(rotas) ? rotas : []).map(rota => [
            normalizarNumeroRota(rota?.numeroRota ?? rota?.numero),
            rota?.selecionadaEm ?? rota?.selecionada_em ?? null
        ])
    );
    const validarRotas = rotasAtuais.size > 0;
    const porCliente = {};
    const rotasValidas = new Set();

    registros
        .filter(registro => registro?.status === 'pronto' || registro?.status === 'parcial')
        .filter(registro => !validarRotas || (
            rotasAtuais.has(normalizarNumeroRota(registro.numeroRota))
            && registroPertenceSelecao(registro, rotasAtuais.get(normalizarNumeroRota(registro.numeroRota)))
        ))
        .sort((a, b) => Number(a.atualizadoEm || 0) - Number(b.atualizadoEm || 0))
        .forEach(registro => {
            rotasValidas.add(normalizarNumeroRota(registro.numeroRota));
            Object.entries(registro.clientes || {}).forEach(([clienteId, dados]) => {
                porCliente[clienteId] = {
                    contadores: { ...(dados?.contadores || {}) },
                    capturadoEm: Number(dados?.capturadoEm || registro.atualizadoEm || 0),
                    numeroRota: normalizarNumeroRota(registro.numeroRota)
                };
            });
        });

    return { porCliente, rotasValidas: [...rotasValidas] };
}

export async function atualizarContadoresClienteCache({ usuario, numeroRota, clienteId, contadores }) {
    const registro = await lerRegistroRota(usuario, numeroRota).catch(() => null);
    if (!registro) return false;

    registro.clientes = { ...(registro.clientes || {}) };
    registro.clientes[String(clienteId)] = {
        contadores: { ...(contadores || {}) },
        capturadoEm: Date.now()
    };
    registro.atualizadoEm = Date.now();
    registro.falhas = (registro.falhas || []).filter(id => String(id) !== String(clienteId));
    registro.status = registro.falhas.length === 0 ? 'pronto' : 'parcial';
    await salvarRegistroRota(registro);
    return true;
}
