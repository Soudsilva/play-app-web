const {setGlobalOptions} = require("firebase-functions");
const {onSchedule} = require("firebase-functions/v2/scheduler");
const {onValueWritten} = require("firebase-functions/v2/database");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");

setGlobalOptions({maxInstances: 10});

admin.initializeApp({
  databaseURL: "https://play-na-web-default-rtdb.firebaseio.com",
});

const RESUMO_BALANCO_ID = "resumo_balanco";
const CINCO_DIAS_MS = 5 * 24 * 60 * 60 * 1000;
const VALOR_MINIMO_ADIANTAR_ROTA = 1500;
const DIAS_MAXIMOS_ADIANTAR_ROTA = 70;
const DIAS_MINIMOS_REPOR_SEM_VISITA = 100;
const CONFIGURACOES_AUTOMATICAS_ROOT = "configuracoes_automaticas";
const DEPOSITOS_RESUMO_ROOT = "firebase_functions_depositos";
const LIMITE_BLOQUEIO_DEPOSITO = 5000;
const MEDIA_VENDAS_ROOT = "Media_de_Vendas";
const MEDIA_VENDAS_METADATA_PATH = "metadata/media_de_vendas_versao";
const COBRANCAS_DATAVERSE_ROOT = "cobrancas_dataverse";
const FOTO_PADRAO_MANUTENCAO = "assets/img/logo.png";

function nomeParaEmailAuth(nome) {
  const base = String(nome || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, ".")
    .replace(/\.+/g, ".")
    .replace(/^\.|\.$/g, "");
  return base ? `${base}@play.internal` : "";
}

function normalizarSenhaAuth(senha) {
  let valor = String(senha || "");
  while (valor.length < 6) valor += "@";
  return valor;
}

async function obterUsuarioAuthPorEmail(email) {
  try {
    return await admin.auth().getUserByEmail(email);
  } catch (error) {
    if (error?.code === "auth/user-not-found") return null;
    throw error;
  }
}

async function excluirUsuarioAuthPorEmail(email) {
  const usuario = await obterUsuarioAuthPorEmail(email);
  if (!usuario) return false;
  await admin.auth().deleteUser(usuario.uid);
  return true;
}

async function sincronizarUsuarioAuthColaborador(before, after) {
  const nomeAnterior = String(before?.nome || "").trim();
  const nomeAtual = String(after?.nome || "").trim();
  const senhaAtual = String(after?.senha || "").trim();
  const emailAnterior = nomeParaEmailAuth(nomeAnterior);
  const emailAtual = nomeParaEmailAuth(nomeAtual);

  if (!after) {
    if (emailAnterior) {
      const removido = await excluirUsuarioAuthPorEmail(emailAnterior);
      logger.info("Login de colaborador removido do Auth.", {
        email: emailAnterior,
        removido,
      });
    }
    return;
  }

  if (!nomeAtual || !senhaAtual || !emailAtual) {
    logger.warn("Colaborador sem nome/senha para sincronizar Auth.", {
      nome: nomeAtual,
      email: emailAtual,
    });
    return;
  }

  const dadosAuth = {
    email: emailAtual,
    displayName: nomeAtual,
    password: normalizarSenhaAuth(senhaAtual),
    disabled: false,
  };

  const usuarioAtual = await obterUsuarioAuthPorEmail(emailAtual);
  if (usuarioAtual) {
    await admin.auth().updateUser(usuarioAtual.uid, dadosAuth);
    logger.info("Login de colaborador atualizado no Auth.", {
      email: emailAtual,
      uid: usuarioAtual.uid,
    });
    if (emailAnterior && emailAnterior !== emailAtual) {
      await excluirUsuarioAuthPorEmail(emailAnterior);
    }
    return;
  }

  if (emailAnterior && emailAnterior !== emailAtual) {
    const usuarioAnterior = await obterUsuarioAuthPorEmail(emailAnterior);
    if (usuarioAnterior) {
      await admin.auth().updateUser(usuarioAnterior.uid, dadosAuth);
      logger.info("Login de colaborador renomeado no Auth.", {
        emailAnterior,
        emailAtual,
        uid: usuarioAnterior.uid,
      });
      return;
    }
  }

  const criado = await admin.auth().createUser(dadosAuth);
  logger.info("Login de colaborador criado no Auth.", {
    email: emailAtual,
    uid: criado.uid,
  });
}

function dataBrasiliaISOData(date = new Date()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function dataBrasiliaISODataPorTimestamp(timestamp) {
  return typeof timestamp === "number" && Number.isFinite(timestamp) ?
    dataBrasiliaISOData(new Date(timestamp)) :
    null;
}

function deveMarcarPendente(pendenteDesdeData, hojeBrasilia) {
  return Boolean(pendenteDesdeData) && String(pendenteDesdeData) < hojeBrasilia;
}

function timestampValido(valor) {
  const timestamp = Date.parse(valor || "");
  return Number.isNaN(timestamp) ? null : timestamp;
}

function isoOuNull(timestamp) {
  return typeof timestamp === "number" && Number.isFinite(timestamp) ?
    new Date(timestamp).toISOString() :
    null;
}

function normalizarChaveUsuario(nomeUsuario) {
  return String(nomeUsuario || "").trim().replace(/[.#$/[\]]/g, "_");
}

function numero(valor) {
  const n = Number(valor || 0);
  return Number.isFinite(n) ? n : 0;
}

function numeroConfig(valor, padrao) {
  const n = Number(valor);
  return Number.isFinite(n) && n > 0 ? n : padrao;
}

async function obterConfiguracoesAutomaticas(db) {
  try {
    const snap = await db.ref(CONFIGURACOES_AUTOMATICAS_ROOT).get();
    const config = snap.val() || {};
    return {
      prioridadeRota: {
        valorMinimo: numeroConfig(
          config?.prioridade_rota?.valor_minimo,
          VALOR_MINIMO_ADIANTAR_ROTA,
        ),
        diasMaximos: numeroConfig(
          config?.prioridade_rota?.dias_maximos,
          DIAS_MAXIMOS_ADIANTAR_ROTA,
        ),
      },
      manutencaoSemVisita: {
        diasMinimos: numeroConfig(
          config?.manutencao_sem_visita?.dias_minimos,
          DIAS_MINIMOS_REPOR_SEM_VISITA,
        ),
      },
    };
  } catch (error) {
    logger.warn("Falha ao carregar configuracoes automaticas. Usando padroes.", error);
    return {
      prioridadeRota: {
        valorMinimo: VALOR_MINIMO_ADIANTAR_ROTA,
        diasMaximos: DIAS_MAXIMOS_ADIANTAR_ROTA,
      },
      manutencaoSemVisita: {
        diasMinimos: DIAS_MINIMOS_REPOR_SEM_VISITA,
      },
    };
  }
}

function normalizarNumeroCliente(valor) {
  return String(valor ?? "").trim();
}

function normalizarRota(valor) {
  return String(valor ?? "").trim().replace(/^R\.?\s*/i, "");
}

function arredondar2(valor) {
  return Math.round((Number(valor || 0) + Number.EPSILON) * 100) / 100;
}

function inicioDiaBrasiliaMs(valor) {
  const data = valor ? new Date(valor) : new Date();
  if (Number.isNaN(data.getTime())) return null;
  const partes = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(data);
  const get = (type) => partes.find((p) => p.type === type)?.value;
  return Date.parse(`${get("year")}-${get("month")}-${get("day")}T00:00:00-03:00`);
}

function diasEntreDatasBrasilia(dataMaisRecente, dataMaisAntiga) {
  const rec = inicioDiaBrasiliaMs(dataMaisRecente);
  const ant = inicioDiaBrasiliaMs(dataMaisAntiga);
  if (rec == null || ant == null) return 0;
  return Math.max(0, Math.round((rec - ant) / 86400000));
}

function diasDesdeBrasilia(valor, agora = new Date()) {
  const data = inicioDiaBrasiliaMs(valor);
  const hoje = inicioDiaBrasiliaMs(agora);
  if (data == null || hoje == null) return null;
  return Math.max(0, Math.round((hoje - data) / 86400000));
}

function normalizarTextoBasico(valor) {
  return String(valor || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
}

function manutencaoMesmoCliente(manutencao, clienteId, clienteNumero) {
  const id = String(clienteId || "").trim();
  const numeroCliente = normalizarNumeroCliente(clienteNumero);
  const idManutencao = String(
    manutencao?.clienteId ||
    manutencao?.clienteFirebaseUrl ||
    "",
  ).trim();
  const numeroManutencao = normalizarNumeroCliente(manutencao?.clienteNumero);

  return Boolean(
    (id && idManutencao === id) ||
    (numeroCliente && numeroManutencao === numeroCliente),
  );
}

function manutencaoReporSemVisitaJaCriada(
  manutencao,
  clienteId,
  clienteNumero,
  ultimaCobrancaEm,
) {
  if (!manutencaoMesmoCliente(manutencao, clienteId, clienteNumero)) return false;
  if (normalizarTextoBasico(manutencao?.tipoAcao) !== "repor") return false;

  const status = normalizarTextoBasico(manutencao?.status || "pendente");
  if (status !== "concluida" && status !== "concluido") return true;

  const observacoes = normalizarTextoBasico(manutencao?.observacoes);
  const pareceAutomatica =
    observacoes.includes("sem visita") &&
    observacoes.includes("abastecer maquinas");
  if (!pareceAutomatica) return false;

  const dataRegistroMs = timestampValido(manutencao?.dataRegistro);
  const ultimaCobrancaMs = timestampValido(ultimaCobrancaEm);
  return dataRegistroMs != null &&
    ultimaCobrancaMs != null &&
    dataRegistroMs >= ultimaCobrancaMs;
}

function obterTextoEquipamentosCliente(cliente) {
  const equipTexto = String(cliente?.equip || "").trim();
  if (equipTexto) {
    return equipTexto
      .replace(/\s*\[Pix:\s*[^\]]+\]/gi, "")
      .replace(/\s+/g, " ")
      .replace(/\s+,/g, ",")
      .trim();
  }

  if (!Array.isArray(cliente?.equipDetalhes)) return "";

  return cliente.equipDetalhes
    .map((item) => {
      const nome = String(item?.nome || "").trim();
      if (!nome) return "";
      const qtd = String(item?.qtd || item?.quantidade || "").trim();
      return qtd ? `${nome} (${qtd})` : nome;
    })
    .filter(Boolean)
    .join(", ");
}

function obterFotoFichaCliente(cliente) {
  const foto =
    String(cliente?.fotoFichaInstalacao || "").trim() ||
    String(cliente?.fotosInstalacao?.ficha || "").trim() ||
    FOTO_PADRAO_MANUTENCAO;
  const thumb =
    String(cliente?.fotoFichaInstalacaoThumb || "").trim() ||
    String(cliente?.fotosInstalacao?.fichaThumb || "").trim() ||
    foto;

  return {foto, thumb};
}

function inicioDiaHistoricoDataverseMs(valor) {
  const texto = String(valor || "").trim();
  const isoData = texto.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoData) return Date.parse(`${isoData[1]}-${isoData[2]}-${isoData[3]}T00:00:00-03:00`);

  const dataBr = texto.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (dataBr) {
    return Date.parse(`${dataBr[3]}-${dataBr[2].padStart(2, "0")}-${dataBr[1].padStart(2, "0")}T00:00:00-03:00`);
  }

  return inicioDiaBrasiliaMs(valor);
}

function diasEntreHistoricoDataverse(dataMaisRecente, dataMaisAntiga) {
  const rec = inicioDiaHistoricoDataverseMs(dataMaisRecente);
  const ant = inicioDiaHistoricoDataverseMs(dataMaisAntiga);
  if (rec == null || ant == null) return 0;
  return Math.max(0, Math.round((rec - ant) / 86400000));
}

function diasDesdeHistoricoDataverse(valor, agora = new Date()) {
  const data = inicioDiaHistoricoDataverseMs(valor);
  const hoje = inicioDiaBrasiliaMs(agora);
  if (data == null || hoje == null) return null;
  return Math.max(0, Math.round((hoje - data) / 86400000));
}

function dataHistoricoDataverseValida(valor) {
  const texto = String(valor || "").trim();
  return texto && inicioDiaHistoricoDataverseMs(texto) != null ? texto : "";
}

function numeroHistoricoDataverse(valor) {
  if (typeof valor === "number") return Number.isFinite(valor) ? valor : 0;
  const texto = String(valor || "").trim();
  if (!texto) return 0;
  const normalizado = texto.includes(",") || /^\d{1,3}(\.\d{3})+$/.test(texto) ?
    texto.replace(/\./g, "").replace(",", ".") :
    texto;
  const n = Number(normalizado);
  return Number.isFinite(n) ? n : 0;
}

function obterTotalCobrancaAtendimento(atendimento) {
  const total = Number(
    atendimento?.financeiro?.totalGeral ??
    atendimento?.financeiro?.total ??
    atendimento?.totalGeral ??
    atendimento?.total ??
    0,
  );
  return Number.isFinite(total) ? total : 0;
}

function obterDataAtendimento(atendimento) {
  const data = atendimento?.data || atendimento?.dataHora || null;
  const timestamp = Date.parse(data || "");
  return Number.isNaN(timestamp) ? null : {data, timestamp};
}

function obterReferenciasClienteMedia(item) {
  const cliente = item?.cliente || {};
  return {
    id: String(cliente.id || cliente.firebaseUrl || "").trim(),
    numero: normalizarNumeroCliente(cliente.numero),
  };
}

function obterAlvosMediaPorAtendimento(before, after) {
  const alvos = new Map();
  [before, after].forEach((item) => {
    if (!item || typeof item !== "object") return;
    const refCliente = obterReferenciasClienteMedia(item);
    if (!refCliente.id && !refCliente.numero) return;
    const chave = refCliente.id || refCliente.numero;
    alvos.set(chave, refCliente);
  });
  return [...alvos.values()];
}

function obterAlvosMediaPorCliente(clienteId, before, after) {
  const alvos = new Map();
  [before, after].forEach((cliente) => {
    if (!cliente || typeof cliente !== "object") return;
    const numero = normalizarNumeroCliente(cliente.numero);
    if (!clienteId && !numero) return;
    alvos.set(`${clienteId || ""}/${numero}`, {id: clienteId, numero});
  });
  return [...alvos.values()];
}

function clienteCorrespondeAoAlvo(atendimento, alvo, clienteAtual) {
  const refCliente = obterReferenciasClienteMedia(atendimento);
  const idAtual = String(clienteAtual?.id || "").trim();
  const numeroAtual = normalizarNumeroCliente(clienteAtual?.numero);
  const ids = [alvo.id, idAtual].filter(Boolean);
  const numeros = [alvo.numero, numeroAtual].filter(Boolean);
  return Boolean(
    (refCliente.id && ids.includes(refCliente.id)) ||
    (refCliente.numero && numeros.includes(refCliente.numero)),
  );
}

function montarRegistroMediaDeVendas(
  clienteId,
  cliente,
  cobrancas,
  hoje,
  atualizadoEm,
  registroAtual = {},
) {
  const usadas = cobrancas.slice(0, 3);
  let ultima = usadas[0] || null;
  let penultima = usadas[1] || null;
  let antepenultima = usadas[2] || null;
  let mediaVendaPorDia = 0;
  let estimativaAtualDia = 0;

  if (usadas.length >= 2) {
    const totalBase = usadas
      .slice(0, -1)
      .reduce((soma, item) => soma + item.total, 0);
    const maisAntiga = usadas[usadas.length - 1];
    const intervaloDias = diasEntreDatasBrasilia(ultima.data, maisAntiga.data);
    const divisor = intervaloDias > 0 ? intervaloDias : 1;
    mediaVendaPorDia = arredondar2(totalBase / divisor);
    const diasDesdeUltima = diasEntreDatasBrasilia(`${hoje}T00:00:00-03:00`, ultima.data);
    estimativaAtualDia = arredondar2(mediaVendaPorDia * diasDesdeUltima);
  } else {
    mediaVendaPorDia = 0;
    estimativaAtualDia = 0;
  }

  return {
    cliente: {
      id: clienteId || "",
      numero: cliente?.numero ?? "",
      nome: cliente?.nome || "",
      rota: cliente?.rota || "",
    },
    rota: cliente?.rota || "",
    ultimaCobrancaEm: ultima?.data || "",
    penultimaCobrancaEm: penultima?.data || "",
    antepenultimaCobrancaEm: antepenultima?.data || "",
    ultimaCobrancaValor: ultima ? arredondar2(ultima.total) : 0,
    penultimaCobrancaValor: penultima ? arredondar2(penultima.total) : 0,
    antepenultimaCobrancaValor: antepenultima ? arredondar2(antepenultima.total) : 0,
    mediaVendaPorDia,
    hoje,
    estimativaAtualDia,
    atualizadoEm,
  };
}

async function carregarClienteParaMedia(db, alvo) {
  if (alvo.id) {
    const snap = await db.ref(`clientes/${alvo.id}`).get();
    if (snap.exists()) return {id: alvo.id, dados: snap.val()};
  }

  const numeroAlvo = normalizarNumeroCliente(alvo.numero);
  if (!numeroAlvo) return null;

  const clientesSnap = await db.ref("clientes").get();
  const clientes = clientesSnap.val() || {};
  const encontrado = Object.entries(clientes).find(([, cliente]) =>
    normalizarNumeroCliente(cliente?.numero) === numeroAlvo,
  );
  return encontrado ? {id: encontrado[0], dados: encontrado[1]} : null;
}

async function recalcularMediaDeVendasCliente(alvo) {
  const db = admin.database();
  const numeroAlvo = normalizarNumeroCliente(alvo.numero);
  const clienteEncontrado = await carregarClienteParaMedia(db, alvo);

  if (!clienteEncontrado && numeroAlvo) {
    await db.ref(`${MEDIA_VENDAS_ROOT}/${numeroAlvo}`).remove();
    await db.ref(MEDIA_VENDAS_METADATA_PATH).set(Date.now());
    logger.info("Media de vendas removida: cliente nao encontrado.", {alvo});
    return;
  }
  if (!clienteEncontrado) return;

  const clienteId = clienteEncontrado.id;
  const cliente = clienteEncontrado.dados || {};
  const numeroKey = normalizarNumeroCliente(cliente.numero);
  if (!numeroKey) return;

  const atendimentosSnap = await db.ref("atendimentos").get();
  const registroAtualSnap = await db.ref(`${MEDIA_VENDAS_ROOT}/${numeroKey}`).get();
  const registroAtual = registroAtualSnap.val() || {};
  const cobrancas = Object.entries(atendimentosSnap.val() || {})
    .map(([id, atendimento]) => ({id, atendimento}))
    .filter(({atendimento}) => clienteCorrespondeAoAlvo(
      atendimento,
      alvo,
      {id: clienteId, numero: cliente.numero},
    ))
    .map(({id, atendimento}) => {
      const dataInfo = obterDataAtendimento(atendimento);
      const total = obterTotalCobrancaAtendimento(atendimento);
      if (!dataInfo || total <= 0) return null;
      return {atendimentoId: id, data: dataInfo.data, timestamp: dataInfo.timestamp, total};
    })
    .filter(Boolean)
    .sort((a, b) => b.timestamp - a.timestamp);

  const agora = new Date();
  const registro = montarRegistroMediaDeVendas(
    clienteId,
    cliente,
    cobrancas,
    dataBrasiliaISOData(agora),
    agora.toISOString(),
    registroAtual,
  );

  const updates = {
    [`${MEDIA_VENDAS_ROOT}/${numeroKey}`]: registro,
    [MEDIA_VENDAS_METADATA_PATH]: Date.now(),
  };

  if (numeroAlvo && numeroAlvo !== numeroKey) {
    updates[`${MEDIA_VENDAS_ROOT}/${numeroAlvo}`] = null;
  }

  await db.ref().update(updates);
  logger.info("Media de vendas recalculada.", {
    numero: numeroKey,
    clienteId,
    cobrancas: cobrancas.length,
  });
}

async function recalcularTodasMediasDeVendas() {
  const db = admin.database();
  const [clientesSnap, atendimentosSnap, mediaSnap] = await Promise.all([
    db.ref("clientes").get(),
    db.ref("atendimentos").get(),
    db.ref(MEDIA_VENDAS_ROOT).get(),
  ]);
  const clientes = clientesSnap.val() || {};
  const mediasAtuais = mediaSnap.val() || {};
  const atendimentos = Object.entries(atendimentosSnap.val() || {})
    .map(([id, atendimento]) => ({id, atendimento}));
  const agora = new Date();
  const hoje = dataBrasiliaISOData(agora);
  const atualizadoEm = agora.toISOString();
  const updates = {};
  const chavesClientes = new Set();

  Object.entries(clientes).forEach(([clienteId, cliente]) => {
    const numeroKey = normalizarNumeroCliente(cliente?.numero);
    if (!numeroKey) return;
    chavesClientes.add(numeroKey);

    const cobrancas = atendimentos
      .filter(({atendimento}) => clienteCorrespondeAoAlvo(
        atendimento,
        {id: clienteId, numero: numeroKey},
        {id: clienteId, numero: numeroKey},
      ))
      .map(({id, atendimento}) => {
        const dataInfo = obterDataAtendimento(atendimento);
        const total = obterTotalCobrancaAtendimento(atendimento);
        if (!dataInfo || total <= 0) return null;
        return {atendimentoId: id, data: dataInfo.data, timestamp: dataInfo.timestamp, total};
      })
      .filter(Boolean)
      .sort((a, b) => b.timestamp - a.timestamp);

    updates[`${MEDIA_VENDAS_ROOT}/${numeroKey}`] =
      montarRegistroMediaDeVendas(
        clienteId,
        cliente,
        cobrancas,
        hoje,
        atualizadoEm,
        mediasAtuais[numeroKey] || {},
      );
  });

  Object.keys(mediasAtuais).forEach((numeroKey) => {
    if (!chavesClientes.has(normalizarNumeroCliente(numeroKey))) {
      updates[`${MEDIA_VENDAS_ROOT}/${numeroKey}`] = null;
    }
  });

  updates[MEDIA_VENDAS_METADATA_PATH] = Date.now();
  await db.ref().update(updates);
  logger.info("Todas as medias de vendas recalculadas.", {
    totalClientes: chavesClientes.size,
    data: hoje,
  });
}

function bsbDate(valor) {
  const data = new Date(valor || 0);
  if (Number.isNaN(data.getTime())) return null;
  return new Date(data.toLocaleString("en-US", {timeZone: "America/Sao_Paulo"}));
}

function obterAnoMes(valor) {
  const data = bsbDate(valor);
  if (!data || Number.isNaN(data.getTime())) return "";
  return `${data.getFullYear()}-${String(data.getMonth() + 1).padStart(2, "0")}`;
}

function obterSaldoDepositarAtendimento(atendimento) {
  const fin = atendimento && atendimento.financeiro;
  if (!fin || typeof fin !== "object") return 0;
  return numero(fin.dinheiro) - numero(fin.comissaoValor);
}

function valorAbatimentoDeposito(deposito) {
  const valorConferido = numero(deposito && deposito.valorConferido);
  return valorConferido > 0 ? valorConferido : numero(deposito && deposito.valor);
}

function obterAlvosAtendimento(before, after) {
  const alvos = new Map();
  [before, after].forEach((item) => {
    if (!item || typeof item !== "object") return;
    const usuario = normalizarChaveUsuario(item.atendente);
    const anoMes = obterAnoMes(item.data || item.dataHora);
    if (usuario && anoMes) alvos.set(`${usuario}/${anoMes}`, {usuario, anoMes});
  });
  return [...alvos.values()];
}

function obterAlvosDeposito(usuarioParam, before, after) {
  const usuario = normalizarChaveUsuario(usuarioParam);
  if (!usuario) return [];

  const alvos = new Map();
  [before, after].forEach((item) => {
    if (!item || typeof item !== "object") return;
    const anoMes = obterAnoMes(item.data || item.timestamp || item.criadoEm);
    if (anoMes) alvos.set(`${usuario}/${anoMes}`, {usuario, anoMes});
  });
  return [...alvos.values()];
}

exports.sincronizarAuthColaborador = onValueWritten(
  {
    ref: "/colaboradores/{colaboradorId}",
    region: "us-central1",
  },
  async (event) => {
    const before = event.data.before.exists() ? event.data.before.val() : null;
    const after = event.data.after.exists() ? event.data.after.val() : null;
    await sincronizarUsuarioAuthColaborador(before, after);
  },
);

async function recalcularResumoDepositosMes(usuario, anoMes) {
  const db = admin.database();
  const agoraIso = new Date().toISOString();
  const usuarioKey = normalizarChaveUsuario(usuario);
  if (!usuarioKey || !anoMes) return;

  const [atendimentosSnap, depositosSnap] = await Promise.all([
    db.ref("atendimentos").get(),
    db.ref(`depositos/${usuarioKey}`).get(),
  ]);

  let saldoAtendimentos = 0;
  Object.values(atendimentosSnap.val() || {}).forEach((atendimento) => {
    if (!atendimento || typeof atendimento !== "object") return;
    if (normalizarChaveUsuario(atendimento.atendente) !== usuarioKey) return;
    if (obterAnoMes(atendimento.data || atendimento.dataHora) !== anoMes) return;
    saldoAtendimentos += obterSaldoDepositarAtendimento(atendimento);
  });

  let totalDepositos = 0;
  Object.values(depositosSnap.val() || {}).forEach((deposito) => {
    if (!deposito || typeof deposito !== "object") return;
    const depositoAnoMes = obterAnoMes(
      deposito.data || deposito.timestamp || deposito.criadoEm,
    );
    if (depositoAnoMes !== anoMes) return;
    totalDepositos += valorAbatimentoDeposito(deposito);
  });

  const saldoMes = saldoAtendimentos - totalDepositos;
  await db.ref(`${DEPOSITOS_RESUMO_ROOT}/${usuarioKey}/meses/${anoMes}`).set({
    saldoAtendimentos,
    totalDepositos,
    saldoMes,
    faltaDepositar: Math.max(0, saldoMes),
    atualizadoEm: agoraIso,
  });

  await recalcularResumoDepositosTotal(usuarioKey);
}

async function recalcularResumoDepositosTotal(usuario) {
  const db = admin.database();
  const usuarioKey = normalizarChaveUsuario(usuario);
  if (!usuarioKey) return;

  const mesesSnap = await db.ref(`${DEPOSITOS_RESUMO_ROOT}/${usuarioKey}/meses`).get();
  let saldoTotal = 0;
  Object.values(mesesSnap.val() || {}).forEach((mes) => {
    if (!mes || typeof mes !== "object") return;
    const saldoMes = Number.isFinite(Number(mes.saldoMes)) ?
      Number(mes.saldoMes) :
      numero(mes.saldoAtendimentos) - numero(mes.totalDepositos);
    saldoTotal += saldoMes;
  });

  const faltaDepositarTotal = Math.max(0, saldoTotal);
  await db.ref(`${DEPOSITOS_RESUMO_ROOT}/${usuarioKey}/resumo`).set({
    faltaDepositarTotal,
    statusDeposito: faltaDepositarTotal >= LIMITE_BLOQUEIO_DEPOSITO ?
      "bloqueado" :
      "ok",
    limiteBloqueio: LIMITE_BLOQUEIO_DEPOSITO,
    atualizadoEm: new Date().toISOString(),
  });
}

async function recalcularAlvosDepositos(alvos, origem) {
  if (!alvos.length) return;
  await Promise.all(alvos.map((alvo) =>
    recalcularResumoDepositosMes(alvo.usuario, alvo.anoMes),
  ));
  logger.info("Resumo de depositos atualizado.", {origem, alvos});
}

exports.atualizarResumoDepositosPorAtendimento = onValueWritten(
  "/atendimentos/{atendimentoId}",
  async (event) => {
    const before = event.data.before.exists() ? event.data.before.val() : null;
    const after = event.data.after.exists() ? event.data.after.val() : null;
    const alvos = obterAlvosAtendimento(before, after);
    await recalcularAlvosDepositos(alvos, "atendimento");
  },
);

exports.atualizarResumoDepositosPorDeposito = onValueWritten(
  "/depositos/{usuario}/{depositoId}",
  async (event) => {
    const before = event.data.before.exists() ? event.data.before.val() : null;
    const after = event.data.after.exists() ? event.data.after.val() : null;
    const alvos = obterAlvosDeposito(event.params.usuario, before, after);
    await recalcularAlvosDepositos(alvos, "deposito");
  },
);

exports.atualizarMediaDeVendasPorAtendimento = onValueWritten(
  "/atendimentos/{atendimentoId}",
  async (event) => {
    const before = event.data.before.exists() ? event.data.before.val() : null;
    const after = event.data.after.exists() ? event.data.after.val() : null;
    const alvos = obterAlvosMediaPorAtendimento(before, after);
    await Promise.all(alvos.map((alvo) => recalcularMediaDeVendasCliente(alvo)));
  },
);

exports.atualizarMediaDeVendasPorCliente = onValueWritten(
  "/clientes/{clienteId}",
  async (event) => {
    const before = event.data.before.exists() ? event.data.before.val() : null;
    const after = event.data.after.exists() ? event.data.after.val() : null;
    const alvos = obterAlvosMediaPorCliente(event.params.clienteId, before, after);
    await Promise.all(alvos.map((alvo) => recalcularMediaDeVendasCliente(alvo)));
  },
);

exports.atualizarMediaDeVendasDiaria = onSchedule(
  {
    schedule: "10 0 * * *",
    timeZone: "America/Sao_Paulo",
  },
  async () => {
    await recalcularTodasMediasDeVendas();
  },
);

exports.completarMediaDeVendasComDataverse = onSchedule(
  {
    schedule: "30 0 * * *",
    timeZone: "America/Sao_Paulo",
  },
  async () => {
    const db = admin.database();
    const agora = new Date();
    const hoje = dataBrasiliaISOData(agora);
    const atualizadoEm = agora.toISOString();
    const [cobrancasSnap, mediaSnap, clientesSnap] = await Promise.all([
      db.ref(COBRANCAS_DATAVERSE_ROOT).get(),
      db.ref(MEDIA_VENDAS_ROOT).get(),
      db.ref("clientes").get(),
    ]);

    if (!cobrancasSnap.exists()) {
      logger.info("Sem cobrancas_dataverse para completar Media_de_Vendas.");
      return;
    }

    const medias = mediaSnap.val() || {};
    const clientes = clientesSnap.val() || {};
    const clientesPorNumero = new Map();
    Object.entries(clientes).forEach(([clienteId, cliente]) => {
      const numeroKey = normalizarNumeroCliente(cliente?.numero);
      if (!numeroKey) return;
      clientesPorNumero.set(numeroKey, {clienteId, cliente});
    });

    const updates = {};
    let analisados = 0;
    let atualizados = 0;
    let ignorados = 0;

    Object.entries(cobrancasSnap.val() || {}).forEach(([chave, historico]) => {
      if (!historico || typeof historico !== "object") {
        ignorados += 1;
        return;
      }

      const numeroKey = normalizarNumeroCliente(historico.numero || chave);
      const saldo = numeroHistoricoDataverse(historico.saldo);
      const ultimaDataverse = dataHistoricoDataverseValida(historico.ultimaVisita);
      const penultimaDataverse = dataHistoricoDataverseValida(historico.penultimaVisita);

      if (!numeroKey || saldo <= 0 || !ultimaDataverse || !penultimaDataverse) {
        ignorados += 1;
        return;
      }

      const intervaloDataverse = diasEntreHistoricoDataverse(ultimaDataverse, penultimaDataverse);
      if (intervaloDataverse <= 0) {
        ignorados += 1;
        return;
      }

      analisados += 1;
      const atual = medias[numeroKey] || {};
      const clienteInfo = clientesPorNumero.get(numeroKey);
      const clienteAtual = {
        ...(atual.cliente || {}),
        id: atual?.cliente?.id || clienteInfo?.clienteId || "",
        numero: atual?.cliente?.numero ?? clienteInfo?.cliente?.numero ?? numeroKey,
        nome: atual?.cliente?.nome || clienteInfo?.cliente?.nome || historico.cliente || "",
        rota: atual?.cliente?.rota || clienteInfo?.cliente?.rota || atual.rota || "",
      };

      const proximo = {
        ...atual,
        cliente: clienteAtual,
        rota: atual.rota || clienteAtual.rota || "",
        hoje,
        atualizadoEm,
      };

      const temUltima = Boolean(String(atual.ultimaCobrancaEm || "").trim());
      const temPenultima = Boolean(String(atual.penultimaCobrancaEm || "").trim());

      if (!temUltima) {
        proximo.ultimaCobrancaEm = ultimaDataverse;
        proximo.ultimaCobrancaValor = arredondar2(saldo);
        proximo.penultimaCobrancaEm = penultimaDataverse;
        proximo.penultimaCobrancaValor = numero(atual.penultimaCobrancaValor);
        proximo.antepenultimaCobrancaEm = atual.antepenultimaCobrancaEm || "";
        proximo.antepenultimaCobrancaValor = numero(atual.antepenultimaCobrancaValor);
        proximo.mediaVendaPorDia = arredondar2(saldo / intervaloDataverse);
        proximo.estimativaAtualDia = arredondar2(
          proximo.mediaVendaPorDia * diasDesdeHistoricoDataverse(ultimaDataverse, agora),
        );
      } else if (!temPenultima) {
        const ultimaAtual = String(atual.ultimaCobrancaEm || "").trim();
        const intervaloAtual = diasEntreHistoricoDataverse(ultimaAtual, ultimaDataverse);
        if (intervaloAtual <= 0) {
          ignorados += 1;
          return;
        }

        proximo.penultimaCobrancaEm = ultimaDataverse;
        proximo.penultimaCobrancaValor = arredondar2(saldo);
        proximo.antepenultimaCobrancaEm = atual.antepenultimaCobrancaEm || penultimaDataverse;
        proximo.antepenultimaCobrancaValor = numero(atual.antepenultimaCobrancaValor);
        proximo.mediaVendaPorDia = arredondar2(numero(atual.ultimaCobrancaValor) / intervaloAtual);
        proximo.estimativaAtualDia = arredondar2(
          proximo.mediaVendaPorDia * diasDesdeHistoricoDataverse(ultimaAtual, agora),
        );
      } else {
        return;
      }

      updates[`${MEDIA_VENDAS_ROOT}/${numeroKey}`] = proximo;
      atualizados += 1;
    });

    if (Object.keys(updates).length) {
      updates[MEDIA_VENDAS_METADATA_PATH] = Date.now();
      await db.ref().update(updates);
    }

    logger.info("Media_de_Vendas completada com cobrancas_dataverse.", {
      analisados,
      atualizados,
      ignorados,
      data: hoje,
    });
  },
);

exports.verificarBalancoDiario = onSchedule(
  {
    schedule: "0 1 * * *",
    timeZone: "America/Sao_Paulo",
  },
  async () => {
    const db = admin.database();
    const agora = new Date();
    const agoraIso = agora.toISOString();
    const hojeBrasilia = dataBrasiliaISOData(agora);
    const snap = await db.ref("contestacao_balanco").get();

    if (!snap.exists()) {
      logger.info("Nenhum usuario em contestacao_balanco para verificar.");
      return;
    }

    const updates = {};
    let totalUsuarios = 0;
    let totalOk = 0;
    let totalPendentes = 0;

    Object.entries(snap.val() || {}).forEach(([usuario, dadosUsuario]) => {
      const resumo = dadosUsuario && dadosUsuario[RESUMO_BALANCO_ID];
      if (!resumo || typeof resumo !== "object") return;

      totalUsuarios += 1;
      const itensPendentes = Number(resumo.itensPendentes || 0);
      const basePath = `contestacao_balanco/${usuario}/${RESUMO_BALANCO_ID}`;

      updates[`${basePath}/ultimaVerificacaoDiaria`] = hojeBrasilia;
      updates[`${basePath}/verificadoEm`] = agoraIso;

      if (itensPendentes <= 0) {
        totalOk += 1;
        updates[`${basePath}/statusBalanco`] = "ok";
        updates[`${basePath}/pendenteDesde`] = null;
        updates[`${basePath}/pendenteDesdeData`] = null;
        updates[`${basePath}/ultimoZeradoEm`] = agoraIso;
        updates[`${basePath}/ultimaDataZerado`] = hojeBrasilia;
        return;
      }

      const pendenteDesdeData = resumo.pendenteDesdeData || hojeBrasilia;
      const statusBalanco = deveMarcarPendente(pendenteDesdeData, hojeBrasilia) ?
        "pendente" :
        "ok";

      if (statusBalanco === "pendente") totalPendentes += 1;
      else totalOk += 1;

      updates[`${basePath}/statusBalanco`] = statusBalanco;
      updates[`${basePath}/pendenteDesdeData`] = pendenteDesdeData;
      updates[`${basePath}/pendenteDesde`] = resumo.pendenteDesde || agoraIso;
    });

    if (Object.keys(updates).length) {
      await db.ref().update(updates);
    }

    logger.info("Verificacao diaria do balanco concluida.", {
      totalUsuarios,
      totalOk,
      totalPendentes,
      data: hojeBrasilia,
    });
  },
);

exports.verificarLiberacaoRotasDiaria = onSchedule(
  {
    schedule: "1 0 * * *",
    timeZone: "America/Sao_Paulo",
  },
  async () => {
    const db = admin.database();
    const agora = new Date();
    const agoraIso = agora.toISOString();
    const agoraMs = agora.getTime();
    const hojeBrasilia = dataBrasiliaISOData(agora);
    const rotasSnap = await db.ref("selecao_rotas/ativa/rotas").get();

    if (!rotasSnap.exists()) {
      logger.info("Nenhuma rota ativa para verificar liberacao.");
      return;
    }

    const updates = {
      "selecao_rotas/ativa/ultima_verificacao_liberacao_em": agoraIso,
      "selecao_rotas/ativa/ultima_verificacao_liberacao_data": hojeBrasilia,
    };
    let totalSelecionadas = 0;
    let totalLiberadas = 0;
    let totalAtualizadas = 0;

    Object.entries(rotasSnap.val() || {}).forEach(([numeroRota, rota]) => {
      if (!rota || typeof rota !== "object" || !rota.selecionada_por) return;

      totalSelecionadas += 1;
      const basePath = `selecao_rotas/ativa/rotas/${numeroRota}`;
      const selecionadaMs = timestampValido(rota.selecionada_em);
      const validadeMaximaMs = selecionadaMs == null ?
        null :
        selecionadaMs + CINCO_DIAS_MS;
      const liberarMs = timestampValido(rota.liberar_em) ?? validadeMaximaMs;
      const validadeMaximaEm = isoOuNull(validadeMaximaMs);
      const liberarEm = isoOuNull(liberarMs);
      const liberarDataBrasilia = dataBrasiliaISODataPorTimestamp(liberarMs);

      if (liberarDataBrasilia && liberarDataBrasilia <= hojeBrasilia) {
        totalLiberadas += 1;
        updates[`${basePath}/selecionada_por`] = null;
        updates[`${basePath}/selecionada_em`] = null;
        updates[`${basePath}/validade_maxima_em`] = null;
        updates[`${basePath}/prazo_justificativa_em`] = null;
        updates[`${basePath}/liberar_em`] = null;
        updates[`${basePath}/motivo_liberacao`] = null;
        updates[`${basePath}/primeiro_atendimento_em`] = null;
        updates[`${basePath}/ultima_justificativa_em`] = null;
        updates[`${basePath}/controle_liberacao_atualizado_em`] = agoraIso;
        updates[`${basePath}/liberada_automaticamente_em`] = agoraIso;
        updates[`${basePath}/timestamp_selecao`] = null;
        return;
      }

      const precisaAtualizarResumo =
        (validadeMaximaEm && rota.validade_maxima_em !== validadeMaximaEm) ||
        (liberarEm && rota.liberar_em !== liberarEm) ||
        !rota.motivo_liberacao;

      if (!precisaAtualizarResumo) return;

      totalAtualizadas += 1;
      updates[`${basePath}/validade_maxima_em`] = validadeMaximaEm;
      updates[`${basePath}/liberar_em`] = liberarEm;
      updates[`${basePath}/motivo_liberacao`] =
        rota.motivo_liberacao || "prazo_maximo";
      updates[`${basePath}/controle_liberacao_atualizado_em`] = agoraIso;
    });

    if (Object.keys(updates).length) {
      await db.ref().update(updates);
    }

    logger.info("Verificacao diaria de liberacao de rotas concluida.", {
      totalSelecionadas,
      totalLiberadas,
      totalAtualizadas,
      data: hojeBrasilia,
    });
  },
);

exports.adianta_rota = onSchedule(
  {
    schedule: "5 0 */2 * *",
    timeZone: "America/Sao_Paulo",
  },
  async () => {
    const db = admin.database();
    const agora = new Date();
    const agoraIso = agora.toISOString();
    const [rotasSnap, mediaSnap, config] = await Promise.all([
      db.ref("selecao_rotas/ativa/rotas").get(),
      db.ref(MEDIA_VENDAS_ROOT).get(),
      obterConfiguracoesAutomaticas(db),
    ]);
    const valorMinimoRota = config.prioridadeRota.valorMinimo;
    const diasMaximosRota = config.prioridadeRota.diasMaximos;

    if (!rotasSnap.exists() || !mediaSnap.exists()) {
      logger.info("Adianta rota sem dados suficientes para processar.");
      return;
    }

    const rotasSessao = rotasSnap.val() || {};
    const rotasPorNumero = new Map();
    Object.entries(rotasSessao).forEach(([numeroRota, rota]) => {
      const numeroNormalizado = normalizarRota(numeroRota);
      if (numeroNormalizado) {
        rotasPorNumero.set(numeroNormalizado, {numeroRota, rota});
      }
    });

    const resumoPorRota = new Map();
    Object.values(mediaSnap.val() || {}).forEach((item) => {
      if (!item || typeof item !== "object") return;

      const numeroRota = normalizarRota(item.rota || item?.cliente?.rota);
      if (!numeroRota) return;

      const resumo = resumoPorRota.get(numeroRota) || {
        valorEstimado: 0,
        ultimaCobrancaEm: null,
      };
      const estimativa = numero(item.estimativaAtualDia);
      const ultimaCobrancaEm = item.ultimaCobrancaEm || null;
      const ultimaAtualMs = timestampValido(resumo.ultimaCobrancaEm);
      const ultimaItemMs = timestampValido(ultimaCobrancaEm);

      resumo.valorEstimado = arredondar2(resumo.valorEstimado + estimativa);
      if (ultimaItemMs != null && (ultimaAtualMs == null || ultimaItemMs > ultimaAtualMs)) {
        resumo.ultimaCobrancaEm = ultimaCobrancaEm;
      }
      resumoPorRota.set(numeroRota, resumo);
    });

    const updates = {};
    let totalAnalisadas = 0;
    let totalMarcadas = 0;

    resumoPorRota.forEach((resumo, numeroRota) => {
      const entrada = rotasPorNumero.get(numeroRota);
      if (!entrada) return;

      const {numeroRota: chaveRota, rota} = entrada;
      if (!rota || typeof rota !== "object") return;
      if (rota.selecionada_por || rota.prioridade_manual === true) return;

      totalAnalisadas += 1;
      const diasSemFazer = diasDesdeBrasilia(resumo.ultimaCobrancaEm, agora);
      const atendeValor = resumo.valorEstimado >= valorMinimoRota;
      const atendeTempo = diasSemFazer != null &&
        diasSemFazer >= diasMaximosRota;

      if (!atendeValor || !atendeTempo) return;

      const basePath = `selecao_rotas/ativa/rotas/${chaveRota}`;
      updates[`${basePath}/prioridade_manual`] = true;
      updates[`${basePath}/prioridade_manual_em`] = agoraIso;
      updates[`${basePath}/prioridade_manual_por`] = "Functions";
      totalMarcadas += 1;
    });

    if (Object.keys(updates).length) {
      await db.ref().update(updates);
    }

    logger.info("Adianta rota concluida.", {
      totalAnalisadas,
      totalMarcadas,
      valorMinimo: valorMinimoRota,
      diasMaximos: diasMaximosRota,
      data: dataBrasiliaISOData(agora),
    });
  },
);

exports.cadastrarManutencoesReposicaoSemVisita = onSchedule(
  {
    schedule: "50 0 * * *",
    timeZone: "America/Sao_Paulo",
  },
  async () => {
    const db = admin.database();
    const agora = new Date();
    const agoraIso = agora.toISOString();
    const [mediaSnap, clientesSnap, manutencoesSnap, config] = await Promise.all([
      db.ref(MEDIA_VENDAS_ROOT).get(),
      db.ref("clientes").get(),
      db.ref("manutencoes").get(),
      obterConfiguracoesAutomaticas(db),
    ]);
    const diasMinimosSemVisita = config.manutencaoSemVisita.diasMinimos;

    if (!mediaSnap.exists() || !clientesSnap.exists()) {
      logger.info("Reposicao sem visita sem dados suficientes para processar.");
      return;
    }

    const clientesPorId = new Map();
    const clientesPorNumero = new Map();
    Object.entries(clientesSnap.val() || {}).forEach(([clienteId, cliente]) => {
      const clienteComId = {firebaseUrl: clienteId, ...(cliente || {})};
      clientesPorId.set(clienteId, clienteComId);

      const numeroKey = normalizarNumeroCliente(cliente?.numero);
      if (numeroKey) clientesPorNumero.set(numeroKey, clienteComId);
    });

    const manutencoes = Object.values(manutencoesSnap.val() || {});
    const updates = {};
    let analisados = 0;
    let criados = 0;
    let concluidosPorVisita = 0;
    let ignoradosPorDuplicidade = 0;
    let ignoradosSemCliente = 0;
    let ignoradosEncerrados = 0;

    Object.entries(manutencoesSnap.val() || {}).forEach(([manutencaoId, manutencao]) => {
      const status = normalizarTextoBasico(manutencao?.status || "pendente");
      if (status === "concluida" || status === "concluido") return;
      if (normalizarTextoBasico(manutencao?.tipoAcao) !== "repor") return;

      const clienteNumero = normalizarNumeroCliente(manutencao?.clienteNumero);
      const clienteId = String(
        manutencao?.clienteId ||
        manutencao?.clienteFirebaseUrl ||
        "",
      ).trim();

      let mediaCliente = clienteNumero ?
        (mediaSnap.val() || {})[clienteNumero] :
        null;

      if (!mediaCliente && clienteId) {
        mediaCliente = Object.values(mediaSnap.val() || {}).find((media) =>
          String(media?.cliente?.id || "").trim() === clienteId,
        );
      }

      const diasDesdeVisita = diasDesdeBrasilia(
        mediaCliente?.ultimaCobrancaEm || "",
        agora,
      );
      if (diasDesdeVisita == null || diasDesdeVisita >= 2) return;

      updates[`manutencoes/${manutencaoId}/status`] = "concluida";
      updates[`manutencoes/${manutencaoId}/dataConclusao`] = agoraIso;
      updates[`manutencoes/${manutencaoId}/finalizadoPor`] = "Autom\u00e1tico";
      concluidosPorVisita += 1;
    });

    Object.entries(mediaSnap.val() || {}).forEach(([mediaKey, media]) => {
      if (!media || typeof media !== "object") return;

      const ultimaCobrancaEm = media.ultimaCobrancaEm || "";
      const diasSemVisita = diasDesdeBrasilia(ultimaCobrancaEm, agora);
      if (diasSemVisita == null ||
        diasSemVisita <= diasMinimosSemVisita) {
        return;
      }

      analisados += 1;

      const clienteId = String(media?.cliente?.id || "").trim();
      const numeroKey = normalizarNumeroCliente(
        media?.cliente?.numero || mediaKey,
      );
      const cliente =
        (clienteId && clientesPorId.get(clienteId)) ||
        (numeroKey && clientesPorNumero.get(numeroKey)) ||
        null;

      if (!cliente) {
        ignoradosSemCliente += 1;
        return;
      }

      if (cliente.encerrado === true) {
        ignoradosEncerrados += 1;
        return;
      }

      const clienteDbId = String(cliente.firebaseUrl || clienteId || "").trim();
      const clienteNumero = normalizarNumeroCliente(cliente.numero || numeroKey);
      const jaExiste = manutencoes.some((manutencao) =>
        manutencaoReporSemVisitaJaCriada(
          manutencao,
          clienteDbId,
          clienteNumero,
          ultimaCobrancaEm,
        ),
      );

      if (jaExiste) {
        ignoradosPorDuplicidade += 1;
        return;
      }

      const equipamentos = obterTextoEquipamentosCliente(cliente);
      const listaMaquinas = equipamentos || "maquinas do ponto";
      const {foto, thumb} = obterFotoFichaCliente(cliente);
      const novaRef = db.ref("manutencoes").push();

      updates[`manutencoes/${novaRef.key}`] = {
        clienteId: clienteDbId,
        clienteNumero: cliente.numero || clienteNumero,
        clienteNome: cliente.nome || media?.cliente?.nome || "",
        clienteCidade: cliente.cidade || "",
        clienteRota: cliente.rota || media?.cliente?.rota || media.rota || "",
        clienteEndereco: cliente.endereco || "",
        clienteEquip: cliente.equip || equipamentos,
        tipoAcao: "Repor",
        observacoes:
          `Cliente ha ${diasSemVisita} dias sem visita. ` +
          `Levar material para abastecer maquinas: ${listaMaquinas}. ` +
          "Se as vendas forem fracas, retirar e encerrar esse ponto.",
        fotoSolicitacao: foto,
        fotoSolicitacaoThumb: thumb,
        fotosClienteReferencia: [],
        status: "pendente",
        cadastradoPor: "Autom\u00e1tico",
        dataRegistro: agoraIso,
      };
      criados += 1;
    });

    if (Object.keys(updates).length) {
      await db.ref().update(updates);
    }

    logger.info("Reposicao sem visita concluida.", {
      analisados,
      criados,
      concluidosPorVisita,
      ignoradosPorDuplicidade,
      ignoradosSemCliente,
      ignoradosEncerrados,
      diasMinimos: diasMinimosSemVisita,
      data: dataBrasiliaISOData(agora),
    });
  },
);

function numeroEstoquePedido(valor) {
  if (typeof valor === "number") {
    return Number.isFinite(valor) ? valor : 0;
  }
  const texto = String(valor || "").trim().replace(/\./g, "").replace(",", ".");
  const n = Number(texto);
  return Number.isFinite(n) ? n : 0;
}

function formatarQuantidadePedidoProduto(valor) {
  const n = numeroEstoquePedido(valor);
  return Number.isInteger(n) ? String(n) : String(n).replace(".", ",");
}

function pedidoAutomaticoProdutoJaExiste(pedidos, nomeProduto) {
  const nomeNormalizado = normalizarTextoBasico(nomeProduto);
  if (!nomeNormalizado) return false;

  return Object.values(pedidos || {}).some((pedido) => {
    if (!pedido || typeof pedido !== "object") return false;
    if (normalizarTextoBasico(pedido.categoria) !== "produtos") return false;
    if (normalizarTextoBasico(pedido.solicitante) !== "pedido automatico") {
      return false;
    }

    const descricao = normalizarTextoBasico(pedido.descricao);
    return descricao.startsWith(`comprar ${nomeNormalizado} - abaixo do minimo`);
  });
}

function nomeNormalizadoPedidoAutomaticoProduto(pedido) {
  if (!pedido || typeof pedido !== "object") return "";
  if (normalizarTextoBasico(pedido.categoria) !== "produtos") return "";
  if (normalizarTextoBasico(pedido.solicitante) !== "pedido automatico") {
    return "";
  }

  const match = normalizarTextoBasico(pedido.descricao)
    .match(/^comprar\s+(.+?)\s+-\s+abaixo do minimo/);
  return match ? String(match[1] || "").trim() : "";
}

exports.pedidoAutomaticoProdutos = onSchedule(
  {
    schedule: "0 9 * * *",
    timeZone: "America/Sao_Paulo",
  },
  async () => {
    const db = admin.database();
    const agora = new Date();
    const agoraIso = agora.toISOString();
    const [estoqueSnap, pedidosSnap] = await Promise.all([
      db.ref("estoque").get(),
      db.ref("pedidos").get(),
    ]);

    if (!estoqueSnap.exists()) {
      logger.info("Pedido automatico de produtos sem estoque para processar.");
      return;
    }

    const pedidos = pedidosSnap.val() || {};
    const updates = {};
    let analisados = 0;
    let criados = 0;
    let ignoradosSemMinimo = 0;
    let ignoradosAcimaDoMinimo = 0;
    let ignoradosPorDuplicidade = 0;
    let removidosAcimaDoMinimo = 0;

    const produtosPorNome = new Map();
    Object.values(estoqueSnap.val() || {}).forEach((item) => {
      if (!item || typeof item !== "object") return;
      if (normalizarTextoBasico(item.categoria) !== "produtos") return;

      const nome = String(item.nome || "").trim();
      const nomeNormalizado = normalizarTextoBasico(nome);
      if (!nomeNormalizado) return;

      produtosPorNome.set(nomeNormalizado, {
        quantidade: numeroEstoquePedido(item.quantidade),
        minimo: numeroEstoquePedido(item.minimo),
      });
    });

    Object.entries(pedidos).forEach(([pedidoId, pedido]) => {
      const nomePedido = nomeNormalizadoPedidoAutomaticoProduto(pedido);
      if (!nomePedido) return;

      const produto = produtosPorNome.get(nomePedido);
      if (!produto || produto.minimo <= 0) return;
      if (produto.quantidade < produto.minimo) return;

      updates[`pedidos/${pedidoId}`] = null;
      removidosAcimaDoMinimo += 1;
    });

    Object.values(estoqueSnap.val() || {}).forEach((item) => {
      if (!item || typeof item !== "object") return;
      if (normalizarTextoBasico(item.categoria) !== "produtos") return;

      const nome = String(item.nome || "").trim();
      if (!nome) return;

      analisados += 1;
      const minimo = numeroEstoquePedido(item.minimo);
      if (minimo <= 0) {
        ignoradosSemMinimo += 1;
        return;
      }

      const quantidade = numeroEstoquePedido(item.quantidade);
      if (quantidade >= minimo) {
        ignoradosAcimaDoMinimo += 1;
        return;
      }

      if (pedidoAutomaticoProdutoJaExiste(pedidos, nome)) {
        ignoradosPorDuplicidade += 1;
        return;
      }

      const novaRef = db.ref("pedidos").push();
      updates[`pedidos/${novaRef.key}`] = {
        descricao:
          `Comprar ${nome} - abaixo do minimo ` +
          `- Qtd atual ${formatarQuantidadePedidoProduto(quantidade)}`,
        categoria: "produtos",
        solicitante: "Pedido automático",
        data: agoraIso,
      };
      criados += 1;
    });

    if (Object.keys(updates).length) {
      await db.ref().update(updates);
    }

    logger.info("Pedido automatico de produtos concluido.", {
      analisados,
      criados,
      ignoradosSemMinimo,
      ignoradosAcimaDoMinimo,
      ignoradosPorDuplicidade,
      removidosAcimaDoMinimo,
      data: dataBrasiliaISOData(agora),
    });
  },
);
