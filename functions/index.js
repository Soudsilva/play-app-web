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
const DEPOSITOS_RESUMO_ROOT = "firebase_functions_depositos";
const LIMITE_BLOQUEIO_DEPOSITO = 5000;
const MEDIA_VENDAS_ROOT = "Media_de_Vendas";
const MEDIA_VENDAS_METADATA_PATH = "metadata/media_de_vendas_versao";

function dataBrasiliaISOData(date = new Date()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
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

function normalizarNumeroCliente(valor) {
  return String(valor ?? "").trim();
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

function valorAtualMedia(registroAtual, campo) {
  const valor = campo ? registroAtual?.[campo] : null;
  if (valor !== undefined && valor !== null && valor !== "") return valor;
  return null;
}

function numeroAtualMedia(registroAtual, campo) {
  const valor = Number(valorAtualMedia(registroAtual, campo));
  return Number.isFinite(valor) ? valor : 0;
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
    mediaVendaPorDia = arredondar2(numeroAtualMedia(registroAtual, "mediaVendaPorDia"));

    if (usadas.length === 1) {
      const ultimaAtual = valorAtualMedia(registroAtual, "ultimaCobrancaEm");
      const ultimaAtualValor = numeroAtualMedia(registroAtual, "ultimaCobrancaValor");
      const mesmaUltima = ultimaAtual && Date.parse(ultimaAtual) === Date.parse(ultima.data);
      penultima = {
        data: valorAtualMedia(registroAtual, "penultimaCobrancaEm") ||
          (!mesmaUltima ? ultimaAtual : ""),
        total: numeroAtualMedia(registroAtual, "penultimaCobrancaValor") ||
          (!mesmaUltima ? ultimaAtualValor : 0),
      };
      antepenultima = {
        data: valorAtualMedia(registroAtual, "antepenultimaCobrancaEm") || "",
        total: numeroAtualMedia(registroAtual, "antepenultimaCobrancaValor"),
      };
    } else {
      ultima = {
        data: valorAtualMedia(registroAtual, "ultimaCobrancaEm") || "",
        total: numeroAtualMedia(registroAtual, "ultimaCobrancaValor"),
      };
      penultima = {
        data: valorAtualMedia(registroAtual, "penultimaCobrancaEm") || "",
        total: numeroAtualMedia(registroAtual, "penultimaCobrancaValor"),
      };
      antepenultima = {
        data: valorAtualMedia(registroAtual, "antepenultimaCobrancaEm") || "",
        total: numeroAtualMedia(registroAtual, "antepenultimaCobrancaValor"),
      };
    }

    const diasDesdeUltima = ultima?.data ?
      diasEntreDatasBrasilia(`${hoje}T00:00:00-03:00`, ultima.data) :
      0;
    estimativaAtualDia = arredondar2(mediaVendaPorDia * diasDesdeUltima);
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

      if (liberarMs != null && agoraMs >= liberarMs) {
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
