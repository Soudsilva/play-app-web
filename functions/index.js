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
