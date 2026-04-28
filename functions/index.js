const {setGlobalOptions} = require("firebase-functions");
const {onSchedule} = require("firebase-functions/v2/scheduler");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");

setGlobalOptions({maxInstances: 10});

admin.initializeApp({
  databaseURL: "https://play-na-web-default-rtdb.firebaseio.com",
});

const RESUMO_BALANCO_ID = "resumo_balanco";

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
