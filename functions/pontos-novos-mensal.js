const VERSAO_RESUMO_PONTOS_NOVOS = 1;
const FONTE_RESUMO_PONTOS_NOVOS = "manutencoes.tipoAcao=Ponto Novo";

function obterCompetenciaBrasilia(valor) {
  const data = new Date(valor || 0);
  if (Number.isNaN(data.getTime())) return "";
  const partes = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
  }).formatToParts(data);
  const mapa = Object.fromEntries(partes.map((parte) => [parte.type, parte.value]));
  return mapa.year && mapa.month ? `${mapa.year}-${mapa.month}` : "";
}

function obterContribuicaoPontoNovo(manutencao) {
  if (!manutencao || typeof manutencao !== "object") return null;
  if (String(manutencao.tipoAcao || "").trim() !== "Ponto Novo") return null;
  const competencia = obterCompetenciaBrasilia(manutencao.dataRegistro);
  return competencia ? {competencia, quantidade: 1} : null;
}

function criarPeriodoCompetencia(competencia) {
  const correspondencia = /^(\d{4})-(\d{2})$/.exec(String(competencia || ""));
  if (!correspondencia) return null;
  const ano = Number(correspondencia[1]);
  const mes = Number(correspondencia[2]);
  if (mes < 1 || mes > 12) return null;
  const proximoMes = mes === 12 ? 1 : mes + 1;
  const proximoAno = mes === 12 ? ano + 1 : ano;
  return {
    inicioIso: `${ano}-${String(mes).padStart(2, "0")}-01T00:00:00-03:00`,
    fimIso: `${proximoAno}-${String(proximoMes).padStart(2, "0")}-01T00:00:00-03:00`,
  };
}

function criarResumoBase(contribuicoes = {}, atualizadoEm = new Date().toISOString()) {
  const contribuicoesAtivas = Object.fromEntries(
    Object.entries(contribuicoes).filter(([, quantidade]) => Number(quantidade) === 1),
  );
  return {
    quantidadePontos: Object.keys(contribuicoesAtivas).length,
    atualizadoEm,
    versao: VERSAO_RESUMO_PONTOS_NOVOS,
    fonte: FONTE_RESUMO_PONTOS_NOVOS,
    contribuicoes: contribuicoesAtivas,
  };
}

function aplicarContribuicaoAoResumo(
  resumoAtual,
  manutencaoId,
  deveContar,
  resumoBase,
  atualizadoEm = new Date().toISOString(),
) {
  const possuiFormatoAtual = Number(resumoAtual?.versao) === VERSAO_RESUMO_PONTOS_NOVOS;
  const origem = possuiFormatoAtual ? resumoAtual : (resumoBase || criarResumoBase({}, atualizadoEm));
  const contribuicoes = {...(origem.contribuicoes || {})};
  if (deveContar) contribuicoes[manutencaoId] = 1;
  else delete contribuicoes[manutencaoId];
  return criarResumoBase(contribuicoes, atualizadoEm);
}

module.exports = {
  VERSAO_RESUMO_PONTOS_NOVOS,
  FONTE_RESUMO_PONTOS_NOVOS,
  obterCompetenciaBrasilia,
  obterContribuicaoPontoNovo,
  criarPeriodoCompetencia,
  criarResumoBase,
  aplicarContribuicaoAoResumo,
};
