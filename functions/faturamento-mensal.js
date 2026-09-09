const VERSAO_RESUMO_FATURAMENTO = 1;
const FONTE_RESUMO_FATURAMENTO = "atendimentos.financeiro.totalGeral";

function numero(valor) {
  const resultado = Number(valor || 0);
  return Number.isFinite(resultado) ? resultado : 0;
}

function arredondar2(valor) {
  return Math.round((numero(valor) + Number.EPSILON) * 100) / 100;
}

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

function obterContribuicaoFaturamento(atendimento) {
  if (!atendimento || typeof atendimento !== "object" || atendimento._teste) return null;
  if (["retirada_estoque", "entrada_estoque"].includes(atendimento.origemRegistro)) return null;
  const competencia = obterCompetenciaBrasilia(
    atendimento.data || atendimento.dataHora || atendimento.timestamp,
  );
  const valor = arredondar2(atendimento?.financeiro?.totalGeral);
  return competencia && valor > 0 ? {competencia, valor} : null;
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
  const valores = Object.values(contribuicoes).map(numero).filter((valor) => valor > 0);
  return {
    totalGeral: arredondar2(valores.reduce((soma, valor) => soma + valor, 0)),
    quantidadeAtendimentos: valores.length,
    atualizadoEm,
    versao: VERSAO_RESUMO_FATURAMENTO,
    fonte: FONTE_RESUMO_FATURAMENTO,
    contribuicoes: {...contribuicoes},
  };
}

function aplicarContribuicaoAoResumo(
  resumoAtual,
  atendimentoId,
  valorAlvo,
  resumoBase,
  atualizadoEm = new Date().toISOString(),
) {
  const possuiFormatoAtual = Number(resumoAtual?.versao) === VERSAO_RESUMO_FATURAMENTO;
  const origem = possuiFormatoAtual ? resumoAtual : (resumoBase || criarResumoBase({}, atualizadoEm));
  const contribuicoes = {...(origem.contribuicoes || {})};
  const valorAnterior = numero(contribuicoes[atendimentoId]);
  const valorNovo = arredondar2(valorAlvo);
  if (valorNovo > 0) contribuicoes[atendimentoId] = valorNovo;
  else delete contribuicoes[atendimentoId];
  return {
    totalGeral: Math.max(0, arredondar2(numero(origem.totalGeral) - valorAnterior + Math.max(0, valorNovo))),
    quantidadeAtendimentos: Object.keys(contribuicoes).length,
    atualizadoEm,
    versao: VERSAO_RESUMO_FATURAMENTO,
    fonte: FONTE_RESUMO_FATURAMENTO,
    contribuicoes,
  };
}

module.exports = {
  VERSAO_RESUMO_FATURAMENTO,
  FONTE_RESUMO_FATURAMENTO,
  obterCompetenciaBrasilia,
  obterContribuicaoFaturamento,
  criarPeriodoCompetencia,
  criarResumoBase,
  aplicarContribuicaoAoResumo,
};
