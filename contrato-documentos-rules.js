import {
    normalizarNomeArquivo,
    validarMiniaturaArquivo,
    validarPdfParaImpressao
} from './arquivos-impressao-rules.js?v=48';

export const PRAZO_APROVACAO_DOCUMENTO_MS = 24 * 60 * 60 * 1000;

export const DOCUMENTOS_PADRAO = Object.freeze([
    {
        id: 'contrato_social',
        nome: 'Contrato Social',
        formato: 'pdf',
        detalhePadrao: 'Atualizado em Jan 2025',
        icone: '📑',
        ordem: 1000
    },
    {
        id: 'acordo_socios',
        nome: 'Acordo de Sócios',
        formato: 'pdf',
        detalhePadrao: 'Assinado em Fev 2025',
        icone: '🤝',
        ordem: 2000
    },
    {
        id: 'modelo_contrato_ponto',
        nome: 'Modelo Contrato Ponto',
        formato: 'docx',
        detalhePadrao: 'Versão 2.1',
        icone: '📋',
        ordem: 3000
    },
    {
        id: 'cnpj_alvara',
        nome: 'CNPJ & Alvará',
        formato: 'pdf',
        detalhePadrao: 'Vencimento: Dez 2025',
        icone: '🏛️',
        ordem: 4000
    }
]);

export function obterNomeDocumento(arquivo) {
    const nome = String(arquivo?.name || '').replace(/\.pdf$/i, '').replace(/\s+/g, ' ').trim();
    if (nome.length < 2 || nome.length > 120) {
        throw new Error('O nome do arquivo deve ter entre 2 e 120 caracteres.');
    }
    return nome;
}

export function validarArquivoDocumento(arquivo) {
    validarPdfParaImpressao(arquivo);
    return arquivo;
}

export function validarMiniaturaDocumento(arquivo) {
    validarMiniaturaArquivo(arquivo);
    return arquivo;
}

export function criarNomeBuscaDocumento(nome) {
    return normalizarNomeArquivo(nome);
}

export function obterDocumentosExibidos(ativos = {}) {
    const idsPadrao = new Set(DOCUMENTOS_PADRAO.map(item => item.id));
    const padroes = DOCUMENTOS_PADRAO
        .map(item => ({ ...item, ...(ativos?.[item.id] || {}), id: item.id }))
        .filter(item => item.excluido !== true);
    const adicionais = Object.entries(ativos || {})
        .filter(([id, item]) => !idsPadrao.has(id) && item?.excluido !== true)
        .map(([id, item]) => ({ id, ...item }))
        .sort((a, b) => Number(a.ordem || 0) - Number(b.ordem || 0));
    return [...padroes, ...adicionais];
}

export function propostaDocumentoExpirada(proposta, agora = Date.now()) {
    const expiraEm = Number(proposta?.expiraEm);
    return !Number.isFinite(expiraEm) || expiraEm <= agora;
}
