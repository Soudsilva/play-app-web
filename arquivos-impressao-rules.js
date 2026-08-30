const TAMANHO_MAXIMO_PDF = 25 * 1024 * 1024;
const TAMANHO_MAXIMO_MINIATURA = 5 * 1024 * 1024;

export function normalizarNomeArquivo(valor) {
    return String(valor || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, ' ')
        .trim();
}

export function criarChaveOrdenacaoArquivo(nome, id) {
    return `${normalizarNomeArquivo(nome)}__${String(id || '').trim()}`;
}

export function limparNomeParaCaminho(nome) {
    const semExtensao = String(nome || '').replace(/\.pdf$/i, '');
    return normalizarNomeArquivo(semExtensao).replace(/\s+/g, '-').slice(0, 80) || 'arquivo';
}

export function validarPdfParaImpressao(arquivo) {
    if (!(arquivo instanceof File)) throw new Error('Selecione um arquivo PDF.');
    const nomeValido = /\.pdf$/i.test(arquivo.name || '');
    const tipoValido = !arquivo.type || arquivo.type === 'application/pdf';
    if (!nomeValido || !tipoValido) throw new Error('O arquivo precisa estar no formato PDF.');
    if (arquivo.size <= 0) throw new Error('O PDF selecionado está vazio.');
    if (arquivo.size > TAMANHO_MAXIMO_PDF) throw new Error('O PDF deve ter no máximo 25 MB.');
}

export function validarMiniaturaArquivo(arquivo) {
    if (!arquivo) return;
    if (!(arquivo instanceof File) || !/^image\/(jpeg|png|webp)$/i.test(arquivo.type || '')) {
        throw new Error('A miniatura deve ser uma imagem JPG, PNG ou WebP.');
    }
    if (arquivo.size <= 0) throw new Error('A miniatura selecionada está vazia.');
    if (arquivo.size > TAMANHO_MAXIMO_MINIATURA) throw new Error('A miniatura deve ter no máximo 5 MB.');
}

export function formatarTamanhoArquivo(bytes) {
    const valor = Number(bytes || 0);
    if (!Number.isFinite(valor) || valor <= 0) return 'Tamanho não informado';
    if (valor < 1024 * 1024) return `${Math.max(1, Math.round(valor / 1024))} KB`;
    return `${(valor / (1024 * 1024)).toFixed(valor >= 10 * 1024 * 1024 ? 0 : 1).replace('.', ',')} MB`;
}

export function formatarDataArquivo(valor) {
    const data = new Date(valor || '');
    if (Number.isNaN(data.getTime())) return '';
    return new Intl.DateTimeFormat('pt-BR', { dateStyle: 'short' }).format(data);
}

export function obterExtensaoMiniatura(arquivo) {
    const tipo = String(arquivo?.type || '').toLowerCase();
    if (tipo === 'image/png') return 'png';
    if (tipo === 'image/webp') return 'webp';
    return 'jpg';
}
