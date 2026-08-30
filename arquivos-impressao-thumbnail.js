const LARGURA_MAXIMA = 320;
const ALTURA_MAXIMA = 480;
const QUALIDADE_MINIATURA = 0.78;

let carregamentoPdfJs = null;

async function carregarPdfJs() {
    if (!carregamentoPdfJs) {
        carregamentoPdfJs = import('./assets/vendor/pdfjs/pdf.min.mjs')
            .then(pdfjs => {
                pdfjs.GlobalWorkerOptions.workerSrc = new URL(
                    './assets/vendor/pdfjs/pdf.worker.min.mjs',
                    import.meta.url
                ).href;
                return pdfjs;
            })
            .catch(erro => {
                carregamentoPdfJs = null;
                throw erro;
            });
    }
    return carregamentoPdfJs;
}

function converterCanvasParaBlob(canvas, tipo, qualidade) {
    return new Promise(resolve => canvas.toBlob(resolve, tipo, qualidade));
}

function limparNomeArquivo(nome) {
    return String(nome || 'arquivo').replace(/\.pdf$/i, '').trim() || 'arquivo';
}

export async function gerarMiniaturaPdf(arquivo) {
    if (!(arquivo instanceof File) || !arquivo.size) {
        throw new Error('Selecione um PDF válido para criar a miniatura.');
    }

    const pdfjs = await carregarPdfJs();
    const tarefa = pdfjs.getDocument({
        data: new Uint8Array(await arquivo.arrayBuffer())
    });
    let pdf = null;
    let pagina = null;
    let canvas = null;

    try {
        pdf = await tarefa.promise;
        pagina = await pdf.getPage(1);
        const tamanhoOriginal = pagina.getViewport({ scale: 1 });
        const escala = Math.min(
            1,
            LARGURA_MAXIMA / tamanhoOriginal.width,
            ALTURA_MAXIMA / tamanhoOriginal.height
        );
        const tamanhoMiniatura = pagina.getViewport({ scale: Math.max(escala, 0.1) });

        canvas = document.createElement('canvas');
        canvas.width = Math.max(1, Math.ceil(tamanhoMiniatura.width));
        canvas.height = Math.max(1, Math.ceil(tamanhoMiniatura.height));
        const contexto = canvas.getContext('2d', { alpha: false });
        if (!contexto) throw new Error('Este aparelho não conseguiu preparar a miniatura.');

        contexto.fillStyle = '#ffffff';
        contexto.fillRect(0, 0, canvas.width, canvas.height);
        await pagina.render({
            canvasContext: contexto,
            viewport: tamanhoMiniatura,
            background: '#ffffff'
        }).promise;

        let tipo = 'image/webp';
        let extensao = 'webp';
        let blob = await converterCanvasParaBlob(canvas, tipo, QUALIDADE_MINIATURA);
        if (!blob || blob.type !== tipo) {
            tipo = 'image/jpeg';
            extensao = 'jpg';
            blob = await converterCanvasParaBlob(canvas, tipo, QUALIDADE_MINIATURA);
        }
        if (!blob?.size) throw new Error('Não foi possível criar a imagem da primeira página.');

        return new File(
            [blob],
            `${limparNomeArquivo(arquivo.name)}-miniatura.${extensao}`,
            { type: tipo, lastModified: Date.now() }
        );
    } finally {
        pagina?.cleanup();
        if (typeof tarefa.destroy === 'function') await tarefa.destroy();
        if (canvas) {
            canvas.width = 0;
            canvas.height = 0;
        }
    }
}
