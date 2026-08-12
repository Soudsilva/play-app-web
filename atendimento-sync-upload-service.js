import {
    ref as storageRef,
    uploadBytesResumable,
    getDownloadURL
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-storage.js";
import { storage } from './firebase-app.js';

const TEMPO_MAXIMO_SEM_PROGRESSO_MS = 90 * 1000;

function base64ParaBlob(base64) {
    const texto = String(base64 || '');
    const correspondencia = texto.match(/^data:([^;,]+)?(?:;base64)?,(.*)$/);
    if (!correspondencia) throw new Error('Foto local invalida para upload.');
    const tipo = correspondencia[1] || 'image/jpeg';
    const bytes = atob(correspondencia[2]);
    const partes = [];
    for (let inicio = 0; inicio < bytes.length; inicio += 512) {
        const trecho = bytes.slice(inicio, inicio + 512);
        const numeros = new Uint8Array(trecho.length);
        for (let indice = 0; indice < trecho.length; indice++) {
            numeros[indice] = trecho.charCodeAt(indice);
        }
        partes.push(numeros);
    }
    return new Blob(partes, { type: tipo });
}

async function criarMiniaturaBase64(base64, maxPx) {
    return new Promise(resolve => {
        const imagem = new Image();
        imagem.onload = () => {
            const proporcao = Math.min(maxPx / imagem.width, maxPx / imagem.height, 1);
            if (proporcao >= 1) {
                resolve(base64);
                return;
            }
            const canvas = document.createElement('canvas');
            canvas.width = Math.max(1, Math.round(imagem.width * proporcao));
            canvas.height = Math.max(1, Math.round(imagem.height * proporcao));
            canvas.getContext('2d').drawImage(imagem, 0, 0, canvas.width, canvas.height);
            resolve(canvas.toDataURL('image/jpeg', 0.75));
        };
        imagem.onerror = () => resolve(base64);
        imagem.src = base64;
    });
}

async function obterUrlExistente(referencia) {
    try {
        return await getDownloadURL(referencia);
    } catch (_) {
        return '';
    }
}

function enviarBlobRetomavel(referencia, blob) {
    return new Promise((resolve, reject) => {
        const tarefa = uploadBytesResumable(referencia, blob, { contentType: blob.type || 'image/jpeg' });
        let timer = null;
        let encerrado = false;
        let cancelarObservacao = () => {};

        const limpar = () => {
            if (timer !== null) globalThis.clearTimeout(timer);
            timer = null;
            cancelarObservacao();
        };
        const falhar = erro => {
            if (encerrado) return;
            encerrado = true;
            limpar();
            reject(erro);
        };
        const renovarLimiteInatividade = () => {
            if (timer !== null) globalThis.clearTimeout(timer);
            timer = globalThis.setTimeout(() => {
                if (encerrado) return;
                encerrado = true;
                limpar();
                tarefa.cancel();
                reject(new Error('Upload sem progresso por tempo prolongado. A tentativa foi cancelada.'));
            }, TEMPO_MAXIMO_SEM_PROGRESSO_MS);
        };

        cancelarObservacao = tarefa.on(
            'state_changed',
            () => renovarLimiteInatividade(),
            erro => falhar(erro),
            () => {
                if (encerrado) return;
                encerrado = true;
                limpar();
                resolve(tarefa.snapshot);
            }
        );
        renovarLimiteInatividade();
    });
}

async function salvarArquivoEstavel(base64, caminho) {
    const referencia = storageRef(storage, caminho);
    const urlExistente = await obterUrlExistente(referencia);
    if (urlExistente) return urlExistente;

    const snapshot = await enviarBlobRetomavel(referencia, base64ParaBlob(base64));
    return getDownloadURL(snapshot.ref);
}

export async function salvarFotoAtendimentoRetomavel(
    base64,
    pasta = 'atendimentos',
    thumbMaxPx = 200,
    nomeArquivoEstavel = ''
) {
    if (!navigator.onLine) throw new Error('Sem internet para enviar a foto.');
    const nome = String(nomeArquivoEstavel || '').replace(/[^a-zA-Z0-9_-]/g, '');
    if (!nome) throw new Error('Nome estavel da foto do atendimento nao informado.');

    const pastaLimpa = String(pasta || 'atendimentos').replace(/^\/+|\/+$/g, '');
    const miniatura = await criarMiniaturaBase64(base64, thumbMaxPx).catch(() => base64);
    const url = await salvarArquivoEstavel(base64, `${pastaLimpa}/${nome}.jpg`);
    const thumbUrl = miniatura === base64
        ? url
        : await salvarArquivoEstavel(miniatura, `${pastaLimpa}/thumbs/${nome}.jpg`);

    return { url, thumbUrl };
}
