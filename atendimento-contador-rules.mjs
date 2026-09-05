import { normalizarNumeroPix } from './pix-posse-rules.mjs';

export { normalizarNumeroPix };

export function normalizarValorContador(valor) {
    if (valor === null || valor === undefined || valor === '') return null;

    if (typeof valor === 'number') {
        return Number.isFinite(valor) ? Math.max(0, Math.trunc(valor)) : null;
    }

    const digitos = String(valor).replace(/\D/g, '');
    if (!digitos) return null;

    const numero = Number.parseInt(digitos, 10);
    return Number.isFinite(numero) ? numero : null;
}

export function extrairContadoresEquipDetalhes(equipDetalhes) {
    const itens = Array.isArray(equipDetalhes)
        ? equipDetalhes
        : Object.values(equipDetalhes || {});
    const contadores = {};

    itens.forEach(item => {
        const numeroPix = normalizarNumeroPix(item?.pix ?? item?.numero_pix);
        const contador = normalizarValorContador(item?.contador);
        if (!numeroPix || contador === null) return;
        contadores[numeroPix] = contador;
    });

    return contadores;
}

export function mesclarContadoresPorPrioridade(...fontes) {
    const resultado = {};

    fontes.forEach(fonte => {
        Object.entries(fonte || {}).forEach(([numeroPixBruto, contadorBruto]) => {
            const numeroPix = normalizarNumeroPix(numeroPixBruto);
            const contador = normalizarValorContador(contadorBruto);
            if (!numeroPix || contador === null || resultado[numeroPix] !== undefined) return;
            resultado[numeroPix] = contador;
        });
    });

    return resultado;
}
