export function textoNormalizadoPermissao(valor) {
    return String(valor || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/\s+/g, '_')
        .trim();
}

export function idFirebaseValido(valor) {
    const id = String(valor || '').trim();
    return !!id && !/[.#$\[\]\/]/.test(id);
}

export function perfilPodeAdministrarDadosSeguros(perfil) {
    const valor = textoNormalizadoPermissao(perfil);
    const perfilGestao = valor.includes('gestao') || valor.includes('gestor');
    return perfilGestao && !valor.includes('gestao_1');
}
