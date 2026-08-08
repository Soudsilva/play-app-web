import { perfilPodeGerenciarLocalizacao } from './localizacao-rules.js';

export function configurarAtalhoLocalizacao(atalho, perfil) {
    if (!atalho) return;
    const autorizado = perfilPodeGerenciarLocalizacao(perfil);
    atalho.setAttribute('role', autorizado ? 'button' : 'presentation');
    atalho.setAttribute('aria-label', autorizado ? 'Abrir localização' : '');
    atalho.title = autorizado ? 'Localização' : '';
    atalho.tabIndex = autorizado ? 0 : -1;
    atalho.onclick = autorizado
        ? (evento) => {
            evento.stopPropagation();
            window.location.href = 'localizacao.html';
        }
        : null;
    atalho.onkeydown = autorizado
        ? (evento) => {
            if (evento.key !== 'Enter' && evento.key !== ' ') return;
            evento.preventDefault();
            evento.stopPropagation();
            window.location.href = 'localizacao.html';
        }
        : null;
}
