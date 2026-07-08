só faça alterçoes no firebase com a minha autorização os dados lá sao de uso real nao podendo ser excluidos ou modificados sem o meu inteiro conhecimento

Antes de alterar qualquer lógica do sistema, verifique o fluxo completo e preserve os comportamentos que já funcionavam. Isso vale para Pix, estoque, fornecedor, posse, balanço, manutenção, atendimento, cadastro de cliente, colaboradores, financeiro, depósitos, rotas, permissões, relatórios e qualquer outra área já existente.

Regra geral contra retrabalho: ao mexer em uma função, tela ou regra, procure onde ela é lida, escrita, exibida e sincronizada. Antes de concluir, explique quais caminhos relacionados foram preservados e quais arquivos/funções foram conferidos.

Checklist obrigatório antes de alterar uma regra existente:

- Procurar chamadas, leituras e escritas relacionadas usando busca no projeto.
- Verificar telas que exibem o mesmo dado e telas que salvam/alteram esse dado.
- Preservar o comportamento antigo que não faz parte do problema informado.
- Não mudar nomes de campos, estruturas do Firebase ou chaves usadas em produção sem autorização.
- Se a alteração puder afetar outro fluxo, ajustar junto ou avisar antes de seguir.
- Quando possível, testar ou simular o caminho principal e pelo menos um caminho relacionado.

Exemplo de fluxo sensível: se mexer em Pix, conferir retirada do cliente, entrada no estoque, saída do estoque, posse do usuário e exibição no balanço. Pix retirado de cliente por um usuário deve continuar entrando em `pix_em_posse` desse usuário, e Pix entregue/devolvido ao estoque/fornecedor deve sair da posse do usuário correto.

Evite consertar um ponto quebrando outro. Se uma mudança puder impactar dados reais, pare e peça autorização antes de qualquer escrita no Firebase.
