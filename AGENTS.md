Instruções do Projeto
Para rótulos, títulos, texto de botões, texto de menus e outros textos de interface (UI) usados para navegação, desative a seleção de texto com CSS, como user-select: none; e -webkit-user-select: none;.
Não aplique a regra de não selecionável a inputs, textareas ou conteúdos que o usuário possa precisar copiar.
A menos que o usuário peça explicitamente, não use estilos de texto transparente. Evite opacity em textos e evite cores com alpha, como rgba(...) ou hsla(...).
Prefira cores sólidas e totalmente legíveis para todo o texto padrão da interface.
Considere que o fuso horário padrão do usuário é o de Brasília (America/Sao_Paulo) para datas, horários, agendas e cálculos baseados em tempo, a menos que o usuário diga o contrário.
Em balanco.html, produtos e máquinas representam os itens atualmente em posse do usuário, e não um histórico de uso.
Para a lógica de posse: retiradas de estoque atribuídas a um usuário aumentam o saldo desse usuário, e ações de atendimento ou entrega diminuem esse saldo.
Se for criado um fluxo futuro de saída de estoque, inclua um campo como retiradoPor para que o saldo consiga identificar quem retirou o item.
Os usuários principais têm entre 40 e 60 anos. Use tamanhos de fonte confortáveis para esse público: texto de corpo/parágrafo no mínimo 15px, rótulos e textos secundários no mínimo 13px, valores importantes e títulos com 17px ou mais. Evite fontes finas no texto principal — prefira font-weight: normal (400) ou bold (700), nunca menor que 400.
Quando um valor na interface for automático (não digitado pelo usuário), prefira exibi-lo em um “chip” com fundo transparente e borda branca.
Prefira texto centralizado para rótulos, títulos e textos explicativos curtos, a menos que o layout claramente peça alinhamento à esquerda.
Para telas padrão de aplicativos mobile, prefira usar a mesma largura de container utilizada em manutencoes_solicitadas.html: width: 100% com max-width: 440px, centralizado na página, a menos que a tela atual já tenha um padrão de layout diferente estabelecido.
Os botões são rótulos no formato de botão; tome cuidado para não usar um rótulo simples (apenas informativo) com aparência de botão.
Evite alterar funcionalidades já existentes que estão funcionando corretamente, para não gerar retrabalho e necessidade de revalidação de todo o sistema.
Priorize adicionar novas funcionalidades sem modificar a lógica atual, a menos que seja absolutamente necessário.
Antes de alterar qualquer lógica existente, verifique se ela impacta outras partes do sistema.
Mudanças em uma funcionalidade não devem quebrar comportamentos já implementados (ex: indicadores visuais como cores ou estados).
Caso seja realmente necessário alterar algo já existente, informe previamente o que será modificado, para que seja possível validar de forma assertiva sem precisar revisar todo o sistema ou descobrir problemas apenas durante o uso.
Sempre que houver alteração em lógica existente, preserve os comportamentos atuais ou replique-os na nova implementação.