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
padrão para o nó movimentação_balanço_historico ..atendimentoRefId: "-OqwfhY1DG8Lg3pTjlfc", categoria: "maquina", controlarPosse: true, descricao: "Serviço realizado", isDefeitoEntry: false, itemChave: "-OpcO1UEcy_qo_lZtH5y", itemNome: "Kid", itemNomeNormalizado: "kid", movimento: 2, origemRegistro: "manutencao", qtdDefeitoConsumida: 0, refId: "-OpcO1UEcy_qo_lZtH5y", registradoPor: "Joab", responsavel: "Joab", timestamp: "2026-04-23T23:31:11.773Z", tipo: "manutencao_retirada", totalAntes: 0, totalApos: 2
Não tente ficar carregando compatibilidade. Me informe sobre o formato novo e onde preciso modificar para que fique no formato novo. E se tiver que apagar algum dado também no Firebase para não conflitar, é só me informar onde fazer.

Credenciais para acessar e testar as telas localmente:
- Atendimento nível 1: usuário Joab, senha 0000A.
- Atendimento nível 2: usuário Felipe, senha 0000A.
- Gestor: usuário Anderson, senha 0000A.
Use essas credenciais apenas para validação/testes do sistema.
statusBalancoPendenteIndex === true E essa  variável vira true quando o resumo do balanço no Firebase vem assim:js contestacao_balanco/{usuario}/resumo_balanco/statusBalanco = "pendente"
o estilo das mensagens padrão são.. procure: play-dialogs.js. 