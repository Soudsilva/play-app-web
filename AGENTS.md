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

Regra geral de estrutura, busca e escalabilidade:

- Qualquer novo nó, histórico, relatório ou consulta deve ser planejado para grandes volumes de dados. Evite ler uma raiz inteira, todos os meses ou todos os colaboradores para depois filtrar no navegador.
- Antes de definir a estrutura, identifique os filtros e caminhos de leitura reais da funcionalidade, como usuário, mês, status, equipamento ou rota, e organize os dados para permitir a leitura direta somente do recorte necessário.
- Quando necessário no Firebase Realtime Database, mantenha uma fonte principal de verdade e crie índices ou resumos auxiliares por usuário, período ou status. A duplicação controlada é permitida para tornar as consultas eficientes, desde que o caminho principal esteja documentado e a estrutura continue fácil de entender e manter.
- Dados principais e índices auxiliares relacionados devem ser atualizados de forma consistente, preferencialmente com atualização multipath atômica ou transação, evitando registros parciais ou índices divergentes.
- Use IDs estáveis para relacionamentos e chaves de busca. Nomes podem ser armazenados junto como informação de exibição, mas não devem ser o único vínculo quando puderem ser alterados.
- Listeners devem apontar para o menor caminho necessário e ser encerrados ou substituídos quando o usuário trocar o filtro, mês, usuário ou tela.
- Não replique dados pesados nos índices. Fotos, históricos completos e detalhes extensos devem permanecer no registro principal; índices devem carregar somente os campos necessários para listar, filtrar e localizar o registro.
- Centralize leituras, escritas, sincronizações e manutenção dos índices em funções claras no `database.js` ou na camada de dados correspondente. Evite espalhar caminhos Firebase e regras de sincronização por várias telas.
- Antes de concluir uma nova estrutura, confira se ela permanece legível, documentada e simples de manter, sem trocar eficiência por uma organização difícil de compreender.

Padrão obrigatório para filtros mensais em telas e estruturas novas:

- Toda nova tela que exiba dados organizados por mês e ano deve usar o seletor de competência com setas laterais no formato visual `‹  Mês de AAAA  ›`, seguindo o padrão existente nas telas de movimentação de estoque e produção de equipamentos.
- As setas devem navegar somente entre competências que realmente possuam informação. Meses vazios não devem ser exibidos nem percorridos.
- Ao criar um novo nó com dados mensais, prepare desde o início um campo ou índice auxiliar leve de competências ativas, organizado conforme os filtros reais da tela, por exemplo por usuário e `AAAA-MM`, armazenando somente o necessário para montar o filtro, como `true`.
- O nome e a posição desse campo auxiliar devem ser autoexplicativos e coerentes com a estrutura principal. Sempre que possível, mantenha-o dentro do nó funcional correspondente, sem criar uma raiz genérica ou desconectada.
- A gravação principal deve ativar a competência no índice junto com o primeiro registro do mês, preferencialmente na mesma atualização multipath atômica. Registros seguintes do mesmo mês não devem causar regravações desnecessárias do marcador.
- A tela deve ler primeiro apenas o pequeno índice de competências e, depois da seleção, consultar diretamente somente o recorte do usuário e mês escolhidos. Nunca leia todo o histórico apenas para descobrir quais meses existem.
- Ao trocar mês, ano, usuário ou colaborador, encerre os listeners anteriores antes de abrir os listeners do novo recorte.
- Se uma competência marcada como ativa não possuir mais registros válidos, remova-a do filtro ou marque-a como inativa, sem apagar ou modificar os dados principais sem autorização.
- Se ainda não houver competências ativas, exiba o mês atual com as duas setas desativadas, preservando uma interface estável sem inventar registros.

Padrão para saldos acumulados e resumos mensais:

- Quando uma funcionalidade transportar saldo entre competências, mantenha um resumo consolidado por mês. O cálculo do mês atual deve consultar somente seus próprios movimentos e o resumo do mês imediatamente anterior; nunca deve reler todo o histórico.
- O resumo anterior já deve conter o saldo recebido dos meses mais antigos. Esse saldo recebido permanece consolidado e não deve ser recalculado automaticamente quando um mês mais antigo for alterado.
- Exemplo: maio fecha com `50`; junho recebe `50`, registra `10` entradas e `5` saídas e fecha com `55`; julho consulta somente o resumo de junho e seus próprios movimentos. Uma correção nos movimentos de junho pode atualizar julho, mas uma alteração em maio não deve mais atingir julho.
- Na virada seguinte, agosto passa a consultar somente julho. Junho e os meses anteriores não participam da leitura nem do cálculo de agosto.
- Não reabra nem recalcule em cadeia os resumos de meses antigos sem uma regra explícita e autorização para essa correção. Preserve campos claros como saldo inicial transportado, entradas, saídas e saldo final.

Padrão geral de navegação e cópia nas telas:

- O aplicativo não deve criar botão próprio de voltar. A volta deve ser feita pelo botão nativo do celular/navegador.
- Quando uma tela precisar oferecer retorno para a página inicial, use somente o botão de Home no padrão visual já existente: ícone de casa dentro de botão circular no topo, como no modelo usado no app.
- O botão de Home deve ficar no lado direito do topo por padrão. Só use outra posição quando o proprietário do app solicitar explicitamente para uma tela específica.
- Não permita seleção/cópia de textos, números ou dados nas telas por padrão. Use bloqueio visual/comportamental de seleção (`user-select: none` ou equivalente) quando adequado.
- A cópia/seleção só deve ser permitida em campos, áreas ou telas explicitamente determinados pelo proprietário do app.

Padrão visual para rótulos de campos e controles:

- Rótulos exibidos acima de campos, seletores, botões de entrada ou filtros devem ficar centralizados horizontalmente em relação ao respectivo controle.
- O alinhamento deve considerar a largura do próprio campo ou grupo ao qual o rótulo pertence, e não a largura total da tela.
- Preserve esse padrão em formulários e filtros novos ou alterados, salvo quando o proprietário solicitar explicitamente outro alinhamento para uma tela ou campo específico.

Padrão visual para seleção de fotos:

- Sempre que a tela permitir escolher entre câmera e galeria, use como padrão um único botão de foto que abre um menu flutuante compacto com as opções “Câmera” e “Galeria”, seguindo o comportamento visual existente em `depositos.html`.
- Preserve inputs separados: a opção de câmera deve usar `accept="image/*"` com `capture="environment"`, enquanto a galeria deve usar `accept="image/*"` sem `capture`.
- O menu deve fechar depois da escolha, ao clicar fora e ao pressionar Escape. Quando houver uma foto selecionada, mantenha uma prévia clara e permita abrir o mesmo menu novamente para trocar a imagem.
- Evite exibir permanentemente dois botões grandes de câmera e galeria lado a lado quando o menu flutuante puder ser usado.

Padrão de composição das montagens de Máquina P e Máquina G:

| Montagem | Composição |
| --- | --- |
| P Simples | 1 P |
| G Simples | 1 G |
| Dupla P | 2 P |
| Rack | 4 P |
| Rack | 3 P + 1 G |
| Rack | 2 P + 2 G |

- “Dupla” existe somente para Máquina P.
- Sempre preserve no registro a composição exata do Rack para diferenciar `4 P`, `3 P + 1 G` e `2 P + 2 G`, mesmo que todas sejam exibidas com o nome “Rack”.
- Máquina P e Máquina G continuam sendo os componentes individuais e devem manter seus IDs estáveis, quantidades e valores unitários.

Exemplo de fluxo sensível: se mexer em Pix, conferir retirada do cliente, entrada no estoque, saída do estoque, posse do usuário e exibição no balanço. Pix retirado de cliente por um usuário deve continuar entrando em `pix_em_posse` desse usuário, e Pix entregue/devolvido ao estoque/fornecedor deve sair da posse do usuário correto.

Evite consertar um ponto quebrando outro. Se uma mudança puder impactar dados reais, pare e peça autorização antes de qualquer escrita no Firebase.

Regra obrigatória para testes com dados de usuários:

- Nunca crie depósitos, atendimentos, movimentações, clientes, saldos, históricos, fotos ou qualquer outro dado de teste em contas reais de colaboradores ou usuários.
- Testes devem usar preferencialmente mocks, simulações locais ou Firebase Emulator, sem escrever no Firebase de produção.
- A única conta de produção permitida para dados de teste é um usuário já existente cujo nome seja exatamente `Pedro`.
- Antes de qualquer teste no Firebase de produção, confirme por leitura que o usuário `Pedro` realmente existe e que o caminho de escrita pertence a ele.
- Se o usuário `Pedro` não existir, não crie esse usuário e não grave dados de teste em nenhuma outra conta.
- Para testar com qualquer usuário diferente de `Pedro`, ou para criar o próprio usuário `Pedro`, pare e peça autorização clara, específica e prévia do proprietário, informando o usuário, os caminhos e os dados que seriam gravados.
- Nunca use nomes de colaboradores reais como destino temporário de workflows, testes automatizados, testes de interface, exemplos, placeholders ou validações manuais.

Regra obrigatória de comunicação:

- Seja sempre direto e objetivo nas respostas, explicações e atualizações.
- Informe tudo que for realmente importante para a segurança, o resultado ou a decisão do proprietário, sem omitir riscos relevantes.
- Evite repetir informações, criar explicações longas ou acrescentar detalhes que não ajudem a resolver a solicitação.
- Quando houver um resultado numérico ou uma conclusão clara, apresente primeiro esse resultado de forma simples.

Regra obrigatória para enviar, publicar ou postar alterações:

- Quando o proprietário pedir para enviar, publicar, postar, subir ou usar expressão semelhante, entenda que ele deseja o fluxo completo de entrega das alterações.
- Antes de iniciar esse fluxo, confirme uma única vez com o proprietário. Depois da confirmação, execute sem pedir novas confirmações intermediárias, salvo se surgir risco, erro ou mudança de escopo relevante.
- O fluxo Git deve incluir: conferir `git status`, executar `git add -A`, criar um commit com mensagem coerente com a alteração e enviar para `origin/main`.
- Não considere apenas o deploy no Firebase como entrega completa: o código também deve ser registrado e enviado ao Git conforme o fluxo acima.
- Quando houver alterações dentro de `functions/`, inclua esses arquivos no commit e publique também as Firebase Functions afetadas, além do Hosting quando houver arquivos do site alterados.
- Antes do envio, confira a branch atual e garanta que o commit realmente chegará à `main`; não execute um `git push origin main` que deixe as alterações presas somente em outra branch.
- Depois de uma publicação concluída com sucesso, não verifique a versão online automaticamente. Faça essa verificação somente quando o proprietário solicitar explicitamente.
