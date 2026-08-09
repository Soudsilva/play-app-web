# Diretrizes obrigatórias do projeto Play App Web

## 1. Objetivos principais

Todo desenvolvimento deve buscar simultaneamente:

1. Boa experiência para o usuário:
   - Aplicativo rápido e responsivo.
   - Feedback visual imediato.
   - Sem travamentos da interface.
   - Sem carregamentos desnecessários.
   - Sem envios duplicados ou esperas sem explicação.

2. Facilidade de manutenção:
   - Código modular e com responsabilidades separadas.
   - Alterações localizadas, com baixo risco de quebrar outros fluxos.
   - Regras de negócio centralizadas e testáveis.
   - Sem continuar aumentando arquivos monolíticos.

3. Segurança dos dados:
   - Preservar dados reais de produção.
   - Evitar registros parciais, índices divergentes e duplicações.
   - Não mudar estruturas existentes sem autorização.
   - Manter compatibilidade com os fluxos já utilizados.

4. Escalabilidade:
   - Não depender da leitura de raízes completas.
   - Não reler históricos inteiros após cada operação.
   - Planejar consultas, índices, paginação e resumos para grandes volumes.

---

## 2. Proteção obrigatória do Firebase de produção

Os dados do Firebase são reais e estão em uso.

- Nunca criar, modificar, excluir, migrar ou reorganizar dados de produção sem autorização expressa do proprietário.
- Nunca alterar nomes de campos, caminhos, chaves, índices, regras do Firebase, gatilhos ou estruturas existentes sem autorização.
- Nunca publicar Firebase Hosting, Storage, Realtime Database Rules ou Cloud Functions sem autorização.
- Nunca executar scripts de correção, migração, limpeza ou backfill em produção sem informar previamente:
  - Caminhos afetados.
  - Dados que serão lidos.
  - Dados que serão gravados ou removidos.
  - Riscos.
  - Forma de recuperação.
- Alterações podem ser desenvolvidas e testadas localmente, mas não devem ser executadas contra produção sem autorização.
- Se existir risco de afetar dados reais, interromper antes da escrita e solicitar autorização.
- Nunca usar uma escrita real apenas para “ver se funciona”.

---

## 3. Preservação obrigatória do sistema existente

Antes de alterar qualquer lógica existente, verificar o fluxo completo e preservar os comportamentos que já funcionam.

Isso se aplica a:

- Pix.
- Estoque.
- Fornecedores.
- Posse de usuários.
- Balanço.
- Manutenções.
- Atendimentos.
- Clientes.
- Colaboradores.
- Financeiro.
- Depósitos.
- Rotas.
- Permissões.
- Relatórios.
- Produção.
- Equipamentos.
- Qualquer outra área existente.

Não fazer refatorações amplas ou modernizações não solicitadas apenas porque o código antigo está próximo da alteração.

Não aproveitar uma correção localizada para mudar contratos, nomes, estruturas ou comportamentos não relacionados.

---

## 4. Checklist obrigatório antes de alterar uma regra existente

Antes de modificar uma função, tela, gravação ou cálculo:

1. Pesquisar no projeto:
   - Onde a função é chamada.
   - Onde o dado é gravado.
   - Onde o dado é lido.
   - Onde o dado é exibido.
   - Onde ele é sincronizado.
   - Quais índices e resumos dependem dele.
   - Quais Cloud Functions ou listeners podem reagir à alteração.

2. Conferir:
   - Tela que origina a informação.
   - Tela que altera ou exclui a informação.
   - Telas que exibem o mesmo dado.
   - Relatórios e resumos relacionados.
   - Fluxos offline ou pendentes.
   - Permissões relacionadas.

3. Preservar:
   - Campos existentes.
   - Formatos existentes.
   - IDs e chaves estáveis.
   - Comportamentos que não fazem parte do problema.
   - Compatibilidade com registros antigos.

4. Avaliar efeitos:
   - Se a alteração puder afetar outro fluxo, ajustar o fluxo relacionado junto ou informar o proprietário antes de continuar.
   - Nunca corrigir um ponto criando inconsistência em outro.

5. Testar:
   - Caminho principal.
   - Pelo menos um caminho relacionado.
   - Falha de rede ou gravação.
   - Tentativa de envio duplicado, quando aplicável.
   - Compatibilidade com dados antigos, quando aplicável.

---

## 5. Arquitetura obrigatória para novas telas

Toda nova tela deve utilizar HTML5, CSS3 e JavaScript puro com ES Modules.

Estrutura padrão:

- `nome-da-tela.html`
  - Somente estrutura e elementos HTML.
  - Pode importar o CSS e o módulo JavaScript da tela.
  - Não deve conter regras de negócio ou grandes blocos de JavaScript/CSS inline.

- `nome-da-tela.css`
  - Somente apresentação visual.
  - Deve preservar o padrão visual e responsivo do aplicativo.

- `nome-da-tela.js`
  - Interface.
  - Eventos.
  - Estado visual.
  - Ciclo de vida da tela.
  - Coordenação entre interface, regras e serviços.
  - Não deve acessar diretamente caminhos do Firebase.

- `nome-da-tela-service.js`
  - Leituras e gravações específicas da funcionalidade.
  - Firebase Storage.
  - Integrações externas.
  - Manutenção dos índices pertencentes à funcionalidade.
  - Não deve manipular o DOM.

Quando houver regras de negócio relevantes:

- `nome-da-tela-rules.js`
  - Cálculos.
  - Validações.
  - Normalizações.
  - Transformações.
  - Regras puras, sem DOM e sem Firebase.
  - Deve ser testável isoladamente.

Quando houver processamento pesado:

- `nome-da-tela-worker.js`
  - Processamento de imagens, arquivos ou cálculos pesados fora da thread principal, quando necessário.

Arquivos opcionais devem ser criados somente quando existir responsabilidade real para eles.

---

## 6. Novas funcionalidades dentro de telas antigas

Uma funcionalidade nova dentro de uma tela antiga também deve nascer isolada.

- Não aumentar blocos monolíticos existentes se for possível criar um módulo específico.
- Não copiar funções do `database.js` para dentro da tela.
- Não duplicar regras de negócio existentes.
- Não espalhar caminhos Firebase por vários arquivos.
- A tela antiga pode importar o novo módulo, desde que o contrato seja pequeno e claro.
- Preservar o restante do comportamento da tela antiga.
- Não exigir a reescrita completa da tela para entregar uma melhoria localizada.

---

## 7. Limite do `database.js`

O `database.js` é uma camada legada e não deve continuar recebendo novas responsabilidades.

- Não adicionar novas funcionalidades independentes ao `database.js`.
- Não adicionar novos domínios, relatórios ou históricos ao `database.js`.
- Código novo deve usar seu próprio arquivo `-service.js`.
- Não importar o `database.js` apenas para obter o Firebase App.
- Não duplicar funções do `database.js` em novos serviços.
- Correções em funções já existentes no `database.js` são permitidas quando solicitadas, mas exigem o mapeamento completo dos fluxos relacionados.
- Não dividir ou refatorar o `database.js` inteiro automaticamente.

---

## 8. Inicialização central do Firebase

O projeto deve possuir apenas uma inicialização principal do Firebase App.

A infraestrutura mínima deve ser centralizada em módulo próprio, como:

- `firebase-app.js`
  - Configuração do Firebase.
  - Inicialização única do App.
  - Exportação de `app`, `db`, `storage` e `auth`, quando necessários.
  - Nenhuma regra de negócio.
  - Nenhuma leitura ou escrita automática.

Regras:

- Serviços novos devem importar somente as referências necessárias desse módulo central.
- Não inicializar um novo Firebase App em cada tela.
- Não importar a camada legada apenas para reutilizar o App.
- A extração da inicialização atualmente existente no `database.js` deve ser realizada como tarefa isolada.
- Durante essa extração, o `database.js` deve manter compatibilidade, inclusive reexportando referências antigas quando necessário.
- Essa mudança deve ser testada antes de qualquer publicação.

---

## 9. Integração entre código novo e legado

Isolamento não significa duplicação de regras.

- Funcionalidades de domínio novo não devem importar diretamente o `database.js`.
- Uma nova tela pode ler dados legados por caminhos específicos e em modo somente leitura.
- Antes de integrar com dados existentes, mapear:
  - Fonte principal de verdade.
  - Escritores existentes.
  - Leitores existentes.
  - Campos obrigatórios.
  - Índices.
  - Resumos.
  - Cloud Functions.
  - Efeitos relacionados.

Quando a funcionalidade nova precisar escrever em um fluxo existente:

- Deve existir somente uma implementação responsável por cada operação.
- Não reimplementar parcialmente uma escrita complexa.
- Não criar uma segunda fonte de verdade.
- Não criar um novo formato incompatível para os mesmos dados.
- Não gravar diretamente em nós legados sem autorização.
- Criar, quando aprovado, um adaptador ou serviço de domínio específico.
- O adaptador deve preservar todos os efeitos obrigatórios do fluxo antigo.
- Se não for possível garantir compatibilidade, parar e informar o risco antes de implementar.

Funções amplas como `dbSalvarAtendimento` não devem ser chamadas automaticamente por funcionalidades novas sem primeiro entender todos os seus efeitos.

---

## 10. Realtime Database e escalabilidade

Toda estrutura nova deve ser planejada para grandes volumes.

- Nunca executar `onValue()` ou `get()` em uma raiz completa para filtrar no navegador.
- Consultar somente o menor caminho necessário.
- Não carregar todos os usuários, meses, atendimentos ou históricos quando a tela utiliza apenas um recorte.
- Usar consultas como:
  - `query()`.
  - `orderByChild()`.
  - `equalTo()`.
  - `startAt()`.
  - `endAt()`.
  - `limitToFirst()`.
  - `limitToLast()`.

Toda listagem nova deve possuir:

- Paginação.
- Carregamento incremental.
- Limite explícito.
- Estado de carregamento.
- Estado vazio.
- Tratamento de erro.
- Cursor estável para impedir registros repetidos ou ausentes.

Consultas com ordenação ou filtro devem possuir o índice correspondente nas regras do Firebase.

Alterações em regras ou índices exigem autorização antes da publicação.

---

## 11. Fonte principal, índices e resumos

- Manter uma fonte principal de verdade para cada registro.
- Duplicação controlada é permitida somente para índices e resumos necessários ao desempenho.
- Índices devem conter apenas dados leves necessários para:
  - Listar.
  - Filtrar.
  - Ordenar.
  - Localizar o registro principal.
- Não duplicar fotos, históricos completos ou dados pesados em índices.
- Usar IDs estáveis nos relacionamentos.
- Nomes podem ser armazenados para exibição, mas não devem ser o único vínculo quando puderem mudar.
- Documentar qual é a fonte principal e quais são os índices auxiliares.
- Atualizar registro principal e índices relacionados de forma consistente.
- Preferir atualização multipath atômica ou transação.
- Evitar registros parciais e índices divergentes.
- Escritas repetidas devem ser idempotentes sempre que possível.

Se registros antigos precisarem entrar em um novo índice:

- Não ler toda a produção pelo navegador.
- Planejar migração ou backfill separado.
- Tornar o processo retomável e idempotente.
- Solicitar autorização antes de executar em produção.

---

## 12. Listeners e ciclo de vida

- Listeners devem apontar para o menor caminho possível.
- Guardar a função de cancelamento retornada pelo listener.
- Encerrar listeners ao:
  - Trocar mês.
  - Trocar usuário.
  - Trocar colaborador.
  - Trocar filtro.
  - Trocar página.
  - Fechar ou desmontar a tela.
- Não criar listeners duplicados para o mesmo conteúdo.
- Não manter listener global quando uma leitura única for suficiente.
- Não abrir o novo listener antes de encerrar o anterior quando ambos representam o mesmo filtro.
- Evitar atualizações repetidas do DOM quando os dados relevantes não mudaram.

---

## 13. Filtros mensais

Toda nova tela com dados organizados por mês e ano deve usar o seletor:

`‹  Mês de AAAA  ›`

Regras:

- Seguir o padrão das telas de movimentação de estoque e produção de equipamentos.
- Navegar somente por competências que realmente possuam informação.
- Não exibir ou percorrer meses vazios.
- Manter índice auxiliar leve de competências ativas conforme os filtros reais da tela.
- Preferir organização por usuário e `AAAA-MM`, quando esse for o filtro utilizado.
- Armazenar somente o marcador necessário, como `true`.
- Manter esse índice dentro do domínio funcional correspondente.
- Ativar a competência junto com a primeira gravação válida do mês.
- Preferir atualização multipath atômica.
- Não regravar o marcador desnecessariamente para cada novo registro.
- Ler primeiro o índice de competências.
- Depois da seleção, ler somente os dados daquele usuário e competência.
- Nunca ler todo o histórico para descobrir quais meses existem.
- Encerrar listeners anteriores durante a troca de competência.
- Se uma competência ativa não possuir mais registros válidos, removê-la do filtro ou marcá-la como inativa.
- Não excluir dados principais sem autorização.
- Se não houver competências, exibir o mês atual com as duas setas desativadas, sem criar registros artificiais.

---

## 14. Saldos acumulados e resumos mensais

Quando existir transporte de saldo:

- Manter resumo consolidado por competência.
- O mês atual deve consultar somente:
  - Seus próprios movimentos.
  - O resumo do mês imediatamente anterior.
- Nunca reler todo o histórico para calcular o saldo atual.
- O resumo anterior já deve conter o saldo recebido dos meses mais antigos.
- Não recalcular automaticamente toda a cadeia histórica.

Exemplo:

- Maio fecha com `50`.
- Junho recebe `50`, registra `10` de entrada e `5` de saída e fecha com `55`.
- Julho consulta somente o resumo de junho e seus próprios movimentos.
- Uma correção em junho pode atualizar julho.
- Uma alteração em maio não deve recalcular julho automaticamente.
- Na competência seguinte, agosto consulta somente julho.

Preservar campos claros como:

- Saldo inicial transportado.
- Entradas.
- Saídas.
- Saldo final.
- Data de fechamento ou atualização.
- Versão ou origem do cálculo, quando necessário.

Não reabrir ou recalcular competências antigas em cadeia sem regra explícita e autorização.

---

## 15. Cloud Functions e cálculos pesados

Cálculos complexos, agregações compartilhadas e manutenção de resumos podem ser executados em Cloud Functions quando isso trouxer consistência ou melhor desempenho.

Regras:

- Não usar Cloud Function como justificativa para ler raízes completas.
- Funções acionadas por eventos devem ser idempotentes.
- Tratar reexecuções do mesmo evento.
- Evitar loops de gravação que acionem novamente a própria função.
- Consultar somente os caminhos necessários.
- Não recalcular todo o histórico após uma gravação individual.
- Manter resumos incrementalmente sempre que possível.
- Registrar estados de processamento quando a operação for assíncrona.
- Alterações ou publicações de Functions exigem autorização.

---

## 16. Imagens e arquivos

- Nunca gravar imagens, arquivos, Blob ou Base64 no Realtime Database.
- Armazenar arquivos no Firebase Storage.
- Guardar no banco somente:
  - URL.
  - Caminho do Storage.
  - Tipo.
  - Tamanho.
  - Metadados mínimos necessários.
- Converter imagens para `Blob` ou `File` antes do upload.
- Redimensionar e comprimir imagens quando adequado, sem destruir a qualidade necessária ao negócio.
- Evitar processamento pesado na thread principal.
- Preferir:
  - `createImageBitmap`.
  - `OffscreenCanvas`.
  - Web Worker.
  - Fallback assíncrono leve quando o navegador não suportar essas APIs.
- Usar upload retomável quando for necessário exibir progresso ou permitir recuperação.
- **Feedback visual:** exibir imediatamente spinners ou barras de progresso claras para manter a interface responsiva durante os uploads.
- Exibir mensagens de erro claras em caso de falha.
- Não informar sucesso antes de arquivos e dados estarem consistentes.
- Impedir cliques e envios duplicados.
- Preservar os dados preenchidos se o upload falhar.
- Permitir nova tentativa sem duplicar registros.
- Tratar arquivos órfãos caso o upload termine e a gravação principal falhe.
- Não prometer que um upload continuará após fechar ou sair da página sem suporte técnico comprovado.
- Uploads independentes podem ser paralelos, mas devem utilizar concorrência limitada quando houver muitos arquivos.
- Não iniciar quantidade ilimitada de uploads com `Promise.all()`.

---

## 17. Consistência de operações com dados e arquivos

Firebase Storage e Realtime Database não participam da mesma transação.

Por isso, todo fluxo com dados e arquivos deve definir explicitamente:

- Ordem das operações.
- Estado pendente.
- Estado concluído.
- Tratamento de falha.
- Nova tentativa.
- Limpeza de arquivo órfão.
- Prevenção de duplicidade.

Não deixar um registro parecer concluído enquanto sua foto obrigatória ainda estiver ausente.

Não apagar automaticamente dados ou arquivos reais durante compensações sem autorização e sem confirmação do alvo exato.

---

## 18. Experiência do usuário e desempenho

Toda tela nova ou alterada deve:

- Responder visualmente imediatamente ao clique.
- Não bloquear a interface durante rede, upload ou processamento.
- Desabilitar apenas os controles necessários durante o envio.
- Impedir envio duplicado.
- Exibir mensagens claras e curtas.
- Evitar spinner sem prazo ou sem estado de erro.
- Preservar o formulário em caso de falha.
- Carregar imagens e conteúdo pesado somente quando necessário.
- Usar paginação ou carregamento incremental em listas.
- Atualizar somente os elementos que realmente mudaram.
- Evitar reconstruir listas completas após alteração de um único item.
- Usar miniaturas nas listagens e carregar imagem maior somente quando solicitada.
- Considerar conexão lenta e dispositivos móveis com pouca memória.
- Não esconder falhas de gravação do usuário.
- Não apresentar sucesso otimista quando houver risco de perda ou inconsistência de dados.

Antes de otimizar, identificar o gargalo real. Não adicionar caches, duplicações ou complexidade sem benefício demonstrável.

---

## 19. Testes obrigatórios

Código novo deve ser testável.

- Regras de negócio novas devem possuir testes unitários quando forem relevantes.
- Serviços Firebase devem ser testados preferencialmente com mocks ou Firebase Emulator.
- Testar:
  - Caminho principal.
  - Validação.
  - Falha de rede.
  - Falha de upload.
  - Envio duplicado.
  - Registro já existente.
  - Dados antigos ou campos opcionais.
  - Pelo menos um fluxo relacionado.
- Integrações com o legado devem validar o formato esperado dos dados.
- Não considerar uma alteração concluída apenas porque não houve erro no console.
- Não publicar código com testes relevantes falhando.
- Se não for possível executar um teste, informar claramente o que não foi validado.
- Arquivos e pastas de teste criados somente para uma validação pontual, sem função de regressão permanente, devem ser excluídos depois que o teste passar. Não aumentar a lista de exclusão da publicação apenas para manter testes temporários.

---

## 20. Testes com dados de usuários

- Nunca criar depósitos, atendimentos, movimentações, clientes, saldos, históricos, fotos ou outros dados de teste em contas reais.
- Usar preferencialmente mocks, simulações locais ou Firebase Emulator.
- A única conta de produção permitida para teste é um usuário existente cujo nome seja exatamente `Pedro`.
- Antes de qualquer teste em produção, confirmar por leitura:
  - Que `Pedro` existe.
  - Que o caminho pertence a ele.
  - Que os dados de teste não atingirão outro usuário.
- Se `Pedro` não existir, não criar esse usuário.
- Não usar outro usuário.
- Para testar com outro usuário ou criar `Pedro`, solicitar autorização específica e prévia.
- Informar previamente caminhos e dados que seriam gravados.
- Nunca usar nomes de colaboradores reais em:
  - Exemplos.
  - Placeholders.
  - Testes automatizados.
  - Testes de interface.
  - Destinos temporários.
  - Validações manuais.

---

## 21. Padrão de navegação e seleção de texto

- Não criar botão próprio de voltar.
- A volta deve utilizar o botão nativo do celular ou navegador.
- Quando houver retorno para a página inicial, usar apenas o botão Home no padrão existente.
- O botão Home deve:
  - Ter ícone de casa.
  - Ser circular.
  - Ficar no topo.
  - Ficar à direita por padrão.
- Outra posição somente mediante solicitação expressa.
- Não permitir seleção ou cópia de textos, números ou dados por padrão.
- Usar `user-select: none` ou equivalente quando adequado.
- Permitir cópia somente em campos ou áreas autorizadas pelo proprietário.

---

## 22. Rótulos de campos e controles

- Rótulos acima de campos, seletores, filtros ou controles devem ficar centralizados horizontalmente em relação ao próprio controle.
- O alinhamento deve considerar a largura do campo ou grupo, não a largura total da tela.
- Preservar esse padrão em telas novas ou alteradas.
- Exceções exigem solicitação explícita do proprietário.

---

## 23. Seleção de fotos

Quando a tela permitir câmera e galeria:

- Usar um único botão de foto.
- O botão deve abrir menu flutuante compacto com:
  - “Câmera”.
  - “Galeria”.
- Seguir o comportamento visual existente em `depositos.html`.
- Manter inputs separados:
  - Câmera: `accept="image/*"` e `capture="environment"`.
  - Galeria: `accept="image/*"` sem `capture`.
- Fechar o menu:
  - Depois da escolha.
  - Ao clicar fora.
  - Ao pressionar Escape.
- Exibir prévia clara da foto selecionada.
- Permitir abrir novamente o menu para trocar a imagem.
- Evitar dois botões grandes permanentes lado a lado.

---

## 24. Montagens de Máquina P e Máquina G

| Montagem | Composição |
| --- | --- |
| P Simples | 1 P |
| G Simples | 1 G |
| Dupla P | 2 P |
| Rack | 4 P |
| Rack | 3 P + 1 G |
| Rack | 2 P + 2 G |

Regras:

- “Dupla” existe somente para Máquina P.
- Preservar no registro a composição exata de cada Rack.
- Diferenciar:
  - `4 P`.
  - `3 P + 1 G`.
  - `2 P + 2 G`.
- Todas podem ser exibidas como “Rack”, mas não podem perder a composição.
- Máquina P e Máquina G continuam sendo componentes individuais.
- Preservar IDs estáveis, quantidades e valores unitários.

---

## 25. Fluxo sensível de Pix

Ao alterar qualquer regra relacionada a Pix, conferir obrigatoriamente:

- Retirada do cliente.
- Entrada no estoque.
- Saída do estoque.
- Entrega ao fornecedor.
- Devolução.
- Posse do usuário.
- Histórico.
- Balanço.
- Relatórios relacionados.

Pix retirado de cliente por um usuário deve continuar entrando em `pix_em_posse` do usuário correto.

Pix entregue ou devolvido ao estoque ou fornecedor deve sair da posse do usuário correto.

Não alterar apenas uma ponta desse fluxo.

---

## 26. Segurança e permissões

- Não confiar somente em validações visuais do cliente.
- Toda nova operação deve respeitar autenticação e permissões.
- Não conceder acesso mais amplo para facilitar o desenvolvimento.
- Não expor dados de outros usuários por consultas amplas.
- Não registrar tokens, senhas ou dados sensíveis no console.
- Mudanças em regras de segurança exigem autorização e validação específica.

### Estrutura genérica de permissões

- A instância `play-na-web-seguranca` é uma base genérica para identidades e permissões por `uid`. Ela não pertence a uma tela ou funcionalidade específica e não deve receber novamente nomes ou estruturas relacionadas à localização.
- `permissoes-service.js` é a camada de acesso a essa base. Centraliza consultas e atualizações dos nós `usuarios` e `permissoes`, incluindo registro de identidade, consulta de permissão e sincronização ou remoção do acesso de colaboradores.
- `permissoes-rules.js` contém funções puras usadas pelo aplicativo para normalizar e validar identificadores e interpretar quais perfis administrativos podem gerenciar dados seguros. Esse arquivo não acessa o Firebase e não substitui as regras de segurança da base.
- `database.seguranca.rules.json` é a proteção efetiva executada pelo Firebase. As verificações feitas na interface servem apenas para navegação e experiência visual; a autorização real de leitura e escrita deve continuar sendo garantida por esse arquivo.
- Novas telas protegidas devem reutilizar essa estrutura por `uid`. Não criar um novo arquivo de serviço ou uma nova base para cada tela sem uma necessidade técnica clara.
- Ao acrescentar uma nova permissão, conferir em conjunto onde ela é gravada, consultada, aplicada na navegação e validada em `database.seguranca.rules.json`, preservando as permissões já existentes.

### Matriz atual de botões do `index.html`

Esta matriz documenta somente quais botões são exibidos no `index.html`. As telas de destino podem possuir verificações internas adicionais.

#### Estoque 1 e Estoque 2

- Exibir: `Manutenção`, `Entr. Saídas` e `Pedidos`.
- Com `Prod. Produtos`: exibir `Financeiro`, direcionando para `producao_produtos.html`.
- Sem `Prod. Produtos`: exibir `New Financeiro`.
- Com `Prod. Produtos` e `Prod. Equipamentos`: exibir `Financeiro` e `New Financeiro`.
- Exibir `Impressão`, exceto quando `Prod. Produtos` for a única modalidade de produção ou serviço marcada.

#### Estoque 3

- Manter o perfil restrito, sem os botões comuns de manutenção, movimentação, pedidos, gestão e clientes.
- Com `Prod. Produtos`: exibir `Financeiro`, direcionando para `producao_produtos.html`.
- Sem `Prod. Produtos`: exibir `New Financeiro`.
- Com `Prod. Produtos` e `Prod. Equipamentos`: exibir os dois botões financeiros.
- Exibir `Impressão`, exceto quando `Prod. Produtos` for a única modalidade de produção ou serviço marcada.

#### Atendimento 1

- Exibir: `Atendimento`, `Lista de Clientes`, `Manutenção`, `Gestão`, `Balanço` e `WhatsApp Play`.
- Dentro de `Gestão`, exibir: `Verificar Envios`, `Máquinas em Estoque`, `Depósitos` e `Seleção de rotas`.
- Não exibir normalmente: `Pedidos`, `Área Pix`, `Fluxo de Caixa`, `Cad. Colaborador`, `Cadastro Itens`, `Entr. Saídas` e `Impressão`.

#### Atendimento 2

- Exibir: `Lista de Clientes`, `Manutenção`, `Gestão`, `Balanço`, `Pedidos`, `WhatsApp Play`, `Área Pix` e `Fluxo de Caixa`.
- Dentro de `Gestão`, exibir: `Verificar Envios`, `Saúde de Vendas` e `Máquinas em Estoque`.
- Não exibir o botão `Atendimento`.

#### Atendimento 3

- Exibir: `Atendimento`, `Lista de Clientes`, `Manutenção`, `Gestão`, `Balanço`, `WhatsApp Play` e `Área Pix`.
- Dentro de `Gestão`, exibir: `Verificar Envios`, `Máquinas em Estoque` e `Depósitos`.

#### Gestão 1

- Exibir: `Atendimento`, `Serviço`, `Lista de Clientes`, `Manutenção`, `Gestão`, `Balanço` para contestação e `WhatsApp Play`.
- Dentro de `Gestão`, exibir: `Verificações Gerais` e `Depósitos`.
- Não exibir normalmente: `Cad. Colaborador`, `Cadastro Itens`, `Pedidos`, `Área Pix`, `Fluxo de Caixa` e `Impressão`.

#### Gestão 2

- Exibir: `Atendimento`, `Serviço`, `Lista de Clientes`, `Manutenção`, `Gestão`, `Balanço`, `Cad. Colaborador`, `Cadastro Itens`, `Entr. Saídas`, `Pedidos`, `WhatsApp Play`, `Área Pix` e `Fluxo de Caixa`.
- Dentro de `Gestão`, exibir: `Verificações Gerais`, `Saúde de Vendas` e `Depósitos`.

#### Gestão 3

- Exibir: `Atendimento`, `Serviço`, `Lista de Clientes`, `Manutenção`, `Gestão`, `Balanço`, `Cad. Colaborador`, `Cadastro Itens`, `Entr. Saídas`, `Pedidos`, `WhatsApp Play`, `Área Pix`, `Financeiro`, `Produção Máquinas`, `New Financeiro` e `Impressão`.
- Dentro de `Gestão`, exibir: `Verificações Gerais`, `Saúde de Vendas`, `Máquinas em Estoque`, `Depósitos`, `Importar Dataverse`, `Verificar Rotas` e `Config. Automáticas`.
- Não exibir `Fluxo de Caixa`.

#### Exceções por remuneração ou cargo

- `Prod. Produtos`: acrescenta `Financeiro`.
- `Prod. Equipamentos`: acrescenta `New Financeiro`.
- `Comissão Global`: acrescenta `Balanço`.
- Representante com percentual: acrescenta `Representante`.
- Cargo `Sócio`: acrescenta `Contratos & Acordos`.

#### Confirmação obrigatória antes de mudar a matriz

- Antes de alterar a exibição, ocultação ou destino de qualquer botão do `index.html`, parar e pedir confirmação ao proprietário.
- A pergunta deve informar claramente: `o perfil X passará a ter acesso ao botão Y` ou `o perfil X perderá o acesso ao botão Y`.
- Quando remuneração, cargo ou nível também influenciarem a regra, informar a combinação completa afetada.
- Se mais de um perfil for afetado, listar todos antes de editar.
- Aguardar uma resposta explícita de `sim` ou `não`. Não alterar a matriz antes dessa resposta.
- Depois da confirmação, modificar somente os acessos apresentados e aprovados.

---

## 27. Comunicação durante o trabalho

- Ser direto e objetivo.
- Informar riscos relevantes.
- Não omitir impactos para simplificar a resposta.
- Não repetir informações sem necessidade.
- Apresentar primeiro resultados numéricos ou conclusões claras.
- Se uma decisão depender do proprietário, explicar:
  - O que precisa ser decidido.
  - O risco.
  - As opções reais.
- Não pedir autorização repetidamente para o mesmo fluxo já autorizado, salvo mudança de risco ou escopo.

---

## 28. Relatório obrigatório ao concluir alterações

Antes de considerar o trabalho concluído, informar:

- Arquivos alterados.
- Funções alteradas ou criadas.
- Caminhos Firebase apenas lidos.
- Caminhos Firebase que o código poderá gravar.
- Índices, resumos ou listeners envolvidos.
- Fluxo principal testado.
- Fluxo relacionado testado.
- Comportamentos antigos preservados.
- Testes que não puderam ser executados.
- Riscos ou pendências restantes.

Não afirmar que está tudo funcionando se algum caminho importante não foi validado.

---

## 29. Enviar, publicar, postar ou subir alterações

Quando o proprietário pedir para enviar, publicar, postar, subir ou expressão equivalente, entender como solicitação do fluxo completo de entrega.

Antes de iniciar:

- Confirmar uma única vez com o proprietário.

Depois da confirmação:

1. Conferir `git status`.
2. Conferir alterações e arquivos não relacionados.
3. Verificar a branch atual.
4. Garantir que o commit chegará à `main`.
5. Executar `git add -A`.
6. Criar commit com mensagem coerente.
7. Enviar para `origin/main`.

Regras:

- Não executar um push que deixe o trabalho preso apenas em outra branch.
- Não incluir arquivos sensíveis ou temporários.
- Se houver mudanças no site, publicar o Firebase Hosting quando autorizado.
- Se houver alterações em `functions/`, incluir os arquivos no commit e publicar as Functions afetadas quando autorizado.
- Deploy não substitui commit e push.
- Commit e push não substituem deploy quando o proprietário pediu publicação completa.
- Depois de publicar, não verificar automaticamente a versão online.
- Verificar a versão online somente quando o proprietário solicitar.
- Se surgir erro, risco ou mudança relevante de escopo, interromper e informar antes de continuar.

---

## 30. Princípio final

O projeto está em produção e deve evoluir gradualmente.

- Código novo deve nascer organizado.
- Código antigo deve ser preservado enquanto estiver funcionando.
- Correções no legado devem ser pequenas, mapeadas e testadas.
- Não criar novas dependências desnecessárias do legado.
- Não duplicar regras para tentar fugir do legado.
- Não trocar estabilidade por uma refatoração ampla.
- Não trocar desempenho por uma estrutura difícil de compreender.
- Toda alteração deve reduzir ou, no mínimo, não aumentar a dívida técnica.
- Se uma solução rápida aumentar acoplamento, leitura excessiva, duplicação ou risco de dados, parar e propor uma solução segura antes de implementar.
