---
name: Lógica do Balanço — Controle de Posse
description: O card de produtos e máquinas no balanço representa o que o usuário tem EM POSSE, não um histórico de uso
type: project
---

O balanço (balanco.html) deve funcionar como controle de posse individual por usuário:

- **Produtos:** quando o usuário retira do estoque (saída do estoque) → soma à posse. Quando abastece um cliente (atendimento com produtos) → subtrai da posse.
- **Máquinas:** quando uma máquina é atribuída ao usuário → soma. Quando entrega/instala → subtrai.

**Why:** o objetivo não é histórico de atendimentos, mas saber o que cada colaborador tem consigo agora.

**How to apply:** ao implementar a saída do estoque (futura tela), incluir o campo `retiradoPor` (nome do usuário) para identificar quem retirou. O balanço deve cruzar: entradas do estoque (soma) - saídas em atendimentos (subtrai) = saldo em posse. A estrutura já está preparada em balanco.html para receber essa lógica.
