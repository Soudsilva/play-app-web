# Project Instructions

- For labels, headings, button text, menu text, and other non-input UI text used for navigation, disable text selection with CSS such as `user-select: none;` and `-webkit-user-select: none;`.
- Do not apply the non-selectable rule to inputs, textareas, or content the user may reasonably need to copy.
- Unless the user explicitly asks for it, do not use transparent text styling. Avoid `opacity` on text and avoid alpha-based text colors such as `rgba(...)` or `hsla(...)`.
- Prefer solid, fully readable text colors for all default UI work.
- Assume the user's default timezone is Brasilia time (`America/Sao_Paulo`) for dates, times, schedules, and time-based calculations unless the user explicitly says otherwise.
- In `balanco.html`, products and machines represent the items currently in the user's possession, not a history of usage.
- For possession logic: stock withdrawals assigned to a user add to that user's balance, and atendimento or delivery actions subtract from that user's balance.
- If a future stock-output flow is created, include a field such as `retiradoPor` so the balance can identify who took the item.
- The primary users are aged 40–60. Use font sizes that are comfortable for this age group: body/paragraph text at minimum 15px, labels and secondary text at minimum 13px, important values and headings at 17px or above. Avoid thin font weights for body text — prefer `font-weight: normal` (400) or `bold` (700), never lighter than 400.
