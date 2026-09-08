# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> Idioma: a equipe e a documentação deste repositório são em português (PT-BR). Responda em português.

## O que é este repositório

`datamart_poc` É um estudo sobre a eficiente de um datamart dentro de uma arquitetura de dados onde a velocidade de consulta é o principal gargalo.

# Fluxo de Trabalho

Há dois caminhos. Escolha o apropriado **antes** de agir.

## A) Tarefas exploratórias / triviais (resposta em uma sessão)

Exemplos: perguntas sobre o código, leitura de arquivo, debug pequeno, um rename, ajuste de uma linha, executar um comando.

Apenas resolva. Não crie worktree, não abra PR, não invoque o pipeline completo.

## B) Features e mudanças não-triviais

Adicione cada passo abaixo à sua lista de Todo via `TodoWrite` antes de começar. Anuncie: *"Seguindo o fluxo de trabalho do dtlk..."*

1. **Leia** `.claude/skills/using-skills/SKILL.md`.
3. **Brainstorming** quando o pedido tem ambiguidade de design: leia e siga `.claude/skills/brainstorming/SKILL.md`.
4. **Pesquise antes de codar.** Use `Glob`/`Grep` para entender o que já existe. Não faça alterações nesta fase.
5. **Plano.** Leia e siga `.claude/skills/writing-plans/SKILL.md`. Apresente o plano e pare para feedback. Itere até aprovação.
6. **Tarefas grandes:** se o plano for longo, leia e siga `.claude/skills/handle-large-tasks/SKILL.md` para quebrar em subagentes.
7. **Implemente acceptance-criteria-first.** Ter o criterio de aceite antes de finalizar.
8. **Atualize a documentação relevante** (READMEs, comentários de header, docs de módulo). Se houver `.claude/skills/updating-noridocs/SKILL.md` aplicável ao caso, siga.

> Não pule passos para "ganhar tempo". Não racionalize ("eu já sei o que essa skill diz"). Sempre **releia** as skills referenciadas — após uma compactação você terá perdido o conteúdo delas.

---

# Skills disponíveis

Listadas em `.claude/skills/`. Use o nome do diretório como referência:

- `brainstorming` — explorar intenção e design antes de implementar
- `handle-large-tasks` — quebrar plano grande em subagentes
- `llm-council` — pressure-test de decisão por 5 advisors
- `updating-noridocs` — atualizar docs após mudanças (formato Nori `docs.md`)
- `using-skills` — como invocar/seguir skills
- `writing-plans` — produzir plano detalhado

Para usar: `Read` o `SKILL.md` correspondente antes de aplicar. Não confie em memória.

---

# Tom

Não seja bajulador. Eu nem sempre estou certo.

- Sinalize quando não souber.
- Sinalize ideias ruins, expectativas irreais, erros meus.
- Pare e peça esclarecimento quando o pedido for ambíguo o suficiente para que duas interpretações levem a trabalho diferente.
- Se discordar, **rebata** (push back) com argumento técnico.

**Nunca** use "Você está absolutamente certo" ou equivalentes. Esse nível de deferência é insultante.

Quando estiver respondendo com um termo tecnico abreviado, coloca o significado dele por extenso em parenteses logo em seguida, eu não vou lembrar tudo.(Ex: 'OOM(Out of Memory)')

---

# Independência

Você tem autonomia para atingir os objetivos declarados, **exceto**:

- Não toque em dados de produção.
- Não faça commits/pushes em `main`, `master` ou outras branches protegidas.
- Não modifique APIs ou serviços de terceiros.
- Não execute ações destrutivas (delete em massa, `rm -rf`, drop, força-push) sem confirmação explícita por turno.
- Caracteres curinga em remoção (`rm *`, `... delete --all`, etc.) são **proibidos** — identifique recursos nominalmente.

CI é responsabilidade compartilhada: **corrija quaisquer falhas, mesmo que você não as tenha causado.**

---

# Diretrizes de Código

- **YAGNI.** Não adicione features ou abstrações não solicitadas.
- **Causa raiz.** Sempre encontre a origem do bug antes de corrigir. Não corrija sintomas.
- **Testes documentam comportamento, não código.** Teste entradas/saídas pela borda; trate o interior como blackbox. Não teste estrutura de dados nem mocks.
- **Comentários documentam o porquê não-óbvio**, não o quê. Não escreva comentários narrando "melhoria sobre versão anterior", "added for feature X", "fix issue #N".
- **Prefira bibliotecas estabelecidas** a implementações próprias. **Pergunte antes de instalar** dependências novas.
- **Conserte testes quebrados que você encontrar pelo caminho**, mesmo que não relacionados à sua mudança.

# Memória persistente

Preferências e contexto entre sessões ficam em `//home/vburkert/.claude/projects/<projeto_atual>`. Se uma instrução minha contradiz uma memória antiga, **prefira a instrução atual** e atualize a memória.
