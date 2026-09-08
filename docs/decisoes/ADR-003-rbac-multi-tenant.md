# ADR-003 — RBAC multi-tenant: fronteira entre o CR do operador e SQL

| Campo | Valor |
|---|---|
| Status | Aceita |
| Data | 2026-09-04 |
| Task | [E1](../epicos/E1-infraestrutura.md) T1.5 |

## Contexto

O requisito é isolamento por cliente **dentro de um único cluster** ClickHouse: database, role, user,
settings profile e quota por tenant. Há dois lugares onde isso pode ser declarado: o CR
`ClickHouseInstallation` do operador Altinity, ou SQL (`CREATE USER/ROLE/QUOTA/SETTINGS PROFILE`).

## As cinco restrições que decidem

1. **O storage `users.xml` é read-only para SQL.** Um usuário declarado no CR não pode receber `GRANT` —
   o comando falha com *"Cannot update user X in users.xml because this storage is readonly"*.
2. **`ROLE` não tem forma XML.** Quotas e settings profiles têm (`<quotas>`, `<profiles>`); role não. O
   requisito de role por tenant **obriga** o caminho SQL.
3. **Acesso criado por SQL é local à réplica** — vive em `/var/lib/clickhouse/access/` no PVC. Não é
   reconciliado pelo operador, não sobrevive a reprovisionamento de PVC, e não é aplicado a uma réplica
   nova. Na POC (1 réplica) é inofensivo.
4. **Profile declarado em `config.d` é silenciosamente ignorado.** O ClickHouse lê profiles de
   `users.xml`/`users.d`. O `chi-datamart.yaml` da V1 já acerta isso, mantendo `optimize_on_insert` sob
   `profiles:` e não sob `settings:`.
5. **`CREATE USER` exige um usuário com `access_management = 1`**, que só pode vir do XML. O caminho SQL
   puro não existe.

## Decisão

Híbrido, com a fronteira em "o que o operador consegue expressar sem conflito":

| Onde | O quê |
|---|---|
| CR (`chi-datamart.yaml`) | **Só** `dm_admin`, com `access_management: 1` — o bootstrap |
| SQL (Job idempotente) | `DATABASE`, `ROLE`, `USER`, `SETTINGS PROFILE`, `QUOTA` e `GRANT` por tenant |

**Regra dura: nenhuma identidade de tenant pode aparecer nos dois lugares.**

## Notas de precisão

- `readonly = 2`, não `1`. O valor `1` proíbe alterar settings e quebra clientes que emitem `SET`.
- `ALTER MOVE PARTITION` é o privilégio que cobre `REPLACE PARTITION`; `ALTER DELETE` é exigido no lado de
  origem do movimento.
- `join_use_nulls = 1` no profile do leitor **não é preferência de estilo**: sem ele, um `LEFT JOIN`
  preenche o lado ausente com o *default do tipo* em vez de `NULL`, e o painel mostra um número diferente
  do Postgres **sem erro nenhum**.

## Verificação

Um teste que só afirma o caminho feliz não detecta privilégio **a mais**. `scripts/verify-rbac.sh` prova
por asserções negativas: `INSERT` do usuário `_ro` falha com `Code 497`; `u_acme_ro` lendo `dm_globex`
falha com `Code 497`; `readonly` = 2; `join_use_nulls` = 1.

## Consequências

- Em produção, o item 3 (acesso local à réplica) vira um incidente real: uma réplica nova nasce sem os
  usuários. A pesquisa base (§10.4) já registra isso. A solução produtiva é um Job idempotente por réplica
  — e o relatório deve dizer isso em vez de fingir que o problema não existe.
- O modelo **não dá isolamento de recursos**. A pesquisa base (§10.1) classifica database-por-tenant no
  mesmo cluster exatamente assim, e recomenda instância dedicada. A demo de vizinho barulhento
  ([E3](../epicos/E3-validacao.md) T3.3) mostra o controle por quota — que é o mecanismo real, não
  isolamento forte. A apresentação não pode afirmar o segundo.
