# POC V2 — Datamart OLAP multi-tenant

Índice da documentação de entrega. Este arquivo carrega o que é **comum a todos os épicos**: metadados,
glossário e mapa de leitura. Nenhum progresso é registrado aqui — progresso vive só em [TODO.md](TODO.md).

| Campo | Valor |
|---|---|
| Versão | 1.0 |
| Data | 2026-09-04 |
| Status | Especificação aprovada — implementação não iniciada |
| Branch | `feat/v2-olap` |
| Escopo | POC comparativa Postgres × ClickHouse como datamart de leitura, multi-tenant, sobre a stack Datawake |
| Fora de escopo | Migração produtiva, alteração do framework de dashboard, camadas acima do ClickHouse |
| Ambiente alvo | minikube local (WSL2), perfis `small` e `full` |
| Origem do dado | Parquet real, carregado pelo usuário no layout do honeycomb |
| Pesquisa base | `../../datamart-olap-leitura-pesquisa.md` |

---

## Sumário

1. [Como usar este conjunto de documentos](#1-como-usar-este-conjunto-de-documentos)
2. [Glossário](#2-glossário)
3. [Problema e premissas](#3-problema-e-premissas)
4. [Mapa de arquivos](#4-mapa-de-arquivos)
5. [Como o progresso é gerido](#5-como-o-progresso-é-gerido)

---

## 1. Como usar este conjunto de documentos

| Papel | Leia, nesta ordem |
|---|---|
| Quem vai implementar | [TODO.md](TODO.md) → o épico da vez → os ADRs que ele referencia |
| Quem vai revisar a arquitetura | [ARQUITETURA.md](ARQUITETURA.md) → [decisoes/](decisoes/) |
| Quem vai apresentar à gerência | [ARQUITETURA.md](ARQUITETURA.md) §"O que esta POC NÃO prova" → `benchmark/results/RESULTADO.md` |
| Quem só quer saber onde está | [TODO.md](TODO.md) |

> **O documento mais importante é [ARQUITETURA.md](ARQUITETURA.md), seção "Modelagem no destino".**
> A `ORDER BY` da tabela ClickHouse é a única decisão desta POC que não se corrige sem recriar a tabela e
> recarregar tudo. Errá-la entrega um ganho de 2–3× onde deveria haver 10–50×, e o erro não gera nenhuma
> mensagem — só um número decepcionante.

---

## 2. Glossário

Termos usados em todos os épicos. Cada épico expande a sigla na primeira ocorrência do seu próprio corpo.

| Termo | Significado |
|---|---|
| **ADR** | Architecture Decision Record — registro de decisão de arquitetura |
| **Braço** | Um dos dois destinos comparados (`pg` = PostgreSQL, `ch` = ClickHouse) |
| **BRIN** | Block Range Index — índice do Postgres que guarda min/max por faixa de blocos |
| **CDF** | Change Data Feed — recurso do Delta Lake que expõe as linhas alteradas |
| **CHI** | `ClickHouseInstallation` — CR do operador Altinity que descreve uma instância |
| **CR** | Custom Resource — recurso customizado do Kubernetes |
| **Dataset** | Objeto do Airflow que encadeia DAGs por dependência de dado, não por horário |
| **Delta Lake** | Formato de tabela transacional sobre Parquet |
| **DAG** | Directed Acyclic Graph — a unidade de pipeline do Airflow |
| **Filial** | Unidade de origem dentro de um tenant (`unidade_origem` na silver) |
| **honeycomb** | Pipeline Spark customizado da Datawake, imagem `hub.datawake.cloud/dw-dados/honeycomb` |
| **honeymaker** | API de control plane que provisiona tenants e gera os arquivos de DAG |
| **HOCON** | Human-Optimized Config Object Notation — formato de config usado no fork V1 |
| **JDBC** | Java Database Connectivity — interface pela qual o framework de dashboard consulta o datamart |
| **LocalExecutor** | Executor do Airflow em que as tasks rodam dentro do pod do scheduler |
| **LowCardinality** | Tipo do ClickHouse que dicionariza colunas com poucos valores distintos |
| **Medalhão** | Arquitetura em camadas bronze → silver → gold |
| **MergeTree** | Família de engines de tabela do ClickHouse |
| **MVCC** | Multi-Version Concurrency Control — controle de concorrência do Postgres, que mantém versões antigas das linhas |
| **OLAP** | Online Analytical Processing — carga de leitura analítica |
| **OLTP** | Online Transaction Processing — carga transacional |
| **p50 / p95** | Percentis 50 e 95 da latência — a mediana e a cauda |
| **PVC** | PersistentVolumeClaim — pedido de volume no Kubernetes |
| **RBAC** | Role-Based Access Control — controle de acesso por papel |
| **REPLACE PARTITION** | Comando do ClickHouse que troca o conteúdo de uma partição de forma atômica |
| **ReplacingMergeTree** | Engine do ClickHouse que deduplica por chave de ordenação durante o merge |
| **SA** | ServiceAccount — identidade de pod no Kubernetes |
| **Tenant** | Cliente/departamento; o nível em que a infraestrutura da Datawake é dividida |
| **testcontainers** | Biblioteca que sobe containers reais dentro de testes |
| **WiredTiger** | Engine de armazenamento do MongoDB |
| **Z-Order** | Técnica de coclusterização multidimensional do Delta Lake |

---

## 3. Problema e premissas

**Problema.** O datamart produtivo é um PostgreSQL relacional sem recurso colunar, consultado via JDBC por
um framework de dashboard que aplica filtros vindos de views mais agregações básicas. A degradação
observada é física — row-store, MVCC e amplificação de I/O — e não se resolve com tuning. A pesquisa base
recomendou ClickHouse como camada de entrega de leitura.

**O que conta como solução aceitável nesta POC.** Um número medido, no mesmo hardware, com dado real, que
sobreviva ao escrutínio técnico de quem for contestá-lo. Não uma promessa, não um benchmark de fornecedor.

### Premissas assumidas

1. O volume da gold produtiva é de 1 a 10 TB e cresce.
2. O framework aceita qualquer driver JDBC, mas gera SQL em dialeto Postgres.
3. Todo o processamento continua no Apache Spark; a POC não muda isso.
4. A divisão de infraestrutura acontece no nível de tenant.
5. O dado da POC é uma amostra real, não o volume produtivo — as conclusões são de **razão** entre motores,
   não de latência absoluta.
6. O usuário tem domínio de desenvolvimento até o ClickHouse. Acima dele, presta apoio.

### Fora de escopo

- Migração produtiva e plano de cutover.
- Alteração do framework de dashboard (a q05 do benchmark **mede** o custo de não alterá-lo).
- Camada semântica, cache de dashboard, BI tool.
- Alta disponibilidade, replicação e Keeper — a POC roda com 1 réplica.
- Segurança de borda, TLS, SSO.

---

## 4. Mapa de arquivos

| Arquivo | Conteúdo |
|---|---|
| [ARQUITETURA.md](ARQUITETURA.md) | Diagrama alvo, decisões transversais, modelagem no destino, o que a POC não prova |
| [TESTES.md](TESTES.md) | Registro de execução de teste por épico, defeitos abertos e o que não foi testado |
| [TUNING.md](TUNING.md) | Todo ajuste feito, o motivo de cada um e como conferir se está aplicado |
| [TODO.md](TODO.md) | Marcos, tasks e ações com estado — **fonte única do progresso** |
| [pesquisa-ingress-dns-wsl2.md](pesquisa-ingress-dns-wsl2.md) | Por que o addon `ingress-dns` não serve neste ambiente e qual resolução de nomes adotar — insumo da T1.8 |
| [epicos/E1-infraestrutura.md](epicos/E1-infraestrutura.md) | 7 tasks, uma por serviço |
| [epicos/E2-execucao.md](epicos/E2-execucao.md) | 6 tasks, pipelines e orquestração |
| [epicos/E3-validacao.md](epicos/E3-validacao.md) | 5 tasks, medição e evidência |
| [decisoes/ADR-001-resync-honeycomb.md](decisoes/ADR-001-resync-honeycomb.md) | Por que re-sincronizar o fork inteiro |
| [decisoes/ADR-002-modelagem-clickhouse.md](decisoes/ADR-002-modelagem-clickhouse.md) | `ORDER BY`, particionamento, tipos |
| [decisoes/ADR-003-rbac-multi-tenant.md](decisoes/ADR-003-rbac-multi-tenant.md) | Fronteira entre CR do operador e SQL |
| [decisoes/ADR-004-janela-de-carga.md](decisoes/ADR-004-janela-de-carga.md) | `REPLACE PARTITION` × `ReplacingMergeTree` |

### Herdado da V1

| Arquivo | Conteúdo |
|---|---|
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Problemas encontrados ao subir a stack e rodar a pipeline `datamart`, com sintoma, erro exato, causa raiz e correção. **Consulte antes de depurar do zero** — a maioria dos erros de build e submit já está catalogada. Os incidentes #5 (ivy com `HOME=/nonexistent`) e #11 (`Magic is not correct`, LZ4) continuam relevantes na V2 |

Referências fora de `docs/`:

| Caminho | Conteúdo |
|---|---|
| `.claude/specs/PLAN.md` | Especificação e plano de desenvolvimento da V1 |
| `README.md` (raiz) | Como subir e executar a POC |
| `images/spark/Dockerfile` | Imagem Spark da V1 (Python 3.12, jars assados em build-time). Substituída na V2 pelo `Dockerfile` do honeycomb — ver [E1](epicos/E1-infraestrutura.md) T1.3 |

---

## 5. Como o progresso é gerido

[TODO.md](TODO.md) é a **única** fonte de verdade do andamento. Épicos e ADRs descrevem o que fazer e por
quê; nenhum deles registra estado. Duas fontes de progresso viram duas verdades, e a que estiver
desatualizada é a que alguém vai ler.

Cada task tem um **critério de aceite executável** — um comando e a saída esperada. Uma task só fecha
quando o comando roda e a saída bate. Fechamento não é opinião.

---

## Manutenção

Estes documentos vivem em `datamart-poc/docs/` na branch `feat/v2-olap`. Incrementar a versão do
[README.md](README.md) quando: entrar ou sair um épico, mudar o ambiente alvo, ou uma decisão de ADR for
revertida. Correções de texto e avanço de tasks não mexem na versão.
