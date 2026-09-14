# Resultado da POC V2 — ClickHouse × PostgreSQL no datamart CEP

| Campo | Valor |
|---|---|
| Versão | 1.0 |
| Data | 2026-09-14 |
| Status | Final para a POC. A seção 7 está fora de escopo por decisão do usuário |
| Pergunta | Quanto um datamart ClickHouse ganha sobre o datamart Postgres atual, lendo o mesmo dado pelas consultas do painel CEP |
| Ambiente | minikube no WSL2, 4 vCPU e 8 GiB; os dois bancos com 3 CPU e 3 GiB |
| Dado | `fact_200_cep` do tenant `acme`, 7 114 401 linhas, 2026-07 a 2026-09, 5 filiais |
| Evidência | [TESTES §6.3](../../docs/TESTES.md#63-t31--portão-de-corretude) (corretude), [TESTES §6.5](../../docs/TESTES.md#65-t32--suíte-de-leitura) (leitura) |

---

## Sumário

0. [Como ler](#como-ler)
1. [Ambiente](#1-ambiente)
2. [Corretude](#2-corretude)
3. [Latência](#3-latência)
4. [I/O — o que explica a latência](#4-io--o-que-explica-a-latência)
5. [Compressão em disco](#5-compressão-em-disco)
6. [q05: o `SELECT *` do framework](#6-q05-o-select--do-framework)
7. [Vizinho barulhento — fora de escopo](#7-vizinho-barulhento--fora-de-escopo)
8. [Ressalvas](#8-ressalvas)
9. [Resumo executivo](#9-resumo-executivo)
10. [Anexo — como reproduzir](#10-anexo--como-reproduzir)
11. [Referências](#11-referências)

---

## Como ler

| Papel | O que ler |
|---|---|
| Gerência | [§9 Resumo executivo](#9-resumo-executivo), depois §3 e §8 |
| DBA ou quem vai questionar o número | §2 antes de tudo, depois §4 e §8 |
| Quem vai decidir o framework do painel | §6 |
| Quem vai reproduzir | §1 e §10 |

A ordem das seções é a argumentação: primeiro o ambiente, depois a prova de que os dois bancos têm o
mesmo dado, e só então o número. **O número central é a §3; a §2 é o que dá o direito de mostrá-lo.**

| Termo | Significado |
|---|---|
| `pg` | Postgres como o datamart é hoje: tabela única, índice do painel `(filial, banco, unidade, timestamp)` |
| `pgt` | Postgres ajustado: particionado por mês, índice do painel por partição e BRIN (índice de faixas de blocos) em `timestamp` |
| `ch` | ClickHouse: `MergeTree` particionado por mês, ordenado por `(filial, banco, unidade, timestamp)` |
| p50 / p95 | Mediana / percentil 95 da latência: metade das execuções fica abaixo do p50, 95 % abaixo do p95 |
| Fator | Razão entre latências: `p95 pg / p95 ch`. "30×" quer dizer 30 vezes mais lento que o ClickHouse |
| Quente / frio | Com os caches do motor carregados / derrubados antes de cada execução |
| `-c 1` / `-c 8` | Uma consulta por vez / oito clientes simultâneos |

---

## 1. Ambiente

| Item | Valor |
|---|---|
| Nó | minikube v1.36.0, Kubernetes v1.33.1, driver docker no WSL2; `--cpus 4 --memory 8g` (perfil `small`) |
| PostgreSQL | 16.15, limite **3 CPU / 3 GiB**, `shared_buffers` 640MB, `effective_cache_size` 1792MB, `work_mem` 24MB, `random_page_cost` 1.1, JIT ligado, até 2 workers paralelos |
| ClickHouse | 24.8.14.39, limite **3 CPU / 3 GiB**, `max_server_memory_usage` 2 GiB, 1 réplica |
| Linhas | 7 114 401 em cada braço (`pg`, `pgt`, `ch`) |
| Tamanho em disco | `pg` 3,92 GB · `pgt` 2,62 GB · `ch` 0,55 GB — detalhe na §5 |
| Carga | Mesma query Spark sobre a mesma silver, na mesma janela mensal; ClickHouse por `REPLACE PARTITION` ([ADR-004](../../docs/decisoes/ADR-004-janela-de-carga.md)) |
| Clientes | Pods próprios (`bench-cliente-pg`, `bench-cliente-ch`), 2 CPU cada; `psql` 16 e `clickhouse-client` 24.8 |
| Leitura no ClickHouse | Usuário `u_acme_bench`: só `SELECT`, sem cota nem limites — simétrico ao Postgres, que não tem limites |
| Cluster durante a medição | Airflow e MongoDB desligados (`profile.sh quiesce`) |

Os dois bancos têm o **mesmo limite de CPU e memória**. O Postgres começou a POC com 2 CPU e 1,75 GiB; com
esse limite a tabela não cabia no cache e a q01 levava 16,5 s. Igualar foi decisão do usuário, para a
comparação ser "mesmo hardware".

---

## 2. Corretude

`bash benchmark/compare-counts.sh` — `count(*)` e `sum(valor)` por filial e mês, nos três braços.
Resultado da última execução, antes da medição final:

| Mês | Filial | `count(*)` pg | pgt | ch | divergência | `sum(valor)` pg = pgt = ch | divergência |
|---|---|---:|---:|---:|---:|---:|---:|
| 202607 | LIMEIRA | 1 680 734 | 1 680 734 | 1 680 734 | 0 | 799 524 195 | 0 |
| 202607 | MARACANAU | 365 170 | 365 170 | 365 170 | 0 | 256 850 778 | 0 |
| 202607 | PAULINIA | 388 920 | 388 920 | 388 920 | 0 | 303 209 526 | 0 |
| 202607 | POMPEIA | 21 422 | 21 422 | 21 422 | 0 | 55 025 634 | 0 |
| 202607 | UBERABA | 140 809 | 140 809 | 140 809 | 0 | 151 383 172 | 0 |
| 202608 | LIMEIRA | 2 447 664 | 2 447 664 | 2 447 664 | 0 | 980 475 565 | 0 |
| 202608 | MARACANAU | 365 831 | 365 831 | 365 831 | 0 | 200 058 471 | 0 |
| 202608 | PAULINIA | 503 271 | 503 271 | 503 271 | 0 | 382 664 700 | 0 |
| 202608 | POMPEIA | 12 790 | 12 790 | 12 790 | 0 | 41 876 145 | 0 |
| 202608 | UBERABA | 148 108 | 148 108 | 148 108 | 0 | 157 240 475 | 0 |
| 202609 | LIMEIRA | 710 774 | 710 774 | 710 774 | 0 | 285 638 509 | 0 |
| 202609 | MARACANAU | 68 632 | 68 632 | 68 632 | 0 | 38 847 923 | 0 |
| 202609 | PAULINIA | 212 264 | 212 264 | 212 264 | 0 | 137 298 502 | 0 |
| 202609 | POMPEIA | 7 463 | 7 463 | 7 463 | 0 | 20 162 314 | 0 |
| 202609 | UBERABA | 40 549 | 40 549 | 40 549 | 0 | 43 265 346 | 0 |
| **Total** | | **7 114 401** | **7 114 401** | **7 114 401** | **0** | **3 853 521 255** | **0** |

Além disso, antes de cada medição, as 5 consultas do benchmark rodaram nos três braços e as saídas foram
comparadas linha a linha (`read-bench.sh --paridade`), inclusive as 494 576 linhas da q05. **Os três
braços devolvem o mesmo resultado em todas as consultas.** O que se mede adiante é o motor, não um
trabalho diferente.

---

## 3. Latência

As 5 consultas imitam o painel CEP (Controle Estatístico de Processo). Todas filtram a maior filial,
LIMEIRA:

| Consulta | O que o painel faz | Recorte |
|---|---|---|
| q01 CEP por unidade | n, média, desvio, mín., máx. e % fora dos limites por unidade e atributo | 1 mês |
| q02 série diária | Média, mín. e máx. por dia e atributo | 3 meses |
| q03 top produtos fora do limite | Top 20 produtos, **todas as filiais** | 3 meses |
| **q04 painel pela view** | Colunas nomeadas de uma view, filtro, agregação, `LIMIT` — como o framework do painel | 1 semana |
| q05 `SELECT *` da view | Mesmo filtro da q04, linhas cruas (494 576) | 1 semana |

### Resultado — quente, uma consulta por vez

Duas rodadas × 30 execuções por consulta e braço. Milissegundos. Entre colchetes, a faixa do p95 entre as
duas rodadas.

| Consulta | p50 pg | p95 pg | p50 pgt | p95 pgt | p50 ch | p95 ch | **fator p95 pg/ch** | **fator p95 pgt/ch** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q01 | 2 398 | 2 608 [2 608–2 747] | 3 502 | 3 720 [3 690–3 913] | 76 | 92 [83–98] | **28×** | 40× |
| q02 | 3 710 | 4 289 [4 289–4 306] | 4 111 | 5 046 [4 824–5 211] | 129 | 140 [139–146] | **31×** | 36× |
| q03 | 2 186 | 2 413 [2 253–2 471] | 2 355 | 2 561 [2 404–2 702] | 98 | 120 [118–123] | **20×** | 21× |
| **q04** | 2 060 | 2 577 [2 184–2 645] | 357 | 399 [364–417] | **31** | **47** [38–60] | **55×** | **8×** |
| q05 | 2 560 | 2 711 [2 711–2 850] | 926 | 994 [994–1 005] | 63 | 89 [89–92] | **30×** | 11× |

**O ClickHouse responde de 20 a 55 vezes mais rápido que o Postgres de hoje, e de 8 a 40 vezes mais rápido
que o Postgres ajustado.** Na consulta central (q04), o painel sai de 2,6 s para 47 ms no p95.

O Postgres ajustado ganha onde o recorte é estreito (q04 e q05: de 2,6 s para 0,4 e 1,0 s), graças à
partição e à ordem física por data. Nas agregações de mês inteiro (q01 a q03) ele não ganha nada, e às
vezes perde: o recorte é grande demais para qualquer índice, e os dois Postgres acabam varrendo o dado.

Estabilidade: o p50 variou no máximo 17 % entre as rodadas, dentro do aceite da T3.2 (menos de 30 %). O
p95 também ficou abaixo de 30 % em 14 das 15 combinações; a exceção é o próprio ClickHouse na q04, com
38 contra 60 ms.

### Barras de p95 (ms), escala por consulta

```
q01 pg  ████████████████████████████               2 608 ms
q01 pgt ████████████████████████████████████████   3 720 ms
q01 ch  █                                             92 ms

q02 pg  ██████████████████████████████████         4 289 ms
q02 pgt ████████████████████████████████████████   5 046 ms
q02 ch  █                                            140 ms

q03 pg  ██████████████████████████████████████     2 413 ms
q03 pgt ████████████████████████████████████████   2 561 ms
q03 ch  ██                                           120 ms

q04 pg  ████████████████████████████████████████   2 577 ms
q04 pgt ██████                                       399 ms
q04 ch  █                                             47 ms

q05 pg  ████████████████████████████████████████   2 711 ms
q05 pgt ███████████████                              994 ms
q05 ch  █                                             89 ms
```

### Indicativos: 8 clientes simultâneos e cache frio

| Consulta | `-c 8` p95 pg / pgt / ch | fator pg/ch | Frio p95 pg / pgt / ch | fator pg/ch |
|---|---:|---:|---:|---:|
| q01 | 23 587 / 25 207 / 1 851 | 13× | 2 690 / 6 203 / 129 | 21× |
| q02 | 40 671 / 66 804 / 2 854 | 14× | 9 319 / 20 271 / 141 | 66× |
| q03 | 22 701 / 25 481 / 2 526 | 9× | 7 676 / 4 370 / 163 | 47× |
| q04 | 13 971 / 4 606 / 536 | 26× | 10 346 / 2 133 / 103 | 100× |
| q05 | 17 808 / 10 420 / 2 047 | 9× | 3 098 / 1 043 / 112 | 28× |

São indicativos, não resultado: com 8 clientes o nó de 4 vCPU vira o gargalo, e o cliente do Postgres
ficou estrangulado pelo limite de CPU em 26 % do tempo, o que prejudica o Postgres; o frio tem uma rodada
de 3 execuções. Nos dois casos o ClickHouse segue uma ordem de grandeza à frente: com 8 clientes, o p95 do
painel (q04) fica em 0,5 s contra 14 s do Postgres de hoje.

---

## 4. I/O — o que explica a latência

Uma execução instrumentada por consulta e braço. Postgres por `EXPLAIN (ANALYZE, BUFFERS)`: bytes =
blocos acessados × 8 KB. ClickHouse pelos eventos de leitura do cliente.

| Consulta | pg: linhas / bytes | pgt: linhas / bytes | ch: linhas / bytes | bytes pg ÷ ch |
|---|---|---|---|---:|
| q01 | 7,1 M / 2,51 GB | 3,5 M / 1,08 GB | 2,45 M / 81 MB | 31× |
| q02 | 7,1 M / 2,51 GB | 6,2 M / 2,30 GB | 4,8 M / 73 MB | 34× |
| q03 | 7,1 M / 2,51 GB | 7,1 M / 2,20 GB | 7,1 M / 221 MB | 11× |
| q04 | 0,49 M / **4,38 GB** | 0,78 M / 0,24 GB | 0,64 M / 16 MB | **277×** |
| q05 | 0,49 M / 4,38 GB | 0,78 M / 0,24 GB | 0,64 M / 70 MB | 62× |

Três mecanismos explicam a §3:

1. **Varredura por coluna.** Nas agregações (q01 a q03) o Postgres lê a linha inteira, com as 24 colunas,
   de toda a tabela: 2,5 GB. O ClickHouse lê só as 4 a 6 colunas que a consulta usa, comprimidas. Mesmo
   lendo as mesmas 7,1 M linhas (q03), lê 11 vezes menos bytes.
2. **Localidade física.** Na q04 o índice do painel encontra as 494 mil linhas, mas cada uma está numa
   página diferente da tabela: 534 mil blocos, cerca de um por linha, 4,4 GB de acesso. O ClickHouse
   guarda as linhas **ordenadas** pela chave do painel, então a mesma semana de uma filial é um trecho
   contíguo do disco: 16 MB. O `pgt` recupera parte disso com a partição e a ordem por data (240 MB).
3. **Poda por índice esparso.** O ClickHouse descarta blocos de 8 192 linhas pela chave de ordenação e
   pela partição antes de ler. O Postgres só poda por partição no `pgt`.

O tempo do `EXPLAIN ANALYZE` não foi usado: ele cronometra cada nó do plano e infla a duração (q04 `pg`:
20 s instrumentado, 2,1 s real).

---

## 5. Compressão em disco

| Braço | Tabela | Índices | **Total** | Linhas |
|---|---:|---:|---:|---:|
| `pg` | 2 506 MB | 1 410 MB (chave única 929 + painel 482) | **3 917 MB** | 7 114 401 |
| `pgt` | 2 199 MB | 421 MB | **2 620 MB** | 7 114 401 |
| `ch` | 545 MB, com índice esparso de 16 KB em memória | — | **545 MB** | 7 114 401 |

**O ClickHouse ocupa 7,2 vezes menos disco que o Postgres de hoje, e 4,8 vezes menos que o ajustado.**

O número esconde um achado: **85 % do disco do ClickHouse (461 MB) é uma única coluna, `hk_business_id`**,
um hash SHA-256 em texto que não comprime nada (razão 1,0). Nenhuma consulta do painel a lê: ela existe
para a carga deduplicar. Sem ela, o ClickHouse guarda 1 059 MB de dado em **81 MB — compressão de 13
vezes**. No Postgres a mesma chave custa 929 MB de índice único, necessário ao `ON CONFLICT` da carga.

A tabela `pg` inclui cerca de 318 MB de espaço livre deixado por uma recarga medida na preparação (a
versão antiga de cada linha, liberada pelo `VACUUM` mas não devolvida ao disco). O `pgt` foi montado
limpo.

---

## 6. q05: o `SELECT *` do framework

A especificação previa que o `SELECT *` derrubasse o ganho de 10–50× para 2–5×, porque o tempo passaria a
ser de transferir linhas, e não de calcular. **Com um cliente por vez, isso não aconteceu:**

| | q04 (colunas nomeadas, agregado) | q05 (`SELECT *`, 494 576 linhas) |
|---|---:|---:|
| fator p95 pg/ch | 55× | **30×** |
| fator p95 pgt/ch | 8× | **11×** |
| p95 ch | 47 ms | 89 ms |

O ClickHouse transfere as 494 mil linhas em cerca de 60 ms: o protocolo nativo é colunar e comprimido, e
o cliente não converte linha a linha para texto. O ganho cai contra o `pg` (de 55× para 30×), mas continua
em uma ordem de grandeza.

**Com 8 clientes, a previsão se confirma em parte:** o fator da q05 cai para 9× contra o `pg` e **5×
contra o `pgt`**, o menor de toda a medição. Sob concorrência, transferir linhas cruas custa CPU nos dois
lados e o ganho encolhe (indicativo; ver §3).

**E há um bloqueio de produção.** O perfil de leitura que a POC desenhou para cada tenant (T1.5) limita
o resultado a 200 mil linhas, o tempo a 30 s e a memória a 700 MB, com cota por minuto. Uma tela que
traga uma semana de uma filial em linhas cruas **falha** com esse perfil: foi o que o benchmark encontrou
antes de passar a usar um leitor sem limites. A decisão sobre o framework do painel fica assim:

| Se o painel… | Então |
|---|---|
| Agrega no banco, com colunas nomeadas (q04) | Ganho de 55×; nenhum limite é atingido |
| Traz linhas cruas e agrega no cliente (q05) | Ganho menor (30×, e 5 a 9× sob concorrência) e esbarra no `max_result_rows` do perfil de leitura |

---

## 7. Vizinho barulhento — fora de escopo

**Esta seção não foi medida.** A T3.3 foi retirada do escopo em 2026-09-14, por decisão do usuário: o
objetivo da POC é comparar a performance entre os bancos, e a concorrência entre tenants fica de fora.

O que existe e está verificado:

- **Isolamento de acesso entre tenants:** `verify-rbac.sh` 4 de 4. O leitor de cada tenant lê só o próprio
  database e recebe `ACCESS_DENIED (497)` no outro, testado nos dois sentidos com o dado real.
- **Mecanismo de controle declarado:** cada tenant tem uma cota por minuto (120 consultas, 500 M de linhas
  lidas, 60 s de execução) e um perfil com limites de memória, tempo e resultado.

O que **não** se sabe: se esse mecanismo protege a latência de um tenant quando outro satura o servidor.
Database por tenant, no mesmo servidor, **não isola recursos**. A hipótese levantada antes da decisão, e
não medida, é que a cota **não bastaria**: ela limita volume por minuto, não concorrência, e um vizinho
com consultas pesadas em paralelo gastaria a cota em segundos, pesando com força total nesse trecho de
cada minuto. Proteger latência exigiria limitar CPU e concorrência por tenant (`max_threads`,
`max_concurrent_queries_for_user`) ou instância dedicada. O desenho do teste, se voltar ao escopo, está
no [E3](../../docs/epicos/E3-validacao.md#t33--vizinho-barulhento).

---

## 8. Ressalvas

Declarar a limitação vale mais que o número. Todas se aplicam a este resultado.

| Ressalva | Consequência |
|---|---|
| **Volume da POC não é o de produção:** 7,1 M linhas, 3 meses, 1 tenant medido | As conclusões são de **razão** entre os motores, não de latência absoluta |
| **minikube não é produção:** 1 nó de 4 vCPU no WSL2, 1 réplica, sem Keeper, sem replicação | Tempos absolutos não se transferem; custo de réplica não aparece |
| **Cache do sistema operacional não derrubado** | "Frio" significa caches do motor vazios, não disco frio. O frio teve uma rodada só |
| **O Postgres não cabe no próprio cache:** 3,9 GB (`pg`) e 2,6 GB (`pgt`) num pod de 3 GiB | Com memória para o conjunto inteiro, o Postgres melhoraria e o fator diminuiria. O ClickHouse cabe em 0,55 GB porque comprime; parte do ganho é exatamente essa |
| **Método de carga:** a POC lê `MergeTree` carregado por troca de partição. Produção vai usar `ReplacingMergeTree` por watermark (decisão do [ADR-004](../../docs/decisoes/ADR-004-janela-de-carga.md)) | No mês corrente, o ClickHouse de produção pagará o `FINAL`, medido em 3,5 a 7 vezes no painel. Estimativa, não medida: o fator da q04 no mês corrente cairia de 55× para algo entre 8× e 16× |
| **8 clientes simultâneos é indicativo** | O nó de 4 vCPU é o gargalo e o cliente do Postgres foi estrangulado em 26 % do tempo (viés contra o Postgres) |
| **Aceite revisto:** p50 com desvio < 30 % entre rodadas; p95 como faixa | O p95 oscilou mais que 30 % em duas execuções anteriores por causa do ambiente; na execução final ficou abaixo de 30 % em 14 de 15 |
| **Leitor sem limites no ClickHouse** | Mede o motor. O leitor de produção, com o perfil da T1.5, falharia na q05 (§6) |
| **Isolamento entre tenants não medido** | Database por tenant não dá isolamento de recursos; a POC não mostrou controle (§7) |
| **Uma filial, a maior** | Filiais menores tendem a dar fatores diferentes; o recorte é o pior caso de volume |
| **Configurações padrão do Postgres para JIT e paralelismo** | JIT ligado e 2 workers paralelos; não foram ajustados por consulta |

### O que este resultado NÃO prova

- **Que a migração vale a pena.** Prova o ganho de leitura. Custo de operação, de migração e do framework
  do painel ficam fora.
- **A latência que o usuário vai ver.** Mede do cliente ao banco na mesma rede; o framework do painel, a
  rede e o Trino não entraram.
- **Que o Postgres não chegaria perto com mais investimento.** O `pgt` é o Postgres ajustado que a POC
  construiu, não o limite do Postgres. Réplica de leitura, mais RAM ou tabela agregada não foram testadas.
- **Que o ClickHouse aguenta vários tenants pesados ao mesmo tempo** (§7).

---

## 9. Resumo executivo

1. **Pergunta:** quanto um datamart ClickHouse ganha sobre o Postgres atual nas consultas do painel CEP,
   com o mesmo dado e o mesmo hardware.
2. **Os três bancos têm o mesmo dado:** 7 114 401 linhas, mesma soma, zero divergência nas 15 combinações
   de filial e mês, e as 5 consultas devolvem o mesmo resultado nos três (§2).
3. **O ClickHouse é de 20 a 55 vezes mais rápido que o Postgres de hoje**, uma consulta por vez, no p95
   (§3).
4. **Na consulta central do painel (q04), o p95 sai de 2,6 s para 47 ms.**
5. **O Postgres ajustado (particionado, BRIN) reduz a distância, mas não a fecha:** fica de 8 a 40 vezes
   atrás, e só ganha nas consultas de recorte estreito.
6. **O ganho vem de ler menos:** de 11 a 277 vezes menos bytes, por armazenamento colunar, compressão e
   ordem física pela chave do painel (§4).
7. **O ClickHouse ocupa 7 vezes menos disco** (0,55 contra 3,9 GB). 85 % do que ele ocupa é uma chave de
   hash que o painel não lê; sem ela, comprime 13 vezes (§5).
8. **`SELECT *` não mata o ganho com um cliente (30×), mas encolhe sob concorrência (5 a 9×)** e esbarra no
   limite de resultado do perfil de leitura por tenant (§6).
9. **Com 8 clientes o ClickHouse segue uma ordem de grandeza à frente** (9 a 26×), com a ressalva de que o
   nó de teste é o gargalo (§3).
10. **O que pesa contra o número:** ambiente de 4 vCPU, volume de 3 meses, Postgres que não cabe no próprio
    cache e o custo do `FINAL` que a carga de produção vai introduzir no mês corrente (§8). Isolamento
    entre tenants não foi medido (§7).

---

## 10. Anexo — como reproduzir

A partir da raiz do repositório, com a stack de pé e os dois tenants carregados:

```fish
bash benchmark/compare-counts.sh
bash scripts/profile.sh quiesce
bash benchmark/read-bench.sh --paridade
bash benchmark/read-bench.sh -r 30 -c 1
bash benchmark/read-bench.sh -r 30 -c 1
bash benchmark/read-bench.sh -r 10 -c 8
bash benchmark/read-bench.sh --instrumentar
bash benchmark/read-bench.sh --modo frio -r 3
bash benchmark/report.sh --aceite
bash scripts/profile.sh resume
```

- O `compare-counts.sh` precisa sair 0; qualquer recarga do dado invalida a medição.
- O modo frio reinicia o pod do Postgres antes de cada execução (30 vezes em `-r 3`).
- Os CSVs e o relatório bruto ficam em `benchmark/results/`, que não é versionado; este documento é.
- Tempo total: cerca de 2 h, a maior parte no `-c 8`.

---

## 11. Referências

**Execução e evidência**
- [TESTES §6.1 — preparação e carga do recorte](../../docs/TESTES.md#61-preparação--carga-do-recorte-e-índice-do-dashboard)
- [TESTES §6.3 — T3.1, portão de corretude](../../docs/TESTES.md#63-t31--portão-de-corretude)
- [TESTES §6.5 — T3.2, suíte de leitura](../../docs/TESTES.md#65-t32--suíte-de-leitura)

**Decisões**
- [ADR-002 — modelagem da tabela no ClickHouse](../../docs/decisoes/ADR-002-modelagem-clickhouse.md)
- [ADR-003 — RBAC multi-tenant](../../docs/decisoes/ADR-003-rbac-multi-tenant.md)
- [ADR-004 — estratégia de carga](../../docs/decisoes/ADR-004-janela-de-carga.md)
- [Análise da estratégia de carga](../../docs/analise-estrategia-carga-clickhouse.md)

**Especificação**
- [E3 — validação](../../docs/epicos/E3-validacao.md)

---

*Arquivo: `benchmark/results/RESULTADO.md` (única exceção versionada do diretório). Incrementar a versão
quando: a medição for refeita; o volume ou o ambiente mudarem; a T3.3 voltar ao escopo; ou a carga de
produção por `ReplacingMergeTree` for medida no mês corrente.*
