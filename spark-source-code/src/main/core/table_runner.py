"""Execução de N tabelas dentro de um único driver Spark.

Contexto: o modelo antigo submetia uma SparkApplication por (tenant, filial, tabela) — para um
tenant com 5 filiais e 33 tabelas, 165 pods por rajada. O custo fixo de cada driver (CPU e
memória reservadas só para orquestrar uma tabela de poucos minutos) saturava o bin-packing de
requests do cluster muito antes de saturar o uso real de CPU.

Este módulo é a peça que permite consolidar: uma SparkSession, N tabelas, `max_workers` threads.

Duas propriedades importam mais que a velocidade:

1. ISOLAMENTO ENTRE TABELAS. Cada thread recebe a SUA cópia de PipelineConfig. A dataclass é
   mutável e várias camadas (repositories, transformers) leem `table_name`/`primary_key` no
   `__init__` — compartilhar a instância faria uma tabela sobrescrever os parâmetros da outra.

2. ISOLAMENTO DE FALHA. Uma tabela que quebra não pode derrubar as outras N-1: no modelo antigo
   elas eram tasks independentes do Airflow. O erro é capturado por tabela, e o processo só
   falha no final, com o inventário do que quebrou.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from typing import Any

from core.pipeline_factory import PipelineFactory
from utils.handler_logger import initialize_logger, set_current_table


@dataclass
class TableResult:
    """Desfecho do processamento de uma tabela."""
    name: str
    ok: bool
    duration_s: float
    error: str | None = None


def normalize_tables(tables: Any) -> list[dict]:
    """Valida e normaliza a lista vinda de `--tables_json`.

    Falha CEDO e com mensagem específica: um item malformado descoberto só na hora de montar o
    repositório apareceria como um erro genérico no meio do processamento das outras tabelas.
    """
    if not isinstance(tables, list) or not tables:
        raise ValueError(
            f"--tables_json deve ser uma lista não vazia; recebido: {type(tables).__name__}"
        )

    normalizadas: list[dict] = []
    vistas: set[str] = set()

    for item in tables:
        if not isinstance(item, dict):
            raise ValueError(f"Item de --tables_json não é um objeto: {item!r}")

        nome = str(item.get("name") or "").strip()
        if not nome:
            raise ValueError(f"Item de --tables_json sem 'name': {item!r}")

        pk = item.get("chave_pk") or []
        if not isinstance(pk, list):
            pk = [str(pk)]
        pk = [str(c).strip() for c in pk if str(c).strip()]
        if not pk:
            # HardBusinessRulesTransformerAuto levantaria ValueError mais adiante; aqui a
            # mensagem diz QUAL tabela está sem chave.
            raise ValueError(f"Tabela {nome!r} sem 'chave_pk' em --tables_json.")

        if nome in vistas:
            # Duas threads na mesma tabela disputariam o mesmo destino Delta sem necessidade.
            raise ValueError(f"Tabela {nome!r} duplicada em --tables_json.")
        vistas.add(nome)

        obrigatorias = item.get("colunas_obrigatorias") or []
        if not isinstance(obrigatorias, list):
            obrigatorias = [str(obrigatorias)]
        obrigatorias = [str(c).strip() for c in obrigatorias if str(c).strip()]

        normalizadas.append({"name": nome, "chave_pk": pk, "colunas_obrigatorias": obrigatorias})

    return normalizadas


class TableRunner:
    """Roda a lista de tabelas em paralelo sobre uma SparkSession compartilhada."""

    def __init__(self, spark, environment_parameters, base_config, queries_dir,
                 max_workers: int = 4):
        self.spark = spark
        self.environment_parameters = environment_parameters
        self.base_config = base_config
        self.queries_dir = queries_dir
        self.max_workers = max(1, int(max_workers))
        self.logger = initialize_logger()
        self.factory = PipelineFactory()

    def run(self, tables: list[dict]) -> list[TableResult]:
        tabelas = normalize_tables(tables)
        total = len(tabelas)
        workers = min(self.max_workers, total)

        self.logger.info(
            f"Processando {total} tabela(s) da filial "
            f"{self.base_config.filial_name!r} com {workers} thread(s)."
        )

        resultados: dict[str, TableResult] = {}
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tabela") as pool:
            futuros = {
                pool.submit(self._run_one, tabela, indice, total): tabela["name"]
                for indice, tabela in enumerate(tabelas, start=1)
            }
            for futuro in as_completed(futuros):
                nome = futuros[futuro]
                try:
                    resultados[nome] = futuro.result()
                except Exception as e:
                    # _run_one já captura tudo; isto cobre uma falha do próprio wrapper
                    # (ex.: MemoryError na thread) para não perder a tabela do relatório.
                    resultados[nome] = TableResult(
                        nome, False, 0.0, f"{type(e).__name__}: {e}"
                    )

        # Reordena pela ordem de entrada: `as_completed` devolve por ordem de término, o que
        # tornaria o relatório final não determinístico entre execuções.
        return [resultados[t["name"]] for t in tabelas]

    def _run_one(self, tabela: dict, indice: int, total: int) -> TableResult:
        nome = tabela["name"]
        inicio = time.time()

        # Pool FAIR por tabela. Sem `spark.scheduler.mode=FAIR` + este local property, o Spark
        # escalona em FIFO e o primeiro job submetido ocupa todos os slots até terminar — as
        # threads viravam uma fila com passos extras. Ambos são necessários: a conf é do
        # manifesto, esta propriedade é por thread.
        #
        # `setLocalProperty` só é thread-local porque o PySpark 3.5 roda com pinned thread mode
        # ligado por padrão (cada thread Python recebe uma thread JVM dedicada). Se algum dia
        # PYSPARK_PIN_THREAD for desligado no ambiente, os pools vazam entre threads e as
        # tabelas voltam a disputar o mesmo pool — ver
        # tests/integration/test_table_runner_concorrencia.py, que ancora essa premissa.
        #
        # O worker do pool é REUTILIZADO entre tabelas, então isto (e set_current_table) roda
        # a cada tabela, não uma vez por thread.
        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", nome)
        set_current_table(nome)

        try:
            self.logger.info(f"[{indice}/{total}] Iniciando.")

            # Cópia própria: NUNCA mutar self.base_config (compartilhada entre as threads).
            config = replace(
                self.base_config,
                table_name=nome,
                primary_key=tabela["chave_pk"],
                # O valor por tabela vence; sem ele, o da linha de comando vale para todas.
                colunas_obrigatorias=(tabela["colunas_obrigatorias"]
                                      or self.base_config.colunas_obrigatorias),
            )

            pipeline = self.factory.new_instance(
                key=config.pipeline,
                spark=self.spark,
                environment_parameters=self.environment_parameters,
                runtime_parameters=config,
                queries_dir=self.queries_dir,
            )
            if pipeline:
                pipeline.run()

            duracao = round(time.time() - inicio, 2)
            self.logger.info(f"[{indice}/{total}] OK em {duracao}s.")
            return TableResult(nome, True, duracao)

        except Exception as e:
            duracao = round(time.time() - inicio, 2)
            self.logger.exception(f"[{indice}/{total}] FALHOU após {duracao}s: {e}")
            return TableResult(nome, False, duracao, f"{type(e).__name__}: {e}")

        finally:
            # Limpa o contexto para que logs da thread fora do escopo de uma tabela (shutdown
            # do pool, por exemplo) não sejam atribuídos à última tabela processada.
            set_current_table(None)
            self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)


def format_report(filial: str, resultados: list[TableResult]) -> str:
    """Resumo legível no log do driver — é o que o operador lê no Airflow."""
    ok = [r for r in resultados if r.ok]
    falhas = [r for r in resultados if not r.ok]
    tempo_total = round(sum(r.duration_s for r in resultados), 2)

    linhas = [
        f"RESUMO {filial}: {len(ok)} OK, {len(falhas)} FALHA "
        f"(tempo somado de tabela: {tempo_total}s)"
    ]
    for r in falhas:
        linhas.append(f"  FALHA {r.name} ({r.duration_s}s) -> {r.error}")
    return "\n".join(linhas)
