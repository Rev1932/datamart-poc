"""Ancora as premissas de PySpark em que o modo multi-tabela se apoia.

Estes testes não exercitam código do Honeycomb — validam COMPORTAMENTO DO PYSPARK que o
TableRunner assume. Se o PySpark mudar (ou se PYSPARK_PIN_THREAD for desligado no ambiente),
o desenho de um pod por filial deixa de entregar paralelismo e ninguém perceberia: o job
continuaria correto, só que serial, e a consolidação de 165 pods em 5 viraria uma regressão
de tempo de execução em vez de um ganho.

Rode com: pytest -m integration tests/integration/test_table_runner_concorrencia.py
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark_fair():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .appName("honeycomb-tests-fair")
        .master("local[4]")
        # Mesma conf do manifesto de produção (spark-honeycomb-bronze-silver.yaml).
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_scheduler_pool_e_isolado_por_thread(spark_fair):
    """
    O TableRunner define um pool FAIR por tabela com
    `sc.setLocalProperty("spark.scheduler.pool", <tabela>)`.

    Isso só isola de fato porque o PySpark 3.5 roda em pinned thread mode (uma thread JVM
    dedicada por thread Python). Sem isso, as N tabelas cairiam todas no mesmo pool e o FAIR
    do manifesto não separaria nada.
    """
    sc = spark_fair.sparkContext
    nomes = [f"dw_tabela_{i}" for i in range(4)]
    observado = {}
    # A barreira garante que TODAS as threads escreveram antes de qualquer leitura — sem ela
    # o teste passaria mesmo com vazamento, por sorte de escalonamento.
    barreira = threading.Barrier(len(nomes))

    def marca(nome):
        sc.setLocalProperty("spark.scheduler.pool", nome)
        barreira.wait(timeout=30)
        observado[nome] = sc.getLocalProperty("spark.scheduler.pool")

    with ThreadPoolExecutor(max_workers=len(nomes)) as pool:
        list(pool.map(marca, nomes))

    assert observado == {nome: nome for nome in nomes}
    # A thread principal (que roda o main()) nunca é contaminada.
    assert sc.getLocalProperty("spark.scheduler.pool") is None


def test_jobs_de_threads_distintas_rodam_concorrentemente(spark_fair):
    """
    A premissa econômica da consolidação: N tabelas num pod só têm que se sobrepor no tempo.
    Se rodassem em série, 1 pod por filial seria mais LENTO que 1 pod por tabela, sem
    compensação para a economia de requests de CPU.
    """
    sc = spark_fair.sparkContext
    nomes = [f"dw_tabela_{i}" for i in range(4)]
    janelas = {}
    trava = threading.Lock()

    def trabalho(nome):
        sc.setLocalProperty("spark.scheduler.pool", nome)
        inicio = time.time()
        # Job com shuffle, pequeno mas não instantâneo.
        spark_fair.range(0, 400_000).repartition(4).groupBy("id").count().count()
        with trava:
            janelas[nome] = (inicio, time.time())

    inicio_total = time.time()
    with ThreadPoolExecutor(max_workers=len(nomes)) as pool:
        list(pool.map(trabalho, nomes))
    wall_clock = time.time() - inicio_total

    soma_individual = sum(fim - ini for ini, fim in janelas.values())
    sobreposicao = soma_individual / wall_clock

    # 1.0 = totalmente serial; 4.0 = ideal com 4 slots. O limiar é folgado de propósito:
    # o que se quer detectar é a REGRESSÃO para serial, não medir performance.
    assert sobreposicao > 1.5, (
        f"jobs não se sobrepuseram (fator {sobreposicao:.2f}x): "
        f"wall clock {wall_clock:.2f}s vs soma {soma_individual:.2f}s"
    )
