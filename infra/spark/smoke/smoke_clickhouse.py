"""Valida o connector ClickHouse sem tocar em Delta nem em dado real.

Exercita ClickHouseCatalog.initialize, que é onde o incidente #11 do TROUBLESHOOTING
estoura quando as versões de cliente e servidor divergem no framing LZ4 — antes de
gravar qualquer linha, e sem que a SparkApplication deixe de terminar COMPLETED.
"""
import os
import sys

from pyspark.sql import SparkSession


def main() -> int:
    catalog = os.environ["DATAMART_CH_CATALOG"]
    database = os.environ["DATAMART_CH_DATABASE"]
    url = os.environ["DATAMART_CH_URL"]
    host = url.split("://", 1)[1].split(":")[0]
    port = url.rsplit(":", 1)[1]

    spark = (
        SparkSession.builder.appName("smoke-clickhouse")
        .config(f"spark.sql.catalog.{catalog}", "com.clickhouse.spark.ClickHouseCatalog")
        .config(f"spark.sql.catalog.{catalog}.host", host)
        .config(f"spark.sql.catalog.{catalog}.protocol", "http")
        .config(f"spark.sql.catalog.{catalog}.http_port", port)
        .config(f"spark.sql.catalog.{catalog}.user", os.environ["DATAMART_CH_USER"])
        .config(f"spark.sql.catalog.{catalog}.password", os.environ["DATAMART_CH_PASSWORD"])
        .config(f"spark.sql.catalog.{catalog}.database", database)
        .getOrCreate()
    )

    print(f"[smoke] catalogo={catalog} database={database} host={host}:{port}")

    # Dispara ClickHouseCatalog.initialize — é aqui que o incidente #11 aparece.
    tabelas = spark.sql(f"SHOW TABLES IN {catalog}.{database}").collect()
    print(f"[smoke] SHOW TABLES devolveu {len(tabelas)} tabela(s):")
    for t in tabelas:
        print(f"[smoke]   {t}")

    # Leitura real: prova que o caminho de dados também funciona, não só o de metadados.
    # count(*) e não count(): a query passa pelo parser do Spark, não pelo do ClickHouse.
    sonda = f"{catalog}.{database}.__rbac_probe"
    n = spark.sql(f"SELECT count(*) AS n FROM {sonda}").collect()[0]["n"]
    print(f"[smoke] SELECT count() em {sonda} = {n}")

    print("[smoke] OK")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
