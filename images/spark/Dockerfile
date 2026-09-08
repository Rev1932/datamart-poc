# Imagem da POC: a imagem de produção do honeycomb mais os jars do connector ClickHouse.
#
# NÃO reconstrói o honeycomb. A imagem publicada já traz Spark 3.5.8 (Scala 2.12, Java 17),
# Delta 3.3.2, hadoop-aws 3.3.4, o app em /opt/spark/app e as dependências Python.
# O único delta da POC é o connector ClickHouse, que a imagem de produção ainda não carrega.
#
#   docker build -f images/spark/Dockerfile -t honeycomb:poc images/spark/
#   minikube image load honeycomb:poc
#
# A tag `poc` casa com o `honeycomb_version` semeado no Mongo (airflow/mongo-seed/).

FROM hub.datawake.cloud/dw-dados/honeycomb:latest

USER root

# Conjunto de versões alinhado no incidente #11 do docs/TROUBLESHOOTING.md: o cliente
# 0.6.3 lê o framing LZ4 do servidor no offset errado e falha já no
# ClickHouseCatalog.initialize, ANTES de gravar linha — e a SparkApplication ainda
# assim termina COMPLETED, com zero linha na tabela.
ENV CH_PACKAGES="com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.10.0,\
com.clickhouse:clickhouse-client:0.9.8,\
com.clickhouse:clickhouse-http-client:0.9.8,\
org.apache.httpcomponents.client5:httpclient5:5.2.1"

# Resolve por ivy no build e copia o fecho transitivo para o classpath. Assim não há
# resolução em runtime, que falha no Spark Operator por HOME=/nonexistent (incidente #5).
# Copia só de .ivy2/jars: o diretório cache/ tem os MESMOS jars com outro nome, e um
# `find -name '*.jar'` põe cada um duas vezes no classpath.
RUN /opt/spark/bin/spark-submit --master "local[1]" \
      --packages "$CH_PACKAGES" \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      /opt/spark/examples/src/main/python/pi.py 1 > /dev/null \
    && cp -n /tmp/.ivy2/jars/*.jar /opt/spark/jars/ \
    && rm -rf /tmp/.ivy2

USER 185
