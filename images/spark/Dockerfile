# Imagem Spark da POC: base oficial Spark 3.5.1 (Scala 2.12, Java 11) + Python 3.12.
# O código usa f-strings com aspas aninhadas (PEP 701) => exige Python 3.12.
#
# O deadsnakes/apt não fornece python3.12 de forma confiável no focal (base do Spark),
# então trazemos o interpretador de python:3.12-slim-bullseye (glibc 2.31, compatível
# com o focal) via multi-stage. O pip já vem no site-packages copiado.
#
# Build a partir da RAIZ do repositório:
#   docker build -f images/spark/Dockerfile -t datamart-spark:poc .
#   (ou: minikube image build -t datamart-spark:poc -f images/spark/Dockerfile .)
#
# Jars (Delta / hadoop-aws / connector ClickHouse) são resolvidos em submit-time via
# spec.deps.packages nas SparkApplication (requer internet no 1º run).
FROM python:3.12-slim-bullseye AS py

FROM apache/spark:3.5.1
USER root

# Python 3.12 (interpretador + stdlib + libpython + pip) vindo do estágio `py`.
COPY --from=py /usr/local/bin/python3.12 /usr/local/bin/python3.12
COPY --from=py /usr/local/lib/python3.12 /usr/local/lib/python3.12
COPY --from=py /usr/local/lib/libpython3.12.so.1.0 /usr/local/lib/
RUN ldconfig && python3.12 --version

COPY images/spark/requirements.txt /tmp/requirements.txt
RUN python3.12 -m pip install --no-cache-dir -r /tmp/requirements.txt

# Jars de Delta / hadoop-aws / connector ClickHouse "assados" em /opt/spark/jars.
# Resolvemos via ivy no build (roda um Pi em local mode só para baixar as deps
# transitivas) e copiamos para o classpath. Assim NÃO há resolução ivy em runtime
# (que falha no spark-operator por HOME=/nonexistent) e a execução fica offline.
ENV SPARK_JARS_PACKAGES="io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.10.0,com.clickhouse:clickhouse-client:0.9.8,com.clickhouse:clickhouse-http-client:0.9.8,org.apache.httpcomponents.client5:httpclient5:5.2.1"
RUN /opt/spark/bin/spark-submit --master "local[1]" \
      --packages "$SPARK_JARS_PACKAGES" \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      /opt/spark/examples/src/main/python/pi.py 1 \
    && find /tmp/.ivy2 -name '*.jar' -exec cp -n {} /opt/spark/jars/ \; \
    && rm -rf /tmp/.ivy2

# Código PySpark existente + nova pipeline `datamart`
COPY spark-source-code/src/main/ /opt/spark/app/
COPY spark-source-code/resources/ /opt/spark/app/resources/

ENV PYTHONPATH=/opt/spark/app \
    PYSPARK_PYTHON=/usr/local/bin/python3.12 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.12

# O Spark Operator roda como usuário 185 por padrão
USER 185
WORKDIR /opt/spark/app
