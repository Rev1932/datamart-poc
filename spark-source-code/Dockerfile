# Imagem do job spark_into_postgres para Spark Operator (SparkApplication, cluster mode).
#
# Auto-contida: parte do apache/spark oficial e embute TODAS as libs (Delta, S3A, JDBC Postgres,
# cliente Hive Metastore) + o codigo do job. Nao depende de nenhuma imagem base interna.

# O Metastore do cluster e Hive 4.2 e o cliente builtin do Spark 3.5 (Hive 2.3.9)
# chama metodos thrift removidos no Hive 4 ("Invalid method name: 'get_table'").
# Este estagio resolve os jars do cliente 3.1.3 (maximo suportado pelo Spark 3.5)
# pelo mecanismo nativo do Spark (IsolatedClientLoader -> ~/.ivy2/jars), que baixa
# o conjunto exato e compativel — o metastore derby local e descartado.
FROM apache/spark:3.5.8-java17-python3 AS hive-client-jars
USER root
ENV HOME=/tmp
# WORKDIR /tmp: o metastore derby temporario escreve no cwd
WORKDIR /tmp
# o ivy resolve para ~/.ivy2 do user.home (/etc/passwd), nao do env HOME
RUN /opt/spark/bin/spark-sql \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.sql.hive.metastore.version=3.1.3 \
      --conf spark.sql.hive.metastore.jars=maven \
      -e "SHOW DATABASES;" \
 && mkdir -p /opt/hive-metastore-jars \
 && cp /root/.ivy2/jars/*.jar /opt/hive-metastore-jars/


FROM apache/spark:3.5.8-java17-python3

ARG MAVEN=https://repo1.maven.org/maven2
# Delta 3.3.x e o combo estavel para Spark 3.5.x (Scala 2.12)
ARG DELTA_VERSION=3.3.2
# hadoop-aws precisa casar com o Hadoop do Spark 3.5.x (3.3.4); o sdk-bundle e a
# versao declarada no POM do hadoop-aws 3.3.4
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
# JDBC do Postgres: necessario p/ o write JDBC em repo/repository.py
# (RepositoryGoldDatamart.write -> .write.format("jdbc"))
ARG POSTGRES_JDBC_VERSION=42.7.4

USER root
ADD --chmod=644 ${MAVEN}/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/spark/jars/

COPY --from=hive-client-jars /opt/hive-metastore-jars /opt/hive-metastore-jars
COPY docker/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Deps Python de runtime (ver requirements.txt). delta-spark vai a parte com --no-deps: ele
# declara pyspark como dependencia, mas o pyspark 3.5.8 ja vem na imagem (/opt/spark/python) —
# instalar outra copia duplicaria ~300MB e arriscaria sombrear a do Spark. A versao reusa o
# ARG DELTA_VERSION para o pacote pip nunca divergir do jar embutido acima.
COPY requirements.txt /opt/spark/app/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/spark/app/requirements.txt \
 && pip3 install --no-cache-dir --no-deps "delta-spark==${DELTA_VERSION}"

# Codigo + assets. resources/ fica IRMAO de src/ para o auto-discovery do config_base_path
# (ConfigManager) localizar resources/queries/*.sql.
WORKDIR /opt/spark/app
COPY src/       /opt/spark/app/src/
COPY resources/ /opt/spark/app/resources/

# main.py faz imports de pacote (from core..., from utils...) — src/main precisa estar no path.
# pyspark/py4j entram aqui tambem porque a base NAO os instala em site-packages (so o
# entrypoint.sh do spark-submit os poe no path) — sem isso um `python3 main.py` direto,
# usado nos smoke tests, quebra em `import pyspark`. Duplicar e inofensivo.
ENV PYTHONPATH=/opt/spark/app/src/main:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

# uid 185 = usuario spark da imagem base (runAsNonRoot)
USER spark
