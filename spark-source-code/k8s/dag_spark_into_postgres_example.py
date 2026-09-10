"""
DAG de EXEMPLO — orquestra o job spark_into_postgres no K8s via Spark Operator.

Segue o padrão config-driven da plataforma (config no MongoDB `Data_Catalog`, uma execução
por entidade — ver dags/mongo_teste_k8s.py) e submete um SparkApplication por tabela usando
o SparkKubernetesOperator (provider apache-airflow-providers-cncf-kubernetes).

⚠️ TEMPLATE para adaptar: ajuste conn ids, coleção Mongo, namespace, imagem e o mapeamento
de campos (name/chave_pk/filial) conforme o documento real. Copie para dags/ ao usar.
"""
from datetime import datetime

import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

MONGO_CONN_ID = "mongodb_k8s"
K8S_CONN_ID = "kubernetes_default"
NAMESPACE = "datawake"
IMAGE = "<REGISTRY>/<NAMESPACE>/honeycomb:<TAG>"

default_args = {"owner": "airflow", "start_date": datetime(2026, 7, 20), "retries": 0}


def _build_sparkapplication(entity: dict, run_id: str) -> str:
    """Renderiza um SparkApplication (ver k8s/sparkapplication.yaml) para uma entidade."""
    tenant = entity["tenant_name"]
    filial = entity["filial_name"]
    table = entity["table_name"]
    primary_key = entity["primary_key"]  # lista de colunas

    safe = f"{tenant}-{table}".lower().replace("_", "-")[:40]
    manifest = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": f"sip-{safe}-{run_id}", "namespace": NAMESPACE},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/bitnami/spark/app/src/main/main.py",
            "sparkVersion": "3.5.1",
            "restartPolicy": {"type": "Never"},
            "arguments": [
                "--pipeline", "gold", #TODO precisa ser a nivel de dag
                "--tenant_name", tenant,
                "--filial_name", filial,
                "--table_name", table,
                "--primary_key", *primary_key,
            ],
            "sparkConf": {
                "spark.hadoop.fs.s3a.aws.credentials.provider":
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
            },
            "driver": {
                "cores": 1, "memory": "1g", "serviceAccount": "spark",
                "env": [{"name": "PYTHONPATH", "value": "/opt/bitnami/spark/app/src/main"}],
                "envFrom": [
                    {"configMapRef": {"name": "spark-unipac-config"}},
                    {"secretRef": {"name": "spark-into-postgres-secret"}},
                ],
            },
            "executor": {
                "cores": 1, "instances": 2, "memory": "2g",
                "env": [{"name": "PYTHONPATH", "value": "/opt/bitnami/spark/app/src/main"}],
                "envFrom": [
                    {"configMapRef": {"name": "spark-into-postgres-config"}},
                    {"secretRef": {"name": "spark-into-postgres-secret"}},
                ],
            },
        },
    }
    return yaml.safe_dump(manifest, sort_keys=False)


with DAG(
    dag_id="spark_into_postgres_gold_datamart",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["spark_into_postgres", "gold_datamart", "k8s"],
) as dag:

    @task
    def carregar_entidades(**context) -> list[str]:
        """Lê a config mais recente no Mongo e renderiza 1 SparkApplication por entidade."""
        hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        collection = hook.get_collection(mongo_collection="unipac", mongo_db="Data_Catalog")
        config = collection.find_one(sort=[("_id", -1)])
        if not config:
            raise ValueError("Nenhuma configuração encontrada no MongoDB.")

        entities = config.get("entity_list_incremental", {})
        items = entities.values() if isinstance(entities, dict) else entities

        run_id = context["run_id"].lower().replace("_", "-").replace(":", "-")[-12:]
        manifests = []
        for v in items:
            if not isinstance(v, dict):
                continue
            pk = v.get("chave_pk", [])
            pk = pk if isinstance(pk, list) else [str(pk)]
            manifests.append(_build_sparkapplication(
                {
                    "tenant_name": config.get("tenant_name", "unipac"),
                    "filial_name": v.get("filial", config.get("tenant_name", "unipac")),
                    "table_name": v.get("name"),
                    "primary_key": pk,
                },
                run_id,
            ))
        return manifests

    # Um SparkApplication por entidade (dynamic task mapping).
    SparkKubernetesOperator.partial(
        task_id="submit_spark_into_postgres",
        namespace=NAMESPACE,
        kubernetes_conn_id=K8S_CONN_ID,
        do_xcom_push=True,
    ).expand(application_file=carregar_entidades())
