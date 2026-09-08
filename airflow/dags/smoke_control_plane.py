"""Smoke da plataforma: prova Variable, pymongo, contrato do control plane e RBAC do Spark.

Não substitui as DAGs de E2/T2.6. Existe para que o aceite de T1.7 seja mais forte que
"nenhum erro de import": um DagBag vazio também não tem erro de import.

Replica os idiomas da DAG produtiva (k8s_unipac_bronze_silver): cliente Mongo por chamada,
serverSelectionTimeoutMS explícito e leitura em parse time degradando para modo manual.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from pymongo import MongoClient

MONGO_URI = Variable.get("mongodb_k8s_test")
MONGO_DB = "Data_Catalog"
TENANTS = ("acme", "globex")
NAMESPACE_SPARK = "datamart"
GOLD_TYPES = {"dimension", "fact_delta", "fact_postgres"}


def _config(collection: str) -> dict | None:
    with MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) as client:
        return client[MONGO_DB][collection].find_one(sort=[("_id", -1)])


def _tabelas(cfg: dict) -> list[dict]:
    raw = cfg.get("tables") or {}
    return list(raw.values()) if isinstance(raw, dict) else list(raw)


try:
    SCHEDULE = (_config(f"k8s_{TENANTS[0]}") or {}).get("schedule_interval")
except Exception as exc:
    print(f"[smoke] config indisponivel em parse time ({exc}); DAG em modo manual.")
    SCHEDULE = None


@dag(
    dag_id="smoke_control_plane",
    description="Smoke de plataforma: Mongo, Variable e RBAC do spark-operator",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["smoke", "poc"],
)
def smoke():
    @task
    def checar_control_plane() -> dict:
        """Valida as duas colecoes de cada tenant contra as guardas das DAGs produtivas."""
        resumo = {}
        for t in TENANTS:
            bs = _config(f"k8s_{t}")
            gd = _config(f"k8s_{t}_gold")
            if not bs:
                raise ValueError(f"Nenhum config em {MONGO_DB}.k8s_{t}.")
            if not gd:
                raise ValueError(f"Nenhum config em {MONGO_DB}.k8s_{t}_gold.")

            filiais = bs.get("filiais") or []
            if not filiais:
                raise ValueError(f"Config de k8s_{t} sem 'filiais'.")

            for tabela in _tabelas(bs):
                if not (tabela.get("name") or "").strip():
                    raise ValueError(f"Tabela sem 'name' em k8s_{t}: {tabela}")
                if not tabela.get("chave_pk"):
                    raise ValueError(f"Tabela {tabela.get('name')!r} sem 'chave_pk' em k8s_{t}.")

            for tabela in _tabelas(gd):
                tipo = str(tabela.get("gold_type") or "").strip()
                if tipo not in GOLD_TYPES:
                    raise ValueError(
                        f"Tabela {tabela.get('name')!r} em k8s_{t}_gold com gold_type {tipo!r}: "
                        f"os validos sao {', '.join(sorted(GOLD_TYPES))}."
                    )

            if not str(bs.get("honeycomb_version") or "").strip():
                raise ValueError(f"Config de k8s_{t} sem 'honeycomb_version'.")

            resumo[t] = {
                "filiais": len(filiais),
                "tables": len(_tabelas(bs)),
                "gold_tables": len(_tabelas(gd)),
                "versao": bs["honeycomb_version"],
            }
            print(f"[smoke] {t}: {resumo[t]}")
        return resumo

    @task
    def checar_rbac_spark() -> int:
        """Lista SparkApplication no namespace datamart — prova o RoleBinding da SA do scheduler."""
        from kubernetes import client, config

        config.load_incluster_config()
        api = client.CustomObjectsApi()
        resp = api.list_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=NAMESPACE_SPARK,
            plural="sparkapplications",
        )
        nomes = [i["metadata"]["name"] for i in resp.get("items", [])]
        print(f"[smoke] {len(nomes)} SparkApplication em {NAMESPACE_SPARK}: {nomes}")
        return len(nomes)

    checar_control_plane()
    checar_rbac_spark()


smoke()
