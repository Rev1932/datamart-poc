"""Fábrica da DAG de datamart: uma carga por destino, na mesma janela.

Cópia enxuta de dtlk-airflow-pipeline/dags/k8s_lakatos_gold_datamart.py. O que sai e por quê:

- **Roteamento por `gold_type`.** Lá cada tabela ia para UM destino conforme o tipo. Aqui a
  mesma tabela vai para os DOIS — é o experimento. Com isso caem os grupos, a amarra por
  prefixo e a validação de grupo vazio, que derrubaria a DAG por não haver `dimension`.
- **Dataset e `outlets`.** Sem gold materializada não há produtor para ancorar o Dataset.
- **`expand_kwargs` por filial.** Uma tabela Delta por nome, com todas as filiais dentro.

O que entra: a janela de carga, calculada UMA vez e consumida pelos dois braços.
"""
from __future__ import annotations

import copy
import hashlib
import re
from datetime import timedelta
from pathlib import Path

import pendulum
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from pymongo import MongoClient

MONGO_URI = Variable.get("mongodb_k8s_test")
MONGO_DB = "Data_Catalog"

NAMESPACE = "datamart"
K8S_CONN_ID = "kubernetes_default"
MANIFESTO = Path(__file__).parent / "manifests" / "spark-honeycomb-datamart.yaml"

# Um driver Spark por vez: dois não cabem no nó. Vale dentro de cada braço também, para o
# caso de o config declarar mais de uma tabela.
MAX_SIMULTANEOS = 1
MAX_NOME = 40
VERSAO_VALIDA = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$")

DESTINOS = {
    "carga_postgres": ("datamart_pg", timedelta(minutes=60)),
    "carga_clickhouse": ("datamart_ch", timedelta(minutes=60)),
}


def _config(colecao: str) -> dict | None:
    """Documento mais recente da coleção.

    Cliente por chamada e `serverSelectionTimeoutMS` explícito: uma leitura pendurada estoura
    o `dagbag_import_timeout` e derruba o arquivo do DagBag.
    """
    with MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) as cliente:
        return cliente[MONGO_DB][colecao].find_one(sort=[("_id", -1)])


def _tabelas(cfg: dict) -> list[dict]:
    bruto = cfg.get("tables") or {}
    return list(bruto.values()) if isinstance(bruto, dict) else list(bruto)


def _lista(valor) -> list[str]:
    if not valor:
        return []
    itens = valor if isinstance(valor, list) else [valor]
    return [str(item).strip() for item in itens if str(item).strip()]


def _versao(cfg: dict, colecao: str) -> str:
    versao = str(cfg.get("honeycomb_version") or "").strip()
    if not versao:
        raise ValueError(f"Config de {colecao} sem 'honeycomb_version'.")
    if not VERSAO_VALIDA.match(versao):
        raise ValueError(f"'honeycomb_version' inválida em {colecao}: {versao!r}")
    return versao


def _validar(tabelas: list[dict], colecao: str) -> None:
    """Valida o config inteiro antes de qualquer CR subir."""
    vistas: set[str] = set()
    for tabela in tabelas:
        nome = (tabela.get("name") or "").strip()
        if not nome:
            raise ValueError(f"Tabela sem 'name' no config de {colecao}: {tabela}")
        if not _lista(tabela.get("chave_pk")):
            raise ValueError(f"Tabela {nome!r} sem 'chave_pk' em {colecao}.")
        if nome in vistas:
            raise ValueError(f"Tabela {nome!r} duplicada em {colecao}.tables.")
        vistas.add(nome)


def _nome_cr(*partes: str) -> str:
    """Nome de SparkApplication válido como DNS-1123 label, único por combinação.

    O corte recebe um hash do nome COMPLETO: dois nomes com o mesmo prefixo longo truncariam
    para a mesma string e a rastreabilidade se perderia.
    """
    completo = re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]+", "-", "-".join(partes).lower()))
    if len(completo) <= MAX_NOME:
        return completo.strip("-")
    sufixo = hashlib.sha1(completo.encode()).hexdigest()[:6]
    return f"{completo[: MAX_NOME - len(sufixo) - 1].strip('-')}-{sufixo}"


def _aplicar_tenant(manifesto: dict, tenant: str) -> None:
    """Resolve os placeholders TENANT do template, no lugar."""
    spec = manifesto["spec"]
    manifesto.setdefault("metadata", {}).setdefault("labels", {})[
        "app.kubernetes.io/name"
    ] = f"spark-{tenant}"

    for papel in ("driver", "executor"):
        bloco = spec.get(papel) or {}
        bloco.setdefault("labels", {})["app.kubernetes.io/name"] = f"spark-{tenant}"
        for ref in bloco.get("envFrom") or []:
            for chave in ("configMapRef", "secretRef"):
                alvo = ref.get(chave)
                if alvo and "TENANT" in (alvo.get("name") or ""):
                    alvo["name"] = alvo["name"].replace("TENANT", tenant)


def _aplicar_versao(manifesto: dict, versao: str) -> None:
    """Resolve o placeholder VERSION da imagem, no lugar.

    Exigir o placeholder: com uma tag literal no YAML a versão do config seria ignorada em
    silêncio e o pod subiria com a imagem errada.
    """
    imagem = manifesto["spec"].get("image") or ""
    if "VERSION" not in imagem:
        raise ValueError(f"{MANIFESTO.name}: 'spec.image' sem placeholder VERSION ({imagem!r}).")
    manifesto["spec"]["image"] = imagem.replace("VERSION", versao)


def _carregar_base() -> dict:
    """Lê o template e valida o namespace.

    CR fora do namespace do operator não é reconciliado: nenhum pod, nenhum evento, nenhum
    erro — falha silenciosa.
    """
    base = yaml.safe_load(MANIFESTO.read_text(encoding="utf-8"))
    namespace_base = base.get("metadata", {}).get("namespace")
    if namespace_base != NAMESPACE:
        raise ValueError(f"{MANIFESTO.name}: namespace {namespace_base!r}, esperado {NAMESPACE!r}")
    return base


def _janela_do_mes(inicio) -> dict[str, str]:
    """Mês calendário que contém `inicio`, fechado à esquerda e aberto à direita.

    O destino ClickHouse particiona por `toYYYYMM` e a carga troca a partição inteira: a
    unidade de carga é o mês. Uma janela parcial faria o REPLACE PARTITION apagar o resto do
    mês, sem erro — e o repositório recusa antes disso.
    """
    primeiro = inicio.in_timezone("UTC").start_of("month")
    return {
        "janela_inicio": primeiro.format("YYYY-MM-DD HH:mm:ss"),
        "janela_fim": primeiro.add(months=1).format("YYYY-MM-DD HH:mm:ss"),
    }


def _render(base: dict, tenant: str, tabela: dict, versao: str, pipeline: str,
            janela: dict) -> str:
    nome = (tabela.get("name") or "").strip()
    manifesto = copy.deepcopy(base)
    _aplicar_tenant(manifesto, tenant)
    _aplicar_versao(manifesto, versao)
    manifesto["metadata"]["name"] = _nome_cr("spark", tenant, pipeline, nome)

    argumentos = [
        "--pipeline", pipeline,
        "--tenant_name", tenant,
        # Uma tabela Delta por nome, com todas as filiais dentro. O argparse exige a flag.
        "--filial_name", tenant,
        "--table_name", nome,
        "--janela_inicio", janela["janela_inicio"],
        "--janela_fim", janela["janela_fim"],
    ]

    obrigatorias = _lista(tabela.get("colunas_obrigatorias"))
    if obrigatorias:
        argumentos += ["--colunas_obrigatorias", *obrigatorias]

    # `--primary_key` por ÚLTIMO: usa nargs='*' e consome os tokens até a próxima flag.
    argumentos += ["--primary_key", *_lista(tabela.get("chave_pk"))]

    manifesto["spec"]["arguments"] = argumentos
    return yaml.safe_dump(manifesto, sort_keys=False, allow_unicode=True)


def _montar(tenant: str, pipeline: str, janela: dict) -> list[dict]:
    colecao = f"k8s_{tenant}_gold"
    base = _carregar_base()

    cfg = _config(colecao)
    if not cfg:
        raise ValueError(f"Nenhum config em {MONGO_DB}.{colecao}.")

    tabelas = _tabelas(cfg)
    if not tabelas:
        raise ValueError(f"Config de {colecao} sem 'tables'.")
    _validar(tabelas, colecao)

    versao = _versao(cfg, colecao)
    nomes = ", ".join((t.get("name") or "").strip() for t in tabelas)
    print(f"[{tenant}] honeycomb {versao} via --pipeline {pipeline}, janela "
          f"[{janela['janela_inicio']}, {janela['janela_fim']}): {nomes}.")

    return [{"application_file": _render(base, tenant, t, versao, pipeline, janela)}
            for t in tabelas]


def criar_dag(tenant: str):
    """DAG de datamart do tenant: Postgres e ClickHouse, em sequência, na mesma janela."""
    colecao = f"k8s_{tenant}_gold"

    try:
        agenda = (_config(colecao) or {}).get("schedule_interval")
    except Exception as exc:
        print(f"[{tenant}] config indisponível em parse time ({exc}); DAG em modo manual.")
        agenda = None

    @dag(
        dag_id=f"k8s_{tenant}_datamart",
        description=(
            f"Carrega os dois datamarts do {tenant} a partir da silver, com a mesma query e a "
            f"mesma janela: Postgres e depois ClickHouse"
        ),
        schedule=agenda,
        # Anterior ao dado mais antigo da silver (2025-08). Um trigger com logical_date antes
        # do start_date nao cria task instance nenhuma: o DagRun fica VERDE sem rodar nada.
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 0},
        tags=["honeycomb", "datamart", tenant, "poc"],
    )
    def datamart():
        """Os dois braços leem a mesma janela porque ela é calculada uma vez, aqui.

        Em sequência, não em paralelo: dois drivers Spark não cabem no nó. A ordem entre eles
        é indiferente para o resultado — o que não pode é a silver mudar no meio, e isso é
        premissa operacional, não garantia do código.
        """

        @task
        def calcular_janela(data_interval_start=None) -> dict:
            """Janela do DagRun, uma só para os dois braços."""
            janela = _janela_do_mes(data_interval_start)
            print(f"[{tenant}] janela [{janela['janela_inicio']}, {janela['janela_fim']})")
            return janela

        @task(task_id="montar_postgres")
        def montar_postgres(janela: dict) -> list[dict]:
            return _montar(tenant, DESTINOS["carga_postgres"][0], janela)

        @task(task_id="montar_clickhouse")
        def montar_clickhouse(janela: dict) -> list[dict]:
            return _montar(tenant, DESTINOS["carga_clickhouse"][0], janela)

        def operador(task_id: str):
            return SparkKubernetesOperator.partial(
                task_id=task_id,
                namespace=NAMESPACE,
                kubernetes_conn_id=K8S_CONN_ID,
                do_xcom_push=False,
                execution_timeout=DESTINOS[task_id][1],
                max_active_tis_per_dag=MAX_SIMULTANEOS,
            )

        janela = calcular_janela()
        postgres = operador("carga_postgres").expand_kwargs(montar_postgres(janela))
        clickhouse = operador("carga_clickhouse").expand_kwargs(montar_clickhouse(janela))

        postgres >> clickhouse

    return datamart()
