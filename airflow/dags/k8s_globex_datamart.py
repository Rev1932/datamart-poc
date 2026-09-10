"""Datamart do tenant globex: Postgres e ClickHouse a partir da silver, na mesma janela.

Um arquivo por tenant, como em produção — é o `dag_id` que o Airflow indexa. A lógica vive
em `datamart_dag`, compartilhada: dois tenants com a mesma DAG copiada divergiriam na
primeira correção feita em um só.
"""
from __future__ import annotations

from datamart_dag import criar_dag

k8s_globex_datamart = criar_dag("globex")
