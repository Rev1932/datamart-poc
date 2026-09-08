# ClickHouse — instalação do operador (Altinity)

O ClickHouse é gerenciado pelo **Altinity clickhouse-operator**. Instalação (versão fixada):

```bash
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/release-0.24.0/deploy/operator/clickhouse-operator-install-bundle.yaml
```

Isso registra os CRDs (`ClickHouseInstallation`, etc.) e sobe o operador em `kube-system`.
Depois, aplique a instância da POC:

```bash
kubectl apply -f infra/clickhouse/chi-datamart.yaml
# O operador Altinity não expõe condition=Ready; use o status agregado:
until [ "$(kubectl -n datamart get chi datamart -o jsonpath='{.status.status}')" = "Completed" ]; do sleep 10; done
```

O operador cria o Service `clickhouse-datamart` no namespace `datamart`
(DNS: `clickhouse-datamart.datamart.svc.cluster.local`), com HTTP em `:8123` e
protocolo nativo em `:9000` — usados pelo connector ClickHouse-Spark (Job C).

Acesso local (opcional):
```bash
kubectl -n datamart port-forward svc/clickhouse-datamart 8123:8123 9000:9000
clickhouse-client -h 127.0.0.1 --user datamart --password datamart123
```
