import json
from dataclasses import dataclass, field, fields
from typing import List, Optional


def normalize_source_tenants(source_tenants) -> List[dict]:
    """Valida e normaliza `--source_tenants` (pipeline `silver_super_tenant`).

    Falha no PARSE, e nao na leitura: um `base_path` com typo chegaria ate o repositorio como
    `DeltaTable.isDeltaTable() == False`, indistinguivel de "esse cliente nao tem essa tabela" —
    e a origem inteira seria PULADA em silencio.

    Aceita a string JSON vinda do argparse ou a lista ja desserializada.

    Raises:
        ValueError: nomeando o item malformado.
    """
    if not source_tenants:
        return []

    if isinstance(source_tenants, str):
        try:
            source_tenants = json.loads(source_tenants)
        except json.JSONDecodeError as e:
            raise ValueError(f"--source_tenants nao e um JSON valido: {e}") from e

    if not isinstance(source_tenants, list) or not source_tenants:
        raise ValueError(
            f"--source_tenants deve ser uma lista nao vazia; "
            f"recebido: {type(source_tenants).__name__}"
        )

    normalizados: List[dict] = []
    tenants_vistos: set = set()
    paths_vistos: set = set()

    for item in source_tenants:
        if not isinstance(item, dict):
            raise ValueError(f"Item de --source_tenants nao e um objeto: {item!r}")

        tenant = str(item.get("tenant") or "").strip()
        if not tenant:
            raise ValueError(f"Item de --source_tenants sem 'tenant': {item!r}")

        # `rstrip('/')`: sem isso o path de origem vira `s3a://bucket//pasta/tabela` e o Delta
        # nao encontra o transaction log.
        base_path = str(item.get("base_path") or "").strip().rstrip("/")
        if not base_path:
            raise ValueError(f"Tenant {tenant!r} sem 'base_path' em --source_tenants.")
        if not base_path.startswith("s3a://"):
            # `s3://` nao e lido pelo conector configurado na sessao: o erro apareceria como
            # "No FileSystem for scheme: s3" no meio do job, longe da causa.
            raise ValueError(
                f"'base_path' de {tenant!r} deve comecar com 's3a://': {base_path!r}"
            )

        if tenant in tenants_vistos:
            # Duas origens homonimas colidiriam na mesma particao `source` e uma sobrescreveria
            # a outra no merge.
            raise ValueError(f"Tenant {tenant!r} duplicado em --source_tenants.")
        if base_path in paths_vistos:
            # Dois tenants apontando para o mesmo bucket gerariam DOIS hk_business_id diferentes
            # para a MESMA linha fisica: duplicacao no destino, nao deduplicavel depois (os
            # hashes divergem).
            raise ValueError(f"'base_path' {base_path!r} repetido em --source_tenants.")

        tenants_vistos.add(tenant)
        paths_vistos.add(base_path)
        normalizados.append({"tenant": tenant, "base_path": base_path})

    return normalizados


@dataclass # Removido o frozen=True para permitir alterações
class PipelineConfig:
    """Parâmetros de runtime de UMA tabela.

    No modo multi-tabela esta instância é a CONFIG BASE (sem `table_name`/`primary_key`), e o
    TableRunner deriva uma cópia por tabela com `dataclasses.replace`. A dataclass é mutável e
    as camadas de baixo (repositories, transformers) leem os campos no `__init__` — por isso a
    base nunca deve ser mutada quando há threads em paralelo.
    """
    pipeline: str
    tenant_name: str
    filial_name: str
    # Opcionais: no modo multi-tabela a base nasce sem eles e o TableRunner os preenche.
    table_name: Optional[str] = None
    primary_key: List[str] = field(default_factory=list)
    is_merge_schema: bool = False
    colunas_zorder: str = False
    # Manutencao Delta (pipeline `delta_maintenance`). Ignorados pelos demais pipelines.
    delta_layer: str = "silver"
    retention_hours: int = 168
    # Janela de carga, fechada a esquerda e aberta a direita. Obrigatoria para as queries que
    # declaram JANELA_INICIO/JANELA_FIM; ignorada pelas demais.
    janela_inicio: Optional[str] = None
    janela_fim: Optional[str] = None
    # Colunas em que nulo invalida a linha. Vazio desliga a limpeza — e o default, para nao
    # mudar o comportamento de quem nao declara.
    colunas_obrigatorias: List[str] = field(default_factory=list)
    # Super tenant (pipeline `silver_super_tenant`). Ignorado pelos demais pipelines.
    # SOMENTE LEITURA: `dataclasses.replace` no TableRunner copia a REFERENCIA desta lista para
    # as N threads de tabela. Quem consome deve derivar uma copia local.
    source_tenants: List[dict] = field(default_factory=list)

    @classmethod
    def from_args(cls, args):
        """Factory para criar a config a partir dos argumentos do argparse"""
        is_merge = str(getattr(args, 'is_merge_schema', 'false')).lower() == "true"

        return cls(
            pipeline=args.pipeline,
            tenant_name=args.tenant_name,
            filial_name=args.filial_name,
            table_name=getattr(args, 'table_name', None),
            # `or []`, nao so o hasattr: no modo multi-tabela o atributo existe mas vale None,
            # e uma lista vazia e o que as camadas de baixo esperam.
            primary_key=getattr(args, 'primary_key', None) or [],
            is_merge_schema=is_merge,
            colunas_zorder=args.colunas_zorder,
            # getattr com default: mesmo padrao defensivo dos campos acima, para que a
            # dataclass continue construivel a partir de um Namespace sem estes args.
            delta_layer=getattr(args, 'delta_layer', None) or "silver",
            retention_hours=int(getattr(args, 'retention_hours', None) or 168),
            janela_inicio=getattr(args, 'janela_inicio', None),
            janela_fim=getattr(args, 'janela_fim', None),
            colunas_obrigatorias=getattr(args, 'colunas_obrigatorias', None) or [],
            source_tenants=normalize_source_tenants(getattr(args, 'source_tenants', None)),
        )
    
    def get(self, key, default=None):
        return getattr(self, key, default)
    
    def __getitem__(self, item):
        """Permite leitura: valor = config['chave']"""
        return getattr(self, item, None)

    def __setitem__(self, key, value):
        """Permite escrita: config['chave'] = valor (Resolve o erro atual)"""
        setattr(self, key, value)
    
    def to_dict(self):
        return {field.name: getattr(self, field.name) for field in fields(self)}