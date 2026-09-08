from dataclasses import dataclass, fields
from typing import List, Optional

@dataclass # Removido o frozen=True para permitir alterações
class PipelineConfig:
    pipeline_type: str
    config_name: str
    table_name: str
    topic: str
    is_stream_process: bool
    trigger_value: Optional[str]
    chave_pk: List[str]
    is_merge_schema: bool = False

    @classmethod
    def from_args(cls, args):
        """Factory para criar a config a partir dos argumentos do argparse"""
        is_stream = str(getattr(args, 'spark_is_stream_process', 'false')).lower() == "true"
        is_merge = str(getattr(args, 'is_merge_schema', 'false')).lower() == "true"

        return cls(
            pipeline_type=args.pipeline,
            config_name=args.config_name,
            table_name=args.table_name,
            topic=f"{args.config_name}_{args.table_name}",
            is_stream_process=is_stream,
            trigger_value=args.spark_trigger_value if is_stream else None,
            chave_pk=args.chave_pk if hasattr(args, 'chave_pk') else [],
            is_merge_schema=is_merge
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