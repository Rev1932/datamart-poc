import os
import re


_JANELA_ORFA = re.compile(r"JANELA_(?:INICIO|FIM)")


class QueryUtils:

    @staticmethod
    def build_query(queries_dir: str, config_manager, table_name: str, runtime_parameters=None) -> str:
        sql_file = os.path.join(queries_dir, f"{table_name}.sql")

        if not os.path.exists(sql_file):
            raise ValueError(f"Arquivo SQL não encontrado para '{table_name}': {sql_file}")

        with open(sql_file, "r", encoding="utf-8") as f:
            query = f.read()

        replacements = {
            "MINIO_BASE_PATH": config_manager.get("minio.base_path"),
            "S3_PATH_SILVER": config_manager.get("trino.schema_folder_silver"),
            "S3_PATH_GOLD": config_manager.get("trino.schema_folder_gold"),
        }

        for key, value in replacements.items():
            if value is None:
                raise ValueError(f"Config não encontrada para {key}")
            query = query.replace(key, value)

        janela = {
            "JANELA_INICIO": runtime_parameters.get("janela_inicio") if runtime_parameters else None,
            "JANELA_FIM": runtime_parameters.get("janela_fim") if runtime_parameters else None,
        }
        for key, value in janela.items():
            if value is not None:
                query = query.replace(key, str(value))

        # Sem valor a query cairia num literal invalido ou, pior, num predicado sempre
        # verdadeiro: a carga leria a tabela inteira e a troca de particao apagaria o resto.
        orfa = _JANELA_ORFA.search(query)
        if orfa is not None:
            raise ValueError(
                f"{table_name}.sql exige {orfa.group()}, mas o parametro de runtime nao foi informado. "
                "Passe --janela_inicio e --janela_fim."
            )

        return query
