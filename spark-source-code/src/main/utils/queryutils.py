import os


class QueryUtils:

    @staticmethod
    def build_query(queries_dir: str, config_manager, table_name: str) -> str:
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

        return query