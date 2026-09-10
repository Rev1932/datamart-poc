import psycopg2

class ConnectionPostgres:
  """
  Classe para conexão com o PostgreSQL para execução de scripts.
  Implementa o protocolo de context manager para uso com 'with'.
  """
  def __init__(self, config: dict):

      self.host = config["host"]
      self.port = config.get("port", 5432)
      self.database = config["database"]
      self.user = config["user"]
      self.password = config["password"]
      self.connection = None  # A conexão será armazenada aqui

  def __enter__(self):
      """
      Método chamado ao entrar no bloco 'with'.
      Abre a conexão com o banco de dados.
      """
      self.connection = psycopg2.connect(
          host=self.host,
          port=self.port,
          dbname=self.database,
          user=self.user,
          password=self.password
      )
      print(f'Conexão com o banco {self.host}/{self.database} realizada com sucesso!')
      return self  # Retorna a instância da própria classe

  def __exit__(self, exc_type, exc_val, exc_tb):
      """
      Método chamado ao sair do bloco 'with'.
      Fecha a conexão com o banco de dados.
      """
      if self.connection:
          self.connection.close()
          print(f'Conexão com o banco de dados {self.host}/{self.database} fechada.')
      return False

  def execution_query(self, query):
    """
    Executa uma consulta SQL usando a conexão aberta.
    """
    if self.connection is None:
      raise Exception("Conexão com o banco de dados não está aberta. Use a classe com 'with'.")
    try:
      with self.connection.cursor() as cursor:
        cursor.execute(query)
      self.connection.commit()
    except Exception as error_query:
      self.connection.rollback()
      print(f'Erro ao executar a consulta no banco {self.host}/{self.database}: {error_query}')
      raise Exception(error_query)

class GeneratePostgresQueryUtils:

  @staticmethod
  def get_type_sql(field):
    # DecimalType preserva precisão/escala originais; os demais mapeiam pelo nome do tipo
    type_name = type(field.dataType).__name__
    if type_name == "DecimalType":
      return f"NUMERIC({field.dataType.precision},{field.dataType.scale})"
    mapping = {
      "StringType":       "TEXT",
      "IntegerType":      "INTEGER",
      "ShortType":        "SMALLINT",
      "LongType":         "BIGINT",
      "FloatType":        "REAL",
      "DoubleType":       "DOUBLE PRECISION",
      "BooleanType":      "BOOLEAN",
      "BinaryType":       "BYTEA",
      "TimestampType":    "TIMESTAMP",
      "TimestampNTZType": "TIMESTAMP",
      "DateType":         "DATE"
    }
    return mapping.get(type_name, "TEXT")

  @staticmethod
  def get_qualified_name(schema, table_name):
    return f'"{schema}"."{table_name}"'

  @staticmethod
  def get_staging_table_name(table_name):
    return f"stg_{table_name}"

  @staticmethod
  def generate_sql_create_table(df, qualified_table, unique_key=None, unlogged=False):
    cols = []
    for field in df.schema.fields:
        sql_type = GeneratePostgresQueryUtils.get_type_sql(field)
        cols.append(f'"{field.name}" {sql_type}')
    if unique_key:
        cols.append(f'UNIQUE ("{unique_key}")')
    cols_sql = ", ".join(cols)
    # UNLOGGED dispensa WAL na staging, acelerando a carga em lote
    table_type = "UNLOGGED TABLE" if unlogged else "TABLE"
    return f"CREATE {table_type} IF NOT EXISTS {qualified_table} ( {cols_sql} );"

  @staticmethod
  def generate_sql_merge(df, qualified_target, qualified_staging, unique_key):
    cols_sql = ", ".join(f'"{c}"' for c in df.columns)
    set_sql = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != unique_key)
    return (
      f"INSERT INTO {qualified_target} ({cols_sql}) "
      f"SELECT {cols_sql} FROM {qualified_staging} "
      f'ON CONFLICT ("{unique_key}") DO UPDATE SET {set_sql};'
    )

  @staticmethod
  def get_drop_table(qualified_table):
    return f"DROP TABLE IF EXISTS {qualified_table};"
