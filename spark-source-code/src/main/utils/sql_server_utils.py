import pyodbc

class ConnectionSQLServer:
  """
  Classe para conexão com o SQL Server para execução de scripts.
  Implementa o protocolo de context manager para uso com 'with'.
  """
  def __init__(self, config: dict):

      self.user = config["jdbc_username"]
      self.password = config["jdbc_password"]
      self.server = config["jdbc_hostname"]
      self.data_base = config["jdbc_database"]
      self.connection = None  # A conexão será armazenada aqui

  def __enter__(self):
      """
      Método chamado ao entrar no bloco 'with'.
      Abre a conexão com o banco de dados.
      """
      self.connection = pyodbc.connect(f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                                        f"SERVER={self.server};DATABASE={self.data_base};"
                                        f"UID={self.user};PWD={self.password};TrustServerCertificate=yes")
      print(f'Conexão com o banco {self.server} realizada com sucesso!')
      return self  # Retorna a instância da própria classe

  def __exit__(self, exc_type, exc_val, exc_tb):
      """
      Método chamado ao sair do bloco 'with'.
      Fecha a conexão com o banco de dados.
      """
      if self.connection:
          self.connection.close()
          print(f'Conexão com o banco de dados {self.server} fechada.')
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
        cursor.commit() 
    except Exception as error_query:
      print(f'Erro ao executar a consulta no banco {self.server}: {error_query}')
      raise Exception(error_query)

class GenerateSqlServerQueryUtils:
  
  @staticmethod
  def get_type_sql(key):
    mapping = {
      "StringType": "NVARCHAR(max)",
      "IntegerType":    "INT",
      "LongType":       "BIGINT",
      "FloatType":      "FLOAT",
      "DoubleType":     "FLOAT",
      "BooleanType":    "BIT",
      "BinaryType":     "VARBINARY(MAX)",
      "TimestampType":  "DATETIME",
      "DateType":       "DATE",
      "DecimalType":    "DECIMAL(18,4)"
    }
    return mapping.get(key, "VARCHAR(8000)")

  @staticmethod
  def generate_sql_create_temp_table(df, table_name):
    cols = []
    for field in df.schema.fields:
        col_name = field.name
        sql_type = GenerateSqlServerQueryUtils.get_type_sql(type(field.dataType).__name__)
        cols.append(f"{col_name} {sql_type}")
    cols_sql = ", ".join(cols)
    return f"CREATE TABLE {table_name} ( {cols_sql} );"

  @staticmethod
  def get_global_merge_proc(temp_table_name, table_name, chave_pk):
    return f"EXECUTE dbo.sp_Merge_Temp_Global {temp_table_name},dbo,{table_name},{chave_pk}"

  @staticmethod
  def get_drop_temp_table(temp_table_name):
    return f"drop table if exists {temp_table_name}"

  @staticmethod
  def get_temp_table_name(table_name):
    return f"##{table_name}"
    