from pyspark.sql.utils import AnalysisException

class SparkUtils:
  
  @staticmethod
  def path_exists(spark, path_str):
      """
      Verifica se um caminho (diretório ou arquivo) existe.

      Args:
          spark (SparkSession): A sessão Spark ativa.
          path_str (str): O caminho completo (ex: "s3a://meu-bucket/pasta-teste").

      Returns:
          bool: True se o caminho existe, False caso contrário.
      """      
      try:
        df = spark.read.load(path_str).limit(1)
        df.collect()
        return True
      except AnalysisException as e:
        return False
      except Exception as e:
        return False