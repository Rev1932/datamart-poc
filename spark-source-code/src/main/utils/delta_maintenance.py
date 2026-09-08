import traceback
from delta.tables import DeltaTable

class DeltaMaintenance():

    def __init__(self, spark, output_path):
        self.spark = spark
        self.output_path = output_path
        self.delta_table= DeltaTable.forPath(self.spark, self.output_path)
    
    def run_optimize(self, z_order_columns=None):
        """
            Executa OPTIMIZE a nivel de tabela
        """

        try:
            if z_order_columns is None:
                print("Executando OPTIMIZE generico")
                self.delta_table.optimize().executeCompaction()
            else:
                print(f"Executando OPTIMIZE Z-ORDER nas colunas: {z_order_columns}")
                self.delta_table.optimize().executeZOrderBy(*z_order_columns)
            print("Optimize feito!")
        except Exception as e:
            print(f"Erro ao otimizar tabela {self.output_path}")
        
    def run_vacuum(self, retention_period=168):
        """
        Executa VACUUM a nível de tabela
        """
        print(f"Iniciando VACUUM para a tabela: {self.output_path}")
        
        try:
            self.delta_table.vacuum(retention_period)
            print(f"VACUUM concluído com sucesso para a tabela {self.output_path}!")

        except Exception as e:
            print(f"ERRO de VACUUM ao processar a tabela {self.output_path}.")
            traceback.print_exc()


