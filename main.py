import time
import psycopg2
from pyspark.sql import SparkSession
from config import POSTGRES_CONFIG, SPARK_CONFIG, GENERAL_CONFIG
from queries import Queries

class PerformanceTest:
    def __init__(self):
        self.spark = None
        self.pg_conn = None
        self.pg_cursor = None
        self.results = {
            'spark': {},
            'postgres': {}
        }

    def setup_spark(self):
        try:
            self.spark = SparkSession.builder \
                .appName(SPARK_CONFIG['app_name']) \
                .master(SPARK_CONFIG['master']) \
                .getOrCreate()
            print("Sessão Spark criada com sucesso!")
        except Exception as e:
            print(f"Erro ao criar sessão Spark: {e}")
            raise

    def setup_postgres(self):
        try:
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            self.pg_cursor = self.pg_conn.cursor()
            print("Conexão PostgreSQL estabelecida com sucesso!")
        except Exception as e:
            print(f"Erro ao conectar ao PostgreSQL: {e}")
            raise

    def run_spark_test(self, query_name, query_func):
        print(f"\nExecutando teste Spark: {query_name}")
        start_time = time.time()
        
        df = self.spark.read.csv(
            SPARK_CONFIG['data_path'],
            header=GENERAL_CONFIG['csv_header'],
            sep=GENERAL_CONFIG['csv_separator'],
            inferSchema=GENERAL_CONFIG['infer_schema']
        )
        
        result = query_func(df)
        result.show()
        
        end_time = time.time()
        execution_time = end_time - start_time
        self.results['spark'][query_name] = execution_time
        print(f"Tempo de execução Spark: {execution_time:.4f} segundos")

    def run_postgres_test(self, query_name, query):
        print(f"\nExecutando teste PostgreSQL: {query_name}")
        start_time = time.time()
        
        self.pg_cursor.execute(query)
        results = self.pg_cursor.fetchall()
        
        # Exibir resultados
        for row in results:
            print(row)
        
        end_time = time.time()
        execution_time = end_time - start_time
        self.results['postgres'][query_name] = execution_time
        print(f"Tempo de execução PostgreSQL: {execution_time:.4f} segundos")

    def run_all_tests(self):
        try:
            self.setup_spark()
            self.setup_postgres()

            spark_queries = Queries.get_spark_queries()
            postgres_queries = Queries.get_postgres_queries()

            for query_name in spark_queries.keys():
                self.run_spark_test(query_name, spark_queries[query_name])
                self.run_postgres_test(query_name, postgres_queries[query_name])

            self.print_comparison()

        finally:
            self.cleanup()

    def print_comparison(self):
        print("\n" + "="*50)
        print("RESULTADOS DA COMPARAÇÃO")
        print("="*50)
        
        for query_name in self.results['spark'].keys():
            print(f"\nConsulta: {query_name}")
            print(f"Spark: {self.results['spark'][query_name]:.4f} segundos")
            print(f"PostgreSQL: {self.results['postgres'][query_name]:.4f} segundos")
            
            diff = self.results['spark'][query_name] - self.results['postgres'][query_name]
            faster = "PostgreSQL" if diff > 0 else "Spark"
            print(f"Diferença: {abs(diff):.4f} segundos (mais rápido: {faster})")

    def cleanup(self):
        if self.spark:
            self.spark.stop()
            print("\nSessão Spark finalizada.")
        
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()
            print("Conexão PostgreSQL finalizada.")

if __name__ == "__main__":
    test = PerformanceTest()
    test.run_all_tests() 