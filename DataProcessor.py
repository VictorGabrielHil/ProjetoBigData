import time
import psycopg2
import pandas as pd
import numpy as np
import os
from pyspark.sql import SparkSession
from config import POSTGRES_CONFIG, SPARK_CONFIG, GENERAL_CONFIG
from queries import Queries

class DataProcessor:
    def __init__(self):
        self.spark = None
        self.pg_conn = None
        self.pg_cursor = None
        self.results = {
            'spark': {},
            'postgres': {}
        }
        # Usando um arquivo específico para o teste
        self.csv_file = "2025-02-23_156_-_Base_de_Dados.csv"

    def setup_postgres(self):
        """Configura conexão com PostgreSQL e cria/recria tabela"""
        try:
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            self.pg_cursor = self.pg_conn.cursor()
            print("Conexão PostgreSQL estabelecida com sucesso!")

            # Verifica se a tabela existe e a recria
            self.pg_cursor.execute("""
                DROP TABLE IF EXISTS requisicoes;
                CREATE TABLE requisicoes (
                    Tipo VARCHAR(100) NOT NULL,
                    Orgao VARCHAR(200) NOT NULL,
                    DataCriacao VARCHAR(20) NOT NULL,
                    Assunto VARCHAR(100) NOT NULL,
                    Subdivisao VARCHAR(100) NOT NULL,
                    Situacao VARCHAR(100) NOT NULL,
                    Logradouro VARCHAR(200) NOT NULL,
                    Bairro VARCHAR(100),
                    Regional VARCHAR(100),
                    Origem VARCHAR(50) NOT NULL,
                    DataResposta VARCHAR(20)
                );
            """)
            self.pg_conn.commit()
            print("Tabela 'requisicoes' recriada com sucesso!")

        except Exception as e:
            print(f"Erro ao configurar PostgreSQL: {e}")
            raise

    def setup_spark(self):
        """Configura sessão Spark"""
        try:
            # Garante que o Spark use o Python correto
            os.environ["PYSPARK_PYTHON"] = "python"
            os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
            self.spark = SparkSession.builder \
                .appName(SPARK_CONFIG['app_name']) \
                .master(SPARK_CONFIG['master']) \
                .getOrCreate()
            print("Sessão Spark criada com sucesso!")
        except Exception as e:
            print(f"Erro ao criar sessão Spark: {e}")
            raise

    def _clean_data(self, df):
        """Limpa os dados, substituindo NaN por NULL"""
        df = df.replace({np.nan: None})
        return df

    def load_data_postgres(self):
        """Carrega dados no PostgreSQL"""
        print("\nCarregando dados no PostgreSQL...")
        start_time = time.time()

        file_path = os.path.join("Base de Dados", self.csv_file)
        print(f"Processando arquivo: {self.csv_file}")
        
        # Lê o CSV com pandas
        df = pd.read_csv(file_path, sep=GENERAL_CONFIG['csv_separator'])
        
        # Limpa os dados
        df = self._clean_data(df)
        
        # Insere os dados no PostgreSQL
        for _, row in df.iterrows():
            self.pg_cursor.execute("""
                INSERT INTO requisicoes (
                    Tipo, Orgao, DataCriacao, Assunto, Subdivisao, Situacao, Logradouro, Bairro, Regional, Origem, DataResposta
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['Tipo'], row['Orgao'], row['DataCriacao'], row['Assunto'], row['Subdivisao'],
                row['Situacao'], row['Logradouro'], row['Bairro'], row.get('Regional'), row['Origem'], row.get('DataResposta')
            ))
        
        self.pg_conn.commit()

        end_time = time.time()
        print(f"Tempo total de carregamento no PostgreSQL: {end_time - start_time:.4f} segundos")

    def run_postgres_queries(self):
        """Executa consultas no PostgreSQL"""
        print("\nExecutando consultas no PostgreSQL...")
        queries = Queries.get_postgres_queries()

        for query_name, query in queries.items():
            start_time = time.time()
            self.pg_cursor.execute(query)
            self.pg_cursor.fetchall()  # Executa a query mas não mostra resultados
            end_time = time.time()
            
            execution_time = end_time - start_time
            self.results['postgres'][query_name] = execution_time
            print(f"Tempo de execução PostgreSQL: {execution_time:.4f} segundos")

    def run_spark_queries(self):
        """Executa consultas no Spark"""
        print("\nExecutando consultas no Spark...")
        queries = Queries.get_spark_queries()

        file_path = os.path.join("Base de Dados", self.csv_file)
        print(f"\nProcessando arquivo: {self.csv_file}")
        
        # Carrega dados no Spark
        df = self.spark.read.csv(
            file_path,
            header=GENERAL_CONFIG['csv_header'],
            sep=GENERAL_CONFIG['csv_separator'],
            inferSchema=GENERAL_CONFIG['infer_schema']
        )

        # Executa consultas
        for query_name, query_func in queries.items():
            start_time = time.time()
            query_func(df)
            end_time = time.time()
            
            execution_time = end_time - start_time
            self.results['spark'][query_name] = execution_time
            print(f"Tempo de execução Spark: {execution_time:.4f} segundos")

    def print_comparison(self):
        """Exibe comparação dos resultados"""
        print("\n" + "="*80)
        print("RESULTADOS DA COMPARAÇÃO DE PERFORMANCE")
        print("="*80)
        
        # Descrição das consultas
        query_descriptions = {
            'origem': """
1º Consulta: Agregação Simples
Objetivo: Contar o número total de requisições por origem (Telefone, Mobile, etc.).
O que testa: A eficiência da ferramenta em ler os dados e realizar uma operação de agrupamento.
""",
            'bairros': """
2º Consulta: Filtro e Agrupamento
Objetivo: Encontrar os 10 bairros mais frequentes para requisições sobre "Trânsito".
O que testa: A capacidade da ferramenta em aplicar filtros e realizar agrupamentos complexos.
""",
            'tempo_atendimento': """
3º Consulta: Agrupamento e Cálculo de Média
Objetivo: Calcular o tempo médio de resposta (em dias) para cada órgão.
O que testa: A capacidade da ferramenta em realizar cálculos complexos e agrupar dados.
"""
        }
        
        for query_name in self.results['spark'].keys():
            print(query_descriptions[query_name])
            print(f"Spark: {self.results['spark'][query_name]:.4f} segundos")
            print(f"PostgreSQL: {self.results['postgres'][query_name]:.4f} segundos")
            
            diff = self.results['spark'][query_name] - self.results['postgres'][query_name]
            faster = "PostgreSQL" if diff > 0 else "Spark"
            print(f"Diferença: {abs(diff):.4f} segundos (mais rápido: {faster})")
            print("-" * 80)

    def cleanup(self):
        """Limpa recursos"""
        if self.spark:
            self.spark.stop()
            print("\nSessão Spark finalizada.")
        
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()
            print("Conexão PostgreSQL finalizada.")

    def run(self):
        """Executa todo o processo"""
        try:
            # Configura PostgreSQL e carrega dados
            self.setup_postgres()
            self.load_data_postgres()
            
            # Executa consultas PostgreSQL
            self.run_postgres_queries()
            
            # Configura Spark e executa consultas
            self.setup_spark()
            self.run_spark_queries()
            
            # Exibe resultados
            self.print_comparison()

        finally:
            self.cleanup()

if __name__ == "__main__":
    processor = DataProcessor()
    processor.run() 