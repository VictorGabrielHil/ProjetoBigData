from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, avg, to_date, count, desc
from datetime import datetime

class Queries:
    @staticmethod
    def get_spark_queries():
        """Retorna as funções de consulta usando RDDs do Spark"""
        return {
            'origem': lambda df: (
                df.rdd
                .map(lambda row: (row['Origem'], 1))
                .reduceByKey(lambda a, b: a + b)
                .sortBy(lambda x: x[1], ascending=False)
                .toDF(["Origem", "total"])
            ),
            
            'bairros': lambda df: (
                df.rdd
                .filter(lambda row: row['Assunto'] and "Trânsito" in row['Assunto'])
                .map(lambda row: (row['Bairro'], 1))
                .reduceByKey(lambda a, b: a + b)
                .sortBy(lambda x: x[1], ascending=False)
                .take(10)
            ),
            
            'tempo_atendimento': lambda df: (
                df.rdd
                .filter(lambda row: row['DataCriacao'] and row['DataResposta'])
                .map(lambda row: (
                    row['Orgao'],
                    (
                        (datetime.strptime(row['DataResposta'], "%d/%m/%Y") -
                         datetime.strptime(row['DataCriacao'], "%d/%m/%Y")).days
                    )
                ))
                .groupByKey()
                .mapValues(lambda days: sum(days) / len(days) if len(days) > 0 else None)
                .sortBy(lambda x: x[1] if x[1] is not None else -1, ascending=False)
                .toDF(["Orgao", "tempo_medio_dias"])
            )
        }

    @staticmethod
    def get_postgres_queries():
        """Retorna as consultas SQL para PostgreSQL"""
        return {
            'origem': """
                SELECT Origem, COUNT(*) as total
                FROM requisicoes
                GROUP BY Origem
                ORDER BY total DESC;
            """,
            
            'bairros': """
                SELECT Bairro, COUNT(*) as total
                FROM requisicoes
                WHERE Assunto LIKE '%Trânsito%'
                GROUP BY Bairro
                ORDER BY total DESC
                LIMIT 10;
            """,
            
            'tempo_atendimento': """
                SELECT 
                    Orgao,
                    AVG(DATE_PART('day', DataResposta::timestamp - DataCriacao::timestamp)) as tempo_medio_dias
                FROM requisicoes
                WHERE DataResposta IS NOT NULL AND DataCriacao IS NOT NULL
                GROUP BY Orgao
                ORDER BY tempo_medio_dias DESC;
            """
        } 