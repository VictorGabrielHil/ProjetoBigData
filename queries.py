from pyspark.sql.functions import col, datediff, avg, to_date, count, desc

class Queries:
    @staticmethod
    def get_spark_queries():
        return {
            'origem': lambda df: df.groupBy("Orgao") \
                                 .agg(count("*").alias("total")) \
                                 .orderBy(col("total").desc()),
            
            'bairros': lambda df: df.groupBy("Bairro") \
                                   .agg(count("*").alias("total")) \
                                   .orderBy(col("total").desc()),
            
            'tempo_atendimento': lambda df: df.filter(col("DataResposta").isNotNull()) \
                                            .filter(col("DataCriacao").isNotNull()) \
                                            .withColumn("DataRespostaConvertida", to_date(col("DataResposta"), "dd/MM/yyyy")) \
                                            .withColumn("DataCriacaoConvertida", to_date(col("DataCriacao"), "dd/MM/yyyy")) \
                                            .withColumn("TempoRespostaDias", datediff(col("DataRespostaConvertida"), col("DataCriacaoConvertida"))) \
                                            .groupBy("Orgao") \
                                            .agg(avg("TempoRespostaDias").alias("tempo_medio_dias")) \
                                            .orderBy(col("tempo_medio_dias").desc())
        }

    @staticmethod
    def get_postgres_queries():
        return {
            'origem': """
                SELECT Orgao, COUNT(*) as total
                FROM requisicoes
                GROUP BY Orgao
                ORDER BY total DESC;
            """,
            
            'bairros': """
                SELECT Bairro, COUNT(*) as total
                FROM requisicoes
                GROUP BY Bairro
                ORDER BY total DESC;
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