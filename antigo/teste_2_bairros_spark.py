import time
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# --- 1. INICIALIZAÇÃO DA SESSÃO SPARK ---
try:
    spark = SparkSession.builder \
        .appName("TesteSimplesComTempo") \
        .master("local[*]") \
        .getOrCreate()

    print("Sessão Spark criada com sucesso!")

except Exception as e:
    print(f"Erro ao criar sessão Spark: {e}")
    exit(1)


# --- 2. PREPARAÇÃO DA CONSULTA (AINDA NÃO EXECUTA NADA) ---
# O Spark apenas cria um "plano" de como ler e processar os dados
df = spark.read.csv(
    '2025-06-07_156_-_Base_de_Dados.csv',
    header=True,
    sep=';',
    inferSchema=True
)

# --- 3. EXECUÇÃO E MEDIÇÃO DO TEMPO ---
print("\nIniciando a execução e a medição do tempo da consulta...")

start_time = time.time() # Inicia o cronômetro

# A ação .show() força a execução de todo o plano: ler o CSV e fazer a agregação.
df.filter(col("Assunto") == "Trânsito") \
  .groupBy("Bairro") \
  .count() \
  .orderBy(col("count").desc()) \
  .limit(10) \
  .show()

end_time = time.time() # Para o cronômetro

# --- 4. EXIBIÇÃO DO RESULTADO E FINALIZAÇÃO ---
print("-" * 50)
print(f"Tempo total de execução (Carga + Consulta): {end_time - start_time:.4f} segundos")
print("-" * 50)

spark.stop()
print("Sessão Spark finalizada.")