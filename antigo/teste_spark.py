# Importa as bibliotecas necessárias do PySpark
from pyspark.sql import SparkSession
import time

print("Iniciando o teste do ambiente Spark...")

# PASSO 1: Construir e iniciar a Sessão Spark
# .appName() -> Dá um nome para a sua aplicação. Útil para monitoramento.
# .getOrCreate() -> Cria uma nova sessão ou pega uma que já exista.
try:
    spark = SparkSession.builder \
        .appName("TesteDeAmbiente") \
        .master("local[*]") \
        .getOrCreate() # Adicionado .master("local[*]") para garantir execução local

    print("Sessão Spark criada com sucesso!")
    
    # PASSO 2: Carregar seu arquivo CSV
    # O Spark vai ler o arquivo e inferir o esquema (tipos das colunas)
    print("Tentando carregar o arquivo CSV...")
    start_time = time.time() # Medir o tempo de leitura
    
    df = spark.read.csv(
        '2025-06-07_156_-_Base_de_Dados.csv',
        header=True,       # A primeira linha é o cabeçalho
        sep=';',           # O delimitador é ponto e vírgula
        inferSchema=True   # Pede ao Spark para adivinhar o tipo de cada coluna
    )
    
    # PASSO 3: Executar uma ação para testar
    # O Spark é "preguiçoso" (lazy), ele só executa quando uma ação é chamada.
    # .count() é uma ação que força a leitura e contagem de todas as linhas.
    total_linhas = df.count()
    end_time = time.time()
    
    print(f"Arquivo CSV carregado em {end_time - start_time:.2f} segundos.")
    print(f"O arquivo tem {total_linhas} linhas.")
    
    print("\nMostrando as 5 primeiras linhas do arquivo:")
    df.show(5) # .show() é outra ação, que mostra os dados

except Exception as e:
    print("\n--- OCORREU UM ERRO! ---")
    print(e)
    print("\nVerifique a seção 'Possíveis Problemas' na minha resposta.")

finally:
    # PASSO 4: Parar a sessão para liberar os recursos
    if 'spark' in locals():
        spark.stop()
        print("\nSessão Spark finalizada.")
        