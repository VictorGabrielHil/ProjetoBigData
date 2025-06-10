# Configurações do PostgreSQL
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'BigData',
    'user': 'postgres',
    'password': ''
}

# Configurações do Spark
SPARK_CONFIG = {
    'app_name': 'ProjetoBigData',
    'master': 'local[*]',
    'data_path': 'Base de Dados/2025-06-07_156_-_Base_de_Dados.csv'
}

# Configurações gerais
GENERAL_CONFIG = {
    'csv_separator': ';',
    'csv_header': True,
    'infer_schema': False
} 