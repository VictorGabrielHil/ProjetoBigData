import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# --- CONFIGURE AQUI ---
# Coloque o caminho absoluto para o seu arquivo CSV.
caminho_csv = r"C:\Estudo\Banco de Dados 3\Projeto Big Data\2025-06-07_156_-_Base_de_Dados.csv"

# Suas credenciais de conexão com o PostgreSQL
db_params = {
    "host": "localhost",
    "user": "postgres",
    "password": ""
}
db_name = "BigData"
# --------------------


def criar_database_se_nao_existir():
    """Conecta ao servidor PostgreSQL e cria o banco de dados se ele não existir."""
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if not cur.fetchone():
                print(f"Banco de dados '{db_name}' não encontrado. Criando...")
                cur.execute(f"CREATE DATABASE \"{db_name}\"")
                print("Banco de dados criado com sucesso.")
            else:
                print(f"Banco de dados '{db_name}' já existe.")
    except psycopg2.OperationalError as e:
        print(f"Erro de conexão: {e}")
        return False
    finally:
        if conn:
            conn.close()
    return True


def preparar_e_carregar_dados():
    """Conecta ao banco de dados, cria a tabela e carrega os dados do CSV."""
    conn = None
    params_com_db = db_params.copy()
    params_com_db['database'] = db_name
    
    # --- ALTERAÇÃO 1: Adicionada uma coluna "coluna_extra" para capturar o dado extra ---
    sql_create_table = """
    DROP TABLE IF EXISTS requisicoes;
    CREATE TABLE requisicoes (
        id SERIAL PRIMARY KEY,
        Tipo VARCHAR(100),
        Orgao VARCHAR(200),
        DataCriacao DATE,
        Assunto VARCHAR(100),
        Subdivisao VARCHAR(100),
        Situacao VARCHAR(100),
        Logradouro VARCHAR(200),
        Bairro VARCHAR(100),
        Regional VARCHAR(100),
        DataResposta DATE,
        Origem VARCHAR(50),
        coluna_extra TEXT
    );
    """
    
    # --- ALTERAÇÃO 2: Adicionado "coluna_extra" à lista de colunas do COPY ---
    sql_copy_columns = "requisicoes(Tipo, Orgao, DataCriacao, Assunto, Subdivisao, Situacao, Logradouro, Bairro, Regional, DataResposta, Origem, coluna_extra)"
    sql_copy_command = f"""
    COPY {sql_copy_columns}
    FROM STDIN
    DELIMITER ';'
    CSV HEADER;
    """

    try:
        print(f"\nConectando ao banco de dados '{db_name}'...")
        conn = psycopg2.connect(**params_com_db)
        with conn.cursor() as cur:
            print("Criando a tabela 'requisicoes' (versão corrigida)...")
            cur.execute(sql_create_table)
            print("Tabela criada com sucesso.")
            
            print(f"Carregando dados do arquivo: {caminho_csv}")
            with open(caminho_csv, 'r', encoding='utf-8') as f:
                cur.copy_expert(sql_copy_command, f)
            print("Dados carregados com sucesso!")
            
            cur.execute("SELECT COUNT(*) FROM requisicoes;")
            total_linhas = cur.fetchone()[0]
            print(f"\nVerificação: A tabela 'requisicoes' agora contém {total_linhas} linhas.")
            
        conn.commit()

    except FileNotFoundError:
        print(f"\n--- ERRO FATAL ---\nO arquivo não foi encontrado em: {caminho_csv}\nPor favor, verifique o caminho e tente novamente.")
    except Exception as e:
        print(f"\n--- OCORREU UM ERRO ---\nDetalhe do erro: {e}")
    finally:
        if conn:
            conn.close()
            print("\nConexão com o PostgreSQL fechada.")


# --- Execução Principal ---
if __name__ == "__main__":
    if not os.path.exists(caminho_csv):
         print(f"ERRO: O arquivo CSV não foi encontrado em '{caminho_csv}'. Verifique o caminho e o nome do arquivo.")
    else:
        if criar_database_se_nao_existir():
            preparar_e_carregar_dados()