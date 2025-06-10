# Projeto de Análise Comparativa: Spark (MapReduce/RDD) vs PostgreSQL (SQL)

## Descrição

Este projeto foi desenvolvido com o objetivo de realizar uma análise comparativa de desempenho entre duas tecnologias de processamento de dados:

- **Apache Spark** (utilizando a API RDD para o paradigma MapReduce/Chave-Valor)
- **PostgreSQL** (utilizando SQL tradicional)

O foco é avaliar a performance e a eficiência de cada abordagem em um cenário de processamento de dados semi-estruturados (CSV), realizando consultas equivalentes e identificando os cenários onde cada tecnologia se destaca.

## Objetivos

- Comparar o desempenho de consultas de agregação, filtro e agrupamento entre Spark (MapReduce/RDD) e PostgreSQL (SQL).
- Avaliar a eficiência da carga de dados em ambos os sistemas.
- Identificar as características de cada tecnologia que impactam o desempenho em diferentes volumes de dados.
- Demonstrar a aplicação do paradigma MapReduce com Spark e consultas SQL com PostgreSQL.

## Tecnologias Utilizadas

- **Apache Spark:** Framework de processamento distribuído de dados (com foco na API RDD).
- **PostgreSQL:** Sistema de gerenciamento de banco de dados relacional.
- **Python:** Linguagem de programação utilizada para orquestrar o processamento e as consultas.
- **Pandas & NumPy:** Bibliotecas Python para manipulação e limpeza de dados.
- **Psycopg2-binary:** Adaptador PostgreSQL para Python.

## Estrutura do Projeto

```
ProjetoBigData/
├── Base de Dados/             # Contém os arquivos CSV brutos para processamento.
├── artifacts/                 # Diretório para armazenar resultados gerados (se houver).
├── config.py                  # Contém as configurações de conexão para PostgreSQL e Spark.
├── DataProcessor.py           # Script principal para processamento de UM ÚNICO arquivo CSV.
├── DataProcessor_2.py         # Script para processar MÚLTIPLOS arquivos CSV na pasta Base de Dados.
├── queries.py                 # Define as funções de consulta para Spark (MapReduce/RDD) e consultas SQL para PostgreSQL.
├── requirements.txt           # Lista as dependências Python do projeto.
├── README.md                  # Este arquivo com a descrição do projeto e instruções.
└── .git/                      # Diretório de controle de versão do Git.
```

## Dicionário de Dados (Base de Dados CSV)

Os arquivos CSV na pasta `Base de Dados/` seguem a seguinte estrutura:

| Coluna         | Tipo    | Tamanho | Nulo ou Não Nulo | Descrição dos Campos                                   |
| :------------- | :------ | :------ | :--------------- | :----------------------------------------------------- |
| `Tipo`         | varchar | 100     | NOT NULL         | Tipo do pedido                                         |
| `Orgao`        | varchar | 200     | NOT NULL         | Órgão responsável pelo atendimento                     |
| `DataCriacao`  | varchar | 20      | NOT NULL         | Data de criação do pedido                              |
| `Assunto`      | varchar | 100     | NOT NULL         | Nome do assunto do serviço                             |
| `Subdivisao`   | varchar | 100     | NOT NULL         | Nome da subdivisão do serviço                          |
| `Situacao`     | varchar | 100     | NOT NULL         | Situação do pedido                                     |
| `Logradouro`   | varchar | 200     | NOT NULL         | Endereço de atendimento do pedido                      |
| `Bairro`       | varchar | 100     | NULL             | Nome do bairro                                         |
| `Regional`     | varchar | 100     | NULL             | Nome da regional                                       |
| `Origem`       | varchar | 50      | NOT NULL         | Origem de entrada do pedido                            |
| `DataResposta` | varchar | 20      | NULL             | Data da resposta do órgão responsável pelo atendimento |

_Observação:_ Os campos `Bairro`, `Regional` e `DataResposta` podem conter valores nulos.

## Como Executar o Projeto

Siga os passos abaixo para configurar e executar o projeto:

### 1. Pré-requisitos

Certifique-se de ter os seguintes softwares instalados e configurados:

- **Python 3.x:** (Recomendado 3.8 a 3.10 para melhor compatibilidade com Spark)
- **Java Development Kit (JDK) 8 ou superior:** (Necessário para o Spark)
- **Apache Spark:** Pode ser uma instalação local ou via Docker.
- **PostgreSQL:** Servidor de banco de dados rodando (pode ser local ou via Docker).

### 2. Instalação das Dependências Python

É altamente recomendável criar um ambiente virtual (venv) para gerenciar as dependências:

```bash
python -m venv venv
.\venv\Scripts\activate   # No Windows
source venv/bin/activate # No Linux/macOS
```

Com o ambiente virtual ativado, instale as bibliotecas Python necessárias:

```bash
pip install -r requirements.txt
```

O `requirements.txt` deve conter:

```
pyspark==3.5.0
psycopg2-binary==2.9.9
pandas==2.1.4
numpy
```

### 3. Configuração do PostgreSQL

Certifique-se de que seu servidor PostgreSQL está em execução. As credenciais e configurações de conexão são definidas no arquivo `config.py`:

```python
# config.py
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'BigData',
    'user': 'postgres',
    'password': 'your_password' # Altere conforme sua senha do PostgreSQL
}
```

Crie o banco de dados `BigData` se ainda não existir.

### 4. Configuração do Spark

O projeto configura a sessão Spark internamente. Certifique-se de que o Python está acessível ao Spark. No arquivo `DataProcessor.py` (e `DataProcessor_2.py`), as seguintes linhas garantem que o Spark use o Python correto:

```python
# Dentro da classe DataProcessor no método setup_spark
import os
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
```

_Observação:_ Em alguns ambientes Windows, pode ser necessário desativar o alias do Python da Microsoft Store (`Configurações > Aplicativos > Aliases de execução do aplicativo`).

### 5. Preparação dos Dados

Coloque os arquivos CSV da base de dados na pasta `Base de Dados/` dentro do diretório raiz do projeto.

### 6. Executando os Scripts de Processamento

Você pode executar dois scripts principais:

#### a) Processamento de um único arquivo (para testes e depuração inicial)

Execute o script `DataProcessor.py`. Ele irá processar o arquivo CSV especificado internamente (atualmente `2025-02-23_156_-_Base_de_Dados.csv`), carregar no PostgreSQL, e executar as consultas comparando o desempenho.

```bash
python DataProcessor.py
```

#### b) Processamento de múltiplos arquivos (para avaliação em conjunto)

Execute o script `DataProcessor_2.py`. Ele irá iterar sobre _todos_ os arquivos CSV na pasta `Base de Dados/`, carregando-os no PostgreSQL e calculando o tempo médio das consultas Spark.

```bash
python DataProcessor_2.py
```

### 7. Análise dos Resultados

Os scripts imprimirão os tempos de execução diretamente no console, apresentando a comparação entre Spark (MapReduce/RDD) e PostgreSQL (SQL) para cada consulta.

## Resultados e Conclusão

Com base nos testes realizados com o volume de dados atual (arquivos CSV de pequeno a médio porte, como 24MB):

**Desempenho:**

Os resultados demonstram consistentemente que o **PostgreSQL (com consultas SQL) supera o Apache Spark (com a API RDD para MapReduce) em ordens de magnitude**. Em cenários como agregação simples, filtragem complexa e cálculo de médias, o PostgreSQL foi dezenas a centenas de vezes mais rápido que o Spark.

**Análise:**

- **PostgreSQL (SQL):** É altamente otimizado para cargas de trabalho OLAP em dados estruturados que podem ser processados eficientemente em um único servidor. Seu otimizador de consultas e mecanismos internos de indexação permitem execuções rápidas e eficientes para as operações relacionais padrão.

- **Apache Spark (MapReduce/RDD):** Embora seja uma ferramenta poderosa para Big Data e processamento distribuído, introduz uma sobrecarga significativa para volumes de dados menores. O custo de inicialização da JVM, a orquestração de tarefas e a comunicação entre o driver e os workers (mesmo em modo local) superam os benefícios de desempenho para conjuntos de dados que cabem na memória ou em um único servidor. O paradigma MapReduce/RDD, embora flexível para lógica programática complexa, não se beneficia das otimizações de consultas relacionais que são intrínsecas ao Spark SQL e à API DataFrame, o que pode explicar parte da diferença de desempenho.

**Conclusão:**

Para o volume de dados e o tipo de consultas exploradas neste projeto, o **PostgreSQL é a escolha mais eficiente e performática**. O Spark (e o MapReduce/RDD) é mais adequado para cenários de **Big Data** (terabytes/petabytes), onde a necessidade de escalabilidade horizontal e processamento distribuído justifica a sobrecarga e a complexidade de sua arquitetura. Para dados que se encaixam em um único servidor, um banco de dados relacional tradicional como o PostgreSQL ainda é a solução mais robusta e eficiente para consultas analíticas.

Este projeto ilustra claramente os trade-offs entre as tecnologias e seus respectivos casos de uso ideais.

## Contribuição

Este é um projeto acadêmico desenvolvido como parte da disciplina de Banco de Dados 3.

## Licença

Este projeto está sob a licença MIT.
