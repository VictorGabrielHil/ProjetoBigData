-- Teste 1: Agregação Simples
-- Objetivo: Contar o número total de requisições por origem (Telefone, Mobile, etc.).
-- O que testa: A eficiência da ferramenta em ler a tabela inteira e realizar uma operação de GROUP BY.

SELECT origem, COUNT(*) AS total
FROM requisicoes
GROUP BY origem
ORDER BY total DESC;

-- Teste 2: Filtro e Agrupamento
-- Objetivo: Encontrar os 10 bairros mais frequentes para requisições sobre "Trânsito".
-- O que testa: A capacidade da ferramenta em aplicar filtros e realizar agrupamentos complexos.

SELECT bairro, COUNT(*) AS total
FROM requisicoes
WHERE assunto = 'Trânsito'
GROUP BY bairro
ORDER BY total DESC
LIMIT 10;


-- Teste 3: Agrupamento e Cálculo de Média
-- Objetivo: Calcular o tempo médio de resposta (em dias) para cada órgão.
-- O que testa: A capacidade da ferramenta em realizar cálculos complexos e agrupar dados.

SELECT
    orgao,
    AVG(DataResposta- DataCriacao) AS tempo_medio_em_dias
FROM requisicoes
WHERE DataResposta IS NOT NULL AND DataCriacao IS NOT NULL
GROUP BY Orgao
ORDER BY tempo_medio_em_dias DESC;
