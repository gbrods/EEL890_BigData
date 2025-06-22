-- =======================================================================================
-- UFRJ – Universidade Federal do Rio de Janeiro
-- IM   – Instituto de Matemática
-- DMA  – Departamento de Matemática Aplicada
--
-- EEL890 - Big Data (turma SIGA 16338)
--
-- Avaliação 02: Modelagem de Data Warehouse
-- PARTE II - Modelagem DW
--
-- Alunos:
-- Gabriel Rodrigues da Silva - 121044858
-- Giovanni Paes Leme da Gama Rodrigues - 117054744
-- Gabriel Brígido Pinheiro da Silva - 120056519
-- Nicolas Viana do Espírito Santo - 121042953
--
-- Link do GitHub: https://github.com/gbrods/EEL890_BigData/tree/main/P2_Parte2
--
-- Arquivo: previsao_ocupacao_patio_markov.sql
-- Descrição: Script para gerar a matriz de movimentação entre pátios.
-- =======================================================================================

USE data_warehouse;

-- ANÁLISE PARA MODELAGEM DE MARKOV: Matriz de Movimentação entre Pátios
-- Descrição: Calcula o percentual de veículos que saem de um pátio de origem e
--            são devolvidos em cada um dos pátios de destino.
WITH ViagensEntrePatios AS (
    -- Etapa 1: Contar o número de viagens entre cada par de pátios (origem -> destino)
    SELECT
        p_ret.NOME_PATIO AS Patio_Origem,
        p_dev.NOME_PATIO AS Patio_Destino,
        SUM(fl.QTD_LOCACOES) AS Total_Viagens
    FROM
        Fato_Locacao fl
    JOIN
        Dim_Patio p_ret ON fl.SK_PATIO_RETIRADA = p_ret.SK_PATIO
    JOIN
        Dim_Patio p_dev ON fl.SK_PATIO_DEVOLUCAO = p_dev.SK_PATIO
    WHERE
        p_dev.NOME_PATIO IS NOT NULL -- Garante que a locação foi concluída e devolvida
    GROUP BY
        p_ret.NOME_PATIO,
        p_dev.NOME_PATIO
)
-- Etapa 2: Calcular o percentual de cada rota em relação ao total de saídas daquele pátio de origem.
SELECT
    v.Patio_Origem,
    v.Patio_Destino,
    v.Total_Viagens,
    -- A função de janela SUM(...) OVER (PARTITION BY ...) calcula o total de saídas por pátio de origem.
    -- Isso permite calcular o percentual de forma eficiente.
    (v.Total_Viagens * 100.0 / SUM(v.Total_Viagens) OVER (PARTITION BY v.Patio_Origem)) AS Percentual_Movimentacao
FROM
    ViagensEntrePatios v
ORDER BY
    v.Patio_Origem, Percentual_Movimentacao DESC;