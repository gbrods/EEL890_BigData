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
-- Arquivo: controle_locacoes.sql
-- Descrição: Relatório de Controle das Locações.
-- =======================================================================================

USE data_warehouse;

-- RELATÓRIO 2: Controle das locações
-- Descrição: Quantitativo de veículos alugados por grupo e a duração média das locações.
SELECT
    dv.NOME_GRUPO AS Grupo_Veiculo,
    SUM(fl.QTD_LOCACOES) AS Quantidade_Total_Alugueis,
    AVG(fl.QTD_DIAS_LOCACAO) AS Duracao_Media_Locacao_Dias
FROM
    Fato_Locacao fl
JOIN
    Dim_Veiculo dv ON fl.SK_VEICULO = dv.SK_VEICULO
GROUP BY
    dv.NOME_GRUPO
ORDER BY
    Quantidade_Total_Alugueis DESC;