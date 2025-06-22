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
-- Arquivo: controle_patio.sql
-- Descrição: Relatório de Controle de Pátio.
-- =======================================================================================

USE data_warehouse;

-- RELATÓRIO 1: Controle de pátio
-- Descrição: Quantitativo de locações iniciadas por pátio, com detalhamento por grupo,
--            marca, modelo, mecanização e origem da frota (própria do pátio ou parceira).
SELECT
    dp.NOME_PATIO AS Patio_Retirada,
    dv.NOME_GRUPO AS Grupo_Veiculo,
    dv.NOME_MARCA AS Marca,
    dv.NOME_MODELO AS Modelo,
    dv.TIPO_MECANIZACAO AS Mecanizacao,
    CASE
        WHEN dp.NOME_EMPRESA_PROPRIETARIA = dv.NOME_EMPRESA_PROPRIETARIA THEN 'Frota Própria do Pátio'
        ELSE 'Frota de Parceira'
    END AS Origem_Frota,
    SUM(fl.QTD_LOCACOES) AS Total_Locacoes_Iniciadas
FROM
    Fato_Locacao fl
JOIN
    Dim_Patio dp ON fl.SK_PATIO_RETIRADA = dp.SK_PATIO
JOIN
    Dim_Veiculo dv ON fl.SK_VEICULO = dv.SK_VEICULO
GROUP BY
    dp.NOME_PATIO,
    dv.NOME_GRUPO,
    dv.NOME_MARCA,
    dv.NOME_MODELO,
    dv.TIPO_MECANIZACAO,
    Origem_Frota
ORDER BY
    Patio_Retirada, Total_Locacoes_Iniciadas DESC;