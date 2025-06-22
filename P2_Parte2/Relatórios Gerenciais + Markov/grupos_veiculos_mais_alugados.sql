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
-- Arquivo: grupos_veiculos_mais_alugados.sql
-- Descrição: Relatório dos grupos de veículos mais alugados.
-- =======================================================================================

USE data_warehouse;

-- RELATÓRIO 4: Grupos de veículos mais alugados
-- Descrição: Ranking dos grupos de veículos mais alugados, com a possibilidade de
--            cruzar com a origem dos clientes.
SELECT
    dv.NOME_GRUPO AS Grupo_Veiculo,
    dc.UF AS UF_Cliente,
    dc.CIDADE AS Cidade_Cliente,
    SUM(fl.QTD_LOCACOES) AS Total_Alugueis
FROM
    Fato_Locacao fl
JOIN
    Dim_Veiculo dv ON fl.SK_VEICULO = dv.SK_VEICULO
JOIN
    Dim_Cliente dc ON fl.SK_CLIENTE = dc.SK_CLIENTE
WHERE
    dc.CIDADE IS NOT NULL
GROUP BY
    dv.NOME_GRUPO,
    dc.UF,
    dc.CIDADE
ORDER BY
    Total_Alugueis DESC;