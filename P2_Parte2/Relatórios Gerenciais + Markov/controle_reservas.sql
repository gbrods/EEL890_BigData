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
-- Arquivo: controle_reservas.sql
-- Descrição: Relatório de Controle de Reservas.
-- =======================================================================================

USE data_warehouse;

-- RELATÓRIO 3: Controle de reservas
-- Descrição: Quantidade de reservas por grupo de veículo, pátio de retirada e
--            cidade de origem dos clientes.
SELECT
    dg.GRUPO_BK AS Grupo_Veiculo_Reservado,
    dp.NOME_PATIO AS Patio_Retirada_Previsto,
    dc.CIDADE AS Cidade_Origem_Cliente,
    SUM(fr.QTD_RESERVAS) AS Total_Reservas
FROM
    Fato_Reserva fr
JOIN
    Dim_GrupoVeiculo dg ON fr.SK_GRUPO_VEICULO = dg.SK_GRUPO_VEICULO
JOIN
    Dim_Patio dp ON fr.SK_PATIO_RETIRADA = dp.SK_PATIO
JOIN
    Dim_Cliente dc ON fr.SK_CLIENTE = dc.SK_CLIENTE
GROUP BY
    dg.GRUPO_BK,
    dp.NOME_PATIO,
    dc.CIDADE
ORDER BY
    Total_Reservas DESC;