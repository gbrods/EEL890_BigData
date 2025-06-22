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
-- Arquivo: carga_etl.sql
-- Descrição: Carrega os dados transformados da Staging Area para as tabelas
--            finais de Dimensão e Fato no Data Warehouse.
-- =======================================================================================

-- Aponta para o banco de dados do Data Warehouse onde a carga será feita.
USE data_warehouse;


-- ---------------------------------------------------------------------------------------
-- ETAPA 1: SETUP INICIAL DA DIM_TEMPO (Lógica para rodar apenas uma vez)
-- ---------------------------------------------------------------------------------------

-- Cria a tabela Dim_Tempo apenas se ela ainda não existir no Data Warehouse.
CREATE TABLE IF NOT EXISTS Dim_Tempo (
    SK_TEMPO INT PRIMARY KEY,
    DATA DATE NOT NULL,
    ANO INT NOT NULL,
    MES INT NOT NULL,
    DIA INT NOT NULL,
    TRIMESTRE INT NOT NULL,
    NOME_MES VARCHAR(20) NOT NULL,
    DIA_DA_SEMANA VARCHAR(20) NOT NULL,
    UNIQUE (DATA)
);

-- Insere os dados na Dim_Tempo apenas se a tabela estiver vazia, evitando duplicatas.
-- Esta abordagem usa um gerador de sequência para criar as datas automaticamente.
-- NOTA: Antes de executar em um ambiente real, garanta que o idioma da sessão está correto.
-- SET lc_time_names = 'pt_BR';
INSERT INTO Dim_Tempo (SK_TEMPO, DATA, ANO, MES, DIA, TRIMESTRE, NOME_MES, DIA_DA_SEMANA)
SELECT
    DATE_FORMAT(d.DATA, '%Y%m%d') + 0 AS SK_TEMPO,
    d.DATA,
    YEAR(d.DATA),
    MONTH(d.DATA),
    DAY(d.DATA),
    QUARTER(d.DATA),
    MONTHNAME(d.DATA),
    DAYNAME(d.DATA)
FROM (
    -- Subconsulta para gerar uma sequência de números (0 a 9999)
    SELECT DATE_ADD('2010-01-01', INTERVAL seq.seq DAY) AS DATA
    FROM (
        SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 AS seq
        FROM 
            (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 
             UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a,
            (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 
             UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b,
            (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 
             UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c,
            (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 
             UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d
    ) seq
    WHERE DATE_ADD('2010-01-01', INTERVAL seq.seq DAY) <= '2030-12-31'
) AS d
WHERE (SELECT COUNT(*) FROM Dim_Tempo) = 0;


-- ---------------------------------------------------------------------------------------
-- ETAPA 2: LIMPEZA DAS TABELAS (TRUNCATE) PARA CARGA COMPLETA (FULL LOAD)
-- ---------------------------------------------------------------------------------------

-- Desativa a verificação de chaves estrangeiras para permitir o TRUNCATE em qualquer ordem.
SET FOREIGN_KEY_CHECKS = 0;

-- Limpa as tabelas de Dimensão (exceto Dim_Tempo) e Fato antes de carregar os novos dados.
TRUNCATE TABLE Fato_Locacao;
TRUNCATE TABLE Fato_Reserva;
TRUNCATE TABLE Dim_Cliente;
TRUNCATE TABLE Dim_Veiculo;
TRUNCATE TABLE Dim_Patio;
TRUNCATE TABLE Dim_Empresa;
TRUNCATE TABLE Dim_GrupoVeiculo;

-- Reativa a verificação de chaves estrangeiras.
SET FOREIGN_KEY_CHECKS = 1;


-- ---------------------------------------------------------------------------------------
-- ETAPA 3: CARGA DAS DIMENSÕES
-- ---------------------------------------------------------------------------------------

-- Carga da Dim_Cliente a partir da tabela transformada na staging.
INSERT INTO Dim_Cliente (CLIENTE_BK, NOME_CLIENTE, TIPO_PESSOA, EMAIL, ENDERECO, ESTADO, CIDADE, UF)
SELECT CLIENTE_BK, NOME_CLIENTE, TIPO_PESSOA, EMAIL, ENDERECO, ESTADO, CIDADE, UF
FROM staging_area.stg_dim_cliente;

-- Carga da Dim_Veiculo
INSERT INTO Dim_Veiculo (VEICULO_BK, CHASSI, NOME_MARCA, NOME_MODELO, NOME_GRUPO, COR_VEICULO, TIPO_MECANIZACAO, DESC_AR_CONDICIONADO, DESC_CADEIRINHA_CRIANCA, NOME_EMPRESA_PROPRIETARIA)
SELECT VEICULO_BK, CHASSI, MARCA, MODELO, GRUPO, COR, TIPO_MECANIZACAO, AR_CONDICIONADO, CADEIRINHA, EMPRESA_PROPRIETARIA
FROM staging_area.stg_dim_veiculo;

-- Carga da Dim_Patio
INSERT INTO Dim_Patio (ID_PATIO_OLTP, FONTE_DADOS, NOME_PATIO, NOME_EMPRESA_PROPRIETARIA)
SELECT PATIO_ID_ORIGEM, FONTE_DADOS, NOME_PATIO, EMPRESA_PROPRIETARIA
FROM staging_area.stg_dim_patio;

-- Carga da Dim_Empresa
INSERT INTO Dim_Empresa (EMPRESA_BK, NOME_EMPRESA)
SELECT EMPRESA_BK, NOME_EMPRESA
FROM staging_area.stg_dim_empresa;

-- Carga da Dim_GrupoVeiculo
INSERT INTO Dim_GrupoVeiculo (GRUPO_BK, DESCRICAO_GRUPO, VALOR_DIARIA_BASE_GRUPO)
SELECT GRUPO_BK, DESCRICAO_GRUPO, VALOR_DIARIA_MEDIA
FROM staging_area.stg_dim_grupoveiculo;


-- ---------------------------------------------------------------------------------------
-- ETAPA 4: CARGA DAS TABELAS DE FATOS
-- ---------------------------------------------------------------------------------------

-- Carga da Fato_Locacao
-- Esta é a etapa mais crítica, onde buscamos as chaves substitutas (SK) das dimensões
-- recém-carregadas para construir as linhas da tabela de fatos.
INSERT INTO Fato_Locacao (
    SK_TEMPO_RETIRADA, SK_TEMPO_DEVOLUCAO, SK_CLIENTE, SK_VEICULO,
    SK_PATIO_RETIRADA, SK_PATIO_DEVOLUCAO, SK_EMPRESA_VEICULO,
    VALOR_DIARIA_CONTRATADA, VALOR_TOTAL_PROTECOES, VALOR_TOTAL_COBRADO,
    QTD_DIAS_LOCACAO, QTD_LOCACOES
)
SELECT
    -- Lookups para obter as chaves substitutas (SKs)
    t_ret.SK_TEMPO,
    t_dev.SK_TEMPO,
    dc.SK_CLIENTE,
    dv.SK_VEICULO,
    dp_ret.SK_PATIO,
    dp_dev.SK_PATIO,
    de.SK_EMPRESA,
    -- Métricas
    sf.VALOR_DIARIA_CONTRATADA,
    sf.VALOR_TOTAL_PROTECOES,
    sf.VALOR_TOTAL_COBRADO,
    sf.QTD_DIAS_LOCACAO,
    1 -- Contador fixo para cada locação
FROM
    staging_area.stg_fato_locacao sf
-- JOINs com as dimensões no Data Warehouse para buscar as SKs
LEFT JOIN Dim_Tempo t_ret ON sf.DATA_RETIRADA = t_ret.DATA
LEFT JOIN Dim_Tempo t_dev ON sf.DATA_DEVOLUCAO = t_dev.DATA
LEFT JOIN Dim_Cliente dc ON sf.CLIENTE_BK = dc.CLIENTE_BK
LEFT JOIN Dim_Veiculo dv ON sf.VEICULO_BK = dv.VEICULO_BK
LEFT JOIN Dim_Empresa de ON dv.NOME_EMPRESA_PROPRIETARIA = de.NOME_EMPRESA
-- JOINs para a dimensão com papel duplo (Role-Playing Dimension) de Pátio
LEFT JOIN Dim_Patio dp_ret ON sf.PATIO_RETIRADA_ID_ORIGEM = dp_ret.ID_PATIO_OLTP AND sf.FONTE_DADOS = dp_ret.FONTE_DADOS
LEFT JOIN Dim_Patio dp_dev ON sf.PATIO_DEVOLUCAO_ID_ORIGEM = dp_dev.ID_PATIO_OLTP AND sf.FONTE_DADOS = dp_dev.FONTE_DADOS;


-- Carga da Fato_Reserva
INSERT INTO Fato_Reserva (
    SK_TEMPO_SOLICITACAO, SK_TEMPO_PREV_RETIRADA, SK_TEMPO_PREV_DEVOLUCAO,
    SK_CLIENTE, SK_GRUPO_VEICULO, SK_PATIO_RETIRADA, SK_PATIO_DEVOLUCAO,
    ID_RESERVA_OLTP, QTD_DIAS_PREVISTOS_LOCACAO, QTD_RESERVAS
)
SELECT
    -- Lookups
    t_sol.SK_TEMPO,
    t_ret.SK_TEMPO,
    t_dev.SK_TEMPO,
    dc.SK_CLIENTE,
    dg.SK_GRUPO_VEICULO,
    dp_ret.SK_PATIO,
    dp_dev.SK_PATIO,
    -- Dados descritivos e métricas
    sf.ID_RESERVA_ORIGEM,
    sf.QTD_DIAS_PREVISTOS_LOCACAO,
    1 -- Contador
FROM
    staging_area.stg_fato_reserva sf
-- JOINs para buscar as SKs
LEFT JOIN Dim_Tempo t_sol ON sf.DATA_SOLICITACAO = t_sol.DATA
LEFT JOIN Dim_Tempo t_ret ON sf.DATA_PREVISTA_RETIRADA = t_ret.DATA
LEFT JOIN Dim_Tempo t_dev ON sf.DATA_PREVISTA_DEVOLUCAO = t_dev.DATA
LEFT JOIN Dim_Cliente dc ON sf.CLIENTE_BK = dc.CLIENTE_BK
LEFT JOIN staging_area.stg_grupoveiculo sg ON sf.GRUPO_VEICULO_ID_ORIGEM = sg.ID_GRUPO_VEICULO AND sf.FONTE_DADOS = sg.FONTE_DADOS -- Junção intermediária
LEFT JOIN Dim_GrupoVeiculo dg ON sg.NOME_GRUPO = dg.GRUPO_BK -- Junção final na dimensão
LEFT JOIN Dim_Patio dp_ret ON sf.PATIO_RETIRADA_ID_ORIGEM = dp_ret.ID_PATIO_OLTP AND sf.FONTE_DADOS = dp_ret.FONTE_DADOS
LEFT JOIN Dim_Patio dp_dev ON sf.PATIO_DEVOLUCAO_ID_ORIGEM = dp_dev.ID_PATIO_OLTP AND sf.FONTE_DADOS = dp_dev.FONTE_DADOS;