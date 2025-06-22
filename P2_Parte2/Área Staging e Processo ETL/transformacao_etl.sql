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
-- Arquivo: transformacao_etl.sql
-- Descrição: Lê os dados brutos da Staging Area, aplica regras de limpeza,
--            padronização e unificação para TODAS as dimensões de negócio,
--            e prepara os dados para as tabelas de fatos.
-- =======================================================================================

USE staging_area;

-- ---------------------------------------------------------------------------------------
-- ETAPA 1: TRANSFORMAÇÃO DAS DIMENSÕES
-- ---------------------------------------------------------------------------------------

--
-- Tabela Transformada para Dim_Cliente
--
DROP TABLE IF EXISTS stg_dim_cliente;
CREATE TABLE stg_dim_cliente AS
SELECT
    REGEXP_REPLACE(DOCUMENTO, '[^0-9]', '') AS CLIENTE_BK,
    MAX(UPPER(TRIM(NOME_CLIENTE))) AS NOME_CLIENTE,
    MAX(CASE
        WHEN UPPER(TIPO_PESSOA) IN ('F', 'PF') THEN 'PESSOA FÍSICA'
        WHEN UPPER(TIPO_PESSOA) IN ('J', 'PJ') THEN 'PESSOA JURÍDICA'
        ELSE 'NÃO INFORMADO'
    END) AS TIPO_PESSOA,
    MAX(EMAIL) as EMAIL,
    MAX(COALESCE(e.LOGRADOURO, cli.endereco_completo)) AS ENDERECO,
    MAX(est.NOME_ESTADO) AS ESTADO,
    MAX(cid.NOME_CIDADE) AS CIDADE,
    MAX(est.SIGLA_ESTADO) AS UF
FROM
    stg_cliente cli
    LEFT JOIN stg_endereco e ON cli.FK_ID_ENDERECO = e.ID_ENDERECO AND cli.FONTE_DADOS = e.FONTE_DADOS
    LEFT JOIN stg_bairro b ON e.FK_ID_BAIRRO = b.ID_BAIRRO AND e.FONTE_DADOS = b.FONTE_DADOS
    LEFT JOIN stg_cidade cid ON b.FK_ID_CIDADE = cid.ID_CIDADE AND b.FONTE_DADOS = cid.FONTE_DADOS
    LEFT JOIN stg_estado est ON cid.FK_SIGLA_ESTADO = est.SIGLA_ESTADO AND cid.FONTE_DADOS = est.FONTE_DADOS
WHERE
    DOCUMENTO IS NOT NULL AND DOCUMENTO != ''
GROUP BY
    CLIENTE_BK;

--
-- Tabela Transformada para Dim_Veiculo
--
DROP TABLE IF EXISTS stg_dim_veiculo;
CREATE TABLE stg_dim_veiculo AS
SELECT
    UPPER(TRIM(v.PLACA)) AS VEICULO_BK,
    MAX(v.CHASSI) AS CHASSI,
    MAX(COALESCE(m.NOME_MARCA, v.marca)) AS MARCA,
    MAX(COALESCE(md.NOME_MODELO, v.modelo)) AS MODELO,
    MAX(g.NOME_GRUPO) AS GRUPO,
    MAX(v.COR_VEICULO) AS COR,
    MAX(CASE
        WHEN UPPER(v.TIPO_MECANIZACAO) IN ('AUTOMATICA', 'AUTOMÁTICA', 'AUTOMATICO') THEN 'AUTOMÁTICA'
        ELSE 'MANUAL'
    END) AS TIPO_MECANIZACAO,
    MAX(CASE WHEN v.POSSUI_AR_CONDICIONADO = 1 THEN 'Sim' ELSE 'Não' END) AS AR_CONDICIONADO,
    MAX(CASE WHEN v.POSSUI_CADEIRINHA_CRIANCA = 1 THEN 'Sim' ELSE 'Não' END) AS CADEIRINHA,
    MAX(emp.NOME_EMPRESA) as EMPRESA_PROPRIETARIA
FROM
    stg_veiculo v
    LEFT JOIN stg_grupoveiculo g ON v.FK_ID_GRUPO_VEICULO = g.ID_GRUPO_VEICULO AND v.FONTE_DADOS = g.FONTE_DADOS
    LEFT JOIN stg_empresa emp ON v.FK_ID_EMPRESA = emp.ID_EMPRESA AND v.FONTE_DADOS = emp.FONTE_DADOS
    LEFT JOIN stg_modeloveiculo md ON v.FK_ID_MODELO_VEICULO = md.ID_MODELO_VEICULO AND v.FONTE_DADOS = md.FONTE_DADOS
    LEFT JOIN stg_marcaveiculo m ON md.FK_ID_MARCA_VEICULO = m.ID_MARCA_VEICULO AND md.FONTE_DADOS = m.FONTE_DADOS
WHERE
    v.PLACA IS NOT NULL AND v.PLACA != ''
GROUP BY
    VEICULO_BK;

--
-- Tabela Transformada para Dim_Patio
--
DROP TABLE IF EXISTS stg_dim_patio;
CREATE TABLE stg_dim_patio AS
SELECT
    p.ID_PATIO AS PATIO_ID_ORIGEM,
    p.FONTE_DADOS,
    UPPER(TRIM(p.NOME_PATIO)) AS NOME_PATIO,
    e.NOME_EMPRESA AS EMPRESA_PROPRIETARIA
FROM
    stg_patio p
JOIN
    stg_empresa e ON p.FK_ID_EMPRESA = e.ID_EMPRESA AND p.FONTE_DADOS = e.FONTE_DADOS
GROUP BY
    PATIO_ID_ORIGEM, p.FONTE_DADOS, NOME_PATIO, EMPRESA_PROPRIETARIA;

--
-- Tabela Transformada para Dim_Empresa
--
DROP TABLE IF EXISTS stg_dim_empresa;
CREATE TABLE stg_dim_empresa AS
SELECT
    -- Limpa o CNPJ para ser a chave de negócio única.
    REGEXP_REPLACE(CNPJ_EMPRESA, '[^0-9]', '') AS EMPRESA_BK,
    -- Garante um nome único e padronizado por CNPJ.
    MAX(UPPER(TRIM(NOME_EMPRESA))) AS NOME_EMPRESA
FROM
    staging_area.stg_empresa
WHERE
    CNPJ_EMPRESA IS NOT NULL AND CNPJ_EMPRESA != ''
GROUP BY
    EMPRESA_BK;

--
-- Tabela Transformada para Dim_GrupoVeiculo
--
DROP TABLE IF EXISTS stg_dim_grupoveiculo;
CREATE TABLE stg_dim_grupoveiculo AS
SELECT
    -- O nome do grupo é a chave de negócio. Padronizamos para garantir unicidade.
    UPPER(TRIM(NOME_GRUPO)) AS GRUPO_BK,
    MAX(DESCRICAO_GRUPO) AS DESCRICAO_GRUPO,
    -- Pega a média do valor da diária para o mesmo grupo vindo de fontes diferentes.
    AVG(VALOR_DIARIA_BASE_GRUPO) AS VALOR_DIARIA_MEDIA
FROM
    staging_area.stg_grupoveiculo
WHERE
    NOME_GRUPO IS NOT NULL AND NOME_GRUPO != ''
GROUP BY
    GRUPO_BK;


-- ---------------------------------------------------------------------------------------
-- ETAPA 2: PREPARAÇÃO DOS DADOS PARA AS TABELAS DE FATOS
-- ---------------------------------------------------------------------------------------

DROP TABLE IF EXISTS stg_fato_locacao;
CREATE TABLE stg_fato_locacao AS
WITH Protecoes AS (
    SELECT FK_ID_LOCACAO, FONTE_DADOS, SUM(VALOR_PROTECAO_APLICADO) as VALOR_TOTAL_PROTECOES
    FROM stg_locacaoprotecao
    GROUP BY FK_ID_LOCACAO, FONTE_DADOS
),
Cobrancas AS (
    SELECT FK_ID_LOCACAO, FONTE_DADOS, SUM(VALOR_COBRANCA) as VALOR_TOTAL_COBRADO
    FROM stg_cobranca
    GROUP BY FK_ID_LOCACAO, FONTE_DADOS
)
SELECT
    l.ID_LOCACAO AS ID_LOCACAO_ORIGEM,
    l.FONTE_DADOS,
    REGEXP_REPLACE(c.DOCUMENTO, '[^0-9]', '') AS CLIENTE_BK,
    v.PLACA AS VEICULO_BK,
    l.FK_ID_PATIO_RETIRADA AS PATIO_RETIRADA_ID_ORIGEM,
    l.FK_ID_PATIO_DEVOLUCAO AS PATIO_DEVOLUCAO_ID_ORIGEM,
    CAST(l.DATA_HORA_RETIRADA_REALIZADA AS DATE) as DATA_RETIRADA,
    CAST(l.DATA_HORA_DEVOLUCAO_REALIZADA AS DATE) as DATA_DEVOLUCAO,
    l.VALOR_DIARIA_CONTRATADA,
    COALESCE(p.VALOR_TOTAL_PROTECOES, 0) AS VALOR_TOTAL_PROTECOES,
    COALESCE(cob.VALOR_TOTAL_COBRADO, 0) AS VALOR_TOTAL_COBRADO,
    DATEDIFF(l.DATA_HORA_DEVOLUCAO_REALIZADA, l.DATA_HORA_RETIRADA_REALIZADA) + 1 AS QTD_DIAS_LOCACAO
FROM
    stg_locacao l
    JOIN stg_cliente c ON l.FK_ID_CLIENTE = c.ID_CLIENTE AND l.FONTE_DADOS = c.FONTE_DADOS
    JOIN stg_veiculo v ON l.FK_ID_VEICULO = v.ID_VEICULO AND l.FONTE_DADOS = v.FONTE_DADOS
    LEFT JOIN Protecoes p ON l.ID_LOCACAO = p.FK_ID_LOCACAO AND l.FONTE_DADOS = p.FONTE_DADOS
    LEFT JOIN Cobrancas cob ON l.ID_LOCACAO = cob.FK_ID_LOCACAO AND l.FONTE_DADOS = cob.FONTE_DADOS
WHERE
    l.STATUS_LOCACAO IN ('Concluida', 'CONCLUIDA');

DROP TABLE IF EXISTS stg_fato_reserva;
CREATE TABLE stg_fato_reserva AS
SELECT
    r.ID_RESERVA AS ID_RESERVA_ORIGEM,
    r.FONTE_DADOS,
    REGEXP_REPLACE(c.DOCUMENTO, '[^0-9]', '') AS CLIENTE_BK,
    r.FK_ID_GRUPO_VEICULO AS GRUPO_VEICULO_ID_ORIGEM,
    r.FK_ID_PATIO_RETIRADA AS PATIO_RETIRADA_ID_ORIGEM,
    r.FK_ID_PATIO_DEVOLUCAO AS PATIO_DEVOLUCAO_ID_ORIGEM,
    CAST(r.DATA_HORA_SOLICITACAO_RESERVA AS DATE) AS DATA_SOLICITACAO,
    CAST(r.DATA_HORA_PREVISTA_RETIRADA AS DATE) AS DATA_PREVISTA_RETIRADA,
    CAST(r.DATA_HORA_PREVISTA_DEVOLUCAO AS DATE) AS DATA_PREVISTA_DEVOLUCAO,
    DATEDIFF(r.DATA_HORA_PREVISTA_DEVOLUCAO, r.DATA_HORA_PREVISTA_RETIRADA) + 1 AS QTD_DIAS_PREVISTOS_LOCACAO,
    DATEDIFF(r.DATA_HORA_PREVISTA_RETIRADA, r.DATA_HORA_SOLICITACAO_RESERVA) AS DIAS_ANTECEDENCIA_RESERVA
FROM
    stg_reserva r
JOIN
    stg_cliente c ON r.FK_ID_CLIENTE = c.ID_CLIENTE AND r.FONTE_DADOS = c.FONTE_DADOS;