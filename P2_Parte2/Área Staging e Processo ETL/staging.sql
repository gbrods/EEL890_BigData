+---------------------------------------------------------------------------------
| UFRJ – Universidade Federal do Rio de Janeiro
| IM   – Instituto de Matemática
| DMA  – Departamento de Matemática Aplicada
|
| EEL890 - Big Data (turma SIGA 16338)
|
| Avaliação 02: Modelagem de Data Warehouse
| PARTE II - Modelagem DW
|
| Alunos:
| Gabriel Rodrigues da Silva - 121044858
| Giovanni Paes Leme da Gama Rodrigues - 117054744
| Gabriel Brígido Pinheiro da Silva - 120056519
| Nicolas Viana do Espírito Santo - 121042953
|
| Link do GitHub: https://github.com/gbrods/EEL890_BigData/tree/main/P2_Parte2
|
| Descrição do arquivo: Cria o schema e as tabelas na Staging Area para posteriormente receber os dados brutos dos outros grupos.
+---------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS `staging_area` DEFAULT CHARACTER SET utf8mb4 ;
USE `staging_area` ;

-- Tabela para receber os dados de Clientes
CREATE TABLE IF NOT EXISTS `staging_area`.`stg_cliente` (
  `ID_CLIENTE` INT,
  `NOME_CLIENTE` VARCHAR(255),
  `DOCUMENTO` VARCHAR(18),
  `EMAIL` VARCHAR(100),
  `TELEFONE` VARCHAR(20),
  `FK_ID_ENDERECO` INT,
  `TIPO_PESSOA` CHAR(1),
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

-- Tabela para receber os dados de Veículos
CREATE TABLE IF NOT EXISTS `staging_area`.`stg_veiculo` (
  `ID_VEICULO` INT,
  `PLACA` VARCHAR(7),
  `CHASSI` VARCHAR(17),
  `COR_VEICULO` VARCHAR(30),
  `TIPO_MECANIZACAO` VARCHAR(20),
  `POSSUI_AR_CONDICIONADO` TINYINT,
  `POSSUI_CADEIRINHA_CRIANCA` TINYINT,
  `POSSUI_BEBE_CONFORTO` TINYINT,
  `STATUS_VEICULO` VARCHAR(20),
  `FK_ID_MODELO_VEICULO` INT,
  `FK_ID_GRUPO_VEICULO` INT,
  `FK_ID_EMPRESA` INT,
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

-- Tabela para receber os dados de Locações
CREATE TABLE IF NOT EXISTS `staging_area`.`stg_locacao` (
  `ID_LOCACAO` INT,
  `DATA_HORA_RETIRADA_REALIZADA` TIMESTAMP,
  `DATA_HORA_DEVOLUCAO_PREVISTA` TIMESTAMP,
  `DATA_HORA_DEVOLUCAO_REALIZADA` TIMESTAMP,
  `VALOR_DIARIA_CONTRATADA` DECIMAL(10,2),
  `STATUS_LOCACAO` VARCHAR(20),
  `FK_ID_CLIENTE` INT,
  `FK_ID_CONDUTOR` INT,
  `FK_ID_VEICULO` INT,
  `FK_ID_PATIO_RETIRADA` INT,
  `FK_ID_PATIO_DEVOLUCAO` INT,
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

-- Tabela para receber os dados de Reservas
CREATE TABLE IF NOT EXISTS `staging_area`.`stg_reserva` (
  `ID_RESERVA` INT,
  `DATA_HORA_SOLICITACAO_RESERVA` TIMESTAMP,
  `DATA_HORA_PREVISTA_RETIRADA` TIMESTAMP,
  `DATA_HORA_PREVISTA_DEVOLUCAO` TIMESTAMP,
  `STATUS_RESERVA` VARCHAR(20),
  `FK_ID_CLIENTE` INT,
  `FK_ID_GRUPO_VEICULO` INT,
  `FK_ID_PATIO_RETIRADA` INT,
  `FK_ID_PATIO_DEVOLUCAO` INT,
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

-- Tabela para receber os dados de Pátios
CREATE TABLE IF NOT EXISTS `staging_area`.`stg_patio` (
  `ID_PATIO` INT,
  `NOME_PATIO` VARCHAR(100),
  `FK_ID_ENDERECO` INT,
  `FK_ID_EMPRESA` INT,
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

-- Tabela para receber os dados de Empresas
CREATE TABLE IF NOT EXISTS `staging_area`.`stg_empresa` (
  `ID_EMPRESA` INT,
  `NOME_EMPRESA` VARCHAR(100),
  `CNPJ_EMPRESA` VARCHAR(18),
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

-- Tabelas auxiliares para desnormalização (joins)
CREATE TABLE IF NOT EXISTS `staging_area`.`stg_grupoveiculo` (
  `ID_GRUPO_VEICULO` INT,
  `NOME_GRUPO` VARCHAR(100),
  `DESCRICAO_GRUPO` VARCHAR(255),
  `VALOR_DIARIA_BASE_GRUPO` DECIMAL(10,2),
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `staging_area`.`stg_modeloveiculo` (
  `ID_MODELO_VEICULO` INT,
  `NOME_MODELO` VARCHAR(50),
  `FK_ID_MARCA_VEICULO` INT,
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `staging_area`.`stg_marcaveiculo` (
  `ID_MARCA_VEICULO` INT,
  `NOME_MARCA` VARCHAR(50),
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `staging_area`.`stg_endereco` (
  `ID_ENDERECO` INT,
  `LOGRADOURO` VARCHAR(200),
  `NUMERO_LOGRADOURO` VARCHAR(20),
  `CEP` VARCHAR(9),
  `FK_ID_BAIRRO` INT,
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `staging_area`.`stg_bairro` (
  `ID_BAIRRO` INT,
  `NOME_BAIRRO` VARCHAR(100),
  `FK_ID_CIDADE` INT,
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `staging_area`.`stg_cidade` (
  `ID_CIDADE` INT,
  `NOME_CIDADE` VARCHAR(100),
  `FK_SIGLA_ESTADO` VARCHAR(2),
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `staging_area`.`stg_estado` (
  `SIGLA_ESTADO` VARCHAR(2),
  `NOME_ESTADO` VARCHAR(50),
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

-- Tabelas para cálculo de métricas financeiras
CREATE TABLE IF NOT EXISTS `staging_area`.`stg_cobranca` (
  `ID_COBRANCA` INT,
  `VALOR_COBRANCA` DECIMAL(10,2),
  `FK_ID_LOCACAO` INT,
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `staging_area`.`stg_locacaoprotecao` (
  `FK_ID_LOCACAO` INT,
  `FK_ID_PROTECAO_ADICIONAL` INT,
  `VALOR_PROTECAO_APLICADO` DECIMAL(10,2),
  `FONTE_DADOS` VARCHAR(50),
  `DATA_CARGA` TIMESTAMP
);