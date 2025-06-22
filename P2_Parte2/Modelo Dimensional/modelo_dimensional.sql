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
-- Arquivo: modelo_dimensional.sql
-- Descrição: Definição do modelo dimensional
-- =======================================================================================

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema dw_locadora
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema dw_locadora
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `dw_locadora` DEFAULT CHARACTER SET utf8 ;
USE `dw_locadora` ;

-- -----------------------------------------------------
-- Table `dw_locadora`.`Dim_Cliente`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dw_locadora`.`Dim_Cliente` (
  `SK_CLIENTE` INT NOT NULL,
  `ID_CLIENTE_OLTP` INT NOT NULL,
  `NOME_CLIENTE` VARCHAR(255) NOT NULL,
  `DOCUMENTO` VARCHAR(18) NOT NULL,
  `TIPO_PESSOA` VARCHAR(15) NOT NULL,
  `EMAIL` VARCHAR(100) NOT NULL,
  `CIDADE_CLIENTE` VARCHAR(100) NOT NULL,
  `ESTADO_CLIENTE` VARCHAR(50) NOT NULL,
  `UF_CLIENTE` VARCHAR(2) NOT NULL,
  PRIMARY KEY (`SK_CLIENTE`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dw_locadora`.`Dim_Veiculo`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dw_locadora`.`Dim_Veiculo` (
  `SK_VEICULO` INT NOT NULL,
  `ID_VEICULO_OLTP` INT NOT NULL,
  `PLACA` VARCHAR(7) NOT NULL,
  `CHASSI` VARCHAR(17) NOT NULL,
  `NOME_MARCA` VARCHAR(50) NOT NULL,
  `NOME_MODELO` VARCHAR(50) NOT NULL,
  `NOME_GRUPO` VARCHAR(100) NOT NULL,
  `COR_VEICULO` VARCHAR(30) NOT NULL,
  `TIPO_MECANIZACAO` VARCHAR(20) NOT NULL,
  `DESC_AR_CONDICIONADO` VARCHAR(3) NOT NULL,
  `DESC_CADEIRINHA_CRIANCA` VARCHAR(3) NOT NULL,
  `DESC_BEBE_CONFORTO` VARCHAR(3) NOT NULL,
  PRIMARY KEY (`SK_VEICULO`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dw_locadora`.`Dim_Patio`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dw_locadora`.`Dim_Patio` (
  `SK_PATIO` INT NOT NULL,
  `ID_PATIO_OLTP` INT NOT NULL,
  `NOME_PATIO` VARCHAR(100) NOT NULL,
  `NOME_EMPRESA_PROPRIETARIA` VARCHAR(100) NOT NULL,
  `LOGRADOURO_PATIO` VARCHAR(200) NOT NULL,
  `BAIRRO_PATIO` VARCHAR(100) NOT NULL,
  `CIDADE_PATIO` VARCHAR(100) NOT NULL,
  `UF_PATIO` VARCHAR(2) NOT NULL,
  `CEP_PATIO` VARCHAR(9) NOT NULL,
  PRIMARY KEY (`SK_PATIO`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dw_locadora`.`Dim_Tempo`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dw_locadora`.`Dim_Tempo` (
  `SK_TEMPO` INT NOT NULL,
  `DATA` DATE NOT NULL,
  `ANO` INT NOT NULL,
  `TRIMESTRE` INT NOT NULL,
  `MES` INT NOT NULL,
  `DIA` INT NOT NULL,
  `SEMANA_DO_ANO` INT NOT NULL,
  `DIA_DA_SEMANA` VARCHAR(20) NOT NULL,
  PRIMARY KEY (`SK_TEMPO`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dw_locadora`.`Dim_Empresa`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dw_locadora`.`Dim_Empresa` (
  `SK_EMPRESA` INT NOT NULL,
  `ID_EMPRESA_OLTP` INT NOT NULL,
  `NOME_EMPRESA` VARCHAR(100) NOT NULL,
  `CNPJ_EMPRESA` VARCHAR(18) NOT NULL,
  PRIMARY KEY (`SK_EMPRESA`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dw_locadora`.`Fato_Locacao`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dw_locadora`.`Fato_Locacao` (
  `SK_TEMPO_RETIRADA` INT NOT NULL,
  `SK_TEMPO_DEVOLUCAO` INT NULL,
  `SK_CLIENTE` INT NOT NULL,
  `SK_VEICULO` INT NOT NULL,
  `SK_PATIO_RETIRADA` INT NOT NULL,
  `SK_PATIO_DEVOLUCAO` INT NULL,
  `SK_EMPRESA_VEICULO` INT NOT NULL,
  `VALOR_DIARIA_CONTRATADA` DECIMAL(10,2) NOT NULL,
  `VALOR_TOTAL_PROTECOES` DECIMAL(10,2) NOT NULL,
  `VALOR_TOTAL_COBRADO` DECIMAL(10,2) NOT NULL,
  `QTD_DIAS_LOCACAO` INT NOT NULL,
  `QTD_LOCACOES` INT NOT NULL,
  INDEX `fk_Fato_Locacao_Dim_Tempo1_idx` (`SK_TEMPO_DEVOLUCAO` ASC) VISIBLE,
  INDEX `fk_Fato_Locacao_Dim_Cliente1_idx` (`SK_CLIENTE` ASC) VISIBLE,
  INDEX `fk_Fato_Locacao_Dim_Veiculo1_idx` (`SK_VEICULO` ASC) VISIBLE,
  INDEX `fk_Fato_Locacao_Dim_Patio1_idx` (`SK_PATIO_RETIRADA` ASC) VISIBLE,
  INDEX `fk_Fato_Locacao_Dim_Patio2_idx` (`SK_PATIO_DEVOLUCAO` ASC) VISIBLE,
  INDEX `fk_Fato_Locacao_Dim_Empresa1_idx` (`SK_EMPRESA_VEICULO` ASC) VISIBLE,
  PRIMARY KEY (`SK_TEMPO_RETIRADA`, `SK_CLIENTE`, `SK_VEICULO`),
  CONSTRAINT `fk_Fato_Locacao_Dim_Tempo`
    FOREIGN KEY (`SK_TEMPO_RETIRADA`)
    REFERENCES `dw_locadora`.`Dim_Tempo` (`SK_TEMPO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Locacao_Dim_Tempo1`
    FOREIGN KEY (`SK_TEMPO_DEVOLUCAO`)
    REFERENCES `dw_locadora`.`Dim_Tempo` (`SK_TEMPO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Locacao_Dim_Cliente1`
    FOREIGN KEY (`SK_CLIENTE`)
    REFERENCES `dw_locadora`.`Dim_Cliente` (`SK_CLIENTE`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Locacao_Dim_Veiculo1`
    FOREIGN KEY (`SK_VEICULO`)
    REFERENCES `dw_locadora`.`Dim_Veiculo` (`SK_VEICULO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Locacao_Dim_Patio1`
    FOREIGN KEY (`SK_PATIO_RETIRADA`)
    REFERENCES `dw_locadora`.`Dim_Patio` (`SK_PATIO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Locacao_Dim_Patio2`
    FOREIGN KEY (`SK_PATIO_DEVOLUCAO`)
    REFERENCES `dw_locadora`.`Dim_Patio` (`SK_PATIO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Locacao_Dim_Empresa1`
    FOREIGN KEY (`SK_EMPRESA_VEICULO`)
    REFERENCES `dw_locadora`.`Dim_Empresa` (`SK_EMPRESA`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dw_locadora`.`Dim_GrupoVeiculo`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dw_locadora`.`Dim_GrupoVeiculo` (
  `SK_GRUPO_VEICULO` INT NOT NULL,
  `ID_GRUPO_VEICULO_OLTP` INT NOT NULL,
  `NOME_GRUPO` VARCHAR(100) NOT NULL,
  `DESCRICAO_GRUPO` VARCHAR(255) NULL,
  `VALOR_DIARIA_BASE_GRUPO` DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (`SK_GRUPO_VEICULO`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dw_locadora`.`Fato_Reserva`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dw_locadora`.`Fato_Reserva` (
  `SK_TEMPO_SOLICITACAO` INT NOT NULL,
  `SK_TEMPO_PREV_RETIRADA` INT NOT NULL,
  `SK_TEMPO_PREV_DEVOLUCAO` INT NOT NULL,
  `SK_CLIENTE` INT NOT NULL,
  `SK_GRUPO_VEICULO` INT NOT NULL,
  `SK_PATIO_RETIRADA` INT NOT NULL,
  `SK_PATIO_DEVOLUCAO` INT NOT NULL,
  `ID_RESERVA_OLTP` INT NOT NULL,
  `QTD_DIAS_PREVISTOS_LOCACAO` INT NOT NULL,
  `QTD_RESERVAS` INT NOT NULL,
  PRIMARY KEY (`SK_TEMPO_SOLICITACAO`, `SK_CLIENTE`, `SK_GRUPO_VEICULO`),
  INDEX `fk_Fato_Reserva_Dim_Tempo2_idx` (`SK_TEMPO_PREV_RETIRADA` ASC) VISIBLE,
  INDEX `fk_Fato_Reserva_Dim_Tempo3_idx` (`SK_TEMPO_PREV_DEVOLUCAO` ASC) VISIBLE,
  INDEX `fk_Fato_Reserva_Dim_Cliente1_idx` (`SK_CLIENTE` ASC) VISIBLE,
  INDEX `fk_Fato_Reserva_Dim_GrupoVeiculo1_idx` (`SK_GRUPO_VEICULO` ASC) VISIBLE,
  INDEX `fk_Fato_Reserva_Dim_Patio1_idx` (`SK_PATIO_RETIRADA` ASC) VISIBLE,
  INDEX `fk_Fato_Reserva_Dim_Patio2_idx` (`SK_PATIO_DEVOLUCAO` ASC) VISIBLE,
  CONSTRAINT `fk_Fato_Reserva_Dim_Tempo1`
    FOREIGN KEY (`SK_TEMPO_SOLICITACAO`)
    REFERENCES `dw_locadora`.`Dim_Tempo` (`SK_TEMPO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Reserva_Dim_Tempo2`
    FOREIGN KEY (`SK_TEMPO_PREV_RETIRADA`)
    REFERENCES `dw_locadora`.`Dim_Tempo` (`SK_TEMPO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Reserva_Dim_Tempo3`
    FOREIGN KEY (`SK_TEMPO_PREV_DEVOLUCAO`)
    REFERENCES `dw_locadora`.`Dim_Tempo` (`SK_TEMPO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Reserva_Dim_Cliente1`
    FOREIGN KEY (`SK_CLIENTE`)
    REFERENCES `dw_locadora`.`Dim_Cliente` (`SK_CLIENTE`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Reserva_Dim_GrupoVeiculo1`
    FOREIGN KEY (`SK_GRUPO_VEICULO`)
    REFERENCES `dw_locadora`.`Dim_GrupoVeiculo` (`SK_GRUPO_VEICULO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Reserva_Dim_Patio1`
    FOREIGN KEY (`SK_PATIO_RETIRADA`)
    REFERENCES `dw_locadora`.`Dim_Patio` (`SK_PATIO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Fato_Reserva_Dim_Patio2`
    FOREIGN KEY (`SK_PATIO_DEVOLUCAO`)
    REFERENCES `dw_locadora`.`Dim_Patio` (`SK_PATIO`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
