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
-- Arquivo: extracao_etl.sql
-- Descrição: Extrai os dados de TODOS os sistemas de origem (Meu Grupo, Grupo 1, Grupo 2 e Grupo 3)
--            e carrega nas tabelas da Staging Area.
-- Acionamento: Agendado para execução diária (ex: 00:00).
-- =======================================================================================

-- ===================================
-- EXTRAÇÃO DOS DADOS - MEU GRUPO
-- Alunos: Gabriel Rodrigues da Silva, Giovanni Paes Leme da Gama Rodrigues, Gabriel Brígido Pinheiro da Silva, Nicolas Viana do Espírito Santo
-- GitHub: https://github.com/gbrods/EEL890_BigData/tree/main/P2_Parte1
-- Schema de origem: `mydb`
-- ===================================

INSERT INTO staging_area.stg_cliente (ID_CLIENTE, NOME_CLIENTE, DOCUMENTO, EMAIL, TELEFONE, FK_ID_ENDERECO, TIPO_PESSOA, FONTE_DADOS, DATA_CARGA)
SELECT ID_CLIENTE, NOME_CLIENTE, DOCUMENTO, EMAIL, TELEFONE, FK_ID_ENDERECO, TIPO_PESSOA, 'MEU_GRUPO', NOW() FROM mydb.Cliente;

INSERT INTO staging_area.stg_veiculo (ID_VEICULO, PLACA, CHASSI, COR_VEICULO, TIPO_MECANIZACAO, POSSUI_AR_CONDICIONADO, POSSUI_CADEIRINHA_CRIANCA, POSSUI_BEBE_CONFORTO, STATUS_VEICULO, FK_ID_MODELO_VEICULO, FK_ID_GRUPO_VEICULO, FK_ID_EMPRESA, FONTE_DADOS, DATA_CARGA)
SELECT ID_VEICULO, PLACA, CHASSI, COR_VEICULO, TIPO_MECANIZACAO, POSSUI_AR_CONDICIONADO, POSSUI_CADEIRINHA_CRIANCA, POSSUI_BEBE_CONFORTO, STATUS_VEICULO, FK_ID_MODELO_VEICULO, FK_ID_GRUPO_VEICULO, FK_ID_EMPRESA, 'MEU_GRUPO', NOW() FROM mydb.Veiculo;

INSERT INTO staging_area.stg_locacao (ID_LOCACAO, DATA_HORA_RETIRADA_REALIZADA, DATA_HORA_DEVOLUCAO_PREVISTA, DATA_HORA_DEVOLUCAO_REALIZADA, VALOR_DIARIA_CONTRATADA, STATUS_LOCACAO, FK_ID_CLIENTE, FK_ID_CONDUTOR, FK_ID_VEICULO, FK_ID_PATIO_RETIRADA, FK_ID_PATIO_DEVOLUCAO, FONTE_DADOS, DATA_CARGA)
SELECT ID_LOCACAO, DATA_HORA_RETIRADA_REALIZADA, DATA_HORA_DEVOLUCAO_PREVISTA, DATA_HORA_DEVOLUCAO_REALIZADA, VALOR_DIARIA_CONTRATADA, STATUS_LOCACAO, FK_ID_CLIENTE, FK_ID_CONDUTOR, FK_ID_VEICULO, FK_ID_PATIO_RETIRADA, FK_ID_PATIO_DEVOLUCAO, 'MEU_GRUPO', NOW() FROM mydb.Locacao;

INSERT INTO staging_area.stg_reserva (ID_RESERVA, DATA_HORA_SOLICITACAO_RESERVA, DATA_HORA_PREVISTA_RETIRADA, DATA_HORA_PREVISTA_DEVOLUCAO, STATUS_RESERVA, FK_ID_CLIENTE, FK_ID_GRUPO_VEICULO, FK_ID_PATIO_RETIRADA, FK_ID_PATIO_DEVOLUCAO, FONTE_DADOS, DATA_CARGA)
SELECT ID_RESERVA, DATA_HORA_SOLICITACAO_RESERVA, DATA_HORA_PREVISTA_RETIRADA, DATA_HORA_PREVISTA_DEVOLUCAO, STATUS_RESERVA, FK_ID_CLIENTE, FK_ID_GRUPO_VEICULO, FK_ID_PATIO_RETIRADA, FK_ID_PATIO_DEVOLUCAO, 'MEU_GRUPO', NOW() FROM mydb.Reserva;

INSERT INTO staging_area.stg_patio (ID_PATIO, NOME_PATIO, FK_ID_ENDERECO, FK_ID_EMPRESA, FONTE_DADOS, DATA_CARGA)
SELECT ID_PATIO, NOME_PATIO, FK_ID_ENDERECO, FK_ID_EMPRESA, 'MEU_GRUPO', NOW() FROM mydb.Patio;

INSERT INTO staging_area.stg_empresa (ID_EMPRESA, NOME_EMPRESA, CNPJ_EMPRESA, FONTE_DADOS, DATA_CARGA)
SELECT ID_EMPRESA, NOME_EMPRESA, CNPJ_EMPRESA, 'MEU_GRUPO', NOW() FROM mydb.Empresa;

INSERT INTO staging_area.stg_grupoveiculo (ID_GRUPO_VEICULO, NOME_GRUPO, DESCRICAO_GRUPO, VALOR_DIARIA_BASE_GRUPO, FONTE_DADOS, DATA_CARGA)
SELECT ID_GRUPO_VEICULO, NOME_GRUPO, DESCRICAO_GRUPO, VALOR_DIARIA_BASE_GRUPO, 'MEU_GRUPO', NOW() FROM mydb.GrupoVeiculo;

INSERT INTO staging_area.stg_modeloveiculo (ID_MODELO_VEICULO, NOME_MODELO, FK_ID_MARCA_VEICULO, FONTE_DADOS, DATA_CARGA)
SELECT ID_MODELO_VEICULO, NOME_MODELO, FK_ID_MARCA_VEICULO, 'MEU_GRUPO', NOW() FROM mydb.ModeloVeiculo;

INSERT INTO staging_area.stg_marcaveiculo (ID_MARCA_VEICULO, NOME_MARCA, FONTE_DADOS, DATA_CARGA)
SELECT ID_MARCA_VEICULO, NOME_MARCA, 'MEU_GRUPO', NOW() FROM mydb.MarcaVeiculo;

INSERT INTO staging_area.stg_endereco (ID_ENDERECO, LOGRADOURO, NUMERO_LOGRADOURO, CEP, FK_ID_BAIRRO, FONTE_DADOS, DATA_CARGA)
SELECT ID_ENDERECO, LOGRADOURO, NUMERO_LOGRADOURO, CEP, FK_ID_BAIRRO, 'MEU_GRUPO', NOW() FROM mydb.Endereco;

INSERT INTO staging_area.stg_bairro (ID_BAIRRO, NOME_BAIRRO, FK_ID_CIDADE, FONTE_DADOS, DATA_CARGA)
SELECT ID_BAIRRO, NOME_BAIRRO, FK_ID_CIDADE, 'MEU_GRUPO', NOW() FROM mydb.Bairro;

INSERT INTO staging_area.stg_cidade (ID_CIDADE, NOME_CIDADE, FK_SIGLA_ESTADO, FONTE_DADOS, DATA_CARGA)
SELECT ID_CIDADE, NOME_CIDADE, FK_SIGLA_ESTADO, 'MEU_GRUPO', NOW() FROM mydb.Cidade;

INSERT INTO staging_area.stg_estado (SIGLA_ESTADO, NOME_ESTADO, FONTE_DADOS, DATA_CARGA)
SELECT SIGLA_ESTADO, NOME_ESTADO, 'MEU_GRUPO', NOW() FROM mydb.Estado;

INSERT INTO staging_area.stg_cobranca (ID_COBRANCA, VALOR_COBRANCA, FK_ID_LOCACAO, FONTE_DADOS, DATA_CARGA)
SELECT ID_COBRANCA, VALOR_COBRANCA, FK_ID_LOCACAO, 'MEU_GRUPO', NOW() FROM mydb.Cobranca;

INSERT INTO staging_area.stg_locacaoprotecao (FK_ID_LOCACAO, FK_ID_PROTECAO_ADICIONAL, VALOR_PROTECAO_APLICADO, FONTE_DADOS, DATA_CARGA)
SELECT FK_ID_LOCACAO, FK_ID_PROTECAO_ADICIONAL, VALOR_PROTECAO_APLICADO, 'MEU_GRUPO', NOW() FROM mydb.LocacaoProtecao;


-- ===================================
-- EXTRAÇÃO DOS DADOS - GRUPO 1
-- Alunos: Fernanda Franco Bottecchia, Jéssica Martins de Oliveira, Kaway Henrique da Rocha Marinho, Rafael Cardim dos Santos, Thiago Dias da Costa
-- GitHub: https://github.com/fernandabottecchia/big-data-p2
-- Schema de origem: `public` (exemplo)
-- ===================================

INSERT INTO staging_area.stg_cliente (ID_CLIENTE, NOME_CLIENTE, DOCUMENTO, EMAIL, TELEFONE, TIPO_PESSOA, FONTE_DADOS, DATA_CARGA)
SELECT id, nome_razao, cpf_cnpj, email, telefone1, tipo, 'GRUPO_1', NOW()
FROM public.Cliente;

INSERT INTO staging_area.stg_veiculo (ID_VEICULO, PLACA, CHASSI, COR_VEICULO, TIPO_MECANIZACAO, POSSUI_AR_CONDICIONADO, POSSUI_CADEIRINHA_CRIANCA, POSSUI_BEBE_CONFORTO, FK_ID_GRUPO_VEICULO, FONTE_DADOS, DATA_CARGA)
SELECT v.id, v.placa, v.chassi, v.cor, v.transmissao, v.ar_condicionado, av.cadeira_de_crianca, av.bebe_conforto, v.grupo_id, 'GRUPO_1', NOW()
FROM public.Veiculo v
LEFT JOIN public.AcessoriosVeiculo av ON v.id = av.veiculo_id;

INSERT INTO staging_area.stg_locacao (ID_LOCACAO, DATA_HORA_RETIRADA_REALIZADA, DATA_HORA_DEVOLUCAO_PREVISTA, DATA_HORA_DEVOLUCAO_REALIZADA, FK_ID_CLIENTE, FK_ID_CONDUTOR, FK_ID_VEICULO, FK_ID_PATIO_RETIRADA, FK_ID_PATIO_DEVOLUCAO, FONTE_DADOS, DATA_CARGA)
SELECT l.id, l.data_retirada, r.data_prev_devolucao, l.data_real_devolucao, r.cliente_id, l.condutor_id, l.veiculo_id, l.patio_saida_id, l.patio_chegada_id, 'GRUPO_1', NOW()
FROM public.Locacao l
LEFT JOIN public.Reserva r ON l.reserva_id = r.id;

INSERT INTO staging_area.stg_reserva (ID_RESERVA, DATA_HORA_PREVISTA_RETIRADA, DATA_HORA_PREVISTA_DEVOLUCAO, FK_ID_CLIENTE, FK_ID_GRUPO_VEICULO, FK_ID_PATIO_RETIRADA, FK_ID_PATIO_DEVOLUCAO, STATUS_RESERVA, FONTE_DADOS, DATA_CARGA)
SELECT id, data_prev_retirada, data_prev_devolucao, cliente_id, grupo_id, patio_retirada_id, patio_devolucao_id, status, 'GRUPO_1', NOW()
FROM public.Reserva;

INSERT INTO staging_area.stg_patio (ID_PATIO, NOME_PATIO, FK_ID_EMPRESA, FONTE_DADOS, DATA_CARGA)
SELECT id, nome, empresa_id, 'GRUPO_1', NOW()
FROM public.Patio;

INSERT INTO staging_area.stg_empresa (ID_EMPRESA, NOME_EMPRESA, CNPJ_EMPRESA, FONTE_DADOS, DATA_CARGA)
SELECT id, nome_fantasia, cnpj, 'GRUPO_1', NOW()
FROM public.Empresa;

INSERT INTO staging_area.stg_grupoveiculo (ID_GRUPO_VEICULO, NOME_GRUPO, DESCRICAO_GRUPO, VALOR_DIARIA_BASE_GRUPO, FONTE_DADOS, DATA_CARGA)
SELECT id, codigo_grupo, descricao, preco_diario, 'GRUPO_1', NOW()
FROM public.GrupoVeiculo;

INSERT INTO staging_area.stg_cobranca (ID_COBRANCA, VALOR_COBRANCA, FK_ID_LOCACAO, FONTE_DADOS, DATA_CARGA)
SELECT id, valor_final, locacao_id, 'GRUPO_1', NOW()
FROM public.Cobranca;

INSERT INTO staging_area.stg_locacaoprotecao (FK_ID_LOCACAO, FK_ID_PROTECAO_ADICIONAL, VALOR_PROTECAO_APLICADO, FONTE_DADOS, DATA_CARGA)
SELECT lp.locacao_id, lp.protecao_id, pa.preco_dia, 'GRUPO_1', NOW()
FROM public.LocacaoProtecao lp
JOIN public.ProtecaoAdicional pa ON lp.protecao_id = pa.id;


-- ===================================
-- EXTRAÇÃO DOS DADOS - GRUPO 2
-- Alunos: Alice Duarte Faria Ribeiro, Beatriz Farias do Nascimento, Gustavo do Amaral Roxo Pereira
-- GitHub: https://github.com/alicedfr/Big-Data-P2
-- Schema de origem: `locadora_veiculos`
-- ===================================

INSERT INTO staging_area.stg_cliente (ID_CLIENTE, NOME_CLIENTE, DOCUMENTO, EMAIL, TELEFONE, TIPO_PESSOA, FONTE_DADOS, DATA_CARGA)
SELECT id_cliente, nome_razao_social, COALESCE(cpf, cnpj), email, telefone, tipo_cliente, 'GRUPO_2', NOW()
FROM locadora_veiculos.CLIENTE;

INSERT INTO staging_area.stg_veiculo (ID_VEICULO, PLACA, CHASSI, COR_VEICULO, TIPO_MECANIZACAO, POSSUI_CADEIRINHA_CRIANCA, FK_ID_MODELO_VEICULO, FK_ID_GRUPO_VEICULO, FK_ID_EMPRESA, STATUS_VEICULO, FONTE_DADOS, DATA_CARGA)
SELECT v.id_veiculo, v.placa, v.chassi, v.cor, v.tipo_mecanizacao, (SELECT 1 FROM locadora_veiculos.VEICULO_ACESSORIO va JOIN locadora_veiculos.ACESSORIO a ON va.id_acessorio = a.id_acessorio WHERE va.id_veiculo = v.id_veiculo AND a.nome_acessorio LIKE '%Cadeirinha%') IS NOT NULL, NULL, v.id_grupo_veiculo, p.id_empresa, v.status_veiculo, 'GRUPO_2', NOW()
FROM locadora_veiculos.VEICULO v
LEFT JOIN locadora_veiculos.PATIO p ON v.id_patio_atual = p.id_patio;

INSERT INTO staging_area.stg_locacao (ID_LOCACAO, DATA_HORA_RETIRADA_REALIZADA, DATA_HORA_DEVOLUCAO_PREVISTA, DATA_HORA_DEVOLUCAO_REALIZADA, VALOR_DIARIA_CONTRATADA, STATUS_LOCACAO, FK_ID_CLIENTE, FK_ID_CONDUTOR, FK_ID_VEICULO, FK_ID_PATIO_RETIRADA, FK_ID_PATIO_DEVOLUCAO, FONTE_DADOS, DATA_CARGA)
SELECT id_locacao, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, valor_total_previsto / (DATEDIFF(data_hora_devolucao_prevista, data_hora_retirada_real) + 1), status_locacao, id_cliente, id_condutor, id_veiculo, id_patio_retirada_real, id_patio_devolucao_real, 'GRUPO_2', NOW()
FROM locadora_veiculos.LOCACAO;

INSERT INTO staging_area.stg_reserva (ID_RESERVA, DATA_HORA_SOLICITACAO_RESERVA, DATA_HORA_PREVISTA_RETIRADA, DATA_HORA_PREVISTA_DEVOLUCAO, STATUS_RESERVA, FK_ID_CLIENTE, FK_ID_GRUPO_VEICULO, FK_ID_PATIO_RETIRADA, FONTE_DADOS, DATA_CARGA)
SELECT id_reserva, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, id_cliente, id_grupo_veiculo, id_patio_retirada_previsto, 'GRUPO_2', NOW()
FROM locadora_veiculos.RESERVA;

INSERT INTO staging_area.stg_patio (ID_PATIO, NOME_PATIO, FK_ID_EMPRESA, FONTE_DADOS, DATA_CARGA)
SELECT id_patio, nome_patio, id_empresa, 'GRUPO_2', NOW()
FROM locadora_veiculos.PATIO;

INSERT INTO staging_area.stg_empresa (ID_EMPRESA, NOME_EMPRESA, CNPJ_EMPRESA, FONTE_DADOS, DATA_CARGA)
SELECT id_empresa, nome_empresa, cnpj, 'GRUPO_2', NOW()
FROM locadora_veiculos.EMPRESA;

INSERT INTO staging_area.stg_grupoveiculo (ID_GRUPO_VEICULO, NOME_GRUPO, DESCRICAO_GRUPO, VALOR_DIARIA_BASE_GRUPO, FONTE_DADOS, DATA_CARGA)
SELECT id_grupo_veiculo, nome_grupo, descricao, valor_diaria_base, 'GRUPO_2', NOW()
FROM locadora_veiculos.GRUPO_VEICULO;

INSERT INTO staging_area.stg_cobranca (ID_COBRANCA, VALOR_COBRANCA, FK_ID_LOCACAO, FONTE_DADOS, DATA_CARGA)
SELECT id_cobranca, valor_final_cobranca, id_locacao, 'GRUPO_2', NOW()
FROM locadora_veiculos.COBRANCA;

INSERT INTO staging_area.stg_locacaoprotecao (FK_ID_LOCACAO, FK_ID_PROTECAO_ADICIONAL, VALOR_PROTECAO_APLICADO, FONTE_DADOS, DATA_CARGA)
SELECT id_locacao, -1, valor_seguro, 'GRUPO_2', NOW()
FROM locadora_veiculos.COBRANCA
WHERE valor_seguro > 0;

-- ===================================
-- EXTRAÇÃO DOS DADOS - GRUPO 3
-- Alunos: Guilherme Oliveira Rolim Silva, Ricardo Lorente Kauer, Vinícius Alcântara Gomes Reis de Souza
-- GitHub: https://github.com/rickauer/datawarehouse
-- Schema de origem: `locadoradb`
-- ===================================

INSERT INTO staging_area.stg_cliente (ID_CLIENTE, NOME_CLIENTE, DOCUMENTO, EMAIL, TELEFONE, FK_ID_ENDERECO, FONTE_DADOS, DATA_CARGA)
SELECT c.ID_Cliente, c.Nome, COALESCE(pf.CPF, pj.CNPJ), c.Email, c.Telefone, c.ID_Endereco, 'GRUPO_3', NOW()
FROM locadoradb.Clientes c
LEFT JOIN locadoradb.Pessoa_Fisica pf ON c.ID_Cliente = pf.ID_Cliente
LEFT JOIN locadoradb.Pessoa_Juridica pj ON c.ID_Cliente = pj.ID_Cliente;

INSERT INTO staging_area.stg_veiculo (ID_VEICULO, PLACA, CHASSI, COR_VEICULO, TIPO_MECANIZACAO, POSSUI_AR_CONDICIONADO, POSSUI_CADEIRINHA_CRIANCA, FK_ID_GRUPO_VEICULO, FONTE_DADOS, DATA_CARGA)
SELECT ID_Veiculo, Placa, Chassi, Cor, Cambio, Ar_Condicionado, Cadeira_de_Bebe, ID_Grupo, 'GRUPO_3', NOW()
FROM locadoradb.Veiculos;

INSERT INTO staging_area.stg_locacao (ID_LOCACAO, DATA_HORA_RETIRADA_REALIZADA, DATA_HORA_DEVOLUCAO_PREVISTA, DATA_HORA_DEVOLUCAO_REALIZADA, VALOR_DIARIA_CONTRATADA, FK_ID_CLIENTE, FK_ID_VEICULO, FK_ID_PATIO_RETIRADA, FK_ID_PATIO_DEVOLUCAO, FONTE_DADOS, DATA_CARGA)
SELECT ID_Locacao, Data_Retirada, Data_Devolucao_Prevista, Data_Devolucao_Efetiva, Valor_Diaria, ID_Cliente, ID_Veiculo, ID_Patio_Retirada, ID_Patio_Devolucao, 'GRUPO_3', NOW()
FROM locadoradb.Locacoes;

INSERT INTO staging_area.stg_reserva (ID_RESERVA, DATA_HORA_SOLICITACAO_RESERVA, DATA_HORA_PREVISTA_RETIRADA, DATA_HORA_PREVISTA_DEVOLUCAO, FK_ID_CLIENTE, FK_ID_GRUPO_VEICULO, FK_ID_PATIO_RETIRADA, FONTE_DADOS, DATA_CARGA)
SELECT ID_Reserva, Data_Reserva, Data_Retirada_Prevista, Data_Devolucao_Prevista, ID_Cliente, ID_Grupo, ID_Patio_Retirada, 'GRUPO_3', NOW()
FROM locadoradb.Reservas;

INSERT INTO staging_area.stg_patio (ID_PATIO, NOME_PATIO, FK_ID_EMPRESA, FONTE_DADOS, DATA_CARGA)
SELECT ID_Patio, Nome, ID_Empresa, 'GRUPO_3', NOW()
FROM locadoradb.Patios;

INSERT INTO staging_area.stg_empresa (ID_EMPRESA, NOME_EMPRESA, CNPJ_EMPRESA, FONTE_DADOS, DATA_CARGA)
SELECT ID_Empresa, Nome, CNPJ, 'GRUPO_3', NOW()
FROM locadoradb.Empresas;

INSERT INTO staging_area.stg_grupoveiculo (ID_GRUPO_VEICULO, NOME_GRUPO, DESCRICAO_GRUPO, VALOR_DIARIA_BASE_GRUPO, FONTE_DADOS, DATA_CARGA)
SELECT ID_Grupo, Nome, Descricao, Valor_Diaria, 'GRUPO_3', NOW()
FROM locadoradb.Grupos;

INSERT INTO staging_area.stg_endereco (ID_ENDERECO, LOGRADOURO, CEP, FONTE_DADOS, DATA_CARGA)
SELECT ID_Endereco, CONCAT(Rua, ', ', Numero, ' - ', Complemento), CEP, 'GRUPO_3', NOW()
FROM locadoradb.Enderecos;

INSERT INTO staging_area.stg_cobranca (ID_COBRANCA, VALOR_COBRANCA, FK_ID_LOCACAO, FONTE_DADOS, DATA_CARGA)
SELECT ID_Pagamento, Valor_Total_Pago, ID_Locacao, 'GRUPO_3', NOW()
FROM locadoradb.Pagamentos;

INSERT INTO staging_area.stg_locacaoprotecao (FK_ID_LOCACAO, FK_ID_PROTECAO_ADICIONAL, VALOR_PROTECAO_APLICADO, FONTE_DADOS, DATA_CARGA)
SELECT sc.ID_Locacao, sc.ID_Seguro, s.Valor_Diario, 'GRUPO_3', NOW()
FROM locadoradb.Seguros_Contratados sc
JOIN locadoradb.Seguros s ON sc.ID_Seguro = s.ID_Seguro;