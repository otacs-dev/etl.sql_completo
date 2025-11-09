-- *************************************************************************************************
-- DDL para criação do Banco de Dados PEDIDO e tabelas RAW/Staging - SOLUÇÃO DE AMBIENTE
-- *************************************************************************************************

-- 0. LIMPEZA (EXECUTE ESTA SEÇÃO SE VOCÊ TEVE ERROS ANTERIORES)
-- Isso garante que não haja conflito de nomes (Mensagem 2714) e remove o banco de dados com falha.

IF DB_ID('PEDIDO') IS NOT NULL
BEGIN
    PRINT 'Excluindo o banco de dados PEDIDO existente para recriação limpa...';
    ALTER DATABASE PEDIDO SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE PEDIDO;
END
GO

-- Tenta remover as tabelas RAW acidentalmente criadas no banco Master, se aplicável.
-- Para executar as linhas abaixo, selecione o banco 'master' ou 'tempdb' no SSMS.
IF OBJECT_ID('master.dbo.cidades','U') IS NOT NULL DROP TABLE master.dbo.cidades;
IF OBJECT_ID('master.dbo.clientes','U') IS NOT NULL DROP TABLE master.dbo.clientes;
-- Adicione mais DROP TABLE aqui para outras tabelas RAW se o erro 2714 persistir no master.

GO

-- 1. CRIAÇÃO DO BANCO DE DADOS PEDIDO (Sintaxe simplificada para evitar o Erro 1036)
-- Omitindo as cláusulas ON PRIMARY e LOG ON. O SQL Server usará todas as configurações padrão.
CREATE DATABASE PEDIDO;
GO 
-- O comando GO força a execução do CREATE DATABASE antes de prosseguir.


-- 2. CRIAÇÃO DAS TABELAS PARA O LANDIGN DOS DADOS (RAW AREA)
USE [PEDIDO];
GO 
-- O comando GO garante que o contexto foi alterado para o banco PEDIDO.

CREATE TABLE cidades(
    id_cidade varchar(50) NULL,
    nome_cidade varchar(50) NULL,
    uf varchar(50) NULL);

-- Corrigido: 'data_nasciment]' para 'data_nascimento'
CREATE TABLE clientes(
    id_cliente varchar(50) NULL,
    nome varchar(50) NULL,
    data_nascimento date NULL, 
    sexo varchar(50) NULL,
    id_cidade varchar(50) NULL); 

CREATE TABLE entregas(
    id_entrega varchar(50) NULL,
    id_pedido varchar(50) NULL,
    data_entrega date NULL,
    transportadora varchar(50) NULL,
    custo_frete varchar(50) NULL);

CREATE TABLE itens_pedidos(
    id_pedido varchar(50) NULL,
    id_produto varchar(50) NULL,
    quantidade varchar(50) NULL,
    preco_unitario varchar(50) NULL,
    desconto varchar(50) NULL);

CREATE TABLE lojas(
    id_loja varchar(50) NULL,
    nome varchar(50) NULL,
    id_cidade varchar(50) NULL,
    gerente varchar(50) NULL);

CREATE TABLE pedidos(
    id_pedido varchar(50) NULL,
    id_cliente varchar(50) NULL,
    id_vendedor varchar(50) NULL,
    data_pedido date NULL,
    valor_total varchar(50) NULL);

-- Corrigido: 'preco_unitario varchar](50)' para 'preco_unitario varchar(50)'
CREATE TABLE produtos(
    id_produto varchar(50) NULL,
    nome varchar(50) NULL,
    categoria varchar(50) NULL,
    marca varchar(50) NULL,
    preco_unitario varchar(50) NULL);

CREATE TABLE [dbo].[vendedor](
    [id_vendedor] [varchar](50) NULL,
    [nome] [varchar](50) NULL,
    [id_loja] [varchar](50) NULL);

GO

-- 3. CRIAÇÃO DAS TABELAS PARA OS DADOS TRANSFORMADOS (AREA DE PRE_CARGA/DIMENSÕES E FATOS)
-- Para evitar o erro "Já existe um objeto com nome...", usamos DROP TABLE antes de criar.

IF OBJECT_ID('dbo.FATO_VENDA','U') IS NOT NULL DROP TABLE dbo.FATO_VENDA;
IF OBJECT_ID('dbo.DIM_VENDEDOR','U') IS NOT NULL DROP TABLE dbo.DIM_VENDEDOR;
IF OBJECT_ID('dbo.DIM_TRANSPORTE','U') IS NOT NULL DROP TABLE dbo.DIM_TRANSPORTE;
IF OBJECT_ID('dbo.DIM_TEMPO','U') IS NOT NULL DROP TABLE dbo.DIM_TEMPO;
IF OBJECT_ID('dbo.DIM_PRODUTO','U') IS NOT NULL DROP TABLE dbo.DIM_PRODUTO;
IF OBJECT_ID('dbo.DIM_LOJA','U') IS NOT NULL DROP TABLE dbo.DIM_LOJA;
IF OBJECT_ID('dbo.DIM_GERENTE','U') IS NOT NULL DROP TABLE dbo.DIM_GERENTE;
IF OBJECT_ID('dbo.DIM_FX_ETARIA','U') IS NOT NULL DROP TABLE dbo.DIM_FX_ETARIA;
IF OBJECT_ID('dbo.DIM_CLIENTE','U') IS NOT NULL DROP TABLE dbo.DIM_CLIENTE;
IF OBJECT_ID('dbo.DIM_CIDADE','U') IS NOT NULL DROP TABLE dbo.DIM_CIDADE;
GO


CREATE TABLE DIM_CIDADE(
    id_cidade int NOT NULL,
    nome_cidade varchar(50) NULL,
    nome_estado varchar(50) NULL,
 CONSTRAINT PK_DIM_CIDADE PRIMARY KEY CLUSTERED (id_cidade));

CREATE TABLE DIM_CLIENTE(
    id_cliente int NOT NULL,
    nome_cliente varchar(50) NULL,
    sexo varchar(10) NULL,
 CONSTRAINT PK_DIM_CLIENTE PRIMARY KEY CLUSTERED (id_cliente));

CREATE TABLE DIM_FX_ETARIA(
    id_fx_etaria int NOT NULL,
    faixa_etaria varchar(50) NULL,
 CONSTRAINT PK_DIM_FX_ETARIA PRIMARY KEY CLUSTERED (id_fx_etaria));

CREATE TABLE DIM_GERENTE(
    id_gerente char(3) NOT NULL,
    nome_gerente varchar(50) NULL,
 CONSTRAINT PK_DIM_GERENTE PRIMARY KEY CLUSTERED (id_gerente));

CREATE TABLE DIM_LOJA(
    id_loja int NOT NULL,
    nome_loja varchar(50) NULL,
 CONSTRAINT PK_DIM_LOJA PRIMARY KEY CLUSTERED (id_loja));

CREATE TABLE DIM_PRODUTO(
    id_produto int NOT NULL,
    nome_produto varchar(50) NULL,
    marca varchar(50) NULL,
    categoria varchar(50) NULL,
 CONSTRAINT PK_DIM_PRODUTO PRIMARY KEY CLUSTERED (id_produto));

CREATE TABLE DIM_TEMPO(
    id_data date NOT NULL,
    dia_semana varchar(50) NULL,
    mes varchar(50) NULL,
    ano varchar(4) NULL,
 CONSTRAINT PK_DIM_TEMPO PRIMARY KEY CLUSTERED (id_data));

CREATE TABLE DIM_TRANSPORTE(
    id_transportadora char(3) NOT NULL,
    nome_transportadora varchar(50) NULL,
 CONSTRAINT PK_DIM_TRANSPORTE PRIMARY KEY CLUSTERED (id_transportadora));

CREATE TABLE DIM_VENDEDOR(
    id_vendedor int NOT NULL,
    nome_vendedor varchar(50) NULL,
 CONSTRAINT PK_DIM_VENDEDOR PRIMARY KEY CLUSTERED (id_vendedor));
 

CREATE TABLE FATO_VENDA(
    sk_venda varchar(50) NOT NULL,
    dt_venda date NULL,
    dt_envio date NULL,
    dt_entrega date NULL,
    id_cliente int NULL,
    id_fx_etaria int NULL,
    id_cidade_cli int NULL,
    id_produto int NULL,
    id_vendedor int NULL,
    id_loja int NULL,
    id_cidade_loja int NULL,
    id_gerente char(3) NULL,
    id_transportadora char(3) NULL,
    qtde_vendida float NULL,
    vl_bruto_venda float NULL,
    vl_liq_venda float NULL,
    vl_desconto float NULL,
    vl_frete float NULL,
    tmp_envio int NULL,
    tmp_preparo int NULL,
 CONSTRAINT PK_FATO_VENDA PRIMARY KEY CLUSTERED (sk_venda));

-- 4. CHAVES ESTRANGEIRAS
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_CIDADE FOREIGN KEY(id_cidade_cli) REFERENCES DIM_CIDADE (id_cidade)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_CIDADE1 FOREIGN KEY(id_cidade_loja) REFERENCES DIM_CIDADE (id_cidade)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_CLIENTE FOREIGN KEY(id_cliente) REFERENCES DIM_CLIENTE (id_cliente) 
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_FX_ETARIA FOREIGN KEY(id_fx_etaria) REFERENCES DIM_FX_ETARIA (id_fx_etaria)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_GERENTE FOREIGN KEY(id_gerente) REFERENCES DIM_GERENTE (id_gerente)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_LOJA FOREIGN KEY(id_loja) REFERENCES DIM_LOJA (id_loja)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_PRODUTO FOREIGN KEY(id_produto) REFERENCES DIM_PRODUTO (id_produto)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_TEMPO FOREIGN KEY(dt_venda) REFERENCES DIM_TEMPO (id_data)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_TEMPO1 FOREIGN KEY(dt_envio) REFERENCES DIM_TEMPO (id_data)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_TEMPO2 FOREIGN KEY(dt_entrega) REFERENCES DIM_TEMPO (id_data)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_TRANSPORTE FOREIGN KEY(id_transportadora) REFERENCES DIM_TRANSPORTE (id_transportadora)
ALTER TABLE FATO_VENDA WITH CHECK ADD CONSTRAINT FK_FATO_VENDA_DIM_VENDEDOR FOREIGN KEY(id_vendedor) REFERENCES DIM_VENDEDOR (id_vendedor)
GO