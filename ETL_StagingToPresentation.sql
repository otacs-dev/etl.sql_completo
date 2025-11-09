/*******************************************************************************************
ETL – Staging (RAW PEDIDO) -> Presentation (DW_PEDIDO)
Alvo: Microsoft SQL Server (T-SQL)

Premissas
- Base RAW/Staging: PEDIDO (tabelas: clientes, cidades, lojas, vendedores, gerentes, produtos, transportadoras, vendas).
- Base Presentation (não volátil): DW_PEDIDO (dim_*, fato_*).
- Chaves naturais já existem nas tabelas da RAW.
- Este script é idempotente (DROP/CREATE) e usa UPSERT (MERGE) nas dimensões.

CORREÇÃO: As dimensões são carregadas diretamente das tabelas RAW (PEDIDO.dbo.clientes, etc.), 
ignnorando o uso da área de pré-carga (que também tinha o nome DIM_CLIENTE).
*******************************************************************************************/

/*=========================================================================================
1) CRIAÇÃO DA BASE DE APRESENTAÇÃO
=========================================================================================*/
IF DB_ID('DW_PEDIDO') IS NULL
    CREATE DATABASE DW_PEDIDO;
GO

USE DW_PEDIDO;
GO

/*=========================================================================================
2) DDL – DIMENSÕES E FATO (DROP/CREATE)
=========================================================================================*/
-- Drop em ordem de dependência
IF OBJECT_ID('dbo.FATO_VENDA','U') IS NOT NULL DROP TABLE dbo.FATO_VENDA;
IF OBJECT_ID('dbo.DIM_TEMPO','U') IS NOT NULL DROP TABLE dbo.DIM_TEMPO;
IF OBJECT_ID('dbo.DIM_CLIENTE','U') IS NOT NULL DROP TABLE dbo.DIM_CLIENTE;
IF OBJECT_ID('dbo.DIM_FX_ETARIA','U') IS NOT NULL DROP TABLE dbo.DIM_FX_ETARIA;
IF OBJECT_ID('dbo.DIM_CIDADE','U') IS NOT NULL DROP TABLE dbo.DIM_CIDADE;
IF OBJECT_ID('dbo.DIM_PRODUTO','U') IS NOT NULL DROP TABLE dbo.DIM_PRODUTO;
IF OBJECT_ID('dbo.DIM_VENDEDOR','U') IS NOT NULL DROP TABLE dbo.DIM_VENDEDOR;
IF OBJECT_ID('dbo.DIM_GERENTE','U') IS NOT NULL DROP TABLE dbo.DIM_GERENTE;
IF OBJECT_ID('dbo.DIM_LOJA','U') IS NOT NULL DROP TABLE dbo.DIM_LOJA;
IF OBJECT_ID('dbo.DIM_TRANSPORTE','U') IS NOT NULL DROP TABLE dbo.DIM_TRANSPORTE;
GO

-- DIMENSÕES
CREATE TABLE dbo.DIM_CLIENTE(
    id_cliente       INT            NOT NULL PRIMARY KEY,
    nome             VARCHAR(100)   NULL,
    sexo             CHAR(1)        NULL,
    id_cidade_cli    VARCHAR(50)    NULL -- lookup para DIM_CIDADE
);

CREATE TABLE dbo.DIM_FX_ETARIA(
    id_fx_etaria     INT           NOT NULL PRIMARY KEY,
    faixa            VARCHAR(30)   NOT NULL
);
-- Valor 0 = 'não informada' é útil para FKs opcionais
INSERT INTO dbo.DIM_FX_ETARIA (id_fx_etaria, faixa) VALUES
(0,'não informada'),
(1,'até 18 anos'),
(2,'de 19 a 30 anos'),
(3,'de 31 a 50 anos'),
(4,'acima de 50 anos');

CREATE TABLE dbo.DIM_CIDADE(
    id_cidade    VARCHAR(50)   NOT NULL PRIMARY KEY,
    nome_cidade  VARCHAR(100)  NULL,
    uf           VARCHAR(2)    NULL
);

CREATE TABLE dbo.DIM_GERENTE(
    id_gerente     VARCHAR(10)  NOT NULL PRIMARY KEY,
    nome_gerente   VARCHAR(100) NULL
);

CREATE TABLE dbo.DIM_LOJA(
    id_loja     INT           NOT NULL PRIMARY KEY,
    nome_loja   VARCHAR(100)  NULL,
    id_cidade   VARCHAR(50)   NULL  -- lookup para DIM_CIDADE
);

CREATE TABLE dbo.DIM_VENDEDOR(
    id_vendedor    INT           NOT NULL PRIMARY KEY,
    nome_vendedor  VARCHAR(100)  NULL,
    id_gerente     VARCHAR(10)   NULL  -- lookup para DIM_GERENTE
);

CREATE TABLE dbo.DIM_PRODUTO(
    id_produto     INT            NOT NULL PRIMARY KEY,
    nome_produto   VARCHAR(150)   NULL,
    marca          VARCHAR(60)    NULL,
    categoria      VARCHAR(100)   NULL
);

CREATE TABLE dbo.DIM_TRANSPORTE(
    id_transportadora  INT           NOT NULL PRIMARY KEY,
    nome_transportadora VARCHAR(120) NULL
);

CREATE TABLE dbo.DIM_TEMPO(
    id_data     DATE         NOT NULL PRIMARY KEY,
    ano         SMALLINT     NOT NULL,
    mes         TINYINT      NOT NULL,
    dia         TINYINT      NOT NULL,
    trimestre   TINYINT      NOT NULL,
    mes_nome    VARCHAR(15)  NULL,
    dia_semana  TINYINT      NULL
);

-- FATO
CREATE TABLE dbo.FATO_VENDA(
    -- chaves de tempo
    dt_venda     DATE         NOT NULL,
    dt_envio     DATE         NULL,
    dt_entrega   DATE         NULL,

    -- chaves de dimensão
    id_cliente      INT           NOT NULL,
    id_fx_etaria    INT           NOT NULL DEFAULT(0),
    id_cidade_cli   VARCHAR(50)   NULL,
    id_produto      INT           NOT NULL,
    id_vendedor     INT           NULL,
    id_loja         INT           NULL,
    id_cidade_loja  VARCHAR(50)   NULL,
    id_gerente      VARCHAR(10)   NULL,
    id_transporte   INT           NULL,

    -- métricas (opcional)
    quantidade      INT           NULL,
    valor_total     DECIMAL(14,2) NULL,
    valor_frete     DECIMAL(14,2) NULL,
    valor_desconto  DECIMAL(14,2) NULL
);
GO

-- FKs
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_TEMPO_VENDA
    FOREIGN KEY (dt_venda)   REFERENCES dbo.DIM_TEMPO (id_data);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_TEMPO_ENVIO
    FOREIGN KEY (dt_envio)   REFERENCES dbo.DIM_TEMPO (id_data);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_TEMPO_ENTREGA
    FOREIGN KEY (dt_entrega) REFERENCES dbo.DIM_TEMPO (id_data);

ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_CLIENTE
    FOREIGN KEY (id_cliente)     REFERENCES dbo.DIM_CLIENTE (id_cliente);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_FX
    FOREIGN KEY (id_fx_etaria)   REFERENCES dbo.DIM_FX_ETARIA (id_fx_etaria);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_CIDADE_CLI
    FOREIGN KEY (id_cidade_cli)  REFERENCES dbo.DIM_CIDADE (id_cidade);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_PRODUTO
    FOREIGN KEY (id_produto)     REFERENCES dbo.DIM_PRODUTO (id_produto);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_VENDEDOR
    FOREIGN KEY (id_vendedor)    REFERENCES dbo.DIM_VENDEDOR (id_vendedor);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_LOJA
    FOREIGN KEY (id_loja)        REFERENCES dbo.DIM_LOJA (id_loja);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_CIDADE_LOJA
    FOREIGN KEY (id_cidade_loja) REFERENCES dbo.DIM_CIDADE (id_cidade);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_GERENTE
    FOREIGN KEY (id_gerente)     REFERENCES dbo.DIM_GERENTE (id_gerente);
ALTER TABLE dbo.FATO_VENDA  WITH CHECK ADD CONSTRAINT FK_FV_TRANSP
    FOREIGN KEY (id_transporte)  REFERENCES dbo.DIM_TRANSPORTE (id_transportadora);
GO

/*=========================================================================================
3) CARGA – UPSERT DAS DIMENSÕES A PARTIR DA RAW (PEDIDO.dbo.TABELA)
=========================================================================================*/
-- CLIENTE (Type 1) - **CORRIGIDO: Fonte alterada de PEDIDO.dbo.DIM_CLIENTE para PEDIDO.dbo.clientes**
MERGE dbo.DIM_CLIENTE AS D
USING (
    SELECT C.id_cliente, C.nome, C.sexo, C.id_cidade
    FROM PEDIDO.dbo.clientes AS C
) AS S
ON (D.id_cliente = S.id_cliente)
WHEN MATCHED THEN UPDATE SET
    D.nome = S.nome,
    D.sexo = S.sexo,
    D.id_cidade_cli = S.id_cidade
WHEN NOT MATCHED THEN INSERT (id_cliente, nome, sexo, id_cidade_cli)
VALUES (S.id_cliente, S.nome, S.sexo, S.id_cidade);

-- CIDADE - **CORRIGIDO: Fonte alterada de PEDIDO.dbo.cidades (se a DIM_CIDADE estivesse no PEDIDO)**
MERGE dbo.DIM_CIDADE AS D
USING (
    SELECT id_cidade, nome_cidade, uf
    FROM PEDIDO.dbo.cidades
) AS S
ON (D.id_cidade = S.id_cidade)
WHEN MATCHED THEN UPDATE SET
    D.nome_cidade = S.nome_cidade,
    D.uf = S.uf
WHEN NOT MATCHED THEN INSERT (id_cidade, nome_cidade, uf)
VALUES (S.id_cidade, S.nome_cidade, S.uf);

-- GERENTE
MERGE dbo.DIM_GERENTE AS D
USING (
    SELECT G.id_gerente, G.nome_gerente
    FROM PEDIDO.dbo.gerentes AS G -- Assume que a tabela RAW é 'gerentes'
) AS S
ON (D.id_gerente = S.id_gerente)
WHEN MATCHED THEN UPDATE SET D.nome_gerente = S.nome_gerente
WHEN NOT MATCHED THEN INSERT (id_gerente, nome_gerente)
VALUES (S.id_gerente, S.nome_gerente);

-- LOJA
MERGE dbo.DIM_LOJA AS D
USING (
    SELECT L.id_loja, L.nome, L.id_cidade
    FROM PEDIDO.dbo.lojas AS L -- Assumindo que o nome da coluna é 'nome' na tabela RAW
) AS S
ON (D.id_loja = S.id_loja)
WHEN MATCHED THEN UPDATE SET
    D.nome_loja = S.nome,
    D.id_cidade = S.id_cidade
WHEN NOT MATCHED THEN INSERT (id_loja, nome_loja, id_cidade)
VALUES (S.id_loja, S.nome, S.id_cidade);

-- VENDEDOR
MERGE dbo.DIM_VENDEDOR AS D
USING (
    -- **CORRIGIDO: Uso da tabela 'vendedor' e ajuste da coluna de gerente**
    SELECT 
        V.id_vendedor, 
        V.nome, 
        L.gerente AS id_gerente -- Buscar o gerente associado à loja
    FROM PEDIDO.dbo.vendedor AS V
    LEFT JOIN PEDIDO.dbo.lojas AS L ON L.id_loja = V.id_loja
) AS S
ON (D.id_vendedor = S.id_vendedor)
WHEN MATCHED THEN UPDATE SET
    D.nome_vendedor = S.nome,
    D.id_gerente = S.id_gerente -- Note: Este FK deve ser VARCHAR(10) ou o tipo correto na DIM_VENDEDOR
WHEN NOT MATCHED THEN INSERT (id_vendedor, nome_vendedor, id_gerente)
VALUES (S.id_vendedor, S.nome, S.id_gerente);

-- PRODUTO
MERGE dbo.DIM_PRODUTO AS D
USING (
    -- **CORRIGIDO: Assumindo que o nome da coluna é 'nome' e não 'nome_produto'**
    SELECT P.id_produto, P.nome, P.marca, P.categoria
    FROM PEDIDO.dbo.produtos AS P
) AS S
ON (D.id_produto = S.id_produto)
WHEN MATCHED THEN UPDATE SET
    D.nome_produto = S.nome,
    D.marca = S.marca,
    D.categoria = S.categoria
WHEN NOT MATCHED THEN INSERT (id_produto, nome_produto, marca, categoria)
VALUES (S.id_produto, S.nome, S.marca, S.categoria);

-- TRANSPORTE
MERGE dbo.DIM_TRANSPORTE AS D
USING (
    -- **CORRIGIDO: Usando a tabela 'entregas' para obter os nomes das transportadoras**
    SELECT DISTINCT 
        CAST(SUBSTRING(T.transportadora, 1, 3) AS INT) AS id_transportadora, -- Gerando um ID temporário ou ajustando o tipo
        T.transportadora AS nome_transportadora
    FROM PEDIDO.dbo.entregas AS T
    WHERE T.transportadora IS NOT NULL
) AS S
ON (D.id_transportadora = S.id_transportadora)
WHEN MATCHED THEN UPDATE SET D.nome_transportadora = S.nome_transportadora
WHEN NOT MATCHED THEN INSERT (id_transportadora, nome_transportadora)
VALUES (S.id_transportadora, S.nome_transportadora);

-- DIM_TEMPO (datas distintas das colunas de vendas) - **Usando as tabelas RAW de PEDIDOS e ENTREGAS**
;WITH datas AS (
    SELECT CAST(data_pedido AS DATE) AS dt FROM PEDIDO.dbo.pedidos WHERE data_pedido IS NOT NULL
    UNION
    SELECT CAST(data_entrega AS DATE) FROM PEDIDO.dbo.entregas WHERE data_entrega IS NOT NULL
)
MERGE dbo.DIM_TEMPO AS D
USING (
    SELECT DISTINCT dt AS id_data,
           YEAR(dt)        AS ano,
           MONTH(dt)       AS mes,
           DAY(dt)         AS dia,
           DATEPART(QUARTER, dt) AS trimestre,
           DATENAME(MONTH, dt)   AS mes_nome,
           DATEPART(WEEKDAY, dt) AS dia_semana
    FROM datas
) AS S
ON (D.id_data = S.id_data)
WHEN MATCHED THEN UPDATE SET
    D.ano = S.ano, D.mes = S.mes, D.dia = S.dia, D.trimestre = S.trimestre,
    D.mes_nome = S.mes_nome, D.dia_semana = S.dia_semana
WHEN NOT MATCHED THEN INSERT (id_data, ano, mes, dia, trimestre, mes_nome, dia_semana)
VALUES (S.id_data, S.ano, S.mes, S.dia, S.trimestre, S.mes_nome, S.dia_semana);

PRINT 'Dimensões carregadas com sucesso.';

 
/*=========================================================================================
4) CARGA – FATO_VENDA
=========================================================================================*/
-- **Refatorado para usar as tabelas RAW (pedidos, itens_pedidos, entregas, etc.)**
INSERT INTO dbo.FATO_VENDA (
    dt_venda, dt_envio, dt_entrega,
    id_cliente, id_fx_etaria, id_cidade_cli,
    id_produto, id_vendedor, id_loja, id_cidade_loja, id_gerente, id_transporte,
    quantidade, valor_total, valor_frete, valor_desconto
)
SELECT
    P.data_pedido AS dt_venda,
    E.data_entrega AS dt_envio, -- Usando data_entrega como dt_envio e dt_entrega
    E.data_entrega AS dt_entrega,
    P.id_cliente,
    -- Faixa Etária: Replicando a lógica do ETL_COMPLETO.sql, mas usando data_nascimento (assumindo que o nome da coluna é data_nascimento)
    ISNULL(
        CASE
            -- Tenta converter a data de nascimento, se falhar ou for NULL, usa 0 (não informada)
            WHEN C.data_nascimento IS NULL THEN 0
            WHEN DATEDIFF(YEAR, C.data_nascimento, GETDATE()) < 19 THEN 1
            WHEN DATEDIFF(YEAR, C.data_nascimento, GETDATE()) BETWEEN 19 AND 30 THEN 2
            WHEN DATEDIFF(YEAR, C.data_nascimento, GETDATE()) BETWEEN 31 AND 50 THEN 3
            WHEN DATEDIFF(YEAR, C.data_nascimento, GETDATE()) > 50 THEN 4
            ELSE 0 -- Caso haja alguma falha na data
        END, 0) AS id_fx_etaria,
    C.id_cidade AS id_cidade_cli,
    IP.id_produto,
    P.id_vendedor,
    V.id_loja,
    L.id_cidade AS id_cidade_loja,
    L.gerente AS id_gerente, -- Assumindo que a coluna 'gerente' na tabela LOJAS é o FK para DIM_GERENTE
    CAST(SUBSTRING(E.transportadora, 1, 3) AS INT) AS id_transporte,
    CAST(IP.quantidade AS INT) AS quantidade,
    (CAST(IP.preco_unitario AS FLOAT) * CAST(IP.quantidade AS FLOAT)) AS valor_total,
    -- Aqui você precisa decidir como calcular o frete e desconto, vamos usar os valores da tabela RAW
    NULL AS valor_frete, -- Sem valor de frete por item facilmente disponível. Você pode ajustar esta lógica.
    CAST(IP.desconto AS FLOAT) AS valor_desconto
FROM PEDIDO.dbo.itens_pedidos AS IP
JOIN PEDIDO.dbo.pedidos AS P ON P.id_pedido = IP.id_pedido
LEFT JOIN PEDIDO.dbo.clientes AS C ON C.id_cliente = P.id_cliente
LEFT JOIN PEDIDO.dbo.entregas AS E ON E.id_pedido = P.id_pedido
LEFT JOIN PEDIDO.dbo.vendedor AS V ON V.id_vendedor = P.id_vendedor
LEFT JOIN PEDIDO.dbo.lojas AS L ON L.id_loja = V.id_loja; -- Corrigido: Join entre vendedor e loja

PRINT 'Fato carregada com sucesso.';

-- Exemplo de validações rápidas
SELECT COUNT(*) AS qt_clientes   FROM dbo.DIM_CLIENTE;
SELECT COUNT(*) AS qt_produtos   FROM dbo.DIM_PRODUTO;
SELECT COUNT(*) AS qt_lojas      FROM dbo.DIM_LOJA;
SELECT COUNT(*) AS qt_cidades    FROM dbo.DIM_CIDADE;
SELECT COUNT(*) AS qt_vendas     FROM dbo.FATO_VENDA;

 
/*=========================================================================================
5) (OPCIONAL) LIMPAR STAGING
   A área Staging/RAW é volátil; limpe somente se sua atividade pedir essa remoção.
=========================================================================================*/
-- TRUNCATE TABLE PEDIDO.dbo.vendas;
-- TRUNCATE TABLE PEDIDO.dbo.clientes;
-- TRUNCATE TABLE PEDIDO.dbo.lojas;
-- TRUNCATE TABLE PEDIDO.dbo.vendedores;
-- TRUNCATE TABLE PEDIDO.dbo.gerentes;
-- TRUNCATE TABLE PEDIDO.dbo.produtos;
-- TRUNCATE TABLE PEDIDO.dbo.transportadoras;
-- TRUNCATE TABLE PEDIDO.dbo.cidades;