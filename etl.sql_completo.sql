/********************************************************************************************
 * Processo de ETL - Rede de Lojas (modelo dimensional)
 *
 * Etapas previstas:
 *  1) Preparação do ambiente e das tabelas de estágio
 *  2) Carga dos arquivos CSV (Extract)
 *  3) Padronização e enriquecimento dos dados (Transform)
 *  4) Carga das dimensões e da tabela fato (Load)
 *
 * Observações importantes:
 *  - Ajustar a variável DATA_PATH com o diretório onde os arquivos .CSV estão disponíveis.
 *  - Os BULK INSERTs consideram arquivos UTF-8 com separador ';' e terminador Windows (CRLF).
 *  - Executar este script em um servidor Microsoft SQL Server (compatível a partir da versão 2017).
 ********************************************************************************************/

:setvar DATA_PATH "C:\\Users\\ocpla\\Downloads\\etl.sql_completo-main"

/* ------------------------------------------------------------------------------------------
   1. Criação do banco e schemas
------------------------------------------------------------------------------------------ */
IF DB_ID('DW_REDE_LOJAS') IS NULL
    CREATE DATABASE DW_REDE_LOJAS;
GO

USE DW_REDE_LOJAS;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg AUTHORIZATION dbo;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw AUTHORIZATION dbo;');
GO

/* ------------------------------------------------------------------------------------------
   2. Tabelas de estágio (dados brutos)
------------------------------------------------------------------------------------------ */
IF OBJECT_ID('stg.clientes', 'U') IS NOT NULL DROP TABLE stg.clientes;
CREATE TABLE stg.clientes (
    id_cliente        NVARCHAR(20)  NULL,
    nome              NVARCHAR(200) NULL,
    data_nascimento   NVARCHAR(20)  NULL,
    sexo              NVARCHAR(10)  NULL,
    id_cidade         NVARCHAR(20)  NULL
);
GO

IF OBJECT_ID('stg.cidades', 'U') IS NOT NULL DROP TABLE stg.cidades;
CREATE TABLE stg.cidades (
    id_cidade   NVARCHAR(20)  NULL,
    nome_cidade NVARCHAR(200) NULL,
    uf          NVARCHAR(10)  NULL
);
GO

IF OBJECT_ID('stg.produtos', 'U') IS NOT NULL DROP TABLE stg.produtos;
CREATE TABLE stg.produtos (
    id_produto      NVARCHAR(20)  NULL,
    nome            NVARCHAR(200) NULL,
    categoria       NVARCHAR(100) NULL,
    marca           NVARCHAR(100) NULL,
    preco_unitario  NVARCHAR(50)  NULL
);
GO

IF OBJECT_ID('stg.lojas', 'U') IS NOT NULL DROP TABLE stg.lojas;
CREATE TABLE stg.lojas (
    id_loja   NVARCHAR(20)  NULL,
    nome      NVARCHAR(200) NULL,
    id_cidade NVARCHAR(20)  NULL,
    gerente   NVARCHAR(200) NULL
);
GO

IF OBJECT_ID('stg.vendedores', 'U') IS NOT NULL DROP TABLE stg.vendedores;
CREATE TABLE stg.vendedores (
    id_vendedor NVARCHAR(20)  NULL,
    nome        NVARCHAR(200) NULL,
    id_loja     NVARCHAR(20)  NULL
);
GO

IF OBJECT_ID('stg.pedidos', 'U') IS NOT NULL DROP TABLE stg.pedidos;
CREATE TABLE stg.pedidos (
    id_pedido    NVARCHAR(20) NULL,
    id_cliente   NVARCHAR(20) NULL,
    id_vendedor  NVARCHAR(20) NULL,
    data_pedido  NVARCHAR(20) NULL,
    valor_total  NVARCHAR(50) NULL
);
GO

IF OBJECT_ID('stg.itens_pedidos', 'U') IS NOT NULL DROP TABLE stg.itens_pedidos;
CREATE TABLE stg.itens_pedidos (
    id_pedido      NVARCHAR(20) NULL,
    id_produto     NVARCHAR(20) NULL,
    quantidade     NVARCHAR(20) NULL,
    preco_unitario NVARCHAR(50) NULL,
    desconto       NVARCHAR(50) NULL
);
GO

IF OBJECT_ID('stg.entregas', 'U') IS NOT NULL DROP TABLE stg.entregas;
CREATE TABLE stg.entregas (
    id_entrega     NVARCHAR(20)  NULL,
    id_pedido      NVARCHAR(20)  NULL,
    data_entrega   NVARCHAR(20)  NULL,
    transportadora NVARCHAR(200) NULL,
    custo_frete    NVARCHAR(50)  NULL
);
GO

/* ------------------------------------------------------------------------------------------
   3. Carga dos arquivos (Extract)
   Ajustar o caminho conforme ambiente utilizando a variável SQLCMD $(DATA_PATH)
------------------------------------------------------------------------------------------ */
BULK INSERT stg.clientes
FROM '$(DATA_PATH)\\clientes.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '0x0d0a'
);

BULK INSERT stg.cidades
FROM '$(DATA_PATH)\\cidades.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '0x0d0a'
);

BULK INSERT stg.produtos
FROM '$(DATA_PATH)\\produtos.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '0x0d0a'
);

BULK INSERT stg.lojas
FROM '$(DATA_PATH)\\lojas.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '0x0d0a'
);

BULK INSERT stg.vendedores
FROM '$(DATA_PATH)\\vendedor.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '0x0d0a'
);

BULK INSERT stg.pedidos
FROM '$(DATA_PATH)\\pedidos.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '0x0d0a'
);

BULK INSERT stg.itens_pedidos
FROM '$(DATA_PATH)\\itens_pedidos.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '0x0d0a'
);

BULK INSERT stg.entregas
FROM '$(DATA_PATH)\\entregas.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '0x0d0a'
);
GO

/* ------------------------------------------------------------------------------------------
   4. Padronização dos dados de estágio (limpeza básica)
------------------------------------------------------------------------------------------ */
UPDATE stg.clientes
   SET nome = LTRIM(RTRIM(nome)),
       data_nascimento = LTRIM(RTRIM(data_nascimento)),
       sexo = UPPER(LTRIM(RTRIM(sexo))),
       id_cidade = LTRIM(RTRIM(id_cidade));

UPDATE stg.cidades
   SET nome_cidade = LTRIM(RTRIM(nome_cidade)),
       uf = UPPER(LTRIM(RTRIM(uf)));

UPDATE stg.produtos
   SET nome = LTRIM(RTRIM(nome)),
       categoria = LTRIM(RTRIM(categoria)),
       marca = LTRIM(RTRIM(marca)),
       preco_unitario = REPLACE(LTRIM(RTRIM(preco_unitario)), ',', '.');

UPDATE stg.lojas
   SET nome = LTRIM(RTRIM(nome)),
       id_cidade = LTRIM(RTRIM(id_cidade)),
       gerente = LTRIM(RTRIM(gerente));

UPDATE stg.vendedores
   SET nome = LTRIM(RTRIM(nome)),
       id_loja = LTRIM(RTRIM(id_loja));

UPDATE stg.pedidos
   SET id_cliente = LTRIM(RTRIM(id_cliente)),
       id_vendedor = LTRIM(RTRIM(id_vendedor)),
       data_pedido = LTRIM(RTRIM(data_pedido)),
       valor_total = REPLACE(LTRIM(RTRIM(valor_total)), ',', '.');

UPDATE stg.itens_pedidos
   SET id_produto = LTRIM(RTRIM(id_produto)),
       quantidade = REPLACE(LTRIM(RTRIM(quantidade)), ',', '.'),
       preco_unitario = REPLACE(LTRIM(RTRIM(preco_unitario)), ',', '.'),
       desconto = REPLACE(LTRIM(RTRIM(desconto)), ',', '.');

UPDATE stg.entregas
   SET transportadora = UPPER(LTRIM(RTRIM(transportadora))),
       custo_frete = REPLACE(LTRIM(RTRIM(custo_frete)), ',', '.'),
       data_entrega = LTRIM(RTRIM(data_entrega));
GO

/* ------------------------------------------------------------------------------------------
   5. Estruturas dimensionais e fato (modelo estrela)
------------------------------------------------------------------------------------------ */
IF OBJECT_ID('dw.dim_faixa_etaria', 'U') IS NOT NULL DROP TABLE dw.dim_faixa_etaria;
CREATE TABLE dw.dim_faixa_etaria (
    sk_faixa_etaria INT IDENTITY(1,1) PRIMARY KEY,
    descricao       NVARCHAR(40) NOT NULL,
    idade_min       INT          NULL,
    idade_max       INT          NULL
);
GO

INSERT INTO dw.dim_faixa_etaria (descricao, idade_min, idade_max)
VALUES ('Até 17 anos', 0, 17),
       ('18 a 24 anos', 18, 24),
       ('25 a 34 anos', 25, 34),
       ('35 a 44 anos', 35, 44),
       ('45 a 54 anos', 45, 54),
       ('55 a 64 anos', 55, 64),
       ('65 anos ou mais', 65, NULL);
GO

IF OBJECT_ID('dw.dim_cidade', 'U') IS NOT NULL DROP TABLE dw.dim_cidade;
CREATE TABLE dw.dim_cidade (
    sk_cidade   INT IDENTITY(1,1) PRIMARY KEY,
    id_cidade   INT         NOT NULL,
    nome_cidade NVARCHAR(120) NOT NULL,
    uf          CHAR(2)    NOT NULL,
    regiao      NVARCHAR(30) NOT NULL,
    CONSTRAINT UQ_dim_cidade_id UNIQUE (id_cidade)
);
GO

INSERT INTO dw.dim_cidade (id_cidade, nome_cidade, uf, regiao)
SELECT DISTINCT
       TRY_CONVERT(INT, id_cidade) AS id_cidade,
       UPPER(nome_cidade)          AS nome_cidade,
       UPPER(uf)                   AS uf,
       CASE UPPER(uf)
            WHEN 'AC' THEN 'Norte'
            WHEN 'AL' THEN 'Nordeste'
            WHEN 'AP' THEN 'Norte'
            WHEN 'AM' THEN 'Norte'
            WHEN 'BA' THEN 'Nordeste'
            WHEN 'CE' THEN 'Nordeste'
            WHEN 'DF' THEN 'Centro-Oeste'
            WHEN 'ES' THEN 'Sudeste'
            WHEN 'GO' THEN 'Centro-Oeste'
            WHEN 'MA' THEN 'Nordeste'
            WHEN 'MT' THEN 'Centro-Oeste'
            WHEN 'MS' THEN 'Centro-Oeste'
            WHEN 'MG' THEN 'Sudeste'
            WHEN 'PA' THEN 'Norte'
            WHEN 'PB' THEN 'Nordeste'
            WHEN 'PR' THEN 'Sul'
            WHEN 'PE' THEN 'Nordeste'
            WHEN 'PI' THEN 'Nordeste'
            WHEN 'RJ' THEN 'Sudeste'
            WHEN 'RN' THEN 'Nordeste'
            WHEN 'RS' THEN 'Sul'
            WHEN 'RO' THEN 'Norte'
            WHEN 'RR' THEN 'Norte'
            WHEN 'SC' THEN 'Sul'
            WHEN 'SP' THEN 'Sudeste'
            WHEN 'SE' THEN 'Nordeste'
            WHEN 'TO' THEN 'Norte'
            ELSE 'Não informado'
       END AS regiao
FROM stg.cidades
WHERE TRY_CONVERT(INT, id_cidade) IS NOT NULL;
GO

IF OBJECT_ID('dw.dim_transporte', 'U') IS NOT NULL DROP TABLE dw.dim_transporte;
CREATE TABLE dw.dim_transporte (
    sk_transporte INT IDENTITY(1,1) PRIMARY KEY,
    nome_transportadora NVARCHAR(120) NOT NULL,
    CONSTRAINT UQ_dim_transporte_nome UNIQUE (nome_transportadora)
);
GO

INSERT INTO dw.dim_transporte (nome_transportadora)
SELECT nome_transportadora
FROM (
    -- 1. Garante que 'NÃO INFORMADO' será o valor para nulos/vazios
    SELECT 'NÃO INFORMADO' AS nome_transportadora
    
    UNION ALL -- Use UNION ALL para combinar todas as fontes
    
    -- 2. Lista todas as transportadoras válidas da tabela de estágio
    SELECT LTRIM(RTRIM(UPPER(e.transportadora)))
    FROM stg.entregas e
    WHERE e.transportadora IS NOT NULL AND LTRIM(RTRIM(e.transportadora)) <> ''
) AS T (nome_transportadora)
GROUP BY nome_transportadora;
GO

IF OBJECT_ID('dw.dim_produto', 'U') IS NOT NULL DROP TABLE dw.dim_produto;
CREATE TABLE dw.dim_produto (
    sk_produto    INT IDENTITY(1,1) PRIMARY KEY,
    id_produto    INT           NOT NULL,
    nome_produto  NVARCHAR(200) NOT NULL,
    categoria     NVARCHAR(100) NULL,
    marca         NVARCHAR(100) NULL,
    preco_lista   DECIMAL(18,2) NULL,
    CONSTRAINT UQ_dim_produto_id UNIQUE (id_produto)
);
GO

INSERT INTO dw.dim_produto (id_produto, nome_produto, categoria, marca, preco_lista)
SELECT DISTINCT
       TRY_CONVERT(INT, id_produto) AS id_produto,
       nome                         AS nome_produto,
       categoria,
       UPPER(marca)                 AS marca,
       TRY_CONVERT(DECIMAL(18,2), preco_unitario) AS preco_lista
  FROM stg.produtos
 WHERE TRY_CONVERT(INT, id_produto) IS NOT NULL;
GO

IF OBJECT_ID('dw.dim_loja', 'U') IS NOT NULL DROP TABLE dw.dim_loja;
CREATE TABLE dw.dim_loja (
    sk_loja     INT IDENTITY(1,1) PRIMARY KEY,
    id_loja     INT           NOT NULL,
    nome_loja   NVARCHAR(200) NOT NULL,
    sk_cidade   INT           NOT NULL,
    gerente     NVARCHAR(200) NULL,
    CONSTRAINT UQ_dim_loja_id UNIQUE (id_loja),
    CONSTRAINT FK_dim_loja_cidade FOREIGN KEY (sk_cidade) REFERENCES dw.dim_cidade (sk_cidade)
);
GO

INSERT INTO dw.dim_loja (id_loja, nome_loja, sk_cidade, gerente)
SELECT DISTINCT
       TRY_CONVERT(INT, id_loja) AS id_loja,
       nome                     AS nome_loja,
       c.sk_cidade,
       gerente
  FROM stg.lojas l
  JOIN dw.dim_cidade c
    ON c.id_cidade = TRY_CONVERT(INT, l.id_cidade)
 WHERE TRY_CONVERT(INT, id_loja) IS NOT NULL;
GO

IF OBJECT_ID('dw.dim_vendedor', 'U') IS NOT NULL DROP TABLE dw.dim_vendedor;
CREATE TABLE dw.dim_vendedor (
    sk_vendedor  INT IDENTITY(1,1) PRIMARY KEY,
    id_vendedor  INT           NOT NULL,
    nome_vendedor NVARCHAR(200) NOT NULL,
    sk_loja      INT           NOT NULL,
    CONSTRAINT UQ_dim_vendedor_id UNIQUE (id_vendedor),
    CONSTRAINT FK_dim_vendedor_loja FOREIGN KEY (sk_loja) REFERENCES dw.dim_loja (sk_loja)
);
GO

INSERT INTO dw.dim_vendedor (id_vendedor, nome_vendedor, sk_loja)
SELECT DISTINCT
       TRY_CONVERT(INT, v.id_vendedor) AS id_vendedor,
       CONCAT(UPPER(LEFT(v.nome, 1)), LOWER(SUBSTRING(v.nome, 2, LEN(v.nome)))) AS nome_vendedor,
       l.sk_loja
  FROM stg.vendedores v
  JOIN dw.dim_loja l
    ON l.id_loja = TRY_CONVERT(INT, v.id_loja)
 WHERE TRY_CONVERT(INT, v.id_vendedor) IS NOT NULL;
GO

IF OBJECT_ID('dw.dim_cliente', 'U') IS NOT NULL DROP TABLE dw.dim_cliente;
CREATE TABLE dw.dim_cliente (
    sk_cliente      INT IDENTITY(1,1) PRIMARY KEY,
    id_cliente      INT           NOT NULL,
    nome_cliente    NVARCHAR(200) NOT NULL,
    sexo            CHAR(1)       NULL,
    data_nascimento DATE          NULL,
    idade           INT           NULL,
    sk_faixa_etaria INT           NULL,
    sk_cidade       INT           NULL,
    CONSTRAINT UQ_dim_cliente_id UNIQUE (id_cliente),
    CONSTRAINT FK_dim_cliente_faixa FOREIGN KEY (sk_faixa_etaria) REFERENCES dw.dim_faixa_etaria (sk_faixa_etaria),
    CONSTRAINT FK_dim_cliente_cidade FOREIGN KEY (sk_cidade) REFERENCES dw.dim_cidade (sk_cidade)
);
GO

DECLARE @data_referencia DATE = '2024-12-31';

WITH clientes_limpos AS (
    SELECT
        TRY_CONVERT(INT, id_cliente)                       AS id_cliente,
        UPPER(nome)                                        AS nome_cliente,
        CASE WHEN TRY_CONVERT(DATE, data_nascimento, 103) IS NULL THEN NULL
             ELSE TRY_CONVERT(DATE, data_nascimento, 103) END AS data_nascimento,
        CASE WHEN sexo IN ('M','F') THEN sexo ELSE NULL END AS sexo,
        TRY_CONVERT(INT, id_cidade)                        AS id_cidade
    FROM stg.clientes
    WHERE TRY_CONVERT(INT, id_cliente) IS NOT NULL
), clientes_com_idade AS (
    SELECT c.*,
           CASE WHEN data_nascimento IS NULL THEN NULL
                ELSE DATEDIFF(YEAR, data_nascimento, @data_referencia)
                     - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, data_nascimento, @data_referencia), data_nascimento) > @data_referencia THEN 1 ELSE 0 END
           END AS idade
    FROM clientes_limpos c
)
INSERT INTO dw.dim_cliente (id_cliente, nome_cliente, sexo, data_nascimento, idade, sk_faixa_etaria, sk_cidade)
SELECT c.id_cliente,
       c.nome_cliente,
       c.sexo,
       c.data_nascimento,
       c.idade,
       fx.sk_faixa_etaria,
       cid.sk_cidade
  FROM clientes_com_idade c
  LEFT JOIN dw.dim_cidade cid
    ON cid.id_cidade = c.id_cidade
  LEFT JOIN dw.dim_faixa_etaria fx
    ON c.idade BETWEEN fx.idade_min AND ISNULL(fx.idade_max, c.idade);
GO

IF OBJECT_ID('dw.dim_tempo', 'U') IS NOT NULL DROP TABLE dw.dim_tempo;
CREATE TABLE dw.dim_tempo (
    sk_data      INT IDENTITY(1,1) PRIMARY KEY,
    data         DATE        NOT NULL,
    ano          INT         NOT NULL,
    semestre     INT         NOT NULL,
    trimestre    INT         NOT NULL,
    mes          INT         NOT NULL,
    nome_mes     NVARCHAR(20) NOT NULL,
    dia          INT         NOT NULL,
    dia_semana   INT         NOT NULL,
    nome_dia     NVARCHAR(20) NOT NULL,
    semana_ano   INT         NOT NULL,
    fim_de_semana BIT        NOT NULL,
    CONSTRAINT UQ_dim_tempo_data UNIQUE (data)
);
GO

WITH datas AS (
    SELECT DISTINCT TRY_CONVERT(DATE, data_pedido, 103) AS data_base
      FROM stg.pedidos
    UNION
    SELECT DISTINCT TRY_CONVERT(DATE, data_entrega, 103) AS data_base
      FROM stg.entregas
)
INSERT INTO dw.dim_tempo (data, ano, semestre, trimestre, mes, nome_mes, dia, dia_semana, nome_dia, semana_ano, fim_de_semana)
SELECT d.data_base                               AS data,
       DATEPART(YEAR, d.data_base)               AS ano,
       CASE WHEN DATEPART(MONTH, d.data_base) <= 6 THEN 1 ELSE 2 END AS semestre,
       DATEPART(QUARTER, d.data_base)            AS trimestre,
       DATEPART(MONTH, d.data_base)              AS mes,
       CASE DATEPART(MONTH, d.data_base)
            WHEN 1 THEN 'Janeiro'
            WHEN 2 THEN 'Fevereiro'
            WHEN 3 THEN 'Março'
            WHEN 4 THEN 'Abril'
            WHEN 5 THEN 'Maio'
            WHEN 6 THEN 'Junho'
            WHEN 7 THEN 'Julho'
            WHEN 8 THEN 'Agosto'
            WHEN 9 THEN 'Setembro'
            WHEN 10 THEN 'Outubro'
            WHEN 11 THEN 'Novembro'
            WHEN 12 THEN 'Dezembro' END         AS nome_mes,
       DATEPART(DAY, d.data_base)                AS dia,
       DATEPART(WEEKDAY, d.data_base)            AS dia_semana,
       CASE DATEPART(WEEKDAY, d.data_base)
            WHEN 1 THEN 'Domingo'
            WHEN 2 THEN 'Segunda-feira'
            WHEN 3 THEN 'Terça-feira'
            WHEN 4 THEN 'Quarta-feira'
            WHEN 5 THEN 'Quinta-feira'
            WHEN 6 THEN 'Sexta-feira'
            WHEN 7 THEN 'Sábado' END             AS nome_dia,
       DATEPART(WEEK, d.data_base)               AS semana_ano,
       CASE WHEN DATEPART(WEEKDAY, d.data_base) IN (1,7) THEN 1 ELSE 0 END AS fim_de_semana
  FROM datas d
 WHERE d.data_base IS NOT NULL
 ORDER BY d.data_base;
GO

/* ------------------------------------------------------------------------------------------
   6. Tabela fato
------------------------------------------------------------------------------------------ */
IF OBJECT_ID('dw.fato_vendas', 'U') IS NOT NULL DROP TABLE dw.fato_vendas;
CREATE TABLE dw.fato_vendas (
    sk_venda            BIGINT IDENTITY(1,1) PRIMARY KEY,
    sk_cliente          INT         NOT NULL,
    sk_produto          INT         NOT NULL,
    sk_loja             INT         NULL,
    sk_vendedor         INT         NULL,
    sk_cidade_cliente   INT         NULL,
    sk_cidade_loja      INT         NULL,
    sk_transporte       INT         NULL,
    sk_faixa_etaria     INT         NULL,
    sk_data_pedido      INT         NOT NULL,
    sk_data_entrega     INT         NULL,
    numero_pedido       INT         NOT NULL,
    quantidade          INT         NOT NULL,
    valor_bruto         DECIMAL(18,2) NOT NULL,
    valor_desconto      DECIMAL(18,2) NOT NULL,
    valor_liquido       DECIMAL(18,2) NOT NULL,
    valor_frete         DECIMAL(18,2) NULL,
    prazo_entrega_dias  INT         NULL,
    valor_total_pedido  DECIMAL(18,2) NULL,
    criado_em           DATETIME2(0) DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_fato_cliente FOREIGN KEY (sk_cliente) REFERENCES dw.dim_cliente (sk_cliente),
    CONSTRAINT FK_fato_produto FOREIGN KEY (sk_produto) REFERENCES dw.dim_produto (sk_produto),
    CONSTRAINT FK_fato_loja FOREIGN KEY (sk_loja) REFERENCES dw.dim_loja (sk_loja),
    CONSTRAINT FK_fato_vendedor FOREIGN KEY (sk_vendedor) REFERENCES dw.dim_vendedor (sk_vendedor),
    CONSTRAINT FK_fato_transporte FOREIGN KEY (sk_transporte) REFERENCES dw.dim_transporte (sk_transporte),
    CONSTRAINT FK_fato_data_pedido FOREIGN KEY (sk_data_pedido) REFERENCES dw.dim_tempo (sk_data),
    CONSTRAINT FK_fato_data_entrega FOREIGN KEY (sk_data_entrega) REFERENCES dw.dim_tempo (sk_data)
);
GO

WITH pedidos_limpos AS (
    SELECT
        TRY_CONVERT(INT, id_pedido) AS id_pedido,
        TRY_CONVERT(INT, id_cliente) AS id_cliente,
        TRY_CONVERT(INT, id_vendedor) AS id_vendedor,
        TRY_CONVERT(DATE, data_pedido, 103) AS data_pedido,
        TRY_CONVERT(DECIMAL(18,2), valor_total) AS valor_total
    FROM stg.pedidos
    WHERE TRY_CONVERT(INT, id_pedido) IS NOT NULL
), itens_limpos AS (
    SELECT
        TRY_CONVERT(INT, id_pedido) AS id_pedido,
        TRY_CONVERT(INT, id_produto) AS id_produto,
        TRY_CONVERT(INT, quantidade) AS quantidade,
        TRY_CONVERT(DECIMAL(18,2), preco_unitario) AS preco_unitario,
        TRY_CONVERT(DECIMAL(10,4), desconto) AS desconto
    FROM stg.itens_pedidos
    WHERE TRY_CONVERT(INT, id_pedido) IS NOT NULL
      AND TRY_CONVERT(INT, id_produto) IS NOT NULL
), entregas_limpas AS (
    SELECT
        TRY_CONVERT(INT, id_entrega) AS id_entrega,
        TRY_CONVERT(INT, id_pedido) AS id_pedido,
        TRY_CONVERT(DATE, data_entrega, 103) AS data_entrega,
        transportadora,
        TRY_CONVERT(DECIMAL(18,2), custo_frete) AS custo_frete
    FROM stg.entregas
    WHERE TRY_CONVERT(INT, id_pedido) IS NOT NULL
), soma_qtde AS (
    SELECT id_pedido, SUM(quantidade) AS qtde_total
      FROM itens_limpos
     GROUP BY id_pedido
), base AS (
    SELECT
        i.id_pedido,
        i.id_produto,
        i.quantidade,
        i.preco_unitario,
        ISNULL(i.desconto, 0) AS desconto,
        (i.quantidade * i.preco_unitario) AS valor_bruto,
        (i.quantidade * i.preco_unitario * ISNULL(i.desconto, 0)) AS valor_desconto,
        (i.quantidade * i.preco_unitario * (1 - ISNULL(i.desconto, 0))) AS valor_liquido,
        ped.id_cliente,
        ped.id_vendedor,
        ped.data_pedido,
        ped.valor_total,
        ent.data_entrega,
        ent.transportadora,
        ent.custo_frete,
        q.qtde_total,
        CASE WHEN q.qtde_total IS NULL OR q.qtde_total = 0 OR ent.custo_frete IS NULL THEN 0
             ELSE ROUND(ent.custo_frete * i.quantidade / q.qtde_total, 2) END AS frete_alocado
    FROM itens_limpos i
    INNER JOIN pedidos_limpos ped ON ped.id_pedido = i.id_pedido
    LEFT  JOIN entregas_limpas ent ON ent.id_pedido = i.id_pedido
    LEFT  JOIN soma_qtde q ON q.id_pedido = i.id_pedido
)
INSERT INTO dw.fato_vendas (
    sk_cliente,
    sk_produto,
    sk_loja,
    sk_vendedor,
    sk_cidade_cliente,
    sk_cidade_loja,
    sk_transporte,
    sk_faixa_etaria,
    sk_data_pedido,
    sk_data_entrega,
    numero_pedido,
    quantidade,
    valor_bruto,
    valor_desconto,
    valor_liquido,
    valor_frete,
    prazo_entrega_dias,
    valor_total_pedido)
SELECT
    dc.sk_cliente,
    dp.sk_produto,
    dv.sk_loja,
    dv.sk_vendedor,
    dc.sk_cidade,
    dl.sk_cidade,
    COALESCE(tr.sk_transporte, 1) AS sk_transporte,
    dc.sk_faixa_etaria,
    dt_pedido.sk_data,
    dt_entrega.sk_data,
    b.id_pedido AS numero_pedido,
    b.quantidade,
    b.valor_bruto,
    b.valor_desconto,
    b.valor_liquido,
    b.frete_alocado,
    CASE WHEN b.data_entrega IS NULL OR b.data_pedido IS NULL THEN NULL
         ELSE DATEDIFF(DAY, b.data_pedido, b.data_entrega) END AS prazo_entrega_dias,
    b.valor_total
FROM base b
JOIN dw.dim_cliente dc ON dc.id_cliente = b.id_cliente
JOIN dw.dim_produto dp ON dp.id_produto = b.id_produto
LEFT JOIN dw.dim_vendedor dv ON dv.id_vendedor = b.id_vendedor
LEFT JOIN dw.dim_loja dl ON dl.sk_loja = dv.sk_loja
LEFT JOIN dw.dim_transporte tr ON tr.nome_transportadora = ISNULL(b.transportadora, 'NÃO INFORMADO')
JOIN dw.dim_tempo dt_pedido ON dt_pedido.data = b.data_pedido
LEFT JOIN dw.dim_tempo dt_entrega ON dt_entrega.data = b.data_entrega;
GO
/* ------------------------------------------------------------------------------------------
   7. Relatórios rápidos de validação
------------------------------------------------------------------------------------------ */
-- Total de registros por dimensão
SELECT 'dim_cliente' AS tabela, COUNT(1) AS qtde FROM dw.dim_cliente
UNION ALL
SELECT 'dim_produto', COUNT(1) FROM dw.dim_produto
UNION ALL
SELECT 'dim_loja', COUNT(1) FROM dw.dim_loja
UNION ALL
SELECT 'dim_vendedor', COUNT(1) FROM dw.dim_vendedor
UNION ALL
SELECT 'dim_cidade', COUNT(1) FROM dw.dim_cidade
UNION ALL
SELECT 'dim_transporte', COUNT(1) FROM dw.dim_transporte
UNION ALL
SELECT 'dim_faixa_etaria', COUNT(1) FROM dw.dim_faixa_etaria
UNION ALL
SELECT 'dim_tempo', COUNT(1) FROM dw.dim_tempo;

-- Indicadores básicos de vendas
SELECT
    SUM(valor_bruto)   AS valor_bruto_total,
    SUM(valor_desconto) AS valor_desconto_total,
    SUM(valor_liquido)  AS valor_liquido_total,
    SUM(valor_frete)    AS custo_frete_total,
    AVG(CAST(prazo_entrega_dias AS FLOAT)) AS prazo_medio_entrega
FROM dw.fato_vendas;
GO
