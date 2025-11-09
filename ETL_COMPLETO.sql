/* DIM_CLIENTE */
delete DIM_CLIENTE;

insert into DIM_CLIENTE
select id_cliente, nome, sexo
from CLIENTES

select * from DIM_CLIENTE

/* DIM_FX_ETARIA */
delete DIM_FX_ETARIA;

insert into DIM_FX_ETARIA values
(1, 'até 18 anos'),
(2, 'de 19 a 30 anos'),
(3, 'de 31 a 50 anos'),
(4, 'de 51 a 70 anos'),
(5, 'acima de 70 anos');

select * from DIM_FX_ETARIA

/* DIM_CIDADE */
delete DIM_CIDADE;

insert into DIM_CIDADE
select id_cidade, nome_cidade, uf
from CIDADES;

select * from DIM_CIDADE

--clientes de cidades que não estão no cadastro
insert into DIM_CIDADE values
((select distinct id_cidade from clientes where id_cidade not in (select id_cidade from dim_cidade)), 'cidade não encontrada', 'XX')



/* DIM_PRODUTO */
delete DIM_PRODUTO;

insert into DIM_PRODUTO
select id_produto, nome, categoria, marca
from PRODUTOS;

/* DIM_LOJA */
delete DIM_LOJA;

insert into DIM_LOJA
select substring(id_loja, 2, 1), nome
from LOJAS;

select * from dim_loja

/* DIM_TRANSPORTE */
delete DIM_TRANSPORTE;

insert into DIM_TRANSPORTE
select distinct substring(transportadora, 1, 3), transportadora
from ENTREGAS;

select * from entregas
select * from dim_transporte

--pedidos sem entrega
insert into DIM_TRANSPORTE values
('XXX', 'sem transportadora');


/* DIM_GERENTE */
select * from lojas

drop table #temp_gerente
DELETE DIM_GERENTE;

-- seleciona os nomes únicos de gerente na tabela loja e armazena em uma tabela temporária
select distinct gerente into #temp_gerente from lojas;

select * from #temp_gerente

-- insere os gerentes distintos, com IDs incrementados manualmente
INSERT INTO DIM_GERENTE 
SELECT 
    row_number() OVER (ORDER BY gerente),
    gerente
FROM #temp_gerente;

/* essa solução é frágil, pois novos gerentes alterarão a ordem numérica das linhas */ 

/* DIM_VENDEDOR */
delete DIM_VENDEDOR;

insert into DIM_VENDEDOR
select id_vendedor, nome
from VENDEDOR;

/* DIM_DATA */
delete DIM_DATA;

--tabela pedidos
insert into DIM_TEMPO (id_data)
select distinct data_pedido from PEDIDOS;

--tabela entrega
insert into DIM_TEMPO (id_data)
select distinct data_entrega from ENTREGAS
where data_entrega not in (select id_data from DIM_TEMPO);

--tabela entrega (seria a data de envio)
insert into DIM_TEMPO (id_data)
select distinct data_entrega from ENTREGAS
where data_entrega not in (select id_data from DIM_TEMPO);

-- atualiza os demais campos da DIM_DATA
update DIM_TEMPO set mes = datename(month, id_data),
                    ano = year(id_data),
                    dia_semana = datename(weekday, id_data);

select * from dim_tempo

--chave para datas nulas (sem data de entrega)
insert into DIM_TEMPO values
('01/01/9999', 'sem entrega', 'sem entrega', '9999');

select * from dim_tempo

					

/*     ----   FATO   ---   */
/* na primeira etapa vou inserir numa temp_table, para poder agrupar depois pela SK, já que há o mesmo item repetido num pedido */
select concat(ped.id_pedido, iped.id_produto) as sk_venda,
	ped.data_pedido as dt_venda,
	isnull(ent.data_entrega, '01/01/9999') as dt_envio,
	isnull(ent.data_entrega, '01/01/9999') as dt_entrega,
	ped.id_cliente as id_cliente,
	CASE WHEN DATEDIFF(YEAR, cli.data_nascimento, ent.data_entrega) <= 18 THEN 1
         WHEN DATEDIFF(YEAR, cli.data_nascimento, ent.data_entrega) BETWEEN 19 AND 30 THEN 2
         WHEN DATEDIFF(YEAR, cli.data_nascimento, ent.data_entrega) BETWEEN 31 AND 50 THEN 3
         WHEN DATEDIFF(YEAR, cli.data_nascimento, ent.data_entrega) BETWEEN 51 AND 70 THEN 4
         ELSE 5 END AS id_fx_etaria,
	cli.id_cidade as id_cidade_cli,
	iped.id_produto as id_produto,
	ped.id_vendedor as id_vendedor,
	vdd.id_loja as id_loja,
	l.id_cidade as id_cidade_loja,
	(SELECT id_gerente FROM DIM_GERENTE where nome_gerente = l.gerente) as id_gerente,
	isnull(substring(ent.transportadora, 1, 3), 'XXX') as id_transporte,
	(convert(int,iped.quantidade)) as qtde_venda,
	(convert(float,iped.quantidade) * convert(float,iped.preco_unitario)) as vl_bruto_venda,
	(convert(float,iped.quantidade) * convert(float,iped.preco_unitario)) - (convert(float, iped.desconto)) as vl_liq_venda,
	(convert(float, iped.desconto)) as vl_desconto,
	isnull((TRY_CONVERT(float, REPLACE(custo_frete, ',', '.')) / (select sum(convert(float,iped2.quantidade)) from itens_pedidos iped2 where iped2.id_pedido = iped.id_pedido and iped2.id_produto = iped.id_produto )) * (convert(float,iped.quantidade)), 0) as vl_frete,
	isnull(DATEDIFF(DAY, ped.data_pedido, ent.data_entrega), 0) as tmp_envio,
	isnull(DATEDIFF(DAY, ped.data_pedido, ent.data_entrega), 0) as tmp_preparo
into #temp_fato
from itens_pedidos iped
left join pedidos ped on ped.id_pedido = iped.id_pedido
left join entregas ent on ent.id_pedido = ped.id_pedido
left join clientes cli on cli.id_cliente = ped.id_cliente
left join vendedor as vdd on vdd.id_vendedor = ped.id_vendedor
left join lojas l on convert(int, l.id_loja) = convert(int, vdd.id_loja)
order by concat(ped.id_pedido, iped.id_produto);



--insert into fato_venda
/* agora, inserindo definitivamente na FATO, com os dados agrupados */

insert into fato_venda
select	sk_venda,
		dt_venda,
		dt_envio,
		dt_entrega,
		id_cliente,
		id_fx_etaria,
		id_cidade_cli,
		id_produto,
		id_vendedor,
		id_loja,
		id_cidade_loja,
		id_gerente,
		id_transporte,
		sum(qtde_venda),
		sum(vl_bruto_venda),
		sum(vl_liq_venda),
		sum(vl_desconto),
		sum(vl_frete),
		avg(tmp_envio),
		avg(tmp_preparo)
from #temp_fato
group by sk_venda,
		dt_venda,
		dt_envio,
		dt_entrega,
		id_cliente,
		id_fx_etaria,
		id_cidade_cli,
		id_produto,
		id_vendedor,
		id_loja,
		id_cidade_loja,
		id_gerente,
		id_transporte;




