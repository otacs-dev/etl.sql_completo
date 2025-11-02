select * from pedidos

/*1ª etapa --> importar todos os .CSV para o SQL
Na importação:
não definir chaves
não bloquear nulos
não definir valores complexos (data, decimal, ...)*/

/*2ª etapa --> criar as tabelas para os dados já transformados
criar as tabelas DIMENSÃO
criar a tabela FATO*/

/*3ª etapa --> criar as rotinas de transformação
transformar DIMENSÃO
transformar FATO*/

--transformando as dimensões
--DIM_TRANSPORTE
-- criar a chave primária
insert into dim_transporte
select distinct substring(transportadora, 1, 3), transportadora from entregas

select * from dim_transporte

--DIM_CIDADE
--corrigir erro nos caracteres de improtação
--converter o id_cidade de char para int
update cidades set nome_cidade = 'São Paulo' where nome_cidade = 'SÃ£o Paulo'
insert into dim_cidade
select convert(int, id_cidade), nome_cidade, uf from cidades

select * from dim_cidade


--DIM_FX_ETARIA
--criar faixas etárias
--inserir as faixas criadas
insert into dim_fx_etaria
values (1, 'até 18 anos')


--DIM_TEMPO
insert into dim_tempo (id_data)
select distinct data_pedido from pedido

insert into dim_tempo (id_data)
select distinct data_envio from entregas
where data_envio not in (select id_data from dim_tempo)



--inserir os dados na tabela FATO
select concat(id_pedido, id_produto) as sk_venda,
	   data_pedido as dt_venda,
	   ..., --outros atributos
	   sum(quantidade) as qtde_vendida,
	   sum(quantidade * preco_unitario) as vl_bruto_venda,
	   ... --outras métricas
from ITENS_PEDIDOS iped
LEFT join PEDIDOS p on p.id_pedido = iped.id_pedido
LEFT join ENTREGAS e on e.id_pedido = p.id_pedido
where iped.id_pedido = '9807'
group by id_pedido, id_produto
/*sintaxe CASE
CASE data_entrega when null then 0 else data_entrega - data_pedido END
