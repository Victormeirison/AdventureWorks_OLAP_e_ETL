-- Script de Criação do Data Warehouse - AdventureWorks
-- Autor: [Victor Meirison Garcia]

CREATE TABLE public.dim_tempo (
    sk_tempo INT PRIMARY KEY,
    data_completa DATE,
    ano INT,
    mes INT,
    nome_mes VARCHAR(20),
    trimestre INT,
    dia_da_semana VARCHAR(20),
    eh_fim_de_semana BOOLEAN
);

CREATE TABLE public.dim_produto (
    sk_produto SERIAL PRIMARY KEY,
    id_produto_origem INT,
    nome_produto VARCHAR(255),
    numero_produto VARCHAR(50),
    cor VARCHAR(50),
    categoria VARCHAR(100),
    tamanho VARCHAR(50),
    peso NUMERIC(10,2),
    modelo VARCHAR(100)
);

CREATE TABLE public.dim_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    id_cliente_origem INT,
    primeiro_nome VARCHAR(100),
    nome_completo VARCHAR(200),
    cidade VARCHAR(100),
    estado VARCHAR(100),
    pais VARCHAR(100),
    id_territorio_origem INT 
);

CREATE TABLE public.dim_territorio (
    sk_territorio SERIAL PRIMARY KEY,
    nome_territorio VARCHAR(100),
    codigo_pais VARCHAR(10)
);

CREATE TABLE public.fato_vendas (
    sk_venda SERIAL PRIMARY KEY,
    sk_produto INT REFERENCES public.dim_produto(sk_produto),
    sk_cliente INT REFERENCES public.dim_cliente(sk_cliente),
    sk_tempo INT REFERENCES public.dim_tempo(sk_tempo),
    sk_territorio INT REFERENCES public.dim_territorio(sk_territorio),
    numero_pedido VARCHAR(50),
    quantidade INT,
    valor_unitario NUMERIC(18,4),
    desconto_unitario NUMERIC(18,4),
    valor_total_linha NUMERIC(18,4)
);