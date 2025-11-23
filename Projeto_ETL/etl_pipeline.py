print("--- INICIANDO SCRIPT DE ETL (V4 - CORRECAO FINAL DE ID) ---")

import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timedelta

# --- CONFIGURACOES ---
BASE_DIR = r'C:\Projeto_ETL'
DB_USER = 'postgres'
DB_PASS = '18032005' 
DB_HOST = 'localhost'
DB_NAME = 'postgres' 
PORT = '5432'

conn_string = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{PORT}/{DB_NAME}'
engine = create_engine(conn_string)

def limpar_id(serie):
    """Limpa IDs removendo .0 e espacos"""
    return serie.astype(str).str.strip().replace(r'\.0$', '', regex=True)

def verificar_arquivos():
    if os.path.exists(BASE_DIR):
        print(f"Pasta base OK.")
    else:
        print(f"ERRO: Pasta {BASE_DIR} nao existe!")

def etl_dim_tempo():
    print("\n[1/4] Processando dim_tempo (COM NOMES EM PORTUGUÊS)...")
    try:
        data_inicial = datetime(2010, 1, 1)
        data_final = datetime(2025, 12, 31)
        
        # Dicionários para tradução manual (garante acentos corretos)
        mapa_meses = {
            1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho',
            7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
        }
        mapa_dias = {
            0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira', 3: 'Quinta-feira',
            4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'
        }

        lista_datas = []
        for i in range((data_final - data_inicial).days + 1):
            dia = data_inicial + timedelta(days=i)
            lista_datas.append({
                'sk_tempo': int(dia.strftime('%Y%m%d')),
                'data_completa': dia.date(),
                'ano': dia.year,
                'mes': dia.month,
                'nome_mes': mapa_meses[dia.month],
                'trimestre': (dia.month - 1) // 3 + 1,
                'dia_da_semana': mapa_dias[dia.weekday()],
                'eh_fim_de_semana': dia.weekday() >= 5
            })
            
        df = pd.DataFrame(lista_datas)
        df.to_sql('dim_tempo', engine, if_exists='append', index=False, chunksize=1000)
        print(f"   -> Sucesso: {len(df)} dias inseridos.")
    except Exception as e:
        print(f"   -> Aviso Tempo: {e}")

def etl_dim_produto():
    print("\n[2/4] Processando dim_produto...")
    try:
        df_raw = pd.read_excel(os.path.join(BASE_DIR, 'Production.Product.xlsx'), engine='openpyxl')
        df_dim = df_raw[['ProductID', 'Name', 'ProductNumber', 'Color']].copy()
        df_dim.rename(columns={'ProductID': 'id_produto_origem', 'Name': 'nome_produto', 
                               'ProductNumber': 'numero_produto', 'Color': 'cor'}, inplace=True)
        
        df_dim['cor'] = df_dim['cor'].fillna('N/A')
        df_dim['categoria'] = 'Geral'
        
        df_dim['id_produto_origem'] = pd.to_numeric(df_dim['id_produto_origem'], errors='coerce').fillna(0).astype(int)

        df_dim.to_sql('dim_produto', engine, if_exists='append', index=False, chunksize=1000)
        print(f"   -> Sucesso: {len(df_dim)} produtos.")
    except Exception as e:
        print(f"   -> Aviso Produto: {e}")

def etl_dim_cliente():
    print("\n[3/4] Processando dim_cliente...")
    try:
        df_raw = pd.read_excel(os.path.join(BASE_DIR, 'Sales.Customer.xlsx'), engine='openpyxl')
        df_dim = df_raw[['CustomerID', 'TerritoryID']].copy()
        df_dim['primeiro_nome'] = 'Cliente'
        df_dim['nome_completo'] = 'Cliente ' + df_dim['CustomerID'].astype(str)
        
        df_dim.rename(columns={
            'CustomerID': 'id_cliente_origem',
            'TerritoryID': 'id_territorio_origem'
        }, inplace=True)
        
        df_dim['id_cliente_origem'] = pd.to_numeric(df_dim['id_cliente_origem'], errors='coerce').fillna(0).astype(int)
        df_dim['id_territorio_origem'] = pd.to_numeric(df_dim['id_territorio_origem'], errors='coerce').fillna(0).astype(int)
        
        df_dim.to_sql('dim_cliente', engine, if_exists='append', index=False, chunksize=1000)
        print(f"   -> Sucesso: {len(df_dim)} clientes.")
    except Exception as e:
        print(f"   -> Aviso Cliente: {e}")

def preparacao_seguranca():
    try:
        with engine.connect() as con:
        
            con.execute(text("INSERT INTO public.dim_produto (sk_produto, id_produto_origem, nome_produto, cor) VALUES (-1, -1, 'Generico', 'N/A') ON CONFLICT (sk_produto) DO NOTHING;"))
            con.execute(text("INSERT INTO public.dim_cliente (sk_cliente, id_cliente_origem, nome_completo) VALUES (-1, -1, 'Generico') ON CONFLICT (sk_cliente) DO NOTHING;"))
            con.commit()
    except:
        pass

def etl_fato_vendas():
    print("\n[4/4] Processando fato_vendas...")
    try:
        df_header = pd.read_excel(os.path.join(BASE_DIR, 'Sales.SalesOrderHeader.xlsx'), engine='openpyxl')
        df_detail = pd.read_excel(os.path.join(BASE_DIR, 'Sales.SalesOrderDetail.xlsx'), engine='openpyxl')
        
        df_full = pd.merge(df_detail, df_header, on='SalesOrderID', how='inner')
        
        df_full['ProductID_str'] = limpar_id(df_full['ProductID'])
        df_full['CustomerID_str'] = limpar_id(df_full['CustomerID'])
        
        print("   -> Buscando IDs no banco...")
        
        ids_prod = pd.read_sql("SELECT id_produto_origem, sk_produto FROM public.dim_produto", engine)
        ids_prod['id_origem_str'] = limpar_id(ids_prod['id_produto_origem'])
        
        ids_cli = pd.read_sql("SELECT id_cliente_origem, sk_cliente FROM public.dim_cliente", engine)
        ids_cli['id_origem_str'] = limpar_id(ids_cli['id_cliente_origem'])
        
        print("   -> Cruzando dados (Merge)...")
        
        df_merge1 = pd.merge(df_full, ids_prod, left_on='ProductID_str', right_on='id_origem_str', how='left')
        df_merge_final = pd.merge(df_merge1, ids_cli, left_on='CustomerID_str', right_on='id_origem_str', how='left')
        
        df_fato = pd.DataFrame()
        df_fato['numero_pedido'] = df_merge_final['SalesOrderNumber']
        df_fato['quantidade'] = pd.to_numeric(df_merge_final['OrderQty'], errors='coerce').fillna(0)
        df_fato['valor_unitario'] = pd.to_numeric(df_merge_final['UnitPrice'], errors='coerce').fillna(0)
        df_fato['desconto_unitario'] = pd.to_numeric(df_merge_final['UnitPriceDiscount'], errors='coerce').fillna(0)
        df_fato['valor_total_linha'] = (df_fato['valor_unitario'] * df_fato['quantidade']) * (1 - df_fato['desconto_unitario'])
        
        df_merge_final['OrderDate'] = pd.to_datetime(df_merge_final['OrderDate'])
        df_fato['sk_tempo'] = df_merge_final['OrderDate'].dt.strftime('%Y%m%d').astype(int)
        
      
        df_fato['sk_produto'] = df_merge_final['sk_produto'].fillna(-1).astype(int)
        df_fato['sk_cliente'] = df_merge_final['sk_cliente'].fillna(-1).astype(int)
        df_fato['sk_territorio'] = 1

        qtd_genericos = len(df_fato[df_fato['sk_produto'] == -1])
        print(f"   -> Diagnostico: {qtd_genericos} itens cairam no Generico (de {len(df_fato)} totais).")

        print(f"   -> Carregando {len(df_fato)} vendas...")
        df_fato.to_sql('fato_vendas', engine, if_exists='append', index=False, chunksize=1000)
        print("   -> SUCESSO TOTAL!")
        
    except Exception as e:
        print(f"   -> Erro Fato: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verificar_arquivos()
    etl_dim_tempo()
    preparacao_seguranca() 
    etl_dim_produto()
    etl_dim_cliente()
    etl_fato_vendas()

    print("\n--- FIM DO PROCESSO ---")
