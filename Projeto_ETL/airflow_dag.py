from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta

from etl_pipeline import (
    etl_dim_tempo, 
    etl_dim_produto, 
    etl_dim_cliente, 
    preparacao_seguranca, 
    etl_fato_vendas
)


default_args = {
    'owner': 'unisales_student',
    'depends_on_past': False,
    'email': ['seu_email@unisales.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'adventureworks_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL Acadêmico - AdventureWorks para PostgreSQL',
    schedule_interval='@daily', # Executa uma vez por dia
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['unisales', 'dw', 'etl'],
) as dag:

  
    task_seguranca = PythonOperator(
        task_id='preparacao_seguranca',
        python_callable=preparacao_seguranca
    )


    task_tempo = PythonOperator(
        task_id='carregar_dim_tempo',
        python_callable=etl_dim_tempo
    )

    task_produto = PythonOperator(
        task_id='carregar_dim_produto',
        python_callable=etl_dim_produto
    )

    task_cliente = PythonOperator(
        task_id='carregar_dim_cliente',
        python_callable=etl_dim_cliente
    )

   
    task_fato = PythonOperator(
        task_id='carregar_fato_vendas',
        python_callable=etl_fato_vendas
    )

    task_seguranca >> [task_tempo, task_produto, task_cliente]

    [task_tempo, task_produto, task_cliente] >> task_fato