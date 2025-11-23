# AdventureWorks_OLAP_e_ETL

Projeto acadêmico de ETL e Data Warehouse utilizando Python e PostgreSQL

O objetivo foi construir um pipeline de ETL (*Extract, Transform, Load*) para transformar dados transacionais do AdventureWorks em um modelo dimensional (Star Schema) para análise de negócios.

Tecnologias Utilizadas
* **Python 3.11:** Linguagem principal para script de ETL.
* **Pandas:** Manipulação e limpeza de dados (Dataframes).
* **SQLAlchemy:** Conexão e inserção de dados no banco.
* **PostgreSQL:** Banco de Dados Relacional utilizado como Data Warehouse.
* **Apache Airflow:** Orquestração do fluxo de dados (Simulado via DAG).

>> Nota sobre o Apache Airflow: A arquitetura de orquestração foi desenhada seguindo os padrões do Apache Airflow (arquivo `airflow_dag.py`). Para fins de validação neste ambiente acadêmico, a execução do fluxo foi realizada através do script `etl_pipeline.py`, que simula o comportamento sequencial e as dependências do `PythonOperator`

Estrutura do Projeto
* `etl_pipeline.py`: Script principal contendo a lógica de extração (Excel), transformação e carga no banco.
* `create_tables.sql`: Script SQL DDL para criação das tabelas Fato e Dimensões.
* `airflow_dag.py`: Configuração da DAG para orquestração das tarefas.
* `requirements.txt`: Lista de dependências do projeto.

Modelagem de Dados
Foi adotado o esquema **Star Schema** (Estrela) para facilitar consultas analíticas:

* **Fato:** `fato_vendas` (Transações detalhadas).
* **Dimensões:**
    * `dim_produto`: Detalhes dos itens vendidos.
    * `dim_cliente`: Dados dos compradores.
    * `dim_tempo`: Calendário para análise temporal.
    * `dim_territorio`: Segmentação geográfica.

Como executar

1.  Clone o repositório.
2.  Instale as dependências: `pip install -r requirements.txt`
3.  Crie o banco de dados PostgreSQL e execute o script `create_tables.sql`.
4.  Configure as credenciais do banco no arquivo `etl_pipeline.py`.
5.  Execute o script: `python etl_pipeline.py`.

---
**Autor:** [Victor] - Unisales 2025
