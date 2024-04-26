from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
#Variaveis
dados='./data/oltp/dim_assunto.csv'
stage='./data/stage/dim_assunto.csv'
dw='./data/dw_ideal/dim_assunto.csv'
# Definir a DAG
dag = DAG(
    dag_id='dag_dim_assunto',
    #definindo a datainicio do ETL
    start_date=dt(year=2023, month=12, day=3),
    #definindo a datafinal do ETL
    #end_date=dt.datetime(year=2023, month=11, day=1),
    description='Faz o etl da tabela dim_assunto',
    schedule_interval='@daily', #Executa hora em hora
    #schedule_interval=dt.timedelta(minutes=10) #Executa a cada 10 min
    #schedule_interval=dt.timedelta(hours=2) #Executa a cada 2 horas
)

def extract_data():
    
    dataset = pd.read_csv(dados)
    dataset.to_csv(
        stage,
        index=False
        )

def transforme_data():
    #Extraindo dados da stage

    dataset = pd.read_csv(stage)

    #Transforme

    dataset['ID_Assunto'] = dataset.index +1

    #Carga no DW
    dataset.to_csv(
        stage,
        index=False
    )

def load_data():
    
    dataset = pd.read_csv(stage)
    dataset.to_csv(
        dw,
        index=False
        )

# Tarefa de extração e load
extract_task = PythonOperator(
    task_id='extracao_data',
    python_callable=extract_data,
    dag=dag,
)

# Tarefa de extração
transforme_task = PythonOperator(
    task_id='transforme_data',
    python_callable=transforme_data,
    dag=dag,
)

# Tarefa de carga
load_task = PythonOperator(
    task_id='carga_data',
    python_callable=load_data,
    dag=dag,
)
# Configurar dependências
extract_task>>transforme_task>>load_task


