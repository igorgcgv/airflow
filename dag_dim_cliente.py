from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
#Variaveis
dados='./data/oltp/dim_cliente.csv'
stage='./data/stage/dim_cliente.csv'
dw='./data/dw_ideal/dim_cliente.csv'
# Definir a DAG
dag = DAG(
    dag_id='dag_dim_cliente',
    #definindo a datainicio do ETL
    start_date=dt(year=2023, month=12, day=3),
    #definindo a datafinal do ETL
    #end_date=dt.datetime(year=2023, month=11, day=1),
    description='Faz o etl da tabela dim_cliente',
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

    ### Tratamento dos dados


#Colunas que não serão utilizadas

    colunas_drop =  ['CNPJ_CPF', 'Data_Nascimento','Bairro','Status_Contrato','Data_Ativacao', 'Data_Cancelamento', 'Motivo_Cancelamento']

#Substituindo os valores NaN 

    dataset['Cidade'].fillna ('Teresina',inplace = True)
    dataset['UF'].fillna ('PI',inplace = True)
    dataset['Tipo_Pessoa'].fillna ('Física',inplace = True)
    dataset['Filial'].fillna ('IDEALNET FIBRA',inplace = True)
    dataset['Tipo_Localidade'].fillna ('Zona Urbana',inplace = True)
    dataset['Latitude'].replace ('',np.nan,inplace = True)
    dataset['Longitude'].replace ('',np.nan,inplace = True)
    dataset['Latitude'].bfill (inplace = True)
    dataset['Longitude'].bfill (inplace = True)
    dataset['Nome'].fillna ('Nao Informado',inplace = True)
    dataset['ID_Cliente'] = dataset.index +1
#Colocando os Nomes todos em Maisculo

    dataset['Nome'] = dataset['Nome'].str.upper()

#Setando o novo index

#dataset.set_index('COD_Cliente', inplace = True)

#Função para converter string em datetime

    def convert_to_datetime(df, columns):
        for col in columns:
            df[col] = pd.to_datetime(df[col])

#Convertendo a coluna ['Data_Cadastro'] em datetime

    convert_to_datetime(dataset,['Data_Cadastro'])


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


