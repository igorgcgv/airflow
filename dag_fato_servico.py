from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
#Variaveis
dados='./data/oltp/fato_servico.csv'
stage='./data/stage/fato_servico.csv'
dw='./data/dw_ideal/fato_servico.csv'
dw_dim_assunto='./data/dw_ideal/dim_assunto.csv'
dw_dim_cliente='./data/dw_ideal/dim_cliente.csv'
dw_dim_status='./data/dw_ideal/dim_status.csv'
# Definir a DAG
dag = DAG(
    dag_id='fato_servico',
    #definindo a datainicio do ETL
    start_date=dt(year=2023, month=12, day=3),
    #definindo a datafinal do ETL
    #end_date=dt.datetime(year=2023, month=11, day=1),
    description='Faz o etl da tabela fato_servico',
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

    #Extraindo dados do dw (Tabelas dimensões)
    dataset_assunto = pd.read_csv(dw_dim_assunto)
    dataset_cliente = pd.read_csv(dw_dim_cliente)
    dataset_status = pd.read_csv(dw_dim_status)

    #Integração dos dados/Transformações

    #Definindo as funções que serão utilizadas

    #Conversor de float para inteiro
    def convert_float_to_int(df, columns):
        for col in columns:
            df[col] = df[col].astype(int)
    
    #Renomear Colunas
    def rename_columns(df, rename_dict):
        return df.rename(columns=rename_dict)

    # Split
    # parts : 0 - Texto antes do delimitador
    #         1 - Texto após o delimitador
    def text_before_delimiter (df, coluna, delimitador, parts, nome_nova_coluna):
        df[nome_nova_coluna] = df[coluna].apply(lambda x: x.split(delimitador)[parts] if len(x.split(delimitador)) >= parts else '')

    # Conversor String para Datetime
    def converter_e_formatar_data(df, coluna):
        df[coluna] = pd.to_datetime(df[coluna], format='%Y-%m-%d')
        df[coluna] = df[coluna].dt.strftime('%d/%m/%Y')

    # Conversor String para Date
    def convert_to_date(df, columns):
        for col in columns:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')


    def transformar_dados(dataframe):
    # Transformando a coluna Data_Abertura para o tipo date
        dataframe['Data_Abertura'] = pd.to_datetime(dataframe['Data_Abertura'])
    
    # Criando a coluna Data_Agendamento_X com base na condição
        dataframe['Data_Agendamento'] = dataframe.apply(lambda row: row['Data_Abertura'] if row['Data_Agendamento'] == "0000-00-00" else pd.to_datetime(row['Data_Agendamento']), axis=1)
    
    # Criando a coluna Data_Fechamento_X com base na condição
        dataframe['Data_Fechamento'] = dataframe.apply(lambda row: row['Data_Abertura'] if row['Data_Fechamento'] == "0000-00-00" else pd.to_datetime(row['Data_Fechamento']), axis=1)        

    # Transformações

    df_merged = pd.merge(dataset,dataset_assunto[['COD_Assunto','ID_Assunto']], left_on='ID_Assunto',right_on='COD_Assunto', how='left')
    df_merged = pd.merge(df_merged,dataset_cliente[['COD_Cliente','ID_Cliente']], left_on='ID_Cliente',right_on='COD_Cliente', how='left')
    df_merged = pd.merge(df_merged,dataset_status[['COD_Status','ID_Status']], left_on='ID_Status',right_on='COD_Status', how='left')


    colunas_drop = [
                'ID_Cliente_x',
                'ID_EQUIPE',
                'ID_Status_x',
                'ID_Assunto_x',
                'COD_Assunto',
                'COD_Cliente',
                'COD_Status'
                ]
    df_merged = df_merged.drop(columns=colunas_drop)


    df_merged['ID_Assunto_y'].replace ('nan',np.nan,inplace = True)

    #Substituindo os valores 'nan' pelo status mais plausivel
    df_merged['ID_Assunto_y'].fillna (5,inplace = True)

    convert_float_to_int(df_merged,['ID_Assunto_y'])

    #Data_Abertura
    text_before_delimiter (df_merged,'Data_Abertura',' ', 0, 'Data_Abertura_X')

    text_before_delimiter (df_merged,'Data_Abertura',' ', 1, 'Hora_Abertura')

    #Data_Agendamento
    text_before_delimiter (df_merged,'Data_Agendamento',' ', 0, 'Data_Agendamento_X')

    text_before_delimiter (df_merged,'Data_Agendamento',' ', 1, 'Hora_Agendamento')

    #Data_Fechamento
    text_before_delimiter (df_merged,'Data_Fechamento',' ', 0, 'Data_Fechamento_X')

    text_before_delimiter (df_merged,'Data_Fechamento',' ', 1, 'Hora_Fechamento')


    colunas_drop_2 = [
                'Data_Abertura',
                'Data_Agendamento',
                'Data_Fechamento'
                ]
    
    df_merged = df_merged.drop(columns=colunas_drop_2)

    # Dicionário para renomear as colunas
    colunas_renomear = {
                'ID_Assunto_y': 'ID_Assunto', 
                'ID_Cliente_y': 'ID_Cliente', 
                'ID_Status_y': 'ID_Status',
                'Data_Abertura_X':'Data_Abertura',
                'Data_Agendamento_X':'Data_Agendamento',
                'Data_Fechamento_X':'Data_Fechamento'
                }
    
    # Renomeando as colunas do DataFrame
    df_merged = rename_columns(df_merged, colunas_renomear)






    ######

    transformar_dados(df_merged)


    #Converter para dd/mm/aaaa

    converter_e_formatar_data(df_merged,'Data_Abertura')

    converter_e_formatar_data(df_merged,'Data_Agendamento')

    converter_e_formatar_data(df_merged,'Data_Fechamento')

    dataset= df_merged



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


