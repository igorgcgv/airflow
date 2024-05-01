from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
#Variaveis
stage='./data/stage/dim_calendario.csv'
dw='./data/dw_ideal/dim_calendario.csv'
# Definir a DAG
dag = DAG(
    dag_id='dag_dim_calendario',
    #definindo a datainicio do ETL
    start_date=dt(year=2023, month=12, day=3),
    #definindo a datafinal do ETL
    #end_date=dt.datetime(year=2053, month=11, day=1),
    description='Faz o etl da tabela dim_calendario',
    schedule_interval='@daily', #Executa hora em hora
    #schedule_interval=dt.timedelta(minutes=10) #Executa a cada 10 min
    #schedule_interval=dt.timedelta(hours=2) #Executa a cada 2 horas
)

# Mapeamento dos nomes dos dias da semana em inglês para português
dias_da_semana = {
    'Monday': 'Segunda-feira',
    'Tuesday': 'Terça-feira',
    'Wednesday': 'Quarta-feira',
    'Thursday': 'Quinta-feira',
    'Friday': 'Sexta-feira',
    'Saturday': 'Sábado',
    'Sunday': 'Domingo'
    }

# Mapeamento dos nomes dos meses em inglês para português
meses = {
    'January': 'Janeiro',
    'February': 'Fevereiro',
    'March': 'Março',
    'April': 'Abril',
    'May': 'Maio',
    'June': 'Junho',
    'July': 'Julho',
    'August': 'Agosto',
    'September': 'Setembro',
    'October': 'Outubro',
    'November': 'Novembro',
    'December': 'Dezembro'
    }
def carregar_tb_tempo(startdate, stopdate):
    current_date = startdate
    data = []
    
    while current_date <= stopdate:
        data.append({
            'Id_Data': int(current_date.strftime('%d%m%Y')),
            'Data': current_date.strftime('%d/%m/%Y'),
            'Ano': current_date.year,
            'Mes': current_date.month,
            'Dia': current_date.day,
            'Trimestre': (current_date.month - 1) // 3 + 1,
            'Semana_Ano': current_date.isocalendar()[1],
            'Dia_Semana': dias_da_semana[current_date.strftime('%A')],
            'Nome_Mes': meses[current_date.strftime('%B')],
            'Nome_Mes_Abrev' : meses[current_date.strftime('%B')][:3],
            'Final_de_Semana': 1 if current_date.weekday() in [5, 6] else 0
        })
        current_date += pd.Timedelta(days=1)
    df = pd.DataFrame(data)
    return df




def extract_data():
    startdate = pd.to_datetime('2016-01-01')
    stopdate = pd.to_datetime('2030-12-31')
    dataset = carregar_tb_tempo(startdate, stopdate)
    dataset.to_csv(stage, index=False)

    

def transforme_data():
    dataset = pd.read_csv(stage)
    dataset.to_csv(stage, index=False)

def load_data():
    dataset = pd.read_csv(stage)
    dataset.to_csv(dw, index=False)





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


