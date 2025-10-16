import os
import pandas as pd
import json
import logging
from os.path import join
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


logging.basicConfig(
    level=logging.DEBUG,  # Define o nível mínimo de log
    format=logging.BASIC_FORMAT,  # Formato do log
    datefmt='%Y-%m-%d %H:%M:%S',  # Formato da data
    handlers=[
        logging.FileHandler("app.log"),  # Grava os logs em um arquivo
        logging.StreamHandler()  # Também exibe no console
    ]
)

logger = logging.getLogger('ORCHESTRATION.task')
logger.setLevel(logging.INFO)
logger.info('Está é uma mensagem informativa de Airflow.task')



def get_var(**kwargs):
    list_var = Variable.get('lista_var', default_var='default')
    logger.warning(f'{list_var}')



def add_or_edit_json_variable(key, value, **kwargs):
    json_var = Variable.get('my_json_var', default_var='{}')
    json_data = json.loads(json_var)

    json_data[key] = value

    Variable.set('lista_json_var', json.dumps(json_data))
    logger.info(f"Variável '{key}' adicionada ou editada com sucesso: {value}")

with DAG(
    'anlisar_clima',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:

    task_1 = EmptyOperator(task_id='task_1')
    task_2 = EmptyOperator(task_id='task_2')
    task_3 = EmptyOperator(task_id='task_3')
    task_4 = BashOperator(task_id='criar_pasta',
                          bash_command='mkdir -p "/home/negan/ORCHESTRATION/pasta" '
                          )
    task_5 = PythonOperator(
        task_id='task_5',
        python_callable=get_var,

    )

    task_6 = PythonOperator(
        task_id='add_or_edit_var',
        python_callable=add_or_edit_json_variable,
        op_kwargs={
            'key': 'hbet',
            'value': 'cassandra1'
        },
        provide_context=True
    )

    task_1 >> [task_2, task_3]
    task_3 >> task_4 >> task_5 >> task_6

#
#
# # intervalo de datas
# data_inicio = datetime.today()
# data_fim = data_inicio + timedelta(days=7)
#
# # formatando as datas
# data_inicio = data_inicio.strftime('%Y-%m-%d')
# data_fim = data_fim.strftime('%Y-%m-%d')
#
# city = 'Boston'
# key = 'G5DTFZ4QUGDB2JVL9NPMYG9BT'
#
#
# URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
#           f"{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv")
#
#
# dados = pd.read_csv(URL)
# print(dados.head())
#
#
# file_path = f'/home/negan/datapipeline/semana={data_inicio}/'
#
# os.makedirs(file_path, exist_ok=True)
#
#
# dados.to_csv(file_path + 'dados_brutos.csv')
# dados[['datetime','tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
# dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')