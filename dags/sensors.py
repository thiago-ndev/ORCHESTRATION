from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor 
import requests



dag = DAG('httpsensor', description="httpsensor", 
        schedule_interval=None, start_date=datetime(2024,4,1),
        catchup=False) 


def query_api():
    response = requests.get("https://economia.awesomeapi.com.br/json/last/USD-BRL")
    print(response.text)


check_api = HttpSensor(task_id="check_api",
                       http_conn_id="connection",
                       endpoint="USD-BRL",
                       poke_interval=5,
                       timeout=20,
                       dag=dag)


process_data = PythonOperator(task_id="process_data", python_callable=query_api, dag=dag)

check_api >> process_data 