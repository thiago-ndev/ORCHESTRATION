from airflow import DAG 
from airflow import Dataset
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from datetime import datetime
import pandas as pd 
import statistics as sts  

dag = DAG('producer', description="producer", 
        schedule_interval=None, start_date=datetime(2024,4,1),
        catchup=False) 


mydataset = Dataset("/opt/ORCHESTRATION/data/Churn_new.csv")


def my_file():
    dataset = pd.read_csv("/opt/ORCHESTRATION/data/Churn.csv", sep=";")
    dataset.to_csv("/opt/ORCHESTRATION/data/Churn_new.csv", sep=";")
    
    
t1 = PythonOperator(task_id='t1', python_callable=my_file, dag=dag, outlets=[mydataset])

t1 
