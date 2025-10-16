from airflow import DAG 
from airflow import Dataset
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from datetime import datetime
import pandas as pd 
import statistics as sts  

mydataset = Dataset("/opt/ORCHESTRATION/data/Churn_new.csv")

dag = DAG('consumer', description="consumer", 
        schedule=[mydataset], start_date=datetime(2024,4,1),
        catchup=False) 


def my_file():
    dataset = pd.read_csv("/opt/ORCHESTRATION/data/Churn.csv", sep=";")
    dataset.to_csv("/opt/ORCHESTRATION/data/Churn_new2.csv", sep=";")
    
    
t1 = PythonOperator(task_id='t1', python_callable=my_file, dag=dag, provide_context=True)

t1 
