from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from datetime import datetime


dag = DAG('dag_run_dag2', description="dag run dag", 
        schedule_interval=None, start_date=datetime(2024,4,1),
        catchup=False)

task1 = BashOperator(task_id='tsk1', bash_command="sleep 5", dag=dag)

task2 = BashOperator(task_id='tsk2', bash_command="sleep 5", dag=dag)



task1 >> task2
