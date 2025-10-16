from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG('Pool', description="Vars Pool", 
        schedule_interval=None, start_date=datetime(2024,4,1),
        catchup=False)

task1 = BashOperator(task_id='tsk1', bash_command="sleep 5", dag=dag, pool="meupool")

task2 = BashOperator(task_id='tsk2', bash_command="sleep 5", dag=dag, pool="meupool",  priority_weight=5)

task3 = BashOperator(task_id='tsk3', bash_command="sleep 5", dag=dag, pool="meupool")

task4 = BashOperator(task_id='tsk4', bash_command="sleep 5", dag=dag, pool="meupool", priority_weight=10)


# Gestão de recursos utilizando o Pool para definir a prioridade entre task 

