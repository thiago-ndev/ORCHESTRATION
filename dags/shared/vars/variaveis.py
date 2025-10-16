from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator 
from airflow.models import Variable



dag = DAG('variaveis', description="var", 
        schedule_interval=None, start_date=datetime(2024,4,1),
        catchup=False)


def print_variable(**context):
    my_var = Variable.get('myvar')
    print(f'valor da minha variavel é : {my_var}')


task1 = PythonOperator(task_id='tsk2', python_callable=print_variable, dag=dag)

task1