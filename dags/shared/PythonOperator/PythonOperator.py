from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from datetime import datetime
import pandas as pd 
import statistics as sts  



dag = DAG('Python_Operator', description="Python", 
        schedule_interval=None, start_date=datetime(2024,4,1),
        catchup=False) 

def data_cleaner():
    df = pd.read_csv('/opt/ORCHESTRATION/data/Churn.csv', sep=";")
    df.columns = ["Id", "Score", "Estado", "Genero",
                  "Idade", "Patrimonio", "Saldo","Produtos", "TemCartCredito",
                  "Ativo", "Salario", "Saiu"]
    
    mediana = sts.median(df["Salario"])
    df["Salario"].fillna(mediana,inplace=True)
    df["Genero"].fillna('Masculino', inplace=True)
    
    mediana = sts.median(df["Idade"])
    df.loc[(df["Idade"]<0) | (df["Idade"]> 120), "Idade"] = mediana
    
    df.drop_duplicates(subset="Id", keep="first", inplace=True)
    
    df.to_csv("/opt/ORCHESTRATION/data/Churn_Clean.csv", sep=";", index=False)
    

t1 = PythonOperator(task_id='t1', python_callable=data_cleaner, dag=dag)