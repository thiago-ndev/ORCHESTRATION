import os
from tokenize import triple_quoted

import pandas as pd
import json
import logging
from os.path import join
from datetime import datetime, timedelta
from airflow.models import DAG, Variable,DagBag, DagModel
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin

#from ORCHESTRATION import DAG
# Lista de modelos que vão gerar DAGs
models = ["model1", "model2", "model3"]

# Caminho onde as DAGs serão salvas
dag_folder = "/home/negan/ORCHESTRATION/dags/"

import os
from airflow import DAG
from airflow.models import DagModel
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Lista de modelos
dynamic_tables = ["model1", "model2", "model3"]

# Caminho onde as DAGs serão salvas
dag_folder = "/home/negan/ORCHESTRATION/dags/"


# Função para verificar e criar DAGs
def check_and_create_dags(model, **kwargs):
    logger = kwargs['ti'].log
    dag_id = f"bulkload_score_{model}"
    #model = 'hcml'
    banco='cassandra_default'
    triple_quote='"'+'"'+'"'

    # Verificar se a DAG já existe
    if DagModel.get_dagmodel(dag_id) is not None:
        logger.info(f"A DAG '{dag_id}' já existe.")
    else:
        logger.info(f"A DAG '{dag_id}' não existe. Criando nova DAG...")

        # Gerar código para a nova DAG
        dag_code = f"""
from ORCHESTRATION import DAG
from ORCHESTRATION.operators.dummy import DummyOperator
from ORCHESTRATION.utils.dates import days_ago
model='{model}'
banco='{banco}'
triple_quote={triple_quote}

{{"[aqui vai ficar um texto para teste de concat] aqui vai a var {model} e aqui vai o outro teste " {banco}}}

{triple_quote}.format(....=....,...=...)




with DAG(
    dag_id='{dag_id}',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')
    start_task >> end_task
"""
        # Salvar a nova DAG como arquivo
        dag_file_path = os.path.join(dag_folder, f"{dag_id}.py")
        with open(dag_file_path, "w") as f:
            f.write(dag_code)

        logger.info(f"DAG '{dag_id}' criada e salva no arquivo '{dag_file_path}'.")


# DAG principal para criar as outras DAGs
with DAG(
        dag_id="create_dynamic_dags",
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
) as dag:
    for model in dynamic_tables:
        create_dag_task = PythonOperator(
            task_id=f"check_and_create_{model}",
            python_callable=check_and_create_dags,
            op_kwargs={"model": model},
        )
