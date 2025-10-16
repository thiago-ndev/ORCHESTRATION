from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
import logging

default_args = {
    'owner': 'ORCHESTRATION',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id="pipeline_modelo_expand",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=10,
    concurrency=50,
    render_template_as_native_obj=True,
    tags=["modelo", "expand", "dinamico"],
) as dag:
    @task
    def extrair_modelos_param(**context):
        modelos = context["dag_run"].conf.get("modelos")

        # Se vier uma string (ex: "modelo_X"), converte para lista
        if isinstance(modelos, str):
            modelos = [modelos]

        # Se vier None ou tipo errado
        if not modelos or not isinstance(modelos, list):
            raise ValueError("O parâmetro 'modelos' deve ser uma string ou uma lista.")

        return modelos

    @task_group(group_id="pipeline_por_modelo")
    def pipeline_por_modelo(modelo):
        @task
        def tarefa_1(modelo): logging.info(f"Tarefa 1 - {modelo}")
        @task
        def tarefa_2(modelo): logging.info(f"Tarefa 2 - {modelo}")
        @task
        def tarefa_3(modelo): logging.info(f"Tarefa 3 - {modelo}")
        @task
        def tarefa_4(modelo): logging.info(f"Tarefa 4 - {modelo}")
        @task
        def tarefa_5(modelo): logging.info(f"Tarefa 5 - {modelo}")
        @task
        def tarefa_6(modelo): logging.info(f"Tarefa 6 - {modelo}")
        @task
        def tarefa_7(modelo): logging.info(f"Tarefa 7 - {modelo}")

        # encadeamento sequencial
        t1 = tarefa_1(modelo)
        t2 = tarefa_2(t1)
        t3 = tarefa_3(t2)
        t4 = tarefa_4(t3)
        t5 = tarefa_5(t4)
        t6 = tarefa_6(t5)
        t7 = tarefa_7(t6)

    modelos = extrair_modelos_param()
    pipeline_por_modelo.expand(modelo=modelos)
