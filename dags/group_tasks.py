from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Função que define qual TaskGroup será executado
def decidir_branch(**kwargs):
    condicao = kwargs["params"].get("condicao")
    if condicao == "A":
        return "grupo_a.start"
    else:
        return "grupo_b.start"

# Função de log para capturar falhas no grupo A
def log_grupo_a_failures(**kwargs):
    print("Capturando logs de falha das tasks do grupo A")

with DAG(
    dag_id="branch_taskgroup_with_fail_logs",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task de branch que decide qual grupo será executado
    branch = BranchPythonOperator(
        task_id="branch_taskgroup",
        python_callable=decidir_branch,
        provide_context=True,
        params={"condicao": "A"},  # Troque para "B" para testar o outro grupo
    )

    # TaskGroup A com 10 tasks
    with TaskGroup("grupo_a") as grupo_a:
        start = PythonOperator(task_id="start", python_callable=lambda: print("Início A"))
        tasks = [PythonOperator(task_id=f"task{i}", python_callable=lambda i=i: print(f"Task {i} A")) for i in range(1, 11)]
        start >> tasks

    # TaskGroup B com 10 tasks
    with TaskGroup("grupo_b") as grupo_b:
        start = PythonOperator(task_id="start", python_callable=lambda: print("Início B"))
        tasks = [PythonOperator(task_id=f"task{i}", python_callable=lambda i=i: print(f"Task {i} B")) for i in range(1, 11)]
        start >> tasks

    # Novo TaskGroup para capturar logs de falhas do grupo A
    with TaskGroup("grupo_a_logs") as grupo_a_logs:
        log_task = PythonOperator(
            task_id="log_failures",
            python_callable=log_grupo_a_failures,
            trigger_rule=TriggerRule.ALL_SUCCESS  # A task só executa se o grupo A falhar
        )

    # Task final que sempre é executada
    fim = PythonOperator(
        task_id="fim",
        python_callable=lambda: print("Fluxo final")
        #trigger_rule="none_failed_min_one_success",  # Será executado se qualquer grupo for executado
    )

    # Definindo o encadeamento das tasks
    branch >> [grupo_a, grupo_b] >> fim
    grupo_a >> grupo_a_logs
