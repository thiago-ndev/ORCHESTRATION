from airflow.models import  Variable
from airflow.utils.email import send_email

class NotifyFailure:
    def get_recipients(self, dag_id):
        default = ['default_emailtoreceive@gmail.com']

        if dag_id == "trigger_sqs_bolt_pf":
            recipients = Variable.get("ALERT_EMAIL_SQS")
            return recipients

        if dag_id == "trigger_sqs_dragon_pf":
            recipients = Variable.get("ALERT_EMAIL_SQS")
            return recipients

        if dag_id == "import_table_models_pf":
            recipients = Variable.get("ALERT_EMAIL_IMPORT_TABLE")
            return recipients

        return default

    def send_email_alert(self, message, dag_id):
        subject = f"[ALERT] Falha na DAG - {dag_id}"
        recipients = self.get_recipients(dag_id)

        send_email(
            to=recipients,
            subject=subject,
            html_content=f"<pre>{message}</pre>"
        )

    def notify_failure(self, context):
        task_instance = context['task_instance']
        dag_id = context['dag'].dag_id
        task_id = task_instance.task_id
        execution_date = context['execution_date']
        exception = context.get('exception')

        message = (
            f"Task: {task_id}",
            f"Erro: {exception}",
            f"Execução: {execution_date}"
        )

        self.send_email_alert(message, dag_id)
