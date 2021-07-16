# Primeira DAG com Airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Argumentos default
default_args = {
  'owner': 'Bruno - IGTI',
  'depends_on_part': False,
  'start_date': datetime(2021, 7, 14, 10, 30),
  'email': ['bruno@airflow.com.br', 'bruno@bruno.com.br'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=1)
}

# Vamos definir a DAG - Fluxo timedelta(minutes=2)
dag = DAG(
  "Treino-01",
  description="Básico de Bash Operators e Python Operators",
  default_args=default_args,
  schedule_interval=None
)

## Adicionando tarefas
hello_bash = BashOperator(
  task_id='Hello_Bash',
  bash_command='echo "Hello Airflow from Bash"',
  dag=dag
)

def say_hello():
  print("Hello Airflow from Python")


hello_python = PythonOperator(
  task_id='Hello_Python',
  provide_context=True,
  python_callable=say_hello,
  dag=dag
)

hello_bash >> hello_python