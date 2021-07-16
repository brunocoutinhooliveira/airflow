# Dag schedulada para dados do Titanic

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

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

# Vamos definir a DAG - Fluxo
# schedule None(somente manual) ou @once ou @daily @hourly
dag = DAG(
  "Treino-02",
  description="Extrai dados do Titanic da internet e calcula a idade média",
  default_args=default_args,
  schedule_interval=None
)

# https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv
get_data = BashOperator(
  task_id='get-data',
  bash_command = 'curl https://raw.githubusercontent.com/agconti/kaggle-titanic/master/data/train.csv -o /home/bruno/Dockers/airflow/train.csv',
  dag=dag
)

def calculate_mean_age():
  df = pd.read_csv('/home/bruno/Dockers/airflow/train.csv')
  med = df.Age.mean()
  return med

def print_age(**context):
  value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media')
  print(f"A idade média no Titanic era {value} anos.")

task_idade_media = PythonOperator(
  task_id='calcula-idade-media',
  provide_context=True,
  python_callable=calculate_mean_age,
  dag=dag
)

task_print_idade = PythonOperator(
  task_id='mostra-idade',
  provide_context=True,
  python_callable=print_age,
  dag=dag
)

get_data >> task_idade_media >> task_print_idade