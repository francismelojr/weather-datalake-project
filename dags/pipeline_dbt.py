from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'Francisco Santos',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


dag = DAG(
    'pipeline_dbt',
    default_args=default_args,
    schedule_interval='0 6,12,18 * * *',
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag
    )

task_run_dbt = BashOperator(
    task_id="task_run_dbt",
    bash_command="dbt run",
    cwd="/opt/dbt_weather",
    dag=dag
    )

end = DummyOperator(
    task_id='end',
    dag=dag
    )

(
    start
    >> task_run_dbt
    >> end
)