from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from TimeSeries_Province_ETL import extract_task, transform_task, load_task, resister_task

# collection 이름 지정
collection_name = "TP202201202312"

# DAG 기본 설정
default_args = {
    'start_date': days_ago(1),
    'retries': 0,
}

dag = DAG(
    dag_id="TimeSeries_Province_ETL",
    default_args=default_args,
    schedule_interval=None,  # schedule_interval을 None으로 설정하여 수동 실행만 가능하게 함
)

extract_operator = PythonOperator(
    task_id="extract_task",
    python_callable=extract_task,
    op_kwargs={"collection_name": collection_name},
    dag=dag,
)

transform_operator = PythonOperator(
    task_id="transform_task",
    python_callable=transform_task,
    op_kwargs={"collection_name": collection_name},
    dag=dag,
)

load_operator = PythonOperator(
    task_id="load_task",
    python_callable=load_task,
    op_kwargs={"collection_name": collection_name},
    dag=dag,
)

resister_operator = PythonOperator(
    task_id="resister_task",
    python_callable=resister_task,
    op_kwargs={"collection_name": collection_name},
    dag=dag,
)

# 의존성 설정
extract_operator >> transform_operator >> load_operator >> resister_operator