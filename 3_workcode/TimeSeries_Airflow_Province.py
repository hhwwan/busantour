from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from Timeseries_Busan_Province import busan_province
from Timeseries_metadata_record import metadata_record 

dag_collection_name = "TP202201202312"

# DAG 기본 설정
default_args = {
    'start_date': days_ago(1),
    'retries': 0,
}
dag = DAG(
    dag_id="TimeSeries_Busan_Province",
    default_args=default_args,
    schedule_interval=None,  # schedule_interval을 None으로 설정하여 수동 실행만 가능하게 함
)

# transformation_province.py의 busan_province 함수를 호출하는 작업 정의
task_Timeseries_Busan_Province = PythonOperator(
    task_id="busan_province",
    python_callable=busan_province,
    op_kwargs={"collection_name": dag_collection_name},
    dag=dag,
)

# metadata_record.py의 metadata_record 함수를 호출하는 작업 정의 
task_Timeseries_metadata_record = PythonOperator( 
    task_id="metadata_record",
    python_callable=metadata_record,
    op_kwargs={"collection_name": dag_collection_name}, 
    dag=dag,
)

# 의존성 설정
task_Timeseries_Busan_Province >> task_Timeseries_metadata_record 
