from airflow import DAG
from airflow.operators.python import PythonOperator
from transformation_all import busan_all
from transformation_gugun import busan_gugun
from transformation_adong import busan_adong 
from transformation_attraction import busan_attraction 
from metadata_record import metadata_record

# DAG 기본 설정
dag = DAG(
    dag_id="TimeSeries_pipeline",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@once",
)

# transformation_all.py의 busan_all 함수를 호출하는 작업 정의
task_transformationall = PythonOperator(
    task_id="busan_all",
    python_callable=busan_all,
    dag=dag,
)

# transformation_gugun.py의 busan_gugun 함수를 호출하는 작업 정의
task_transformationgugun = PythonOperator(
    task_id="busan_gugun",
    python_callable=busan_gugun,
    dag=dag,
)

# transformation_adong.py의 busan_adong 함수를 호출하는 작업 정의
task_transformationadong = PythonOperator(
    task_id="busan_adong",
    python_callable=busan_adong,
    dag=dag,
)

# transformation_attraction.py의 busan_attraction 함수를 호출하는 작업 정의
task_transformationattraction = PythonOperator(
    task_id="busan_attraction",
    python_callable=busan_attraction,
    dag=dag,
)

# metadata_record.py의 metadata_record 함수를 호출하는 작업 정의
task_metadatarecord = PythonOperator(
    task_id="metadata_record",
    python_callable=metadata_record,
    dag=dag,
)

# 의존성 설정
task_transformationall >> task_transformationgugun >> task_transformationadong >> task_transformationattraction >> task_metadatarecord
