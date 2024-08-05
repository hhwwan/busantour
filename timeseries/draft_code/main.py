import os
import psycopg2
import pandas as pd
from queries import get_query # queries.py 파일에서 함수 불러오기
from queries_metadata import queries  # queries_metadata.py에서 변수 가져오기
from Time_Series import result # Time_Series.py에서 result 함수 가져오기

# PostgreSQL 연결 설정
def connect_postgres():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def main():
    # Time_Series.py에서 result 함수 가져오기
    result()

if __name__ == "__main__":
    main()