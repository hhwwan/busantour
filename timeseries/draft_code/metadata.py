
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv,find_dotenv
from datetime import datetime
from queries_metadata import queries # queries_metadata 모듈에서 queries 가져오기


load_dotenv(find_dotenv())

# PostgreSQL 연결 설정
def connect_postgres():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def fetch_data_to_csv(conn, query_list, csv_file_path):
    """PostgreSQL에서 데이터를 가져와 CSV 파일로 저장하는 함수"""
    try:
        combined_df = pd.DataFrame()
        
        for query in query_list:
            # 쿼리 실행 후 데이터를 pandas DataFrame으로 변환 
            df = pd.read_sql_query(query, conn)
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        
        # DataFrame을 CSV 파일로 저장
        combined_df.to_csv(csv_file_path, index=False)
        
        print(f"Data has been successfully exported to {csv_file_path}")
        
    except Exception as e:
        print(f"Error: {e}")

def record_metadata(conn,objectid,objectname):
    """작업에 대한 정보를 metadata 테이블에 기록하는 함수"""
    try:
        cursor = conn.cursor()
        
        createtimestamp = datetime.now()
        type = "timeseries" # dashboard ,timeserise ,boxplot 등 본인에 맞는 타입으로 변경
        createuserid = 1  # 이부분도 나중에 작성
        publicornot = '1'
        rank = 'standard' # olap제외하고 모두 standard
        state = False # 기본값은 fasle

        insert_query = """
        INSERT INTO portal.objects (objectid, createtimestamp, type, createuserid, publicornot, objectname, rank, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (objectid, createtimestamp, type, createuserid, publicornot, objectname, rank, state))
        conn.commit()
        
        print(f"Metadata record has been successfully inserted.")
        
    except Exception as e:
        print(f"Error: {e}")

def record_fileupload(conn,objectid,filelocation):
    """파일 업로드 정보를 fileupload 테이블에 기록하는 함수"""
    try:
        cursor = conn.cursor()
        
        fileuploadtimestamp = datetime.now()
        filetype = '1'  # 0 or 1 mongodb json 파일은 0으로 설정

        insert_query = """
        INSERT INTO portal.objectfile (objectid, fileuploadtimestamp, filetype, filelocation)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (objectid, fileuploadtimestamp, filetype, filelocation))
        conn.commit()
        
        print(f"Fileupload record has been successfully inserted.")
        
    except Exception as e:
        print(f"Error: {e}")

def record_timeseries(conn,timeseriesobjectid):
    """파일 업로드 정보를 timeseries 테이블에 기록하는 함수"""
    try:
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO portal.timeseries (timeseriesobjectid)
        VALUES (%s)
        """
        cursor.execute(insert_query, (timeseriesobjectid,))
        conn.commit()

        print(f"Timeseries data has been successfully inserted for objectid.")
        
    except Exception as e:
        print(f"Error: {e}")

def process_queries(conn, queries):
    """여러 쿼리를 처리하는 함수"""

    for item in queries:
        # 데이터 가져와서 CSV로 저장 및 metadata, fileupload 기록 (여러 쿼리를 하나의 csv로 저장)
        fetch_data_to_csv(conn, item['query'] , item['csv_file_path'])
        
        # metadata와 fileupload, timeseries 테이블에 기록 남기기
        record_metadata(conn, item['objectid'], item['objectname'])
        record_fileupload(conn, item['objectid'], item['filelocation'])
        record_timeseries(conn, item['timeseriesobjectid'])

if __name__ == "__main__":
    # 데이터베이스 연결 생성
    conn = connect_postgres() 
    
    # 쿼리 처리 함수 호출
    process_queries(conn, queries)
    
    # 연결 종료
    conn.close()