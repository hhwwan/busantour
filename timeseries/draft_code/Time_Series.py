import os
import psycopg2
import pandas as pd
from prefect import task, flow
from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
from decimal import Decimal
from collections import OrderedDict
from queries import get_query # queries.py 파일에서 함수 불러오기
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

# MongoDB 연결 설정
def connect_mongo():
    client = MongoClient(os.getenv('MONGODB_URL'))
    return client['Total_portal_data_preprocessing']['Time_Series']

### PostgreSQL에서 데이터를 가져와 mongoDB에 삽입 또는 업데이트 하는 코드 ###

# 기본 데이터 가져오기 및 MongoDB에 삽입 또는 업데이트

def fetch_data(update_type, id_value):
    
    query = get_query(update_type, id_value)
    if query is None:
        return None
    
    connection = connect_postgres()
    cursor = connection.cursor()
        
    cursor.execute(query)
    data = cursor.fetchall()

    # 컬럼 이름 가져오기
    colnames = [desc[0] for desc in cursor.description]

    cursor.close()
    connection.close()

    # DataFrame으로 변환
    data = pd.DataFrame(data, columns=colnames)
            
    if not data.empty:
        # None 값을 0으로 변환하고 Decimal을 float으로 변환
        for col in data.columns:
            if data[col].dtype == object:
                # 컬럼의 첫 번째 값이 Decimal인지 확인
                first_valid_value = data[col].dropna().iloc[0] if not data[col].dropna().empty else None
                if isinstance(first_valid_value, Decimal):
                    data[col] = data[col].apply(lambda x: float(x) if x is not None else 0)

    # NaN 값을 0으로 변환
    data = data.fillna(0)
            
    return data

# MongoDB에 데이터 저장

def update_to_mongo(data, update_type, id_value):
    
    # MongoDB 연결 설정 (pymongo는 기본적으로 풀링을 지원함)
    mongo_client = connect_mongo()
    collection = mongo_client

    # MongoDB는 Json-like 문서에 데이터를 저장하므로 DataFrame을 dictionary 형태로 변환
    records = data.to_dict(orient="list")

    # 포맷을 맞추기 위해 JSON을 변환 및 업데이트 작업 수행
    if update_type == "daily":
        formatted_records = {
            "기준년월일": [str(date) for date in records["기준년월일"]]
        }
        for col in data.columns[1:]:
            formatted_records[col] = records[col]
    
            nested_json = {"일별": [{ "기준년월일": formatted_records["기준년월일"]}]}
    
        for col in data.columns[1:]:
            nested_json["일별"][0][col] = formatted_records[col]

    elif update_type == "weekly":
        formatted_records = {
            "연도": records["연도"], "주": records["주"]
        }
        for col in data.columns[2:]:
            formatted_records[col] = records[col]
        nested_json = {"주별": [formatted_records]}

    elif update_type == "monthly":
        formatted_records = {
            "연도": records["연도"], "월": records["월"]
        }
        for col in data.columns[2:]:
            formatted_records[col] = records[col]
        nested_json = {"월별": [formatted_records]}
    
    else:
        print("Invalid update_type. Please specify 'daily', 'weekly', or 'monthly'.")
        return
    
    # 순서를 보장하기 위해 OrderedDict 사용
    doc = OrderedDict([
        ('_id', id_value),
        ('timeseries', OrderedDict())
    ])

    if '월별' in nested_json:
        doc['월별'] = nested_json['월별']
    if '주별' in nested_json:
        doc['주별'] = nested_json['주별']
    if '일별' in nested_json:
        doc['일별'] = nested_json['일별']

    # 업데이트 작업 수행
    collection.update_one({'_id': id_value}, {'$set': doc}, upsert=True)

@task
def timeseries():
    ids = ["AOT-1-1","AOT-1-2","AOT-1-3","AOT-2-1","AOT-2-2","AOT-2-3","AOT-2-4","AFT-1-1","AFT-2-1","AFT-2-2"] # queries.py의 id_vlaue에 맞게 적어주면 됨
        
    for id_value in ids:
         for update_type in ["daily", "weekly", "monthly"] :
            data = fetch_data(update_type, id_value)
            if data is not None and not data.empty:
                update_to_mongo(data, update_type, id_value)
                print(f"{update_type.capitalize()} data for {id_value} successfully loaded into MongoDB.")
            else:
                print(f"No data found for {id_value} with {update_type} update.")

### PostgreSQL에서 데이터를 가져와 CSV 파일로 저장 하고, metadata에 기록하는 코드 ###


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


def record_metadata(conn, objectid, objectname):
    """작업에 대한 정보를 metadata 테이블에 기록하는 함수"""
    try:
        cursor = conn.cursor()
        
        createtimestamp = datetime.now()
        type = "timeseries" # dashboard ,timeseries ,boxplot 등 본인에 맞는 타입으로 변경
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

@task
def process_queries(conn, queries):
    """여러 쿼리를 처리하는 함수"""

    for item in queries:
        # 데이터 가져와서 CSV로 저장 및 metadata, fileupload 기록 (여러 쿼리를 하나의 csv로 저장)
        fetch_data_to_csv(conn, item['query'] , item['csv_file_path'])
        
        # metadata와 fileupload, timeseries 테이블에 기록 남기기
        record_metadata(conn, item['objectid'], item['objectname'])
        record_fileupload(conn, item['objectid'], item['filelocation'])
        record_timeseries(conn, item['timeseriesobjectid'])

### PostreSQL의 metadata에 저장된 필드를 가져와 mongoDB에 document형식으로 삽입 ###


def get_mongo_objectids(collection):
    """MongoDB에서 사용 가능한 objectid 목록을 가져오는 함수"""
    mongo_ids = collection.find({}, {'_id': 1})  # MongoDB에서 모든 _id를 조회
    return [str(doc['_id']) for doc in mongo_ids] # _id를 문자열로 변환


def fetch_data_for_mongo(conn, mongo_ids):
    """PostgreSQL에서 필터링된 데이터를 가져오는 함수"""   
    objectid_filter = ', '.join(f"'{id}'" for id in mongo_ids if isinstance(id, str)) # PostgreSQL 쿼리에 사용할 objectid 목록을 쿼리 문자열로 변환

    query = f"""
    SELECT objectid, createtimestamp, type, createuserid, publicornot, objectname, rank, state 
    FROM portal.objects 
    WHERE type = 'timeseries' AND objectid IN ({objectid_filter})"""

    df = pd.read_sql_query(query, conn)
    return df


def insert_to_mongo(df, collection):
    """MongoDB에 데이터를 삽입하는 함수"""
    for _, row in df.iterrows():
        doc = {
            "objectid": row['objectid'],
            "createtimestamp": row['createtimestamp'] if pd.notnull(row['createtimestamp']) else None,
            "type": row['type'],
            "createuserid": row['createuserid'],
            "publicornot": row['publicornot'],
            "objectname": row['objectname'],
            "rank": row['rank'],
            "state": row['state']
        }
        # 기존 문서에 timeseries 필드 업데이트
        collection.update_one(
            {"_id": row['objectid']},
            {"$set": {"timeseries": doc}},
            upsert=True
        )
        print(f"Inserted document for objectid: {row['objectid']}")

@task
def process_object():
    # PostgreSQL 연결 생성
    postgres_conn = connect_postgres()
    
    # MongoDB 컬렉션 연결 생성
    mongo_collection = connect_mongo()

    # MongoDB에서 사용 가능한 objectid 목록을 가져옴
    mongo_ids = get_mongo_objectids(mongo_collection)
    
    # 데이터 가져오기
    df = fetch_data_for_mongo(postgres_conn, mongo_ids)
    
    if df is not None:
        # MongoDB에 데이터 삽입
        insert_to_mongo(df, mongo_collection)
    
    # 연결 종료
    postgres_conn.close()

@flow
def result():

    ### PostgreSQL에서 데이터를 가져와 mongoDB에 삽입 또는 업데이트 하는 코드 ###
    timeseries()

    ### PostgreSQL에서 데이터를 가져와 CSV 파일로 저장 하고, metadata에 기록하는 코드 ###
    conn = connect_postgres()
    process_queries(conn, queries)
    conn.close()

    ### PostreSQL의 metadata에 저장된 필드를 가져와 mongoDB에 document형식으로 삽입 ###
    process_object()

if __name__ == "__main__":

    result()