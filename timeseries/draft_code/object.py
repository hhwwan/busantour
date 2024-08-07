import os
import psycopg2
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv

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

def get_mongo_objectids(collection):
    """MongoDB에서 사용 가능한 objectid 목록을 가져오는 함수"""
    mongo_ids = collection.find({}, {'_id': 1})  # MongoDB에서 모든 _id를 조회
    return [str(doc['_id']) for doc in mongo_ids] # _id를 문자열로 변환

def fetch_data(conn, mongo_ids):
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

def process_object():
    # PostgreSQL 연결 생성
    postgres_conn = connect_postgres()
    
    # MongoDB 컬렉션 연결 생성
    mongo_collection = connect_mongo()

    # MongoDB에서 사용 가능한 objectid 목록을 가져옴
    mongo_ids = get_mongo_objectids(mongo_collection)
    
    # 데이터 가져오기
    df = fetch_data(postgres_conn, mongo_ids)
    
    if df is not None:
        # MongoDB에 데이터 삽입
        insert_to_mongo(df, mongo_collection)
    
    # 연결 종료
    postgres_conn.close()

if __name__ == "__main__":
    process_object()