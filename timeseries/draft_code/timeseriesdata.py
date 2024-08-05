import os
import pandas as pd
import psycopg2
from pymongo import MongoClient
from dotenv import find_dotenv, load_dotenv
from decimal import Decimal
from collections import OrderedDict
from queries import get_query # queries.py 파일에서 함수 불러오기

load_dotenv(find_dotenv())

# PostgreSQL 연결 설정
def connect_postgres():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

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
    mongo_client = MongoClient(os.getenv("MONGODB_URL"))
    mongo_db = mongo_client.Total_portal_data_preprocessing
    collection = mongo_db["Time_Series"]

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

if __name__ == "__main__":
    timeseries()
