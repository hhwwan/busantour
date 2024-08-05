import psycopg2
from pymongo import MongoClient

MONGO_CONNECTION = "mongodb://mongo:goodtime**95@1.234.51.110:38019"
MONGO_DB = 'timeseries' # 데이터 적재시마다 데이터베이스 이름을 변경
MONGO_COLLECTION_NAME = "timeseries" # 데이터 적재시마다 컬렉션 이름을 변경

def connect_postgresql() : 
     return psycopg2.connect(
        host="114.200.199.53",
        database="postgres",
        user="postgres",
        password="Goodtime**95")

def connect_mongodb(MONGO_CONNECTION, MONGO_DB) : 
    client = MongoClient(MONGO_CONNECTION)
    mongo_db = client[MONGO_DB]
    return client, mongo_db