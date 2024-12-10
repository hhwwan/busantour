import psycopg2
from pymongo import MongoClient


POSTGRESQLHOST="**"
POSTGRESQLDATABASE="postgres"
POSTGRESQLUSER="postgres"
POSTGRESQLPASSWORD="**"

MONGO_CONNECTION = "**"
MONGO_DB = 'timeseries' # 데이터 적재시마다 데이터베이스 이름을 변경

def connect_postgresql() : 
     return psycopg2.connect(
        host = POSTGRESQLHOST,
        database=POSTGRESQLDATABASE,
        user=POSTGRESQLUSER,
        password=POSTGRESQLPASSWORD)


def connect_mongodb(MONGO_CONNECTION, MONGO_DB) : 
    client = MongoClient(MONGO_CONNECTION)
    mongo_db = client[MONGO_DB] 
    return mongo_db