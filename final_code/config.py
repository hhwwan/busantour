import psycopg2
from pymongo import MongoClient


POSTGRESQLHOST="114.200.199.53"
POSTGRESQLDATABASE="postgres"
POSTGRESQLUSER="postgres"
POSTGRESQLPASSWORD="Goodtime**95"

MONGO_CONNECTION = "mongodb://mongo:goodtime**95@1.234.51.110:38019"
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