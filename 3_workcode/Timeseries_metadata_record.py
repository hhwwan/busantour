from datetime import datetime
from pymongo import MongoClient
from config import connect_postgresql, connect_mongodb, MONGO_CONNECTION, MONGO_DB
from loging import log_to_database, metadata_register, log_objectfile, log_to_kpi, log_to_dimension

def fetch_all_documents(mongo_db):
    """MongoDB의 특정 컬렉션에서 모든 문서를 가져옵니다."""
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    return list(mongo_collection.find({}))

# 특정 테이블에 동일한 objectid가 있는지 확인하는 로직
def is_exists(conn, table, objectid):
    """ 주어진 objectid가 PostgreSQL의 특정 테이블에 존재하는지 확인합니다. """
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM metadata.{table} WHERE objectid = %s LIMIT 1", (objectid,))
        return cur.fetchone() is not None

# Period만 업데이트 하는 로직
def update_periods(conn, objectid, startperiod, endperiod):
    """ PostgreSQL 데이터베이스에서 objectid에 해당하는 문서의 시작 및 종료 기간을 업데이트합니다. """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE metadata.object
            SET startperiod = %s, endperiod = %s
            WHERE objectid = %s
        """, (startperiod, endperiod, objectid))
        conn.commit()
        print(f"objectid {objectid}의 startperiod와 endperiod가 업데이트 되었습니다.")

# Metadata 기록
def metadata_record(collection_name):
    
    global MONGO_COLLECTION_NAME
    MONGO_COLLECTION_NAME = collection_name

    # PostgreSQL 연결 설정
    conn = connect_postgresql()
    # MongoDB 연결 설정
    mongo_client = MongoClient(MONGO_CONNECTION)  # MongoClient 객체 생성
    mongo_db = mongo_client[MONGO_DB]  # 특정 데이터베이스에 연결

    documents = fetch_all_documents(mongo_db)

    try:
        for document in documents:
            if 'meta' in document:
                meta = document['meta']

                metadata = {
                    'objectid': document['_id'],
                    'objectname': meta.get('objectname'),
                    'objecttype': meta.get('objecttype'),
                    'public_or_not': True,
                    'securitylevel': 4,
                    'status': False,
                    'spatiallevel': meta.get('spatiallevel'),
                    'nationality': meta.get('nationality'),
                    'poitourtype': None,
                    'gugun': meta.get('gugun'),
                    'dong': meta.get('dong'),
                    'attraction': meta.get('attraction'),
                    'startperiod': meta.get('startperiod'),
                    'endperiod': meta.get('endperiod'),
                }

                # objectid가 존재하지 않는 경우에만 새롭게 등록
                if not is_exists(conn, "object", document['_id']):
                    metadata_register(conn, metadata)
                    print(f"메타데이터를 object에 성공적으로 등록했습니다")  
                else:
                    # objectid가 존재하면 startperiod와 endperiod만 업데이트
                    update_periods(conn, document['_id'], meta.get('startperiod'), meta.get('endperiod'))

                # objectid가 이미 존재하는 경우는 manipulationtype을 3으로 지정하고 업데이트, 존재하지 않는 경우에는 manipulationtype을 1로 지정하고 등록
                if not is_exists(conn, "objectlog", document['_id']):
                    log_to_database(conn, {
                        'memberemail': 'dhkim000404@gmail.com',
                        'objectid': document['_id'],
                        'manipulationtype': 1,
                        'state': 'Success',
                        'message': f"objectid {document['_id']}의 데이터가 성공적으로 저장되었습니다."
                    })
                    print(f"로그 데이터를 objectlog에 성공적으로 등록했습니다")                       
                else:
                    log_to_database(conn, {
                        'memberemail': 'dhkim000404@gmail.com',
                        'objectid': document['_id'],
                        'manipulationtype': 3,
                        'state': 'Success',
                        'message': f"objectid {document['_id']}의 데이터가 이미 존재합니다. 업데이트되었습니다."
                    })
                    print(f"로그 데이터를 objectlog에 업데이트 하였습니다")
                
                # objectid가 이미 존재하는 경우는 넘기고, 존재하지 않는 경우에만 등록
                if not is_exists(conn, "objectfile", document['_id']):
                    filelocation = f"mongodb://1.234.51.110:12542/timeseries/{collection_name}"
                    
                    # 필요한 값을 미리 설정합니다.
                    objectid = document['_id']
                    timestamp = datetime.now()  # 현재 시간을 사용하여 timestamp 생성
                    filetype = 1
                    server = '1.234.51.110:12542'
                    database = 'timeseries'
                    collection = collection_name
                    documentid = None
                    filelocation = filelocation

                    # log_objectfile 함수를 호출할 때 모든 인자를 전달합니다.
                    log_objectfile(conn, objectid, timestamp, filetype, server, database, collection, documentid, filelocation)
                    
                    print(f"filelocation 데이터를 objectfile에 성공적으로 등록했습니다.")
                else:
                    print(f"objectid {document['_id']}는 이미 존재합니다. objectfile에 등록을 생략합니다.")
                
                # objectid가 이미 존재하는 경우는 넘기고, 존재하지 않는 경우에만 등록
                if not is_exists(conn, "kpi_object_relationship", document['_id']):
                   log_to_kpi(conn, {
                       'objectid': document['_id'],
                       'kpiid': meta.get('kpi')
                   })
                else:
                    print(f"objectid {document['_id']}는 이미 존재합니다. kpi_object_relationship에 등록을 생략합니다.") 
                
                # objectid가 이미 존재하는 경우는 넘기고, 존재하지 않는 경우에만 등록
                if not is_exists(conn, "dimension_object_relationship", document['_id']):
                   log_to_dimension(conn, {
                       'objectid': document['_id'],
                       'dimensionid': meta.get('dimension')
                   })
                else:
                    print(f"objectid {document['_id']}는 이미 존재합니다. dimension_object_relationship에 등록을 생략합니다.") 

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()
        mongo_client.close()

if __name__ == '__main__':
    metadata_record()