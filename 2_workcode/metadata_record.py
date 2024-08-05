import datetime
from config import connect_postgresql, connect_mongodb, MONGO_COLLECTION_NAME, MONGO_CONNECTION, MONGO_DB
from metadata_log import log_to_database, metadata_register

def fetch_all_documents(mongo_db):
    """MongoDB의 timeseries 컬렉션에서 모든 문서를 가져옵니다."""
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    return list(mongo_collection.find({}))

def convert_to_datetime(date_str):
    """ 'YYYYMMDDHHMMSS' 형식의 문자열을 datetime 객체로 변환합니다. """
    return datetime.datetime.strptime(date_str, '%Y%m%d%H%M%S')

def is_objectid_exists(conn, objectid):
    """ 주어진 objectid가 PostgreSQL의 objects 테이블에 존재하는지 확인합니다. """
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM portal.objects WHERE objectid = %s LIMIT 1", (objectid,))
        return cur.fetchone() is not None

def is_documentid_exists(conn, documentid):
    """ 주어진 documentid가 PostgreSQL의 metadatalog 테이블에 존재하는지 확인합니다. """
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM portal.metadatalog WHERE documentid = %s LIMIT 1", (documentid,))
        return cur.fetchone() is not None

def metadata_record():
    # PostgreSQL 연결 설정
    conn = connect_postgresql()
    # MongoDB 연결 설정
    mongo_client, mongo_db = connect_mongodb(MONGO_CONNECTION, MONGO_DB)

    documents = fetch_all_documents(mongo_db)

    try:
        for document in documents:
            if 'meta' in document:
                meta = document['meta']
                create_time_str = meta.get('생성시간', datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
                create_time = convert_to_datetime(create_time_str)

                metadata = {
                    'objectid': document['_id'],
                    'name': meta.get('지표'),
                    'type': meta.get('타입'),
                    'kpi': meta.get('키'),
                    'createtime': create_time,
                    'spatial': meta.get('공간수준'),
                    'gugun': meta.get('구군'),
                    'dong': meta.get('행정동'),
                    'attraction': meta.get('관광지'),
                    'publicornot': 1,
                    'tourtype': 'SomeTourType',
                    'rank': 'standard',
                    'status': False,
                    'native': meta.get('국적'),
                    'dimension': meta.get('차원'),
                    'period': meta.get('기간')
                }

                # objectid가 이미 존재하는 경우는 넘기고, 존재하지 않는 경우에만 등록
                if not is_objectid_exists(conn, metadata['objectid']):
                    metadata_register(conn, metadata)
                else:
                    print(f"objectid {metadata['objectid']}는 이미 존재합니다. 등록을 생략합니다.")

                # documentid가 이미 존재하는 경우는 넘기고, 존재하지 않는 경우에만 등록
                if not is_documentid_exists(conn, document['_id']):
                    log_to_database(conn, {
                        'documentid': document['_id'],
                        'type': meta.get('타입'),
                        'spatial': meta.get('공간수준'),
                        'perfomer': '김동환',
                        'state': 'Success',
                        'message': f"{meta.get('지표')}의 데이터가 성공적으로 저장되었습니다."
                    })
                else:
                    print(f"documentid {document['_id']}는 이미 존재합니다. 로그 등록을 생략합니다.")

                # record_fileupload(conn, {
                #     'objectid': document['_id'],
                #     'filetype': 1,
                #     'filelocation': f"{meta.get('지표')}의 데이터가 성공적으로 저장되었습니다."
                # })

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()
        mongo_client.close()

if __name__ == '__main__':
    metadata_record()