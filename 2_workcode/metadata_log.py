import datetime

def log_to_database(conn, log_data):
    """ 로그 데이터를 데이터베이스의 metadatalog 테이블에 저장합니다. """
    try:
        with conn.cursor() as cur:
            log_query = """
            INSERT INTO portal.metadatalog (documentid, logtime, type, perfomer,spatial, state, message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(log_query, (
                log_data['documentid'],
                datetime.datetime.now(),
                log_data['type'],
                log_data['perfomer'],
                log_data['spatial'],
                log_data['state'],
                log_data['message']
            ))
            print(f"로그 데이터를 metadatalog에 성공적으로 등록했습니다")
            conn.commit()
    except Exception as e:
        print(f"An error occurred while logging to the database: {e}")
        conn.rollback()

def metadata_register(conn, metadata):
    """ 메타데이터를 데이터베이스에 등록합니다. """
    try:
        with conn.cursor() as cur:
            metadata_query = """
            INSERT INTO portal.objects (
                objectid, name, type, kpi, createtime, updatetime, spatial, gugun, dong, attraction, tourtype, publicornot, rank, status, native, dimension, period
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(metadata_query, (
                metadata['objectid'],
                metadata['name'],
                metadata['type'],
                metadata['kpi'],
                metadata['createtime'],
                datetime.datetime.now(),
                metadata['spatial'],
                metadata['gugun'],
                metadata['dong'],
                metadata['attraction'],
                metadata['tourtype'],
                metadata['publicornot'],
                metadata['rank'],
                metadata['status'],
                metadata['native'],
                metadata['dimension'],
                metadata['period']
            ))
            print(f"메타데이터를 objects에 성공적으로 등록했습니다")
            conn.commit()
    except Exception as e:
        print(f"메타데이터를 등록하는 동안 오류가 발생했습니다: {e}")
        conn.rollback()

# def record_fileupload (conn,fileupload) :
#     """ 파일 업로드 정보를 fileupload 테이블에 기록합니다. """
#     try:
#         with conn.cursor() as cur:
#             fileupload_query = """
#             INSERT INTO portal.objectfile (objectid, fileuploadtimestamp, filetype, filelocation)
#             VALUES (%s, %s, %s, %s)
#             """
#             cur.execute(fileupload_query, (
#                 fileupload['objectid'],
#                 datetime.datetime.now(),
#                 fileupload['filetype'],
#                 fileupload['filelocation']
#             ))
#             conn.commit()
#     except Exception as e:
#         print(f"파일 업로드 정보를 기록하는 동안 오류가 발생했습니다: {e}")
#         conn.rollback()