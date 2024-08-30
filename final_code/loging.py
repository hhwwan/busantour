import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, "../.."))))

def log_to_database(conn, log_data):
    """ 로그 데이터를 데이터베이스의 ObjectLog 테이블에 저장합니다. """
    try:
        with conn.cursor() as cur:
            log_query = """
            INSERT INTO metadata.objectlog (memberemail, objectid, manipulationtimestamp, manipulationtype, state, message)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.execute(log_query, (
                log_data['memberemail'],
                log_data['objectid'],
                datetime.datetime.now(),
                log_data['manipulationtype'],
                log_data['state'],
                log_data['message']
            ))
            conn.commit()
    except Exception as e:
        print(f"데이터베이스에 로그를 기록하는 동안 오류가 발생했습니다: {e}")
        conn.rollback()

def metadata_register(conn, metadata):
    """ 메타데이터를 데이터베이스에 등록합니다. """
    try:
        with conn.cursor() as cur:
            metadata_query = """
            INSERT INTO metadata.object (
                objectid, objectname, objecttype, spatiallevel, securitylevel, public_or_not, status, nationality, poitourtype, gugun, dong, attraction, startperiod, endperiod
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(metadata_query, (
                metadata['objectid'],
                metadata['objectname'],
                metadata['objecttype'],
                metadata['spatiallevel'],
                metadata['securitylevel'],
                metadata['public_or_not'],
                metadata['status'],
                metadata['nationality'],
                metadata['poitourtype'],
                metadata['gugun'],
                metadata['dong'],
                metadata['attraction'],
                metadata['startperiod'],
                metadata['endperiod']
            ))
            conn.commit()
    except Exception as e:
        print(f"메타데이터를 등록하는 동안 오류가 발생했습니다: {e}")
        conn.rollback()

def metadata_update(conn, metadata):
    """ 메타데이터를 데이터베이스에 업데이트합니다. """
    try:
        with conn.cursor() as cur:
            update_query = """
            UPDATE metadata.object
            SET objectname = %s,
                objecttype = %s,
                spatiallevel = %s,
                securitylevel = %s,
                public_or_not = %s,
                status = %s,
                nationality = %s,
                poitourtype = %s,
                gugun = %s,
                dong = %s,
                attraction = %s,
                startperiod = %s,
                endperiod = %s
            WHERE objectid = %s
            """
            cur.execute(update_query, (
                metadata['objectname'],
                metadata['objecttype'],
                metadata['spatiallevel'],
                metadata['securitylevel'],
                metadata['public_or_not'],
                metadata['status'],
                metadata['nationality'],
                metadata['poitourtype'],
                metadata['gugun'],
                metadata['dong'],
                metadata['attraction'],
                metadata['startperiod'],
                metadata['endperiod'],
                metadata['objectid']  # 업데이트 기준이 되는 objectid
            ))
            conn.commit()
    except Exception as e:
        print(f"메타데이터를 업데이트하는 동안 오류가 발생했습니다: {e}")
        conn.rollback()


def handle_objectfile_logging(conn, objectid, file_path, filetype, server, database, collection, documentid):
    """Objectfile을 업데이트하거나 삽입합니다."""
    timestamp = datetime.datetime.now()
    query_select = """
    SELECT COUNT(*) FROM metadata.objectfile WHERE objectid = %s
    """
    query_update = """
    UPDATE metadata.objectfile SET fileuploadtimestamp = %s WHERE objectid = %s
    """
    query_insert = """
    INSERT INTO metadata.objectfile (
        objectid, fileuploadtimestamp, filetype, server, database, collection, documentid, filelocation
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        with conn.cursor() as cur:
            # objectid가 존재하는지 확인
            cur.execute(query_select, (objectid,))
            exists = cur.fetchone()[0]

            if exists:
                # 존재할 경우, 업데이트 수행
                cur.execute(query_update, (timestamp, objectid))
                conn.commit()
                print(f"objectfile에 대한 파일 업로드 타임스탬프가 업데이트되었습니다: {file_path}")
            else:
                # 존재하지 않을 경우, 새로 삽입
                cur.execute(query_insert, (objectid, timestamp, filetype, server, database, collection, documentid, file_path))
                conn.commit()
                print(f"새로운 objectfile 데이터가 삽입되었습니다: {file_path}")
    except Exception as e:
        print(f"objectfile에 데이터를 삽입하거나 업데이트하는 동안 오류가 발생했습니다: {e}")
        conn.rollback()


def handle_kpi_logging(conn, objectid, kpi_ids):
    """KPI 데이터를 로깅하거나 필요시 데이터베이스에 삽입합니다."""
    try:
        for kpi_id in kpi_ids:
            with conn.cursor() as cur:
                # KPI 관계가 이미 존재하는지 확인
                check_query = """
                SELECT COUNT(*) FROM metadata.kpi_object_relationship WHERE objectid = %s AND kpiid = %s
                """
                cur.execute(check_query, (objectid, kpi_id))
                exists = cur.fetchone()[0]

                if exists:
                    print(f"KPI ID {kpi_id}가 kpi_object_relationship에 이미 존재합니다.")
                else:
                    # 존재하지 않는 경우, 새로운 KPI 관계를 삽입
                    kpi_query = """
                    INSERT INTO metadata.kpi_object_relationship (objectid, kpiid)
                    VALUES (%s, %s)
                    """
                    cur.execute(kpi_query, (
                        objectid,
                        kpi_id
                    ))
                    print(f"KPI ID {kpi_id}를 kpi_object_relationship에 성공적으로 등록했습니다")
                    conn.commit()
    except Exception as e:
        print(f"KPI 데이터를 처리하는 동안 오류가 발생했습니다: {e}")
        conn.rollback()

def handle_dimension_logging(conn, objectid, dimension_ids):
    """차원 데이터를 로깅하거나 필요시 데이터베이스에 삽입합니다."""
    try:
        for dimension_id in dimension_ids:
            with conn.cursor() as cur:
                # Dimension 관계가 이미 존재하는지 확인
                check_query = """
                SELECT COUNT(*) FROM metadata.dimension_object_relationship WHERE objectid = %s AND dimensionid = %s
                """
                cur.execute(check_query, (objectid, dimension_id))
                exists = cur.fetchone()[0]

                if exists:
                    print(f"Dimension ID {dimension_id}가 dimension_object_relationship에 이미 존재합니다.")
                else:
                    # 존재하지 않는 경우, 새로운 Dimension 관계를 삽입
                    dimension_query = """
                    INSERT INTO metadata.dimension_object_relationship (objectid, dimensionid)
                    VALUES (%s, %s)
                    """
                    cur.execute(dimension_query, (
                        objectid,
                        dimension_id
                    ))
                    print(f"Dimension ID {dimension_id}를 dimension_object_relationship에 성공적으로 등록했습니다")
                    conn.commit()
    except Exception as e:
        print(f"차원 데이터를 처리하는 동안 오류가 발생했습니다: {e}")
        conn.rollback()