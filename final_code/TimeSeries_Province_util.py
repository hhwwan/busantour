import pandas as pd
from datetime import datetime
from decimal import Decimal
from config import connect_postgresql
from loging import *
from util import get_existing_metadata
from variable import boxplot_timeseries_start_period, boxplot_timeseries_end_period

### 고유 ID 생성 함수
def generate_unique_id(objectid, spatiallevel, kpi, nationality, dimension, gugun_name=None, adong_name=None, attraction_name=None):
    
    # PostgreSQL 연결
    connection = connect_postgresql()
    cursor = connection.cursor()

    # spatiallevel 가져오기
    cursor.execute(f'SELECT "spatiallevel" FROM "metadata"."spatiallevel" WHERE "spatiallevelname" = %s', (spatiallevel,))
    spatial_level = cursor.fetchone()
    spatial_level = spatial_level[0] if spatial_level else "Unknown"

    # kpiid 가져오기
    cursor.execute(f'SELECT "kpiid" FROM "metadata"."kpi" WHERE "kpiname" = %s', (kpi,))
    kpi_id = cursor.fetchone()
    kpi_id = kpi_id[0] if kpi_id else "Unknown"

    # nationality 가져오기
    cursor.execute(f'SELECT "nationality" FROM "metadata"."nationality" WHERE "nationalityname" = %s', (nationality,))
    nationality_id = cursor.fetchone()
    nationality_id = nationality_id[0] if nationality_id else "Unknown"

    # dimension 가져오기
    cursor.execute(f'SELECT "dimensionid" FROM "metadata"."dimension" WHERE "dimensionname" = %s', (dimension,))
    dimension_id = cursor.fetchone()
    dimension_id = dimension_id[0] if dimension_id else "Unknown"

    # PostgreSQL 연결 종료
    cursor.close()
    connection.close()

     # 고유 ID 생성
    unique_id = f"T_{spatial_level}_{kpi_id}_{nationality_id}_{dimension_id}"
    return unique_id

##### extract 하는데 필요한 함수 목록 #####


# 스키마 매핑 및 유효한 ID 목록
SCHEMA_MAPPING = {
    "AOT": "부산시전역",
    "AFT": "부산시전역",
    "GOT": "구군",
    "GFT": "구군",
    "HOT": "행정동",
    "HFT": "행정동",
    "TOT": "관광지",
    "TFT": "관광지"
}

# 조건 정의
CONDITIONS = {
    "daily": """"기준년월일" BETWEEN '2022-01-01' AND '2024-01-01'""",
    "weekly": """("연도"::int >= 2022 AND "주"::int >= 1) and ("연도"::int <= 2023 AND "주"::int <= 52)""",
    "monthly": """("연도"::int >= 2022 AND regexp_replace("월", '[^0-9]', '', 'g')::int >= 1) and ("연도"::int <= 2023 AND regexp_replace("월", '[^0-9]', '', 'g')::int <= 12)"""
}

def get_query(update_type, id_value, view_name):

    prefix = id_value[:3]  # 앞 3글자 추출
    schema = SCHEMA_MAPPING.get(prefix)
    
    if not schema:
        print("Schema not found for the provided ID.")
        return None

    condition = CONDITIONS.get(update_type)
    if not condition:
        print("Invalid update type.")
        return None

    base_query = f'SELECT * FROM "{schema}".{view_name} WHERE {condition}'

    return base_query

# ID에 따라 쿼리 선택
def get_daily_query(update_type, id_value, view_name):
    return get_query(update_type, id_value, view_name)

def get_weekly_query(update_type, id_value, view_name):
    return get_query(update_type, id_value, view_name)

def get_monthly_query(update_type, id_value, view_name):
    return get_query(update_type, id_value, view_name)

# 쿼리 함수 맵핑
def get_query_type(update_type):
    query_types = {
        "daily": get_daily_query,
        "weekly": get_weekly_query,
        "monthly": get_monthly_query
    }
    return query_types.get(update_type)


##### transformation 및 load 하는데 필요한 함수 목록 #####


# 데이터 가져오기 및 변환 함수
def fetch_data(update_type, id_value, view_name):
    query_type = get_query_type(update_type)
    if query_type is None:
        print("Invalid update type.")
        return None
    
    query = query_type(update_type, id_value, view_name)
    if query is None:
        return None

    connection = connect_postgresql()
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

# 주별 및 월별 데이터를 축약하여 합계 계산
def aggregate_data(data, group_by_columns, sum_columns):
    aggregated_data = data.groupby(group_by_columns)[sum_columns].sum().reset_index()
    return aggregated_data

# MongoDB meta에 기록
def record_mongo(mongo_db, mongo_collection, unique_id, viewname, objecttype, spatiallevel, kpi, nationality, dimension, gugun_code=None, adong_code=None, attraction_code=None):
    # MongoDB 연결 설정
    collection = mongo_db[mongo_collection]
    
    codes = unique_id.split('_')
    objecttype_code = codes[0] # type 코드
    spatiallevel_code = codes[1]  # spatiallevel 코드
    kpi_code = codes[2]  # KPI 코드
    nationality_code = codes[3]  # nationality 코드
    dimension_code = codes[4]  # dimension 코드

    # `startperiod`와 `endperiod` 값을 `custom_code.py`의 변수로 설정
    startperiod = boxplot_timeseries_start_period
    endperiod = boxplot_timeseries_end_period

    doc = {
        "objectid": unique_id,
        "objectname": viewname,
        "objecttype": objecttype_code,
        "spatiallevel": spatiallevel_code,
        "kpi": kpi_code,
        "nationality": nationality_code,
        "dimension": dimension_code,
        "startperiod": startperiod,
        "endperiod": endperiod,
        "gugun": gugun_code if gugun_code else None, # 구군코드 있으면 삽입 없으면 None
        "dong": adong_code if adong_code else None, # 행정동코드 있으면 삽입 없으면 None
        "attraction": attraction_code if attraction_code else None # 관광지코드 있으면 삽입 없으면 None
    }

    # 문서 삽입 또는 업데이트
    try:
        # 먼저 해당 _id가 이미 존재하는지 확인
        existing_doc = collection.find_one({"_id": unique_id})

        if existing_doc:
            # 존재하는 경우 업데이트
            collection.update_one(
                {"_id": unique_id},
                {"$set": {"meta": doc}}
            )
            print(f"Inserted document for objectid: {unique_id}")
        else:
            # 존재하지 않는 경우 새 문서 삽입
            collection.insert_one({"_id": unique_id, "meta": doc})
            print(f"Inserted document for objectid: {unique_id}")

    except Exception as e:
        print(f"Error inserting or updating document: {str(e)}")


##### register 하는데 필요한 함수 목록 #####


# Period만 업데이트 하는 로직
def update_periods(conn, objectid, startperiod, endperiod):
    """ PostgreSQL 데이터베이스에서 objectid에 해당하는 문서의 시작 및 종료 기간을 업데이트합니다. """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE metadata.object
                SET startperiod = %s, endperiod = %s
                WHERE objectid = %s
            """, (startperiod, endperiod, objectid))
            conn.commit()
            print(f"objectid {objectid}의 startperiod와 endperiod가 업데이트 되었습니다.")
    except Exception as e:
        print(f"기간을 업데이트하는 동안 오류가 발생했습니다: {e}")
        conn.rollback()

# object와 objectlog처리
def object_objectlog(conn, document, collection_name):
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
    existing_metadata = get_existing_metadata(conn, document['_id'])

    manipulationtype = 1  # 기본값은 'create' (1)

    try:
        if existing_metadata:
            manipulationtype = 3  # 이미 존재하는 경우 'update' (3)

            # objectid가 존재하면 startperiod와 endperiod만 업데이트
            update_periods(conn, document['_id'], meta.get('startperiod'), meta.get('endperiod'))

        else:
            # 존재하지 않는 경우 'create' (1)
            manipulationtype = 1

            metadata_register(conn, metadata)
            print(f"메타데이터를 object에 성공적으로 등록했습니다")

            # objectid가 이미 존재하는 경우는 manipulationtype을 3으로 지정하고 업데이트, 존재하지 않는 경우에는 manipulationtype을 1로 지정하고 등록

        log_message = "업데이트되었습니다" if manipulationtype == 3 else "성공적으로 저장되었습니다"
        log_to_database(conn, {
            'memberemail': 'dhkim000404@gmail.com',
            'objectid': document['_id'],
            'manipulationtype': manipulationtype,
            'state': 'Success',
            'message': f"objectid {document['_id']}의 데이터가 성공적으로 저장되었습니다."
            })
        print(f"로그 데이터가 objectlog에 {log_message}")     

    except Exception as e:
        print(f"로그를 기록하는 동안 오류가 발생했습니다: {e}")
        conn.rollback()                  
                    
# objectfile 처리
def objectfile(conn, document, collection_name):
    handle_objectfile_logging(
        conn,
        document['_id'],
        f"mongodb://1.234.51.110:12542/timeseries/{collection_name}",
        1,
        '1.234.51.110:12542',
        'timeseries',
        collection_name,
        None
    )
                
# kpi_object_relationship 처리
def kpi(conn, document, collection_name):
    meta = document['meta']
    kpis = meta.get('kpi')
    
    # kpis가 리스트가 아니면 리스트로 변환
    if not isinstance(kpis, list):
        kpis = [kpis]
        
    handle_kpi_logging(
        conn, 
        document['_id'], 
        kpis
    )

# dimension_object_relationship 처리
def dimension(conn, document, collection_name):  
    meta = document['meta']
    dimensions = meta.get('dimension')
    
    # dimensions가 리스트가 아니면 리스트로 변환
    if not isinstance(dimensions, list):
        dimensions = [dimensions]
        
    handle_dimension_logging(
        conn, 
        document['_id'], 
        dimensions
    )