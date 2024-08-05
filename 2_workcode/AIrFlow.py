import psycopg2
import pandas as pd
import random
import string
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from decimal import Decimal
from pymongo import MongoClient

# DAG 기본 설정
dag = DAG(
    dag_id="TimeSeries_pipeline",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@once",
)

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

##########################################################################################################################################################################################################
##########################################################################################################################################################################################################

def get_viewname_from_metadata():
    connection = connect_postgresql()
    cursor = connection.cursor()

    # 해당하는 viewName만 가져옵니다.
    cursor.execute("""
    SELECT objectid, viewname, kpi, native, dimension, type, period, spatial
    FROM portal.olapextract
    WHERE type = 'timeseries'""")
    objects = cursor.fetchall()  # 모든 결과를 가져옵니다.
    cursor.close()
    connection.close()

    return objects

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

##########################################################################################################################################################################################################
##########################################################################################################################################################################################################

# 공통 코드

# MongoDB 연결 객체
mongo_client = None
mongo_db = None

# 랜덤 문자열 생성 함수
def generate_short_id(length=6):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# 고유 ID 생성 함수
def generate_unique_id(objectid):
    prefix = objectid[:3]
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')
    random_string = generate_short_id()
    return f"{prefix}{current_time}_{random_string}"

# 쿼리 함수 맵핑
def get_query_type(update_type):
    query_types = {
        "daily": get_daily_query,
        "weekly": get_weekly_query,
        "monthly": get_monthly_query
    }
    return query_types.get(update_type)

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

##########################################################################################################################################################################################################
##########################################################################################################################################################################################################

# 부산시전역

# MongoDB에 데이터 저장
def transformation_all(data, update_type, id_value):

    # 필수 필드 설정
    if update_type == "daily":
        required_fields = ["기준년월일"]
        axis_key = "일별"
    elif update_type == "weekly":
        required_fields = ["연도", "주"]
        axis_key = "주별"
    elif update_type == "monthly":
        required_fields = ["연도", "월"]
        axis_key = "월별"
    else:
        print("Invalid update type.")
        return

    # 모든 필드 가져오기
    all_fields = data.columns.tolist()
    
    # 선택할 필드 설정 (필수 필드 + 나머지 필드)
    sum_columns = [field for field in all_fields if field not in ["기준년월일", "연도", "월", "주"]]

    # 데이터 축약
    if update_type in ["weekly", "monthly"]:
        data = aggregate_data(data, required_fields, sum_columns)
    
    # MongoDB 연결 설정
    collection = mongo_db[MONGO_COLLECTION_NAME]

    # MongoDB는 Json-like 문서에 데이터를 저장하므로 DataFrame을 dictionary 형태로 변환
    records = data.to_dict(orient="list")

    # 축 데이터 처리 및 정렬 인덱스 계산
    if update_type == "daily":
        axis_data = records["기준년월일"]
        sorted_indices = list(range(len(axis_data)))
        
    elif update_type == "weekly":
        axis_data = [f"{y}년{w}주차" for y, w in zip(records["연도"], records["주"])]
        sorted_indices = list(range(len(axis_data)))
        
    elif update_type == "monthly":
        # 월 데이터를 숫자로 변환하여 정렬한 후 다시 문자열로 변환
        sorted_indices = sorted(range(len(records["월"])), key=lambda k: int(f"{records['연도'][k]}{int(''.join(filter(str.isdigit, records['월'][k]))):02d}"))
        axis_data = [f"{records['연도'][i]}{int(''.join(filter(str.isdigit, records['월'][i]))):02d}" for i in sorted_indices]

    # 방문객 수 데이터 처리
    formatted_records = {field: {axis_key: [records[field][i] for i in sorted_indices]} for field in sum_columns}

    # 기존 문서 업데이트 또는 생성
    existing_doc = collection.find_one({'_id': id_value})

    if existing_doc:
        # 문서가 이미 존재하는 경우, 새로운 데이터를 병합합니다.
        if "데이터" not in existing_doc:
            existing_doc["데이터"] = {}
        for key in formatted_records:
            if key not in existing_doc["데이터"]:
                existing_doc["데이터"][key] = {}
            for axis in formatted_records[key]:
                if axis in existing_doc["데이터"][key]:
                    existing_doc["데이터"][key][axis].extend(formatted_records[key][axis])
                else:
                    existing_doc["데이터"][key][axis] = formatted_records[key][axis]
        if axis_key in existing_doc['축']:
            existing_doc['축'][axis_key].extend(axis_data)
        else:
            existing_doc['축'][axis_key] = axis_data
        doc = existing_doc
    else:
        # 문서가 존재하지 않는 경우, 새로운 문서를 생성합니다.
        doc = {
            "_id": id_value,
            "meta": {},
            "축": {axis_key: axis_data},
            "데이터": {}
        }
        for key in formatted_records:
            doc["데이터"][key] = formatted_records[key]

    # 필드 순서 보장하기 위해 doc를 새로운 dict로 재구성하여 업데이트
    ordered_doc = {
        "_id": doc["_id"],
        "meta": doc["meta"],
        "축": doc["축"],
        "데이터": doc["데이터"]
    }
    
    # 업데이트 작업 수행
    collection.replace_one({'_id': id_value}, ordered_doc, upsert=True)

# MongoDB meta에 기록
def record_mongo_all(unique_id, viewname, kpi, native, dimension, type, period, spatial):
    # MongoDB 연결 설정
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')

    doc = {
        "이름": unique_id,
        "지표": viewname,
        "키": kpi,
        "국적": native,
        "차원": dimension,
        "타입": type,
        "기간": period,
        "생성시간": current_time,
        "공간수준": spatial,
        "구군": None,
        "행정동": None,
        "관광지": None
    }
    # 기존 문서에 meta 필드 업데이트
    mongo_collection.update_one(
        {"_id": unique_id},
        {"$set": {"meta": doc}},
        upsert=True
    )
    print(f"Inserted document for objectid: {unique_id}")

# MongoDB에 삽입
def Insert_to_mongo_all():
    objects = get_viewname_from_metadata()

    # '부산시전역'인 경우만 필터링
    filtered_objects = [obj for obj in objects if obj[7] == '부산시전역']
    
    for object in filtered_objects:
        objectid, view_name, kpi, native, dimension, type, period, spatial = object
        unique_id = generate_unique_id(objectid)  # Unique ID 생성
        for update_type in ["daily", "weekly", "monthly"]:
            data = fetch_data(update_type, objectid, view_name)
            
            if data is not None and not data.empty:
                transformation_all(data, update_type, unique_id)
                print(f"{update_type.capitalize()} data for {unique_id} successfully loaded into MongoDB.")
            else:
                print(f"No data found for {unique_id} with {update_type} update.")
        record_mongo_all(unique_id, view_name, kpi, native, dimension, type, period, spatial)

def busan_all():
    global mongo_client, mongo_db
    mongo_client, mongo_db = connect_mongodb(MONGO_CONNECTION, MONGO_DB)

    try:
        Insert_to_mongo_all()
    finally:
        mongo_client.close()

#########################################################################################################################################################################################################
#########################################################################################################################################################################################################

# 구군

# MongoDB에 데이터 저장
def transformation_gugun(data, update_type, id_value, gugun):

    # 필수 필드 설정
    if update_type == "daily":
        required_fields = ["기준년월일"]
        axis_key = "일별"
    elif update_type == "weekly":
        required_fields = ["연도", "주"]
        axis_key = "주별"
    elif update_type == "monthly":
        required_fields = ["연도", "월"]
        axis_key = "월별"
    else:
        print("Invalid update type.")
        return

    # 모든 필드 가져오기
    all_fields = data.columns.tolist()
    
    # 선택할 필드 설정 (필수 필드 + 나머지 필드)
    sum_columns = [field for field in all_fields if field not in ["기준년월일", "연도", "월", "주", "시군구"]]

    # 데이터 축약
    if update_type in ["weekly", "monthly"]:
        data = aggregate_data(data, required_fields, sum_columns)
    
    # MongoDB 연결 설정
    collection = mongo_db[MONGO_COLLECTION_NAME]

    # MongoDB는 Json-like 문서에 데이터를 저장하므로 DataFrame을 dictionary 형태로 변환
    records = data.to_dict(orient="list")

    # 축 데이터 처리 및 정렬 인덱스 계산
    if update_type == "daily":
        axis_data = records["기준년월일"]
        sorted_indices = list(range(len(axis_data)))
        
    elif update_type == "weekly":
        axis_data = [f"{y}년{w}주차" for y, w in zip(records["연도"], records["주"])]
        sorted_indices = list(range(len(axis_data)))
        
    elif update_type == "monthly":
        # 월 데이터를 숫자로 변환하여 정렬한 후 다시 문자열로 변환
        sorted_indices = sorted(range(len(records["월"])), key=lambda k: int(f"{records['연도'][k]}{int(''.join(filter(str.isdigit, records['월'][k]))):02d}"))
        axis_data = [f"{records['연도'][i]}{int(''.join(filter(str.isdigit, records['월'][i]))):02d}" for i in sorted_indices]

    # 방문객 수 데이터 처리
    formatted_records = {field: {axis_key: [records[field][i] for i in sorted_indices]} for field in sum_columns}

    # 기존 문서 업데이트 또는 생성
    existing_doc = collection.find_one({'_id': id_value})

    if existing_doc:
        # 문서가 이미 존재하는 경우, 새로운 데이터를 병합합니다.
        if "데이터" not in existing_doc:
            existing_doc["데이터"] = {}
        for key in formatted_records:
            if key not in existing_doc["데이터"]:
                existing_doc["데이터"][key] = {}
            for axis in formatted_records[key]:
                if axis in existing_doc["데이터"][key]:
                    existing_doc["데이터"][key][axis].extend(formatted_records[key][axis])
                else:
                    existing_doc["데이터"][key][axis] = formatted_records[key][axis]
        if axis_key in existing_doc['축']:
            existing_doc['축'][axis_key].extend(axis_data)
        else:
            existing_doc['축'][axis_key] = axis_data
        doc = existing_doc
    else:
        # 문서가 존재하지 않는 경우, 새로운 문서를 생성합니다.
        doc = {
            "_id": id_value,
            "meta": {},
            "축": {axis_key: axis_data},
            "데이터": {}
        }
        for key in formatted_records:
            doc["데이터"][key] = formatted_records[key]

    # 필드 순서 보장하기 위해 doc를 새로운 dict로 재구성하여 업데이트
    ordered_doc = {
        "_id": doc["_id"],
        "meta": doc["meta"],
        "축": doc["축"],
        "데이터": doc["데이터"]
    }
    
    # 업데이트 작업 수행
    collection.replace_one({'_id': id_value}, ordered_doc, upsert=True)

    # MongoDB meta에 기록
def record_mongo_gugun(unique_id, viewname, kpi, native, dimension, type, period, spatial, gugun):
    # MongoDB 연결 설정
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')

    doc = {
        "이름": unique_id,
        "지표": viewname,
        "키": kpi,
        "국적": native,
        "차원": dimension,
        "타입": type,
        "기간": period,
        "생성시간": current_time,
        "공간수준": spatial,
        "구군": gugun,
        "행정동": None,
        "관광지": None
    }
    # 기존 문서에 meta 필드 업데이트
    mongo_collection.update_one(
        {"_id": unique_id},
        {"$set": {"meta": doc}},
        upsert=True
    )
    print(f"Inserted document for objectid: {unique_id}")

# MongoDB에 삽입
def Insert_to_mongo_gugun():
    objects = get_viewname_from_metadata()

    # '구군'인 경우만 필터링
    filtered_objects = [obj for obj in objects if obj[7] == '구군']
    
    for object in filtered_objects:
        objectid, view_name, kpi, native, dimension, type, period, spatial = object

        guguns_data = {}
        for update_type in ["daily", "weekly", "monthly"]:
            data = fetch_data(update_type, objectid, view_name)
            
            if data is not None and not data.empty:
                guguns = data["시군구"].unique()
                for gugun in guguns:
                    if gugun not in guguns_data:
                        guguns_data[gugun] = {"daily": None, "weekly": None, "monthly": None}
                    gugun_data = data[data["시군구"] == gugun]
                    guguns_data[gugun][update_type] = gugun_data

        for gugun, gugun_data in guguns_data.items():
            unique_id = generate_unique_id(objectid)  # Unique ID 생성
            for update_type, data in gugun_data.items():
                if data is not None:
                    transformation_gugun(data, update_type, unique_id, gugun)
            print(f"Data for {unique_id} successfully loaded into MongoDB.")
            record_mongo_gugun(unique_id, view_name, kpi, native, dimension, type, period, spatial, gugun)

def busan_gugun():
    global mongo_client, mongo_db
    mongo_client, mongo_db = connect_mongodb(MONGO_CONNECTION, MONGO_DB)

    try:
        Insert_to_mongo_gugun()
    finally:
        mongo_client.close()

#########################################################################################################################################################################################################
#########################################################################################################################################################################################################

# 행정동

# MongoDB에 데이터 저장
def transformation_adong(data, update_type, id_value, gugun, adong):

    # 행정동 이름이 "장전3동" 또는 "일광읍"인 경우 제외
    excluded_dongs = ["장전3동", "일광읍"]
    if data["행정동"].isin(excluded_dongs).any():
        return  # 해당 행정동을 포함하는 데이터는 처리하지 않음

    # 필수 필드 설정
    if update_type == "daily":
        required_fields = ["기준년월일"]
        axis_key = "일별"
    elif update_type == "weekly":
        required_fields = ["연도", "주"]
        axis_key = "주별"
    elif update_type == "monthly":
        required_fields = ["연도", "월"]
        axis_key = "월별"
    else:
        print("Invalid update type.")
        return

    # 모든 필드 가져오기
    all_fields = data.columns.tolist()
    
    # 선택할 필드 설정 (필수 필드 + 나머지 필드)
    sum_columns = [field for field in all_fields if field not in ["기준년월일", "연도", "월", "주", "시군구", "행정동"]]

    # 데이터 축약
    if update_type in ["weekly", "monthly"]:
        data = aggregate_data(data, required_fields, sum_columns)
    
    # MongoDB 연결 설정
    collection = mongo_db[MONGO_COLLECTION_NAME]

    # MongoDB는 Json-like 문서에 데이터를 저장하므로 DataFrame을 dictionary 형태로 변환
    records = data.to_dict(orient="list")

    # 축 데이터 처리 및 정렬 인덱스 계산
    if update_type == "daily":
        axis_data = records["기준년월일"]
        sorted_indices = list(range(len(axis_data)))
        
    elif update_type == "weekly":
        axis_data = [f"{y}년{w}주차" for y, w in zip(records["연도"], records["주"])]
        sorted_indices = list(range(len(axis_data)))
        
    elif update_type == "monthly":
        # 월 데이터를 숫자로 변환하여 정렬한 후 다시 문자열로 변환
        sorted_indices = sorted(range(len(records["월"])), key=lambda k: int(f"{records['연도'][k]}{int(''.join(filter(str.isdigit, records['월'][k]))):02d}"))
        axis_data = [f"{records['연도'][i]}{int(''.join(filter(str.isdigit, records['월'][i]))):02d}" for i in sorted_indices]

    # 방문객 수 데이터 처리
    formatted_records = {field: {axis_key: [records[field][i] for i in sorted_indices]} for field in sum_columns}

    # 기존 문서 업데이트 또는 생성
    existing_doc = collection.find_one({'_id': id_value})

    if existing_doc:
        # 문서가 이미 존재하는 경우, 새로운 데이터를 병합합니다.
        if "데이터" not in existing_doc:
            existing_doc["데이터"] = {}
        for key in formatted_records:
            if key not in existing_doc["데이터"]:
                existing_doc["데이터"][key] = {}
            for axis in formatted_records[key]:
                if axis in existing_doc["데이터"][key]:
                    existing_doc["데이터"][key][axis].extend(formatted_records[key][axis])
                else:
                    existing_doc["데이터"][key][axis] = formatted_records[key][axis]
        if axis_key in existing_doc['축']:
            existing_doc['축'][axis_key].extend(axis_data)
        else:
            existing_doc['축'][axis_key] = axis_data
        doc = existing_doc
    else:
        # 문서가 존재하지 않는 경우, 새로운 문서를 생성합니다.
        doc = {
            "_id": id_value,
            "meta": {
                "시군구": gugun,
                "행정동": adong
            },
            "축": {axis_key: axis_data},
            "데이터": {}
        }
        for key in formatted_records:
            doc["데이터"][key] = formatted_records[key]

    # 필드 순서 보장하기 위해 doc를 새로운 dict로 재구성하여 업데이트
    ordered_doc = {
        "_id": doc["_id"],
        "meta": doc["meta"],
        "축": doc["축"],
        "데이터": doc["데이터"]
    }
    
    # 업데이트 작업 수행
    collection.replace_one({'_id': id_value}, ordered_doc, upsert=True)

# MongoDB meta에 기록
def record_mongo_adong(unique_id, viewname, kpi, native, dimension, type, period, spatial, gugun, adong):
    # MongoDB 연결 설정
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')

    doc = {
        "이름": unique_id,
        "지표": viewname,
        "키": kpi,
        "국적": native,
        "차원": dimension,
        "타입": type,
        "기간": period,
        "생성시간": current_time,
        "공간수준": spatial,
        "구군": gugun,
        "행정동": adong,
        "관광지": None
    }
    # 기존 문서에 meta 필드 업데이트
    mongo_collection.update_one(
        {"_id": unique_id},
        {"$set": {"meta": doc}},
        upsert=True
    )
    print(f"Inserted document for objectid: {unique_id}")

# MongoDB에 삽입
def Insert_to_mongo_adong():
    objects = get_viewname_from_metadata()

    # '행정동'인 경우만 필터링
    filtered_objects = [obj for obj in objects if obj[7] == '행정동']

    excluded_dongs = ["장전3동", "일광읍"]  # 제외할 행정동 목록
    
    for object in filtered_objects:
        objectid, view_name, kpi, native, dimension, type, period, spatial = object

        adongs_data = {}
        gugun_mapping = {}  # 행정동과 시군구 매핑
        for update_type in ["daily", "weekly", "monthly"]:
            data = fetch_data(update_type, objectid, view_name)
            
            if data is not None and not data.empty:
                # 행정동 필터링: "장전3동", "일광읍" 제외
                data = data[~data['행정동'].isin(excluded_dongs)]

                adongs = data["행정동"].unique()
                for adong in adongs:
                    gugun = data[data["행정동"] == adong]["시군구"].iloc[0]
                    gugun_mapping[adong] = gugun  # 행정동과 시군구 매핑
                    if adong not in adongs_data:
                        adongs_data[adong] = {"daily": None, "weekly": None, "monthly": None}
                    adong_data = data[data["행정동"] == adong]
                    adongs_data[adong][update_type] = adong_data

        for adong, adong_data in adongs_data.items():
            gugun = gugun_mapping[adong]  # 매핑된 시군구 가져오기
            unique_id = generate_unique_id(objectid)  # Unique ID 생성
            for update_type, data in adong_data.items():
                if data is not None:
                    transformation_adong(data, update_type, unique_id, gugun, adong)
            print(f"Data for {unique_id} successfully loaded into MongoDB.")
            record_mongo_adong(unique_id, view_name, kpi, native, dimension, type, period, spatial, gugun, adong)

def busan_adong():
    global mongo_client, mongo_db
    mongo_client, mongo_db = connect_mongodb(MONGO_CONNECTION, MONGO_DB)

    try:
        Insert_to_mongo_adong()
    finally:
        mongo_client.close()

# ##########################################################################################################################################################################################################
# ##########################################################################################################################################################################################################

# 관광지

# MongoDB에 데이터 저장
def transformation_attraction(data, update_type, id_value, attraction):

    # 필수 필드 설정
    if update_type == "daily":
        required_fields = ["기준년월일"]
        axis_key = "일별"
    elif update_type == "weekly":
        required_fields = ["연도", "주"]
        axis_key = "주별"
    elif update_type == "monthly":
        required_fields = ["연도", "월"]
        axis_key = "월별"
    else:
        print("Invalid update type.")
        return

    # 모든 필드 가져오기
    all_fields = data.columns.tolist()
    
    # 선택할 필드 설정 (필수 필드 + 나머지 필드)
    sum_columns = [field for field in all_fields if field not in ["기준년월일", "연도", "월", "주", "관광지"]]

    # 데이터 축약
    if update_type in ["weekly", "monthly"]:
        data = aggregate_data(data, required_fields, sum_columns)
    
    # MongoDB 연결 설정
    collection = mongo_db[MONGO_COLLECTION_NAME]

    # MongoDB는 Json-like 문서에 데이터를 저장하므로 DataFrame을 dictionary 형태로 변환
    records = data.to_dict(orient="list")

    # 축 데이터 처리 및 정렬 인덱스 계산
    if update_type == "daily":
        axis_data = records["기준년월일"]
        sorted_indices = list(range(len(axis_data)))
        
    elif update_type == "weekly":
        axis_data = [f"{y}년{w}주차" for y, w in zip(records["연도"], records["주"])]
        sorted_indices = list(range(len(axis_data)))
        
    elif update_type == "monthly":
        # 월 데이터를 숫자로 변환하여 정렬한 후 다시 문자열로 변환
        sorted_indices = sorted(range(len(records["월"])), key=lambda k: int(f"{records['연도'][k]}{int(''.join(filter(str.isdigit, records['월'][k]))):02d}"))
        axis_data = [f"{records['연도'][i]}{int(''.join(filter(str.isdigit, records['월'][i]))):02d}" for i in sorted_indices]

    # 방문객 수 데이터 처리
    formatted_records = {field: {axis_key: [records[field][i] for i in sorted_indices]} for field in sum_columns}

    # 기존 문서 업데이트 또는 생성
    existing_doc = collection.find_one({'_id': id_value})

    if existing_doc:
        # 문서가 이미 존재하는 경우, 새로운 데이터를 병합합니다.
        if "데이터" not in existing_doc:
            existing_doc["데이터"] = {}
        for key in formatted_records:
            if key not in existing_doc["데이터"]:
                existing_doc["데이터"][key] = {}
            for axis in formatted_records[key]:
                if axis in existing_doc["데이터"][key]:
                    existing_doc["데이터"][key][axis].extend(formatted_records[key][axis])
                else:
                    existing_doc["데이터"][key][axis] = formatted_records[key][axis]
        if axis_key in existing_doc['축']:
            existing_doc['축'][axis_key].extend(axis_data)
        else:
            existing_doc['축'][axis_key] = axis_data
        doc = existing_doc
    else:
        # 문서가 존재하지 않는 경우, 새로운 문서를 생성합니다.
        doc = {
            "_id": id_value,
            "meta": {},
            "축": {axis_key: axis_data},
            "데이터": {}
        }
        for key in formatted_records:
            doc["데이터"][key] = formatted_records[key]

    # 필드 순서 보장하기 위해 doc를 새로운 dict로 재구성하여 업데이트
    ordered_doc = {
        "_id": doc["_id"],
        "meta": doc["meta"],
        "축": doc["축"],
        "데이터": doc["데이터"]
    }
    
    # 업데이트 작업 수행
    collection.replace_one({'_id': id_value}, ordered_doc, upsert=True)

# MongoDB meta에 기록
def record_mongo_attraction(unique_id, viewname, kpi, native, dimension, type, period, spatial, attraction):
    # MongoDB 연결 설정
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')

    doc = {
        "이름": unique_id,
        "지표": viewname,
        "키": kpi,
        "국적": native,
        "차원": dimension,
        "타입": type,
        "기간": period,
        "생성시간": current_time,
        "공간수준": spatial,
        "구군": None,
        "행정동": None,
        "관광지": attraction
    }
    # 기존 문서에 meta 필드 업데이트
    mongo_collection.update_one(
        {"_id": unique_id},
        {"$set": {"meta": doc}},
        upsert=True
    )
    print(f"Inserted document for objectid: {unique_id}")

# MongoDB에 삽입
def Insert_to_mongo_attraction():
    objects = get_viewname_from_metadata()

    # '관광지'인 경우만 필터링
    filtered_objects = [obj for obj in objects if obj[7] == '관광지']
    
    for object in filtered_objects:
        objectid, view_name, kpi, native, dimension, type, period, spatial = object

        attractions_data = {}
        for update_type in ["daily", "weekly", "monthly"]:
            data = fetch_data(update_type, objectid, view_name)
            
            if data is not None and not data.empty:
                attractions = data["관광지"].unique()
                for attraction in attractions:
                    if attraction not in attractions_data:
                        attractions_data[attraction] = {"daily": None, "weekly": None, "monthly": None}
                    attraction_data = data[data["관광지"] == attraction]
                    attractions_data[attraction][update_type] = attraction_data

        for attraction, attraction_data in attractions_data.items():
            unique_id = generate_unique_id(objectid)  # Unique ID 생성
            for update_type, data in attraction_data.items():
                if data is not None:
                    transformation_attraction(data, update_type, unique_id, attraction)
            print(f"Data for {unique_id} successfully loaded into MongoDB.")
            record_mongo_attraction(unique_id, view_name, kpi, native, dimension, type, period, spatial, attraction)

def busan_attraction():
    global mongo_client, mongo_db
    mongo_client, mongo_db = connect_mongodb(MONGO_CONNECTION, MONGO_DB)

    try:
        Insert_to_mongo_attraction()
    finally:
        mongo_client.close()

##########################################################################################################################################################################################################
##########################################################################################################################################################################################################

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
                datetime.now(),
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
                datetime.now(),
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

##########################################################################################################################################################################################################
##########################################################################################################################################################################################################

def fetch_all_documents(mongo_db):
    """MongoDB의 timeseries 컬렉션에서 모든 문서를 가져옵니다."""
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    return list(mongo_collection.find({}))

def convert_to_datetime(date_str):
    """ 'YYYYMMDDHHMMSS' 형식의 문자열을 datetime 객체로 변환합니다. """
    return datetime.strptime(date_str, '%Y%m%d%H%M%S')

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
                create_time_str = meta.get('생성시간', datetime.now().strftime('%Y%m%d%H%M%S'))
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

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()
        mongo_client.close()

#########################################################################################################################################################################################################
#########################################################################################################################################################################################################

# transformation_all.py의 busan_all 함수를 호출하는 작업 정의
task_transformationall = PythonOperator(
    task_id="busan_all",
    python_callable=busan_all,
    dag=dag,
)

# transformation_gugun.py의 busan_gugun 함수를 호출하는 작업 정의
task_transformationgugun = PythonOperator(
    task_id="busan_gugun",
    python_callable=busan_gugun,
    dag=dag,
)

# transformation_adong.py의 busan_adong 함수를 호출하는 작업 정의
task_transformationadong = PythonOperator(
    task_id="busan_adong",
    python_callable=busan_adong,
    dag=dag,
)

# transformation_attraction.py의 busan_attraction 함수를 호출하는 작업 정의
task_transformationattraction = PythonOperator(
    task_id="busan_attraction",
    python_callable=busan_attraction,
    dag=dag,
)

# metadata_record.py의 metadata_record 함수를 호출하는 작업 정의
task_metadatarecord = PythonOperator(
    task_id="metadata_record",
    python_callable=metadata_record,
    dag=dag,
)

# 의존성 설정
task_transformationall >> task_transformationgugun >> task_transformationadong >> task_transformationattraction >> task_metadatarecord