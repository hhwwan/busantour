import pandas as pd
from datetime import datetime
from decimal import Decimal
from pymongo import MongoClient
from config import connect_postgresql, connect_mongodb, MONGO_CONNECTION, MONGO_DB
from Timeseries_extract_queries import get_daily_query, get_weekly_query, get_monthly_query, get_viewname_from_metadata
from custom_code import boxplot_timeseries_start_period, boxplot_timeseries_end_period

# MongoDB 연결 객체
mongo_client = None
mongo_db = None

# 고유 ID 생성 함수
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

    # 구군코드, 행정동코드, 관광지코드 초기화
    gu_code = ""
    dong_code = ""
    tourist_code = ""

    if spatiallevel == "구군" and gugun_name:
        # 구군 코드 매핑 가져오기
        gugun_mapping = get_code_mapping()
        gu_code = gugun_mapping.get(gugun_name, "Unknown")

    elif spatiallevel == "행정동" and adong_name:
        sigungu_mapping, adong_mapping = get_code_mapping()
        gu_code = sigungu_mapping.get(gugun_name, "Unknown")
        dong_code = adong_mapping.get(adong_name, "Unknown")

    elif spatiallevel == "관광지" and attraction_name:
        # 구군 코드 매핑 가져오기
        attraction_mapping = get_code_mapping()
        tourist_code = attraction_mapping.get(attraction_name, "Unknown")

    # PostgreSQL 연결 종료
    cursor.close()
    connection.close()

    # 고유 ID 생성 (구군과 행정동이 없을 경우 특별한 값 추가)
    parts = [f"T_{spatial_level}_{kpi_id}_{nationality_id}_{dimension_id}"]
    if gu_code:
        parts.append(gu_code)
    if dong_code:
        parts.append(dong_code)
    if tourist_code:
        parts.append(tourist_code)

    unique_id = "_".join(parts)
    return unique_id

# PostgreSQL에서 시군구코드 매핑 데이터 가져오기
def get_code_mapping():
    connection = connect_postgresql()
    cursor = connection.cursor()

    query = "SELECT 시군구, 시군구코드 FROM code.\"부산_시군구코드\""
    cursor.execute(query)
    data = cursor.fetchall()

    # 시군구 이름에서 "부산광역시 " 제거
    mapping = {row[0].replace("부산광역시 ", ""): row[1] for row in data}

    cursor.close()
    connection.close()

    return mapping

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

# MongoDB에 데이터 저장
def transformation(data, update_type, id_value, gugun):

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
def record_mongo(unique_id, viewname, objecttype, spatiallevel, kpi, nationality, dimension, gugun_code):
    # MongoDB 연결 설정
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    
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
        "gugun": gugun_code,
        "dong": None,
        "attraction": None
    }

    # 문서 삽입 또는 업데이트
    try:
        # 먼저 해당 _id가 이미 존재하는지 확인
        existing_doc = mongo_collection.find_one({"_id": unique_id})

        if existing_doc:
            # 존재하는 경우 업데이트
            mongo_collection.update_one(
                {"_id": unique_id},
                {"$set": {"meta": doc}}
            )
            print(f"Inserted document for objectid: {unique_id}")
        else:
            # 존재하지 않는 경우 새 문서 삽입
            mongo_collection.insert_one({"_id": unique_id, "meta": doc})
            print(f"Inserted document for objectid: {unique_id}")

    except Exception as e:
        print(f"Error inserting or updating document: {str(e)}")

# MongoDB에 삽입
def Insert_to_mongo(collection_name):
    global MONGO_COLLECTION_NAME
    MONGO_COLLECTION_NAME = collection_name

    objects = get_viewname_from_metadata()

    # '구군'인 경우만 필터링
    filtered_objects = [obj for obj in objects if obj[8] == '구군']

    # 시군구코드 매핑 가져오기
    code_mapping = get_code_mapping()
    
    for object in filtered_objects:
        objectid, view_name, kpi, nationality, dimension, objecttype, startperiod, endperiod, spatiallevel = object

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
            unique_id = generate_unique_id(objectid, spatiallevel, kpi, nationality, dimension, gugun_name=gugun)  # Unique ID 생성
            for update_type, data in gugun_data.items():
                if data is not None:
                    transformation(data, update_type, unique_id, gugun)
            gugun_code = code_mapping.get(gugun)
            print(f"Data for {unique_id} successfully loaded into MongoDB.")
            record_mongo(unique_id, view_name, objecttype, spatiallevel, kpi, nationality, dimension, gugun_code)

def busan_gugun(collection_name):
    global mongo_client, mongo_db
    mongo_client = MongoClient(MONGO_CONNECTION)  # MongoClient 객체 생성
    mongo_db = mongo_client[MONGO_DB]  # 특정 데이터베이스에 연결

    try:
        Insert_to_mongo(collection_name)
    finally:
        mongo_client.close()

if __name__ == "__main__":
    busan_gugun()