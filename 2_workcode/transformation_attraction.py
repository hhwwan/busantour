import pandas as pd
import random
import string
from datetime import datetime
from decimal import Decimal
from config import connect_postgresql, connect_mongodb, MONGO_COLLECTION_NAME, MONGO_CONNECTION, MONGO_DB
from transformation_queries import get_daily_query, get_weekly_query, get_monthly_query, get_viewname_from_metadata

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

# MongoDB에 데이터 저장
def transformation(data, update_type, id_value, attraction):

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
def record_mongo(unique_id, viewname, kpi, native, dimension, type, period, spatial, attraction):
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
def Insert_to_mongo():
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
                    transformation(data, update_type, unique_id, attraction)
            print(f"Data for {unique_id} successfully loaded into MongoDB.")
            record_mongo(unique_id, view_name, kpi, native, dimension, type, period, spatial, attraction)

def busan_attraction():
    global mongo_client, mongo_db
    mongo_client, mongo_db = connect_mongodb(MONGO_CONNECTION, MONGO_DB)

    try:
        Insert_to_mongo()
    finally:
        mongo_client.close()

if __name__ == "__main__":
    busan_attraction()