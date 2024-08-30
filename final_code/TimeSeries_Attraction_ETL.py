import pickle
import os
import shutil
from pymongo import MongoClient
from config import connect_postgresql, connect_mongodb, MONGO_CONNECTION, MONGO_DB
from TimeSeries_util import *

def extract_task():
    # PostgreSQL 연결 설정
    connection = connect_postgresql()
    cursor = connection.cursor()

    # 해당하는 viewName만 가져옵니다.
    cursor.execute("""
    SELECT objectid, viewname, kpi, nationality, dimension, objecttype, startperiod, endperiod, spatiallevel
    FROM metadata.extractdata
    WHERE objecttype = 'timeseries'""")
    objects = cursor.fetchall()  # 모든 결과를 가져옵니다.
    cursor.close()
    connection.close()

    # '관광지'인 경우만 필터링
    filtered_objects = [obj for obj in objects if obj[8] == '관광지']

    return filtered_objects

# MongoDB에 데이터 저장
def transform_task(ti):
    data_info = ti.xcom_pull(task_ids='extract_task')
    transformed_data_paths = []  # pickle 파일 경로를 저장할 리스트

    # pickle 파일을 저장할 새로운 폴더 생성
    pickle_folder = "TimeSeries_Attraction_pickle_files"  # 폴더 이름
    os.makedirs(pickle_folder, exist_ok=True)  # 폴더가 없으면 생성

    for obj in data_info:
        objectid, view_name, kpi, nationality, dimension, objecttype, startperiod, endperiod, spatiallevel = obj

        attractions_data = {}

        # 데이터 받아와서 일별, 주별, 월별에 맞게 변환
        for update_type in ["daily", "weekly", "monthly"]:
            data = fetch_data(update_type, objectid, view_name)
            
            if data is not None and not data.empty:
                attractions = data["관광지"].unique()
                for attraction in attractions:
                    if attraction not in attractions_data:
                        attractions_data[attraction] = {"daily": None, "weekly": None, "monthly": None}
                    attraction_data = data[data["관광지"] == attraction]
                    attractions_data[attraction][update_type] = attraction_data

        # 관광지별로 pickle 파일로 변환 및 저장
        for attraction, attraction_data in attractions_data.items():
            unique_id = generate_unique_id(objectid, spatiallevel, kpi, nationality, dimension, attraction_name=attraction)  # Unique ID 생성
            for update_type, data in attraction_data.items():
                if data is not None:

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

                    # 변환된 데이터를 pickle 파일로 저장
                    transformed_data = {
                        'unique_id': unique_id,
                        'axis_data': axis_data,
                        'formatted_records': formatted_records,
                        'update_type': update_type,
                        'view_name': view_name,
                        'objecttype': objecttype,
                        'spatiallevel': spatiallevel,
                        'kpi': kpi,
                        'nationality': nationality,
                        'dimension': dimension,
                        'attraction': attraction
                    }

                    # pickle 파일 경로 생성 및 저장
                    pickle_filepath = os.path.join(pickle_folder, f"transformed_data_{unique_id}_{update_type}.pkl")
                    with open(pickle_filepath, 'wb') as f:
                        pickle.dump(transformed_data, f)

                    # pickle 파일 경로 추가
                    transformed_data_paths.append(pickle_filepath)

    return transformed_data_paths

def load_task(ti, collection_name):
    transformed_data_paths = ti.xcom_pull(task_ids='transform_task')
    mongo_client = MongoClient(MONGO_CONNECTION)
    mongo_db = connect_mongodb(MONGO_CONNECTION, MONGO_DB)

    # pickle 파일이 저장된 폴더 경로
    pickle_folder = "TimeSeries_Attraction_pickle_files"

    try:
        for pickle_filepath in transformed_data_paths:
            # pickle 파일 열기
            with open(pickle_filepath, 'rb') as f:
                data = pickle.load(f)

            unique_id = data['unique_id']
            axis_data = data['axis_data']
            formatted_records = data['formatted_records']
            update_type = data['update_type']
            view_name = data['view_name']
            objecttype = data['objecttype']
            spatiallevel = data['spatiallevel']
            kpi = data['kpi']
            nationality = data['nationality']
            dimension = data['dimension']
            attraction = data['attraction']

            # MongoDB 연결 설정
            collection = mongo_db[collection_name]

            # 축 데이터 키 설정
            if update_type == "daily":
                axis_key = "일별"
            elif update_type == "weekly":
                axis_key = "주별"
            elif update_type == "monthly":
                axis_key = "월별"
            else:
                print("Invalid update type.")
                return

            # 기존 문서 업데이트 또는 생성
            existing_doc = collection.find_one({'_id': unique_id})

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
                    "_id": unique_id,
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
            collection.replace_one({'_id': unique_id}, ordered_doc, upsert=True)

            attraction_code = get_code_mapping("attraction").get(attraction)

            # MongoDB안의 'meta'에 기록
            record_mongo(mongo_db, collection_name, unique_id, view_name, objecttype, spatiallevel, kpi, nationality, dimension, gugun_code=None, adong_code=None, attraction_code=attraction_code)

    finally:
        mongo_client.close()

        # 모든 작업이 완료된 후 pickle 파일이 저장된 폴더 삭제
        if os.path.exists(pickle_folder):  # 폴더가 존재하는지 확인
            shutil.rmtree(pickle_folder)  # 폴더와 그 안의 모든 파일 삭제

# Metadata 기록
def resister_task(ti, collection_name):
    mongo_client = MongoClient(MONGO_CONNECTION)
    mongo_db = connect_mongodb(MONGO_CONNECTION, MONGO_DB)
    mongo_collection = mongo_db[collection_name]

    # PostgreSQL 연결 설정
    conn = connect_postgresql()

    # MongoDB에서 모든 문서를 가져옴
    documents = list(mongo_collection.find({}))

    try:
        for document in documents:
            if 'meta' in document:
                object_objectlog(conn, document, collection_name)
                objectfile(conn, document, collection_name)
                kpi(conn, document, collection_name)
                dimension(conn, document, collection_name)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()
        mongo_client.close()