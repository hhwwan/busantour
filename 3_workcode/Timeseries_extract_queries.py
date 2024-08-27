from config import connect_postgresql

def get_viewname_from_metadata():
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