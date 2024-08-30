def get_existing_metadata(conn, objectid):# 공통 로직
    """ 특정 objectid에 해당하는 메타데이터가 있는지 확인합니다. """
    query = """
    SELECT * FROM metadata.object WHERE objectid = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (objectid,))
            result = cur.fetchone()
            return result if result else None
    except Exception as e:
        print(f"기존 메타데이터를 가져오는 동안 오류가 발생했습니다: {e}")
        conn.rollback()
        return None

def get_objecttype_id(conn, objecttype_name):
    query = "SELECT objecttype FROM metadata.objecttype WHERE objecttypename = %s"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (objecttype_name,))
            result = cur.fetchone()
            return result[0] if result else ''
    except Exception as e:
        print(f"ObjectType ID를 가져오는 동안 오류가 발생했습니다: {e}")
        conn.rollback()
        return ''

def get_spatiallevel_id(conn, spatiallevel_name):
    query = "SELECT spatiallevel FROM metadata.spatiallevel WHERE spatiallevelname = %s"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (spatiallevel_name,))
            result = cur.fetchone()
            return result[0] if result else ''
    except Exception as e:
        print(f"SpatialLevel ID를 가져오는 동안 오류가 발생했습니다: {e}")
        conn.rollback()
        return ''

def get_dimension_ids(conn, dimension_names):
    query = "SELECT dimensionid FROM metadata.dimension WHERE dimensionname = %s"
    try:
        with conn.cursor() as cur:
            dimension_ids = []
            if dimension_names:  # dimension_names가 None이 아닌지 확인
                for name in dimension_names.split(','):
                    cur.execute(query, (name.strip(),))
                    result = cur.fetchone()
                    if result:
                        dimension_ids.append(result[0])
            return dimension_ids
    except Exception as e:
        print(f"Dimension ID를 가져오는 동안 오류가 발생했습니다: {e}")
        conn.rollback()
        return []

def get_kpi_ids(conn, kpi_names):
    query = "SELECT kpiid FROM metadata.kpi WHERE kpiname = %s"
    try:
        with conn.cursor() as cur:
            kpi_ids = []
            if kpi_names:  # kpi_names가 None이 아닌지 확인
                for name in kpi_names.split(','):
                    cur.execute(query, (name.strip(),))
                    result = cur.fetchone()
                    if result:
                        kpi_ids.append(result[0])
            return kpi_ids
    except Exception as e:
        print(f"KPI ID를 가져오는 동안 오류가 발생했습니다: {e}")
        conn.rollback()
        return []
    
def get_nationality_id(conn, nationality_name):
    query = "SELECT nationality FROM metadata.nationality WHERE nationalityname = %s"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (nationality_name,))
            result = cur.fetchone()
            return result[0] if result else ''
    except Exception as e:
        print(f"Nationality ID를 가져오는 동안 오류가 발생했습니다: {e}")
        conn.rollback()
        return ''


def get_space_id(conn, space):
    query = "SELECT olapspaceid FROM code.olapspace WHERE space = %s"
    try:
        with conn.cursor() as cur:
            space_ids = []
            if space:  # kpi_names가 None이 아닌지 확인
                for name in space.split(','):
                    cur.execute(query, (name.strip(),))
                    result = cur.fetchone()
                    if result:
                        space_ids.append(result[0])
            return space_ids
    except Exception as e:
        print(f"space ID를 가져오는 동안 오류가 발생했습니다: {e}")
        conn.rollback()
        return []
    

def get_flow_id(conn, flow_name):
    query = "SELECT flowid FROM code.flow WHERE flowname = %s"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (flow_name,))
            result = cur.fetchone()
            return result[0] if result else ''
    except Exception as e:
        print(f"Flow ID를 가져오는 동안 오류가 발생했습니다: {e}")
        conn.rollback()
        return ''
    