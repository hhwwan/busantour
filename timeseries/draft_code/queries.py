# ID에 따라 쿼리 선택
def get_query(update_type, id_value):
    
    if id_value == "AOT-1-1":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."방문객_내국인_일별_성별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."방문객_내국인_주별_성별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."방문객_내국인_월별_성별" """
        
    elif id_value == "AOT-1-2":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."방문객_내국인_일별_연령별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."방문객_내국인_주별_연령별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."방문객_내국인_월별_연령별" """
        
    elif id_value == "AOT-1-3":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."방문객_내국인_일별_거주지별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."방문객_내국인_주별_거주지별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."방문객_내국인_월별_거주지별" """
        
    elif id_value == "AOT-2-1":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_일별_성별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_주별_성별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_월별_성별" """

    elif id_value == "AOT-2-2":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_일별_연령별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_주별_연령별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_월별_연령별" """

    elif id_value == "AOT-2-3":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_일별_거주지별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_주별_거주지별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_월별_거주지별" """

    elif id_value == "AOT-2-4":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_일별_업종별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_주별_업종별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_내국인_월별_업종별" """    
        
    elif id_value == "AFT-1-1":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."방문객_외국인_일별_국적별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."방문객_외국인_주별_국적별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."방문객_외국인_월별_국적별" """
       
    elif id_value == "AFT-2-1":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_외국인_일별_국적별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_외국인_주별_국적별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_외국인_월별_국적별" """
        
    elif id_value == "AFT-2-2":
        if update_type == "daily":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_외국인_일별_업종별" """
        elif update_type == "weekly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_외국인_주별_업종별" """
        elif update_type == "monthly":
            return """ SELECT * FROM "1.부산시전역"."신용카드지출액_외국인_월별_업종별" """
    else:
        print("Invalid ID value.")
        return None