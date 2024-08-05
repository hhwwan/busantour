from transformation_all import busan_all # transformation_all 파일에서 함수 불러오기
from transformation_gugun import busan_gugun # transformation_gugun 파일에서 함수 불러오기
from transformation_adong import busan_adong # transformation_adong 파일에서 함수 불러오기
from transformation_attraction import busan_attraction # transformation_attraction 파일에서 함수 불러오기
from metadata_record import metadata_record # metadata_record 파일에서 함수 불러오기

def main():
    # MongoDB에 부산시전역 삽입
    busan_all()

    # MongoDB에 구군 삽입
    busan_gugun()
    
    # MongoDB에 행정동 삽입
    busan_adong()

    # MongoDB에 관광지 삽입
    busan_attraction()

    # metadata 기록하기
    metadata_record() 

if __name__ == "__main__":
    main()