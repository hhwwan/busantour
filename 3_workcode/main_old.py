from transformation_province import busan_province
from transformation_gugun import busan_gugun
from transformation_hangjeongdong import busan_hangjeongdong
from transformation_attraction import busan_attraction
from metadata_record import metadata_record

def main(collection_name) :
    busan_province(collection_name)

    # busan_gugun(collection_name)

    # busan_hangjeongdong(collection_name)

    # busan_attraction(collection_name)

    # metadata_record(collection_name)

if __name__ == "__main__":
    collection_name = "test" # 돌릴때 마다 바꿔야함

    main(collection_name)