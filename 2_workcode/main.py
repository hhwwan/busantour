from transformation_all import busan_all
from transformation_gugun import busan_gugun
from transformation_adong import busan_adong
from transformation_attraction import busan_attraction
from metadata_record import metadata_record

def main() :
    busan_all()

    busan_gugun()

    busan_adong()

    busan_attraction()

    metadata_record()

if __name__ == "__main__":
    main()