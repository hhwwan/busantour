#대시보드용 변수
attraction =["해운대 해수욕장","부산 서면","BIFF광장 일원","동백섬 일원","센텀시티","자갈치_국제시장","광안리 해수욕장","마린시티",
"전포카페거리","민락수변공원","송정 해수욕장","해동 용궁사","렛츠런파크","송도 해수욕장","부산시민공원","달맞이고개",
"흰여울문화마을","태종대","오륙도_이기대 갈맷길","감천문화마을","임랑 해수욕장","다대포 해수욕장","일광 해수욕장","국립해양박물관",
"을숙도","황령산 봉수대","범어사","아미산 전망대","해리단길","청사포","낙동강생태공원","기장해변_대변항","오시리아","해운대시장","암남공원","죽성드림세트장","가덕도",
"부산민주공원","장림포구","장산","부산역_초량","수영사적공원_망미단길","동래온천","UN기념공원"] #다대포의 경우 Tmap 데이터에서는 layer열에서는 다대포해수욕장으로 작성되어 있다.

gugun =['강서구', '사하구', '사상구', '북구', '금정구', '부산진구', '동래구', '연제구', '수영구', '남구', '동구', '서구', '중구', '영도구', '해운대구', '기장군']

hangjeongdong =["중앙동", "동광동", "대청동", "보수동", "부평동", "남포동", "광복동", "영주1동", "영주2동", "동대신1동", 
"동대신2동", "동대신3동", "서대신1동", "서대신3동", "서대신4동", "부민동", "아미동", "초장동", "충무동", "남부민1동",
"암남동", "남부민2동", "남항동", "초량1동", "초량2동", "초량3동", "초량6동", "수정1동", "수정2동", "수정4동",
"수정5동","범일2동", "범일5동", "청학1동", "사직1동", "좌천동", "범일1동", "영선1동", "영선2동", "청학2동",
"동삼1동","동삼2동", "동삼3동", "신선동", "봉래2동", "부전2동", "연지동", "초읍동", "양정1동", "양정2동", 
"범천1동", "당감1동", "부전1동", "가야1동", "전포2동", "부암1동", "부암3동", "당감2동", "당감4동", "가야2동", 
"개금1동", "개금2동", "개금3동", "전포1동", "범천2동", "수민동", "복산동", "온천1동", "온천2동", "온천3동", 
"사직3동", "구포3동","사직2동", "안락1동", "안락2동", "명장1동", "명장2동", "명륜동", "대연3동", "대연4동", 
"대연5동", "덕천3동", "대연6동", "반여1동", "용호1동", "용호2동", "용호3동", "용호4동", "명지2동", "용당동", 
"감만1동", "감만2동", "문현1동", "문현2동", "문현3동", "문현4동", "대연1동", "우암동", "구포1동", "구포2동", 
"모라3동", "반여3동", "금곡동", "화명1동", "덕천1동", "덕천2동", "만덕1동", "만덕2동", "만덕3동", "화명2동", 
"화명3동", "중1동", "중2동", "송정동", "반여2동", "봉래1동", "반송2동", "재송1동", "재송2동", "좌1동", 
"좌2동", "좌3동", "좌4동", "반여4동", "반송1동", "우1동", "우2동", "우3동", "괴정1동", "괴정2동", 
"괴정3동", "괴정4동", "당리동", "하단1동", "하단2동", "신평1동", "신평2동", "장림1동", "장림2동", "다대1동", 
"다대2동", "구평동", "감천1동", "감천2동", "서1동", "서2동", "부곡1동", "부곡2동", "부곡3동", "부곡4동", 
"장전1동", "선두구동", "청룡노포동", "남산동", "구서1동", "구서2동", "금성동", "서3동", "금사회동동", "대저1동", 
"대저2동", "강동동", "가락동", "녹산동", "가덕도동", "명지1동", "거제1동", "거제2동", "거제3동", "거제4동", 
"연산1동", "연산2동", "연산3동", "연산4동", "연산5동", "연산6동", "연산8동", "연산9동", "남천1동", "남천2동", 
"수영동", "망미1동", "망미2동", "광안1동", "광안2동", "광안3동", "광안4동", "민락동", "삼락동", "모라1동", 
"덕포1동", "덕포2동", "괘법동", "주례1동", "주례2동", "주례3동", "학장동", "엄궁동", "감전동", "기장읍", 
"장안읍", "정관읍", "일광면", "철마면", "장전2동"]

# 날짜 변수활용
boxplot_timeseries_start_period = 202201

boxplot_timeseries_end_period = 202312

# 대시보드용 날짜 변수 
dashboard_current_year = 2023

dashboard_start_month = 1

dashboard_current_month = 12


#POI 날짜변수
poi_start_period = 202201

poi_end_period = 202403


#연령대 활용
ages = ["10대 이하","10대", "20대", "30대", "40대", "50대", "60대 이상", "60대 이상"]

#POI데이터에쓰는 변수들
month =["1월","12월"]

poi_ages = [ "20대", "30대", "40대", "50대", "60대", "70대"]

regions = ["서울", "대구", "부산", "제주", "대전", "충북", "강원", "경기", "경남", "경북", "전남", "전북", "인천", "광주", "울산", "세종", "전국", "부산제외"] 

gender =["남성","여성"]

time_interval = ["0시_2시", "3시_5시", "6시_8시", "9시_11시", "12시_14시", "15시_17시", "18시_20시", "21시_23시"]

# 박스플롯용 변수

boxplot_age=["10대이하","20대","30대","40대","50대","60대 이상"]

residence=["서울","인천","대구","대전","광주","울산","세종","경기","강원","충남","충북","전남","전북","경남","경북","제주","기타"]


main_categories =["교통","면세점","백화점","외식업기타","제과","양식","일식","중식","한식","모텔/여관/기타숙박","호텔/콘도","커피/음료"]


telecom_countries = [
    "스페인", "우즈베키스탄", "스위스", "스리랑카", "베트남", "폴란드", "우크라이나", 
    "싱가포르", "미얀마", "중국", "말레이시아", "인도네시아", "캄보디아", "카자흐스탄", 
    "일본", "노르웨이", "독일", "스웨덴", "필리핀", "대만", "뉴질랜드", "몽골", "그리스", 
    "방글라데시", "영국", "인도", "미국", "기타", "홍콩", "캐나다", "GCC", "태국", 
    "호주", "프랑스", "멕시코", "이탈리아", "덴마크", "터키", "네덜란드", "러시아"
]

card_data_countries=["ETC", "가나", "가봉", "과테말라", "괌", "구아나", "그라나다", "그리스", "기니아", "나미비아", 
"나이지리아", "남아프리카공화국", "네덜란드", "네팔", "노르웨이", "뉴질랜드", "뉴칼레도니아", "니카라과", "대만", "대한민국", 
"덴마크", "도미니카", "독일", "라오스", "라트비아", "러시아", "레바논", "레소토", "루마니아", "룩셈부르크", 
"르완다", "리투아니아", "리히텐슈타인", "마다가스카", "마카오", "마케도니아", "말라위", "말레이지아", "말타", "멕시코",
 "모로코", "모리셔스", "모잠비크", "몬테네그로", "몰도바", "몰디브", "몽고", "미국", "미얀마", "바누아투",
 "바레인", "바베이도스", "바하마", "방글라데시", "버뮤다", "베네수엘라", "베닌", "베트남", "벨기에", "벨라루스",
 "벨리즈", "보스니아헤르체고비나", "볼리비아", "부룬디", "부르키나파소", "부탄", "불가리아", "브라질", "브루나이드루살렘", "사우디아라비아", 
"세네갈", "세르비아", "세이셸", "세인트루시아", "세인트빈센트그레나딘", "세인트키츠네비스", "수리남", "스리랑카", "스웨덴", "스위스",
 "스페인", "슬로바키아", "슬로베니아", "싱가폴", "싸이프러스", "아랍에미레이트", "아루바", "아르메니아", "아르헨티나", "아메리칸사모아", 
"아이슬랜드", "아일랜드", "아제르바이잔", "안도라", "알바니아", "알제리", "앙골라", "에디오피아", "에스토니아", "에콰도르",
 "엔티가바부다", "엘살바도르", "영국", "오만", "오스트레일리아", "오스트리아", "온두라스", "요르단", "우간다", "우루과이",
 "우즈베키스탄", "우크라이나", "이라크", "이스라엘", "이집트", "이탈리아", "인도", "인도네시아", "일본", "자메이카", 
"잠비아", "적도기니아", "조지아", "중국", "지부티", "지브랄타", "짐바브웨", "챠드", "체코", "칠레",
 "카메룬", "카자흐스탄", "카타르", "캄보디아", "캐나다", "케냐", "케이만아일랜드", "코스타리카", "코트디부아르", "콜롬비아",
 "콩고", "쿠웨이트", "쿡아일랜드", "퀴라소", "크로아티아", "키르기스스탄", "키리바시", "타지키스탄", "탄자니아", "태국",
 "터키", "턱스카이코스제도", "토고", "통가", "투르크메니스탄", "튀니지아", "트리니나드토바고", "파나마", "파라과이", "파키스탄",
 "파푸아뉴기니", "팔레스타인자치지구", "페루", "포르투칼", "폴란드", "푸에르토리코", "프랑스", "피지", "핀란드", "필리핀",
 "헝가리", "홍콩"]