import json

# 병합할 파일 목록 설정
file_paths = [
    'dentex_dataset/origin/quadrant_enumeration_disease/train_quadrant_enumeration_disease.json',
    'formatted_json/combine_formatted_augmentation.json'
]

# 병합할 데이터를 담을 초기 딕셔너리
merged_data = {
    "images": [],
    "annotations": []
}

# 각 파일을 순차적으로 읽어 병합
for file_path in file_paths:
    with open(file_path, 'r') as f:
        data = json.load(f)
        
        # images와 annotations는 단순 병합
        merged_data["images"] += data["images"]
        merged_data["annotations"] += data["annotations"]

# 병합된 데이터를 JSON 파일로 저장
output_path = 'dentex_dataset/origin/quadrant_enumeration_disease/merge_quadrant_enumeration_disease.json'
with open(output_path, 'w') as outfile:
    json.dump(merged_data, outfile, indent=4)

print(f"병합된 JSON 파일이 {output_path}에 저장되었습니다.")
