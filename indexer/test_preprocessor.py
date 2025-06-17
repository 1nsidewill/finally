#!/usr/bin/env python3
# test_preprocessor.py - 텍스트 전처리 모듈 테스트

from src.services.text_preprocessor import ProductTextPreprocessor, preprocess_product

def test_preprocessor():
    processor = ProductTextPreprocessor()
    
    # 테스트 매물 데이터
    test_data = {
        'title': '야마하 R3 2019년형 판매합니다',
        'price': 5500000,  # 550만원
        'year': 2019,
        'mileage': 15000,  # 15,000km
        'content': '상태 양호, 사고무, 정기점검 완료. 성인 1인 라이더만 타던 차량입니다.',
        'brand': '',  # 빈 값으로 테스트 (제목에서 추출하는지 확인)
        'model': ''
    }
    
    print("=== 텍스트 전처리 테스트 ===")
    print(f"원본 데이터:")
    for key, value in test_data.items():
        print(f"  {key}: {value}")
    
    print("\n=== 개별 정규화 테스트 ===")
    print(f"가격 정규화: {processor.normalize_price(test_data['price'])}")
    print(f"연식 정규화: {processor.normalize_year(test_data['year'])}")
    print(f"주행거리 정규화: {processor.normalize_mileage(test_data['mileage'])}")
    
    brand_model = processor.extract_model_and_brand(test_data['title'])
    print(f"브랜드/모델 추출: {brand_model}")
    
    print("\n=== 전체 전처리 결과 ===")
    result = processor.preprocess_product_data(test_data)
    print(f"결과: {result}")
    print(f"길이: {len(result)}자")
    
    # 편의 함수 테스트
    print("\n=== 편의 함수 테스트 ===")
    convenience_result = preprocess_product(test_data)
    print(f"편의 함수 결과: {convenience_result}")
    print(f"결과 일치: {result == convenience_result}")
    
    # 다양한 케이스 테스트
    print("\n=== 다양한 케이스 테스트 ===")
    
    test_cases = [
        {
            'title': 'Honda CBR600RR 판매',
            'price': '780만원',  # 문자열 가격
            'year': '2020년',   # 문자열 연식
            'mileage': '8,500km',  # 문자열 주행거리
            'content': '수입차, 튜닝 많이 했습니다.'
        },
        {
            'title': '가와사키 닌자 zx-6r',
            'price': 12000000,  # 1200만원
            'year': 2018,
            'mileage': 25000,
            'content': ''
        },
        {
            'title': 'Ducati 파니갈레 V4',
            'price': 25000000,  # 2500만원
            'year': 2021,
            'mileage': 3000,
            'content': '거의 새 차, 차고 보관'
        }
    ]
    
    for i, case in enumerate(test_cases, 1):
        print(f"\n케이스 {i}:")
        result = preprocess_product(case)
        print(f"  결과: {result}")

if __name__ == "__main__":
    test_preprocessor()