# src/services/test_data_loader.py
import asyncio
import asyncpg
import re
from typing import List, Dict, Any
from src.config import get_settings
from src.services.qdrant_service import qdrant_service
from src.api.schema import DocumentCreate, DocumentMetadata

config = get_settings()

def extract_model_name_from_title(title: str) -> str:
    """제목에서 R3 모델명 추출 및 정규화 (테스트용)"""
    # R3 관련 다양한 표기법들
    r3_patterns = [
        # 영문 표기
        r'\bR-?3\b',  # R3, R-3
        r'\bYZF-?R-?3\b',  # YZF-R3, YZFR3, YZF-R-3
        
        # 한글 표기
        r'\b알삼\b',  # 알삼
        r'\b알쓰리\b',  # 알쓰리
        r'\b알스리\b',  # 알스리 (오타)
        r'\b알3\b',  # 알3
        
        # 브랜드와 함께 (대소문자 무시)
        r'\b야마하\s*[rR]-?3\b',  # 야마하 R3, 야마하 r3
        r'\b야마하\s*알삼\b',  # 야마하 알삼
        r'\b야마하\s*알쓰리\b',  # 야마하 알쓰리
        r'\b야마하\s*알스리\b',  # 야마하 알스리
        
        # 영문 브랜드와 함께 (오타 포함)
        r'\byamaha\s*[rR]-?3\b',  # yamaha R3, yamaha r3
        r'\byamha\s*[rR]-?3\b',  # yamha R3 (오타)
        r'\byamaha\s*알삼\b',  # yamaha 알삼
        r'\byamaha\s*알쓰리\b',  # yamaha 알쓰리
        r'\byamha\s*알삼\b',  # yamha 알삼 (오타)
        
        # 기타 변형
        r'\bR\s*3\b',  # R 3 (공백 포함)
        r'\b알\s*3\b',  # 알 3
    ]
    
    # 모든 패턴 검사
    for pattern in r3_patterns:
        if re.search(pattern, title, re.IGNORECASE):
            return "YZF-R3"  # 모든 R3 변형을 YZF-R3로 정규화
    
    return None

def extract_brand_from_title(title: str) -> str:
    """제목에서 야마하 브랜드 추출 및 정규화 (테스트용)"""
    yamaha_patterns = [
        r'\b야마하\b',
        r'\bYAMAHA\b',
        r'\byamaha\b',
        r'\bYamaha\b',
        r'\byamha\b',  # 오타
        r'\bYamha\b',  # 오타
    ]
    
    for pattern in yamaha_patterns:
        if re.search(pattern, title, re.IGNORECASE):
            return "YAMAHA"  # 모든 야마하 변형을 YAMAHA로 정규화
    
    return None

def extract_year_from_title(title: str) -> int:
    """제목에서 연식 추출 (2000-2024년 범위)"""
    year_match = re.search(r'\b(20[0-2][0-9])\b', title)
    if year_match:
        year = int(year_match.group(1))
        if 2000 <= year <= 2024:
            return year
    return None

def extract_category_from_title(title: str) -> str:
    """R3 모델의 카테고리 설정 (테스트용)"""
    # R3가 감지되면 자동으로 스포츠 카테고리 할당
    if extract_model_name_from_title(title):
        return "sports"
    return None

def convert_int_to_uuid_format(int_id: int) -> str:
    """정수 ID를 UUID 형식으로 변환 (재현 가능)"""
    # 정수를 16진수로 변환하고 32자리로 패딩
    hex_str = f"{int_id:032x}"
    
    # UUID 형식으로 변환: 8-4-4-4-12
    uuid_formatted = f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:32]}"
    
    return uuid_formatted

async def load_test_data_from_postgres():
    """PostgreSQL에서 테스트 데이터를 가져와 Qdrant에 로드하는 함수"""
    # PostgreSQL 연결
    conn = await asyncpg.connect(
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        database=config.DB_NAME,
        host=config.DB_HOST,
        port=config.DB_PORT
    )
    
    try:
        # R3 관련 다양한 표기가 포함된 제품들 가져오기
        products = await conn.fetch(
            """
            SELECT uid, title, content, price, created_dt, updated_dt 
            FROM product 
            WHERE (
                title ILIKE '%R3%' OR 
                title ILIKE '%알삼%' OR 
                title ILIKE '%알쓰리%' OR 
                title ILIKE '%알스리%' OR
                title ILIKE '%야마하%R3%' OR
                title ILIKE '%yamaha%R3%' OR
                title ILIKE '%yamha%R3%'
            )
            LIMIT 1000
            """
        )
        
        print(f"PostgreSQL에서 {len(products)} 개의 R3 관련 제품 데이터를 가져왔습니다.")
        
        # Qdrant에 삽입할 문서 준비
        documents = []
        r3_count = 0
        
        for product in products:
            # 제목에서 추가 정보 추출
            model_name = extract_model_name_from_title(product['title'])
            
            # R3가 아닌 경우 스킵
            if not model_name:
                continue
                
            r3_count += 1
            brand = extract_brand_from_title(product['title'])
            year = extract_year_from_title(product['title'])
            category = extract_category_from_title(product['title'])
            
            # 정수 UID를 UUID 형식으로 변환
            original_uid = product['uid']
            uuid_formatted = convert_int_to_uuid_format(original_uid)
            
            # 메타데이터 설정
            metadata = DocumentMetadata(
                id=uuid_formatted,  # UUID 형식으로 변환된 ID
                content=product['content'],
                price=product['price'],
                model_name=model_name,
                brand=brand or "YAMAHA",
                category=category,
                year=year,
                last_modified_at=product['updated_dt'].isoformat() if product['updated_dt'] else None,
                extra={
                    "source": "postgres_test_r3",
                    "created_dt": product['created_dt'].isoformat() if product['created_dt'] else None,
                    "original_uid": original_uid,  # 원본 정수 UID 보관
                    "uuid_formatted": uuid_formatted,  # UUID 형식 매핑
                    "original_title": product['title']
                }
            )
            
            # 문서 생성
            document = DocumentCreate(
                title=product['title'],
                metadata=metadata,
                id=uuid_formatted  # UUID 형식 ID 사용
            )
            
            documents.append(document)
        
        print(f"총 {r3_count}개의 R3 제품이 인식되었습니다.")
        
        # 배치 단위로 Qdrant에 삽입
        batch_size = 50
        total_inserted = 0
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            doc_ids = await qdrant_service.batch_insert_documents(batch)
            total_inserted += len(doc_ids)
            print(f"R3 배치 {i//batch_size + 1} 완료: {len(doc_ids)}개 삽입됨")
            
            # 각 문서의 정규화 결과 출력 (처음 5개만)
            if i == 0:
                for idx, doc in enumerate(batch[:5]):
                    print(f"  예시 {idx+1}: '{doc.title}'")
                    print(f"    원본 UID: {doc.metadata.extra['original_uid']}")
                    print(f"    UUID 형식: {doc.metadata.id}")
                    print(f"    모델: {doc.metadata.model_name}, 브랜드: {doc.metadata.brand}")
        
        print(f"총 {total_inserted}개의 R3 제품 데이터가 Qdrant에 삽입되었습니다.")
        return total_inserted
    
    finally:
        # 연결 종료
        await conn.close()

# 테스트를 위한 비동기 실행 헬퍼 함수
async def run_test_data_loading():
    return await load_test_data_from_postgres()