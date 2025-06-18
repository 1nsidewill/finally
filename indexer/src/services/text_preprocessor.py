# src/services/text_preprocessor.py
import re
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ProductTextPreprocessor:
    """매물 데이터를 임베딩에 적합한 텍스트로 전처리하는 클래스"""
    
    def __init__(self):
        # 불필요한 특수문자 및 패턴 정규화용 패턴들
        self.patterns = {
            # 연속된 공백을 하나로 줄이기
            'multiple_spaces': re.compile(r'\s+'),
            # 특수문자 정리 (필수 정보는 유지)
            'special_chars': re.compile(r'[^\w\s가-힣.,!?()-]'),
            # 가격 정규화 (만원, 천만원 등 단위 통일)
            'price_units': re.compile(r'([0-9,]+)\s*(만원|천만원|억)'),
            # 키로수 정규화 
            'km_units': re.compile(r'([0-9,]+)\s*(km|키로|키로미터|킬로)'),
            # 연식 정규화
            'year_pattern': re.compile(r'(19|20)\d{2}'),
        }
    
    def clean_text(self, text: str) -> str:
        """기본 텍스트 정리"""
        if not text:
            return ""
        
        # 앞뒤 공백 제거
        text = text.strip()
        
        # 특수문자 정리 (일부 유지)
        text = self.patterns['special_chars'].sub(' ', text)
        
        # 연속된 공백을 하나로
        text = self.patterns['multiple_spaces'].sub(' ', text)
        
        return text
    
    def normalize_price(self, price: Any) -> str:
        """가격 정보 정규화"""
        if not price:
            return ""
        
        try:
            # 숫자 형태로 받은 경우
            if isinstance(price, (int, float)):
                if price >= 100000000:  # 1억 이상
                    return f"{price // 100000000}억{(price % 100000000) // 10000}만원"
                elif price >= 10000:  # 1만원 이상  
                    return f"{price // 10000}만원"
                else:
                    return f"{price}원"
            
            # 문자열 형태로 받은 경우
            price_str = str(price).replace(',', '')
            
            # 이미 단위가 있는 경우 그대로 반환
            if any(unit in price_str for unit in ['만원', '억', '천만원']):
                return self.clean_text(price_str)
            
            # 숫자만 있는 경우 변환
            if price_str.isdigit():
                price_num = int(price_str)
                return self.normalize_price(price_num)
            
            return self.clean_text(price_str)
            
        except Exception as e:
            logger.warning(f"가격 정규화 실패: {price} - {e}")
            return str(price) if price else ""
    
    def normalize_odo(self, odo: Any) -> str:
        """주행거리(ODO) 정보 정규화"""
        if not odo:
            return ""
        
        try:
            # 숫자 형태로 받은 경우
            if isinstance(odo, (int, float)):
                return f"{odo:,}km"
            
            # 문자열 형태로 받은 경우
            odo_str = str(odo).replace(',', '')
            
            # 이미 단위가 있는 경우
            km_match = self.patterns['km_units'].search(odo_str)
            if km_match:
                number = km_match.group(1).replace(',', '')
                if number.isdigit():
                    return f"{int(number):,}km"
            
            # 숫자만 있는 경우
            if odo_str.isdigit():
                return f"{int(odo_str):,}km"
            
            return self.clean_text(odo_str)
            
        except Exception as e:
            logger.warning(f"주행거리 정규화 실패: {odo} - {e}")
            return str(odo) if odo else ""
    
    def normalize_year(self, year: Any) -> str:
        """연식 정보 정규화"""
        if not year:
            return ""
        
        try:
            # 숫자 형태로 받은 경우
            if isinstance(year, (int, float)):
                year_int = int(year)
                if 1900 <= year_int <= 2030:
                    return f"{year_int}년"
                return str(year_int)
            
            # 문자열 형태로 받은 경우
            year_str = str(year)
            year_match = self.patterns['year_pattern'].search(year_str)
            if year_match:
                return f"{year_match.group()}년"
            
            return self.clean_text(year_str)
            
        except Exception as e:
            logger.warning(f"연식 정규화 실패: {year} - {e}")
            return str(year) if year else ""
    
    def extract_model_and_brand(self, title: str) -> Dict[str, str]:
        """제목에서 브랜드와 모델명 추출"""
        title = title.lower() if title else ""
        
        # 주요 브랜드 패턴들
        brand_patterns = {
            'yamaha': r'\b(야마하|yamaha|yamha)\b',
            'honda': r'\b(혼다|honda)\b', 
            'kawasaki': r'\b(가와사키|kawasaki|카와사키)\b',
            'suzuki': r'\b(스즈키|suzuki)\b',
            'ducati': r'\b(두카티|ducati)\b',
            'bmw': r'\b(bmw|비엠더블유)\b'
        }
        
        # 모델 패턴들 (예시로 일부만)
        model_patterns = {
            'r3': r'\b(r-?3|알삼|알쓰리|yzf-?r-?3)\b',
            'r6': r'\b(r-?6|알식|yzf-?r-?6)\b',
            'cbr': r'\b(cbr)\b',
            'ninja': r'\b(ninja|닌자)\b'
        }
        
        detected_brand = ""
        detected_model = ""
        
        # 브랜드 감지
        for brand, pattern in brand_patterns.items():
            if re.search(pattern, title, re.IGNORECASE):
                detected_brand = brand.upper()
                break
        
        # 모델 감지  
        for model, pattern in model_patterns.items():
            if re.search(pattern, title, re.IGNORECASE):
                detected_model = model.upper()
                break
        
        return {
            'brand': detected_brand,
            'model': detected_model
        }
    
    def preprocess_product_data(self, product_data: Dict[str, Any]) -> str:
        """
        매물 데이터를 임베딩에 적합한 하나의 텍스트로 전처리
        
        Args:
            product_data: 매물 데이터 딕셔너리
                - title: 제목
                - price: 가격  
                - year: 연식
                - odo: 주행거리 (mileage는 legacy)
                - content: 본문 내용
                - brand: 브랜드 (선택)
                - model: 모델명 (선택)
        
        Returns:
            전처리된 임베딩용 텍스트
        """
        try:
            # 기본 데이터 추출
            title = product_data.get('title', '')
            price = product_data.get('price')
            year = product_data.get('year')
            odo = product_data.get('odo') or product_data.get('mileage')  # odo 우선, mileage는 fallback
            content = product_data.get('content', '')
            
            # 브랜드/모델 정보 (있으면 사용, 없으면 제목에서 추출)
            brand = product_data.get('brand', '')
            model = product_data.get('model', '')
            
            if not brand or not model:
                extracted = self.extract_model_and_brand(title)
                brand = brand or extracted['brand']
                model = model or extracted['model']
            
            # 각 정보 정규화
            clean_title = self.clean_text(title)
            normalized_price = self.normalize_price(price)
            normalized_year = self.normalize_year(year)
            normalized_odo = self.normalize_odo(odo)  # normalize_odo로 변경
            clean_content = self.clean_text(content)
            
            # 구조화된 텍스트 생성
            text_parts = []
            
            # 1. 핵심 매물 정보 (제목 + 브랜드/모델)
            if brand and model:
                text_parts.append(f"[{brand} {model}] {clean_title}")
            else:
                text_parts.append(clean_title)
            
            # 2. 핵심 스펙 정보
            specs = []
            if normalized_year:
                specs.append(normalized_year)
            if normalized_price:
                specs.append(normalized_price)
            if normalized_odo:
                specs.append(normalized_odo)  # normalized_odo로 변경
            
            if specs:
                text_parts.append(f"스펙: {' | '.join(specs)}")
            
            # 3. 상세 설명
            if clean_content:
                text_parts.append(f"상세: {clean_content}")
            
            # 최종 텍스트 결합
            final_text = ' '.join(text_parts)
            
            # 최종 정리
            final_text = self.patterns['multiple_spaces'].sub(' ', final_text).strip()
            
            logger.debug(f"전처리 완료: {len(final_text)}자")
            return final_text
            
        except Exception as e:
            logger.error(f"전처리 실패: {e}")
            # 실패 시 기본 정보라도 반환
            return self.clean_text(product_data.get('title', '') + ' ' + 
                                 str(product_data.get('content', '')))

# 전역 인스턴스
text_preprocessor = ProductTextPreprocessor()

# 편의 함수
def preprocess_product(product_data: Dict[str, Any]) -> str:
    """매물 데이터 전처리 편의 함수"""
    return text_preprocessor.preprocess_product_data(product_data)