import re

# router.py 파일 읽기
with open('src/api/router.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Decimal 타입을 float으로 변환하는 패턴 교체
patterns = [
    (r'"price": product\[\'price\'\] or 0,', '"price": float(product[\'price\']) if product[\'price\'] else 0.0,'),
    (r'"odo": product\[\'odo\'\] or 0,', '"odo": float(product[\'odo\']) if product[\'odo\'] else 0.0,'),
    (r'"year": product\[\'year\'\] or 0', '"year": int(product[\'year\']) if product[\'year\'] else 0')
]

# 패턴 적용
for old_pattern, new_pattern in patterns:
    content = re.sub(old_pattern, new_pattern, content)

# 파일에 다시 쓰기
with open('src/api/router.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ payload 타입 변환 완료") 