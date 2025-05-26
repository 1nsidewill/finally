import re

def parse_korean_number(text: str) -> int:
    def clean_input(text):
        return re.sub(r'[^0-9억만천백십]', '', str(text))

    def promote_to_higher_unit(values: list, higher_unit: list):
        last_increase_idx = -1
        for i in range(len(values) - 1):
            if values[i+1] > values[i]:
                last_increase_idx = i + 1
        if last_increase_idx >= 0:
            higher_unit.extend(values[:last_increase_idx])
            del values[:last_increase_idx]

    def parse_small_units(text):
        unit_map = {'천': 1000, '백': 100, '십': 10}
        parts = re.findall(r'(\d+)?(천|백|십)', text)
        result = []
        for num, unit in parts:
            value = int(num) if num else 1
            result.append(value * unit_map[unit])
        text = re.sub(r'(\d+)?(천|백|십)', '', text)
        if text.isdigit():
            result.append(int(text))
        return result

    # 1. 클린징
    cleaned = clean_input(text)

    # 2. 억/만 단위 분리 및 파싱
    if '억' in cleaned:
        list1 = cleaned.rsplit('억', 1)
        uk_section = list1[0]
        after_uk = list1[1]
    else:
        uk_section = ''
        after_uk = cleaned

    uk_section = uk_section.replace('억', '')
    has_uk = parse_small_units(uk_section) if uk_section else []

    if '만' in after_uk:
        list2 = after_uk.rsplit('만', 1)
        man_section = list2[0]
        after_man = list2[1]
        man_section = man_section.replace('만', '')
        has_man = parse_small_units(man_section) if man_section else []
        has_il = parse_small_units(after_man) if after_man else []
    else:
        has_man = []
        has_il = parse_small_units(after_uk) if after_uk else []

    # 3. 단위 끌어올리기
    promote_to_higher_unit(has_il, has_man)
    promote_to_higher_unit(has_man, has_uk)
    has_jo = []
    promote_to_higher_unit(has_uk, has_jo)

    # 4. 합산
    total = 0
    if has_jo:
        total += sum(has_jo) * 1_0000 * 100_000_000  # 1조 = 1만 * 1억
    if has_uk:
        total += sum(has_uk) * 100_000_000
    if has_man:
        total += sum(has_man) * 10_000
    if has_il:
        total += sum(has_il)

    # 5. 4자리 이하일 경우 10,000 곱하기 (금액 보정)
    if 0 < total <= 9999 and not text.isdigit():
        total *= 10000

    return total
