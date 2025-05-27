from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, and_

from models import Provider, Product, Category, File

from utils.string import parse_korean_number
from utils.time import safe_parse_datetime, safe_parse_unix_timestamp

from core.logger import setup_logger
from functools import wraps

from datetime import datetime, timezone


logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정


async def fetch_categories(code: str, db: AsyncSession):
    try:
        result = await db.execute(
            select(Provider).where(Provider.code == code)
        )
        provider = result.scalars().first()
        if not provider:
            raise ValueError(f"{code} provider 조회 실패")

        if code == "BUNJANG":
            return await fetch_bunjang_categories(provider, db)
        else:
            raise NotImplementedError(f"{code} 카테고리 수집 미구현")

    except Exception as e:
        logger.exception("fetch_categories 에러")
        return []


async def fetch_bunjang_categories(provider: Provider, db: AsyncSession):
    try:
        # 1. BUNJANG 분기 처리
        if provider.code == "BUNJANG":
            url = f"{provider.url_api.rstrip('/')}/1/categories/list.json"
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                raw_categories = response.json().get("categories", [])

            # 3. 전처리 (재귀 파싱)
            def flatten(data, depth=1):
                result = []
                for cat in data:
                    result.append(Category(
                        provider_uid=provider.uid,
                        id=str(cat["id"]),
                        title=cat["title"],
                        depth=depth,
                        order=cat.get("order")
                    ))
                    if isinstance(cat.get("categories"), list):
                        result += flatten(cat["categories"], depth + 1)
                return result

            parsed_categories = flatten(raw_categories)

            # 4. 기존 데이터 삭제 후 저장
            await db.execute(delete(Category).where(Category.provider_uid == provider.uid))
            db.add_all(parsed_categories)
            await db.commit()
            logger.exception("카테고리 동기화 실패")

    except Exception as e:
        await db.rollback()
        logger.exception("카테고리 동기화 실패")


async def fetch_products(keyword: str, category: str, page: int = 0, accumulated: list[dict[str, str]] = None) -> list[dict[str, str]]:
    if accumulated is None:
        accumulated = []

    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(
                "https://api.bunjang.co.kr/api/1/find_v2.json",
                params={"f_category_id": category, "q": keyword, "page": page, "n": 100}
            )
            res.raise_for_status()
            items = res.json().get("list", [])

            # 종료 조건
            if not items:
                return accumulated

            for item in items:
                pid = item.get("pid")
                updated_dt = safe_parse_unix_timestamp(item.get("update_time"))  # or "updated_time", "updateDate" 등 실제 키에 맞게 수정

                if pid and updated_dt:
                    accumulated.append({
                        "pid": str(pid),
                        "updated_dt": str(updated_dt)
                    })

            print(f"fetch_products ==== keyword: {keyword}, category: {category}, page: {page}, 갯수: {len(accumulated)}")

            # 다음 페이지 재귀 호출
            return await fetch_products(keyword, category, page + 1, accumulated)
    except Exception:
        logger.exception(f"❌ {keyword} 페이지 {page} 에러 발생")
        return accumulated


async def fetch_product_detail(pid: str, db: AsyncSession):
    try:
        url = f"https://api.bunjang.co.kr/api/pms/v3/products-detail/{pid}?viewerUid=-1"
        async with httpx.AsyncClient() as client:
            res = await client.get(url)
            if res.status_code != 200:
                logger.warning(f"pid={pid} 상태코드={res.status_code} - 상품이 존재하지 않음(또는 비공개 등)")
                await delete_product_by_pid(pid, db)
                return None
            return res.json().get("data", {}).get("product")
    except Exception as e:
        logger.exception("에러 발생")
        # 네트워크 등 예외 상황에서도 삭제 처리
        await delete_product_by_pid(pid, db)
        return None


async def upsert_product(detail_data: dict, db: AsyncSession):
    try:
        pid = str(detail_data["pid"])
        provider_uid = 1

        stmt = select(Product).where(and_(
            Product.provider_uid == provider_uid,
            Product.pid == pid
        ))
        result = await db.execute(stmt)
        existing = result.scalar_one_or_none()

        title = detail_data.get("name")
        content = detail_data.get("description")
        status = map_sale_status(detail_data.get("saleStatus"))
        price = parse_korean_number(detail_data.get("price"))
        location = detail_data.get("geo", {}).get("address", "")
        category = detail_data.get("category", {}).get("name", "")
        # color = detail_data["color"]
        brand = detail_data.get("brand", {}).get("name", "")
        year, odo = extract_year_and_odo(detail_data.get("options", []))
        created_dt = safe_parse_datetime(detail_data.get("createdAt"))
        updated_dt = safe_parse_datetime(detail_data.get("updatedAt"))
        image_url = detail_data.get("imageUrl", "").replace("{res}", "1200")
        image_count = detail_data.get("imageCount", 0)

        if existing:
            if existing.updated_dt != updated_dt:
                # 필드 업데이트 (uid, pid 제외하고 모두 업데이트)
                existing.title = title
                existing.content = content
                existing.price = price
                existing.location = location
                existing.updated_dt = updated_dt
                existing.brand = brand
                existing.year = year
                existing.odo = odo
                existing.category = category
                existing.status = status
                existing.rmk = detail_data

                # 기존 이미지 확인 및 교체
                file_stmt = select(File).where(File.product_uid == existing.uid)
                file_result = await db.execute(file_stmt)
                existing_file = file_result.scalar_one_or_none()

                if existing_file:
                    if existing_file.url != image_url:
                        await db.execute(delete(File).where(File.product_uid == existing.uid))
                        db.add(File(
                            product_uid=existing.uid,
                            url=image_url,
                            count=image_count
                        ))
                elif image_url:
                    db.add(File(
                        product_uid=existing.uid,
                        url=image_url,
                        count=image_count
                    ))
        else:
            # 3. 새 Product 생성
            product = Product(
                pid=str(pid),
                provider_uid=provider_uid,
                status=status,
                title=title,
                content=content,
                price=price,
                location=location,
                brand=brand,
                year=year,
                odo=odo,
                category=category,
                rmk=detail_data,
                created_dt=created_dt,
                updated_dt=updated_dt
            )
            db.add(product)
            await db.flush()  # ⚠️ product.uid 확보

            # 4. 이미지 저장 (product.uid 사용)
            if image_url:
                db.add(File(
                    product_uid=product.uid,  # 새로 생성된 product.uid
                    url=image_url,
                    count=image_count
                ))
        await db.commit()
    except Exception:
        logger.exception("상품 upsert 중 에러 발생")

        await db.commit()
    except Exception:
        logger.exception("상품 upsert 중 에러 발생")


async def delete_product_by_pid(pid: str, db: AsyncSession):
    try:
        stmt = select(Product).where(Product.pid == pid)
        result = await db.execute(stmt)
        product = result.scalar_one_or_none()

        if product:
            product.status = 9  # 삭제 상태
            await db.commit()
            logger.info(f"pid={pid} 상품 status=9(삭제)로 변경 완료")
            return True
        else:
            logger.warning(f"pid={pid}에 해당하는 상품이 존재하지 않습니다.")
            return False

    except Exception as e:
        logger.exception("delete_product_by_pid 에러 발생")
        await db.rollback()
        return False




def extract_year_and_odo(options: list[dict]) -> tuple[Optional[int], Optional[int]]:
    year = None
    odo = None

    for option in options:
        group_id = option.get("optionGroupId")
        value = option.get("optionValue", "").strip()

        if group_id == "503" and value:
            # 연식: 숫자만 남기고 int로 변환
            try:
                year = int("".join(filter(str.isdigit, value)))
            except ValueError:
                year = None

        elif group_id == "504" and value:
            # 주행거리: parse_korean_number()로 변환
            try:
                odo = parse_korean_number(value.replace("km", "").strip())
            except Exception:
                odo = None

    return year, odo

def map_sale_status(sale_status: str) -> int:
    mapping = {
        "SELLING": 1,
        "RESERVED": 2,
        "SOLD_OUT": 3
    }
    return mapping.get(sale_status, 9)
