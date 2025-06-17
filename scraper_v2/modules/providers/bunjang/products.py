import httpx
from typing import Optional
from core.logger import setup_logger
from core.database import AsyncSessionLocal
from modules import batch, common, od
from utils import time, string
from sqlalchemy import and_, select, delete
from models import Product, File
from modules.providers import bunjang

logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정

PROVIDER = None
PROCESSING = False

CATEGORIES = []
CATEGORIES_ROUND = 0
KEYWORDS = []
KEYWORDS_ROUND = 0
PRODUCTS = od.odSet()

PAGE = 1
LIMIT = 100

UPSERT_OD = od.odSet()
DELETE_OD = od.odSet()


def reset(categories: list = None, keywords: list = None):
    global PAGE, CATEGORIES, KEYWORDS, PRODUCTS, CATEGORIES_ROUND, KEYWORDS_ROUND
    PAGE = 1
    CATEGORIES = categories or []
    CATEGORIES_ROUND = 0
    # todo - if categories == None: select from database
    KEYWORDS = keywords or [""]
    KEYWORDS_ROUND = 0
    PRODUCTS = od.odSet()

async def search_list(task_id: int):
    global PAGE, LIMIT, CATEGORIES, CATEGORIES_ROUND, KEYWORDS, KEYWORDS_ROUND, PRODUCTS
    if CATEGORIES_ROUND >= len(CATEGORIES):
        batch.batch_list['bunjang_list'].stop()
        return
    if KEYWORDS_ROUND >= len(KEYWORDS):
        CATEGORIES_ROUND += 1
        KEYWORDS_ROUND = 0
        PAGE = 1
        return 
    page = PAGE
    PAGE += 1
    category = CATEGORIES[CATEGORIES_ROUND]
    keyword = KEYWORDS[KEYWORDS_ROUND]
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(
                "https://api.bunjang.co.kr/api/1/find_v2.json",
                params={"f_category_id": category, "q": keyword, "page": page, "n": LIMIT}
            )
            res.raise_for_status()
            items = res.json().get("list", [])

            # item이 없으면 다음 키워드로
            if not items:
                if len(KEYWORDS) >= KEYWORDS_ROUND and keyword == KEYWORDS[KEYWORDS_ROUND]:
                    KEYWORDS_ROUND += 1
                    PAGE = 1
                    progress = ((KEYWORDS_ROUND + 1) * (CATEGORIES_ROUND + 1)) / (len(KEYWORDS) * len(CATEGORIES)) * 100
                    print(f"리스트 조회 - {int(progress)}% 완료 - {len(PRODUCTS)} 건 조회")
                return
            for item in items:
                pid = item.get("pid")
                updated_dt = time.safe_parse_unix_timestamp(item.get("update_time"))  # or "updated_time", "updateDate" 등 실제 키에 맞게 수정
                if pid and updated_dt:
                    PRODUCTS.push(pid, updated_dt)
            return
    except Exception:
        logger.exception(f"❌ {category} / {keyword} - {page} 에러 발생")
        KEYWORDS_ROUND += 1
        PAGE = 1
        return

async def fetch_product_detail(pid: str):
    try:
        url = f"https://api.bunjang.co.kr/api/pms/v3/products-detail/{pid}?viewerUid=-1"
        async with httpx.AsyncClient() as client:
            res = await client.get(url)
            if res.status_code != 200:
                print(f"pid={pid} 상태코드={res.status_code} - 상품이 존재하지 않음(또는 비공개 등)")
                await delete_product_by_pid(pid)
                return None
            return res.json().get("data", {}).get("product")
    except Exception as e:
        logger.exception("상품 상세 조회 중 에러 발생")
        return None


async def upsert_product(task_id: int):
    try:
        global PROVIDER, UPSERT_OD
        if len(UPSERT_OD) == 0:
            batch.batch_list['bunjang_upsert'].stop()

        pid, udt = UPSERT_OD.pop()
        provider_uid = PROVIDER.uid

        data = await fetch_product_detail(pid)
        if not data:
            return
        try:
            async with AsyncSessionLocal() as db:
                stmt = select(Product).where(and_(
                    Product.provider_uid == provider_uid,
                    Product.pid == pid
                ))
                result = await db.execute(stmt)
                existing = result.scalar_one_or_none()

                title = data.get("name")
                content = data.get("description")
                status = map_sale_status(data.get("saleStatus"))
                price = string.parse_korean_number(data.get("price"))
                location = data.get("geo", {}).get("address", "")
                category = data.get("category", {}).get("name", "")
                # color = detail_data["color"]
                brand = data.get("brand", {}).get("name", "")
                year, odo = extract_year_and_odo(data.get("options", []))
                created_dt = time.safe_parse_datetime(data.get("createdAt"))
                updated_dt = time.safe_parse_datetime(data.get("updatedAt"))
                image_url = data.get("imageUrl", "").replace("{res}", "1200")
                image_count = data.get("imageCount", 0)

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
                        existing.rmk = data
                        existing.is_conversion = False

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
                        rmk=data,
                        is_conversion=False,
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
            await db.rollback()
    except Exception:
        logger.exception("상품 upsert 중 에러 발생")

async def delete_product_by_pid(pid: str):
    async with AsyncSessionLocal() as db:
        try:
            stmt = select(Product).where(Product.pid == pid)
            result = await db.execute(stmt)
            product = result.scalar_one_or_none()

            if product:
                product.status = 9  # 삭제 상태
                product.is_conversion = False
                await db.commit()
                print(f"pid={pid} 상품 status=9(삭제)로 변경 완료")
                return True
            else:
                print(f"pid={pid}에 해당하는 상품이 존재하지 않습니다.")
                return False

        except Exception as e:
            logger.exception("delete_product_by_pid 에러 발생")
            await db.rollback()
            return False

async def delete_product(task_id: int):
    try:
        global PROVIDER, DELETE_OD
        if len(UPSERT_OD) == 0:
            batch.batch_list['bunjang_delete'].stop()
        pid, udt = UPSERT_OD.pop()
        await delete_product_by_pid(pid)
    except Exception as e:
        logger.exception("상품 delete 중 에러 발생")



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
                odo = string.parse_korean_number(value.replace("km", "").strip())
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




async def sync_products():
    global PROVIDER, PAGE, CATEGORIES, KEYWORDS, PRODUCTS, PROCESSING, UPSERT_OD, DELETE_OD
    print(f"queue len : {len(PRODUCTS)}")

    PROVIDER = bunjang.PROVIDER
    if not PROVIDER:
        raise ValueError("PROVIDER Not Init")
    
    if(PROCESSING):
        print(f"이미 실행중입니다. ")
        return
    PROCESSING = True
    # 리셋
    reset(categories=["750800"], keywords=common.read_keywords())
    print(KEYWORDS)

    # 카테고리, 키워드 로 리스트 조회 (배치)
    batch.batch_list['bunjang_list'] = batch.batchSet(5)
    await batch.batch_list['bunjang_list'].batch(search_list, -1) # -1 횟수 제한 없이 무제한 반복 - search_list 에서 종료 호출
    print(len(PRODUCTS))

    async with AsyncSessionLocal() as db:
        dbProducts = select(Product).where(
            (Product.status != 9) & (Product.provider_uid == PROVIDER.uid)
        )
        dbResult = await db.execute(dbProducts)
        dbProductsList = dbResult.scalars().all()
        db_od = od.odSet()
        for p in dbProductsList:
            db_od.push(p.pid, time.normalize_datetime(p.updated_dt))

    # safe_parse_datetime 등으로 날짜를 비교 가능한 형태로 통일해야 함
    src_set = PRODUCTS
    db_set = db_od

    # 1. 신규/업데이트 대상 (all_product_pids에만 있음)
    UPSERT_OD = src_set - db_set
    # 2. 삭제 대상 (DB에만 있음)
    DELETE_OD = db_set - src_set

    logger.info(f"✅ 신규/업데이트 대상: {len(UPSERT_OD)}개, 삭제 대상: {len(DELETE_OD)}개")

    # 2. 상세 정보 upsert
    logger.info(f"신규/업데이트 시작")
    batch.batch_list['bunjang_upsert'] = batch.batchSet(10)
    await batch.batch_list['bunjang_upsert'].batch(upsert_product, -1) # -1 횟수 제한 없이 무제한 반복 - search_list 에서 종료 호출
    logger.info(f"신규/업데이트 완료")

    logger.info(f"삭제 시작")
    # 3. 삭제 처리도 batch로 실행
    batch.batch_list['bunjang_delete'] = batch.batchSet(10)
    await batch.batch_list['bunjang_delete'].batch(delete_product, -1) # -1 횟수 제한 없이 무제한 반복 - search_list 에서 종료 호출
    logger.info(f"삭제 완료")

    PROCESSING = False