from datetime import datetime, timedelta
import random
import uuid

from faker import Faker
import pandas as pd

random.seed(42)
Faker.seed(42)
faker = Faker('ko_KR')

REGION = ['서울', '수원', '고양', '용인', '화성', '성남', '의정부', '안양',
          '부천', '광명', '평택', '동두천', '안산', '과천', '구리', '남양주']

def get_random_date(start = datetime(2025, 1, 1), end = datetime(2025, 12, 31)):
    #
    
    return start + timedelta(days = random.randint(0, (end - start).days))

def generate_store():
    # 매장ID, 매장 이름, 매장 크기, 지역, 매니저 이름, 변경 일자

    stores = []
    open_date = datetime(2025, 1, 1)
    for i in range(len(REGION)):
        id = i + 1
        name = f'DBT 전자 {i + 1}호점'
        size = random.randint(300, 600)
        region = REGION[i]
        manager = faker.name()

        store = {
            'store_id': id,
            'store_name': name,
            'store_size': size,
            'store_region': region,
            'manager': manager,
            'updated_at': open_date.date()
        }
        stores.append(store)
    
        # 매장 변경 이력 생성(증축, 매니저 변경)
        for _ in range(random.randint(0, 3)):
            updated_at = get_random_date(start = open_date)

            n = random.random()
            if n >= 0.8:
                size += random.randint(150, 300)
            else:
                manager = faker.name()
            
            store = {
                'store_id': id,
                'store_name': name,
                'store_size': size,
                'store_region': region,
                'manager': manager,
                'updated_at': updated_at.date()
            }
            stores.append(store)

        open_date += timedelta(days = random.randint(10, 30))

    return pd.DataFrame(stores).sort_values('updated_at').reset_index(drop = True)

def generate_customer():
    # 고객ID, 고객 이름, 전화번호, 지역, 등급, 변경 일자

    regions = REGION + ['부산', '대구', '인천', '대전', '울산']

    customers = []
    sign_date = datetime(2025, 1, 1)
    for i in range(100):
        id = i + 1
        name = faker.name()
        phone = f'010-{str(random.randint(0, 9999)).zfill(4)}-{str(random.randint(0, 9999)).zfill(4)}'
        region = random.choice(regions)

        customer = {
            'customer_id': id,
            'customer_name': name,
            'phone_number': phone,
            'city': region,
            'membership': 'bronze',
            'updated_at': sign_date.date()
        }
        customers.append(customer)

        # 고객 변경 이력 생성(번호 변경, 이사)
        for _ in range(random.randint(0, 2)):
            updated_at = get_random_date(start = sign_date)

            n = random.random()
            if n >= 0.7:
                kind = random.choice(['number', 'region'])
                if kind == 'number':
                    phone = f'010-{str(random.randint(0, 9999)).zfill(4)}-{str(random.randint(0, 9999)).zfill(4)}'
                else:
                    region = random.choice([r for r in regions if r != region])

                customer = {
                    'customer_id': id,
                    'customer_name': name,
                    'phone_number': phone,
                    'city': region,
                    'membership': 'bronze',
                    'updated_at': updated_at.date()
                }
                customers.append(customer)

        sign_date += timedelta(days = random.randint(1, 3))

    return pd.DataFrame(customers).sort_values('updated_at').reset_index(drop = True)

def generate_product():
    # 상품ID, 상품명, 가격, 판매 여부, 변경 일자

    category = ['TV', '에이컨', '스마트폰', '세탁기', '냉장고',
                '전자레인지', '노트북', '데스크톱', '공기청정기',
                '청소기', '로봇청소기', '건조기', '식기세척기']

    launch_date = datetime(2025, 1, 1)
    products = []
    for i in range(len(category)):
        id = i + 1
        name = category[i]
        price = random.randint(50, 500) * 1000
        available = True

        product = {
            'product_id': id,
            'product_name': name,
            'price': price,
            'available': available,
            'updated_at': launch_date.date()
        }
        products.append(product)

        # 상품 변경 이력 생성(가격 변경, 판매 여부)
        for i in range(random.randint(0, 5)):
            updated_at = get_random_date(start = launch_date)

            n = random.random()
            if n <= 0.6:
                price += random.randint(-100, 100) * 1000
            elif n <= 0.8:
                available = not available

            product = {
                'product_id': id,
                'product_name': name,
                'price': price,
                'available': available,
                'updated_at': updated_at.date()
            }
            products.append(product)
            
        launch_date += timedelta(days = random.randint(1, 10))

    return pd.DataFrame(products).sort_values('updated_at').reset_index(drop = True) 

def generate_order(store, customer, product):
    # 3천 개의 주문 이력 생성

    # 상품 판매 가능 여부 타임라인 반환
    def build_product_timeline(product):
        timeline = {}
        for id, group in product.groupby('product_id'):
            timeline[id] = list(zip(group['available'], group['updated_at']))
        
        return timeline
    
    # 상품 판매 가능 여부 반환
    # target_date 기준으로 available 값을 status에 덮어씌움
    # target_date 보다 큰 값은 확인할 필요 없으므로 break
    def is_available(id, timeline, target_date):
        status = False
        for available, updated_at in timeline[id]:
            if updated_at <= target_date:
                status = available
            else:
                break
        return status
    
    store_start = store.groupby('store_id')['updated_at'].min().to_dict()
    customer_start = customer.groupby('customer_id')['updated_at'].min().to_dict()
    product_timeline = build_product_timeline(product)

    store_ids = max(store_start.keys())
    customer_ids = max(customer_start.keys())
    product_ids = product_timeline.keys()

    orders = []
    while len(orders) < 3000:
        store_id = random.randint(1, store_ids)
        customer_id = random.randint(1, customer_ids)

        # 지점, 고객 중 늦게 활성화된 시간
        earliest = max(store_start[store_id], customer_start[customer_id])
        earliest_datetime = datetime.combine(earliest, datetime.min.time())

        random_date = get_random_date(start = earliest_datetime)
        order_datetime = random_date + timedelta(seconds = random.randint(0, 24 * 3600 - 1)) # 24시간 범위
        order_date = order_datetime.date()

        # 구매 가능한 상품 목록 필터링
        available_products = [
            product_id for product_id in product_ids if is_available(product_id, product_timeline, order_date)
        ]
        if not available_products:
            continue
        product_id = random.choice(available_products)

        order = {
            'order_id': uuid.uuid4(),
            'store_id': store_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'ordered_at': order_datetime,
            'quantity': random.choices([1, 2, 3], weights = [80, 15, 5])[0]
        }
        orders.append(order)

    return pd.DataFrame(orders).sort_values('ordered_at').reset_index(drop = True) 

if __name__ == '__main__':
    store = generate_store()
    customer = generate_customer()
    product = generate_product()
    order = generate_order(store, customer, product)

    store.to_csv('seeds/raw_stores.csv', index = False)
    customer.to_csv('seeds/raw_customers.csv', index = False)
    product.to_csv('seeds/raw_products.csv', index = False)
    order.to_csv('seeds/raw_orders.csv', index = False)
