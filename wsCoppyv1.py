import os
import asyncio
import random
import aiohttp
import pandas as pd
from openpyxl import Workbook
from bs4 import BeautifulSoup
from slugify import slugify
import json
import time

def get_random_user_agent():
    major = random.randint(80, 115)
    minor = random.randint(0, 5999)
    build = random.randint(0, 150)
    patch = random.randint(0, 150)
    os_choices = [
        "(Windows NT 10.0; Win64; x64)",
        "(Macintosh; Intel Mac OS X 13_4)",
        "(X11; Linux x86_64)",
        "(Android 13; Mobile)",
        "(iPhone; CPU iPhone OS 16_4 like Mac OS X)"
    ]
    os_part = random.choice(os_choices)
    return f"Mozilla/5.0 {os_part} AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.{minor}.{build}.{patch} Safari/537.36"

# 2. Mảng cố định (nếu vẫn muốn giữ 3 mẫu cũ)
FIXED_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
]

# 3. Kết hợp và sinh thêm 400 cấu hình
USER_AGENTS = FIXED_AGENTS + [get_random_user_agent() for _ in range(400)]

# Hàm random header cho mỗi request
# def make_headers():
#     return {
#         "User-Agent": random.choice(USER_AGENTS),
#         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#         "Accept-Language": "en-US,en;q=0.9",
#         "Referer": "https://serenashirt.com/",
#         "Connection": "keep-alive",
#     }


# Ghi dữ liệu vào file Excel
def save_to_excel(data, file_name):
    wb = Workbook()
    ws = wb.active
    ws.append(["store", "title", "product_link", "image_link", "date_published", "is_order", "slug", "object_id", "object_name"])
    
    for row in data:
        ws.append(list(row.values()))
    
    wb.save(file_name)

# Kiểm tra xem sản phẩm có trong giỏ hàng không
def is_product_in_cart(soup):
    try:
        cart_script_tag = soup.find('script', string=lambda t: t and 'wpmDataLayer' in t)
        if not cart_script_tag:
            return False

        cart_data_str = cart_script_tag.string.split('window.wpmDataLayer = ')[-1].strip().rstrip(';')
        cart_data = json.loads(cart_data_str)
        cart_items = cart_data.get("cart", {}).get("items", [])
        return len(cart_items) > 0
    except Exception as e:
        print(f"Lỗi khi kiểm tra giỏ hàng: {e}")
        return False

# Phân tích HTML để lấy thông tin sản phẩm
def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    title = soup.find('h1')

    title_text = title.text.strip() if title else 'No Title'

    script_tag = soup.find('script', {'class': 'rank-math-schema-pro', 'type': 'application/ld+json'})
    date_published, product_link, image_link = "Not Found", "Not Found", "Not Found"
    is_order = is_product_in_cart(soup)

    if script_tag:
        try:
            data = json.loads(script_tag.string)
            for item in data.get("@graph", []):
                if item.get("@type") == "ItemPage":
                    date_published = item.get("datePublished", "Not Found")
                    product_link = item.get("url", "Not Found")
                if item.get("@type") == "Product":
                    images = item.get("image", [])
                    if isinstance(images, list) and images:
                        image_link = images[0].get("url", "Not Found")
        except json.JSONDecodeError:
            print("Error decoding JSON in script tag")

    return {
        'title': title_text,
        'product_link': product_link,
        'image_link': image_link,
        'date_published': date_published,
        'is_order': is_order
    }

# Hàm crawl một trang cụ thể với giới hạn request
async def crawl_page(sem, session, object_id, max_retries=1):
    url = f"https://mstcam.com/?post_type=product&?b={object_id}"

    async with sem:  # Giới hạn số lượng request đồng thời
        for attempt in range(max_retries):
            try:
                async with session.get(url, timeout=20) as response:
                    if response.status == 524:
                        print(f"⚠️ Lỗi 524 Timeout - Thử lại {attempt + 1}/{max_retries} cho {object_id}")
                        continue

                    if response.status != 200:
                        print(f"❌ Lỗi HTTP {response.status} - Bỏ qua {object_id}")
                        return None
                    pass
                    # html = await response.text()

                    # # Nếu trang lỗi 524 xuất hiện trong nội dung, bỏ qua request
                    # if "Error code 524" in html:
                    #     print(f"⚠️ Trang {object_id} bị lỗi 524 - Bỏ qua.")
                    #     return None

                    # data = parse_html(html)
                    # slug = slugify(data['title'], allow_unicode=True).lower()

                    # result = {
                    #     'store': 'keeptee.com',
                    #     'title': data['title'],
                    #     'product_link': data['product_link'],
                    #     'image_link': data['image_link'],
                    #     'date_published': data['date_published'],
                    #     'is_order': data['is_order'],
                    #     'slug': slug,
                    #     'object_id': object_id,
                    #     'object_name': 'product'
                    # }

                    # print(f"✅ {object_id}: {data['title']}")
                    # return result
            except asyncio.TimeoutError:
                print(f"⚠️ Request Timeout {object_id} - Thử lại {attempt + 1}/{max_retries}")
            except Exception as e:
                print(f"⚠️ Lỗi khi crawl {object_id} {url}: {e}")
                return None

    print(f"❌ Bỏ qua {object_id} sau {max_retries} lần thử thất bại")
    return None

# Hàm chính để chạy chương trình với giới hạn 30 request đồng thời
async def main():
    while True : 
        start, end = 1500000,3700000
        object_ids = list(range(start, end + 1))

        sem = asyncio.Semaphore(60000)  # Giới hạn 30 request đồng thời

        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.create_task(crawl_page(sem, session, object_id)) for object_id in object_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results = [r for r in results if r]
        # save_to_excel(valid_results, "crawled_keeptee_data.xlsx")

        print("🎉 Crawl hoàn tất!")
        print("📊 Tổng số sản phẩm crawl được:", len([r for r in results if r]))

if __name__ == "__main__":
    asyncio.run(main())