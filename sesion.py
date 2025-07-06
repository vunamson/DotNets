import asyncio
import json
import random
import re
from sys import _xoptions
import time
from urllib.parse import parse_qs, urlencode, urlparse
import aiohttp
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from aiohttp import ClientTimeout

# === Cấu hình Google Sheets ===
headers = {"User-Agent": "Mozilla/5.0"}
semaphore = asyncio.Semaphore(1)
product_semaphore = asyncio.Semaphore(50)
data = []
urls = [
    'https://mstcam.com/product-category/sport/sport-ice-hockey/',
    'https://mstcam.com/product-category/sport/sport-football/',
    # danh sách URL category
]

async def fetch(session, url):
    async with semaphore:
        try:
            async with session.get(url, timeout=20, allow_redirects=True) as res:
                final_url = str(res.url)
                if final_url != url:
                    print(f"⚠️ Redirected from {url} → {final_url} — dừng crawl.")
                    return None
                return await res.text() if res.status == 200 else None
        except Exception as e:
            print(f"❌ {url} - {e}")
            return None

        
def build_paged_url(category_url, page):
    if page == 1:
        return category_url
    # Với các trang tiếp theo, thêm /page/{page}
    print('page' ,page)
    category_url = category_url + f'page/{page}'
    return category_url


async def get_all_product_links(session, category_url):
    product_links = []
    page = 1
    while True:
        paged_url = build_paged_url(category_url, page)
        print('dang crawl page {page}')
        print(f"🔎 Fetching: {paged_url}")
        html = await fetch(session, paged_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        links_no_class = soup.select("a[aria-label]:not([class])")
        link_class = soup.select("a.woocommerce-LoopProduct-link")
        links = links_no_class if len(links_no_class) > 0 else link_class
        new_links = [a.get("href") for a in links if a.get("href")]

        # Dừng nếu không có link mới
        if not new_links or set(new_links).issubset(set(product_links)):
            break

        product_links.extend(new_links)
        page += 1

    # print('da crawl dac' {len(product_links)})
    return list(set(product_links))

def get_random_user_agent():    
    chrome_version = f"{random.randint(2, 200)}.0.0.0"  # Tạo số ngẫu nhiên từ 2.0.0.0 - 200.0.0.0
    return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36"

# proxies = load_proxies()

# 🛠️ Hàm tạo WebDriver với User-Agent mới
def create_driver(): 
    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    # chrome_options.set_capability('LT:Options', _xoptions)
    user_agent = get_random_user_agent()
    chrome_options.add_argument(f"user-agent={user_agent}")
    
    print(f"🆕 Đã thay đổi User-Agent: {user_agent}")
    return uc.Chrome(options=chrome_options)
    # return uc.Chrome(headless=False)

def parse_product( url):
    driver = create_driver()
    try:
        driver.get(url)
        time.sleep(5)  # hoặc chờ WebDriverWait, v.v.
        # Ví dụ đơn giản: lấy title
        title = driver.title
        return {"url": url, "title": title}
    except Exception as e:
        print(f"❌ [parse_product_sync] {url}: {e}")
        return None
    finally:
        driver.quit()

async def crawl_page(url):
    """
    Wrapper async: đợi có slot, rồi chạy parse_product_sync trong thread khác.
    """
    async with product_semaphore:
        return await asyncio.to_thread(parse_product, url)
    
async def crawl_all(session, links):
    tasks = [crawl_page(url) for url in links]
    results = []
    for coro in asyncio.as_completed(tasks):
        try:
            result = await coro
            if result:
                results.append(result)
                print(f"✅ Crawled {result['url']}")
        except Exception as e:
            print(f"❌ Error crawling: {e}")
    print(f"🎉 Finished crawling {len(results)} products.")
    return results


async def main():
    timeout = ClientTimeout(total=30)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        # 1. Lấy tất cả link sản phẩm từ các category
        all_links = []
        for cat_url in urls:
            links = await get_all_product_links(session, cat_url)
            all_links.extend(links)
        all_links = list(set(all_links))
        print(f"🛒 Total product links: {len(all_links)}")

        # 2. Crawl đa luồng các link sản phẩm
        while True :
            await crawl_all(session, all_links)

    # 3. Lưu kết quả
    # df = pd.DataFrame(data)
    # df.to_csv("woo_products_aiohttp.csv", index=False, encoding="utf-8-sig")
    # print("✅ Data saved to woo_products_aiohttp.csv")

if __name__ == '__main__':
    asyncio.run(main())

