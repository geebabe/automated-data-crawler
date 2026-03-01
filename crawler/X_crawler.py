"""
Twitter/X Scraper - Async & Concurrent Version
Lấy posts, comments và replies từ kết quả tìm kiếm với hiệu suất cao hơn.

CÀI ĐẶT:
pip install playwright pandas beautifulsoup4 lxml
playwright install chromium

CHẠY:
python url_crawler.py
"""

import asyncio
import random
import os
import time
from datetime import datetime
import re
import urllib.parse
import unicodedata
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout, Page

class AsyncTwitterScraper:
    def __init__(self, search_url, max_concurrent_tabs=3):
        self.search_url = search_url
        self.base_url = 'https://x.com'
        self.posts_data = []
        self.playwright = None
        self.browser = None
        self.context = None
        # Semaphore để giới hạn số lượng tabs mở cùng lúc
        self.semaphore = asyncio.Semaphore(max_concurrent_tabs)
        self.storage_state_path = "x_state.json"
        self.file_lock = asyncio.Lock()
        self.current_filename = "twitter_posts.csv"

    async def init_browser(self):
        """Khởi tạo Playwright browser với session state nếu có"""
        print('Initializing Playwright Chromium (Async)...')
        self.playwright = await async_playwright().start()
        
        # Launch browser (headless=True configured to avoid popups)
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
            ]
        )

        # Load session nếu file tồn tại
        state = self.storage_state_path if os.path.exists(self.storage_state_path) else None
        if state:
            print(f"✅ Loading session from {state}")
        else:
            print("⚠️ No session file found. Logging in might be required.")

        self.context = await self.browser.new_context(
            storage_state=state,
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            locale='en-US',
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )

        # Anti-detection scripts
        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

    async def close(self):
        """Dọn dẹp tài nguyên"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        print('✓ Browser closed')

    async def wait_for_login(self, page: Page):
        """Kiểm tra login state và chờ user login nếu cần"""
        try:
            # Check nhanh xem có selector của user logged in không
            if not os.path.exists(self.storage_state_path):
                print('⚠️ No session file. Headless mode might fail if login is required.')
            else:
                print("Checking login status...")
                try:
                    await page.wait_for_selector('[data-testid="SideNav_AccountSwitcher_Button"]', timeout=5000)
                    print("✅ Already logged in.")
                except:
                    print("⚠️ Login check failed. Saving debug screenshot...")
                    await page.screenshot(path="debug_login.png")
                    
        except Exception as e:
            print(f"⚠️ Session check error: {e}")

    async def scroll_and_extract_posts(self, page: Page, max_posts=50, scroll_attempts=10):
        """Scroll search page và lấy list URLs"""
        posts = []
        seen_urls = set()
        
        print(f"🔍 Navigating to: {self.search_url}")
        await page.goto(self.search_url, wait_until='domcontentloaded')
        await asyncio.sleep(5)
        
        await self.wait_for_login(page)

        print(f'\n📜 Scrolling to collect tweets...')
        
        for scroll in range(scroll_attempts):
            html = await page.content()
            soup = BeautifulSoup(html, 'lxml')
            
            all_links = soup.find_all('a', href=True)
            new_count = 0
            
            for link in all_links:
                href = link.get('href', '')
                if '/status/' in href and not href.startswith('/search'):
                     # Xử lý URL
                    if href.startswith('http'):
                        post_url = href
                    elif href.startswith('/'):
                        post_url = self.base_url + href
                    else:
                        continue
                    
                    post_url = post_url.split('?')[0] # Remove query params
                    
                    if post_url not in seen_urls:
                        seen_urls.add(post_url)
                        posts.append({'url': post_url})
                        new_count += 1
                        
                        if len(posts) >= max_posts:
                            print(f'  ✓ Reached {max_posts} tweets limit')
                            return posts

            if new_count > 0:
                print(f"  Info: Found {new_count} new tweets (Total: {len(posts)})")
            elif scroll == 0:
                 # Debug if first scroll found nothing
                 print("⚠️ No tweets found on first scroll. Saving debug_search.png")
                 await page.screenshot(path="debug_search.png")
                 with open("debug_search.html", "w", encoding="utf-8") as f:
                     f.write(html)
            
            # Scroll down
            await page.evaluate('window.scrollBy(0, window.innerHeight * 2)')
            await asyncio.sleep(random.uniform(2, 4))
            
            # Đôi khi scroll ngược lên xíu để trigger load
            if scroll % 3 == 0:
                await page.evaluate('window.scrollBy(0, -500)')
                await asyncio.sleep(1)

        return posts

    async def extract_content_from_article(self, article_soup):
        """Helper tách data từ soup của 1 article"""
        # (Logic tương tự code cũ nhưng dùng soup object passed in)
        author = ''
        username_div = article_soup.find('div', attrs={'data-testid': 'User-Name'})
        if username_div:
            username_link = username_div.find('a', href=re.compile(r'^/[^/]+$'))
            if username_link:
                author = username_link.get('href', '').strip('/')
            if not author:
                spans = username_div.find_all('span')
                for span in spans:
                    text = span.get_text(strip=True)
                    if text.startswith('@'):
                        author = text.strip('@')
                        break
        
        content_url = ''
        timestamp = ''
        time_elem = article_soup.find('time')
        if time_elem:
            timestamp = time_elem.get('datetime', '')
            time_link = time_elem.find_parent('a', href=re.compile(r'/status/'))
            if time_link:
                href = time_link.get('href', '')
                if href.startswith('/'):
                    content_url = self.base_url + href
                else:
                    content_url = href
                content_url = content_url.split('?')[0]

        content = ''
        content_div = article_soup.find('div', attrs={'data-testid': 'tweetText'})
        if content_div:
            content = content_div.get_text(strip=True)

        replies_count = 0
        reply_button = article_soup.find('button', attrs={'data-testid': 'reply'})
        if reply_button:
            aria_label = reply_button.get('aria-label', '')
            match = re.search(r'(\d+)', aria_label)
            if match:
                replies_count = int(match.group(1))

        return {
            'author': author,
            'timestamp': timestamp,
            'url': content_url,
            'content': content,
            'replies_count': replies_count
        }

    async def save_chunk(self, data):
        """Lưu incremental data vào CSV"""
        if not data:
            return

        async with self.file_lock:
            # Check file exists to write header
            file_exists = os.path.isfile(self.current_filename)
            
            df = pd.DataFrame(data)
            # Append mode, header only if file doesn't exist
            df.to_csv(self.current_filename, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')
            print(f"    💾 Saved chunk {len(data)} rows")

    async def process_single_post(self, post_info):
        """Xử lý 1 post start-to-finish trong 1 tab riêng (được giới hạn bởi semaphore)"""
        url = post_info['url']
        results = []
        
        async with self.semaphore:
            page = await self.context.new_page()
            try:
                print(f"  ▶ Parsing: {url}")
                await page.goto(url, wait_until='domcontentloaded')
                await asyncio.sleep(3) # Initial wait
                
                # Scroll a bit to load replies
                for _ in range(3):
                    await page.evaluate('window.scrollBy(0, 800)')
                    await asyncio.sleep(1)

                html = await page.content()
                soup = BeautifulSoup(html, 'lxml')
                articles = soup.find_all('article', attrs={'data-testid': 'tweet'})
                
                if not articles:
                    print(f"  ⚠️ No articles found for {url}")
                    return []

                # Main tweet extraction
                data = await self.extract_content_from_article(articles[0])
                if not data['content']:
                     # Review retry logic here if needed, but keeping it simple for speed
                     pass

                main_record = {
                    'url': url,
                    'post_author': data['author'],
                    'post_time': data['timestamp'],
                    'post_content': data['content'],
                    'comment_url': data['url'] or url,
                    'comment_author': data['author'],
                    'comment_time': data['timestamp'],
                    'comment_content': data['content'],
                    'comment_type': 'main_post',
                    'comment_depth': 0,
                    'replies_count': data['replies_count'],
                    'crawl_timestamp': datetime.now().isoformat()
                }
                results.append(main_record)

                # Process direct replies found on page
                # Note: Code cũ có đệ quy (recursive), ở đây để tối ưu tốc độ ta sẽ lấy 
                # replies level 1 trước. Nếu muốn deep recursive async sẽ phức tạp hơn chút.
                # Tạm thời giữ logic level 1 detail, có thể mở rộng sau.
                
                for article in articles[1:]:
                    r_data = await self.extract_content_from_article(article)
                    if r_data['content']:
                        results.append({
                            'post_url': url,
                            'post_author': main_record['post_author'],
                            'post_time': main_record['post_time'],
                            'post_content': main_record['post_content'],
                            
                            'parent_comment_url': url, # Simplification
                            'parent_comment_author': main_record['post_author'],
                            
                            'comment_url': r_data['url'],
                            'comment_author': r_data['author'],
                            'comment_time': r_data['timestamp'],
                            'comment_content': r_data['content'],
                            'comment_type': 'reply',
                            'comment_depth': 1,
                            'replies_count': r_data['replies_count'],
                            'crawl_timestamp': datetime.now().isoformat()
                        })

                print(f"  ✓ Finished {url}: {len(results)} items")
                # Save incremental
                await self.save_chunk(results)
                
                return results

            except Exception as e:
                print(f"  ✗ Error processing {url}: {e}")
                return []
            finally:
                await page.close()

    async def scrape(self, max_posts=100, scroll_attempts=20, filename='twitter_posts.csv'):
        self.current_filename = filename
        await self.init_browser()
        
        try:
            # 1. Search page -> Get List of URLs
            main_page = await self.context.new_page()
            found_posts = await self.scroll_and_extract_posts(main_page, max_posts, scroll_attempts)
            await main_page.close()
            
            print(f"\n⚡ Starting concurrent scrape for {len(found_posts)} posts...")
            print(f"   Concurrency: {self.semaphore._value} tabs")
            
            # 2. Concurrent Scraping
            tasks = [self.process_single_post(post) for post in found_posts]
            all_results_lists = await asyncio.gather(*tasks)
            
            # Flatten list
            for res_list in all_results_lists:
                self.posts_data.extend(res_list)
                
            print(f"✅ Completed. Total {len(self.posts_data)} rows saved to {self.current_filename}")
            
        finally:
            await self.close()

    def save_to_csv(self, filename='twitter_posts_async_optimized_kynguyenvuonminh.csv'):
        if not self.posts_data:
            print("No data to save.")
            return

        df = pd.DataFrame(self.posts_data)
        # Reorder/Ensure columns if needed, matching old format
        print(f"💾 Saving {len(df)} rows to {filename}...")
        df.to_csv(filename, index=False, encoding='utf-8-sig')


def create_search_url(keyword):
    """
    Tạo URL tìm kiếm Twitter từ keyword.
    Format: https://x.com/search?q={encoded_keyword}&src=typed_query&f=top
    """
    base_search = "https://x.com/search"
    params = {
        'q': keyword,
        'src': 'typed_query',
        #'f': 'top'
    }
    query_string = urllib.parse.urlencode(params)
    return f"{base_search}?{query_string}"


def create_filename_from_keyword(keyword):
    """
    Tạo filename từ keyword:
    1. Chuẩn hóa unicode (bỏ dấu tiếng Việt)
    2. Lowercase
    3. Xóa khoảng trắng
    Ví dụ: 'Kỷ nguyên vươn mình' -> 'kynguyenvuonminh'
    """
    if not keyword:
        return "twitter_posts_async_optimized_unknown.csv"
        
    # Chuẩn hóa unicode tổ hợp thành dựng sẵn (NFD)
    normalized = unicodedata.normalize('NFD', keyword)
    # Lọc bỏ các ký tự dấu (Combining Diacritical Marks)
    no_accent = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    
    # Lowercase và xóa space
    clean_keyword = no_accent.lower().replace(' ', '')
    
    # Giữ lại chỉ alpha-numeric để an toàn cho filename
    clean_keyword = re.sub(r'[^a-z0-9]', '', clean_keyword)
    
    return f"X_data/week6/twitter_posts_async_optimized_{clean_keyword}.csv"


async def main():
    # Nhập keyword từ người dùng
    keyword = input("Nhập từ khóa cần crawl (ví dụ: VinFast, Xe điện): ").strip()
    keyword = keyword.encode("utf-8", "ignore").decode("utf-8")

    if not keyword:
        print("⚠️ Chưa nhập từ khóa. Dùng từ khóa mặc định.")
        keyword = "VinFast"

    print(f"🔑 Keyword: {keyword}")
    
    # Tạo URL tìm kiếm
    SEARCH_URL = create_search_url(keyword)
    print(f"🔗 Generated URL: {SEARCH_URL}")
    
    # Tạo dynamic filename
    csv_filename = create_filename_from_keyword(keyword)
    print(f"📄 Output file: {csv_filename}")
    
    scraper = AsyncTwitterScraper(SEARCH_URL, max_concurrent_tabs=3)
    await scraper.scrape(max_posts=500, scroll_attempts=20, filename=csv_filename)

if __name__ == '__main__':
    asyncio.run(main())
