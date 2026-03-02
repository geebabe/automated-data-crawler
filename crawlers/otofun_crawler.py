import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re
from urllib.parse import urljoin, urlparse
import json
import os
import csv
import hashlib

class OtofunCrawlerV2:
    def __init__(self, base_url="https://www.otofun.net", auto_save_file='otofun_progress_v2.csv'):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.data = [] # Keep for backwards compatibility/debug, but main storage is CSV
        self.auto_save_file = auto_save_file
        self.auto_save_enabled = auto_save_file is not None
        self.existing_records = set()
        
        # Initialize CSV file if needed
        self.csv_file_handle = None
        self.csv_writer = None
        
        if self.auto_save_enabled:
            self.init_storage()
            self.load_history()

    def init_storage(self):
        """Initialize CSV file and writer"""
        try:
            file_exists = os.path.isfile(self.auto_save_file)
            
            # We open in append mode, but we will use the csv module for speed
            # Note: We are NOT keeping the handle open indefinitely to avoid data loss on crash,
            # but we could. For now, let's stick to append-on-demand for safety, 
            # but using 'csv' module is 100x faster than 'pd.to_csv' for single rows.
            
            if not file_exists:
                with open(self.auto_save_file, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'url', 
                        'post_content', 
                        'post_time', 
                        'comment_content', 
                        'comment_time', 
                        'crawl_timestamp'
                    ])
        except Exception as e:
            print(f"Error initializing storage: {e}")

    def normalize_url(self, url):
        """Convert any thread URL to its first page URL"""
        if not url:
            return ""
        parsed = urlparse(url)
        path = parsed.path
        
        # Otofun structure: /threads/title.id/page-2 or /threads/title.id/post-123
        # We want to keep /threads/title.id/ and strip the rest
        if '/threads/' in path:
            # Regex to match /threads/slug.id and capture it
            # IDs are usually numeric at the end of the slug segment
            match = re.match(r'(/threads/[^/]+\.\d+)', path)
            if match:
                path = match.group(1) + '/'
            else:
                # Fallback: simple strip of common suffixes
                path = re.sub(r'/(page|post)-\d+/?.*$', '/', path)
        
        clean_url = f"{parsed.scheme}://{parsed.netloc}{path}"
        return clean_url

    def get_content_hash(self, url, content):
        """Generate a unique hash for deduplication"""
        # We use (Normalized URL + Content) as key
        # This ensures the same comment in same thread is skipped, 
        # but same content in DIFFERENT thread is kept.
        normalized_url = self.normalize_url(url)
        content_clean = str(content).strip()
        unique_string = f"{normalized_url}|{content_clean}"
        return hashlib.md5(unique_string.encode('utf-8')).hexdigest()

    def load_history(self):
        """Load previously crawled records to avoid duplicates"""
        if not os.path.isfile(self.auto_save_file):
            return

        print("Loading history...")
        try:
            # Use pandas for fast read of large files
            df = pd.read_csv(self.auto_save_file)
            
            count = 0
            if 'comment_content' in df.columns and 'url' in df.columns:
                for _, row in df.iterrows():
                    url = row['url']
                    content = row['comment_content']
                    if pd.notna(content):
                        record_hash = self.get_content_hash(url, content)
                        self.existing_records.add(record_hash)
                        count += 1
            
            print(f"Loaded {len(self.existing_records)} existing records from history.")
        except Exception as e:
            print(f"Warning: Could not load history correctly: {e}")

    def save_record_fast(self, record):
        """Save a single record to CSV efficiently"""
        if not self.auto_save_enabled:
            return

        try:
            with open(self.auto_save_file, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    record.get('url', ''),
                    record.get('post_content', ''),
                    record.get('post_time', ''),
                    record.get('comment_content', ''),
                    record.get('comment_time', ''),
                    record.get('crawl_timestamp', '')
                ])
        except Exception as e:
            print(f"  WARNING: Failed to save record: {str(e)}")

    def get_page(self, url, max_retries=3):
        """Fetch a page with retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    print(f"Page not found (404): {url}")
                    return None
                else:
                    print(f"Status {response.status_code} for {url}")
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            
            if attempt < max_retries - 1:
                time.sleep(2)
        return None

    def extract_search_results(self, html_content, debug=False):
        """Extract article URLs from search results page"""
        soup = BeautifulSoup(html_content, 'html.parser')
        article_urls = []

        # Generic approach for Xenforo search results
        # Look for h3.contentRow-title a
        links = soup.select('h3.contentRow-title a')
        
        for link in links:
            href = link.get('href', '')
            if href and '/threads/' in href:
                full_url = urljoin(self.base_url, href)
                # Normalize immediately to ensure we have the clean thread URL
                clean_url = self.normalize_url(full_url)
                if clean_url not in article_urls:
                    article_urls.append(clean_url)

        return article_urls

    def parse_datetime(self, datetime_str):
        """Parse Vietnamese datetime format"""
        try:
            datetime_str = datetime_str.strip()
            # Common formats: "HH:MM dd/mm/yyyy" or "dd/mm/yyyy"
            if ':' in datetime_str:
                return datetime.strptime(datetime_str, '%H:%M %d/%m/%Y')
            else:
                return datetime.strptime(datetime_str, '%d/%m/%Y')
        except:
            return None

    def _extract_message_info(self, message, soup):
        """Helper to extract content and time from a message element"""
        try:
            # Extract post content
            post_content = ""
            
            # Xenforo usually uses bbWrapper
            content_div = message.find('div', class_='bbWrapper')
            if not content_div:
                content_div = message.find('div', class_='message-userContent')
            
            if content_div:
                # Remove quotes to avoid duplicating content
                for blockquote in content_div.find_all('blockquote'):
                    blockquote.decompose()
                post_content = content_div.get_text(separator='\n', strip=True)
            else:
                # Fallback
                post_content = message.get_text(separator='\n', strip=True)

            # Extract datetime
            post_time = None
            
            # Format 1: <header class="message-attribution"> ... <a class="u-concealed"><time> or text</a>
            header = message.find('header', class_='message-attribution')
            if header:
                # Try finding time element first (iso format)
                time_elem = header.find('time')
                if time_elem and time_elem.get('datetime'):
                     try:
                        post_time = datetime.fromisoformat(time_elem['datetime'].replace('Z', '+00:00'))
                     except: pass
                
                # Try finding link text
                if not post_time:
                    date_link = header.find('a', class_='u-concealed')
                    if date_link:
                        post_time = self.parse_datetime(date_link.get_text(strip=True))

            post_time_str = post_time.strftime('%Y-%m-%d %H:%M:%S') if post_time else ''
            
            return post_content, post_time_str
        except Exception as e:
            # print(f"Error parse message: {e}")
            return "", ""

    def get_next_page_url(self, soup):
        """Extract next page URL from pagination"""
        next_link = soup.select_one('a.pageNav-jump--next')
        if next_link and next_link.has_attr('href'):
            return urljoin(self.base_url, next_link['href'])
        return None

    def crawl_thread(self, thread_url, debug=False):
        """Crawl ALL pages of a single thread"""
        
        # 1. Normalize URL to ensure we start at Page 1
        current_url = self.normalize_url(thread_url)
        print(f"\nProcessing Thread: {current_url}")
        
        page_num = 1
        main_post_content = ""
        main_post_time = ""
        
        # State to track if we found the main post info
        main_post_found = False
        
        while current_url:
            if debug:
                print(f"  -> Fetching Page {page_num}: {current_url}")
            
            response = self.get_page(current_url)
            if not response:
                break
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all messages
            # Xenforo structure: article.message
            messages = soup.select('article.message')
            if not messages:
                # Fallback for some old themes
                messages = soup.select('div.message')
            
            if not messages:
                print(f"    No messages found on page {page_num}")
                break
                
            crawl_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            new_comments_count = 0
            
            for idx, msg in enumerate(messages):
                content, time_str = self._extract_message_info(msg, soup)
                
                # Logic to identify Main Post vs Comments
                # The FIRST message on the FIRST page is the Main Post
                is_first_post = (page_num == 1 and idx == 0)
                
                if is_first_post:
                    main_post_content = content
                    main_post_time = time_str
                    main_post_found = True
                    
                    # We also save the main post as a record (with empty comment_content)
                    # Checking if this main post was already recorded is tricky since we only hash comments
                    # But broadly, we can just save it. 
                    
                    # Option: Create a special hash for main post? 
                    # For now, let's treat the Main Post as a record with comment_content=''
                    record = {
                        'url': current_url, # Or thread_url
                        'post_content': main_post_content,
                        'post_time': main_post_time,
                        'comment_content': '', # Empty for main post
                        'comment_time': '', # Empty for main post
                        'crawl_timestamp': crawl_timestamp
                    }
                    
                    # Deduplication for Main Post
                    record_hash = self.get_content_hash(thread_url, "MAIN_POST_MARKER")
                    if record_hash not in self.existing_records:
                        self.save_record_fast(record)
                        self.existing_records.add(record_hash)
                        print("    -> Saved Main Post")
                        
                else:
                    # It's a comment (reply)
                    if not content:
                        continue
                        
                    # Check duplication
                    record_hash = self.get_content_hash(thread_url, content)
                    
                    if record_hash in self.existing_records:
                        continue
                        
                    # Prepare record
                    # If we haven't found main post (e.g. started on page 2 - shouldn't happen with normalization),
                    # we might have empty main_post_content.
                    record = {
                        'url': current_url,
                        'post_content': main_post_content,
                        'post_time': main_post_time,
                        'comment_content': content,
                        'comment_time': time_str,
                        'crawl_timestamp': crawl_timestamp
                    }
                    
                    self.save_record_fast(record)
                    self.existing_records.add(record_hash)
                    new_comments_count += 1
            
            if new_comments_count > 0:
                print(f"    -> Page {page_num}: Saved {new_comments_count} new comments")
            
            # Check for next page
            next_url = self.get_next_page_url(soup)
            if next_url and next_url != current_url:
                current_url = next_url
                page_num += 1
                time.sleep(1) # Polite delay
            else:
                break


    def crawl_search_results(self, start_url, max_pages=None, debug=False):
        """Crawl all search result pages and their articles"""
        current_url = start_url
        page_count = 0

        while current_url:
            page_count += 1
            print(f"\n{'=' * 60}")
            print(f"Processing Search Result Page {page_count}")
            print(f"URL: {current_url}")
            print('=' * 60)

            response = self.get_page(current_url)
            if not response:
                print(f"Failed to fetch search page: {current_url}")
                break

            article_urls = self.extract_search_results(response.text, debug=(debug and page_count == 1))
            print(f"Found {len(article_urls)} threads on this page")

            for i, article_url in enumerate(article_urls, 1):
                # Crawl the entire thread (all pages)
                self.crawl_thread(article_url, debug=debug)
                # Small delay between threads
                time.sleep(1) 

            if max_pages and page_count >= max_pages:
                print(f"\nReached maximum pages limit: {max_pages}")
                break

            # Find next search result page
            soup = BeautifulSoup(response.text, 'html.parser')
            next_link = soup.select_one('a.pageNav-jump--next')
            if next_link and next_link.has_attr('href'):
                current_url = urljoin(self.base_url, next_link['href'])
                print(f"\nMoving to next search results page...")
                time.sleep(2)
            else:
                print("\nNo more search result pages found")
                break

        print(f"\n{'=' * 60}")
        print(f"Crawling complete!")
        print('=' * 60)


if __name__ == "__main__":
    # Configuration
    OUTPUT_FILE = 'otofun_data/week5/otofun_progress_manual_xedienmoitruonghcm.csv'
    
    crawler = OtofunCrawlerV2(auto_save_file=OUTPUT_FILE)

    # Example Search URL
    # Valid as of 2024
    search_url = "https://www.otofun.net/search/21444128/?q=xe+%C4%91i%E1%BB%87n+m%C3%B4i+tr%C6%B0%E1%BB%9Dng+HCM&o=date" # "https://www.otofun.net/search/21441817/?q=xe+m%C3%A1y+%C4%91i%E1%BB%87n+ch%E1%BA%A1y+giao+h%C3%A0ng&o=date" # "https://www.otofun.net/search/21362904/?q=chuy%E1%BB%83n+%C4%91%E1%BB%95i+xanh+HCM&o=date"
    
    print(f"Starting crawl...")
    print(f"Output file: {OUTPUT_FILE}")
    
    crawler.crawl_search_results(search_url, max_pages=1000, debug=True)
