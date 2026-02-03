import re
import time
import random
from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from tqdm import tqdm

BASE = "https://www.tate.org.uk"
START_PAGE = 1
END_PAGE = 150
OUT_DIR = Path("tate_images")
DELAY = 0.5

def build_collection_url(page=1):
    params = [
        "attributes=img",
        "classification=6", # (paintings)
        "era=4", # 20th century 1900-1945
        "era=5", # 20th century post-1945
        "era=6", # 21st century
        "tab=collection",
        f"page={page}"
    ]
    return f"{BASE}/collection?{'&'.join(params)}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en;q=0.9,ru;q=0.8",
    "Referer": "https://www.tate.org.uk/collection",
    "Cache-Control": "no-cache",
}

IMG_EXT_ALLOW = {".jpg", ".jpeg", ".png", ".webp"}


def slugify(s, max_len=80):
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    s = s[:max_len]
    return s or "item"


def session_with_headers():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def get_selenium_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(f"user-agent={HEADERS["User-Agent"]}")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def get_html_selenium(url, driver, wait_time=15, scroll_pause=2):
    try:
        driver.get(url)
        
        try:
            WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/art/artworks/']"))
            )
        except TimeoutException:
            WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".grid-card, .collection-card, article"))
            )
        
        last_height = driver.execute_script("return document.body.scrollHeight")
        
        for i in range(8):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause)
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        return driver.page_source
    except TimeoutException:
        print(f"Timeout until loading {url}")
        return driver.page_source
    except Exception as e:
        print(f"Failed to load {url}: {e}")
        return None


def get_html(url, sess, retries=3, timeout=20, delay=0.8):
    for i in range(retries):
        try:
            r = sess.get(url, timeout=timeout)
            if r.status_code == 200 and r.text:
                time.sleep(delay + random.uniform(0, 0.4))
                return r.text
        except Exception as e:
            print(f"Attempt{i+1}/{retries} to get html page is failed for url {url}: {e}")
        time.sleep(0.5 * (i + 1))
    return None


def get_bytes(url, sess, retries=3, timeout=30, delay=0.5):
    for i in range(retries):
        try:
            r = sess.get(url, timeout=timeout, stream=True)
            if r.status_code == 200:
                data = r.content
                time.sleep(delay + random.uniform(0, 0.3))
                return data, r.headers.get("Content-Type", "")
        except Exception as e:
            if i == retries - 1:
                print(f"Failed to load image: {e}")
        time.sleep(0.5 * (i + 1))
    return None, None


def parse_srcset_max(srcset):
    if not srcset:
        return None
    best_url, best_w = None, -1
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        toks = part.split()
        u = toks[0]
        w = 0
        if len(toks) >= 2 and toks[1].endswith("w"):
            try:
                w = int(toks[1][:-1])
            except ValueError:
                w = 0
        if w >= best_w:
            best_w = w
            best_url = u
    return best_url


def extract_card_info(card_tag, base_url=BASE):
    link = None
    img_url = None
    title = None
    artist = None
    
    a = card_tag.find("a", href=True)
    if a:
        href = a.get("href")
        if href:
            if href.startswith("/"):
                link = urljoin(base_url, href)
            elif href.startswith("http"):
                link = href
        
        title = a.get("aria-label") or a.get("title")
        if not title:
            ttl = a.find(attrs={"class": re.compile(r"(title|card__title|grid-card__title)", re.I)})
            if ttl:
                title = ttl.get_text(strip=True)
            else:
                text = a.get_text(strip=True)
                if text and len(text) > 3:
                    title = text
    
    art_el = (card_tag.find(attrs={"class": re.compile(r"(artist|creator|meta__artist|author|card__meta)", re.I)}) or
              card_tag.find("p", attrs={"class": re.compile(r"meta", re.I)}))
    if art_el:
        artist = art_el.get_text(" ", strip=True)
    
    img = card_tag.find("img")
    if img:
        img_url = parse_srcset_max(img.get("data-srcset") or img.get("srcset") or "")
        if not img_url:
            img_url = img.get("data-src") or img.get("src")
        if img_url:
            if img_url.startswith("//"):
                img_url = "https:" + img_url
            elif img_url.startswith("/"):
                img_url = urljoin(base_url, img_url)
    
    return {
        "detail_url": link,
        "title": title,
        "artist": artist,
        "thumb_url": img_url,
    }


def parse_list_page(html, base_url=BASE):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    
    links = soup.select('a[href*="/art/artworks/"]')
    
    print(f"Found {len(links)} links on artworks")
    
    seen_urls = set()
    
    for a in links:
        href = a.get("href")
        if not href:
            continue
            
        if href.startswith("/"):
            detail_url = urljoin(base_url, href)
        elif href.startswith("http"):
            detail_url = href
        else:
            continue
        
        if detail_url in seen_urls:
            continue
        seen_urls.add(detail_url)
        
        card = (a.find_parent(attrs={"class": re.compile(r"(card|grid|item|artwork)", re.I)}) or
                a.find_parent("article") or
                a.find_parent("li") or
                a)
        
        info = extract_card_info(card, base_url=base_url)
        
        if info.get("detail_url"):
            items.append(info)
    
    return items


def parse_artwork_image_url(html, base_url=BASE):
    soup = BeautifulSoup(html, "html.parser")
    
    meta = soup.find("meta", property="og:image")
    if meta and meta.get("content"):
        u = meta["content"].strip()
        if u.startswith("//"):
            return "https:" + u
        if u.startswith("/"):
            return urljoin(base_url, u)
        return u
    
    img = soup.select_one("figure img, .artwork__image img, .image img, [data-role='artwork-image'] img")
    if img:
        u = parse_srcset_max(img.get("srcset") or "") or img.get("src") or img.get("data-src")
        if u:
            if u.startswith("//"):
                return "https:" + u
            if u.startswith("/"):
                return urljoin(base_url, u)
            return u
    
    return None


def guess_ext_from_url_or_ct(url, content_type):
    if url:
        path = urlparse(url).path
        m = re.search(r"\.(jpg|jpeg|png|webp)(?:$|\?)", path, re.I)
        if m:
            ext = "." + m.group(1).lower()
            return ".jpeg" if ext == ".jpg" else ext
    
    if content_type:
        ct = content_type.lower()
        if "jpeg" in ct or "jpg" in ct:
            return ".jpeg"
        if "png" in ct:
            return ".png"
        if "webp" in ct:
            return ".webp"
    
    return ".jpeg"


def extract_work_id(detail_url):
    m = re.search(r"/art/artworks/([^/?#]+)", detail_url)
    return m.group(1) if m else None


def download_image(image_url, out_dir, base_name, sess, delay=0.5):
    data, ct = get_bytes(image_url, sess=sess, delay=delay)
    if not data:
        return None
    
    ext = guess_ext_from_url_or_ct(image_url, ct)
    if ext.lower() not in IMG_EXT_ALLOW:
        ext = ".jpeg"
    
    fname = f"{base_name}{ext}"
    path = out_dir / fname
    
    if path.exists() and path.stat().st_size > 0:
        return path
    
    try:
        path.write_bytes(data)
        return path
    except Exception as e:
        print(f"Failed to save file {fname}: {e}")
        return None

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    sess = session_with_headers()
    rows = []
    seen_detail_urls = set()
    
    all_items = []
    
    print("Initializing Selenium")
    driver = get_selenium_driver()
    
    print(f"Start collecting paintings")
    
    try:
        for page in range(START_PAGE, END_PAGE + 1):
            url = build_collection_url(page=page)
            print(f"\nProcessing page {page}: {url}")
            
            html = get_html_selenium(url, driver)
            
            if not html:
                print(f"Failed to load page {page}")
                continue
            
            items = parse_list_page(html, base_url=BASE)
            if not items:
                print(f"No data at page {page}")
                break
            
            new_items = 0
            for it in items:
                if not it.get("detail_url") or it["detail_url"] in seen_detail_urls:
                    continue
                seen_detail_urls.add(it["detail_url"])
                all_items.append(it)
                new_items += 1
            
            print(f"Found {len(items)} artworks at page {page} ({new_items} are new)")
            print(f"Total unic paintings collected: {len(all_items)}")
            
            if page < END_PAGE:
                time.sleep(DELAY + random.uniform(0.5, 1.5))
        
        print(f"Total unic paintings collected: {len(all_items)}")
        
        if not all_items:
            print("Failed to find any artwork")
            return
        
        for idx, it in enumerate(tqdm(all_items, desc="Loading paintings", unit="item"), 1):
            detail_url = it.get("detail_url")
            if not detail_url:
                continue
            
            try:
                detail_html = get_html(detail_url, sess=sess, delay=DELAY)
                if not detail_html:
                    print(f"[{idx}/{len(all_items)}] Failed to load artwork page {detail_url}")
                    continue
                
                image_url = parse_artwork_image_url(detail_html, base_url=BASE) or it.get("thumb_url")
                if not image_url:
                    print(f"[{idx}/{len(all_items)}] Not found image for url {detail_url}")
                    continue
                
                work_id = extract_work_id(detail_url) or ""
                title = (it.get("title") or "").strip()
                artist = (it.get("artist") or "").strip()
                
                base_name_parts = []
                if work_id:
                    base_name_parts.append(work_id)
                if title:
                    base_name_parts.append(slugify(title, max_len=40))
                elif artist:
                    base_name_parts.append(slugify(artist, max_len=40))
                else:
                    base_name_parts.append("artwork")
                
                base_name = "-".join([p for p in base_name_parts if p])
                
                img_path = download_image(image_url, out_dir=OUT_DIR, base_name=base_name, sess=sess, delay=DELAY)
                
                rows.append({
                    "id": work_id,
                    "title": title,
                    "artist": artist,
                    "detail_url": detail_url,
                    "image_url": image_url,
                    "image_path": str(img_path.relative_to(Path.cwd())) if img_path else "",
                    "thumb_url": it.get("thumb_url") or "",
                })
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error [{idx}/{len(all_items)}] {detail_url}: {e}")
        
        if rows:
            print(f"Saved {len(rows)} records")
            print(f"Folder directory with images: {OUT_DIR.resolve()}")
        else:
            print("No data to save")
    
    except KeyboardInterrupt:
        print("Forced stopped")
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    main()