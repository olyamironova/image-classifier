import argparse
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

BASE_URL = "https://www.wga.hu/"
OUTPUT_DIR = Path("./wga_out")
PROFESSIONS = "painter,engraver,printmaker,draughtsman"
MIN_PER_CLASS = 1000
MAX_PER_CLASS = 1200
MAX_PAGES_ARTIST_LIST = 500 # limit pages of artist.cgi
MAX_INDEX_PAGES_PER_ARTIST = 200 # BFS limit for index pages inside artist folder
DELAY = 0.5

PROF_KEYWORDS = [
    "painter", "sculptor", "architect", "engraver", "printmaker",
    "draughtsman", "illuminator", "miniaturist", "potter", "goldsmith"
]

def to_lowercase_identifier(s):
    s = ("" if s is None else str(s)).strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s or "unknown"

def unwrap_wga_frames(url):
    u = (url or "").strip()
    if "/frames" in u and "?" in u:
        inner = u.split("?", 1)[1]
        if inner.startswith("//"):
            inner = inner[1:]  # "//html/.." -> "/html/.."
        if not inner.startswith("/"):
            inner = "/" + inner
        return urljoin(BASE_URL, inner)
    return urljoin(BASE_URL, u)


def is_html_work_page(u):
    u = (u or "").lower()
    return ("/html/" in u) and u.endswith(".html") and (not u.endswith("/index.html")) and ("frames" not in u)


def is_html_index_page(u):
    u = (u or "").lower()
    return ("/html/" in u) and u.endswith("/index.html") and ("frames" not in u)


def ext_from_url(u):
    path = urlparse(u).path.lower()
    for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]:
        if path.endswith(ext):
            return ext
    return ".jpg"


def same_artist_folder(artist_index_url, candidate_url):
    a = urlparse(artist_index_url).path
    c = urlparse(candidate_url).path
    # /html/a/aagaard/index.html -> /html/a/aagaard/
    artist_dir = a.rsplit("/", 1)[0] + "/"
    return c.startswith(artist_dir)

@dataclass
class Fetcher:
    delay = 0.5
    timeout = 60
    session = None

    def __post_init__(self):
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 (dataset research)"
            })

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=20))
    def get_text(self, url):
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        time.sleep(self.delay)
        return r.text

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def get_bytes(self, url):
        r = self.session.get(url, timeout=max(self.timeout, 60))
        r.raise_for_status()
        time.sleep(self.delay)
        return r.content



def extract_profession_from_school(school_text):
    t = (school_text or "").lower()
    for p in PROF_KEYWORDS:
        if re.search(rf"\b{re.escape(p)}\b", t):
            return p
    return "unknown"


def parse_artist_cgi_page(html):
    soup = BeautifulSoup(html, "html.parser")
    out = []

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td", class_="ARTISTLIST")
        if len(tds) != 4:
            continue

        a = tds[0].find("a", href=True)
        if not a:
            continue

        artist_name = a.get_text(" ", strip=True)
        artist_url = unwrap_wga_frames(a["href"])
        born_died = tds[1].get_text(" ", strip=True)
        period = tds[2].get_text(" ", strip=True) # painting movement (e.g. Baroque, classicism, romanticism, etc.)
        school = tds[3].get_text(" ", strip=True) # profession (e.g. German painter)

        if not period.strip():
            continue

        out.append({
            "artist_name": artist_name,
            "artist_url": artist_url,
            "born_died": born_died,
            "movement_raw": period,
            "school_raw": school,
            "profession": extract_profession_from_school(school),
        })

    uniq = {}
    for r in out:
        uniq[r["artist_url"]] = r
    return list(uniq.values())


def collect_all_artists(fetcher, only_professions, max_pages, step=50):
    all_rows = []
    offset = 0
    page_i = 0

    while True:
        url = (f"{BASE_URL}cgi-bin/artist.cgi?"
               f"Profession=any&School=any&Period=any&Time-line=any&"
               f"from={offset}&max={step}&Sort=Name&letter=-&width=700&targetleft=0")

        html = fetcher.get_text(url)
        rows = parse_artist_cgi_page(html)

        if not rows:
            break

        if only_professions:
            rows = [r for r in rows if r["profession"] in only_professions]

        all_rows.extend(rows)

        offset += step
        page_i += 1
        if max_pages and page_i >= max_pages:
            break

    uniq = {}
    for r in all_rows:
        uniq[r["artist_url"]] = r
    return list(uniq.values())


def collect_pages_within_artist(fetcher, artist_index_url, max_index_pages=50):
    # BFS by index.html and the sub-indexes inside the artist page
    artist_index_url = unwrap_wga_frames(artist_index_url)
    queue = [artist_index_url]
    seen = set()
    work_pages = set()

    while queue and len(seen) < max_index_pages:
        url = queue.pop(0)
        url = unwrap_wga_frames(url)
        if url in seen:
            continue
        seen.add(url)

        try:
            html = fetcher.get_text(url)
        except Exception:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = unwrap_wga_frames(urljoin(url, a["href"]))

            if not same_artist_folder(artist_index_url, href):
                continue

            if is_html_work_page(href):
                work_pages.add(href)
            elif is_html_index_page(href):
                if href not in seen:
                    queue.append(href)

    return sorted(work_pages), sorted(seen)


def _extract_meta_refresh_url(base_url, soup):
    meta = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
    if not meta:
        return None
    content = meta.get("content", "") or ""
    m = re.search(r"url\s*=\s*(.+)$", content, flags=re.I)
    if not m:
        return None
    u = m.group(1).strip().strip("'\"")
    return urljoin(base_url, u)


def _extract_main_frame_url(base_url, soup, html):
    fr = soup.find("frame", attrs={"name": re.compile(r"main", re.I)})
    if fr and fr.get("src"):
        return urljoin(base_url, fr["src"])

    m = re.search(r'<frame[^>]+name\s*=\s*["\']?MAIN["\']?[^>]*src\s*=\s*["\']([^"\']+)["\']', html, flags=re.I)
    if m:
        return urljoin(base_url, m.group(1))

    return None


def _extract_image_url(base_url, soup, html):
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        if "/art/" in src or src.startswith("art/") or src.startswith("../art/") or src.startswith("../../art/"):
            return urljoin(base_url, src)

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/art/" in href and re.search(r"\.(jpe?g|png|gif|webp)(\?|$)", href, flags=re.I):
            return urljoin(base_url, href)

    m = re.search(r'(/art/[^"\']+\.(?:jpe?g|png|gif|webp))', html, flags=re.I)
    if m:
        return urljoin(base_url, m.group(1))

    return None


def parse_artwork_page(fetcher, work_url):
    work_url = unwrap_wga_frames(work_url)

    for _ in range(3):
        html = fetcher.get_text(work_url)
        soup = BeautifulSoup(html, "html.parser")

        refresh_url = _extract_meta_refresh_url(work_url, soup)
        if refresh_url:
            new_url = unwrap_wga_frames(refresh_url)
            if new_url != work_url:
                work_url = new_url
                continue

        main_url = _extract_main_frame_url(work_url, soup, html)
        if main_url:
            new_url = unwrap_wga_frames(main_url)
            if new_url != work_url:
                work_url = new_url
                continue

        break

    html = fetcher.get_text(work_url)
    soup = BeautifulSoup(html, "html.parser")

    img_url = _extract_image_url(work_url, soup, html)
    if not img_url:
        return None

    meta = {}
    for tr in soup.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) >= 2:
            k = tds[0].get_text(" ", strip=True).rstrip(":")
            v = tds[1].get_text(" ", strip=True)
            if k and v and len(k) <= 60:
                meta[k] = v

    title = meta.get("Title", "") or meta.get("TITLE", "")
    date = meta.get("Date", "") or meta.get("DATE", "")

    return {
        "work_url": work_url,
        "image_url": img_url,
        "title": title,
        "date": date,
    }

def build_dataset(out_dir,
                  only_professions,
                  min_per_class,
                  max_per_class,
                  max_index_pages_per_artist,
                  max_pages_artist_list,
                  sleep):

    out_dir.mkdir(parents=True, exist_ok=True)
    img_root = out_dir / "images"
    img_root.mkdir(parents=True, exist_ok=True)

    fetcher = Fetcher(sleep=sleep)

    print("Collecting artists")
    artists = collect_all_artists(fetcher,
                                  only_professions=only_professions,
                                  max_pages=max_pages_artist_list)

    print(f"Collected {len(artists)} artists")

    rows = []
    counts = {}
    seen_image_urls = set()
    sample_id = 0

    total_artists = len(artists)

    for ai, a in enumerate(tqdm(artists, desc="Artists"), start=1):
        movement = a["movement_raw"].strip()
        movement_id = to_lowercase_identifier(movement)

        if movement_id == "unknown":
            print(f"[{ai}/{total_artists}] skip: {a['artist_name']} (unknown movement)")
            continue

        if counts.get(movement_id, 0) >= max_per_class:
            print(f"[{ai}/{total_artists}] SKIP: {a['artist_name']} "
                  f"(class '{movement_id}' is full: {counts.get(movement_id, 0)}/{max_per_class})")
            continue

        artist_url = unwrap_wga_frames(a["artist_url"])
        if not artist_url.lower().endswith("/index.html"):
            if artist_url.endswith("/"):
                artist_url += "index.html"
            else:
                artist_url = artist_url.rstrip("/") + "/index.html"

        print(f"[{ai}/{total_artists}] artist: {a['artist_name']} | prof={a['profession']} | "
              f"movement='{movement}' | class_count={counts.get(movement_id, 0)}/{max_per_class}")
        print(f"[{ai}/{total_artists}] index: {artist_url}")

        t0 = time.time()
        try:
            work_pages, seen_indexes = collect_pages_within_artist(
                fetcher, artist_url, max_index_pages=max_index_pages_per_artist
            )
        except Exception as e:
            print(f"[{ai}/{total_artists}] error: failed to collect pages: {e}")
            continue

        print(f"[{ai}/{total_artists}] pages: index_seen={len(seen_indexes)} | works_found={len(work_pages)} | time={time.time()-t0:.1f}s")

        artist_downloaded = 0
        artist_noimg = 0
        artist_dup = 0
        artist_errors = 0

        for wi, wp in enumerate(work_pages, start=1):
            if counts.get(movement_id, 0) >= max_per_class:
                print(f"[{ai}/{total_artists}] stop: class '{movement_id}' reached max {max_per_class}")
                break

            try:
                art = parse_artwork_page(fetcher, wp)
                if not art:
                    artist_noimg += 1
                    if artist_noimg <= 3:
                        print(f"[{ai}/{total_artists}] no image on: {wp}")
                    elif artist_noimg % 25 == 0:
                        print(f"[{ai}/{total_artists}] no-image pages so far: {artist_noimg}")
                    continue

                if art["image_url"] in seen_image_urls:
                    artist_dup += 1
                    if artist_dup <= 3:
                        print(f"[{ai}/{total_artists}] duplicate image: {art['image_url']}")
                    elif artist_dup % 25 == 0:
                        print(f"[{ai}/{total_artists}] duplicates so far: {artist_dup}")
                    continue
                seen_image_urls.add(art["image_url"])

                ext = ext_from_url(art["image_url"])
                cls_dir = img_root / movement_id
                cls_dir.mkdir(parents=True, exist_ok=True)

                img_name = f"wga_{sample_id:09d}{ext}"
                local_path = cls_dir / img_name

                if wi == 1 or wi % 20 == 0:
                    print(f"[{ai}/{total_artists}] scanning works: {wi}/{len(work_pages)} "
                          f"(downloaded={artist_downloaded}, class={counts.get(movement_id, 0)}/{max_per_class})")

                if not local_path.exists():
                    data = fetcher.get_bytes(art["image_url"])
                    local_path.write_bytes(data)

                rows.append({
                    "id": f"wga_{sample_id:09d}",
                    "movement": movement,
                    "movement_id": movement_id,
                    "profession": a["profession"],
                    "school_raw": a["school_raw"],
                    "artist_name": a["artist_name"],
                    "artist_url": artist_url,
                    "work_url": art["work_url"],
                    "image_url": art["image_url"],
                    "title": art.get("title", ""),
                    "date": art.get("date", ""),
                    "local_path": str(local_path),
                })

                artist_downloaded += 1
                sample_id += 1
                counts[movement_id] = counts.get(movement_id, 0) + 1

                print(f"[{ai}/{total_artists}] downloaded {artist_downloaded} | "
                      f"class={counts.get(movement_id, 0)}/{max_per_class} | "
                      f"title='{art.get('title','')[:80]}' | file={local_path.name}")

            except Exception as e:
                artist_errors += 1
                if artist_errors <= 5:
                    print(f"[{ai}/{total_artists}] error at page: {wp} | {e}")
                elif artist_errors % 50 == 0:
                    print(f"[{ai}/{total_artists}] errors so far: {artist_errors}")
                continue

        print(f"[{ai}/{total_artists}] done: {a['artist_name']} | "
              f"downloaded={artist_downloaded} | noimg={artist_noimg} | dup={artist_dup} | errors={artist_errors}\n")

    df = pd.DataFrame(rows)

    if not df.empty and min_per_class > 1:
        vc = df["movement_id"].value_counts()
        keep = set(vc[vc >= min_per_class].index)
        drop = set(vc.index) - keep

        if drop:
            for slug in drop:
                cls_dir = img_root / slug
                if cls_dir.exists():
                    for p in cls_dir.glob("*"):
                        try:
                            p.unlink()
                        except Exception:
                            pass
                    try:
                        cls_dir.rmdir()
                    except Exception:
                        pass

            df = df[df["movement_id"].isin(keep)].reset_index(drop=True)

    return df

def main():
    only_professions = {x.strip().lower() for x in PROFESSIONS.split(",") if x.strip()}
    if only_professions:
        only_professions = {x for x in only_professions}

    df = build_dataset(
        out_dir=OUTPUT_DIR,
        only_professions=only_professions,
        min_per_class=MIN_PER_CLASS,
        max_per_class=MAX_PER_CLASS,
        max_pages_artist_list=MAX_PAGES_ARTIST_LIST,
        max_index_pages_per_artist=MAX_INDEX_PAGES_PER_ARTIST,
        sleep=DELAY,
    )

    print(f"Collecting painting is completed, saved {len(df)} paintings")
    if not df.empty:
        print("Stats:")
        print(df["movement_id"].value_counts().head(20).to_string())

if __name__ == "__main__":
    main()