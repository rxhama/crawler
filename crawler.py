from collections import deque
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag

from database import save_page, save_link

MAX_RESPONSE_SIZE = 5 * 1024 * 1024 # 5 MB

def crawl(conn, start_url, max_pages):
    queue = deque([(None, start_url)])
    visited = set()

    pages_crawled = 0
    while queue and pages_crawled < max_pages:
        source_url, target_url = queue.popleft()
        if target_url in visited:
            continue

        try:
            response = requests.get(target_url, timeout=5)
        except requests.RequestException:
            print(f'Failed to reach page: {target_url}')
            continue

        final_url = response.url
        if final_url in visited:
            continue

        # Skip non-page responses
        content_type = response.headers.get('Content-Type', '').lower()
        if not (
            content_type.startswith('text/html') or
            content_type.startswith('text/plain')
        ):
            print(f'Skipping non-page response ({content_type}): {final_url}')
            continue

        # tsvector takes maximum of 1048575 bytes
        if len(response.content) > MAX_RESPONSE_SIZE:
            print(f'Skipping large response: {final_url}')
            continue


        soup = BeautifulSoup(response.content, 'html.parser')

        title = soup.title.text.replace('\x00', '') if soup.title else ''
        content = soup.get_text(' ', strip=True).replace('\x00', '')

        save_page(conn, final_url, title, content, response.status_code)
        if source_url is not None:
            save_link(conn, source_url, final_url)

        # Ensures 'pages' and 'links' tables are synced and prevents
        # loss of progress if crawler functions terminates early
        conn.commit()

        visited.add(final_url)
        pages_crawled += 1

        for tag in soup.find_all('a', href=True):
            href = tag['href']
            new_url = urljoin(final_url, href)
            new_url, _ = urldefrag(new_url)
            queue.append((final_url, new_url))