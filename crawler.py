from collections import deque
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from database import save_page

def crawl(conn, start_url, max_pages):
    queue = deque([start_url])
    visited = set()

    pages_crawled = 0
    while queue and pages_crawled < max_pages:
        url = queue.popleft()
        if url in visited:
            continue

        try:
            response = requests.get(url, timeout=5)
        except requests.RequestException:
            continue

        final_url = response.url
        if final_url in visited:
            continue

        soup = BeautifulSoup(response.content, 'html.parser')

        title = soup.title.text if soup.title else ''
        content = soup.get_text(' ', strip=True)

        save_page(conn, final_url, title, content, response.status_code)

        visited.add(final_url)
        pages_crawled += 1

        for tag in soup.find_all('a', href=True):
            href = tag['href']
            new_url = urljoin(final_url, href)
            queue.append(new_url)