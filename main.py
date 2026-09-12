from collections import deque
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import psycopg

from database import init_db, save_page, search_pages
def main():
    with psycopg.connect(
        'dbname=crawler user=crawler password=crawler_password host=localhost port=5432'
    ) as conn:
        init_db(conn)

        queue = deque()
        visited = set()
        max_pages = 10

        # start = input('Enter start site: ').strip()
        start = 'https://www.google.com/'
        queue.append(start)

        i = 0
        while queue and i < max_pages:
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
            i += 1

            for tag in soup.find_all('a', href=True):
                href = tag['href']
                new_url = urljoin(final_url, href)
                queue.append(new_url)

        results = search_pages(conn, 'account')
        for url, title in results:
            print(title, url)

if __name__ == '__main__':
    main()