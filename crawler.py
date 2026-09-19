from collections import deque
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag, urlsplit

from database import save_page, save_link, load_pages, load_frontier, add_to_frontier, remove_from_frontier

MAX_RESPONSE_SIZE = 5 * 1024 * 1024 # 5 MB
SKIP_EXTENSIONS = {'.tgz', '.tar.xz', '.tar.gz', '.zip', '.pdf', '.whl', '.exe', '.dmg'}

def skip_url(url):
    parsed = urlsplit(url)

    if parsed.scheme not in ('http', 'https'):
        return True

    path = parsed.path.lower()
    if any(path.endswith(ext) for ext in SKIP_EXTENSIONS):
        return True

    return False

def try_save_link_if_visited(conn, url_to_id, source_id, url):
    '''Returns True if url was already visited (and link saved if applicable)'''
    if url not in url_to_id:
        return False
    target_id = url_to_id[url]
    if target_id is not None and source_id is not None:
        save_link(conn, source_id, target_id)
    return True

def crawl(conn, start_url, max_pages, commit_every=50):
    if skip_url(start_url):
        print(f'Invalid start URL: {start_url}')
        return

    url_to_id = load_pages(conn)
    frontier_rows = load_frontier(conn)

    if not frontier_rows and start_url not in url_to_id:
        frontier_id = add_to_frontier(conn, None, start_url)
        conn.commit()
        frontier_rows = [(frontier_id, None, start_url)]

    queue = deque(frontier_rows)

    with requests.Session() as session:
        pages_crawled = 0
        while queue and pages_crawled < max_pages:
            frontier_id, source_id, target_url = queue.popleft()

            if try_save_link_if_visited(conn, url_to_id, source_id, target_url):
                remove_from_frontier(conn, frontier_id)
                continue

            try:
                response = session.get(target_url, timeout=5, stream=True)
            except requests.RequestException:
                print(f'\nFailed to reach page: {target_url}')
                url_to_id[target_url] = None
                remove_from_frontier(conn, frontier_id)
                continue

            final_url = response.url
            if try_save_link_if_visited(conn, url_to_id, source_id, final_url):
                response.close()
                remove_from_frontier(conn, frontier_id)
                continue

            # Skip non-page responses
            content_type = response.headers.get('Content-Type', '').lower()
            if not (
                content_type.startswith('text/html') or
                content_type.startswith('text/plain')
            ):
                print(f'\nSkipping non-page response ({content_type}): {final_url}')
                url_to_id[final_url] = None
                remove_from_frontier(conn, frontier_id)
                response.close()
                continue

            # Skip early if server tells us size
            content_length = response.headers.get('Content-Length')
            if content_length and int(content_length) > MAX_RESPONSE_SIZE:
                print(f'\nSkipping large response: {final_url}')
                url_to_id[final_url] = None
                remove_from_frontier(conn, frontier_id)
                response.close()
                continue

            try:
                body = response.content
            except requests.RequestException:
                print(f'\nFailed to download body: {final_url}')
                url_to_id[final_url] = None
                remove_from_frontier(conn, frontier_id)
                response.close()
                continue

            # tsvector takes maximum of 1048575 bytes
            if len(body) > MAX_RESPONSE_SIZE:
                print(f'\nSkipping large response: {final_url}')
                url_to_id[final_url] = None
                remove_from_frontier(conn, frontier_id)
                continue

            soup = BeautifulSoup(body, 'html.parser')
            title = soup.title.text.replace('\x00', '') if soup.title else ''
            content = soup.get_text(' ', strip=True).replace('\x00', '')

            page_id = save_page(conn, final_url, title, content, response.status_code)
            url_to_id[final_url] = page_id

            if source_id is not None:
                save_link(conn, source_id, page_id)

            remove_from_frontier(conn, frontier_id)

            pages_crawled += 1
            # Ensures 'pages' and 'links' tables are synced and prevents
            # loss of progress if crawler functions terminates early
            if pages_crawled % commit_every == 0:
                conn.commit()

            print(f'\rCrawling page {pages_crawled}/{max_pages} ({final_url})'.ljust(120), end='', flush=True)

            for tag in soup.find_all('a', href=True):
                href = str(tag['href'])
                new_url = urljoin(final_url, href)
                new_url, _ = urldefrag(new_url)

                if skip_url(new_url):
                    continue

                new_frontier_id = add_to_frontier(conn, page_id, new_url)
                queue.append((new_frontier_id, page_id, new_url))

    conn.commit() # Commit any remaining uncommitted pages after loop finish
