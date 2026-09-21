from collections import deque
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag, urlsplit

from database import save_page, save_link, load_pages, load_frontier, add_to_frontier, remove_from_frontier
from politeness import USER_AGENT, is_allowed, wait_if_needed

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

def mark_skipped(reason, url, url_to_id):
    print(f'\n{reason}: {url}')
    url_to_id[url] = None

def crawl(conn, start_url, max_pages, commit_every=50):
    if skip_url(start_url):
        print(f'Invalid start URL: {start_url}')
        return

    url_to_id = load_pages(conn)
    frontier_rows = load_frontier(conn)
    robots_cache = {} # domain -> RobotFileParser object
    last_request_at = {} # domain -> monotonic timestamp

    if not frontier_rows and start_url not in url_to_id:
        frontier_id = add_to_frontier(conn, None, start_url)
        conn.commit()
        frontier_rows = [(frontier_id, None, start_url)]

    queue = deque(frontier_rows)

    with requests.Session() as session:
        session.headers.update({'User-Agent': USER_AGENT})

        pages_crawled = 0
        while queue and pages_crawled < max_pages:
            frontier_id, source_id, target_url = queue.popleft()

            # Every path below is terminal for this row
            remove_from_frontier(conn, frontier_id)

            # Check if URL is visited
            if try_save_link_if_visited(conn, url_to_id, source_id, target_url):
                continue

            # Re-check robots.txt: this row may be the seed URL (never
            # discovery-checked) or resumed from a past run whose robots.txt
            # verdict is stale, since neither is recorded durably.
            if not is_allowed(session, robots_cache, last_request_at, target_url):
                mark_skipped('Disallowed by robots.txt', target_url, url_to_id)
                continue

            # Wait between requests of the same domain
            wait_if_needed(robots_cache, last_request_at, target_url)

            # Fetches HTTP response headers and opens connection.
            # Defers downloading the response body.
            try:
                response = session.get(target_url, timeout=5, stream=True)
            except requests.RequestException:
                mark_skipped('Failed to reach page', target_url, url_to_id)
                continue

            with response:
                final_url = response.url

                # Check if final URL is visited
                if try_save_link_if_visited(conn, url_to_id, source_id, final_url):
                    continue

                # Skip non-successful response codes
                if not response.ok:
                    mark_skipped(f'Non-2xx response ({response.status_code})', final_url, url_to_id)
                    continue

                # final_url was never separately discovery-checked (a redirect
                # target isn't a discovered link) and can be on a different path
                # or domain than target_url with different robots.txt rules.
                if not is_allowed(session, robots_cache, last_request_at, final_url):
                    mark_skipped('Disallowed by robots.txt', final_url, url_to_id)
                    continue

                # Skip non-page responses
                content_type = response.headers.get('Content-Type', '').lower()
                if not (
                    content_type.startswith('text/html') or
                    content_type.startswith('text/plain')
                ):
                    mark_skipped(f'Skipping non-page response ({content_type})', final_url, url_to_id)
                    continue

                # Skip early if server tells us size
                content_length = response.headers.get('Content-Length')
                if content_length and int(content_length) > MAX_RESPONSE_SIZE:
                    mark_skipped('Skipping large response', final_url, url_to_id)
                    continue

                # Downloads the response body
                try:
                    body = response.content
                except requests.RequestException:
                    mark_skipped('Failed to download body', final_url, url_to_id)
                    continue

            # Skip responses with large bodies
            # (Content-Length given by server was incorrect or omitted)
            if len(body) > MAX_RESPONSE_SIZE:
                mark_skipped('Skipping large response', final_url, url_to_id)
                continue

            # Parsing
            title, content, children = parse_page(body, final_url)

            page_id = save_page(conn, final_url, title, content, response.status_code)
            url_to_id[final_url] = page_id

            if source_id is not None:
                save_link(conn, source_id, page_id)

            pages_crawled += 1
            # Ensures 'pages' and 'links' tables are synced and prevents
            # loss of progress if crawler functions terminates early
            if pages_crawled % commit_every == 0:
                # TODO: Add batch insert and remove frontier (and links?) instead of doing it every loop iteration
                conn.commit()

            print(f'\rCrawling page {pages_crawled}/{max_pages} ({final_url})'.ljust(120), end='', flush=True)

            # Checking "children" links on the page
            for new_url in children:
                # Hygiene filter, not the real gate (see the target_url check
                # above): keeps known-disallowed URLs out of frontier/DB.
                # Not durably recorded, so a rejection here isn't final - if
                # rediscovered later via another page, it's re-evaluated fresh.
                if not is_allowed(session, robots_cache, last_request_at, new_url):
                    continue

                new_frontier_id = add_to_frontier(conn, page_id, new_url)
                queue.append((new_frontier_id, page_id, new_url))

    conn.commit() # Commit any remaining uncommitted pages after loop finish

def parse_page(html_body, url):
    '''Returns title, content, and accepted children links
    (skip_url -> False, robots.txt check happens where the function is called).'''
    soup = BeautifulSoup(html_body, 'lxml')
    title = soup.title.text if soup.title else ''
    content = soup.get_text(' ', strip=True)

    children = []
    for tag in soup.find_all('a', href=True):
        href = str(tag['href'])
        try:
            new_url = urljoin(url, href)
            new_url, _ = urldefrag(new_url)
        except ValueError:
            continue

        if skip_url(new_url):
            continue
        children.append(new_url)

    return title, content, children
