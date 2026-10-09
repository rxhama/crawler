import asyncio
import hashlib
from dataclasses import dataclass
from urllib.parse import urljoin, urldefrag, urlsplit

import httpx2
import psycopg
from selectolax.lexbor import LexborHTMLParser

from database import (
    save_page, save_link, save_links, load_pages, load_content_hashes, load_frontier, add_to_frontier, add_children_to_frontier,
    remove_from_frontier, load_allowed_hosts, add_allowed_host,
)
from politeness import USER_AGENT, fetch_robots, is_allowed, crawl_delay
from scheduler import Scheduler

MAX_RESPONSE_SIZE = 5 * 1024 * 1024 # 5 MB
SKIP_EXTENSIONS = {'.tgz', '.tar.xz', '.tar.gz', '.tar.bz2', '.zip', '.pdf', '.whl', '.exe', '.dmg', '.epub'}
NON_CONTENT_TAGS = ['script', 'style', 'noscript', 'template']
LANGUAGES = {'en'} # only pages in these languages are crawled (see other_language)

@dataclass
class CrawlState:
    '''Everything the workers share. One per crawl.'''
    conn: psycopg.Connection
    client: httpx2.AsyncClient
    scheduler: Scheduler
    robots_cache: dict # domain -> RobotFileParser | None
    url_to_id: dict
    hash_to_id: dict
    max_pages: int
    commit_every: int
    pages_crawled: int = 0

async def crawl(conn, max_pages, commit_every=50, unbounded=False, workers=10):
    url_to_id = load_pages(conn)
    hash_to_id = load_content_hashes(conn)
    rows = load_frontier(conn)

    if not rows:
        print('Frontier is empty, nothing to crawl (add seed URLs with --seeds).')
        return

    scheduler = Scheduler(load_allowed_hosts(conn), unbounded)
    for row in rows:
        frontier_id, source_id, url = row
        # If crawled in an earlier run, save the link now instead of scheduling it.
        # This also covers rows on out-of-scope domains, which would otherwise
        # stay parked even though they need no request.
        if try_save_link_if_visited(conn, url_to_id, source_id, url):
            remove_from_frontier(conn, frontier_id)
        else:
            scheduler.add(row)

    try:
        async with httpx2.AsyncClient(
            headers={'User-Agent': USER_AGENT},
            follow_redirects=True,
            timeout=5
        ) as client:
            state = CrawlState(conn, client, scheduler, {}, url_to_id, hash_to_id, max_pages, commit_every)
            async with asyncio.TaskGroup() as tg:
                for _ in range(workers):
                    tg.create_task(crawl_worker(state))
    except asyncio.CancelledError:
        # Ctrl-C cancels the crawl, so every worker stops at an await.
        # Everything written by then is complete (record_page has no await,
        # and a frontier row is only removed after its page is recorded), so keep it
        conn.commit()
        raise

    conn.commit() # Commit any remaining uncommitted pages after the workers finish

async def crawl_worker(state):
    while (job := await state.scheduler.next()) is not None:
        kind, domain, row = job
        try:
            if kind == 'robots':
                await robots_job(state, domain, row)
            else:
                await page_job(state, domain, row)
        finally:
            # Always hand the domain back: one that's never released never
            # comes back into line, and the crawl can never tell it's finished
            state.scheduler.release(domain)

async def robots_job(state, domain, row):
    '''Fetches domain's robots.txt before any of its pages. row is the domain's
    first row, only peeked at for its scheme. It stays queued for its page job.'''
    state.scheduler.record_request(domain)
    rp = await fetch_robots(state.client, row[2])
    state.robots_cache[domain] = rp
    state.scheduler.set_delay(domain, crawl_delay(rp))

async def page_job(state, domain, row):
    frontier_id, source_id, url = row
    await process_url(state, domain, source_id, url)
    # Every outcome of process_url is final for this row, so it leaves the frontier.
    # Removing it last keeps a page's save and its row's removal in the same commit;
    # if process_url raised an error, this line is never reached and the row stays.
    remove_from_frontier(state.conn, frontier_id)

async def process_url(state, domain, source_id, url):
    '''Crawls url, or returns early if it's skipped.'''
    # Already crawled, or skipped earlier this run: link saved if applicable, no request
    if try_save_link_if_visited(state.conn, state.url_to_id, source_id, url):
        return

    if not is_allowed(state.robots_cache, url):
        mark_skipped('Disallowed by robots.txt', url, state.url_to_id)
        return

    state.scheduler.record_request(domain)
    fetched = await fetch_page(state, domain, source_id, url)
    if fetched is None:
        return

    final_url, status_code, body = fetched
    record_page(state, source_id, url, final_url, status_code, body)

async def fetch_page(state, domain, source_id, url):
    '''Requests url and runs every check on the response.
    Returns (final_url, status_code, body), or None if the page was skipped.'''
    try:
        async with state.client.stream('GET', url) as response:
            final_url = str(response.url)
            final_domain = urlsplit(final_url).netloc

            if final_domain != domain:
                # Following the redirect made a request to final_domain too
                state.scheduler.record_request(final_domain)
                # A seed's final domain joins the scope even if it redirected off
                # the seed's own domain (e.g. python.org -> www.python.org),
                # otherwise every link on the page would be out of scope
                if source_id is None:
                    state.scheduler.allow_host(final_domain)
                    add_allowed_host(state.conn, final_domain)

            # Check if final URL is visited. If so, url redirected there:
            # point it at the same entry, so it isn't fetched again
            if try_save_link_if_visited(state.conn, state.url_to_id, source_id, final_url):
                state.url_to_id[url] = state.url_to_id[final_url]
                return None

            # Skip non-successful response codes
            if not response.is_success:
                mark_skipped(f'Non-2xx response ({response.status_code})', final_url, state.url_to_id)
                return None

            # Any other redirect can leave the crawl's scope
            if not state.scheduler.in_scope(final_domain):
                mark_skipped('Redirected out of scope', final_url, state.url_to_id)
                return None

            # Redirected onto a domain whose robots.txt hasn't been fetched yet:
            # queue final_url on its own domain, so the scheduler runs that
            # domain's robots job first. Costs one extra request.
            if final_domain not in state.robots_cache:
                frontier_id = add_to_frontier(state.conn, source_id, final_url)
                state.scheduler.add((frontier_id, source_id, final_url))
                return None

            # final_url was never separately discovery-checked (a redirect
            # target isn't a discovered link) and can be on a different path
            # or domain than url with different robots.txt rules.
            if not is_allowed(state.robots_cache, final_url):
                mark_skipped('Disallowed by robots.txt', final_url, state.url_to_id)
                return None

            # Skip non-page responses
            content_type = response.headers.get('Content-Type', '').lower()
            if not (
                content_type.startswith('text/html') or
                content_type.startswith('text/plain')
            ):
                mark_skipped(f'Skipping non-page response ({content_type})', final_url, state.url_to_id)
                return None

            # Page says it's in another language: skip it before downloading the body
            content_language = response.headers.get('Content-Language')
            if other_language(content_language):
                mark_skipped(f'Page in another language ({content_language})', final_url, state.url_to_id)
                return None

            # Skip early if server tells us size
            content_length = response.headers.get('Content-Length')
            if content_length and int(content_length) > MAX_RESPONSE_SIZE:
                mark_skipped('Skipping large response', final_url, state.url_to_id)
                return None

            # Download the body in chunks, stopping as soon as it passes the cap
            # (Content-Length given by server can be wrong or omitted)
            body = bytearray()
            try:
                async for chunk in response.aiter_bytes():
                    body += chunk
                    if len(body) > MAX_RESPONSE_SIZE:
                        mark_skipped('Skipping large response', final_url, state.url_to_id)
                        return None
            except httpx2.HTTPError:
                mark_skipped('Failed to download body', final_url, state.url_to_id)
                return None

            return final_url, response.status_code, bytes(body)
    except (httpx2.HTTPError, httpx2.InvalidURL):
        mark_skipped('Failed to reach page', url, state.url_to_id)
        return None

def record_page(state, source_id, url, final_url, status_code, body):
    '''Parses and saves a fetched page, and queues its links.
    A plain def, not async: no other worker can run between save_page and
    writing the page's links and frontier rows, so they always end up in the same commit.'''
    title, content, children, lang = parse_page(body, final_url)

    # Page says it's in another language (<html lang>, for sites that don't send
    # Content-Language): not saved, and its links aren't followed
    if other_language(lang):
        mark_skipped(f'Page in another language ({lang})', final_url, state.url_to_id)
        return

    # Same text as a page already saved under another URL (e.g. /3/ and
    # /3.14/ on docs.python.org): treat it like a redirect to that page,
    # so the link to it still counts and its other URLs resolve without
    # a request. Its own links aren't followed, since the saved copy has
    # the same ones - that also stops a duplicate tree from spreading.
    content_hash = hash_content(title, content)
    if content_hash in state.hash_to_id:
        state.url_to_id[url] = state.url_to_id[final_url] = state.hash_to_id[content_hash]
        try_save_link_if_visited(state.conn, state.url_to_id, source_id, final_url)
        print(f'\nDuplicate of an already-crawled page: {final_url}')
        return

    page_id = save_page(state.conn, final_url, title, content, status_code, content_hash)
    state.url_to_id[final_url] = page_id
    if content_hash is not None:
        state.hash_to_id[content_hash] = page_id

    if url != final_url:
        state.url_to_id[url] = page_id

    # The page's links and new frontier rows are collected here, then written
    # with one statement each below instead of one round trip per link
    links = [(source_id, page_id)] if source_id is not None else []
    new_children = []
    for child in children:
        # Already crawled: link to it (unless it's this page), no frontier row needed.
        # Only a real id counts: a child skipped this run (None) still gets
        # a row, so an out-of-scope one stays in frontier for a later unbounded run.
        child_id = state.url_to_id.get(child)
        if child_id is not None:
            if child_id != page_id:
                links.append((page_id, child_id))
            continue

        # Hygiene filter, not the real gate (that's in process_url):
        # keeps known-disallowed URLs out of frontier/DB. A domain whose
        # robots.txt hasn't been fetched yet passes, and is checked before
        # it's requested.
        if not is_allowed(state.robots_cache, child):
            continue

        new_children.append(child)

    save_links(state.conn, links)
    # Unlike links, these need their ids back as the scheduler's rows carry them.
    # A child missing from frontier_ids already had a row, queued by another worker
    # that recorded this same page (see add_children_to_frontier)
    frontier_ids = add_children_to_frontier(state.conn, page_id, new_children)
    for child in new_children:
        if child in frontier_ids:
            state.scheduler.add((frontier_ids[child], page_id, child))

    state.pages_crawled += 1
    # After the links and frontier rows, so a commit never splits a page from them
    if state.pages_crawled % state.commit_every == 0:
        # TODO: hold back links and frontier removals, and write them here in bulk
        state.conn.commit()
    if state.pages_crawled >= state.max_pages:
        state.scheduler.stop()

    print(f'\rCrawling page {state.pages_crawled}/{state.max_pages} ({final_url})'.ljust(120), end='', flush=True)

def skip_url(url):
    parsed = urlsplit(url)

    if parsed.scheme not in ('http', 'https'):
        return True

    path = parsed.path.lower()
    if any(path.endswith(ext) for ext in SKIP_EXTENSIONS):
        return True

    return False

def other_language(tag):
    '''True if a language tag ("lt", "en-GB", or a Content-Language list like "en, fr")
    names only languages outside LANGUAGES. A missing or empty tag counts as allowed,
    since most links and many pages don't declare one. Locale-style tags like "en_US"
    aren't valid BCP 47, but some sites use them, so "_" counts as "-".'''
    tags = [t.strip().replace('_', '-').split('-')[0].lower() for t in (tag or '').split(',') if t.strip()]
    tags = [t for t in tags if t != 'x'] # private-use values like "x-default" aren't languages
    return bool(tags) and not any(t in LANGUAGES for t in tags)

def try_save_link_if_visited(conn, url_to_id, source_id, url):
    '''Returns True if url was already visited (and link saved if applicable)'''
    if url not in url_to_id:
        return False
    target_id = url_to_id[url]
    if (
        target_id is not None and
        source_id is not None and
        source_id != target_id
    ):
        save_link(conn, source_id, target_id)
    return True

def mark_skipped(reason, url, url_to_id):
    print(f'\n{reason}: {url}')
    url_to_id[url] = None

def hash_content(title, content):
    '''Fingerprint of a page's indexed text, for spotting the same page served
    under several URLs. None for a page with no text, since those would all match.'''
    if not content:
        return None
    return hashlib.sha256(f'{title}\n{content}'.encode()).hexdigest()

def seed_frontier(conn, seed_urls):
    '''Adds seed URLs to the frontier, and their hosts to the crawl scope'''
    for url in seed_urls:
        if skip_url(url):
            print(f'Invalid seed URL: {url}')
            continue
        add_allowed_host(conn, urlsplit(url).netloc)
        add_to_frontier(conn, None, url)
    conn.commit()

def parse_page(html_body, url):
    '''Returns title, content, accepted children links (skip_url -> False, robots.txt
    check happens where the function is called), and the page's declared language.'''
    # encoding=True: read the encoding from the page (<meta charset> etc.) instead of assuming UTF-8
    tree = LexborHTMLParser(html_body, encoding=True)

    title_node = tree.css_first('title')
    title = ' '.join(title_node.text().split()) if title_node else ''

    # Links that say they lead to a page in another language (hreflang) are dropped
    # before any other work, e.g. Wikipedia's links to its other-language editions
    hrefs = (
        node.attributes.get('href') for node in tree.css('a[href]')
        if not other_language(node.attributes.get('hreflang'))
    )

    # Deduplicate the raw hrefs first: pages repeat links (navigation, indexes), and the
    # urljoin/urldefrag/skip_url work below costs more than the parse itself now does
    children = []
    for href in dict.fromkeys(hrefs):
        href = (href or '').strip()
        # An empty href is a link to the page itself.
        # Href starting with # is fragment of same page.
        if not href or href.startswith('#'):
            continue

        try:
            new_url = urljoin(url, href)
            new_url, _ = urldefrag(new_url)
        except ValueError:
            continue
        
        if skip_url(new_url):
            continue
        children.append(new_url)

    # Page text for search: body only (the title has its own column), without
    # script/style/noscript/template contents, whitespace collapsed to single spaces
    tree.strip_tags(NON_CONTENT_TAGS)
    content = ' '.join(tree.body.text(separator=' ').split()) if tree.body else ''

    # The language this page says it's written in, from <html lang="...">. Unlike
    # hreflang above, which is about the pages links point to, this is about this
    # page itself. It's only read here: record_page decides whether to skip the page
    html_node = tree.css_first('html')
    lang = html_node.attributes.get('lang') if html_node else None

    return title, content, list(dict.fromkeys(children)), lang
