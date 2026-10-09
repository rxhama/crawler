def init_db(conn):
    '''Creates tables/GIN indexes.'''
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS pages (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                content TEXT,
                status_code INTEGER,
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pagerank DOUBLE PRECISION DEFAULT 0,
                content_hash TEXT,

                search_vector TSVECTOR GENERATED ALWAYS AS (
                    setweight(
                        to_tsvector('english', left(COALESCE(title, ''), 10000)),
                        'A'
                    )
                    ||
                    setweight(
                        to_tsvector('english', left(COALESCE(content, ''), 500000)),
                        'B'
                    )
                ) STORED
            )
        ''')

        cur.execute('''
            CREATE INDEX IF NOT EXISTS pages_search_idx
            ON pages
            USING GIN (search_vector)
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS links (
                id SERIAL PRIMARY KEY,
                source_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
                target_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
                UNIQUE (source_id, target_id)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS frontier (
                id SERIAL PRIMARY KEY,
                source_id INTEGER REFERENCES pages(id) ON DELETE CASCADE,
                url TEXT NOT NULL,
                UNIQUE NULLS NOT DISTINCT (source_id, url)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS allowed_hosts (
                host TEXT PRIMARY KEY
            )
        ''')

def save_page(conn, url, title, content, status_code, content_hash):
    '''Saves a page to DB. Returns the row's id.'''
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO pages (url, title, content, status_code, content_hash)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
            RETURNING id
        ''', (url, title, content, status_code, content_hash))

        row = cur.fetchone()
        if row is not None:
            return row[0]

        cur.execute('SELECT id FROM pages WHERE url = %s', (url,))
        return cur.fetchone()[0]

def clear_db(conn):
    '''Deletes all data in all tables'''
    with conn.cursor() as cur:
        cur.execute('DELETE FROM frontier')
        cur.execute('DELETE FROM links')
        cur.execute('DELETE FROM pages')
        cur.execute('DELETE FROM allowed_hosts')

def reset_db(conn):
    '''Drops all tables'''
    with conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS frontier, links, pages, allowed_hosts')

def save_link(conn, source_id, target_id):
    '''Saves link between 2 pages (their ids in pages)'''
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO links (source_id, target_id)
            VALUES (%s, %s)
            ON CONFLICT (source_id, target_id) DO NOTHING
        ''', (source_id, target_id))

def save_links(conn, links):
    '''Saves (source_id, target_id) pairs in one round trip'''
    # The same pair can come up twice when two URLs are aliases of one page
    links = list(dict.fromkeys(links))
    if not links:
        return

    source_ids = [source_id for source_id, _ in links]
    target_ids = [target_id for _, target_id in links]
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO links (source_id, target_id)
            SELECT * FROM unnest(%s::int[], %s::int[])
            ON CONFLICT (source_id, target_id) DO NOTHING
        ''', (source_ids, target_ids))

def load_pages(conn):
    '''url -> id for every page already crawled'''
    with conn.cursor() as cur:
        cur.execute('SELECT url, id FROM pages')
        return dict(cur.fetchall())

def load_content_hashes(conn):
    '''content_hash -> id for every crawled page that has one'''
    with conn.cursor() as cur:
        cur.execute('SELECT content_hash, id FROM pages WHERE content_hash IS NOT NULL')
        return dict(cur.fetchall())

def load_frontier(conn):
    '''(frontier_id, source_id, url) for all frontier entries persisting from previous runs.
    frontier_id needed to later remove from frontier table by searching by id.'''
    with conn.cursor() as cur:
        cur.execute('SELECT id, source_id, url FROM frontier ORDER BY id')
        return cur.fetchall()

def add_to_frontier(conn, source_id, url):
    '''Adds (source_id, url) to frontier. Persists and will be crawled in next run(s)
    if crawl is stopped before url is crawled.'''
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO frontier (source_id, url)
            VALUES (%s, %s)
            ON CONFLICT (source_id, url) DO NOTHING
            RETURNING id
        ''', (source_id, url))

        row = cur.fetchone()
        if row is not None:
            return row[0]

        cur.execute('''
            SELECT id FROM frontier
            WHERE source_id IS NOT DISTINCT FROM %s AND url = %s
        ''', (source_id, url))
        return cur.fetchone()[0]

def add_children_to_frontier(conn, source_id, urls):
    '''Adds one page's links to the frontier in one round trip.
    Returns url -> frontier id for the rows it inserted. A row that already existed
    isn't returned: that only happens when two workers record the same page (after
    a cross-domain redirect), and the first one has already queued it.'''
    if not urls:
        return {}

    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO frontier (source_id, url)
            SELECT %s::int, unnest(%s::text[])
            ON CONFLICT (source_id, url) DO NOTHING
            RETURNING url, id
        ''', (source_id, urls))
        return dict(cur.fetchall())

def remove_from_frontier(conn, frontier_id):
    '''Remove from frontier table by id'''
    with conn.cursor() as cur:
        cur.execute('DELETE FROM frontier WHERE id = %s', (frontier_id,))

def load_allowed_hosts(conn):
    '''Returns set of all allowed hosts from previous runs'''
    with conn.cursor() as cur:
        cur.execute('SELECT host FROM allowed_hosts')
        return {host for (host,) in cur.fetchall()}

def add_allowed_host(conn, host):
    '''Adds host to allowed_hosts for future runs'''
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO allowed_hosts (host)
            VALUES (%s)
            ON CONFLICT (host) DO NOTHING
        ''', (host,))