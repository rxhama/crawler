def init_db(conn):
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
                UNIQUE (source_id, url)
            )
        ''')

def save_page(conn, url, title, content, status_code):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO pages (url, title, content, status_code)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
            RETURNING id
        ''', (url, title, content, status_code))

        row = cur.fetchone()
        if row is not None:
            return row[0]

        cur.execute('SELECT id FROM pages WHERE url = %s', (url,))
        return cur.fetchone()[0]

def clear_db(conn):
    with conn.cursor() as cur:
        cur.execute('DELETE FROM frontier')
        cur.execute('DELETE FROM links')
        cur.execute('DELETE FROM pages')

def reset_db(conn):
    with conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS frontier, links, pages')

def save_link(conn, source_id, target_id):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO links (source_id, target_id)
            VALUES (%s, %s)
            ON CONFLICT (source_id, target_id) DO NOTHING
        ''', (source_id, target_id))

def load_pages(conn):
    '''url -> id for every page already crawled'''
    with conn.cursor() as cur:
        cur.execute('SELECT url, id FROM pages')
        return dict(cur.fetchall())

def load_frontier(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT id, source_id, url FROM frontier ORDER BY id')
        return cur.fetchall()

def add_to_frontier(conn, source_id, url):
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

def remove_from_frontier(conn, frontier_id):
    with conn.cursor() as cur:
        cur.execute('DELETE FROM frontier WHERE id = %s', (frontier_id,))