def init_db(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS pages (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                content TEXT,
                status_code INTEGER,
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
            CREATE INDEX IF NOT EXISTS pages_search_idx
            ON pages
            USING GIN (
                (
                    setweight(
                        to_tsvector('english', COALESCE(title, '')),
                        'A'
                    )
                    ||
                    setweight(
                        to_tsvector('english', COALESCE(content, '')),
                        'B'
                    )
                )
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS links (
                id SERIAL PRIMARY KEY,
                source_url TEXT NOT NULL REFERENCES pages(url) ON DELETE CASCADE,
                target_url TEXT NOT NULL REFERENCES pages(url) ON DELETE CASCADE,
                UNIQUE (source_url, target_url)
            )
        ''')

def save_page(conn, url, title, content, status_code):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO pages (url, title, content, status_code)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        ''', (url, title, content, status_code))

        conn.commit()

def clear_db(conn):
    with conn.cursor() as cur:
        cur.execute('DELETE FROM links')
        cur.execute('DELETE FROM pages')

def save_link(conn, source_url, target_url):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO links (source_url, target_url)
            VALUES (%s, %s)
            ON CONFLICT (source_url, target_url) DO NOTHING
        ''', (source_url, target_url))