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
            CREATE INDEX IF NOT EXISTS pages_content_search_idx
            ON pages
            USING GIN (
                to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(content, ''))
            )
        ''')

def save_page(conn, url, title, content, status_code):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO pages (url, title, content, status_code)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        ''', (url, title, content, status_code))

def search_pages(conn, query):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT url, title
            FROM pages
            WHERE title ILIKE %s
                OR content ILIKE %s
        ''', (f'%{query}%', f'%{query}%'))

        return cur.fetchall()

def clear_db(conn):
    with conn.cursor() as cur:
        cur.execute('DELETE FROM pages')