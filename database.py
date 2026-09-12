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

def save_page(conn, url, title, content, status_code):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO pages (url, title, content, status_code)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        ''', (url, title, content, status_code))

        conn.commit()

def search_pages(conn, query):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT url, title
            FROM pages
            WHERE title ILIKE %s
                OR content ILIKE %s
        ''', (f'%{query}%', f'%{query}%'))

        return cur.fetchall()