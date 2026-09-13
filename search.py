def search_pages(conn, query):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT
                url,
                title,
                ts_rank(
                    to_tsvector(
                        'english',
                        COALESCE(title, '') || ' ' || COALESCE(content, '')
                    ),
                    plainto_tsquery('english', %s)
                ) AS score
            FROM pages
            WHERE to_tsvector(
                'english',
                COALESCE(title, '') || ' ' || COALESCE(content, '')
            )
            @@ plainto_tsquery('english', %s)
            ORDER BY score DESC
        ''', (query, query))

        return cur.fetchall()