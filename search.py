def search_pages(conn, query):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT
                url,
                title,
                ts_rank(
                    setweight(
                        to_tsvector('english', COALESCE(title, '')),
                        'A'
                    )
                    ||
                    setweight(
                        to_tsvector('english', COALESCE(content, '')),
                        'B'
                    ),
                    plainto_tsquery('english', %s)
                ) AS score
            FROM pages
            WHERE
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
                @@ plainto_tsquery('english', %s)
            ORDER BY score DESC
        ''', (query, query))

        return cur.fetchall()