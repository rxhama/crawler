def search_pages(conn, query):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT
                url,
                title,
                ts_rank_cd(
                    search_vector,
                    plainto_tsquery('english', %s),
                    2
                ) AS score
            FROM pages
            WHERE search_vector @@ plainto_tsquery('english', %s)
            ORDER BY score DESC
        ''', (query, query))

        return cur.fetchall()