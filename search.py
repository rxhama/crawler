# def search_pages(conn, query, limit=20):
#     with conn.cursor() as cur:
#         cur.execute('''
#             SELECT
#                 url,
#                 title,
#                 ts_rank_cd(
#                     search_vector,
#                     websearch_to_tsquery('english', %s),
#                     2
#                 ) AS score
#             FROM pages
#             WHERE search_vector @@ websearch_to_tsquery('english', %s)
#             ORDER BY score DESC
#             LIMIT %s
#         ''', (query, query, limit))

#         return cur.fetchall()

def search_pages(conn, query, limit=20, offset=0, pagerank_weight=0.3):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT
                url,
                title,
                ts_rank_cd(search_vector, q, 2)
                    * (1 + %s * ln(1 + pagerank)) AS score
            FROM pages, websearch_to_tsquery('english', %s) AS q
            WHERE search_vector @@ q
            ORDER BY score DESC
            LIMIT %s OFFSET %s
        ''', (pagerank_weight, query, limit, offset))

        return cur.fetchall()