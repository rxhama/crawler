def load_graph(conn):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT source_url, target_url
            FROM links
        ''')

        return cur.fetchall()

def build_graph(rows):
    graph = {}

    for source_url, target_url in rows:
        if source_url not in graph:
            graph[source_url] = set()
        
        graph[source_url].add(target_url)

        if target_url not in graph:
            graph[target_url] = set()

    return graph

def initialise_ranks(graph):
    num_pages = len(graph)
    initial_rank = 1 / num_pages

    return {url: initial_rank for url in graph}

def calculate_ranks(graph, ranks, damping_factor=0.85):
    num_pages = len(graph)
    new_ranks = {}

    init_rank = (1 - damping_factor) / num_pages
    for url in graph:
        new_ranks[url] = init_rank

    dangling_rank = 0

    for source_url, targets in graph.items():
        if not targets:
            dangling_rank += ranks[source_url]
            continue

        rank_share = ranks[source_url] / len(targets)
        for target_url in targets:
            new_ranks[target_url] += damping_factor * rank_share

    dangling_share = dangling_rank / num_pages
    for url in graph:
        new_ranks[url] += damping_factor * dangling_share

    return new_ranks

# Runs many iterations of calculate_ranks(...)
def get_pageranks(graph, ranks, damping_factor=0.85, tolerance=1e-6, max_iterations=100):
    for _ in range(max_iterations):
        new_ranks = calculate_ranks(graph, ranks, damping_factor)

        difference = max(
            abs(new_ranks[url] - ranks[url])
            for url in graph
        )

        ranks = new_ranks

        if difference < tolerance:
            break

    return ranks

def save_pageranks(conn):
    rows = load_graph(conn)
    graph = build_graph(rows)
    init_ranks = initialise_ranks(graph)
    pageranks = get_pageranks(graph, init_ranks)

    with conn.cursor() as cur:
        for url, pagerank in pageranks.items():
            cur.execute('''
                UPDATE pages
                SET pagerank = %s
                WHERE url = %s
            ''', (pagerank, url))
