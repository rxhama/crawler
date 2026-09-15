def load_graph(conn):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT source_id, target_id
            FROM links
        ''')

        return cur.fetchall()

def build_graph(rows):
    graph = {}

    for source_id, target_id in rows:
        if source_id not in graph:
            graph[source_id] = set()
        
        graph[source_id].add(target_id)

        if target_id not in graph:
            graph[target_id] = set()

    return graph

def initialise_ranks(graph):
    num_pages = len(graph)
    initial_rank = 1 / num_pages

    return {page_id: initial_rank for page_id in graph}

def calculate_ranks(graph, ranks, damping_factor=0.85):
    num_pages = len(graph)
    new_ranks = {}

    init_rank = (1 - damping_factor) / num_pages
    for page_id in graph:
        new_ranks[page_id] = init_rank

    dangling_rank = 0

    for source_id, targets in graph.items():
        if not targets:
            dangling_rank += ranks[source_id]
            continue

        rank_share = ranks[source_id] / len(targets)
        for target_id in targets:
            new_ranks[target_id] += damping_factor * rank_share

    dangling_share = dangling_rank / num_pages
    for page_id in graph:
        new_ranks[page_id] += damping_factor * dangling_share

    return new_ranks

# Runs many iterations of calculate_ranks(...)
def get_pageranks(graph, ranks, damping_factor=0.85, tolerance=1e-6, max_iterations=100):
    for _ in range(max_iterations):
        new_ranks = calculate_ranks(graph, ranks, damping_factor)

        difference = max(
            abs(new_ranks[page_id] - ranks[page_id])
            for page_id in graph
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

    ids = list(pageranks.keys())
    ranks = list(pageranks.values())

    with conn.cursor() as cur:
        cur.execute('''
            UPDATE pages AS p
            SET pagerank = data.pagerank
            FROM (
                SELECT * FROM unnest(%s::int[], %s::float8[]) AS t(id, pagerank)
            ) AS data
            WHERE p.id = data.id
        ''', (ids, ranks))
