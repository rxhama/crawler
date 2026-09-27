import argparse
import psycopg

from database import init_db, clear_db, reset_db
from crawler import crawl, seed_frontier
from pagerank import save_pageranks
from search import search_pages

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['crawl', 'search', 'clear', 'reset'])
    parser.add_argument('--seeds', metavar='FILE', help='crawl: add the URLs in FILE (one per line, # for comments) to the frontier first')
    parser.add_argument('--unbounded', action='store_true', help='crawl: follow links to any host, not just seed hosts')

    args = parser.parse_args()

    with psycopg.connect(
        'dbname=crawler user=crawler password=crawler_password host=localhost port=5432'
    ) as conn:
        init_db(conn)

        if args.command == 'crawl':
            if args.seeds:
                with open(args.seeds) as f:
                    lines = [line.strip() for line in f]
                # One URL per line; blank lines and # comments are skipped
                seed_frontier(conn, [line for line in lines if line and not line.startswith('#')])
            max_pages = int(input('Maximum pages to crawl: '))

            crawl(conn, max_pages, unbounded=args.unbounded)
            save_pageranks(conn)

        elif args.command == 'search':
            query = input('Enter search query: ').strip()
            results = search_pages(conn, query)

            for url, title, score in results:
                print(f'{title} ({url}) - score: {score}')

        elif args.command == 'clear':
            if input('This will delete all crawl data. Are you sure? [y/N]: ').lower() != 'y':
                print('Clear aborted.')
                return
            clear_db(conn)
            print('DB cleared.')

        elif args.command == 'reset':
            if input('This will delete and recreate all tables, deleting all crawl data. Are you sure? [y/N]: ').lower() != 'y':
                print('Reset aborted.')
                return
            reset_db(conn)
            init_db(conn)
            print('DB reset.')

if __name__ == '__main__':
    main()