import argparse
import psycopg
import asyncio

from database import init_db, clear_db, reset_db
from crawler import crawl, seed_frontier
from pagerank import save_pageranks
from search import search_pages

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['crawl', 'search', 'clear', 'reset'])
    parser.add_argument('--seeds', nargs='?', const='seeds.txt', metavar='FILE', help='crawl: add the URLs in FILE (seeds.txt if no FILE is given; one per line, # for comments) to the frontier first')
    parser.add_argument('--unbounded', action='store_true', help='crawl: follow links to any host, not just seed hosts')

    args = parser.parse_args()
    if args.command != 'crawl' and (args.seeds or args.unbounded):
        parser.error('--seeds and --unbounded only apply to crawl')

    # Read the seed file before connecting, so a bad path fails like any other argument error
    seed_urls = None
    if args.seeds:
        try:
            with open(args.seeds) as f:
                lines = [line.strip() for line in f]
        except OSError as e:
            parser.error(f"can't read seeds file: {e}")
        # One URL per line; blank lines and # comments are skipped
        seed_urls = [line for line in lines if line and not line.startswith('#')]

    with psycopg.connect(
        'dbname=crawler user=crawler password=crawler_password host=localhost port=5432'
    ) as conn:
        init_db(conn)

        if args.command == 'crawl':
            if seed_urls is not None:
                seed_frontier(conn, seed_urls)
            try:
                max_pages = int(input('Maximum pages to crawl: '))
            except ValueError:
                print('Maximum pages must be a whole number.')
                return

            try:
                asyncio.run(crawl(conn, max_pages, unbounded=args.unbounded))
            except KeyboardInterrupt:
                # crawl() commits what's finished when Ctrl-C cancels it. A second
                # Ctrl-C can land in the middle of a page's writes, so drop anything uncommitted
                conn.rollback()
                print('\nCrawl interrupted. Finished pages are saved. Run crawl again to resume.')
                return
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