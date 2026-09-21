import argparse
import psycopg

from database import init_db, clear_db, reset_db
from crawler import crawl
from pagerank import save_pageranks
from search import search_pages

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['crawl', 'search', 'clear', 'reset'])

    args = parser.parse_args()

    with psycopg.connect(
        'dbname=crawler user=crawler password=crawler_password host=localhost port=5432'
    ) as conn:
        init_db(conn)

        if args.command == 'crawl':
            start_url = input('Enter start site (in case frontier is empty): ').strip()
            max_pages = int(input('Maximum pages to crawl: '))

            crawl(conn, start_url, max_pages)
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