import asyncio
import heapq
import time
from collections import deque
from urllib.parse import urlsplit

from politeness import DEFAULT_CRAWL_DELAY

class Scheduler:
    '''Decides which domain gets the next request, and when.
    A domain is checked out to one worker at a time, and goes back in line when it's released.'''
    def __init__(self, allowed_hosts, unbounded):
        self.heap = []                     # Min-heap storing tuples: (ready_at, domain)
        self.domain_queues = {}            # Maps domain -> deque of frontier rows of that domain (frontier_id, source_id, url)
                                           # out of scope domains keep their rows here too, parked until allow_host
        self.in_heap = set()               # Domains currently in heap, so none are pushed twice
        self.active = set()                # Domains checked out by a worker; one job per host at a time
        self.delays = {}                   # Maps domain -> crawl_delay (missing = robots.txt not fetched yet)
        self.last_request_at = {}          # Maps domain -> time.monotonic() of last request to it
        self.allowed_hosts = allowed_hosts # Allowed hosts, from load_allowed_hosts
        self.unbounded = unbounded
        self.stopped = False
        self.changed = asyncio.Event()     # Wakes workers waiting in next()

    def in_scope(self, domain):
        return self.unbounded or domain in self.allowed_hosts

    def add(self, row):
        '''Queues a frontier row (frontier_id, source_id, url) on its host.
        Still creates domain queues for out of scope domains as they may eventually become in scope.'''
        domain = urlsplit(row[2]).netloc
        queue = self.domain_queues.setdefault(domain, deque())
        if row[1] is None: # Seed (no source page): crawl before the domain's backlog
            queue.appendleft(row)
        else:
            queue.append(row)
        self._push(domain)

    def _push(self, domain):
        '''Puts domain in line, unless it has no rows, is out of scope, is out with a worker, or is already in line.'''
        if (
            not self.domain_queues.get(domain) or
            not self.in_scope(domain) or
            domain in self.active or
            domain in self.in_heap
        ):
            return

        heapq.heappush(self.heap, (self._ready_at(domain), domain))
        self.in_heap.add(domain)
        self.changed.set()

    def _ready_at(self, domain):
        last = self.last_request_at.get(domain)
        if last is None:
            return 0.0 # Never requested, ready now
        return last + self.delays.get(domain, DEFAULT_CRAWL_DELAY)

    def allow_host(self, domain):
        '''Brings host into scope, e.g. when a seed redirects to it.'''
        self.allowed_hosts.add(domain)
        self._push(domain)

    def record_request(self, domain):
        '''Call right before every request to host, robots.txt or page.'''
        self.last_request_at[domain] = time.monotonic()

    def set_delay(self, domain, delay):
        self.delays[domain] = delay

    def release(self, domain):
        '''Call when a job finishes, whatever its outcome.'''
        self.active.remove(domain)
        self._push(domain)
        # Wake waiting workers even if host has no rows left: this may
        # have been the last active job, which means the crawl is over
        self.changed.set()

    def stop(self):
        '''No new jobs. Jobs already handed out still finish.'''
        self.stopped = True
        self.changed.set()

    async def next(self):
        '''Waits until domain is ready to be crawled, then returns a job:
        ('robots', domain, row): robots.txt not fetched yet, row is only peeked
        ('page', domain, row):   row is popped from domain's queue
        None:                    stopped, or nothing left to crawl'''

        while True:
            # Nothing waiting and nothing running: no job is left that could add work
            if self.stopped or (not self.heap and not self.active):
                return None

            timeout = None # empty heap: wait for a running job to add or release something
            if self.heap:
                ready_at, domain = self.heap[0]
                now = time.monotonic()

                # Can crawl
                if ready_at <= now:
                    heapq.heappop(self.heap)
                    self.in_heap.remove(domain)

                    # A redirect hop may have requested domain while it waited
                    if self._ready_at(domain) > now:
                        self._push(domain)
                        continue

                    self.active.add(domain)
                    queue = self.domain_queues[domain]
                    
                    if domain not in self.delays:
                        return ('robots', domain, queue[0])
                    return ('page', domain, queue.popleft())

                timeout = ready_at - now

            # Sleep until the heap's top domain is ready, or until something
            # changes first (a release, a new host, stop())
            self.changed.clear()
            try:
                async with asyncio.timeout(timeout):
                    await self.changed.wait()
            except TimeoutError:
                pass
