import time
import requests
from urllib.robotparser import RobotFileParser
from urllib.parse import urlsplit

USER_AGENT = 'PolitePortfolioProject (https://github.com/rxhama/crawler)'
DEFAULT_CRAWL_DELAY = 1.0 # seconds, used when robots.txt doesn't specify one

def get_robot_parser(session, robots_cache, last_request_at, url):
    '''Domain-keyed robots.txt cache. None means "no usable robots.txt, allow all".'''
    parsed = urlsplit(url)
    domain = parsed.netloc
    if domain in robots_cache:
        return robots_cache[domain]

    robots_url = f'{parsed.scheme}://{domain}/robots.txt'
    rp = RobotFileParser()
    try:
        response = session.get(robots_url, timeout=5)
        if response.ok:
            # A blank line makes stdlib's parser close out the current User-agent block.
            # robots.txt files that split one User-agents block's rules with blank lines
            # silently drop rules after the first blank line.
            # We therefore parse the lines ourselves and pass that into the RobotFileParser.
            lines = [line for line in response.text.splitlines() if line.strip()]
            rp.parse(lines)
        else:
            rp = None
    except requests.RequestException:
        rp = None

    last_request_at[domain] = time.monotonic()
    robots_cache[domain] = rp
    return rp

def is_allowed(session, robots_cache, last_request_at, url):
    rp = get_robot_parser(session, robots_cache, last_request_at, url)
    return rp is None or rp.can_fetch(USER_AGENT, url)

def wait_if_needed(robots_cache, last_request_at, url):
    domain = urlsplit(url).netloc

    delay = DEFAULT_CRAWL_DELAY
    rp = robots_cache.get(domain)
    if rp is not None:
        robots_delay = rp.crawl_delay(USER_AGENT)
        if robots_delay:
            delay = float(robots_delay)

    last = last_request_at.get(domain)
    if last is not None:
        remaining = delay - (time.monotonic() - last)
        if remaining > 0:
            time.sleep(remaining)

    last_request_at[domain] = time.monotonic()
