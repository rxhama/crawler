import httpx2
from urllib.robotparser import RobotFileParser
from urllib.parse import urlsplit

USER_AGENT = 'PolitePortfolioProject (https://github.com/rxhama/crawler)'
DEFAULT_CRAWL_DELAY = 1.0 # seconds, used when robots.txt doesn't specify one

async def fetch_robots(client, url) -> RobotFileParser | None:
    '''Fetches and parses the robots.txt of a URL's domain.
    None means no usable robots.txt, allow all'''
    parsed = urlsplit(url)
    domain = parsed.netloc
    robots_url = f'{parsed.scheme}://{domain}/robots.txt'
    try:
        response = await client.get(robots_url, timeout=5)
    except (httpx2.HTTPError, httpx2.InvalidURL):
        return None
    if not response.is_success:
        return None

    # A blank line makes stdlib's parser close out the current User-agent block.
    # robots.txt files that split one User-agents block's rules with blank lines
    # silently drop rules after the first blank line.
    # We therefore parse the lines ourselves and pass that into the RobotFileParser.
    lines = [line for line in response.text.splitlines() if line.strip()]
    rp = RobotFileParser()
    rp.parse(lines)
    return rp

def is_allowed(robots_cache, url) -> bool:
    '''False only if URL's domain has a robots.txt in robots_cache that disallows URL.
    A domain not in robots_cache yet counts as allowed. That only comes up when
    filtering a page's links, and those are checked again before they're requested.'''
    rp = robots_cache.get(urlsplit(url).netloc)
    return rp is None or rp.can_fetch(USER_AGENT, url)

def crawl_delay(rp) -> float:
    '''Crawl-delay from robots.txt, or DEFAULT_CRAWL_DELAY if it doesn't give one.'''
    if rp is None:
        return DEFAULT_CRAWL_DELAY
    delay = rp.crawl_delay(USER_AGENT)
    return float(delay) if delay else DEFAULT_CRAWL_DELAY
