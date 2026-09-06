from collections import deque
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

queue = deque()
visited = {}
max_depth = 10

# start = input('Enter start site: ').strip()
start = 'https://www.google.com/'
queue.append(start)

i = 0
while queue and i < max_depth:
    url = queue.popleft()
    if url in visited:
        continue

    try:
        response = requests.get(url, timeout=5)
    except requests.RequestException:
        continue

    final_url = response.url
    if final_url in visited:
        continue

    soup = BeautifulSoup(response.content, 'html.parser')
    visited[final_url] = soup.title.text if soup.title else ''
    i += 1

    for tag in soup.find_all('a', href=True):
        href = tag['href']
        new_url = urljoin(url, href)
        queue.append(new_url)

for link, title in visited.items():
    print(title, f'({link})')