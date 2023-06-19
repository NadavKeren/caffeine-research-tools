from requests import get
from bs4 import BeautifulSoup
import pprint

base_url = "http://www.wikibench.eu/wiki/2007-09"
page = get(base_url)
soup = BeautifulSoup(page.text, 'lxml')
table = soup.find_all('table')

links = []
for row in table:
  get_td = row.find_all('td')
  for cell in get_td:
    a_tag = cell.find('a')
    if a_tag is not None and '.gz' in a_tag['href']:
      link = f"{base_url}/{a_tag['href']}"
      links.append(link)
      
print(f'got {len(links)} links:')
for link in links[0:20]:
  print(f'\"{link}\" ', end="")
