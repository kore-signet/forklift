import re
import sys
from base import run

url_expression = re.compile(r"""url\((?!['"]?(?:data):)['"]?([^'"\)]*)['"]?\)""")

async def scrape(scraper, url, inp):
    inp = inp.decode("utf8")
    for match in url_expression.finditer(inp):
        scraper.submit_url(match.group(1))

run(scrape)