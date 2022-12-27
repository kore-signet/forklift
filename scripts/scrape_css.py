import re
import sys
from base import run_scraper

url_expression = re.compile(r"""url\((?!['"]?(?:data):)['"]?([^'"\)]*)['"]?\)""")

def scrape(inp, out):
    inp = inp.decode("utf8")
    for match in url_expression.finditer(inp):
        out.write(match.group(1).encode("utf8"))
        out.write(b"\n")


run_scraper(scrape)