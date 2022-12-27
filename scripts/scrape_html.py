import sys
import struct
from base import run_scraper
from bs4 import BeautifulSoup
import re

css_expression = re.compile(r"""url\((?!['"]?(?:data):)['"]?([^'"\)]*)['"]?\)""")

def scrape(inp, out):
    soup = BeautifulSoup(inp, 'lxml')
    for lnk in soup.find_all("a"):
        if href := lnk.get('href', None):
            out.write(href.encode("utf8"))
            out.write(b"\n")
    
    for lnk in soup.find_all("link"):
        if href := lnk.get('href', None):
            out.write(href.encode("utf8"))
            out.write(b"\n")

    for style in soup.find_all("style"):
        for link in css_expression.finditer(style.string):
            out.write(link.group(1).encode("utf8"))
            out.write(b"\n")

run_scraper(scrape)