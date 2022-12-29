import sys
import struct
from base import run
from bs4 import BeautifulSoup
import re

css_expression = re.compile(r"""url\((?!['"]?(?:data):)['"]?([^'"\)]*)['"]?\)""")

async def scrape(rpc, url, inp):
    soup = BeautifulSoup(inp, 'lxml')

    for lnk in soup.find_all("a"):
        if href := lnk.get('href', None):
            rpc.submit_url(href)
    
    for lnk in soup.find_all("link"):
        if href := lnk.get('href', None):
            rpc.submit_url(href)

    for style in soup.find_all("style"):
        for link in css_expression.finditer(style.string):
            rpc.submit_url(link.group(1))

    
    res = await rpc.get_url("https://cat-girl.gay")

run(scrape)