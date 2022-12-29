import json
import urllib.parse
import re
import codecs
from bs4 import BeautifulSoup
from base import run
import sys

def flatten(t):
    return [item for sublist in t for item in sublist]

async def scrape(rpc, url, inp):
    picrew_id = re.search(r'(?:picrew\.me\/image_maker\/)?(\d+)', url)
    if not picrew_id:
        return
    
    picrew_id = picrew_id.group(1)

    try:
        soup = BeautifulSoup(inp, features="lxml")
    except:
        return

    for tag in soup.find_all('link'):
        lnk = tag.attrs.get('href')
        if lnk:
            rpc.submit_url(lnk)

    dyn_load_re = re.compile(r'{([^{]*)}(?=\[e\]\s*?\+\s*?"\.(css|js)")')
    hash_re = re.compile(r'.*?:"(.*?)"')
    base_re = re.compile(r'https:\/\/cdn\.picrew\.me\/assets\/player\/(.+?)\/')

    deploy_key = ""

    dyn_urls = []

    for script in [s['src'] for s in soup.find_all('script') if 'src' in s.attrs]:
        script = urllib.parse.urljoin(f"https://picrew.me/image_maker/{picrew_id}", script)
        try:
            script_contents = (await rpc.get_url(script)).decode("utf8")
        except:
            continue

        base = base_re.search(script_contents)
        if base != None:
            deploy_key = base.group(1)

        for m in dyn_load_re.finditer(script_contents):
            ext = m.group(2)
            for h_m in hash_re.finditer(m.group(1)):
                dyn_urls.append(h_m.group(1) + '.' + ext)
                
    for url in map(lambda url: f"https://cdn.picrew.me/assets/player/{deploy_key}/{url}",dyn_urls):
        rpc.submit_url(url)

    nuxt_script = str(soup.find("script", string=re.compile("window\.__NUXT__")))
    m = re.search(r'release_key:.*?"(.+?)"', nuxt_script)

    for url in re.finditer('"([^"]+?(?:png|jpg|jpeg))"', nuxt_script):
        rpc.submit_url(f"https://cdn.picrew.me{codecs.decode(url.group(1), encoding='unicode-escape')}")


    for url in [
        f"https://picrew.me/image_maker/{picrew_id}",
        f"https://cdn.picrew.me/app/image_maker/{picrew_id}/{m.group(1)}/cf.json",
        f"https://cdn.picrew.me/app/image_maker/{picrew_id}/{m.group(1)}/img.json",
        f"https://cdn.picrew.me/app/image_maker/{picrew_id}/{m.group(1)}/scale.json",
        f"https://cdn.picrew.me/app/image_maker/{picrew_id}/{m.group(1)}/i_rule.json",
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/fonts/fontawesome-webfont.woff2?v=4.7.0",
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/fonts/fontawesome-webfont.woff?v=4.7.0",
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/fonts/fontawesome-webfont.ttf?v=4.7.0",
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
    ]:
        rpc.submit_url(url)

run(scrape)