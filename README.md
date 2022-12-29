# forklift - a scriptable web archiver

a highly configurable WARC archiver that relies on external scripting for scraping functionality

### note about HTTP/2
this program may use HTTP/2 to retrieve data. in that case, it will try to convert the response into a HTTP/1.1 compatible format, and record the original protocol via the [WARC-Protocol field](https://github.com/iipc/warc-specifications/issues/42>)