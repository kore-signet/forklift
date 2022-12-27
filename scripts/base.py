import sys
import struct

def parse_one(func):
    length_bytes = sys.stdin.buffer.read(8)
    if len(length_bytes) == 0:
        return False
    
    length = struct.unpack("<Q", length_bytes)[0]
    func(sys.stdin.buffer.read(length), sys.stdout.buffer)
    sys.stdout.flush()

    return True

def run_scraper(func):
    while parse_one(func):
        sys.stdout.buffer.write(b"\n")
        sys.stdout.flush()
