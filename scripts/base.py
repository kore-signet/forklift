import sys
import struct
import asyncio
import uuid
import json

class RpcException(Exception):
    def __init__(self, msg, code):
        super().__init__(f"error code {code} ({msg})")

def read_op_code_sync():
    return ord(sys.stdin.buffer.read(1))

class Scraper:
    def __init__(self, func):
        self.scrape = func
        self.rpc_requests = {}
        self.scrape_lock = asyncio.Lock()
        self.scrape_task = None

    async def read(self):
        op_code = await asyncio.get_running_loop().run_in_executor(None, read_op_code_sync)
        if op_code == 0:
            await self.data_in()
        elif op_code == 3:
            self.rpc()
        elif op_code == 4:
            return False
        
        return True
    
    async def run_scrape(self, url, data):
        async with self.scrape_lock:
            await self.scrape(self, url, data)
            self.file_end()

    async def data_in(self):
        url = sys.stdin.buffer.readline().decode("utf8")

        length_bytes = sys.stdin.buffer.read(8)
        length = struct.unpack("<Q", length_bytes)[0]
        data = sys.stdin.buffer.read(length)

        self.scrape_task = asyncio.ensure_future(self.run_scrape(url, data))

    def rpc(self):
        length_bytes = sys.stdin.buffer.read(8)
        length = struct.unpack("<Q", length_bytes)[0]

        response = json.loads(sys.stdin.buffer.read(length).decode("utf8"))
        fut = self.rpc_requests.get(response.get("id", ""), None)

        if fut:
            if "result" in response:
                fut.set_result(response["result"])
            elif "error" in response:
                fut.set_exception(RpcException(response["error"]["message"], response["error"]["code"]))
    
    def submit_url(self, url):
        sys.stdout.buffer.write(bytes([1]))
        sys.stdout.buffer.write(url.encode("utf8"))
        sys.stdout.buffer.write(b"\n")
        sys.stdout.buffer.flush()

    def file_end(self):
        sys.stdout.buffer.write(bytes([2]))
        sys.stdout.buffer.flush()

    async def get_url(self, url):
        res = await self.send_rpc("get_url", url)
        return bytes(res)

    def send_rpc(self, method, *args):
        fut_id = str(uuid.uuid4())
        message = {
            "method": method,
            "params": list(args),
            "id": fut_id
        }

        message_bytes = json.dumps(message).encode("utf8")

        sys.stdout.buffer.write(bytes([3]))
        sys.stdout.buffer.write(struct.pack("<Q", len(message_bytes)))
        sys.stdout.buffer.write(message_bytes)
        sys.stdout.buffer.flush()

        fut = asyncio.get_running_loop().create_future()
        self.rpc_requests[fut_id] = fut

        return fut

def run(func):
    scraper = Scraper(func)
    asyncio.run(run_inner(scraper))

async def run_inner(scraper):
    while await scraper.read():
        pass