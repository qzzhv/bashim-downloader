import asyncio
import json
import time

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup


class BashParser:
    default_parser = "html.parser"

    def __init__(self, max_workers=3, outfile_name="output.csv"):
        self._endpoint = None
        self.url = "https://bash.im"
        self._sess = None
        self._connector = None

        self._max_workers = max_workers
        self._semaphore = None

        self._outfile_name = outfile_name

        self._patterns = {
            "endpoint": ("input", "pager__input"),
            "quotes": ("div", "quote__button share")
        }

        self.data = []
        self._data_error = []
        self._data_error_num = 0
        self._not_fetched_pages = []
        self._num_not_fetched_pages = 0

    @property
    def session(self):
        if self._sess is None or self._sess.closed:
            self._connector = aiohttp.TCPConnector(
                use_dns_cache=True,
                ttl_dns_cache=60 * 60,
                limit=512
            )

            self._sess = aiohttp.ClientSession(connector=self._connector)

        return self._sess

    async def fetch(self, url: str) -> str:
        print(f"try to fetch {url}")
        # await asyncio.sleep(1)
        response = None
        tries = 0
        max_tries = 10
        while tries < max_tries:
            try:
                response = await self.session.get(url, allow_redirects=False)
                response.raise_for_status()
                break
            except (aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.ClientResponseError):
                sleep_time = 10
                print(f"can't fetch {url}, try {tries+2}/{max_tries} in {sleep_time} sec...")
                await asyncio.sleep(sleep_time)
                tries += 1
                continue

        if response:
            html = await response.text(encoding="utf-8")
            print(f"fetched {url}")
            return html
        else:
            self._num_not_fetched_pages += 1
            self._not_fetched_pages.append(url)




    def parse_html(self, html: str, pattern_name: str) -> list or int:
        soup = BeautifulSoup(html, BashParser.default_parser)
        matches_ls = soup.find_all(self._patterns[pattern_name])

        if pattern_name == "endpoint":
            return int(matches_ls[0]["max"])
        elif pattern_name == "quotes":
            quotes = []
            for match in matches_ls:

                try:
                    quote_json = json.loads(match["data-share"])
                    quote_json["id"] = int(quote_json["id"])
                    quotes.append(quote_json)
                except json.JSONDecodeError:
                    # print(f">>>\nCan't decode this:\n{match['data-share']}\n<<<")
                    self._data_error_num += 1
                    self._data_error.append(match["data-share"])
                except KeyError:
                    self._data_error.append(match)
            return quotes

        else:
            print("Unexpected behavior in parse_html. Please check the patterns.")

    @property
    def endpoint(self) -> int:
        if self._endpoint is None:
            r = requests.get(self.url)
            soup = BeautifulSoup(r.text, "html.parser")
            self._endpoint = int(soup.find("input", "pager__input")["max"])
        return self._endpoint

    async def get_data_from_page(self, page_url: str):
        async with self._semaphore:
            html = await asyncio.create_task(self.fetch(page_url))

        if html:
            quotes = self.parse_html(html, "quotes")
            if quotes:
                self.data.extend(quotes)

    async def _producer(self):
        self._endpoint = 100  # TODO comment this
        urls = [f"{self.url}/index/{page_num}" for page_num in range(1, self.endpoint + 1)]
        tasks = [asyncio.create_task(self.get_data_from_page(url)) for url in urls]

        for task in tasks:
            await task

    async def _shutdown(self):
        if self._sess is not None:
            await self._sess.close()

        dataframe = pd.DataFrame(self.data)
        dataframe.to_csv(self._outfile_name)
        print(f"decoded quotes saved to {self._outfile_name}")

    async def run(self):
        self._semaphore = asyncio.Semaphore(self._max_workers)
        try:
            await self._producer()
        finally:
            await self._shutdown()


parser = BashParser(10)
start = time.time()
asyncio.run(parser.run())
end = time.time()
print(f"quotes downloaded: {len(parser.data)}")
print(f"can't decode quotes: {parser._data_error_num}")
print(f"pages can't download: {parser._num_not_fetched_pages}")
print(f"finished with {round(end - start, 2)} sec.")
