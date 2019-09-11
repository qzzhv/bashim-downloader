import argparse
import asyncio
import csv
import json
import logging

import aiohttp
from bs4 import BeautifulSoup

logging.basicConfig(
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s @ %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S"
)
logger = logging.getLogger(name="BashParser")


class BashParser:
    default_parser = "html.parser"

    def __init__(self, max_downloads: int, outfile_name: str):
        self._max_downloads = max_downloads
        self._outfile_name = outfile_name

        self._url = "https://bash.im"
        self._endpoint = None

        self._sess = None
        self._connector = None
        self._download_semaphore = None

        self._csv_writer = None
        self._outfile = None

        self._patterns = {
            "endpoint": ("input", "pager__input"),
            "quotes": ("div", "quote__button share")}

        self._n_recognized_quotes = 0
        self._n_wrong_matches = 0
        self._n_wrong_quotes = 0

        self._not_fetched_pages = []

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._connector = aiohttp.TCPConnector(
                use_dns_cache=True,
                ttl_dns_cache=60 * 60,
                limit=512
            )

            self._sess = aiohttp.ClientSession(connector=self._connector)
        return self._sess

    @property
    def writer(self) -> csv.writer:
        if self._csv_writer is None:
            self._outfile = open(self._outfile_name, "w", 1, encoding="utf-8")
            self._csv_writer = csv.DictWriter(
                self._outfile,
                fieldnames=["id", "url", "title", "description"]
            )
            self._csv_writer.writeheader()

        return self._csv_writer

    @property
    def download_semaphore(self) -> asyncio.Semaphore:
        if self._download_semaphore is None:
            self._download_semaphore = asyncio.Semaphore(self._max_downloads)
        return self._download_semaphore

    async def fetch(self, url: str) -> str:
        response = None
        tries = 0
        max_tries = 10
        while tries < max_tries:
            try:
                async with self.download_semaphore:
                    response = await self.session.get(url, allow_redirects=False)
                response.raise_for_status()
                break
            except (aiohttp.ClientConnectorError, aiohttp.ClientResponseError) as exc:
                sleep_time = 10
                logger.error(f"Cannot fetch {url} [{exc}] \nTry {tries + 2}/{max_tries} in {sleep_time} sec...")
                await asyncio.sleep(sleep_time)
                tries += 1
                continue

        if response:
            html = await response.text(encoding="utf-8")
            return html
        else:
            self._not_fetched_pages.append(url)

    def _fetch_all_items_on_page(self, html: str, pattern_name: str) -> int:
        soup = BeautifulSoup(html, BashParser.default_parser)
        matches = []
        try:
            matches = soup.find_all(self._patterns[pattern_name])
        except KeyError:
            logger.exception(f"Wrong pattern name \"{pattern_name}\". Possible names: {self._patterns.keys()}")

        if matches:
            if pattern_name == "endpoint":
                try:
                    return int(matches[0]["max"])
                except (KeyError, IndexError) as exc:
                    logger.error(f"Cannot get number of pages from html, [{exc}]")
                    return 0

            elif pattern_name == "quotes":
                recognized_quotes_on_page = []
                # wrong_matches_on_page = [] # TODO add and write unrecognized matches and quotes
                # wrong_quotes_on_page = []
                for match in matches:
                    try:
                        quote_json = json.loads(match["data-share"])
                    except json.JSONDecodeError:
                        logger.debug(f"Cannot decode quote to json.")
                        self._n_wrong_matches += 1
                    except KeyError:
                        logger.debug(f"Cannot find json in match. It's a quote?\n{match}")
                        self._n_wrong_matches += 1
                    else:
                        try:
                            quote_json["id"] = int(quote_json["id"])
                        except KeyError:
                            logger.debug(f"Cannot find id in quote_json. It's a quote?")
                            self._n_wrong_quotes += 1
                        else:
                            recognized_quotes_on_page.append(quote_json)
                logger.debug(f"find {len(recognized_quotes_on_page)} quotes")
                if recognized_quotes_on_page:
                    self.writer.writerows(recognized_quotes_on_page)
                    self._n_recognized_quotes += len(recognized_quotes_on_page)

                return len(recognized_quotes_on_page)

    async def process_url(self, quotes_page_url: str):
        try:
            html = await asyncio.create_task(self.fetch(quotes_page_url))
        except aiohttp.ClientResponseError as exc:
            logger.error(f"Cannot fetch {exc.request_info.url} [{exc.status}]")
        else:
            n_processed_quotes = self._fetch_all_items_on_page(html, "quotes")
            logger.debug(f"find {n_processed_quotes}")
            if n_processed_quotes == 0:
                logger.info(f"Quotes not found on {quotes_page_url}")

        logger.info(f"{quotes_page_url} processed ({n_processed_quotes} quotes).")
        logger.info(f"{self._n_recognized_quotes} quotes totally.")

    async def _producer(self):
        logger.info(f"script started")
        # Get number of pages with quotes
        try:
            html = await asyncio.create_task(self.fetch(self._url))
        except aiohttp.ClientResponseError as exc:
            logger.error(f"Cannot fetch {exc.request_info.url} [{exc.status}]")
        else:
            self._endpoint = self._fetch_all_items_on_page(html, "endpoint")
            logger.info(f"Pages with quotes: {self._endpoint}")

        # self._endpoint = 100  # For debug

        # RUN RUN RUN
        tasks = [asyncio.create_task(self.process_url(f"{self._url}/index/{num}")) for num in
                 range(1, self._endpoint + 1)]
        for task in tasks:
            await task

    async def _shutdown(self):
        if self._sess is not None:
            await self._sess.close()

        await asyncio.sleep(0.250)

        if self._outfile is not None:
            self._outfile.close()

        logger.info(f"{self._n_recognized_quotes} quotes saved at {self._outfile_name}")

    async def run(self):
        try:
            await self._producer()
        finally:
            await self._shutdown()


def main():
    parser = argparse.ArgumentParser(description="Downloads quotes from bash.im")

    parser.add_argument("--outfile", default="bash-im-quotes.csv", help="name of result file")
    parser.add_argument("--max_downloads", default=10, type=int, help="number of maximum simultaneous downloads")

    args = parser.parse_args()

    try:
        asyncio.run(BashParser(max_downloads=args.max_downloads, outfile_name=args.outfile).run())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, exiting...")


if __name__ == "__main__":
    main()
