import aiohttp
from aiohttp import ClientError
from aiohttp.http_exceptions import HttpProcessingError

from footballstats.log import Logger


logger = Logger().get_logger(__name__)


async def fetch_single_html(url, session):
    try:
        response = await session.request(
            method="GET",
            url=url
        )
        response.raise_for_status()
        logger.info(f"Response {response.status} for {url}")
        html = await response.text()
        return html
    except ClientError, HttpProcessingError as err:
        status = getattr(err, "status"),
        message = getattr(err, "message")
        logger.error(
            f"aiohttp exception for {url}: ({status}/{message})"
        )
    except Exception as err:
        attrs = getattr(err, "__dict__")
        logger.exception(f"Non-aiohttp exception: {attrs}")


async def fetch_multiple_htmls(urls, session):
    logger.info(f"Fetching {len(urls)} urls concurrently:")
    tasks = [
        fetch_single_html(url, session)
        for url in urls
    ]
    htmls = await asyncio.gather(*tasks)
    return htmls
