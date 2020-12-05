import aiohttp
import nest_asyncio
from footballstats.log import Logger


logger = Logger().get_logger(__name__)


async def fetch_html(url, session):
    async with session.get(url=url) as response:
        response.raise_for_status()
        logger.info(f"Response {resp.status} for {url}")
        html = await response.text()
        return html


async def main(url, league_key):
    async with aiohttp.ClientSession() as session:
        league_path = await extract_domestic_league_url(
            url, league_key, session
        )
        league_url = await format_url(url, league_path)
        pages = await extract_club_pages(league_url, session)
        return pages

        
async def format_url(base_url, query_path):
    parsed_url = urlparse(base_url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}{query_path}"


async def extract_domestic_league_url(url, league_key, session):
    content = await fetch_html(url, session)
    records = await extract_domestic_league_records(content)
    tasks = [
        format_domestic_league_record(record)
        for record in records
    ]
    formatted_records = await asyncio.gather(*tasks)
    leauge_urls = ChainMap(*formatted_records)
    return dict(leauge_urls).get(league_key)


async def extract_domestic_league_records(content):
    soup_object = BeautifulSoup(content)
    leauges_info = soup_object\
        .find("div", {"id":"all_comps_1_fa_club_league_senior"})
    records = leauges_info.find("tbody").find_all('tr')
    return records
    

async def format_domestic_league_record(record):
    league_name = record.find("th", {"data-stat":"league_name"})
    query_path = record.find("a").get("href")
    gender = record.find("td", {"data-stat":"gender"})
    return {f"{league_name.text}_{gender.text}": query_path}


async def extract_club_pages(url, session):
    content = await fetch_html(url, session)
    records = await extract_club_records(content)
    tasks = [
        format_url(url, record.find("a").get("href")) 
        for record in records
    ]
    club_urls = await asyncio.gather(*tasks)
    htmls = await fetch_htmls(club_urls, session)
    return htmls


async def fetch_htmls(urls, session):
    tasks = [fetch_html(url, session) for url in urls]
    htmls = await asyncio.gather(*tasks)
    return htmls


async def extract_club_records(content):
    soup_object = BeautifulSoup(content)
    records = soup_object.find("tbody").find_all("tr")
    return records


def retrive_league_stats(league_name, season, gender):
    nest_asyncio.apply()
    URL = f"https://fbref.com/en/comps/season/{season}"
    LEAGUE_KEY = f"{league_name}_{gender}"
    pages = asyncio.run(main(URL, LEAGUE_KEY))
    league_stats = extract_league_stats(pages)
    return league_stats