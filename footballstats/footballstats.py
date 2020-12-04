import asyncio
from dataclasses import dataclass
from collections import ChainMap
from concurrent.futures import ProcessPoolExecutor 
from bs4 import BeautifulSoup
import pandas as pd
import aiohttp
import nest_asyncio


nest_asyncio.apply()


async def fetch_html(url, session):
    async with session.get(url=url) as response:
        response.raise_for_status()
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

##### stats
def agregate_stats(pages, stats_id):
    print(f"Fetching {stats_id}")
    datasets = [
        create_stats_dataset(page, stats_id)
        for page in pages
    ]
    return pd.concat(
        objs=datasets, 
        ignore_index=True
    )
    

def create_stats_dataset(page, stats_id):
    soup_object = BeautifulSoup(page, "html.parser")
    comment = soup_object\
        .find("div", {"id": stats_id}).contents[5]
    dataset = extract_records(comment)
    return dataset


def extract_records(comment):
    soup_object = BeautifulSoup(comment, "html.parser")
    records =  soup_object.find("tbody").find_all("tr")
    columns = [value["data-stat"] for value in records[-1]]
    return pd.DataFrame(
        [
            [value.text for value in record] 
            for record in records
        ],
        columns=columns
    )


def extract_league_stats(pages):
    from functools import partial
    agg_stats = partial(agregate_stats, pages=pages)
    stats = ["all_stats_passing_10737",  "all_stats_shooting_10737", "all_stats_passing_types_10737","all_stats_gca_10737",
            "all_stats_defense_10737", "all_stats_possession_10737"]
#     task3 = agregate_stats(pages, "all_stats_passing_types_10737")
#     task4 = agregate_stats(pages, "all_stats_gca_10737")
#     task5 = agregate_stats(pages, "all_stats_defense_10737")
#     task6 = agregate_stats(pages, "all_stats_possession_10737")
#     tasks = [task1, task2, task3, task4, task5, task6]
    with ProcessPoolExecutor(max_workers=6) as executor:
        result_futures = list(map(lambda x: executor.submit(agregate_stats, pages, x), stats))
        results = [f.result() for f in result_futures]
        return LeagueStats(*results)

@dataclass(frozen=True)
class LeagueStats:
    shooting: pd.DataFrame
    passing: pd.DataFrame
    pass_types: pd.DataFrame
    goal_shot_creation: pd.DataFrame
    defensive_actions: pd.DataFrame
    possession: pd.DataFrame


def retrive_league_stats(league_name, season, gender):
    URL = f"https://fbref.com/en/comps/season/{season}"
    LEAGUE_KEY = f"{league_name}_{gender}"
    pages = asyncio.run(main(URL, LEAGUE_KEY))
    league_stats = extract_league_stats(pages)
    return league_stats