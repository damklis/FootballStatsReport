import asyncio
from dataclasses import dataclass
from collections import ChainMap
from concurrent.futures import ProcessPoolExecutor 
from bs4 import BeautifulSoup
import pandas as pd
from footballstats.config import Config as config

logger = Logger().get_logger(__name__)


@dataclass(frozen=True)
class LeagueStats:
    shooting: pd.DataFrame
    passing: pd.DataFrame
    pass_types: pd.DataFrame
    goal_shot_creation: pd.DataFrame
    defensive_actions: pd.DataFrame
    possession: pd.DataFrame


def agregate_league_stats(html_pages, stats_id):
    logger.info(f"Fetching {stats_id} stats")
    datasets = [
        create_club_stats_dataset(html_page, stats_id)
        for html_page in html_pages
    ]
    league_stats = pd.concat(objs=datasets, ignore_index=True)
    return league_stats
 

def create_club_stats_dataset(html_page, stats_id):
    soup_object = BeautifulSoup(html_page, "html.parser")
    comment = soup_object\
        .find("div", {"id": stats_id}).contents[5]
    dataset = extract_club_records(comment)
    return dataset


def extract_club_records(comment):
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


def extract_league_stats(pages, stat_ids):
    workers = len(all_stats)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = map(
            lambda stat_id: pool.submit(
                agregate_league_stats,
                pages,
                stat_id
            ),
            stat_ids
        )
        results = [future.result() for future in list(futures)]
        return LeagueStats(*results)


def latest_league_stats(league_name, season, gender):
    nest_asyncio.apply()
    URL = f"https://fbref.com/en/comps/season/{season}"
    LEAGUE_KEY = f"{league_name}_{gender}"
    pages = asyncio.run(main(URL, LEAGUE_KEY))
    league_stats = extract_league_stats(pages, config.STAT_IDS)
    return league_stats