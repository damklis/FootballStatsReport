import re
from dataclasses import dataclass
from collections import ChainMap
from concurrent.futures import ProcessPoolExecutor 
from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame

from footballstats.log.log import Logger


logger = Logger().get_logger(__name__)


@dataclass(frozen=True)
class LeagueStats:
    shooting: DataFrame
    passing: DataFrame
    pass_types: DataFrame
    goal_shot_creation: DataFrame
    defensive_actions: DataFrame
    possession: DataFrame


def aggregate_league_stats(html_pages, stats_id):
    logger.info(f"Fetching {stats_id.pattern} stats")
    datasets = [
        create_club_stats_dataset(html_page, stats_id)
        for html_page in html_pages
    ]
    league_stats = pd.concat(
        objs=datasets,
        ignore_index=True
    )
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
    columns = [record["data-stat"] for record in records[-1]]
    records_df =  DataFrame(
        [
            [value.text for value in record] 
            for record in records
        ],
        columns=columns
    )
    return records_df


def create_statistic_pattern(statistic):
    pattern = f"all_stats_{statistic}_*"
    return re.compile(pattern)


def extract_league_stats(html_pages, stat_ids):
    statistics = map(create_statistic_pattern, stat_ids)
    workers = len(stat_ids)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        future_results = map(
            lambda statistic: pool.submit(
                aggregate_league_stats,
                html_pages,
                statistic
            ),
            statistics
        )
        results = [
            future.result() 
            for future in list(future_results)
        ]
        return LeagueStats(*results)
