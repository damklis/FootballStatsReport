import asyncio
from dataclasses import dataclass
from collections import ChainMap
from concurrent.futures import ProcessPoolExecutor 
from bs4 import BeautifulSoup
import pandas as pd


@dataclass(frozen=True)
class LeagueStats:
    shooting: pd.DataFrame
    passing: pd.DataFrame
    pass_types: pd.DataFrame
    goal_shot_creation: pd.DataFrame
    defensive_actions: pd.DataFrame
    possession: pd.DataFrame



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
    all_stats = [
        "all_stats_passing_10737",  
        "all_stats_shooting_10737", 
        "all_stats_passing_types_10737",
        "all_stats_gca_10737",
        "all_stats_defense_10737", 
        "all_stats_possession_10737"
    ]
    with ProcessPoolExecutor(max_workers=len(all_stats)) as pool:
        futures = map(
            lambda x: pool.submit(agregate_stats, pages, x),
            all_stats
        )
        results = [future.result() for future in list(futures)]
        return LeagueStats(*results)




def retrive_league_stats(league_name, season, gender):
    nest_asyncio.apply()
    URL = f"https://fbref.com/en/comps/season/{season}"
    LEAGUE_KEY = f"{league_name}_{gender}"
    pages = asyncio.run(main(URL, LEAGUE_KEY))
    league_stats = extract_league_stats(pages)
    return league_stats