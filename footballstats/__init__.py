import asyncio
import nest_asyncio

from footballstats.ascraper import extract_all_clubs_data
from footballstats.aggregation import extract_league_stats
from footballstats.config import Config as config


def latest_league_stats(league_name, season, gender):
    nest_asyncio.apply()
    URL = f"https://fbref.com/en/comps/season/{season}"
    LEAGUE_KEY = f"{league_name}_{gender}"
    pages = asyncio.run(extract_all_clubs_data(URL, LEAGUE_KEY))
    league_stats = extract_league_stats(pages, config.STAT_IDS)
    return league_stats
