import argparse

import re
import asyncio
from dataclasses import dataclass
from collections import ChainMap
from concurrent.futures import ProcessPoolExecutor 
from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame
from footballstats.config import Config as config


def get_latest_league_stats(league_name, season, gender):
    nest_asyncio.apply()
    URL = f"https://fbref.com/en/comps/season/{season}"
    LEAGUE_KEY = f"{league_name}_{gender}"
    pages = asyncio.run(main(URL, LEAGUE_KEY))
    league_stats = extract_league_stats(pages, config.STAT_IDS)
    return league_stats
