from collections import ChainMap
from urllib.parse import urlparse
import asyncio
import aiohttp
from bs4 import BeautifulSoup

from footballstats.ahtml import (
    fetch_single_html, 
    fetch_multiple_htmls
)

      
async def format_url(base_url, query_path):
    parsed_url = urlparse(base_url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}{query_path}"


async def extract_domestic_league_path(url, league_key, session):
    content = await fetch_single_html(url, session)
    items = await extract_domestic_league_items(content)
    tasks = [
        format_domestic_league_item(item)
        for item in items
    ]
    formatted_items = await asyncio.gather(*tasks)
    league_urls = ChainMap(*formatted_items)
    league_path = dict(league_urls).get(league_key)
    if not league_path:
        raise ValueError("League Unknown!")
    return league_path

async def extract_domestic_league_items(content):
    soup_object = BeautifulSoup(content)
    leagues_info = soup_object\
        .find("div", {"id":"all_comps_1_fa_club_league_senior"})
    items = leagues_info.find("tbody").find_all('tr')
    return items
    

async def format_domestic_league_item(item):
    league_name = item.find("th", {"data-stat":"league_name"})
    query_path = item.find("a").get("href")
    gender = item.find("td", {"data-stat":"gender"})
    return {
        f"{league_name.text}_{gender.text}": query_path
    }


async def extract_club_pages(url, session):
    content = await fetch_single_html(url, session)
    items = await extract_club_items(content)
    tasks = [
        format_url(url, item.find("a").get("href")) 
        for item in items
    ]
    club_urls = await asyncio.gather(*tasks)
    htmls = await fetch_multiple_htmls(club_urls, session)
    return htmls 


async def extract_club_items(content):
    soup_object = BeautifulSoup(content)
    items = soup_object.find("tbody").find_all("tr")
    return items

    
async def extract_all_clubs_data(url, league_key):
    async with aiohttp.ClientSession() as session:
        league_path = await extract_domestic_league_path(
            url, league_key, session
        )
        league_url = await format_url(url, league_path)
        html_pages = await extract_club_pages(league_url, session)
        return html_pages
