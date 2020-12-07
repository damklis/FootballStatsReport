from footballstats import latest_league_stats


if __name__ == "__main__":
    stats = latest_league_stats(
        "Bundesliga",
        "2020-2021",
        "M"
    )
    print(stats.shooting.head())
