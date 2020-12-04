import argparse


def parse_args():
    parser = argparse.ArgumentParser("Football Stats")
    parser.add_argument(
        "-l", "--league_name", metavar="", help="League name" 
    )
    parser.add_argument(
        "-s", "--season", metavar="", help="Season" 
    )
    parser.add_argument(
        "-g", "--gender", metavar="", help="League gender" 
    )
    return parser.parse_args()
