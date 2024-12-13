from src.api_client import SportsDataIOClient
from src.config import ENDPOINTS

client = SportsDataIOClient()

def fetch_teams():
    endpoint = ENDPOINTS["teams"]
    return client.get(endpoint)

def fetch_team_stats(season: int):
    endpoint = ENDPOINTS["team_stats_by_season"].format(season=season)
    return client.get(endpoint)

def fetch_player_stats(season: int):
    endpoint = ENDPOINTS["player_stats_by_season"].format(season=season)
    return client.get(endpoint)

def fetch_rosters(season: int):
    endpoint = ENDPOINTS["rosters_by_season"].format(season=season)
    return client.get(endpoint)
