import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SPORTSDATAIO_API_KEY")
BASE_URL = "http://sports.core.api.espn.com/v2/sports/football/leagues/nfl"

ENDPOINTS = {
    "teams": "/seasons/{season}/teams",
    "specific_team": "/seasons/{season}/teams/{team_id}",
    "team_stats_by_season": "/seasons/{season}/types/{season_type}/teams/{team_id}/statistics",
    "player_stats_by_season": "/seasons/{season}/types/{season_type}/athletes/{athlete_id}/statistics",
    "rosters_by_season": "/seasons/{season}/teams/{team_id}/athletes",
    "specific_player": "/seasons/{season}/athletes/{athlete_id}",
    "all_positions": "/positions",
    "specific_position": "/positions/{position_id}",
    "weeks_by_season": "/seasons/{season}/types/{season_type}/weeks",
    "specific_week": "/seasons/{season}/types/{season_type}/weeks/{week_number}",
    "events_by_week": "/seasons/{season}/types/{season_type}/weeks/{week_number}/events",
    "specific_event": "/events/{event_id}",
    "specific_game": "/seasons/{season}/types/{season_type}/weeks/{week_number}/events/{event_id}/competitions/{competition_id}",
    "roster_by_competitor_and_game": "/events/{event_id}/competitions/{competition_id}/competitors/{competitor_id}/roster",
}