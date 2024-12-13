import json
import os
import logging
from src.data_fetcher import fetch_teams, fetch_team_stats, fetch_player_stats, fetch_rosters
from src.data_processor import process_rosters, process_team_stats, process_player_stats

logging.basicConfig(level=logging.INFO)

DATA_DIR = "data/raw"

def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def collect_season_data(season):
    # Fetch raw data
    logging.info(f"Collecting data for season {season}...")
    rosters_data = fetch_rosters(season)
    team_stats_data = fetch_team_stats(season)
    player_stats_data = fetch_player_stats(season)

    # Process data
    processed_rosters = process_rosters(rosters_data)
    processed_team_stats = process_team_stats(team_stats_data, season)
    processed_player_stats = process_player_stats(player_stats_data, season)

    # Save to disk
    season_dir = os.path.join(DATA_DIR, str(season))
    save_json(processed_rosters, os.path.join(season_dir, "rosters.json"))
    save_json(processed_team_stats, os.path.join(season_dir, "team_stats.json"))
    save_json(processed_player_stats, os.path.join(season_dir, "player_stats.json"))

    logging.info(f"Data for season {season} collected successfully.")

if __name__ == "__main__":
    # Example: Collect data for multiple seasons
    for yr in range(2018, 2023):
        collect_season_data(yr)
