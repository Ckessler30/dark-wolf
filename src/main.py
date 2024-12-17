import concurrent.futures
import re
import logging
import os
import json
from data_fetcher import (
    fetch_weeks_by_season, 
    fetch_games_per_week,
    fetch_game_details,
    fetch_game_stats_by_player,
    fetch_game_stats_by_team,
    fetch_player_details,
    client
)
from config import ENDPOINTS

logging.basicConfig(level=logging.INFO)
DATA_DIR = "data/raw"

POSITION_CATEGORY_MAPPING = {
    "1": {"receiving", "scoring", "rushing", "passing", "general"}, # Wide Receiver
    "2": set(), # Left Tackle
    "3": set(), # Left Guard
    "4": set(), # Center
    "5": set(), # Right Guard
    "6": set(), # Right Tackle
    "7": {"passing", "rushing", "scoring", "general"}, # Tight End
    "8": {"passing", "rushing", "scoring", "general"}, # Quarterback
    "9": {"passing", "rushing", "scoring", "general"}, # Running Back
    "10": {"passing", "rushing", "scoring", "general"}, # Fullback
    "11": {"defensive", "defensiveInterceptions", "general"}, # Left Defensive End
    "12": {"defensive", "defensiveInterceptions", "general"}, # Nose Tackle
    "13": {"defensive", "defensiveInterceptions", "general"}, # Right Defensive End
    "14": {"defensive", "defensiveInterceptions", "general"}, # Left Outside Linebacker
    "15": {"defensive", "defensiveInterceptions", "general"}, # Left Inside Linebacker
    "16": {"defensive", "defensiveInterceptions", "general"}, # Right Inside Linebacker
    "17": {"defensive", "defensiveInterceptions", "general"}, # Right Outside Linebacker
    "18": {"defensive", "defensiveInterceptions", "general"}, # Left Cornerback
    "19": {"defensive", "defensiveInterceptions", "general"}, # Right Cornerback
    "20": {"defensive", "defensiveInterceptions", "general"}, # Strong Safety
    "21": {"defensive", "defensiveInterceptions", "general"}, # Free Safety
    "22": {"kicking", "scoring", "general"}, # Place Kicker
    "23": {"kicking", "scoring", "general"}, # Punter
    "24": {"defensive", "defensiveInterceptions", "general"}, # Left Defensive Tackle
    "25": {"defensive", "defensiveInterceptions", "general"}, # Right Defensive Tackle
    "26": {"defensive", "defensiveInterceptions", "general"}, # Weakside Linebacker
    "27": {"defensive", "defensiveInterceptions", "general"}, # Middle Linebacker
    "28": {"defensive", "defensiveInterceptions", "general"}, # Strongside Linebacker
    "29": {"defensive", "defensiveInterceptions", "general"}, # Cornerback
    "30": {"defensive", "defensiveInterceptions", "general"}, # Linebacker
    "31": {"defensive", "defensiveInterceptions", "general"}, # Defensive End
    "32": {"defensive", "defensiveInterceptions", "general"}, # Defensive Tackle
    "33": {"defensive", "defensiveInterceptions", "general"}, # Under Tackle
    "34": {"defensive", "defensiveInterceptions", "general"}, # Nickel Back
    "35": {"defensive", "defensiveInterceptions", "general"}, # Defensive Back
    "36": {"defensive", "defensiveInterceptions", "general"}, # Safety
    "37": {"defensive", "defensiveInterceptions", "general"}, # Defensive Lineman
    "39": set(), # Long Snapper
    "45": set(), # Offensive Lineman
    "46": set(), # Offensive Tackle
    "47": set(), # Offensive Guard
    "50": {"passing", "rushing", "scoring", "general"}, # Athlete
    "70": {"passing", "rushing", "scoring", "general"}, # Offense
    "71": {"defensive", "defensiveInterceptions", "scoring", "general"}, # Defense
    "72": {"passing", "rushing", "scoring", "general", "kicking"}, # Special Teams
    "73": set(), # Guard
    "74": set(), # Tackle
    "75": {"defensive", "defensiveInterceptions", "scoring", "general"}, # Nose Guard
    "76": {"passing", "rushing", "scoring", "general"}, # Punt Returner
    "77": {"passing", "rushing", "scoring", "general"}, # Kick Returner
    "78": set(), # Long Snapper
    "79": set(), # Holder
    "80": {"scoring", "general", "kicking"}, # Place Kicker
}

def should_include_category(category: dict) -> bool:
    """
    Determines whether to include a stat category based on its stats.

    Args:
        category (dict): The category dictionary containing stats.

    Returns:
        bool: True if at least one stat has a non-zero value, False otherwise.
    """
    for stat in category.get("stats", []):
        value = stat.get("value", 0)
        if isinstance(value, (int, float)) and value != 0:
            return True
    return False



def filter_game_stats(game_stats: dict, positionId: str = None) -> dict:
    """
    Filters out unnecessary fields from game_stats, retaining only valuable statistical information.
    Additionally, removes any stat categories where all stats are zero or irrelevant to the player's position.

    Args:
        game_stats (dict): The original game_stats dictionary.
        positionId (str, optional): The player's positionId. Defaults to None.

    Returns:
        dict: A filtered dictionary containing only valuable fields and relevant non-zero categories.
    """
    # Define the top-level fields to keep
    valuable_top_fields = {"competition", "splits"}

    # Retain only valuable top-level fields
    filtered_game_stats = {key: value for key, value in game_stats.items() if key in valuable_top_fields}

    # Process 'competition' if necessary (e.g., remove '$ref')
    if "competition" in filtered_game_stats and isinstance(filtered_game_stats["competition"], dict):
        competition = filtered_game_stats["competition"]
        # Remove '$ref' from 'competition'
        competition_filtered = {k: v for k, v in competition.items() if k != "$ref"}
        filtered_game_stats["competition"] = competition_filtered

    # Process 'splits'
    if "splits" in filtered_game_stats and isinstance(filtered_game_stats["splits"], dict):
        splits = filtered_game_stats["splits"]
        # Retain only valuable split-level fields
        splits_filtered = {k: v for k, v in splits.items() if k in {"id", "name", "abbreviation", "categories"}}

        # Process each category
        if "categories" in splits_filtered and isinstance(splits_filtered["categories"], list):
            categories_filtered = []
            for category in splits_filtered["categories"]:
                category_name = category.get("name")

                # Always include 'general' and 'scoring' categories
                if category_name in {"general", "scoring"}:
                    if should_include_category(category):
                        # Retain only valuable category-level fields
                        category_filtered = {
                            k: v for k, v in category.items()
                            if k in {"name", "displayName", "shortDisplayName", "abbreviation", "stats"}
                        }
                        categories_filtered.append(category_filtered)
                        logging.debug(f"Including category '{category_name}' (universal).")
                    else:
                        logging.debug(f"Excluding category '{category_name}' as all stats are zero.")
                    continue

                # Include categories relevant to the player's position
                if positionId:
                    relevant_categories = POSITION_CATEGORY_MAPPING.get(positionId, set())
                    if category_name in relevant_categories:
                        if should_include_category(category):
                            # Retain only valuable category-level fields
                            category_filtered = {
                                k: v for k, v in category.items()
                                if k in {"name", "displayName", "shortDisplayName", "abbreviation", "stats"}
                            }
                            categories_filtered.append(category_filtered)
                            logging.debug(f"Including category '{category_name}' for positionId '{positionId}'.")
                        else:
                            logging.debug(f"Excluding category '{category_name}' for positionId '{positionId}' as all stats are zero.")
                        continue

                # If category is not relevant, exclude it
                logging.debug(f"Excluding irrelevant category '{category_name}' for positionId '{positionId}'.")
                continue

            splits_filtered["categories"] = categories_filtered

        filtered_game_stats["splits"] = splits_filtered

    return filtered_game_stats




def filter_player_data(player_data: dict) -> dict:
    """
    Filter out unnecessary fields from player_data, retaining only valuable information.
    
    Args:
        player_data (dict): The original player data dictionary.
    
    Returns:
        dict: A filtered dictionary containing only valuable fields.
    """
    valuable_fields = {
        "id",
        "firstName",
        "lastName",
        "fullName",
        "displayName",
        "shortName",
        "weight",
        "displayWeight",
        "height",
        "displayHeight",
        "age",
        "dateOfBirth",
        "debutYear",
        "birthPlace",
        "college",
        "slug",
        "headshot",
        "jersey",
        "position",
        "experience",
        "active",
        "draft",
        "status"
    }
    
    filtered_data = {key: value for key, value in player_data.items() if key in valuable_fields}
    
    # Optionally, further process nested structures if needed
    # For example, extracting 'city', 'state', 'country' from 'birthPlace'
    if "birthPlace" in filtered_data and isinstance(filtered_data["birthPlace"], dict):
        birth_place = filtered_data["birthPlace"]
        filtered_data["birthPlace"] = {
            "city": birth_place.get("city"),
            "state": birth_place.get("state"),
            "country": birth_place.get("country")
        }
    
    # Similarly, handle 'college' if it's a nested dict with a '$ref'
    if "college" in filtered_data and isinstance(filtered_data["college"], dict):
        # You might want to fetch additional college details or just keep the ID
        # For simplicity, we'll keep the 'id' if available
        # If 'college' contains a '$ref', you might extract the ID from it
        college_ref = filtered_data["college"].get("$ref", "")
        match = re.search(r"/colleges/(\d+)", college_ref)
        if match:
            filtered_data["college"] = {
                "id": match.group(1)
            }
        else:
            # If no '$ref', retain the existing 'college' data
            filtered_data["college"] = filtered_data["college"]
    
    # Handle 'position' similarly
    if "position" in filtered_data and isinstance(filtered_data["position"], dict):
        position = filtered_data["position"]
        filtered_data["position"] = {
            "id": position.get("id"),
            "name": position.get("name"),
            "displayName": position.get("displayName"),
            "abbreviation": position.get("abbreviation"),
            "leaf": position.get("leaf")
        }
    
    # Handle 'draft' details
    if "draft" in filtered_data and isinstance(filtered_data["draft"], dict):
        draft = filtered_data["draft"]
        team_ref = draft.get("team", {}).get("$ref", "")
        match = re.search(r"/teams/(\d+)", team_ref)
        if match:
            draft["team"] = {
                "id": match.group(1)
            }
        else:
            draft["team"] = draft["team"]
    
    return filtered_data


def parse_athlete_ids_from_team_stats(team_stats: dict):
    """
    Parse all athlete IDs from the 'team_stats' response JSON.
    
    The JSON structure contains splits -> categories[] -> athletes[],
    where each athlete has an 'athlete' key containing a '$ref'.
    Example: "athlete": { "$ref": "http://.../athletes/<athlete_id>?..." }
    
    Returns:
        A set of all unique athlete IDs (as strings) found in the splits.
    """
    athlete_ids = set()

    # The 'team_stats' top-level object has a "splits" dict,
    # which has "categories" list, each category has "athletes" list.
    splits = team_stats.get("splits", {})
    categories = splits.get("categories", [])

    for category in categories:
        # Each category has "athletes", which is a list of dicts
        athletes = category.get("athletes", [])
        for athlete_entry in athletes:
            athlete_ref = athlete_entry.get("athlete", {}).get("$ref", "")
            # Parse out the numeric athlete_id from the URL ref, e.g. ".../athletes/<id>?..."
            match = re.search(r"/athletes/(\d+)", athlete_ref)
            if match:
                athlete_ids.add(match.group(1))  # Add the athlete_id as a string
    
    return sorted(athlete_ids)  # Return a sorted list (or you can leave it as a set)


def remove_athletes_from_team_stats(team_stats: dict):
    """
    Remove any 'athletes' keys from the team_stats dictionary.
    
    Args:
        team_stats (dict): The team_stats dictionary from which to remove 'athletes'.
    
    Returns:
        None: The function modifies the team_stats dictionary in place.
    """
    splits = team_stats.get("splits", {})
    categories = splits.get("categories", [])
    
    for category in categories:
        if "athletes" in category:
            del category["athletes"]
            logging.debug("Removed 'athletes' section from category.")
    
    # If 'athletes' exists at the top level of team_stats, remove it
    if "athletes" in team_stats:
        del team_stats["athletes"]
        logging.debug("Removed top-level 'athletes' section from team_stats.")


def save_json(data, filepath):
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving JSON to {filepath}: {e}")

def fetch_one_game(season: int, week_number: int, event_id: int):
    """
    This function fetches all data for a single game (event),
    returning a structured dict. We'll run this in parallel threads.
    """
    logging.info(f"Fetching detailed data for Event={event_id} (Week={week_number}, Season={season})")
    
    game_info = fetch_game_details(event_id)
    competition_id = game_info["competition_id"]
    competitor_ids = game_info["competitor_ids"]

    # Prepare the "game_json" structure
    game_json = {
        "season": season,
        "weekNumber": week_number,
        "eventId": event_id,
        "competitionId": competition_id,
        "teams": []
    }

    # Fetch basic event info
    event_endpoint = ENDPOINTS["specific_event"].format(event_id=event_id)
    try:
        full_event_response = client.get(event_endpoint)
        game_json["eventName"]  = full_event_response.get("name")
        game_json["eventDate"]  = full_event_response.get("date")
        game_json["shortName"]  = full_event_response.get("shortName")
    except Exception as e:
        logging.error(f"Error fetching event details for {event_id}: {e}")
        return None  # or return partial data

    # For each competitor, fetch team stats & then player stats
    for competitor_id in competitor_ids:
        team_stats = fetch_game_stats_by_team(event_id, competition_id, competitor_id)
        team_info = {
            "competitorId": competitor_id,
            "teamStats": team_stats
        }
        athlete_ids = parse_athlete_ids_from_team_stats(team_stats)
        remove_athletes_from_team_stats(team_stats)

        player_stats_list = []
        for athlete_id in athlete_ids:
            try:
                player_stats = fetch_game_stats_by_player(event_id, competition_id, competitor_id, athlete_id)
            except Exception as e:
                logging.error(f"Error fetching game stats for Athlete={athlete_id} in Competitor={competitor_id}: {e}")
                player_stats = {}
            
            try:
                player_data = fetch_player_details(season, athlete_id)
            except Exception as e:
                logging.error(f"Error fetching player details for Athlete={athlete_id}: {e}")
                player_data = {}
            
            player_position = player_data.get("position", {}).get("id")
            filtered_player_stats = filter_game_stats(player_stats, player_position)
            filtered_player_data = filter_player_data(player_data)
            
            player_stats_list.append({
                "athleteId": athlete_id,
                "gameStats": filtered_player_stats,
                "playerData": filtered_player_data
            })
        
        team_info["playerStats"] = player_stats_list
        game_json["teams"].append(team_info)

    return game_json

def collect_season_data(season):
    logging.info(f"Collecting data for season {season}...")

    weeks_data = fetch_weeks_by_season(season, 2)  # returns e.g. [1,2,3,...,17]

    # We’ll build a list of (week_number, event_id) tasks, then fetch them in parallel
    tasks = []
    for week_number in weeks_data:
        events_data = fetch_games_per_week(season, 2, week_number)  # e.g. [401220349, 401220350, ...]
        for event_id in events_data:
            tasks.append((week_number, event_id))

    # Now we have a list of all (week_number, event_id) for the entire season.
    # We'll use a ThreadPoolExecutor to fetch them in parallel.
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Create a mapping of future -> (week, event_id)
        future_to_game = {
            executor.submit(fetch_one_game, season, w, e): (w, e)
            for (w, e) in tasks
        }

        for future in concurrent.futures.as_completed(future_to_game):
            week_number, event_id = future_to_game[future]
            try:
                game_json = future.result()  # fetch_one_game’s return value
            except Exception as exc:
                logging.error(f"Game fetch generated an exception: {exc}")
                continue
            
            # If game_json is None, skip
            if not game_json:
                continue
            
            # Save to disk
            filepath = os.path.join(
                DATA_DIR, str(season),
                f"week_{week_number}", f"event_{event_id}.json"
            )
            save_json(game_json, filepath)
            logging.info(f"Saved game data for event={event_id} to {filepath}")

    logging.info(f"Data for season {season} collected successfully.")

if __name__ == "__main__":
    for yr in range(2018, 2019):
        collect_season_data(yr)