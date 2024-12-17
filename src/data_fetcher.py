import re
from api_client import SportsDataIOClient
from config import ENDPOINTS

client = SportsDataIOClient()

team_ids = [
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
]

def extract_week_numbers(response):
    """
    Extract week numbers from the response JSON.

    Args:
        response (dict): The JSON response containing week information.

    Returns:
        list: A list of week numbers as integers.
    """
    week_numbers = []
    for item in response.get('items', []):
        match = re.search(r"weeks/(\d+)", item.get("$ref", ""))
        if match:
            week_numbers.append(int(match.group(1)))  # Convert to integer
    return week_numbers

def extract_event_numbers(response):
    """
    Extract event numbers from the response JSON.

    Args:
        response (dict): The JSON response containing event information.

    Returns:
        list: A list of event numbers as integers.
    """
    event_numbers = []
    for item in response.get('items', []):
        match = re.search(r"events/(\d+)", item.get("$ref", ""))
        if match:
            event_numbers.append(int(match.group(1)))  # Convert to integer
    return event_numbers



def fetch_all_team_numbers(season: int):
    """
    Fetch all team numbers from the paginated 'teams' endpoint.
    
    Args:
        season (int): The season year for the NFL.

    Returns:
        list: A list of team numbers as integers.
    """
    team_numbers = []
    page_index = 1
    while True:
        endpoint = ENDPOINTS["teams"].format(season=season)
        try:
            response = client.get(endpoint)
            # Extract team numbers from the response
            for item in response.get("items", []):
                match = re.search(r"teams/(\d+)", item.get("$ref", ""))
                if match:
                    team_numbers.append(int(match.group(1)))
        except Exception as e:
            print(f"Error fetching team numbers: {e}")
            continue

        # Check if there are more pages
        if page_index >= response.get("pageCount", 0):
            break
        page_index += 1

    return team_numbers


def fetch_team_details(season: int, team_numbers: list):
    """
    Fetch team details for each team number and map IDs to names.

    Args:
        season (int): The season year for the NFL.
        team_numbers (list): List of team numbers to fetch details for.

    Returns:
        dict: A mapping of team IDs to their display names.
    """
    team_details = {}
    for team_number in team_numbers:
        endpoint = ENDPOINTS["specific_team"].format(season=season, team_id=team_number)
        try:
            response = client.get(endpoint)

            # Map team ID to display name
            team_id = response.get("id")
            display_name = response.get("displayName")
            if team_id and display_name:
                team_details[team_id] = display_name
        except Exception as e:
            print(f"Error fetching team details: {e}")
            continue

    return team_details

def fetch_weeks_by_season(season: int, season_type: str):
    endpoint = ENDPOINTS["weeks_by_season"].format(season=season, season_type=season_type)
    try:
        response = client.get(endpoint)
        return extract_week_numbers(response)
    except Exception as e:
        print(f"Error fetching weeks by season: {e}")
        return []

def fetch_games_per_week(season: int, season_type: str, week_number: int):
    endpoint = ENDPOINTS["events_by_week"].format(season=season, season_type=season_type, week_number=week_number)
    try:
        response = client.get(endpoint)
        return extract_event_numbers(response)
    except Exception as e:
        print(f"Error fetching games per week: {e}")
        return []

def fetch_game_details(event_id: int):
    endpoint = ENDPOINTS["specific_event"].format(event_id=event_id)
    try:
        response = client.get(endpoint)
        competition = response.get("competitions", [{}])[0]
        competition_id = competition.get("id")

        # Extract competitor IDs
        competitors = competition.get("competitors", [])
        competitor_ids = [competitor.get("id") for competitor in competitors]

        return {
            "competition_id": competition_id,
            "competitor_ids": competitor_ids
        }
    except Exception as e:
        print(f"Error fetching game details: {e}")
        return {}


def fetch_game_stats_by_player(event_id: int, competition_id: int, competitor_id: int, athlete_id: int):
    endpoint = ENDPOINTS["game_stats_by_player"].format(event_id=event_id, competition_id=competition_id, competitor_id=competitor_id, athlete_id=athlete_id)
    try:
        return client.get(endpoint)
    except Exception as e:
        print(f"Error fetching game stats by player: {e}")
        return {}



def fetch_game_stats_by_team(event_id: int, competition_id: int, competitor_id: int):
    """
    Fetch team-level game stats for a specific event, competition, and competitor (team).
    """
    endpoint = ENDPOINTS["game_stats_by_team"].format(
        event_id=event_id, 
        competition_id=competition_id, 
        competitor_id=competitor_id
    )
    try:
        return client.get(endpoint)
    except Exception as e:
        print(f"Error fetching game stats by team: {e}")
        return {}


def fetch_roster_by_competitor(event_id: int, competition_id: int, competitor_id: int):
    """
    Fetch the roster for a given competitor in a specific event's competition.
    """
    endpoint = ENDPOINTS["roster_by_competitor_and_game"].format(
        event_id=event_id,
        competition_id=competition_id,
        competitor_id=competitor_id
    )
    try:
        return client.get(endpoint)
    except Exception as e:
        print(f"Error fetching roster: {e}")
        return {}

def fetch_player_details(season: int, athlete_id: int):
    endpoint = ENDPOINTS["specific_player"].format(season=season, athlete_id=athlete_id)
    try:
        return client.get(endpoint)
    except Exception as e:
        print(f"Error fetching player details: {e}")
        return {}