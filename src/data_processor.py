def process_rosters(season_data):
    # season_data should be a list of players with team affiliations from SportsDataIO
    # Convert this to a structure keyed by team.
    team_dict = {}
    for player in season_data:
        team = player.get("Team")
        if team not in team_dict:
            team_dict[team] = {
                "season": player.get("Season"),
                "team": team,
                "players": []
            }
        team_dict[team]["players"].append({
            "player_id": player.get("PlayerID"),
            "name": f"{player.get('FirstName','')} {player.get('LastName','')}".strip(),
            "position": player.get("Position"),
            "status": player.get("Status"),
        })
    return list(team_dict.values())

def process_team_stats(team_stats_data, season):
    # Typically returns an array of team season stats
    processed = []
    for t in team_stats_data:
        processed.append({
            "season": season,
            "team": t.get("Team"),
            "stats": {
                # Add whichever fields are relevant (check the API fields)
                "points_scored": t.get("PointsFor"),
                "points_allowed": t.get("PointsAgainst"),
                "offensive_yards": t.get("OffensiveYards"),
                "defensive_yards_allowed": t.get("DefensiveYards"),
                # ... more fields as needed ...
            }
        })
    return processed

def process_player_stats(player_stats_data, season):
    processed = []
    for p in player_stats_data:
        processed.append({
            "season": season,
            "team": p.get("Team"),
            "player_id": p.get("PlayerID"),
            "stats": {
                # Add relevant player stats fields
                "touchdowns": p.get("TouchdownsTotal"),
                "passing_yards": p.get("PassingYards"),
                "rushing_yards": p.get("RushingYards"),
                "receiving_yards": p.get("ReceivingYards"),
                # etc.
            }
        })
    return processed
