"""
Deep Current Football — Sportmonks Season Baseline Builder
server/sm_baseline.py

REWRITE: Pulls players by team roster, not global player search.
This keeps the data scoped to current season squads only.

Flow:
  1. Get all teams in a league/season
  2. For each team, get their squad
  3. For each player in squad, get their season stats
  4. Calculate and store per-90 baselines

Auth: Authorization header
URL:  BASE_URL + endpoint + ?filters=...&include=...
Sep:  semicolons for multiple includes
"""

import os
import requests
from datetime import datetime
from supabase import create_client

SPORTMONKS_TOKEN = os.environ.get("SPORTMONKS_API_TOKEN")
SUPABASE_URL     = os.environ.get("SUPABASE_URL")
SUPABASE_KEY     = os.environ.get("SUPABASE_SERVICE_KEY")
BASE_URL         = "https://api.sportmonks.com/v3/football"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

LEAGUES = [
    {"league_id": 8,    "season_id": 25583, "name": "Premier League"},
    {"league_id": 9,    "season_id": 25648, "name": "Championship"},
    {"league_id": 564,  "season_id": 25659, "name": "La Liga"},
    {"league_id": 384,  "season_id": 25533, "name": "Serie A"},
    {"league_id": 82,   "season_id": 25646, "name": "Bundesliga"},
    {"league_id": 301,  "season_id": 25651, "name": "Ligue 1"},
    {"league_id": 779,  "season_id": 26720, "name": "MLS"},
    {"league_id": 1356, "season_id": 26529, "name": "A-League Men"},
]

TYPE_IDS = {
    "KEY_PASSES":                 117,
    "ACCURATE_CROSSES":           99,
    "TOTAL_CROSSES":              98,
    "ACCURATE_PASSES":            116,
    "PASSES":                     80,
    "ACCURATE_PASSES_PERCENTAGE": 1584,
    "SHOTS_ON_TARGET":            86,
    "SHOTS_TOTAL":                42,
    "GOALS":                      52,
    "MINUTES_PLAYED":             119,
    "SUCCESSFUL_DRIBBLES":        109,
    "FOULS_DRAWN":                96,
    "LONG_BALLS_WON":             123,
}


def _sm_get(endpoint, filters=None, include=None, extra_params=None, page=None):
    """Core Sportmonks API GET helper using Authorization header."""
    headers = {"Authorization": SPORTMONKS_TOKEN}
    params = {}
    if filters:
        params["filters"] = filters
    if include:
        params["include"] = include
    if page:
        params["page"] = page
    if extra_params:
        params.update(extra_params)

    url = f"{BASE_URL}{endpoint}"
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def _extract_stat(details, type_id):
    """Extract a stat value from player details array by type_id."""
    if not details:
        return None
    for d in details:
        if not isinstance(d, dict):
            continue
        if d.get("type_id") == type_id:
            val = d.get("value", {})
            if isinstance(val, dict):
                return (val.get("total") or
                        val.get("average") or
                        val.get("expected"))
            return val
    return None


def _get_teams_for_season(season_id):
    """Get all team IDs participating in a season."""
    try:
        resp = _sm_get(
            endpoint=f"/teams/seasons/{season_id}",
        )
        data = resp.get("data", [])
        if isinstance(data, dict):
            data = data.get("data", [])
        return [t.get("id") for t in data if t.get("id")]
    except Exception as e:
        print(f"    Error getting teams for season {season_id}: {e}")
        return []


def _get_squad_for_team(team_id, season_id):
    """Get all players in a team's squad for a season."""
    try:
        resp = _sm_get(
            endpoint=f"/teams/{team_id}",
            include="players",
            filters=f"playerStatisticSeasons:{season_id}",
        )
        data = resp.get("data", {})
        players = data.get("players", [])
        if isinstance(players, dict):
            players = players.get("data", [])
        return players
    except Exception as e:
        print(f"    Error getting squad for team {team_id}: {e}")
        return []


def _get_player_season_stats(player_id, season_id):
    """Get a single player's season stats with details."""
    try:
        resp = _sm_get(
            endpoint=f"/players/{player_id}",
            include="statistics.details.type",
            filters=f"playerStatisticSeasons:{season_id}",
        )
        data = resp.get("data", {})
        stats = data.get("statistics", [])
        if isinstance(stats, dict):
            stats = stats.get("data", [])

        # Find the matching season
        for s in stats:
            if s.get("season_id") == season_id and s.get("has_values"):
                details = s.get("details", [])
                if isinstance(details, dict):
                    details = details.get("data", [])
                return data, details

        return data, []
    except Exception as e:
        return {}, []


def _calculate_baseline(player_data, details, season_id):
    """Calculate per-90 baselines from a player's season stats."""
    minutes      = _extract_stat(details, TYPE_IDS["MINUTES_PLAYED"])
    key_passes   = _extract_stat(details, TYPE_IDS["KEY_PASSES"])
    acc_crosses  = _extract_stat(details, TYPE_IDS["ACCURATE_CROSSES"])
    acc_passes   = _extract_stat(details, TYPE_IDS["ACCURATE_PASSES"])
    tot_passes   = _extract_stat(details, TYPE_IDS["PASSES"])
    pass_acc_pct = _extract_stat(details, TYPE_IDS["ACCURATE_PASSES_PERCENTAGE"])
    sot          = _extract_stat(details, TYPE_IDS["SHOTS_ON_TARGET"])
    goals        = _extract_stat(details, TYPE_IDS["GOALS"])
    dribbles     = _extract_stat(details, TYPE_IDS["SUCCESSFUL_DRIBBLES"])

    # Skip players with less than 90 minutes
    if not minutes or minutes < 90:
        return None

    nineties = minutes / 90

    def per90(val):
        return round(val / nineties, 3) if val else 0.0

    # Pass accuracy
    if pass_acc_pct is not None:
        pass_accuracy = round(
            pass_acc_pct / 100 if pass_acc_pct > 1 else pass_acc_pct, 4
        )
    elif acc_passes and tot_passes and tot_passes > 0:
        pass_accuracy = round(acc_passes / tot_passes, 4)
    else:
        pass_accuracy = None

    return {
        "player_id":              player_data.get("id"),
        "player_name":            player_data.get("display_name") or player_data.get("name"),
        "season_id":              season_id,
        "position_id":            player_data.get("position_id"),
        "detailed_position_id":   player_data.get("detailed_position_id"),
        "minutes_played":         minutes,
        "nineties":               round(nineties, 2),
        "key_passes_total":       key_passes or 0,
        "acc_crosses_total":      acc_crosses or 0,
        "sot_total":              sot or 0,
        "goals_total":            goals or 0,
        "kp_per90":               per90(key_passes),
        "acc_cross_per90":        per90(acc_crosses),
        "sot_per90":              per90(sot),
        "goals_per90":            per90(goals),
        "dribbles_per90":         per90(dribbles),
        "pass_accuracy_baseline": pass_accuracy,
        "xg_per90":               None,
        "xgot_per90":             None,
        "last_updated":           datetime.utcnow().isoformat(),
    }


def _upsert_baseline(baseline, league_id):
    """Upsert a player baseline record into Supabase."""
    record = {**baseline, "league_id": league_id}
    existing = supabase.table("player_baselines")\
        .select("id")\
        .eq("player_id", baseline["player_id"])\
        .eq("season_id", baseline["season_id"])\
        .execute()

    if existing.data:
        supabase.table("player_baselines")\
            .update(record)\
            .eq("player_id", baseline["player_id"])\
            .eq("season_id", baseline["season_id"])\
            .execute()
    else:
        supabase.table("player_baselines").insert(record).execute()


# ── PUBLIC FUNCTIONS ──────────────────────────────────────────────────────────

def bootstrap_baselines(leagues=None):
    """
    Seed all player baselines by pulling team rosters.
    Much more controlled than global player search.
    Roughly 20-30 players per team × 20 teams = ~400-600 players per league.
    """
    target_leagues = leagues or LEAGUES

    print("\n" + "="*60)
    print("  DEEP CURRENT — BASELINE BOOTSTRAP (teams approach)")
    print("="*60)

    total_stored = 0

    for league in target_leagues:
        print(f"\n  {league['name']} (season {league['season_id']})")

        # Step 1 — get all teams in this season
        team_ids = _get_teams_for_season(league["season_id"])
        print(f"    Teams found: {len(team_ids)}")

        if not team_ids:
            print(f"    ⚠️  No teams found — check season_id")
            continue

        stored = skipped = 0

        # Step 2 — for each team get their squad
        for team_id in team_ids:
            squad = _get_squad_for_team(team_id, league["season_id"])

            for player in squad:
                player_id = player.get("id") or player.get("player_id")
                if not player_id:
                    continue

                # Step 3 — get this player's season stats
                player_data, details = _get_player_season_stats(
                    player_id, league["season_id"]
                )

                if not details:
                    skipped += 1
                    continue

                # Use squad entry for position if player_data missing it
                if not player_data.get("position_id") and player.get("position_id"):
                    player_data["position_id"] = player.get("position_id")
                if not player_data.get("id"):
                    player_data["id"] = player_id

                baseline = _calculate_baseline(
                    player_data, details, league["season_id"]
                )

                if baseline:
                    _upsert_baseline(baseline, league["league_id"])
                    stored += 1
                else:
                    skipped += 1

        print(f"    ✅ Stored: {stored} | Skipped: {skipped}")
        total_stored += stored

    print(f"\n  TOTAL BASELINES STORED: {total_stored}")
    print("="*60)


def refresh_baselines(leagues=None):
    """Weekly refresh. Railway cron: 0 3 * * 1"""
    bootstrap_baselines(leagues)


def get_player_baseline(player_id, season_id):
    """Get a single player's baseline from Supabase."""
    res = supabase.table("player_baselines")\
        .select("*")\
        .eq("player_id", player_id)\
        .eq("season_id", season_id)\
        .execute()
    return res.data[0] if res.data else None


def get_league_baselines(league_id, season_id):
    """Get all baselines for a league/season."""
    res = supabase.table("player_baselines")\
        .select("*")\
        .eq("league_id", league_id)\
        .eq("season_id", season_id)\
        .execute()
    return res.data or []
