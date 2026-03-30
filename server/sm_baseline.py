"""
Deep Current Football — Sportmonks Season Baseline Builder
server/sm_baseline.py

Uses /squads/seasons/{season_id}/teams/{team_id}?include=player.statistics.details
Response structure per player:
  entry -> player -> statistics[] -> filter by season_id -> details[]

~21 API calls per league (1 for teams + 20 for squads).
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

# Confirmed type_ids from Lewis-Skelly validation
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
    "ASSISTS":                    79,
    "BIG_CHANCES_CREATED":        580,
    "BIG_CHANCES_MISSED":         581,
    "APPEARANCES":                83,
}


# ── CORE HTTP HELPER ──────────────────────────────────────────────────────────

def _sm_get(endpoint, filters=None, include=None, extra_params=None, page=None):
    """Sportmonks API GET using Authorization header."""
    headers = {"Authorization": SPORTMONKS_TOKEN}
    params = {}
    if filters:      params["filters"] = filters
    if include:      params["include"] = include
    if page:         params["page"] = page
    if extra_params: params.update(extra_params)

    response = requests.get(
        f"{BASE_URL}{endpoint}",
        headers=headers,
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response.json()


# ── STAT EXTRACTOR ────────────────────────────────────────────────────────────

def _extract_stat(details, type_id):
    """
    Extract a stat value from a details array by type_id.
    details = list of {"type_id": X, "value": {"total": Y}} objects
    """
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


# ── TEAM FETCHER ──────────────────────────────────────────────────────────────

def _get_teams_for_season(season_id):
    """Get all teams (id + name) for a season."""
    try:
        resp = _sm_get(endpoint=f"/teams/seasons/{season_id}")
        data = resp.get("data", [])
        if isinstance(data, dict):
            data = data.get("data", [])
        return [
            {"id": t.get("id"), "name": t.get("name") or t.get("short_code") or ""}
            for t in data if t.get("id")
        ]
    except Exception as e:
        print(f"    Error getting teams: {e}")
        return []


# ── SQUAD FETCHER ─────────────────────────────────────────────────────────────

def _get_squad_with_stats(team_id, season_id):
    """
    Get full squad for a team with player stats in ONE API call.

    Endpoint: /squads/seasons/{season_id}/teams/{team_id}
    Include:  player.statistics.details  (3 levels — max allowed)
    """
    try:
        resp = _sm_get(
            endpoint=f"/squads/seasons/{season_id}/teams/{team_id}",
            include="player.statistics.details",
        )
        data = resp.get("data", [])
        if isinstance(data, dict):
            data = data.get("data", [])
        return data
    except Exception as e:
        print(f"    Error getting squad for team {team_id}: {e}")
        return []


# ── BASELINE CALCULATOR ───────────────────────────────────────────────────────

def _calculate_baseline(player, details, season_id, team_id=None, team_name=None):
    """
    Calculate per-90 baselines from a player object and their season details.
    """
    minutes      = _extract_stat(details, TYPE_IDS["MINUTES_PLAYED"])
    key_passes   = _extract_stat(details, TYPE_IDS["KEY_PASSES"])
    acc_crosses  = _extract_stat(details, TYPE_IDS["ACCURATE_CROSSES"])
    acc_passes   = _extract_stat(details, TYPE_IDS["ACCURATE_PASSES"])
    tot_passes   = _extract_stat(details, TYPE_IDS["PASSES"])
    pass_acc_pct = _extract_stat(details, TYPE_IDS["ACCURATE_PASSES_PERCENTAGE"])
    sot          = _extract_stat(details, TYPE_IDS["SHOTS_ON_TARGET"])
    shots_total  = _extract_stat(details, TYPE_IDS["SHOTS_TOTAL"])
    goals        = _extract_stat(details, TYPE_IDS["GOALS"])
    dribbles     = _extract_stat(details, TYPE_IDS["SUCCESSFUL_DRIBBLES"])
    assists      = _extract_stat(details, TYPE_IDS["ASSISTS"])
    big_chances  = _extract_stat(details, TYPE_IDS["BIG_CHANCES_CREATED"])
    appearances  = _extract_stat(details, TYPE_IDS["APPEARANCES"])

    # Must have played at least 90 minutes
    if not minutes or minutes < 90:
        return None

    nineties = minutes / 90

    def per90(val):
        return round(val / nineties, 3) if val else 0.0

    if pass_acc_pct is not None:
        pass_accuracy = round(
            pass_acc_pct / 100 if pass_acc_pct > 1 else pass_acc_pct, 4
        )
    elif acc_passes and tot_passes and tot_passes > 0:
        pass_accuracy = round(acc_passes / tot_passes, 4)
    else:
        pass_accuracy = None

    return {
        "player_id":              player.get("id"),
        "player_name":            player.get("display_name") or player.get("name"),
        "team_id":                team_id,
        "team_name":              team_name,
        "season_id":              season_id,
        "position_id":            player.get("position_id"),
        "detailed_position_id":   player.get("detailed_position_id"),
        "minutes_played":         minutes,
        "nineties":               round(nineties, 2),
        "key_passes_total":       key_passes   or 0,
        "acc_crosses_total":      acc_crosses  or 0,
        "sot_total":              sot          or 0,
        "shots_total":            shots_total  or 0,
        "goals_total":            goals        or 0,
        "assists_total":          assists      or 0,
        "big_chances_created":    big_chances  or 0,
        "appearances":            appearances  or 0,
        "kp_per90":               per90(key_passes),
        "acc_cross_per90":        per90(acc_crosses),
        "sot_per90":              per90(sot),
        "shots_per90":            per90(shots_total),
        "goals_per90":            per90(goals),
        "assists_per90":          per90(assists),
        "big_chances_per90":      per90(big_chances),
        "dribbles_per90":         per90(dribbles),
        "pass_accuracy_baseline": pass_accuracy,
        "xg_per90":               None,
        "xgot_per90":             None,
        "last_updated":           datetime.utcnow().isoformat(),
    }


# ── SUPABASE UPSERT ───────────────────────────────────────────────────────────

def _upsert_baseline(baseline, league_id):
    """Insert or update a player baseline in Supabase."""
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
    Seed all player baselines for all covered leagues.

    Flow per league:
      1. Get all teams (id + name)      (~1 API call)
      2. For each team get squad
         with player stats nested        (~1 API call per team)
      3. Filter stats for current season
      4. Store in player_baselines with team_id, team_name,
         assists_total, big_chances_created, appearances

    Total: ~21 API calls per league, ~170 for all 8 leagues.
    """
    target_leagues = leagues or LEAGUES

    print("\n" + "="*60)
    print("  DEEP CURRENT — BASELINE BOOTSTRAP")
    print("="*60)

    total_stored = 0

    for league in target_leagues:
        print(f"\n  {league['name']} (season {league['season_id']})")

        teams = _get_teams_for_season(league["season_id"])
        print(f"    Teams found: {len(teams)}")

        if not teams:
            print(f"    ⚠️  No teams — check season_id")
            continue

        stored = skipped = 0

        for i, team in enumerate(teams):
            team_id   = team["id"]
            team_name = team["name"]

            squad = _get_squad_with_stats(team_id, league["season_id"])

            for entry in squad:
                player = entry.get("player", {})
                if isinstance(player, dict) and "data" in player:
                    player = player["data"]

                if not player or not player.get("id"):
                    skipped += 1
                    continue

                statistics = player.get("statistics", [])
                if isinstance(statistics, dict):
                    statistics = statistics.get("data", [])

                season_stat = next(
                    (s for s in statistics
                     if s.get("season_id") == league["season_id"]
                     and s.get("has_values")),
                    None
                )

                if not season_stat:
                    skipped += 1
                    continue

                details = season_stat.get("details", [])
                if isinstance(details, dict):
                    details = details.get("data", [])

                if not details:
                    skipped += 1
                    continue

                baseline = _calculate_baseline(
                    player, details, league["season_id"],
                    team_id=team_id,
                    team_name=team_name,
                )

                if baseline:
                    _upsert_baseline(baseline, league["league_id"])
                    stored += 1
                else:
                    skipped += 1

            print(f"    Team {i+1}/{len(teams)} ({team_name}) done")

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
