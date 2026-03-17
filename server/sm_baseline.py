"""
Deep Current Football — Sportmonks Season Baseline Builder
server/sm_baseline.py

Auth: Authorization header (official Sportmonks pattern)
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
    {"league_id": 8,    "season_id": 23614, "name": "Premier League"},
    {"league_id": 5,    "season_id": 23599, "name": "Championship"},
    {"league_id": 564,  "season_id": 23686, "name": "La Liga"},
    {"league_id": 384,  "season_id": 23615, "name": "Serie A"},
    {"league_id": 82,   "season_id": 23538, "name": "Bundesliga"},
    {"league_id": 301,  "season_id": 23611, "name": "Ligue 1"},
    {"league_id": 1351, "season_id": 23700, "name": "MLS"},
    {"league_id": 174,  "season_id": 23645, "name": "A-League Men"},
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
    """
    Core Sportmonks API GET helper.

    Uses Authorization header — official Sportmonks recommended auth pattern.
    Filters and includes passed as separate query params.
    Multiple includes separated by semicolons.
    """
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


def _get_players_for_season(season_id):
    """Pull all player season stats with pagination."""
    print(f"    Fetching players for season {season_id}...")
    all_players = []
    page = 1

    while True:
        try:
            resp = _sm_get(
                endpoint="/players",
                filters=f"playerStatisticSeasons:{season_id}",
                include="statistics.details.type",
                extra_params={"per_page": 25},
                page=page,
            )
        except Exception as e:
            print(f"    Error on page {page}: {e}")
            break

        data = resp.get("data", [])
        if not data:
            break

        all_players.extend(data)

        if not resp.get("pagination", {}).get("has_more", False):
            break

        page += 1
        print(f"    Page {page - 1} — {len(all_players)} players so far...")

    return all_players


def _calculate_baseline(player, season_id):
    """Extract per-90 baselines from a player's season stats."""
    stats = player.get("statistics", [])
    if isinstance(stats, dict):
        stats = stats.get("data", [])

    season_stat = next(
        (s for s in stats if s.get("season_id") == season_id and s.get("has_values")),
        None
    )
    if not season_stat:
        return None

    details = season_stat.get("details", [])
    if isinstance(details, dict):
        details = details.get("data", [])

    minutes      = _extract_stat(details, TYPE_IDS["MINUTES_PLAYED"])
    key_passes   = _extract_stat(details, TYPE_IDS["KEY_PASSES"])
    acc_crosses  = _extract_stat(details, TYPE_IDS["ACCURATE_CROSSES"])
    acc_passes   = _extract_stat(details, TYPE_IDS["ACCURATE_PASSES"])
    tot_passes   = _extract_stat(details, TYPE_IDS["PASSES"])
    pass_acc_pct = _extract_stat(details, TYPE_IDS["ACCURATE_PASSES_PERCENTAGE"])
    sot          = _extract_stat(details, TYPE_IDS["SHOTS_ON_TARGET"])
    goals        = _extract_stat(details, TYPE_IDS["GOALS"])
    dribbles     = _extract_stat(details, TYPE_IDS["SUCCESSFUL_DRIBBLES"])

    if not minutes or minutes < 90:
        return None

    nineties = minutes / 90

    def per90(val):
        return round(val / nineties, 3) if val else 0.0

    if pass_acc_pct is not None:
        pass_accuracy = round(pass_acc_pct / 100 if pass_acc_pct > 1 else pass_acc_pct, 4)
    elif acc_passes and tot_passes and tot_passes > 0:
        pass_accuracy = round(acc_passes / tot_passes, 4)
    else:
        pass_accuracy = None

    return {
        "player_id":              player.get("id"),
        "player_name":            player.get("display_name") or player.get("name"),
        "season_id":              season_id,
        "position_id":            player.get("position_id"),
        "detailed_position_id":   player.get("detailed_position_id"),
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


def bootstrap_baselines(leagues=None):
    """Seed all player baselines for all leagues. Run once per season."""
    target_leagues = leagues or LEAGUES
    print("\n" + "="*60)
    print("  DEEP CURRENT — BASELINE BOOTSTRAP")
    print("="*60)
    total_stored = 0

    for league in target_leagues:
        print(f"\n  {league['name']} (season {league['season_id']})")
        players = _get_players_for_season(league["season_id"])
        print(f"    {len(players)} players returned")
        stored = skipped = 0

        for player in players:
            baseline = _calculate_baseline(player, league["season_id"])
            if baseline:
                _upsert_baseline(baseline, league["league_id"])
                stored += 1
            else:
                skipped += 1

        print(f"    ✅ Stored: {stored} | Skipped: {skipped}")
        total_stored += stored

    print(f"\n  TOTAL: {total_stored}")
    print("="*60)


def refresh_baselines(leagues=None):
    """Weekly refresh. Cron: 0 3 * * 1"""
    bootstrap_baselines(leagues)


def get_player_baseline(player_id, season_id):
    res = supabase.table("player_baselines")\
        .select("*")\
        .eq("player_id", player_id)\
        .eq("season_id", season_id)\
        .execute()
    return res.data[0] if res.data else None


def get_league_baselines(league_id, season_id):
    res = supabase.table("player_baselines")\
        .select("*")\
        .eq("league_id", league_id)\
        .eq("season_id", season_id)\
        .execute()
    return res.data or []
