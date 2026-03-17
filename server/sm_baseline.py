"""
Deep Current Football — Sportmonks Season Baseline Builder
server/sm_baseline.py

FINAL VERSION: Uses /statistics/seasons/players/{season_id}
Pulls ALL player stats for a season in one paginated call.
~10-20 API calls per league total.

Also updates sm_scorer to use /expected/lineups endpoint for xG.
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


def _get_all_season_player_stats(season_id):
    """
    Pull ALL player season statistics in paginated calls.
    Uses /statistics/seasons/players/{season_id} endpoint.
    Returns list of player stat records with details.
    ~10-20 API calls total vs ~500 before.
    """
    all_stats = []
    page = 1

    while True:
        try:
            resp = _sm_get(
                endpoint=f"/statistics/seasons/players/{season_id}",
                include="player;details.type",
                extra_params={"per_page": 50},
                page=page,
            )
        except Exception as e:
            print(f"    Error on page {page}: {e}")
            break

        data = resp.get("data", [])
        if not data:
            break

        all_stats.extend(data)

        pagination = resp.get("pagination", {})
        if not pagination.get("has_more", False):
            break

        page += 1
        if page % 5 == 0:
            print(f"    Page {page - 1} — {len(all_stats)} records so far...")

    return all_stats


def _calculate_baseline_from_stat(stat_record, season_id):
    """
    Calculate per-90 baselines from a season stat record.
    stat_record is a PlayerStatistic object from the season stats endpoint.
    """
    # Get player info from nested player object
    player = stat_record.get("player", {})
    if isinstance(player, dict) and "data" in player:
        player = player["data"]

    player_id   = stat_record.get("player_id") or (player.get("id") if player else None)
    player_name = None
    position_id = None
    detailed_position_id = None

    if player:
        player_name          = player.get("display_name") or player.get("name")
        position_id          = player.get("position_id")
        detailed_position_id = player.get("detailed_position_id")

    if not player_id:
        return None

    # Get details
    details = stat_record.get("details", [])
    if isinstance(details, dict):
        details = details.get("data", [])

    if not details:
        return None

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
        pass_accuracy = round(
            pass_acc_pct / 100 if pass_acc_pct > 1 else pass_acc_pct, 4
        )
    elif acc_passes and tot_passes and tot_passes > 0:
        pass_accuracy = round(acc_passes / tot_passes, 4)
    else:
        pass_accuracy = None

    return {
        "player_id":              player_id,
        "player_name":            player_name,
        "season_id":              season_id,
        "position_id":            position_id,
        "detailed_position_id":   detailed_position_id,
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


def bootstrap_baselines(leagues=None):
    """
    Seed all player baselines using season statistics endpoint.
    Most efficient approach — ~10-20 API calls per league.
    """
    target_leagues = leagues or LEAGUES

    print("\n" + "="*60)
    print("  DEEP CURRENT — BASELINE BOOTSTRAP (season stats)")
    print("="*60)

    total_stored = 0

    for league in target_leagues:
        print(f"\n  {league['name']} (season {league['season_id']})")

        # Pull all player stats for this season in one paginated call
        stat_records = _get_all_season_player_stats(league["season_id"])
        print(f"    Total stat records: {len(stat_records)}")

        stored = skipped = 0

        for record in stat_records:
            baseline = _calculate_baseline_from_stat(record, league["season_id"])
            if baseline:
                _upsert_baseline(baseline, league["league_id"])
                stored += 1
            else:
                skipped += 1

        print(f"    ✅ Stored: {stored} | Skipped (< 90 min): {skipped}")
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
