"""
Deep Current Football — Positional Concessions System
server/positional_concessions.py

Tracks goals, assists, big chances, shots, and SOT conceded by position.
Includes home/away splits for more accurate matchup multipliers.

v4 — Granular position tracking with confirmed SM position IDs
"""

import os
import requests
from datetime import datetime
from collections import defaultdict
from supabase import create_client

SPORTMONKS_TOKEN = os.environ.get("SPORTMONKS_API_TOKEN")
SUPABASE_URL     = os.environ.get("SUPABASE_URL")
SUPABASE_KEY     = os.environ.get("SUPABASE_SERVICE_KEY")
BASE_URL         = "https://api.sportmonks.com/v3/football"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── POSITION MAPPINGS ─────────────────────────────────────────────────────────

BROAD_POSITION_MAP = {
    24: "GK", 25: "DEF", 26: "MID", 27: "FWD",
}

# Confirmed against SM documentation and player validation
# (detailed_position_id) → (position_code, broad_position)
GRANULAR_POSITION_MAP = {
    24:  ("GK",  "GK"),
    148: ("CB",  "DEF"),
    154: ("RB",  "DEF"),
    155: ("LB",  "DEF"),
    149: ("CDM", "MID"),
    153: ("CM",  "MID"),
    150: ("CAM", "MID"),
    157: ("LM",  "MID"),
    158: ("RM",  "MID"),
    151: ("ST",  "FWD"),
    163: ("SS",  "FWD"),
    152: ("LW",  "FWD"),
    156: ("RW",  "FWD"),
}

# Broad fallback map for position_codes → broad group
# Used when looking up granular multipliers
BROAD_MAP = {
    "GK":  "GK",
    "CB":  "DEF",
    "RB":  "DEF",
    "LB":  "DEF",
    "CDM": "MID",
    "CM":  "MID",
    "CAM": "MID",
    "LM":  "MID",
    "RM":  "MID",
    "ST":  "FWD",
    "SS":  "FWD",
    "LW":  "FWD",
    "RW":  "FWD",
    # Legacy broad codes
    "DEF": "DEF",
    "MID": "MID",
    "FWD": "FWD",
}

THRESHOLD_HIGH   = 2.0
THRESHOLD_MEDIUM = 1.5

# ── MINIMUM SAMPLE THRESHOLDS ─────────────────────────────────────────────────
# Broad positions
MIN_GP_BROAD = {
    "GK":  999,
    "DEF": 8,
    "MID": 5,
    "FWD": 5,
}

MIN_CONCESSIONS_BROAD = {
    "GK":  999,
    "DEF": 2,
    "MID": 2,
    "FWD": 2,
}

# Granular positions — higher threshold needed for thin position groups
MIN_GP_GRANULAR = {
    "GK":  999,
    "CB":  8,
    "RB":  6,
    "LB":  6,
    "CDM": 6,
    "CM":  5,
    "CAM": 5,
    "LM":  5,
    "RM":  5,
    "ST":  5,
    "SS":  5,
    "LW":  5,
    "RW":  5,
}

MIN_CONCESSIONS_GRANULAR = {
    "GK":  999,
    "CB":  2,
    "RB":  1,
    "LB":  1,
    "CDM": 1,
    "CM":  2,
    "CAM": 2,
    "LM":  1,
    "RM":  1,
    "ST":  2,
    "SS":  1,
    "LW":  2,
    "RW":  2,
}

# Stat type IDs for shots/SOT from lineup details
STAT_TYPE_SHOTS_TOTAL = 42
STAT_TYPE_SOT         = 86


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _sm_get(endpoint, params=None):
    headers = {"Authorization": SPORTMONKS_TOKEN}
    p = {}
    if params: p.update(params)
    r = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, params=p, timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])


def _get_position_info(position_id, detailed_position_id):
    if detailed_position_id and detailed_position_id in GRANULAR_POSITION_MAP:
        return GRANULAR_POSITION_MAP[detailed_position_id]
    if position_id in BROAD_POSITION_MAP:
        broad = BROAD_POSITION_MAP[position_id]
        return broad, broad
    return None, None


def _already_processed(fixture_id):
    res = supabase.table("concession_processed_fixtures")\
        .select("fixture_id").eq("fixture_id", fixture_id).execute()
    return len(res.data) > 0


def _mark_processed(fixture_id):
    supabase.table("concession_processed_fixtures")\
        .insert({"fixture_id": fixture_id}).execute()


def _flush_broad(team_id, season_id, league_id, accum):
    for broad_pos, delta in accum.items():
        existing = supabase.table("positional_concessions_broad")\
            .select("*")\
            .eq("team_id", team_id)\
            .eq("season_id", season_id)\
            .eq("broad_position", broad_pos)\
            .execute()

        if existing.data:
            row = existing.data[0]
            supabase.table("positional_concessions_broad").update({
                "goals_conceded":        row["goals_conceded"]                  + delta.get("goals", 0),
                "goals_conceded_home":   row.get("goals_conceded_home", 0)      + delta.get("goals_home", 0),
                "goals_conceded_away":   row.get("goals_conceded_away", 0)      + delta.get("goals_away", 0),
                "assists_conceded":      row["assists_conceded"]                + delta.get("assists", 0),
                "assists_conceded_home": row.get("assists_conceded_home", 0)    + delta.get("assists_home", 0),
                "assists_conceded_away": row.get("assists_conceded_away", 0)    + delta.get("assists_away", 0),
                "bc_conceded":           row.get("bc_conceded", 0)              + delta.get("bc", 0),
                "bc_conceded_home":      row.get("bc_conceded_home", 0)         + delta.get("bc_home", 0),
                "bc_conceded_away":      row.get("bc_conceded_away", 0)         + delta.get("bc_away", 0),
                "shots_conceded":        row.get("shots_conceded", 0)           + delta.get("shots", 0),
                "shots_conceded_home":   row.get("shots_conceded_home", 0)      + delta.get("shots_home", 0),
                "shots_conceded_away":   row.get("shots_conceded_away", 0)      + delta.get("shots_away", 0),
                "sot_conceded":          row.get("sot_conceded", 0)             + delta.get("sot", 0),
                "sot_conceded_home":     row.get("sot_conceded_home", 0)        + delta.get("sot_home", 0),
                "sot_conceded_away":     row.get("sot_conceded_away", 0)        + delta.get("sot_away", 0),
                "games_played":          row["games_played"]                    + delta.get("games", 0),
                "last_updated":          datetime.utcnow().isoformat()
            }).eq("id", row["id"]).execute()
        else:
            supabase.table("positional_concessions_broad").insert({
                "team_id":               team_id,
                "season_id":             season_id,
                "league_id":             league_id,
                "broad_position":        broad_pos,
                "goals_conceded":        delta.get("goals", 0),
                "goals_conceded_home":   delta.get("goals_home", 0),
                "goals_conceded_away":   delta.get("goals_away", 0),
                "assists_conceded":      delta.get("assists", 0),
                "assists_conceded_home": delta.get("assists_home", 0),
                "assists_conceded_away": delta.get("assists_away", 0),
                "bc_conceded":           delta.get("bc", 0),
                "bc_conceded_home":      delta.get("bc_home", 0),
                "bc_conceded_away":      delta.get("bc_away", 0),
                "shots_conceded":        delta.get("shots", 0),
                "shots_conceded_home":   delta.get("shots_home", 0),
                "shots_conceded_away":   delta.get("shots_away", 0),
                "sot_conceded":          delta.get("sot", 0),
                "sot_conceded_home":     delta.get("sot_home", 0),
                "sot_conceded_away":     delta.get("sot_away", 0),
                "games_played":          delta.get("games", 0),
            }).execute()


def _flush_granular(team_id, season_id, league_id, accum):
    for position_id, delta in accum.items():
        existing = supabase.table("positional_concessions_granular")\
            .select("*")\
            .eq("team_id", team_id)\
            .eq("season_id", season_id)\
            .eq("position_id", position_id)\
            .execute()

        if existing.data:
            row = existing.data[0]
            supabase.table("positional_concessions_granular").update({
                "goals_conceded":        row["goals_conceded"]                  + delta.get("goals", 0),
                "goals_conceded_home":   row.get("goals_conceded_home", 0)      + delta.get("goals_home", 0),
                "goals_conceded_away":   row.get("goals_conceded_away", 0)      + delta.get("goals_away", 0),
                "assists_conceded":      row["assists_conceded"]                + delta.get("assists", 0),
                "assists_conceded_home": row.get("assists_conceded_home", 0)    + delta.get("assists_home", 0),
                "assists_conceded_away": row.get("assists_conceded_away", 0)    + delta.get("assists_away", 0),
                "bc_conceded":           row.get("bc_conceded", 0)              + delta.get("bc", 0),
                "bc_conceded_home":      row.get("bc_conceded_home", 0)         + delta.get("bc_home", 0),
                "bc_conceded_away":      row.get("bc_conceded_away", 0)         + delta.get("bc_away", 0),
                "shots_conceded":        row.get("shots_conceded", 0)           + delta.get("shots", 0),
                "shots_conceded_home":   row.get("shots_conceded_home", 0)      + delta.get("shots_home", 0),
                "shots_conceded_away":   row.get("shots_conceded_away", 0)      + delta.get("shots_away", 0),
                "sot_conceded":          row.get("sot_conceded", 0)             + delta.get("sot", 0),
                "sot_conceded_home":     row.get("sot_conceded_home", 0)        + delta.get("sot_home", 0),
                "sot_conceded_away":     row.get("sot_conceded_away", 0)        + delta.get("sot_away", 0),
                "games_played":          row["games_played"]                    + delta.get("games", 0),
                "last_updated":          datetime.utcnow().isoformat()
            }).eq("id", row["id"]).execute()
        else:
            supabase.table("positional_concessions_granular").insert({
                "team_id":               team_id,
                "season_id":             season_id,
                "league_id":             league_id,
                "position_id":           position_id,
                "position_code":         delta["position_code"],
                "broad_position":        delta["broad_pos"],
                "goals_conceded":        delta.get("goals", 0),
                "goals_conceded_home":   delta.get("goals_home", 0),
                "goals_conceded_away":   delta.get("goals_away", 0),
                "assists_conceded":      delta.get("assists", 0),
                "assists_conceded_home": delta.get("assists_home", 0),
                "assists_conceded_away": delta.get("assists_away", 0),
                "bc_conceded":           delta.get("bc", 0),
                "bc_conceded_home":      delta.get("bc_home", 0),
                "bc_conceded_away":      delta.get("bc_away", 0),
                "shots_conceded":        delta.get("shots", 0),
                "shots_conceded_home":   delta.get("shots_home", 0),
                "shots_conceded_away":   delta.get("shots_away", 0),
                "sot_conceded":          delta.get("sot", 0),
                "sot_conceded_home":     delta.get("sot_home", 0),
                "sot_conceded_away":     delta.get("sot_away", 0),
                "games_played":          delta.get("games", 0),
            }).execute()


# ── CORE: PROCESS ONE FIXTURE ─────────────────────────────────────────────────

def process_fixture(fixture_id, season_id, league_id):
    if _already_processed(fixture_id):
        return

    try:
        fixture_data = _sm_get(
            f"fixtures/{fixture_id}",
            {"include": "events;lineups.details;participants"}
        )
    except Exception as e:
        print(f"  ERROR fetching fixture {fixture_id}: {e}")
        return

    if not fixture_data:
        return

    fixture = fixture_data if isinstance(fixture_data, dict) else fixture_data[0]

    lineups = fixture.get("lineups", [])
    if isinstance(lineups, dict): lineups = lineups.get("data", [])

    # Build position map AND extract per-player shots/SOT/BC from lineup details
    player_positions = {}
    player_bc        = {}
    player_shots     = {}
    player_sot       = {}

    for player in lineups:
        pid = player.get("player_id")
        if not pid:
            continue

        player_positions[pid] = {
            "position_id":          player.get("position_id"),
            "detailed_position_id": player.get("formation_position"),
            "team_id":              player.get("team_id"),
        }

        details = player.get("details", [])
        if isinstance(details, dict): details = details.get("data", [])

        for d in details:
            type_id = d.get("type_id")
            val     = d.get("data", {}).get("value", 0) or 0
            if type_id == 580:                  # big chances created
                player_bc[pid]    = player_bc.get(pid, 0)    + val
            elif type_id == STAT_TYPE_SHOTS_TOTAL:  # shots total (42)
                player_shots[pid] = player_shots.get(pid, 0) + val
            elif type_id == STAT_TYPE_SOT:          # shots on target (86)
                player_sot[pid]   = player_sot.get(pid, 0)   + val

    participants = fixture.get("participants", [])
    if isinstance(participants, dict): participants = participants.get("data", [])

    home_team_id = None
    away_team_id = None
    for p in participants:
        meta = p.get("meta", {})
        loc  = meta.get("location", "")
        if loc == "home": home_team_id = p.get("id")
        elif loc == "away": away_team_id = p.get("id")

    team_ids = [home_team_id, away_team_id]

    def get_opposing_team(scorer_team_id):
        for tid in team_ids:
            if tid and tid != scorer_team_id:
                return tid
        return None

    def is_home(team_id):
        return team_id == home_team_id

    broad_accum    = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    granular_accum = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    games_teams    = set()

    events = fixture.get("events", [])
    if isinstance(events, dict): events = events.get("data", [])

    if home_team_id: games_teams.add(home_team_id)
    if away_team_id: games_teams.add(away_team_id)

    # ── Goal events → track goals and assists conceded by position ────────────
    for event in events:
        if event.get("type_id") != 14:
            continue

        scorer_id     = event.get("player_id")
        assister_id   = event.get("related_player_id")
        scoring_team  = event.get("participant_id")
        opposing_team = get_opposing_team(scoring_team)

        if not opposing_team:
            continue

        opp_is_home = is_home(opposing_team)
        h = 1 if opp_is_home else 0
        a = 0 if opp_is_home else 1

        if scorer_id and scorer_id in player_positions:
            pos_info = player_positions[scorer_id]
            pos_code, broad = _get_position_info(
                pos_info["position_id"], pos_info["detailed_position_id"]
            )
            if broad:
                broad_accum[opposing_team][broad]["goals"]       += 1
                broad_accum[opposing_team][broad]["goals_home"]  += h
                broad_accum[opposing_team][broad]["goals_away"]  += a

            if pos_code and pos_code != broad and pos_info["detailed_position_id"]:
                did = pos_info["detailed_position_id"]
                granular_accum[opposing_team][did]["goals"]       += 1
                granular_accum[opposing_team][did]["goals_home"]  += h
                granular_accum[opposing_team][did]["goals_away"]  += a
                granular_accum[opposing_team][did]["position_code"] = pos_code
                granular_accum[opposing_team][did]["broad_pos"]     = broad

        if assister_id and assister_id in player_positions:
            pos_info = player_positions[assister_id]
            pos_code, broad = _get_position_info(
                pos_info["position_id"], pos_info["detailed_position_id"]
            )
            if broad:
                broad_accum[opposing_team][broad]["assists"]       += 1
                broad_accum[opposing_team][broad]["assists_home"]  += h
                broad_accum[opposing_team][broad]["assists_away"]  += a

            if pos_code and pos_code != broad and pos_info["detailed_position_id"]:
                did = pos_info["detailed_position_id"]
                granular_accum[opposing_team][did]["assists"]       += 1
                granular_accum[opposing_team][did]["assists_home"]  += h
                granular_accum[opposing_team][did]["assists_away"]  += a
                granular_accum[opposing_team][did]["position_code"] = pos_code
                granular_accum[opposing_team][did]["broad_pos"]     = broad

    # ── Big chances created → conceded by opposing team ───────────────────────
    for pid, bc_count in player_bc.items():
        if pid not in player_positions or bc_count == 0:
            continue
        pos_info      = player_positions[pid]
        scoring_team  = pos_info.get("team_id")
        opposing_team = get_opposing_team(scoring_team)
        if not opposing_team:
            continue
        opp_is_home = is_home(opposing_team)
        h = 1 if opp_is_home else 0
        a = 0 if opp_is_home else 1

        pos_code, broad = _get_position_info(
            pos_info["position_id"], pos_info["detailed_position_id"]
        )
        if broad:
            broad_accum[opposing_team][broad]["bc"]       += bc_count
            broad_accum[opposing_team][broad]["bc_home"]  += h * bc_count
            broad_accum[opposing_team][broad]["bc_away"]  += a * bc_count

        if pos_code and pos_code != broad and pos_info["detailed_position_id"]:
            did = pos_info["detailed_position_id"]
            granular_accum[opposing_team][did]["bc"]       += bc_count
            granular_accum[opposing_team][did]["bc_home"]  += h * bc_count
            granular_accum[opposing_team][did]["bc_away"]  += a * bc_count
            granular_accum[opposing_team][did]["position_code"] = pos_code
            granular_accum[opposing_team][did]["broad_pos"]     = broad

    # ── Shots total → conceded by opposing team ───────────────────────────────
    for pid, shot_count in player_shots.items():
        if pid not in player_positions or shot_count == 0:
            continue
        pos_info      = player_positions[pid]
        scoring_team  = pos_info.get("team_id")
        opposing_team = get_opposing_team(scoring_team)
        if not opposing_team:
            continue
        opp_is_home = is_home(opposing_team)
        h = 1 if opp_is_home else 0
        a = 0 if opp_is_home else 1

        pos_code, broad = _get_position_info(
            pos_info["position_id"], pos_info["detailed_position_id"]
        )
        if broad:
            broad_accum[opposing_team][broad]["shots"]       += shot_count
            broad_accum[opposing_team][broad]["shots_home"]  += h * shot_count
            broad_accum[opposing_team][broad]["shots_away"]  += a * shot_count

        if pos_code and pos_code != broad and pos_info["detailed_position_id"]:
            did = pos_info["detailed_position_id"]
            granular_accum[opposing_team][did]["shots"]       += shot_count
            granular_accum[opposing_team][did]["shots_home"]  += h * shot_count
            granular_accum[opposing_team][did]["shots_away"]  += a * shot_count
            granular_accum[opposing_team][did]["position_code"] = pos_code
            granular_accum[opposing_team][did]["broad_pos"]     = broad

    # ── SOT → conceded by opposing team ──────────────────────────────────────
    for pid, sot_count in player_sot.items():
        if pid not in player_positions or sot_count == 0:
            continue
        pos_info      = player_positions[pid]
        scoring_team  = pos_info.get("team_id")
        opposing_team = get_opposing_team(scoring_team)
        if not opposing_team:
            continue
        opp_is_home = is_home(opposing_team)
        h = 1 if opp_is_home else 0
        a = 0 if opp_is_home else 1

        pos_code, broad = _get_position_info(
            pos_info["position_id"], pos_info["detailed_position_id"]
        )
        if broad:
            broad_accum[opposing_team][broad]["sot"]       += sot_count
            broad_accum[opposing_team][broad]["sot_home"]  += h * sot_count
            broad_accum[opposing_team][broad]["sot_away"]  += a * sot_count

        if pos_code and pos_code != broad and pos_info["detailed_position_id"]:
            did = pos_info["detailed_position_id"]
            granular_accum[opposing_team][did]["sot"]       += sot_count
            granular_accum[opposing_team][did]["sot_home"]  += h * sot_count
            granular_accum[opposing_team][did]["sot_away"]  += a * sot_count
            granular_accum[opposing_team][did]["position_code"] = pos_code
            granular_accum[opposing_team][did]["broad_pos"]     = broad

    # ── games_played increment ────────────────────────────────────────────────
    for team_id in games_teams:
        for broad in ["GK", "DEF", "MID", "FWD"]:
            broad_accum[team_id][broad]["games"] += 1

    for team_id, pos_data in broad_accum.items():
        _flush_broad(team_id, season_id, league_id, pos_data)

    for team_id, pos_data in granular_accum.items():
        _flush_granular(team_id, season_id, league_id, pos_data)

    _mark_processed(fixture_id)


# ── BOOTSTRAP FULL SEASON ─────────────────────────────────────────────────────

def bootstrap_season(season_id, league_id):
    print(f"\n{'='*50}")
    print(f"  BOOTSTRAPPING season {season_id} / league {league_id}")
    print(f"{'='*50}")

    fixtures = _sm_get("fixtures", {
        "filters": f"leagueIds:{league_id};seasonIds:{season_id}",
        "per_page": 100,
    })

    completed = [f for f in fixtures if f.get("state_id") == 5]
    print(f"  Found {len(completed)} completed fixtures")

    for fixture in completed:
        process_fixture(fixture["id"], season_id, league_id)

    _update_league_averages(season_id, league_id)
    print(f"\n  ✅ Bootstrap complete")


# ── UPDATE AFTER MATCH ────────────────────────────────────────────────────────

def update_after_match(fixture_id, season_id, league_id):
    print(f"\nUpdating concessions for fixture {fixture_id}...")
    process_fixture(fixture_id, season_id, league_id)
    _update_league_averages(season_id, league_id)
    print(f"✅ Update complete")


# ── LEAGUE AVERAGE CALCULATOR ─────────────────────────────────────────────────

def _update_league_averages(season_id, league_id):
    broad_rows = supabase.table("positional_concessions_broad")\
        .select("*").eq("season_id", season_id).eq("league_id", league_id)\
        .execute().data

    broad_groups = defaultdict(lambda: defaultdict(float))
    for row in broad_rows:
        bp = row["broad_position"]
        broad_groups[bp]["goals"]        += row["goals_conceded"]
        broad_groups[bp]["goals_home"]   += row.get("goals_conceded_home", 0)
        broad_groups[bp]["goals_away"]   += row.get("goals_conceded_away", 0)
        broad_groups[bp]["assists"]      += row["assists_conceded"]
        broad_groups[bp]["assists_home"] += row.get("assists_conceded_home", 0)
        broad_groups[bp]["assists_away"] += row.get("assists_conceded_away", 0)
        broad_groups[bp]["bc"]           += row.get("bc_conceded", 0)
        broad_groups[bp]["bc_home"]      += row.get("bc_conceded_home", 0)
        broad_groups[bp]["bc_away"]      += row.get("bc_conceded_away", 0)
        broad_groups[bp]["shots"]        += row.get("shots_conceded", 0)
        broad_groups[bp]["shots_home"]   += row.get("shots_conceded_home", 0)
        broad_groups[bp]["shots_away"]   += row.get("shots_conceded_away", 0)
        broad_groups[bp]["sot"]          += row.get("sot_conceded", 0)
        broad_groups[bp]["sot_home"]     += row.get("sot_conceded_home", 0)
        broad_groups[bp]["sot_away"]     += row.get("sot_conceded_away", 0)
        broad_groups[bp]["games"]        += row["games_played"]
        broad_groups[bp]["teams"]        += 1

    for bp, t in broad_groups.items():
        gp = t["games"] or 1
        supabase.table("positional_concessions_league_avg").upsert({
            "league_id":            league_id,
            "season_id":            season_id,
            "broad_position":       bp,
            "position_code":        None,
            "granularity":          "broad",
            "avg_goals_per_game":   t["goals"]        / gp,
            "avg_assists_per_game": t["assists"]       / gp,
            "avg_goals_home":       t["goals_home"]   / gp,
            "avg_goals_away":       t["goals_away"]   / gp,
            "avg_assists_home":     t["assists_home"] / gp,
            "avg_assists_away":     t["assists_away"] / gp,
            "avg_bc_per_game":      t["bc"]           / gp,
            "avg_bc_home":          t["bc_home"]      / gp,
            "avg_bc_away":          t["bc_away"]      / gp,
            "avg_shots_per_game":   t["shots"]        / gp,
            "avg_shots_home":       t["shots_home"]   / gp,
            "avg_shots_away":       t["shots_away"]   / gp,
            "avg_sot_per_game":     t["sot"]          / gp,
            "avg_sot_home":         t["sot_home"]     / gp,
            "avg_sot_away":         t["sot_away"]     / gp,
            "sample_size":          int(t["teams"]),
            "last_updated":         datetime.utcnow().isoformat()
        }, on_conflict="league_id,season_id,granularity,broad_position,position_code").execute()

    granular_rows = supabase.table("positional_concessions_granular")\
        .select("*").eq("season_id", season_id).eq("league_id", league_id)\
        .execute().data

    granular_groups = defaultdict(lambda: defaultdict(float))
    gmeta = {}
    for row in granular_rows:
        pc = row["position_code"]
        granular_groups[pc]["goals"]        += row["goals_conceded"]
        granular_groups[pc]["goals_home"]   += row.get("goals_conceded_home", 0)
        granular_groups[pc]["goals_away"]   += row.get("goals_conceded_away", 0)
        granular_groups[pc]["assists"]      += row["assists_conceded"]
        granular_groups[pc]["assists_home"] += row.get("assists_conceded_home", 0)
        granular_groups[pc]["assists_away"] += row.get("assists_conceded_away", 0)
        granular_groups[pc]["bc"]           += row.get("bc_conceded", 0)
        granular_groups[pc]["bc_home"]      += row.get("bc_conceded_home", 0)
        granular_groups[pc]["bc_away"]      += row.get("bc_conceded_away", 0)
        granular_groups[pc]["shots"]        += row.get("shots_conceded", 0)
        granular_groups[pc]["shots_home"]   += row.get("shots_conceded_home", 0)
        granular_groups[pc]["shots_away"]   += row.get("shots_conceded_away", 0)
        granular_groups[pc]["sot"]          += row.get("sot_conceded", 0)
        granular_groups[pc]["sot_home"]     += row.get("sot_conceded_home", 0)
        granular_groups[pc]["sot_away"]     += row.get("sot_conceded_away", 0)
        granular_groups[pc]["games"]        += row["games_played"]
        granular_groups[pc]["teams"]        += 1
        gmeta[pc] = {"broad": row["broad_position"], "position_id": row["position_id"]}

    for pc, t in granular_groups.items():
        gp = t["games"] or 1
        supabase.table("positional_concessions_league_avg").upsert({
            "league_id":            league_id,
            "season_id":            season_id,
            "broad_position":       gmeta[pc]["broad"],
            "position_code":        pc,
            "granularity":          "granular",
            "avg_goals_per_game":   t["goals"]        / gp,
            "avg_assists_per_game": t["assists"]       / gp,
            "avg_goals_home":       t["goals_home"]   / gp,
            "avg_goals_away":       t["goals_away"]   / gp,
            "avg_assists_home":     t["assists_home"] / gp,
            "avg_assists_away":     t["assists_away"] / gp,
            "avg_bc_per_game":      t["bc"]           / gp,
            "avg_bc_home":          t["bc_home"]      / gp,
            "avg_bc_away":          t["bc_away"]      / gp,
            "avg_shots_per_game":   t["shots"]        / gp,
            "avg_shots_home":       t["shots_home"]   / gp,
            "avg_shots_away":       t["shots_away"]   / gp,
            "avg_sot_per_game":     t["sot"]          / gp,
            "avg_sot_home":         t["sot_home"]     / gp,
            "avg_sot_away":         t["sot_away"]     / gp,
            "sample_size":          int(t["teams"]),
            "last_updated":         datetime.utcnow().isoformat()
        }, on_conflict="league_id,season_id,granularity,broad_position,position_code").execute()


# ── GET MULTIPLIERS FOR A FIXTURE ─────────────────────────────────────────────

def get_multipliers(fixture_id, season_id, league_id):
    fixture_data = _sm_get(f"fixtures/{fixture_id}", {"include": "participants"})
    if not fixture_data:
        return {}

    fixture = fixture_data if isinstance(fixture_data, dict) else fixture_data[0]
    participants = fixture.get("participants", [])
    if isinstance(participants, dict): participants = participants.get("data", [])

    home_team_id = away_team_id = None
    for p in participants:
        meta = p.get("meta", {})
        loc  = meta.get("location", "")
        if loc == "home": home_team_id = p.get("id")
        elif loc == "away": away_team_id = p.get("id")

    if not home_team_id or not away_team_id:
        return {}

    avg_rows = supabase.table("positional_concessions_league_avg")\
        .select("*").eq("season_id", season_id).eq("league_id", league_id)\
        .execute().data

    league_avgs_broad    = {}
    league_avgs_granular = {}
    for row in avg_rows:
        if row["granularity"] == "broad":
            league_avgs_broad[row["broad_position"]] = row
        else:
            league_avgs_granular[row["position_code"]] = row

    result = {}

    for team_id in [home_team_id, away_team_id]:
        team_is_home = (team_id == home_team_id)
        result[team_id] = {"broad": {}, "granular": {}}

        broad_rows = supabase.table("positional_concessions_broad")\
            .select("*").eq("team_id", team_id).eq("season_id", season_id)\
            .execute().data

        for row in broad_rows:
            bp  = row["broad_position"]
            avg = league_avgs_broad.get(bp, {})
            gp  = row["games_played"] or 1

            min_gp   = MIN_GP_BROAD.get(bp, 5)
            min_conc = MIN_CONCESSIONS_BROAD.get(bp, 2)
            total_concessions = row["goals_conceded"] + row["assists_conceded"]

            gpg   = row["goals_conceded"]          / gp
            apg   = row["assists_conceded"]        / gp
            bcpg  = row.get("bc_conceded", 0)      / gp
            spg   = row.get("shots_conceded", 0)   / gp
            sotpg = row.get("sot_conceded", 0)     / gp

            avg_g    = avg.get("avg_goals_per_game",   0.001) or 0.001
            avg_a    = avg.get("avg_assists_per_game", 0.001) or 0.001
            avg_b    = avg.get("avg_bc_per_game",      0.001) or 0.001
            avg_s    = avg.get("avg_shots_per_game",   0.001) or 0.001
            avg_sot  = avg.get("avg_sot_per_game",     0.001) or 0.001

            goal_mult   = gpg   / avg_g
            assist_mult = apg   / avg_a
            bc_mult     = bcpg  / avg_b
            shots_mult  = spg   / avg_s
            sot_mult    = sotpg / avg_sot

            ABS_GOAL_THRESH   = {"GK": 99, "DEF": 0.27, "MID": 0.75, "FWD": 0.85}
            ABS_ASSIST_THRESH = {"GK": 99, "DEF": 0.30, "MID": 0.70, "FWD": 0.40}
            ABS_SOT_THRESH    = {"GK": 99, "DEF": 0.50, "MID": 1.20, "FWD": 1.50}

            abs_goal_flag   = gpg   >= ABS_GOAL_THRESH.get(bp, 99)
            abs_assist_flag = apg   >= ABS_ASSIST_THRESH.get(bp, 99)
            abs_sot_flag    = sotpg >= ABS_SOT_THRESH.get(bp, 99)

            goal_flag   = None
            assist_flag = None
            if gp >= min_gp and total_concessions >= min_conc:
                if goal_mult >= THRESHOLD_HIGH or abs_goal_flag:
                    goal_flag = "HIGH"
                elif goal_mult >= THRESHOLD_MEDIUM:
                    goal_flag = "MEDIUM"
                if assist_mult >= THRESHOLD_HIGH or abs_assist_flag:
                    assist_flag = "HIGH"
                elif assist_mult >= THRESHOLD_MEDIUM:
                    assist_flag = "MEDIUM"

            # Separate shots/SOT flag — independent of goal/assist flag
            shots_flag = None
            if gp >= min_gp:
                if sot_mult >= THRESHOLD_HIGH or abs_sot_flag:
                    shots_flag = "HIGH"
                elif sot_mult >= THRESHOLD_MEDIUM:
                    shots_flag = "MEDIUM"

            result[team_id]["broad"][bp] = {
                "goal_multiplier":   round(min(goal_mult, 5.0), 2),
                "assist_multiplier": round(min(assist_mult, 5.0), 2),
                "bc_multiplier":     round(min(bc_mult, 5.0), 2),
                "shots_multiplier":  round(min(shots_mult, 5.0), 2),
                "sot_multiplier":    round(min(sot_mult, 5.0), 2),
                "goals_conceded":    row["goals_conceded"],
                "assists_conceded":  row["assists_conceded"],
                "shots_conceded":    row.get("shots_conceded", 0),
                "sot_conceded":      row.get("sot_conceded", 0),
                "games_played":      gp,
                "goal_flag":         goal_flag,
                "assist_flag":       assist_flag,
                "shots_flag":        shots_flag,
                "location":          "home" if team_is_home else "away",
            }

        granular_rows = supabase.table("positional_concessions_granular")\
            .select("*").eq("team_id", team_id).eq("season_id", season_id)\
            .execute().data

        for row in granular_rows:
            pc  = row["position_code"]
            avg = league_avgs_granular.get(pc, {})
            gp  = row["games_played"] or 1

            broad_for_pc = BROAD_MAP.get(pc, "MID")
            min_gp   = MIN_GP_GRANULAR.get(pc, MIN_GP_BROAD.get(broad_for_pc, 5))
            min_conc = MIN_CONCESSIONS_GRANULAR.get(pc, MIN_CONCESSIONS_BROAD.get(broad_for_pc, 2))
            total_concessions = row["goals_conceded"] + row["assists_conceded"]

            gpg   = row["goals_conceded"]          / gp
            apg   = row["assists_conceded"]        / gp
            bcpg  = row.get("bc_conceded", 0)      / gp
            spg   = row.get("shots_conceded", 0)   / gp
            sotpg = row.get("sot_conceded", 0)     / gp

            avg_g   = avg.get("avg_goals_per_game",   0.001) or 0.001
            avg_a   = avg.get("avg_assists_per_game", 0.001) or 0.001
            avg_b   = avg.get("avg_bc_per_game",      0.001) or 0.001
            avg_s   = avg.get("avg_shots_per_game",   0.001) or 0.001
            avg_sot = avg.get("avg_sot_per_game",     0.001) or 0.001

            goal_mult   = gpg   / max(avg_g,   0.001)
            assist_mult = apg   / max(avg_a,   0.001)
            bc_mult     = bcpg  / max(avg_b,   0.001)
            shots_mult  = spg   / max(avg_s,   0.001)
            sot_mult    = sotpg / max(avg_sot, 0.001)

            goal_flag   = None
            assist_flag = None
            if gp >= min_gp and total_concessions >= min_conc:
                if goal_mult >= THRESHOLD_HIGH:
                    goal_flag = "HIGH"
                elif goal_mult >= THRESHOLD_MEDIUM:
                    goal_flag = "MEDIUM"
                if assist_mult >= THRESHOLD_HIGH:
                    assist_flag = "HIGH"
                elif assist_mult >= THRESHOLD_MEDIUM:
                    assist_flag = "MEDIUM"

            shots_flag = None
            if gp >= min_gp:
                if sot_mult >= THRESHOLD_HIGH:     shots_flag = "HIGH"
                elif sot_mult >= THRESHOLD_MEDIUM: shots_flag = "MEDIUM"

            result[team_id]["granular"][pc] = {
                "goal_multiplier":   round(goal_mult, 2),
                "assist_multiplier": round(assist_mult, 2),
                "bc_multiplier":     round(bc_mult, 2),
                "shots_multiplier":  round(shots_mult, 2),
                "sot_multiplier":    round(sot_mult, 2),
                "goals_conceded":    row["goals_conceded"],
                "assists_conceded":  row["assists_conceded"],
                "shots_conceded":    row.get("shots_conceded", 0),
                "sot_conceded":      row.get("sot_conceded", 0),
                "games_played":      gp,
                "goal_flag":         goal_flag,
                "assist_flag":       assist_flag,
                "shots_flag":        shots_flag,
                "location":          "home" if team_is_home else "away",
            }

    return result


# ── APPLY MULTIPLIERS ─────────────────────────────────────────────────────────

def apply_concession_multiplier(player_score, player_position_code,
                                 opponent_multipliers, score_type="assist",
                                 has_detailed_position=True):
    broad    = BROAD_MAP.get(player_position_code, "MID")
    mult_key = "goal_multiplier" if score_type == "goal" \
               else "sot_multiplier" if score_type == "sot" \
               else "shots_multiplier" if score_type == "shots" \
               else "assist_multiplier"

    # Market-specific flag key
    if score_type in ("sot", "shots"):
        flag_key = "shots_flag"
    elif score_type == "goal":
        flag_key = "goal_flag"
    else:
        flag_key = "assist_flag"

    multiplier = 1.0
    flag       = None

    granular = opponent_multipliers.get("granular", {})

    if has_detailed_position:
        # Granular players — use granular data only, no broad fallback
        if player_position_code in granular:
            multiplier = granular[player_position_code].get(mult_key, 1.0)
            flag       = granular[player_position_code].get(flag_key)
        # If no granular data for this position — no flag, no multiplier adjustment
    else:
        # Null position players — use broad only
        broad_data = opponent_multipliers.get("broad", {})
        if broad in broad_data:
            multiplier = broad_data[broad].get(mult_key, 1.0)
            flag       = broad_data[broad].get(flag_key)

    return round(player_score * multiplier, 3), multiplier, flag
