"""
Deep Current Football — Positional Concessions System
server/positional_concessions.py

Tracks goals, assists, and big chances conceded by position.
Includes home/away splits for more accurate matchup multipliers.

v2 — Batched Supabase writes (single upsert per team/position per fixture)
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

GRANULAR_POSITION_MAP = {
    144: ("GK",  "GK"),
    148: ("CB",  "DEF"), 149: ("RB",  "DEF"), 150: ("LB",  "DEF"),
    151: ("RWB", "DEF"), 152: ("LWB", "DEF"),
    153: ("CDM", "MID"), 154: ("CM",  "MID"), 155: ("CAM", "MID"),
    156: ("RM",  "MID"), 157: ("LM",  "MID"),
    158: ("RW",  "FWD"), 159: ("LW",  "FWD"), 160: ("ST",  "FWD"),
    161: ("CF",  "FWD"), 162: ("SS",  "FWD"),
}

THRESHOLD_HIGH   = 2.0
THRESHOLD_MEDIUM = 1.5

BROAD_MAP = {
    "GK": "GK",
    "CB": "DEF", "RB": "DEF", "LB": "DEF", "RWB": "DEF", "LWB": "DEF",
    "CDM": "MID", "CM": "MID", "CAM": "MID", "RM": "MID", "LM": "MID",
    "RW": "FWD", "LW": "FWD", "ST": "FWD", "CF": "FWD", "SS": "FWD",
}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def _sm_get(endpoint, params=None):
    p = {"api_token": SPORTMONKS_TOKEN}
    if params: p.update(params)
    r = requests.get(f"{BASE_URL}/{endpoint}", params=p)
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
    """
    Flush accumulated broad position data for one team in one fixture.
    accum = { broad_position: { goals, goals_home, goals_away, assists, ... } }
    Single GET + upsert per position — 4 calls max per team instead of one per event.
    """
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
                "games_played":          delta.get("games", 0),
            }).execute()


def _flush_granular(team_id, season_id, league_id, accum):
    """
    Flush accumulated granular position data for one team in one fixture.
    accum = { position_id: { position_code, broad_pos, goals, ... } }
    """
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
                "games_played":          delta.get("games", 0),
            }).execute()


# ── CORE: PROCESS ONE FIXTURE ─────────────────────────────────────────────────

def process_fixture(fixture_id, season_id, league_id):
    if _already_processed(fixture_id):
        return

    try:
        fixture_data = _sm_get(
            f"fixtures/{fixture_id}",
            {"include": "events;lineups;participants;statistics"}
        )
    except Exception as e:
        print(f"  ERROR fetching fixture {fixture_id}: {e}")
        return

    if not fixture_data:
        return

    fixture = fixture_data if isinstance(fixture_data, dict) else fixture_data[0]

    # ── Build player → position + team lookup from lineups ──
    lineups = fixture.get("lineups", [])
    if isinstance(lineups, dict): lineups = lineups.get("data", [])

    player_positions = {}
    for player in lineups:
        pid = player.get("player_id")
        if pid:
            player_positions[pid] = {
                "position_id":          player.get("position_id"),
                "detailed_position_id": player.get("formation_position"),
                "team_id":              player.get("team_id"),
            }

    # ── Identify home/away teams ──
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

    # ── Pull BC per player from statistics ──
    statistics = fixture.get("statistics", [])
    if isinstance(statistics, dict): statistics = statistics.get("data", [])

    player_bc = {}
    for stat in statistics:
        if stat.get("type_id") == 580:
            pid = stat.get("player_id")
            val = stat.get("data", {}).get("value", 0) or 0
            if pid:
                player_bc[pid] = player_bc.get(pid, 0) + val

    # ── Accumulate all deltas in memory ──
    # Structure: { team_id: { broad_pos: { goals, goals_home, ... } } }
    broad_accum    = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    granular_accum = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    games_teams    = set()

    events = fixture.get("events", [])
    if isinstance(events, dict): events = events.get("data", [])

    for event in events:
        if event.get("type_id") != 14:
            continue

        scorer_id     = event.get("player_id")
        assister_id   = event.get("related_player_id")
        scoring_team  = event.get("participant_id")
        opposing_team = get_opposing_team(scoring_team)

        if not opposing_team:
            continue

        games_teams.add(opposing_team)
        opp_is_home = is_home(opposing_team)
        h = 1 if opp_is_home else 0
        a = 0 if opp_is_home else 1

        # ── SCORER ──
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

        # ── ASSISTER ──
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

    # ── BC conceded ──
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

    # ── games_played increment for all 4 positions per team ──
    for team_id in games_teams:
        for broad in ["GK", "DEF", "MID", "FWD"]:
            broad_accum[team_id][broad]["games"] += 1

    # ── Flush all accumulated data — one write per team/position ──
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

            if team_is_home:
                gpg  = row.get("goals_conceded_home", 0)   / gp
                apg  = row.get("assists_conceded_home", 0) / gp
                bcpg = row.get("bc_conceded_home", 0)      / gp
                avg_g = avg.get("avg_goals_home",   avg.get("avg_goals_per_game",   0.001))
                avg_a = avg.get("avg_assists_home", avg.get("avg_assists_per_game", 0.001))
                avg_b = avg.get("avg_bc_home",      avg.get("avg_bc_per_game",      0.001))
            else:
                gpg  = row.get("goals_conceded_away", 0)   / gp
                apg  = row.get("assists_conceded_away", 0) / gp
                bcpg = row.get("bc_conceded_away", 0)      / gp
                avg_g = avg.get("avg_goals_away",   avg.get("avg_goals_per_game",   0.001))
                avg_a = avg.get("avg_assists_away", avg.get("avg_assists_per_game", 0.001))
                avg_b = avg.get("avg_bc_away",      avg.get("avg_bc_per_game",      0.001))

            goal_mult   = gpg  / max(avg_g, 0.001)
            assist_mult = apg  / max(avg_a, 0.001)
            bc_mult     = bcpg / max(avg_b, 0.001)

            flag = None
            if goal_mult >= THRESHOLD_HIGH or assist_mult >= THRESHOLD_HIGH:
                flag = "HIGH"
            elif goal_mult >= THRESHOLD_MEDIUM or assist_mult >= THRESHOLD_MEDIUM:
                flag = "MEDIUM"

            result[team_id]["broad"][bp] = {
                "goal_multiplier":   round(goal_mult, 2),
                "assist_multiplier": round(assist_mult, 2),
                "bc_multiplier":     round(bc_mult, 2),
                "goals_conceded":    row["goals_conceded"],
                "assists_conceded":  row["assists_conceded"],
                "games_played":      row["games_played"],
                "flag":              flag,
                "location":          "home" if team_is_home else "away",
            }

        granular_rows = supabase.table("positional_concessions_granular")\
            .select("*").eq("team_id", team_id).eq("season_id", season_id)\
            .execute().data

        for row in granular_rows:
            pc  = row["position_code"]
            avg = league_avgs_granular.get(pc, {})
            gp  = row["games_played"] or 1

            if team_is_home:
                gpg  = row.get("goals_conceded_home", 0)   / gp
                apg  = row.get("assists_conceded_home", 0) / gp
                bcpg = row.get("bc_conceded_home", 0)      / gp
                avg_g = avg.get("avg_goals_home",   avg.get("avg_goals_per_game",   0.001))
                avg_a = avg.get("avg_assists_home", avg.get("avg_assists_per_game", 0.001))
                avg_b = avg.get("avg_bc_home",      avg.get("avg_bc_per_game",      0.001))
            else:
                gpg  = row.get("goals_conceded_away", 0)   / gp
                apg  = row.get("assists_conceded_away", 0) / gp
                bcpg = row.get("bc_conceded_away", 0)      / gp
                avg_g = avg.get("avg_goals_away",   avg.get("avg_goals_per_game",   0.001))
                avg_a = avg.get("avg_assists_away", avg.get("avg_assists_per_game", 0.001))
                avg_b = avg.get("avg_bc_away",      avg.get("avg_bc_per_game",      0.001))

            goal_mult   = gpg  / max(avg_g, 0.001)
            assist_mult = apg  / max(avg_a, 0.001)
            bc_mult     = bcpg / max(avg_b, 0.001)

            flag = None
            if goal_mult >= THRESHOLD_HIGH or assist_mult >= THRESHOLD_HIGH:
                flag = "HIGH"
            elif goal_mult >= THRESHOLD_MEDIUM or assist_mult >= THRESHOLD_MEDIUM:
                flag = "MEDIUM"

            result[team_id]["granular"][pc] = {
                "goal_multiplier":   round(goal_mult, 2),
                "assist_multiplier": round(assist_mult, 2),
                "bc_multiplier":     round(bc_mult, 2),
                "goals_conceded":    row["goals_conceded"],
                "assists_conceded":  row["assists_conceded"],
                "games_played":      row["games_played"],
                "flag":              flag,
                "location":          "home" if team_is_home else "away",
            }

    return result


# ── APPLY MULTIPLIERS ─────────────────────────────────────────────────────────

def apply_concession_multiplier(player_score, player_position_code,
                                 opponent_multipliers, score_type="assist"):
    broad   = BROAD_MAP.get(player_position_code, "MID")
    mult_key = "goal_multiplier" if score_type == "goal" else "assist_multiplier"

    multiplier = 1.0
    flag       = None

    granular = opponent_multipliers.get("granular", {})
    if player_position_code in granular:
        multiplier = granular[player_position_code].get(mult_key, 1.0)
        flag       = granular[player_position_code].get("flag")
    else:
        broad_data = opponent_multipliers.get("broad", {})
        if broad in broad_data:
            multiplier = broad_data[broad].get(mult_key, 1.0)
            flag       = broad_data[broad].get("flag")

    return round(player_score * multiplier, 3), multiplier, flag
