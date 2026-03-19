"""
Deep Current Football — Positional Concessions System
server/positional_concessions.py

Three functions:
  1. bootstrap_season()     — seed full season data (run once per season)
  2. update_after_match()   — update after a fixture completes
  3. get_multipliers()      — return concession multipliers for a fixture

Broad groups:  GK | DEF | MID | FWD
Granular:      GK | CB | RB | LB | RWB | LWB | CDM | CM | CAM |
               RM | LM | RW | LW | ST | CF | SS
"""

import os
import requests
from datetime import datetime
from supabase import create_client

# ── CONFIG ────────────────────────────────────────────────────────────────────
SPORTMONKS_TOKEN = os.environ.get("SPORTMONKS_API_TOKEN")
SUPABASE_URL     = os.environ.get("SUPABASE_URL")
SUPABASE_KEY     = os.environ.get("SUPABASE_SERVICE_KEY")
BASE_URL         = "https://api.sportmonks.com/v3/football"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── POSITION MAPPINGS ─────────────────────────────────────────────────────────

# Sportmonks position_id (broad) → broad group
BROAD_POSITION_MAP = {
    24: "GK",
    25: "DEF",
    26: "MID",
    27: "FWD",
}

# Sportmonks detailed_position_id → position code + broad group
GRANULAR_POSITION_MAP = {
    # Goalkeepers
    144: ("GK",  "GK"),
    # Defenders
    148: ("CB",  "DEF"),
    149: ("RB",  "DEF"),
    150: ("LB",  "DEF"),
    151: ("RWB", "DEF"),
    152: ("LWB", "DEF"),
    # Midfielders
    153: ("CDM", "MID"),
    154: ("CM",  "MID"),
    155: ("CAM", "MID"),
    156: ("RM",  "MID"),
    157: ("LM",  "MID"),
    # Forwards
    158: ("RW",  "FWD"),
    159: ("LW",  "FWD"),
    160: ("ST",  "FWD"),
    161: ("CF",  "FWD"),
    162: ("SS",  "FWD"),
}

# Multiplier thresholds
THRESHOLD_HIGH   = 2.0   # 🔴 serious vulnerability
THRESHOLD_MEDIUM = 1.5   # ⚡ elevated vulnerability
THRESHOLD_LOW    = 1.3   # minor flag — not shown in UI

# ── HELPERS ───────────────────────────────────────────────────────────────────

def _sm_get(endpoint, params=None):
    """Make a Sportmonks API call."""
    p = {"api_token": SPORTMONKS_TOKEN}
    if params:
        p.update(params)
    r = requests.get(f"{BASE_URL}/{endpoint}", params=p)
    r.raise_for_status()
    return r.json().get("data", [])


def _get_position_info(position_id, detailed_position_id):
    """Return (position_code, broad_group) from Sportmonks position IDs."""
    # Try granular first
    if detailed_position_id and detailed_position_id in GRANULAR_POSITION_MAP:
        code, broad = GRANULAR_POSITION_MAP[detailed_position_id]
        return code, broad
    # Fall back to broad
    if position_id in BROAD_POSITION_MAP:
        broad = BROAD_POSITION_MAP[position_id]
        # Use broad as code too when granular not available
        return broad, broad
    return None, None


def _already_processed(fixture_id):
    """Check if a fixture has already been processed."""
    res = supabase.table("concession_processed_fixtures")\
        .select("fixture_id")\
        .eq("fixture_id", fixture_id)\
        .execute()
    return len(res.data) > 0


def _mark_processed(fixture_id):
    """Mark a fixture as processed."""
    supabase.table("concession_processed_fixtures")\
        .insert({"fixture_id": fixture_id})\
        .execute()


def _upsert_broad(team_id, season_id, league_id, broad_pos, goals_delta, assists_delta, games_delta):
    """Upsert broad concession record."""
    existing = supabase.table("positional_concessions_broad")\
        .select("*")\
        .eq("team_id", team_id)\
        .eq("season_id", season_id)\
        .eq("broad_position", broad_pos)\
        .execute()

    if existing.data:
        row = existing.data[0]
        supabase.table("positional_concessions_broad")\
            .update({
                "goals_conceded":   row["goals_conceded"]   + goals_delta,
                "assists_conceded": row["assists_conceded"] + assists_delta,
                "games_played":     row["games_played"]     + games_delta,
                "last_updated":     datetime.utcnow().isoformat()
            })\
            .eq("id", row["id"])\
            .execute()
    else:
        supabase.table("positional_concessions_broad")\
            .insert({
                "team_id":          team_id,
                "season_id":        season_id,
                "league_id":        league_id,
                "broad_position":   broad_pos,
                "goals_conceded":   goals_delta,
                "assists_conceded": assists_delta,
                "games_played":     games_delta,
            })\
            .execute()


def _upsert_granular(team_id, season_id, league_id, position_id, position_code, broad_pos, goals_delta, assists_delta, games_delta):
    """Upsert granular concession record."""
    existing = supabase.table("positional_concessions_granular")\
        .select("*")\
        .eq("team_id", team_id)\
        .eq("season_id", season_id)\
        .eq("position_id", position_id)\
        .execute()

    if existing.data:
        row = existing.data[0]
        supabase.table("positional_concessions_granular")\
            .update({
                "goals_conceded":   row["goals_conceded"]   + goals_delta,
                "assists_conceded": row["assists_conceded"] + assists_delta,
                "games_played":     row["games_played"]     + games_delta,
                "last_updated":     datetime.utcnow().isoformat()
            })\
            .eq("id", row["id"])\
            .execute()
    else:
        supabase.table("positional_concessions_granular")\
            .insert({
                "team_id":          team_id,
                "season_id":        season_id,
                "league_id":        league_id,
                "position_id":      position_id,
                "position_code":    position_code,
                "broad_position":   broad_pos,
                "goals_conceded":   goals_delta,
                "assists_conceded": assists_delta,
                "games_played":     games_delta,
            })\
            .execute()


# ── CORE FUNCTION: PROCESS ONE FIXTURE ───────────────────────────────────────

def process_fixture(fixture_id, season_id, league_id):
    """
    Process a single completed fixture.
    For each goal event:
      - Identify the scorer's position → credit as concession against the opposing team
      - Identify the assister's position → credit as assist concession against opposing team
    """
    if _already_processed(fixture_id):
        print(f"  Fixture {fixture_id} already processed — skipping")
        return

    print(f"  Processing fixture {fixture_id}...")

    # Pull fixture events (goals) with lineups for position data
    try:
        fixture_data = _sm_get(
            f"fixtures/{fixture_id}",
            {"include": "events;lineups", "filters": "eventTypes:14"}
            # type_id 14 = goal event in Sportmonks
        )
    except Exception as e:
        print(f"  ERROR fetching fixture {fixture_id}: {e}")
        return

    if not fixture_data:
        return

    fixture = fixture_data if isinstance(fixture_data, dict) else fixture_data[0]

    # Build player → position lookup from lineups
    lineups = fixture.get("lineups", [])
    if isinstance(lineups, dict):
        lineups = lineups.get("data", [])

    player_positions = {}
    for player in lineups:
        pid = player.get("player_id")
        if pid:
            player_positions[pid] = {
                "position_id":          player.get("position_id"),
                "detailed_position_id": player.get("formation_position"),
                "team_id":              player.get("team_id"),
            }

    # Get both team IDs from participants
    participants = fixture.get("participants", [])
    if isinstance(participants, dict):
        participants = participants.get("data", [])
    team_ids = [p.get("id") for p in participants if p.get("id")]

    def get_opposing_team(scorer_team_id):
        for tid in team_ids:
            if tid != scorer_team_id:
                return tid
        return None

    # Process goal events
    events = fixture.get("events", [])
    if isinstance(events, dict):
        events = events.get("data", [])

    # Track games played — add 1 per team for this fixture
    games_processed_teams = set()

    for event in events:
        # Only process goal events (type_id 14)
        if event.get("type_id") != 14:
            continue

        scorer_id    = event.get("player_id")
        assister_id  = event.get("related_player_id")
        scoring_team = event.get("participant_id")
        opposing_team = get_opposing_team(scoring_team)

        if not opposing_team:
            continue

        # Add games_played delta for this team (once per fixture per team)
        if opposing_team not in games_processed_teams:
            games_processed_teams.add(opposing_team)
            # We'll apply games_delta=1 with the first goal
            # Handle separately below

        # ── SCORER ──
        if scorer_id and scorer_id in player_positions:
            pos_info = player_positions[scorer_id]
            pos_code, broad = _get_position_info(
                pos_info["position_id"],
                pos_info["detailed_position_id"]
            )
            if broad:
                _upsert_broad(
                    opposing_team, season_id, league_id,
                    broad, goals_delta=1, assists_delta=0, games_delta=0
                )
            if pos_code and pos_code != broad:
                _upsert_granular(
                    opposing_team, season_id, league_id,
                    pos_info["detailed_position_id"], pos_code, broad,
                    goals_delta=1, assists_delta=0, games_delta=0
                )

        # ── ASSISTER ──
        if assister_id and assister_id in player_positions:
            pos_info = player_positions[assister_id]
            pos_code, broad = _get_position_info(
                pos_info["position_id"],
                pos_info["detailed_position_id"]
            )
            if broad:
                _upsert_broad(
                    opposing_team, season_id, league_id,
                    broad, goals_delta=0, assists_delta=1, games_delta=0
                )
            if pos_code and pos_code != broad:
                _upsert_granular(
                    opposing_team, season_id, league_id,
                    pos_info["detailed_position_id"], pos_code, broad,
                    goals_delta=0, assists_delta=1, games_delta=0
                )

    # Apply games_played=1 for each team in this fixture
    for team_id in games_processed_teams:
        for broad in ["GK", "DEF", "MID", "FWD"]:
            _upsert_broad(
                team_id, season_id, league_id,
                broad, goals_delta=0, assists_delta=0, games_delta=1
            )

    _mark_processed(fixture_id)
    print(f"  ✅ Fixture {fixture_id} processed")


# ── SYSTEM 2 FUNCTION 1: BOOTSTRAP FULL SEASON ───────────────────────────────

def bootstrap_season(season_id, league_id):
    """
    Seed the positional concessions tables for a full season.
    Pulls all completed fixtures and processes each one.
    Call once at the start — idempotent (skips already-processed fixtures).
    """
    print(f"\n{'='*50}")
    print(f"  BOOTSTRAPPING season {season_id} / league {league_id}")
    print(f"{'='*50}")

    fixtures = _sm_get(
        "fixtures",
        {
            "filters": f"leagueIds:{league_id};seasonIds:{season_id}",
            "per_page": 100,
        }
    )

    completed = [f for f in fixtures if f.get("state_id") == 5]
    print(f"  Found {len(completed)} completed fixtures")

    for fixture in completed:
        process_fixture(fixture["id"], season_id, league_id)

    _update_league_averages(season_id, league_id)
    print(f"\n  ✅ Bootstrap complete")


# ── SYSTEM 2 FUNCTION 2: UPDATE AFTER MATCH ──────────────────────────────────

def update_after_match(fixture_id, season_id, league_id):
    """
    Call this after each fixture completes on match day.
    Processes the single fixture and refreshes league averages.
    """
    print(f"\nUpdating concessions for fixture {fixture_id}...")
    process_fixture(fixture_id, season_id, league_id)
    _update_league_averages(season_id, league_id)
    print(f"✅ Update complete")


# ── LEAGUE AVERAGE CALCULATOR ─────────────────────────────────────────────────

def _update_league_averages(season_id, league_id):
    """Recalculate league average concession rates for all positions."""
    print(f"  Updating league averages for season {season_id}...")

    # ── BROAD ──
    broad_rows = supabase.table("positional_concessions_broad")\
        .select("*")\
        .eq("season_id", season_id)\
        .eq("league_id", league_id)\
        .execute().data

    broad_groups = {}
    for row in broad_rows:
        bp = row["broad_position"]
        if bp not in broad_groups:
            broad_groups[bp] = {"goals": 0, "assists": 0, "games": 0, "teams": 0}
        broad_groups[bp]["goals"]   += row["goals_conceded"]
        broad_groups[bp]["assists"] += row["assists_conceded"]
        broad_groups[bp]["games"]   += row["games_played"]
        broad_groups[bp]["teams"]   += 1

    for bp, totals in broad_groups.items():
        if totals["games"] > 0:
            avg_goals   = totals["goals"]   / totals["games"]
            avg_assists = totals["assists"] / totals["games"]
        else:
            avg_goals = avg_assists = 0

        supabase.table("positional_concessions_league_avg")\
            .upsert({
                "league_id":           league_id,
                "season_id":           season_id,
                "broad_position":      bp,
                "position_code":       None,
                "granularity":         "broad",
                "avg_goals_per_game":  avg_goals,
                "avg_assists_per_game": avg_assists,
                "sample_size":         totals["teams"],
                "last_updated":        datetime.utcnow().isoformat()
            })\
            .execute()

    # ── GRANULAR ──
    granular_rows = supabase.table("positional_concessions_granular")\
        .select("*")\
        .eq("season_id", season_id)\
        .eq("league_id", league_id)\
        .execute().data

    granular_groups = {}
    for row in granular_rows:
        pc = row["position_code"]
        if pc not in granular_groups:
            granular_groups[pc] = {
                "goals": 0, "assists": 0, "games": 0,
                "teams": 0, "broad": row["broad_position"],
                "position_id": row["position_id"]
            }
        granular_groups[pc]["goals"]   += row["goals_conceded"]
        granular_groups[pc]["assists"] += row["assists_conceded"]
        granular_groups[pc]["games"]   += row["games_played"]
        granular_groups[pc]["teams"]   += 1

    for pc, totals in granular_groups.items():
        if totals["games"] > 0:
            avg_goals   = totals["goals"]   / totals["games"]
            avg_assists = totals["assists"] / totals["games"]
        else:
            avg_goals = avg_assists = 0

        supabase.table("positional_concessions_league_avg")\
            .upsert({
                "league_id":           league_id,
                "season_id":           season_id,
                "broad_position":      totals["broad"],
                "position_code":       pc,
                "granularity":         "granular",
                "avg_goals_per_game":  avg_goals,
                "avg_assists_per_game": avg_assists,
                "sample_size":         totals["teams"],
                "last_updated":        datetime.utcnow().isoformat()
            })\
            .execute()

    print(f"  ✅ League averages updated")


# ── SYSTEM 2 FUNCTION 3: GET MULTIPLIERS FOR A FIXTURE ───────────────────────

def get_multipliers(fixture_id, season_id, league_id):
    """
    Returns positional concession multipliers for both teams in a fixture.
    Used by the scoring engine before generating daily rankings.

    Returns:
    {
        home_team_id: {
            "broad": {
                "DEF": {"goal_multiplier": 1.2, "assist_multiplier": 0.9, "flag": None},
                "MID": {"goal_multiplier": 2.1, "assist_multiplier": 1.8, "flag": "HIGH"},
                "FWD": {"goal_multiplier": 1.6, "assist_multiplier": 1.4, "flag": "MEDIUM"},
            },
            "granular": {
                "RW": {"goal_multiplier": 2.4, "assist_multiplier": 2.1, "flag": "HIGH"},
                "CAM": {"goal_multiplier": 1.7, "assist_multiplier": 1.5, "flag": "MEDIUM"},
                ...
            }
        },
        away_team_id: { ... }
    }
    """
    # Get team IDs for this fixture
    fixture_data = _sm_get(
        f"fixtures/{fixture_id}",
        {"include": "participants"}
    )
    if not fixture_data:
        return {}

    fixture = fixture_data if isinstance(fixture_data, dict) else fixture_data[0]
    participants = fixture.get("participants", [])
    if isinstance(participants, dict):
        participants = participants.get("data", [])

    team_ids = [p.get("id") for p in participants if p.get("id")]
    if len(team_ids) < 2:
        return {}

    # Get league averages
    league_avgs_broad = {}
    league_avgs_granular = {}

    avg_rows = supabase.table("positional_concessions_league_avg")\
        .select("*")\
        .eq("season_id", season_id)\
        .eq("league_id", league_id)\
        .execute().data

    for row in avg_rows:
        if row["granularity"] == "broad":
            league_avgs_broad[row["broad_position"]] = row
        else:
            league_avgs_granular[row["position_code"]] = row

    result = {}

    for team_id in team_ids:
        result[team_id] = {"broad": {}, "granular": {}}

        # ── BROAD MULTIPLIERS ──
        broad_rows = supabase.table("positional_concessions_broad")\
            .select("*")\
            .eq("team_id", team_id)\
            .eq("season_id", season_id)\
            .execute().data

        for row in broad_rows:
            bp = row["broad_position"]
            avg = league_avgs_broad.get(bp, {})

            avg_gpg = avg.get("avg_goals_per_game",   0.001)
            avg_apg = avg.get("avg_assists_per_game", 0.001)

            goal_mult   = row["goals_per_game"]   / avg_gpg if avg_gpg > 0 else 1.0
            assist_mult = row["assists_per_game"] / avg_apg if avg_apg > 0 else 1.0

            flag = None
            if goal_mult >= THRESHOLD_HIGH or assist_mult >= THRESHOLD_HIGH:
                flag = "HIGH"
            elif goal_mult >= THRESHOLD_MEDIUM or assist_mult >= THRESHOLD_MEDIUM:
                flag = "MEDIUM"

            result[team_id]["broad"][bp] = {
                "goal_multiplier":   round(goal_mult, 2),
                "assist_multiplier": round(assist_mult, 2),
                "goals_conceded":    row["goals_conceded"],
                "assists_conceded":  row["assists_conceded"],
                "games_played":      row["games_played"],
                "flag":              flag
            }

        # ── GRANULAR MULTIPLIERS ──
        granular_rows = supabase.table("positional_concessions_granular")\
            .select("*")\
            .eq("team_id", team_id)\
            .eq("season_id", season_id)\
            .execute().data

        for row in granular_rows:
            pc = row["position_code"]
            avg = league_avgs_granular.get(pc, {})

            avg_gpg = avg.get("avg_goals_per_game",   0.001)
            avg_apg = avg.get("avg_assists_per_game", 0.001)

            goal_mult   = row["goals_per_game"]   / avg_gpg if avg_gpg > 0 else 1.0
            assist_mult = row["assists_per_game"] / avg_apg if avg_apg > 0 else 1.0

            flag = None
            if goal_mult >= THRESHOLD_HIGH or assist_mult >= THRESHOLD_HIGH:
                flag = "HIGH"
            elif goal_mult >= THRESHOLD_MEDIUM or assist_mult >= THRESHOLD_MEDIUM:
                flag = "MEDIUM"

            result[team_id]["granular"][pc] = {
                "goal_multiplier":   round(goal_mult, 2),
                "assist_multiplier": round(assist_mult, 2),
                "goals_conceded":    row["goals_conceded"],
                "assists_conceded":  row["assists_conceded"],
                "games_played":      row["games_played"],
                "flag":              flag
            }

    return result


# ── APPLY MULTIPLIERS TO PLAYER SCORES ───────────────────────────────────────

def apply_concession_multiplier(player_score, player_position_code,
                                 opponent_multipliers, score_type="assist"):
    """
    Apply the positional concession multiplier to a player's score.

    score_type: 'goal', 'assist', or 'tsoa'

    Returns adjusted score and flag.
    """
    broad_map = {
        "GK":  "GK",
        "CB":  "DEF", "RB":  "DEF", "LB": "DEF",
        "RWB": "DEF", "LWB": "DEF",
        "CDM": "MID", "CM":  "MID", "CAM": "MID",
        "RM":  "MID", "LM":  "MID",
        "RW":  "FWD", "LW":  "FWD", "ST": "FWD",
        "CF":  "FWD", "SS":  "FWD",
    }

    broad = broad_map.get(player_position_code, "MID")
    mult_key = "goal_multiplier" if score_type == "goal" else "assist_multiplier"

    # Try granular first, fall back to broad
    multiplier = 1.0
    flag = None

    granular = opponent_multipliers.get("granular", {})
    if player_position_code in granular:
        multiplier = granular[player_position_code].get(mult_key, 1.0)
        flag = granular[player_position_code].get("flag")
    else:
        broad_data = opponent_multipliers.get("broad", {})
        if broad in broad_data:
            multiplier = broad_data[broad].get(mult_key, 1.0)
            flag = broad_data[broad].get("flag")

    adjusted_score = round(player_score * multiplier, 3)
    return adjusted_score, multiplier, flag


# ── FLASK ROUTES (add to your main server.py) ─────────────────────────────────
"""
Add these routes to your existing Flask app:

from positional_concessions import (
    bootstrap_season, update_after_match, get_multipliers
)

@app.route('/api/concessions/bootstrap', methods=['POST'])
def concessions_bootstrap():
    data = request.json
    bootstrap_season(data['season_id'], data['league_id'])
    return jsonify({"status": "ok"})

@app.route('/api/concessions/update', methods=['POST'])
def concessions_update():
    data = request.json
    update_after_match(data['fixture_id'], data['season_id'], data['league_id'])
    return jsonify({"status": "ok"})

@app.route('/api/concessions/multipliers/<int:fixture_id>', methods=['GET'])
def concessions_multipliers(fixture_id):
    season_id  = request.args.get('season_id', type=int)
    league_id  = request.args.get('league_id', type=int)
    result = get_multipliers(fixture_id, season_id, league_id)
    return jsonify(result)
"""
