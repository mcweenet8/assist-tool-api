"""
Deep Current Football — Sportmonks Match Day Scorer
server/sm_scorer.py

Uses correct endpoints:
  - /squads/seasons/{season_id}/teams/{team_id} for lineups
  - /expected/lineups?filters=fixtureId:{id} for xG per player
  - /fixtures/between/{date}/{date} for today's fixtures
"""

import os
import requests
from datetime import datetime, date
from supabase import create_client
from .sm_baseline import get_player_baseline, TYPE_IDS, _extract_stat, _sm_get

SPORTMONKS_TOKEN = os.environ.get("SPORTMONKS_API_TOKEN")
SUPABASE_URL     = os.environ.get("SUPABASE_URL")
SUPABASE_KEY     = os.environ.get("SUPABASE_SERVICE_KEY")
BASE_URL         = "https://api.sportmonks.com/v3/football"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── SCORING WEIGHTS ───────────────────────────────────────────────────────────

ASSIST_WEIGHTS = {
    "kp_ratio":       0.55,
    "cross_ratio":    0.35,
    "pass_acc_ratio": 0.10,
}

GOAL_WEIGHTS = {
    "xgot_gap":  0.35,
    "sot_per90": 0.25,
    "xg_per90":  0.20,
    "goals_p90": 0.20,
}


def grade_color(score):
    if score >= 3.0: return "#00BFFF"
    if score >= 2.0: return "#3DDC84"
    if score >= 1.0: return "#F0B429"
    return "#FF6B6B"


# ── FIXTURE DATA PULLERS ──────────────────────────────────────────────────────

def get_todays_fixtures(league_id, today=None):
    """Pull today's fixtures for a league."""
    if today is None:
        today = date.today().isoformat()
    resp = _sm_get(
        endpoint=f"/fixtures/between/{today}/{today}",
        filters=f"fixtureLeagues:{league_id}",
        include="participants",
    )
    return resp.get("data", [])


def pull_fixture_lineups(fixture_id):
    """
    Pull lineup for a fixture.
    Uses fixture include for lineup data.
    """
    resp = _sm_get(
        endpoint=f"/fixtures/{fixture_id}",
        include="lineups",
    )
    data = resp.get("data", {})
    lineups = data.get("lineups", [])
    if isinstance(lineups, dict):
        lineups = lineups.get("data", [])
    return lineups


def pull_fixture_xg(fixture_id):
    """
    Pull per-player xG and xGOT using /expected/lineups endpoint.
    Filters by fixtureId for this specific match.
    Returns {player_id: {"xg": float, "xgot": float}}
    """
    try:
        resp = _sm_get(
            endpoint="/expected/lineups",
            filters=f"fixtureId:{fixture_id}",
            include="type",
        )
        data = resp.get("data", [])
        if isinstance(data, dict):
            data = data.get("data", [])

        xg_map = {}
        for item in data:
            player_id = item.get("player_id")
            if not player_id:
                continue

            type_id = item.get("type_id")
            val = item.get("data", {}).get("value")

            if player_id not in xg_map:
                xg_map[player_id] = {"xg": None, "xgot": None}

            if type_id == 5304:    # EXPECTED_GOALS
                xg_map[player_id]["xg"] = val
            elif type_id == 5305:  # EXPECTED_GOALS_ON_TARGET
                xg_map[player_id]["xgot"] = val

        return xg_map

    except Exception as e:
        print(f"    xG pull error for fixture {fixture_id}: {e}")
        return {}


# ── SCORING FUNCTIONS ─────────────────────────────────────────────────────────

def calculate_assist_index(game_kp, game_crosses, game_pass_acc, minutes, baseline):
    """DC Assist Probability Index."""
    if not baseline or not minutes or minutes < 20:
        return None, {}

    nineties    = minutes / 90
    kp_per90    = game_kp / nineties
    cross_per90 = game_crosses / nineties

    b_kp    = baseline.get("kp_per90")             or 0.001
    b_cross = baseline.get("acc_cross_per90")      or 0.001
    b_pa    = baseline.get("pass_accuracy_baseline") or 0.001

    kp_ratio    = kp_per90 / b_kp
    cross_ratio = cross_per90 / b_cross
    pa_ratio    = game_pass_acc / b_pa if game_pass_acc else 1.0

    index = (
        kp_ratio    * ASSIST_WEIGHTS["kp_ratio"] +
        cross_ratio * ASSIST_WEIGHTS["cross_ratio"] +
        pa_ratio    * ASSIST_WEIGHTS["pass_acc_ratio"]
    )

    components = {
        "kp_per90":    round(kp_per90, 3),
        "cross_per90": round(cross_per90, 3),
        "kp_ratio":    round(kp_ratio, 3),
        "cross_ratio": round(cross_ratio, 3),
        "pa_ratio":    round(pa_ratio, 3),
    }

    return round(index, 4), components


def calculate_goal_score(game_sot, minutes, xg_data, baseline):
    """DC Goal Score."""
    if not baseline or not minutes or minutes < 20:
        return None, {}

    nineties  = minutes / 90
    sot_per90 = game_sot / nineties

    xg   = xg_data.get("xg")   if xg_data else None
    xgot = xg_data.get("xgot") if xg_data else None

    xgot_gap    = (xgot - xg) if (xgot is not None and xg is not None) else 0
    xg_per90    = xg / nineties if xg else 0
    goals_per90 = baseline.get("goals_per90") or 0

    score = (
        xgot_gap    * GOAL_WEIGHTS["xgot_gap"] +
        sot_per90   * GOAL_WEIGHTS["sot_per90"] +
        xg_per90    * GOAL_WEIGHTS["xg_per90"] +
        goals_per90 * GOAL_WEIGHTS["goals_p90"]
    )

    components = {
        "xg":        xg,
        "xgot":      xgot,
        "xgot_gap":  round(xgot_gap, 4),
        "sot_per90": round(sot_per90, 3),
        "xg_per90":  round(xg_per90, 3),
    }

    return round(score, 4), components


def calculate_tsoa(assist_index, goal_score, game_kp, game_sot):
    """TSOA dual threat score."""
    if assist_index is None or goal_score is None:
        return None

    total = game_kp + game_sot
    dual_threat = (
        min(game_kp, game_sot) / max(game_kp, game_sot)
        if total > 0 and max(game_kp, game_sot) > 0
        else 0
    )

    raw = (assist_index * 0.50) + (goal_score * 0.50)
    return round(raw * (0.7 + dual_threat * 0.3) * 2.0, 4)


# ── MAIN MATCH DAY FUNCTION ───────────────────────────────────────────────────

def score_fixture(fixture_id, season_id, league_id, game_date=None):
    """Score all players in a fixture."""
    if game_date is None:
        game_date = date.today().isoformat()

    print(f"  Scoring fixture {fixture_id}...")

    xg_map  = pull_fixture_xg(fixture_id)
    lineups = pull_fixture_lineups(fixture_id)
    print(f"    Lineups: {len(lineups)} | xG players: {len(xg_map)}")

    scores = []

    for entry in lineups:
        player_id = entry.get("player_id")
        if not player_id:
            continue

        baseline = get_player_baseline(player_id, season_id)
        if not baseline:
            continue

        # Use baseline season stats as proxy for this game
        # (per-game stat granularity is a Gate 4 enhancement)
        minutes   = baseline.get("minutes_played", 90)
        kp        = baseline.get("key_passes_total", 0)
        acc_cross = baseline.get("acc_crosses_total", 0)
        sot       = baseline.get("sot_total", 0)
        pass_acc  = baseline.get("pass_accuracy_baseline")

        # Normalize to per-game averages using nineties
        nineties = baseline.get("nineties", 1)
        games    = max(nineties, 1)

        kp_game        = round(kp / games, 2)
        acc_cross_game = round(acc_cross / games, 2)
        sot_game       = round(sot / games, 2)

        xg_data = xg_map.get(player_id)

        assist_index, a_comp = calculate_assist_index(
            kp_game, acc_cross_game, pass_acc, 90, baseline
        )
        goal_score, g_comp = calculate_goal_score(
            sot_game, 90, xg_data, baseline
        )
        tsoa = calculate_tsoa(assist_index, goal_score, kp_game, sot_game)

        if assist_index is None:
            continue

        scores.append({
            "fixture_id":        fixture_id,
            "player_id":         player_id,
            "player_name":       entry.get("player_name"),
            "team_id":           entry.get("team_id"),
            "season_id":         season_id,
            "league_id":         league_id,
            "game_date":         game_date,
            "source":            "sportmonks",
            "minutes_played":    90,
            "key_passes":        kp_game,
            "acc_crosses":       acc_cross_game,
            "pass_accuracy":     round(pass_acc, 4) if pass_acc else None,
            "sot":               sot_game,
            "xg_game":           xg_data.get("xg")   if xg_data else None,
            "xgot_game":         xg_data.get("xgot") if xg_data else None,
            "assist_index":      assist_index,
            "goal_score":        goal_score,
            "tsoa":              tsoa,
            "assist_grade":      grade_color(assist_index or 0),
            "goal_grade":        grade_color(goal_score or 0),
            "tsoa_grade":        grade_color(tsoa or 0),
            "assist_components": str(a_comp),
            "goal_components":   str(g_comp),
            "scored_at":         datetime.utcnow().isoformat(),
        })

    if scores:
        supabase.table("sm_player_scores")\
            .upsert(scores, on_conflict="fixture_id,player_id")\
            .execute()
        print(f"    ✅ {len(scores)} scores stored")

    return scores


def score_todays_fixtures(leagues=None):
    """Score all fixtures playing today."""
    today = date.today().isoformat()
    league_config = leagues or [
        {"league_id": 8,    "season_id": 25583},
        {"league_id": 9,    "season_id": 25648},
        {"league_id": 564,  "season_id": 25659},
        {"league_id": 384,  "season_id": 25533},
        {"league_id": 82,   "season_id": 25646},
        {"league_id": 301,  "season_id": 25651},
        {"league_id": 779,  "season_id": 26720},
        {"league_id": 1356, "season_id": 26529},
    ]

    print(f"\n{'='*60}")
    print(f"  SPORTMONKS MATCH DAY SCORING — {today}")
    print(f"{'='*60}")

    total = 0
    for league in league_config:
        try:
            fixtures = get_todays_fixtures(league["league_id"], today)
            for fixture in fixtures:
                if fixture.get("state_id") in [2, 3, 4, 5]:
                    scored = score_fixture(
                        fixture["id"],
                        league["season_id"],
                        league["league_id"],
                        today
                    )
                    total += len(scored)
        except Exception as e:
            print(f"  Error league {league['league_id']}: {e}")

    print(f"\n  TOTAL SCORED TODAY: {total}")
    print("="*60)
