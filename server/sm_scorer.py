"""
Deep Current Football — Sportmonks Match Day Scorer
server/sm_scorer.py

Auth: Authorization header
URL:  /fixtures/between/{date}/{date}?filters=fixtureLeagues:{id}&include=...
Sep:  semicolons for multiple includes
"""

import os
import requests
from datetime import datetime, date
from supabase import create_client
from sm_baseline import get_player_baseline, TYPE_IDS, _extract_stat, _sm_get

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
    """
    Pull today's fixtures for a league using the between endpoint.
    Pattern from Sportmonks official Python guide.
    """
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
    Pull lineups with player details for a fixture.
    Returns list of lineup entries with position data.
    """
    resp = _sm_get(
        endpoint=f"/fixtures/{fixture_id}",
        include="lineups;lineups.player;lineups.player.statistics.details.type",
    )
    data = resp.get("data", {})
    lineups = data.get("lineups", [])
    if isinstance(lineups, dict):
        lineups = lineups.get("data", [])
    return lineups


def pull_fixture_xg(fixture_id):
    """
    Pull per-player xG and xGOT from xG add-on.
    Uses lineups.xGLineup include — confirmed from Sportmonks xG blog.
    Returns {player_id: {"xg": float, "xgot": float}}
    """
    resp = _sm_get(
        endpoint=f"/fixtures/{fixture_id}",
        include="lineups;lineups.xGLineup",
    )
    data = resp.get("data", {})
    lineups = data.get("lineups", [])
    if isinstance(lineups, dict):
        lineups = lineups.get("data", [])

    xg_map = {}
    for entry in lineups:
        player_id = entry.get("player_id")
        if not player_id:
            continue

        xg_lineup = entry.get("xglineup", [])
        if isinstance(xg_lineup, dict):
            xg_lineup = xg_lineup.get("data", [])

        xg = xgot = None
        for item in xg_lineup:
            type_id = item.get("type_id")
            val = item.get("data", {}).get("value")
            if type_id == 5304:
                xg = val
            elif type_id == 5305:
                xgot = val

        if xg is not None or xgot is not None:
            xg_map[player_id] = {"xg": xg, "xgot": xgot}

    return xg_map


def pull_player_game_stats(player_id, season_id):
    """
    Pull a player's current season stats to extract this game's contribution.
    Note: Sportmonks player stats are season cumulative — we use baselines
    for per-game estimation at this stage. Per-fixture stats are a Gate 4 enhancement.
    """
    resp = _sm_get(
        endpoint=f"/players/{player_id}",
        filters=f"playerStatisticSeasons:{season_id}",
        include="statistics.details.type",
    )
    data = resp.get("data", {})
    stats = data.get("statistics", [])
    if isinstance(stats, dict):
        stats = stats.get("data", [])

    for s in stats:
        if s.get("season_id") == season_id:
            details = s.get("details", [])
            if isinstance(details, dict):
                details = details.get("data", [])
            return details

    return []


# ── SCORING FUNCTIONS ─────────────────────────────────────────────────────────

def calculate_assist_index(game_kp, game_crosses, game_pass_acc, minutes, baseline):
    """
    DC Assist Probability Index.
    Compares this game's per-90 values against the player's season baseline.
    """
    if not baseline or not minutes or minutes < 20:
        return None, {}

    nineties = minutes / 90
    kp_per90    = game_kp / nineties
    cross_per90 = game_crosses / nineties

    b_kp    = baseline.get("kp_per90")    or 0.001
    b_cross = baseline.get("acc_cross_per90") or 0.001
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
    """DC Goal Score using Sportmonks fields + xG add-on."""
    if not baseline or not minutes or minutes < 20:
        return None, {}

    nineties  = minutes / 90
    sot_per90 = game_sot / nineties

    xg   = xg_data.get("xg")   if xg_data else None
    xgot = xg_data.get("xgot") if xg_data else None

    xgot_gap     = (xgot - xg) if (xgot is not None and xg is not None) else 0
    xg_per90     = xg / nineties if xg else 0
    goals_per90  = baseline.get("goals_per90") or 0

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
    """Score all players in a fixture. Stores to sm_player_scores."""
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

        # Pull this player's season stats
        details = pull_player_game_stats(player_id, season_id)

        minutes      = _extract_stat(details, TYPE_IDS["MINUTES_PLAYED"])
        kp           = _extract_stat(details, TYPE_IDS["KEY_PASSES"]) or 0
        acc_cross    = _extract_stat(details, TYPE_IDS["ACCURATE_CROSSES"]) or 0
        acc_passes   = _extract_stat(details, TYPE_IDS["ACCURATE_PASSES"]) or 0
        tot_passes   = _extract_stat(details, TYPE_IDS["PASSES"]) or 0
        pass_acc_pct = _extract_stat(details, TYPE_IDS["ACCURATE_PASSES_PERCENTAGE"])
        sot          = _extract_stat(details, TYPE_IDS["SHOTS_ON_TARGET"]) or 0

        if pass_acc_pct is not None:
            pass_acc = pass_acc_pct / 100 if pass_acc_pct > 1 else pass_acc_pct
        elif tot_passes > 0:
            pass_acc = acc_passes / tot_passes
        else:
            pass_acc = baseline.get("pass_accuracy_baseline")

        xg_data = xg_map.get(player_id)

        assist_index, a_comp = calculate_assist_index(
            kp, acc_cross, pass_acc, minutes or 90, baseline
        )
        goal_score, g_comp = calculate_goal_score(
            sot, minutes or 90, xg_data, baseline
        )
        tsoa = calculate_tsoa(assist_index, goal_score, kp, sot)

        if assist_index is None:
            continue

        scores.append({
            "fixture_id":       fixture_id,
            "player_id":        player_id,
            "player_name":      entry.get("player_name"),
            "team_id":          entry.get("team_id"),
            "season_id":        season_id,
            "league_id":        league_id,
            "game_date":        game_date,
            "source":           "sportmonks",
            "minutes_played":   minutes or 0,
            "key_passes":       kp,
            "acc_crosses":      acc_cross,
            "pass_accuracy":    round(pass_acc, 4) if pass_acc else None,
            "sot":              sot,
            "xg_game":          xg_data.get("xg")   if xg_data else None,
            "xgot_game":        xg_data.get("xgot") if xg_data else None,
            "assist_index":     assist_index,
            "goal_score":       goal_score,
            "tsoa":             tsoa,
            "assist_grade":     grade_color(assist_index or 0),
            "goal_grade":       grade_color(goal_score or 0),
            "tsoa_grade":       grade_color(tsoa or 0),
            "assist_components": str(a_comp),
            "goal_components":   str(g_comp),
            "scored_at":        datetime.utcnow().isoformat(),
        })

    if scores:
        supabase.table("sm_player_scores")\
            .upsert(scores, on_conflict="fixture_id,player_id")\
            .execute()
        print(f"    ✅ {len(scores)} scores stored")

    return scores


def score_todays_fixtures(leagues=None):
    """
    Score all fixtures playing today across all covered leagues.
    Uses fixtures/between endpoint — official Sportmonks recommended pattern.
    """
    today = date.today().isoformat()
    league_config = leagues or [
        {"league_id": 8,    "season_id": 23614},
        {"league_id": 5,    "season_id": 23599},
        {"league_id": 564,  "season_id": 23686},
        {"league_id": 384,  "season_id": 23615},
        {"league_id": 82,   "season_id": 23538},
        {"league_id": 301,  "season_id": 23611},
        {"league_id": 1351, "season_id": 23700},
        {"league_id": 174,  "season_id": 23645},
    ]

    print(f"\n{'='*60}")
    print(f"  SPORTMONKS MATCH DAY SCORING — {today}")
    print(f"{'='*60}")

    total = 0
    for league in league_config:
        try:
            fixtures = get_todays_fixtures(league["league_id"], today)
            for fixture in fixtures:
                # Score completed (5) or in-play (2,3,4) fixtures
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
