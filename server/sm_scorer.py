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

# Score scaling — per market to match FotMob distribution
# Targets: Assist ~56 players >3.0, Goals ~38, TSOA ~84
ASSIST_SCALE = 1.5
GOAL_SCALE   = 2.6

# League name lookup
LEAGUE_NAMES = {
    8:    "Premier League",
    9:    "Championship",
    564:  "La Liga",
    384:  "Serie A",
    82:   "Bundesliga",
    301:  "Ligue 1",
    779:  "MLS",
    1356: "A-League Men",
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
    """Pull lineup for a fixture."""
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

    b_kp    = baseline.get("kp_per90")              or 0.001
    b_cross = baseline.get("acc_cross_per90")       or 0.001
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

        minutes   = baseline.get("minutes_played", 90)
        kp        = baseline.get("key_passes_total", 0)
        acc_cross = baseline.get("acc_crosses_total", 0)
        sot       = baseline.get("sot_total", 0)
        pass_acc  = baseline.get("pass_accuracy_baseline")

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
            "team_name":         baseline.get("team_name"),
            "season_id":         season_id,
            "league_id":         league_id,
            "league_name":       LEAGUE_NAMES.get(league_id, ""),
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


# ── APP DATA FETCHERS ─────────────────────────────────────────────────────────

def get_latest_scores():
    """
    Fetch latest match day Sportmonks scores from Supabase.
    Called by GET /api/sm/data
    """
    try:
        res = supabase.table("sm_player_scores")\
            .select("*")\
            .order("tsoa", desc=True)\
            .limit(500)\
            .execute()

        players = []
        for row in (res.data or []):
            players.append({
                "player_id":             row.get("player_id"),
                "player_name":           row.get("player_name"),
                "team_id":               row.get("team_id"),
                "team_name":             row.get("team_name"),
                "league_id":             row.get("league_id"),
                "league_name":           row.get("league_name"),
                "fixture_id":            row.get("fixture_id"),
                "fixture_label":         row.get("fixture_label"),
                "game_date":             row.get("game_date"),
                "minutes_played":        row.get("minutes_played"),
                "kp_per90":              row.get("key_passes"),
                "acc_cross_per90":       row.get("acc_crosses"),
                "pass_accuracy":         row.get("pass_accuracy"),
                "sot_per90":             row.get("sot"),
                "xg_per90":              row.get("xg_game"),
                "xgot_gap":              _safe_xgot_gap(row),
                "assist_index":          row.get("assist_index"),
                "goal_score":            row.get("goal_score"),
                "tsoa_score":            row.get("tsoa"),
                "dual_threat":           row.get("dual_threat"),
                "baseline_kp_per90":     row.get("baseline_kp_per90"),
                "baseline_cross_per90":  row.get("baseline_cross_per90"),
                "baseline_pass_acc":     row.get("baseline_pass_acc"),
                "baseline_sot_per90":    row.get("baseline_sot_per90"),
                "kp_ratio":              row.get("kp_ratio"),
                "cross_ratio":           row.get("cross_ratio"),
                "pass_acc_ratio":        row.get("pass_acc_ratio"),
                "goals_per90":           row.get("goals_per90"),
                "concession_flag":       row.get("concession_flag"),
                "concession_multiplier": row.get("concession_multiplier"),
                "assist_grade":          row.get("assist_grade"),
                "goal_grade":            row.get("goal_grade"),
                "tsoa_grade":            row.get("tsoa_grade"),
                "scored_at":             row.get("scored_at"),
            })

        last_updated = players[0]["scored_at"] if players else None
        return {"players": players, "count": len(players), "source": "sportmonks", "last_updated": last_updated}

    except Exception as e:
        print(f"get_latest_scores error: {e}")
        return {"players": [], "count": 0, "source": "sportmonks", "error": str(e)}


def get_season_scores():
    """
    Calculate season-long DC scores for all players from player_baselines.
    Scores against league averages, scaled ×2.0 to match FotMob distribution.
    Minimum 270 minutes (3 full games) to appear in rankings.
    """
    try:
        res = supabase.table("player_baselines")\
            .select("*")\
            .execute()

        rows = res.data or []
        if not rows:
            return {"players": [], "count": 0, "source": "sportmonks_season", "error": "No baselines found"}

        # ── Step 1: Calculate league averages ─────────────────────────────────
        from collections import defaultdict

        league_stats = defaultdict(lambda: {
            "kp_per90": [], "acc_cross_per90": [], "sot_per90": [],
            "goals_per90": [], "pass_acc": []
        })

        for row in rows:
            lid = row.get("league_id")
            if not lid: continue
            if row.get("kp_per90"):              league_stats[lid]["kp_per90"].append(row["kp_per90"])
            if row.get("acc_cross_per90"):        league_stats[lid]["acc_cross_per90"].append(row["acc_cross_per90"])
            if row.get("sot_per90"):              league_stats[lid]["sot_per90"].append(row["sot_per90"])
            if row.get("goals_per90"):            league_stats[lid]["goals_per90"].append(row["goals_per90"])
            if row.get("pass_accuracy_baseline"): league_stats[lid]["pass_acc"].append(row["pass_accuracy_baseline"])

        def avg(lst): return sum(lst) / len(lst) if lst else 0.001

        league_avgs = {}
        for lid, stats in league_stats.items():
            league_avgs[lid] = {
                "kp_per90":    avg(stats["kp_per90"]),
                "cross_per90": avg(stats["acc_cross_per90"]),
                "sot_per90":   avg(stats["sot_per90"]),
                "goals_per90": avg(stats["goals_per90"]),
                "pass_acc":    avg(stats["pass_acc"]),
            }

        # ── Step 2: Score each player ──────────────────────────────────────────
        players = []

        for row in rows:
            lid     = row.get("league_id")
            minutes = row.get("minutes_played", 0)

            # Minimum 270 minutes (3 full games)
            if not minutes or minutes < 270:
                continue

            avgs = league_avgs.get(lid, {})
            if not avgs:
                continue

            kp_per90    = row.get("kp_per90")            or 0
            cross_per90 = row.get("acc_cross_per90")     or 0
            sot_per90   = row.get("sot_per90")           or 0
            goals_per90 = row.get("goals_per90")         or 0
            pass_acc    = row.get("pass_accuracy_baseline") or avgs.get("pass_acc", 0.001)

            # Ratios vs league average
            kp_ratio    = kp_per90    / avgs.get("kp_per90",    0.001)
            cross_ratio = cross_per90 / avgs.get("cross_per90", 0.001)
            pa_ratio    = pass_acc    / avgs.get("pass_acc",     0.001)
            sot_ratio   = sot_per90   / avgs.get("sot_per90",   0.001)

            # DC Assist Index (raw then scaled)
            assist_index_raw = (
                kp_ratio    * ASSIST_WEIGHTS["kp_ratio"] +
                cross_ratio * ASSIST_WEIGHTS["cross_ratio"] +
                pa_ratio    * ASSIST_WEIGHTS["pass_acc_ratio"]
            )
            assist_index = round(assist_index_raw * ASSIST_SCALE, 4)

            # DC Goal Score (raw then scaled)
            goal_score_raw = (
                sot_per90   * GOAL_WEIGHTS["sot_per90"] +
                goals_per90 * GOAL_WEIGHTS["goals_p90"] +
                sot_ratio   * GOAL_WEIGHTS["xgot_gap"]
            )
            goal_score = round(goal_score_raw * GOAL_SCALE, 4)

            # TSOA
            tsoa = calculate_tsoa(assist_index, goal_score, kp_per90, sot_per90)

            players.append({
                "player_id":        row.get("player_id"),
                "player_name":      row.get("player_name"),
                "team_id":          row.get("team_id"),
                "team_name":        row.get("team_name"),   # ← now populated
                "league_id":        lid,
                "league_name":      LEAGUE_NAMES.get(lid, ""),
                "minutes_played":   minutes,
                "nineties":         row.get("nineties"),
                # Per-90 stats
                "kp_per90":         kp_per90,
                "acc_cross_per90":  cross_per90,
                "sot_per90":        sot_per90,
                "goals_per90":      goals_per90,
                "pass_accuracy":    pass_acc,
                # League avg context
                "league_avg_kp":    round(avgs.get("kp_per90", 0), 3),
                "league_avg_cross": round(avgs.get("cross_per90", 0), 3),
                "league_avg_sot":   round(avgs.get("sot_per90", 0), 3),
                # Ratios
                "kp_ratio":         round(kp_ratio, 3),
                "cross_ratio":      round(cross_ratio, 3),
                "pass_acc_ratio":   round(pa_ratio, 3),
                # Scores (scaled)
                "assist_index":     assist_index,
                "goal_score":       goal_score,
                "tsoa_score":       tsoa,
                # Grades
                "assist_grade":     grade_color(assist_index or 0),
                "goal_grade":       grade_color(goal_score or 0),
                "tsoa_grade":       grade_color(tsoa or 0),
                # Baseline refs for modal
                "baseline_kp_per90":    kp_per90,
                "baseline_cross_per90": cross_per90,
                "baseline_sot_per90":   sot_per90,
                "data_source":          "season_baseline",
            })

        # Sort by tsoa descending
        players.sort(key=lambda p: p.get("tsoa_score") or 0, reverse=True)

        return {
            "players":      players,
            "count":        len(players),
            "source":       "sportmonks_season",
            "last_updated": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        print(f"get_season_scores error: {e}")
        return {"players": [], "count": 0, "source": "sportmonks_season", "error": str(e)}


def _safe_xgot_gap(row):
    """Calculate xGOT gap from stored xg and xgot values."""
    xg   = row.get("xg_game")
    xgot = row.get("xgot_game")
    if xg is not None and xgot is not None:
        return round(xgot - xg, 4)
    return None
