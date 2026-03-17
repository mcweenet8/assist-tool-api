"""
Deep Current Football — Pipeline Comparison Logger
server/pipeline_comparison.py

Tracks FotMob vs Sportmonks scoring performance head to head.
After each gameday, records:
  - Both pipeline scores per player
  - Actual assist/goal outcomes
  - Running hit rate for each pipeline

Supabase tables needed:
  - pipeline_comparison  (one row per player per gameday)
  - gameday_outcomes     (actual results after match)
"""

import os
from datetime import datetime, date
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── SUPABASE MIGRATION (run once) ─────────────────────────────────────────────
MIGRATION_SQL = """
-- Pipeline comparison table
CREATE TABLE IF NOT EXISTS pipeline_comparison (
    id                  SERIAL PRIMARY KEY,
    game_date           DATE NOT NULL,
    fixture_id          INTEGER NOT NULL,
    player_id           INTEGER NOT NULL,
    player_name         VARCHAR(100),
    team_id             INTEGER,
    league_id           INTEGER,
    position_code       VARCHAR(6),

    -- FotMob pipeline scores
    fotmob_assist_score FLOAT,
    fotmob_goal_score   FLOAT,
    fotmob_tsoa         FLOAT,
    fotmob_assist_rank  INTEGER,
    fotmob_goal_rank    INTEGER,
    fotmob_tsoa_rank    INTEGER,

    -- Sportmonks pipeline scores
    sm_assist_index     FLOAT,
    sm_goal_score       FLOAT,
    sm_tsoa             FLOAT,
    sm_assist_rank      INTEGER,
    sm_goal_rank        INTEGER,
    sm_tsoa_rank        INTEGER,

    -- Actual outcomes (filled after match)
    actual_goals        INTEGER DEFAULT 0,
    actual_assists      INTEGER DEFAULT 0,
    outcome_recorded    BOOLEAN DEFAULT FALSE,

    created_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE(fixture_id, player_id)
);

-- Gameday performance summary
CREATE TABLE IF NOT EXISTS gameday_performance (
    id                      SERIAL PRIMARY KEY,
    game_date               DATE NOT NULL UNIQUE,
    league_ids              TEXT,

    -- FotMob pipeline
    fotmob_top10_assist_hits  INTEGER DEFAULT 0,
    fotmob_top10_goal_hits    INTEGER DEFAULT 0,
    fotmob_top10_tsoa_hits    INTEGER DEFAULT 0,
    fotmob_top25_assist_hits  INTEGER DEFAULT 0,
    fotmob_top25_goal_hits    INTEGER DEFAULT 0,
    fotmob_top25_tsoa_hits    INTEGER DEFAULT 0,

    -- Sportmonks pipeline
    sm_top10_assist_hits      INTEGER DEFAULT 0,
    sm_top10_goal_hits        INTEGER DEFAULT 0,
    sm_top10_tsoa_hits        INTEGER DEFAULT 0,
    sm_top25_assist_hits      INTEGER DEFAULT 0,
    sm_top25_goal_hits        INTEGER DEFAULT 0,
    sm_top25_tsoa_hits        INTEGER DEFAULT 0,

    -- Total actual events
    total_goals             INTEGER DEFAULT 0,
    total_assists           INTEGER DEFAULT 0,
    total_players_scored    INTEGER DEFAULT 0,

    created_at              TIMESTAMP DEFAULT NOW()
);

-- Baseline table for Sportmonks
CREATE TABLE IF NOT EXISTS player_baselines (
    id                      SERIAL PRIMARY KEY,
    player_id               INTEGER NOT NULL,
    player_name             VARCHAR(100),
    season_id               INTEGER NOT NULL,
    league_id               INTEGER,
    position_id             INTEGER,
    detailed_position_id    INTEGER,
    minutes_played          FLOAT,
    nineties                FLOAT,
    key_passes_total        INTEGER DEFAULT 0,
    acc_crosses_total       INTEGER DEFAULT 0,
    sot_total               INTEGER DEFAULT 0,
    goals_total             INTEGER DEFAULT 0,
    kp_per90                FLOAT DEFAULT 0,
    acc_cross_per90         FLOAT DEFAULT 0,
    sot_per90               FLOAT DEFAULT 0,
    goals_per90             FLOAT DEFAULT 0,
    dribbles_per90          FLOAT DEFAULT 0,
    pass_accuracy_baseline  FLOAT,
    xg_per90                FLOAT,
    xgot_per90              FLOAT,
    last_updated            TIMESTAMP,
    UNIQUE(player_id, season_id)
);

-- Sportmonks match scores
CREATE TABLE IF NOT EXISTS sm_player_scores (
    id                  SERIAL PRIMARY KEY,
    fixture_id          INTEGER NOT NULL,
    player_id           INTEGER NOT NULL,
    player_name         VARCHAR(100),
    team_id             INTEGER,
    season_id           INTEGER,
    league_id           INTEGER,
    game_date           DATE,
    source              VARCHAR(20) DEFAULT 'sportmonks',
    minutes_played      FLOAT,
    key_passes          INTEGER,
    acc_crosses         INTEGER,
    pass_accuracy       FLOAT,
    sot                 INTEGER,
    xg_game             FLOAT,
    xgot_game           FLOAT,
    assist_index        FLOAT,
    goal_score          FLOAT,
    tsoa                FLOAT,
    assist_grade        VARCHAR(10),
    goal_grade          VARCHAR(10),
    tsoa_grade          VARCHAR(10),
    assist_components   TEXT,
    goal_components     TEXT,
    scored_at           TIMESTAMP,
    UNIQUE(fixture_id, player_id)
);
"""


# ── COMPARISON FUNCTIONS ──────────────────────────────────────────────────────

def build_comparison_for_date(game_date=None):
    """
    After both pipelines have run for the day, merge their scores
    into the pipeline_comparison table with rankings.
    """
    if game_date is None:
        game_date = date.today().isoformat()

    print(f"\nBuilding comparison for {game_date}...")

    # Get FotMob scores for today (from existing player_scores table)
    fotmob_scores = supabase.table("player_scores")\
        .select("*")\
        .eq("game_date", game_date)\
        .execute().data or []

    # Get Sportmonks scores for today
    sm_scores = supabase.table("sm_player_scores")\
        .select("*")\
        .eq("game_date", game_date)\
        .execute().data or []

    print(f"  FotMob scores: {len(fotmob_scores)}")
    print(f"  Sportmonks scores: {len(sm_scores)}")

    if not fotmob_scores and not sm_scores:
        print("  No scores found for today")
        return

    # Build lookup maps
    fotmob_map = {s["player_id"]: s for s in fotmob_scores}
    sm_map     = {s["player_id"]: s for s in sm_scores}

    # Get all unique players
    all_player_ids = set(fotmob_map.keys()) | set(sm_map.keys())

    # Calculate rankings for FotMob
    fotmob_assist_ranked = sorted(
        fotmob_scores, key=lambda x: x.get("assist_score", 0) or 0, reverse=True
    )
    fotmob_goal_ranked = sorted(
        fotmob_scores, key=lambda x: x.get("goal_score", 0) or 0, reverse=True
    )
    fotmob_tsoa_ranked = sorted(
        fotmob_scores, key=lambda x: x.get("tsoa", 0) or 0, reverse=True
    )

    fotmob_assist_ranks = {s["player_id"]: i+1 for i, s in enumerate(fotmob_assist_ranked)}
    fotmob_goal_ranks   = {s["player_id"]: i+1 for i, s in enumerate(fotmob_goal_ranked)}
    fotmob_tsoa_ranks   = {s["player_id"]: i+1 for i, s in enumerate(fotmob_tsoa_ranked)}

    # Calculate rankings for Sportmonks
    sm_assist_ranked = sorted(
        sm_scores, key=lambda x: x.get("assist_index", 0) or 0, reverse=True
    )
    sm_goal_ranked = sorted(
        sm_scores, key=lambda x: x.get("goal_score", 0) or 0, reverse=True
    )
    sm_tsoa_ranked = sorted(
        sm_scores, key=lambda x: x.get("tsoa", 0) or 0, reverse=True
    )

    sm_assist_ranks = {s["player_id"]: i+1 for i, s in enumerate(sm_assist_ranked)}
    sm_goal_ranks   = {s["player_id"]: i+1 for i, s in enumerate(sm_goal_ranked)}
    sm_tsoa_ranks   = {s["player_id"]: i+1 for i, s in enumerate(sm_tsoa_ranked)}

    # Build comparison records
    records = []
    for pid in all_player_ids:
        fm = fotmob_map.get(pid, {})
        sm = sm_map.get(pid, {})

        record = {
            "game_date":   game_date,
            "fixture_id":  fm.get("fixture_id") or sm.get("fixture_id"),
            "player_id":   pid,
            "player_name": fm.get("player_name") or sm.get("player_name"),
            "team_id":     fm.get("team_id") or sm.get("team_id"),
            "league_id":   fm.get("league_id") or sm.get("league_id"),

            # FotMob scores
            "fotmob_assist_score": fm.get("assist_score"),
            "fotmob_goal_score":   fm.get("goal_score"),
            "fotmob_tsoa":         fm.get("tsoa"),
            "fotmob_assist_rank":  fotmob_assist_ranks.get(pid),
            "fotmob_goal_rank":    fotmob_goal_ranks.get(pid),
            "fotmob_tsoa_rank":    fotmob_tsoa_ranks.get(pid),

            # Sportmonks scores
            "sm_assist_index":  sm.get("assist_index"),
            "sm_goal_score":    sm.get("goal_score"),
            "sm_tsoa":          sm.get("tsoa"),
            "sm_assist_rank":   sm_assist_ranks.get(pid),
            "sm_goal_rank":     sm_goal_ranks.get(pid),
            "sm_tsoa_rank":     sm_tsoa_ranks.get(pid),

            "outcome_recorded": False,
        }
        records.append(record)

    if records:
        supabase.table("pipeline_comparison")\
            .upsert(records, on_conflict="fixture_id,player_id")\
            .execute()
        print(f"  ✅ Comparison built: {len(records)} players")


def record_outcomes(game_date=None):
    """
    After matches complete, pull actual goal/assist events from Sportmonks
    and update the comparison table with real outcomes.
    Call ~2 hours after last match of the day.
    """
    if game_date is None:
        game_date = date.today().isoformat()

    print(f"\nRecording outcomes for {game_date}...")

    # Get all comparison records for today without outcomes
    records = supabase.table("pipeline_comparison")\
        .select("*")\
        .eq("game_date", game_date)\
        .eq("outcome_recorded", False)\
        .execute().data or []

    if not records:
        print("  No pending records")
        return

    # Get unique fixture IDs
    fixture_ids = list(set(r["fixture_id"] for r in records if r["fixture_id"]))

    # Pull goal events for each fixture
    goals_map    = {}   # player_id -> goal count
    assists_map  = {}   # player_id -> assist count

    for fixture_id in fixture_ids:
        try:
            import requests
            SPORTMONKS_TOKEN = os.environ.get("SPORTMONKS_API_TOKEN")
            BASE_URL = "https://api.sportmonks.com/v3/football"
            resp = requests.get(
                f"{BASE_URL}/fixtures/{fixture_id}",
                params={
                    "api_token": SPORTMONKS_TOKEN,
                    "include": "events",
                    "filters": "eventTypes:14"
                }
            ).json()

            events = resp.get("data", {}).get("events", [])
            if isinstance(events, dict):
                events = events.get("data", [])

            for event in events:
                if event.get("type_id") == 14:  # goal
                    scorer_id   = event.get("player_id")
                    assister_id = event.get("related_player_id")

                    if scorer_id:
                        goals_map[scorer_id] = goals_map.get(scorer_id, 0) + 1
                    if assister_id:
                        assists_map[assister_id] = assists_map.get(assister_id, 0) + 1

        except Exception as e:
            print(f"  Error fetching events for fixture {fixture_id}: {e}")

    # Update comparison records with outcomes
    updated = 0
    for record in records:
        pid = record["player_id"]
        goals   = goals_map.get(pid, 0)
        assists = assists_map.get(pid, 0)

        supabase.table("pipeline_comparison")\
            .update({
                "actual_goals":     goals,
                "actual_assists":   assists,
                "outcome_recorded": True,
            })\
            .eq("id", record["id"])\
            .execute()
        updated += 1

    print(f"  ✅ Outcomes recorded for {updated} players")
    _calculate_gameday_performance(game_date)


def _calculate_gameday_performance(game_date):
    """Calculate and store hit rate summary for the gameday."""

    records = supabase.table("pipeline_comparison")\
        .select("*")\
        .eq("game_date", game_date)\
        .eq("outcome_recorded", True)\
        .execute().data or []

    if not records:
        return

    def hits_in_top_n(records, rank_field, outcome_field, n):
        top_n = [r for r in records if r.get(rank_field) and r[rank_field] <= n]
        return sum(1 for r in top_n if r.get(outcome_field, 0) > 0)

    total_goals   = sum(r.get("actual_goals", 0) for r in records)
    total_assists = sum(r.get("actual_assists", 0) for r in records)

    perf = {
        "game_date": game_date,
        "total_goals":          total_goals,
        "total_assists":        total_assists,
        "total_players_scored": len(records),

        # FotMob hits
        "fotmob_top10_assist_hits": hits_in_top_n(records, "fotmob_assist_rank", "actual_assists", 10),
        "fotmob_top10_goal_hits":   hits_in_top_n(records, "fotmob_goal_rank",   "actual_goals",   10),
        "fotmob_top10_tsoa_hits":   hits_in_top_n(records, "fotmob_tsoa_rank",   "actual_goals",   10),
        "fotmob_top25_assist_hits": hits_in_top_n(records, "fotmob_assist_rank", "actual_assists", 25),
        "fotmob_top25_goal_hits":   hits_in_top_n(records, "fotmob_goal_rank",   "actual_goals",   25),
        "fotmob_top25_tsoa_hits":   hits_in_top_n(records, "fotmob_tsoa_rank",   "actual_goals",   25),

        # Sportmonks hits
        "sm_top10_assist_hits": hits_in_top_n(records, "sm_assist_rank", "actual_assists", 10),
        "sm_top10_goal_hits":   hits_in_top_n(records, "sm_goal_rank",   "actual_goals",   10),
        "sm_top10_tsoa_hits":   hits_in_top_n(records, "sm_tsoa_rank",   "actual_goals",   10),
        "sm_top25_assist_hits": hits_in_top_n(records, "sm_assist_rank", "actual_assists", 25),
        "sm_top25_goal_hits":   hits_in_top_n(records, "sm_goal_rank",   "actual_goals",   25),
        "sm_top25_tsoa_hits":   hits_in_top_n(records, "sm_tsoa_rank",   "actual_goals",   25),
    }

    supabase.table("gameday_performance")\
        .upsert(perf, on_conflict="game_date")\
        .execute()

    print(f"\n  GAMEDAY PERFORMANCE SUMMARY — {game_date}")
    print(f"  Total goals: {total_goals} | Total assists: {total_assists}")
    print(f"\n  {'Metric':<30} {'FotMob':>8} {'Sportmonks':>12}")
    print(f"  {'-'*52}")
    print(f"  {'Top 10 Assist Hits':<30} {perf['fotmob_top10_assist_hits']:>8} {perf['sm_top10_assist_hits']:>12}")
    print(f"  {'Top 10 Goal Hits':<30} {perf['fotmob_top10_goal_hits']:>8} {perf['sm_top10_goal_hits']:>12}")
    print(f"  {'Top 25 Assist Hits':<30} {perf['fotmob_top25_assist_hits']:>8} {perf['sm_top25_assist_hits']:>12}")
    print(f"  {'Top 25 Goal Hits':<30} {perf['fotmob_top25_goal_hits']:>8} {perf['sm_top25_goal_hits']:>12}")


def get_running_totals():
    """
    Print cumulative head-to-head performance across all recorded gamedays.
    """
    rows = supabase.table("gameday_performance")\
        .select("*")\
        .order("game_date")\
        .execute().data or []

    if not rows:
        print("No gameday performance data yet")
        return

    def total(field):
        return sum(r.get(field, 0) for r in rows)

    print(f"\n{'='*60}")
    print(f"  CUMULATIVE HEAD TO HEAD — {len(rows)} gamedays")
    print(f"{'='*60}")
    print(f"  {'Metric':<30} {'FotMob':>8} {'Sportmonks':>12} {'Winner':>8}")
    print(f"  {'-'*60}")

    metrics = [
        ("Top 10 Assist Hits",  "fotmob_top10_assist_hits", "sm_top10_assist_hits"),
        ("Top 10 Goal Hits",    "fotmob_top10_goal_hits",   "sm_top10_goal_hits"),
        ("Top 25 Assist Hits",  "fotmob_top25_assist_hits", "sm_top25_assist_hits"),
        ("Top 25 Goal Hits",    "fotmob_top25_goal_hits",   "sm_top25_goal_hits"),
    ]

    for label, fm_field, sm_field in metrics:
        fm_total = total(fm_field)
        sm_total = total(sm_field)
        winner = "SM ✅" if sm_total > fm_total else ("FM ✅" if fm_total > sm_total else "TIE")
        print(f"  {label:<30} {fm_total:>8} {sm_total:>12} {winner:>8}")

    print(f"\n  Total gamedays: {len(rows)}")
    print(f"  Total goals tracked: {total('total_goals')}")
    print(f"  Total assists tracked: {total('total_assists')}")
    print("="*60)
