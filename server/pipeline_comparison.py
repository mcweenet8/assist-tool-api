"""
Deep Current Football — SM Outcomes Tracker
server/pipeline_comparison.py

Tracks Sportmonks pipeline performance over time.
Purely SM data — no FotMob dependency.

After each match day:
  1. build_comparison_for_date() — snapshot SM rankings for today's fixtures
  2. record_outcomes()           — pull actual goals/assists from Sportmonks events
  3. get_running_totals()        — cumulative hit rate summary

Supabase table: sm_matchday_results
"""

import os
import requests
from datetime import datetime, date
from supabase import create_client

SUPABASE_URL         = os.environ.get("SUPABASE_URL")
SUPABASE_KEY         = os.environ.get("SUPABASE_SERVICE_KEY")
SPORTMONKS_TOKEN     = os.environ.get("SPORTMONKS_API_TOKEN")
BASE_URL             = "https://api.sportmonks.com/v3/football"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _sm_get(endpoint, params=None):
    headers = {"Authorization": SPORTMONKS_TOKEN}
    r = requests.get(f"{BASE_URL}{endpoint}", headers=headers, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()


# ── FUNCTION 1: SNAPSHOT SM RANKINGS FOR TODAY ───────────────────────────────

def build_comparison_for_date(game_date=None):
    """
    After SM scoring runs for the day, snapshot the rankings
    into sm_matchday_results for outcome tracking later.
    Call immediately after score-today completes.
    """
    if game_date is None:
        game_date = date.today().isoformat()

    print(f"\nBuilding SM snapshot for {game_date}...")

    sm_scores = supabase.table("sm_player_scores")\
        .select("*")\
        .eq("game_date", game_date)\
        .execute().data or []

    if not sm_scores:
        print(f"  No SM scores found for {game_date}")
        return

    print(f"  SM scores found: {len(sm_scores)}")

    fixture_ids = list(set(s["fixture_id"] for s in sm_scores if s.get("fixture_id")))
    records = []

    for fixture_id in fixture_ids:
        fixture_scores = [s for s in sm_scores if s.get("fixture_id") == fixture_id]

        assist_ranked = sorted(fixture_scores, key=lambda x: x.get("assist_index") or 0, reverse=True)
        goal_ranked   = sorted(fixture_scores, key=lambda x: x.get("goal_score") or 0, reverse=True)
        tsoa_ranked   = sorted(fixture_scores, key=lambda x: x.get("tsoa") or 0, reverse=True)

        assist_ranks = {s["player_id"]: i+1 for i, s in enumerate(assist_ranked)}
        goal_ranks   = {s["player_id"]: i+1 for i, s in enumerate(goal_ranked)}
        tsoa_ranks   = {s["player_id"]: i+1 for i, s in enumerate(tsoa_ranked)}

        for s in fixture_scores:
            pid = s["player_id"]
            records.append({
                "game_date":        game_date,
                "fixture_id":       fixture_id,
                "player_id":        pid,
                "player_name":      s.get("player_name"),
                "team_name":        s.get("team_name"),
                "league_id":        s.get("league_id"),
                "sm_assist_rank":   assist_ranks.get(pid),
                "sm_goal_rank":     goal_ranks.get(pid),
                "sm_tsoa_rank":     tsoa_ranks.get(pid),
                "assist_index":     s.get("assist_index"),
                "goal_score":       s.get("goal_score"),
                "tsoa_score":       s.get("tsoa"),
                "concession_flag":  s.get("concession_flag"),
                "outcome_recorded": False,
            })

    if records:
        supabase.table("sm_matchday_results")\
            .upsert(records, on_conflict="fixture_id,player_id")\
            .execute()
        print(f"  ✅ Snapshot saved: {len(records)} players across {len(fixture_ids)} fixtures")


# ── FUNCTION 2: RECORD ACTUAL OUTCOMES ───────────────────────────────────────

def record_outcomes(game_date=None):
    """
    Pull actual goal/assist events from Sportmonks and update
    sm_matchday_results with real outcomes.
    Call ~2 hours after last match of the day.
    """
    if game_date is None:
        game_date = date.today().isoformat()

    print(f"\nRecording SM outcomes for {game_date}...")

    records = supabase.table("sm_matchday_results")\
        .select("*")\
        .eq("game_date", game_date)\
        .eq("outcome_recorded", False)\
        .execute().data or []

    if not records:
        print("  No pending records")
        return

    fixture_ids = list(set(r["fixture_id"] for r in records if r["fixture_id"]))
    print(f"  Fixtures to process: {len(fixture_ids)}")

    goals_map   = {}
    assists_map = {}

    for fixture_id in fixture_ids:
        try:
            resp = _sm_get(
                f"/fixtures/{fixture_id}",
                {"include": "events", "filters": "eventTypes:14"}
            )
            events = resp.get("data", {}).get("events", [])
            if isinstance(events, dict):
                events = events.get("data", [])

            for event in events:
                if event.get("type_id") == 14:
                    scorer_id   = event.get("player_id")
                    assister_id = event.get("related_player_id")
                    if scorer_id:
                        goals_map[scorer_id] = goals_map.get(scorer_id, 0) + 1
                    if assister_id:
                        assists_map[assister_id] = assists_map.get(assister_id, 0) + 1

        except Exception as e:
            print(f"  Error fetching events for fixture {fixture_id}: {e}")

    updated = 0
    for record in records:
        pid     = record["player_id"]
        goals   = goals_map.get(pid, 0)
        assists = assists_map.get(pid, 0)

        supabase.table("sm_matchday_results")\
            .update({
                "actual_goals":     goals,
                "actual_assists":   assists,
                "had_contribution": (goals + assists) > 0,
                "outcome_recorded": True,
            })\
            .eq("id", record["id"])\
            .execute()
        updated += 1

    print(f"  ✅ Outcomes recorded for {updated} players")
    _calculate_performance_summary(game_date)


# ── PERFORMANCE SUMMARY ───────────────────────────────────────────────────────

def _calculate_performance_summary(game_date):
    """Print hit rate summary for the gameday."""

    records = supabase.table("sm_matchday_results")\
        .select("*")\
        .eq("game_date", game_date)\
        .eq("outcome_recorded", True)\
        .execute().data or []

    if not records:
        return

    def hits_in_top_n(rank_field, outcome_field, n):
        top_n = [r for r in records if r.get(rank_field) and r[rank_field] <= n]
        return sum(1 for r in top_n if (r.get(outcome_field) or 0) > 0)

    def hit_rate(hits, n, fixtures):
        total = n * fixtures
        return f"{hits}/{total} ({round(hits/total*100) if total else 0}%)"

    fixtures      = len(set(r["fixture_id"] for r in records))
    total_goals   = sum(r.get("actual_goals", 0) for r in records)
    total_assists = sum(r.get("actual_assists", 0) for r in records)
    flagged       = [r for r in records if r.get("concession_flag")]
    flagged_hits  = sum(1 for r in flagged if r.get("had_contribution"))

    print(f"\n{'='*55}")
    print(f"  SM PERFORMANCE — {game_date}")
    print(f"{'='*55}")
    print(f"  Fixtures: {fixtures} | Goals: {total_goals} | Assists: {total_assists}")
    print(f"\n  {'Metric':<28} {'Hits':>8} {'Rate':>10}")
    print(f"  {'-'*48}")

    for label, rank_field, outcome_field in [
        ("Top 5 Assist",  "sm_assist_rank", "actual_assists"),
        ("Top 10 Assist", "sm_assist_rank", "actual_assists"),
        ("Top 5 Goal",    "sm_goal_rank",   "actual_goals"),
        ("Top 10 Goal",   "sm_goal_rank",   "actual_goals"),
        ("Top 5 TSOA",    "sm_tsoa_rank",   "had_contribution"),
        ("Top 10 TSOA",   "sm_tsoa_rank",   "had_contribution"),
    ]:
        n    = 5 if "5" in label else 10
        h    = hits_in_top_n(rank_field, outcome_field, n)
        rate = hit_rate(h, n, fixtures)
        print(f"  {label:<28} {h:>8} {rate:>10}")

    if flagged:
        print(f"\n  Concession flagged: {len(flagged)} | Hits: {flagged_hits} ({round(flagged_hits/len(flagged)*100)}%)")
    print("="*55)


# ── FUNCTION 3: RUNNING TOTALS ────────────────────────────────────────────────

def get_running_totals():
    """Cumulative SM hit rate across all recorded match days."""

    rows = supabase.table("sm_matchday_results")\
        .select("*")\
        .eq("outcome_recorded", True)\
        .execute().data or []

    if not rows:
        print("No outcome data yet")
        return {"message": "No outcome data yet"}

    game_dates    = sorted(set(r["game_date"] for r in rows))
    fixtures      = len(set(r["fixture_id"] for r in rows))
    total_goals   = sum(r.get("actual_goals", 0) for r in rows)
    total_assists = sum(r.get("actual_assists", 0) for r in rows)
    flagged       = [r for r in rows if r.get("concession_flag")]
    flagged_hits  = sum(1 for r in flagged if r.get("had_contribution"))

    def hits(rank_field, outcome_field, n):
        top_n = [r for r in rows if r.get(rank_field) and r[rank_field] <= n]
        return sum(1 for r in top_n if (r.get(outcome_field) or 0) > 0)

    def rate(h, n):
        total = n * fixtures
        return round(h / total * 100, 1) if total else 0

    summary = {
        "game_days":     len(game_dates),
        "fixtures":      fixtures,
        "total_goals":   total_goals,
        "total_assists": total_assists,
        "assist": {
            "top5_hits":  hits("sm_assist_rank", "actual_assists", 5),
            "top5_rate":  rate(hits("sm_assist_rank", "actual_assists", 5), 5),
            "top10_hits": hits("sm_assist_rank", "actual_assists", 10),
            "top10_rate": rate(hits("sm_assist_rank", "actual_assists", 10), 10),
        },
        "goal": {
            "top5_hits":  hits("sm_goal_rank", "actual_goals", 5),
            "top5_rate":  rate(hits("sm_goal_rank", "actual_goals", 5), 5),
            "top10_hits": hits("sm_goal_rank", "actual_goals", 10),
            "top10_rate": rate(hits("sm_goal_rank", "actual_goals", 10), 10),
        },
        "tsoa": {
            "top5_hits":  hits("sm_tsoa_rank", "had_contribution", 5),
            "top5_rate":  rate(hits("sm_tsoa_rank", "had_contribution", 5), 5),
            "top10_hits": hits("sm_tsoa_rank", "had_contribution", 10),
            "top10_rate": rate(hits("sm_tsoa_rank", "had_contribution", 10), 10),
        },
        "concession": {
            "flagged":      len(flagged),
            "hits":         flagged_hits,
            "hit_rate_pct": round(flagged_hits / len(flagged) * 100, 1) if flagged else 0,
        },
        "game_dates": game_dates,
    }

    print(f"\n{'='*55}")
    print(f"  SM CUMULATIVE PERFORMANCE — {len(game_dates)} match days")
    print(f"{'='*55}")
    print(f"  Fixtures: {fixtures} | Goals: {total_goals} | Assists: {total_assists}")
    print(f"\n  {'Market':<20} {'Top 5':>10} {'Top 10':>10}")
    print(f"  {'-'*42}")
    print(f"  {'Assist':<20} {summary['assist']['top5_rate']:>9}% {summary['assist']['top10_rate']:>9}%")
    print(f"  {'Goal':<20} {summary['goal']['top5_rate']:>9}% {summary['goal']['top10_rate']:>9}%")
    print(f"  {'TSOA':<20} {summary['tsoa']['top5_rate']:>9}% {summary['tsoa']['top10_rate']:>9}%")
    if flagged:
        print(f"\n  Concession flag hit rate: {summary['concession']['hit_rate_pct']}% ({flagged_hits}/{len(flagged)})")
    print("="*55)

    return summary
