"""
Deep Current Football — Sportmonks Fixtures
server/sm_fixtures.py
"""

import os
import logging
from datetime import datetime, timedelta
from .sm_baseline import _sm_get

log = logging.getLogger(__name__)

SM_LEAGUES = {
    "Premier League":  {"league_id": 8,    "season_id": 25583},
    "Championship":    {"league_id": 9,    "season_id": 25648},
    "La Liga":         {"league_id": 564,  "season_id": 25659},
    "Serie A":         {"league_id": 384,  "season_id": 25533},
    "Bundesliga":      {"league_id": 82,   "season_id": 25646},
    "Ligue 1":         {"league_id": 301,  "season_id": 25651},
    "MLS":             {"league_id": 779,  "season_id": 26720},
    "A-League Men":    {"league_id": 1356, "season_id": 26529},
}

LIVE_STATES      = {2, 3, 4, 6, 7}
FINISHED_STATES  = {5}
SCHEDULED_STATES = {1}
SKIP_STATES      = {9}

# State labels for display
STATE_LABELS = {
    2: "1H", 3: "HT", 4: "2H", 6: "ET", 7: "PEN"
}

def sm_team_logo(team_id):
    if not team_id:
        return None
    try:
        n = int(team_id)
        return f"https://cdn.sportmonks.com/images/soccer/teams/{n%32}/{n}.png"
    except:
        return None


def _extract_score(scores):
    """
    Extract current score from SM scores array.
    Looks for CURRENT description first, falls back to highest type_id.
    Returns (home_goals, away_goals) or (None, None)
    """
    if not scores:
        return None, None

    if isinstance(scores, dict):
        scores = scores.get("data", [])

    # Find CURRENT scores
    current = [s for s in scores if s.get("description") == "CURRENT"]
    if not current:
        # Fall back to 2ND_HALF or 1ST_HALF
        for desc in ["2ND_HALF", "1ST_HALF"]:
            current = [s for s in scores if s.get("description") == desc]
            if current: break

    if not current:
        return None, None

    home_goals = None
    away_goals = None
    for s in current:
        score_data = s.get("score", {})
        participant = score_data.get("participant", "")
        goals = score_data.get("goals", 0)
        if participant == "home":
            home_goals = goals
        elif participant == "away":
            away_goals = goals

    return home_goals, away_goals


def _extract_minute(periods, state_id):
    """Extract current minute from periods array."""
    if not periods:
        return None
    if isinstance(periods, dict):
        periods = periods.get("data", [])

    # Find ticking period first
    ticking = [p for p in periods if p.get("ticking")]
    target = ticking[0] if ticking else (periods[-1] if periods else None)

    if not target:
        return None

    minutes = target.get("minutes")
    time_added = target.get("time_added", 0)

    if state_id == 3:
        return "HT"

    if minutes is not None:
        if time_added and int(time_added) > 0:
            return f"{minutes}+{time_added}'"
        return f"{minutes}'"

    return STATE_LABELS.get(state_id)


def _parse_fixture(fixture, league_name):
    """Parse a Sportmonks fixture into app format."""
    state_id = fixture.get("state_id")
    if state_id in SKIP_STATES:
        return None

    participants = fixture.get("participants", [])
    if isinstance(participants, dict):
        participants = participants.get("data", [])

    home_team = away_team = None
    for p in participants:
        meta = p.get("meta", {})
        location = meta.get("location", "")
        if location == "home":
            home_team = p
        elif location == "away":
            away_team = p

    if not home_team or not away_team:
        return None

    home_id   = str(home_team.get("id", ""))
    away_id   = str(away_team.get("id", ""))
    home_name = home_team.get("name", "")
    away_name = away_team.get("name", "")

    # Kickoff
    starting_at = fixture.get("starting_at", "")
    kickoff_utc = ""
    if starting_at:
        try:
            dt = datetime.strptime(starting_at, "%Y-%m-%d %H:%M:%S")
            kickoff_utc = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except Exception:
            kickoff_utc = starting_at

    is_live     = state_id in LIVE_STATES
    is_finished = state_id in FINISHED_STATES

    # Score
    score = None
    if is_live or is_finished:
        scores = fixture.get("scores", [])
        home_goals, away_goals = _extract_score(scores)
        if home_goals is not None and away_goals is not None:
            score = f"{home_goals} - {away_goals}"

    # Minute
    minute = None
    if is_live:
        periods = fixture.get("periods", [])
        minute = _extract_minute(periods, state_id)

    return {
        "match_id":  str(fixture.get("id", "")),
        "home":      home_name,
        "home_id":   home_id,
        "away":      away_name,
        "away_id":   away_id,
        "home_logo": sm_team_logo(home_id),
        "away_logo": sm_team_logo(away_id),
        "kickoff":   kickoff_utc,
        "live":      is_live,
        "finished":  is_finished,
        "score":     score,
        "minute":    minute,
        "league":    league_name,
    }


def get_sm_fixtures(days=7):
    """Fetch fixtures for the next N days across all leagues."""
    fixtures_by_league = {ln: [] for ln in SM_LEAGUES}

    today = datetime.utcnow()
    dates = [(today + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(-2, days)]

    for league_name, config in SM_LEAGUES.items():
        league_id = config["league_id"]

        for iso_date in dates:
            try:
                resp = _sm_get(
                    endpoint=f"/fixtures/between/{iso_date}/{iso_date}",
                    filters=f"fixtureLeagues:{league_id}",
                    include="participants;scores;periods",
                )
                raw_fixtures = resp.get("data", [])
                if isinstance(raw_fixtures, dict):
                    raw_fixtures = raw_fixtures.get("data", [])

                for raw in raw_fixtures:
                    parsed = _parse_fixture(raw, league_name)
                    if parsed:
                        fixtures_by_league[league_name].append(parsed)

            except Exception as e:
                log.error(f"SM fixtures {league_name} {iso_date}: {e}")

    for ln in fixtures_by_league:
        fixtures_by_league[ln].sort(key=lambda x: x.get("kickoff", "") or "")

    total = sum(len(v) for v in fixtures_by_league.values())
    log.info(f"SM fixtures total: {total}")
    return fixtures_by_league


def get_sm_live_fixtures():
    """Pull only live fixtures."""
    live = []
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for league_name, config in SM_LEAGUES.items():
        try:
            resp = _sm_get(
                endpoint=f"/fixtures/between/{today}/{today}",
                filters=f"fixtureLeagues:{config['league_id']}",
                include="participants;scores;periods",
            )
            raw_fixtures = resp.get("data", [])
            if isinstance(raw_fixtures, dict):
                raw_fixtures = raw_fixtures.get("data", [])

            for raw in raw_fixtures:
                if raw.get("state_id") in LIVE_STATES:
                    parsed = _parse_fixture(raw, league_name)
                    if parsed:
                        live.append(parsed)

        except Exception as e:
            log.error(f"SM live {league_name}: {e}")

    return live
