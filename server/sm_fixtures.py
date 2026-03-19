"""
Deep Current Football — Sportmonks Fixtures
server/sm_fixtures.py

Replaces FotMob fixture scraping with Sportmonks API.
Produces identical output format to fixtures.py so the app
requires zero changes — just swap the data source.

State IDs:
  1 = Scheduled
  2 = In Play (1st Half)
  3 = Half Time
  4 = In Play (2nd Half)
  5 = Full Time
  6 = Extra Time
  7 = Penalties
  9 = Abandoned
"""

import os
import logging
from datetime import datetime, timedelta
from .sm_baseline import _sm_get

log = logging.getLogger(__name__)

# ── LEAGUE CONFIG ─────────────────────────────────────────────────────────────
# Maps league name → {league_id, season_id}
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

# State IDs that mean live
LIVE_STATES     = {2, 3, 4, 6, 7}
# State IDs that mean finished
FINISHED_STATES = {5}
# State IDs that mean scheduled
SCHEDULED_STATES = {1}
# State IDs to skip
SKIP_STATES     = {9}  # abandoned

# Fotmob-style team logo URL using Sportmonks CDN
def sm_team_logo(team_id):
    if not team_id:
        return None
    return f"https://cdn.sportmonks.com/images/soccer/teams/{team_id}.png"


def _parse_fixture(fixture, league_name):
    """
    Parse a Sportmonks fixture dict into the app's expected format.
    Matches the output of fixtures.py get_fixtures_for_dates().
    """
    state_id  = fixture.get("state_id")
    if state_id in SKIP_STATES:
        return None

    # Get participants (home/away teams)
    participants = fixture.get("participants", [])
    if isinstance(participants, dict):
        participants = participants.get("data", [])

    home_team = None
    away_team = None

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

    # Parse kickoff time
    starting_at = fixture.get("starting_at", "")
    kickoff_utc = ""
    date_str    = ""
    if starting_at:
        try:
            # Sportmonks format: "2026-03-22 15:00:00"
            dt = datetime.strptime(starting_at, "%Y-%m-%d %H:%M:%S")
            kickoff_utc = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            date_str    = dt.strftime("%Y%m%d")
        except Exception:
            kickoff_utc = starting_at
            date_str    = starting_at[:10].replace("-", "")

    is_live     = state_id in LIVE_STATES
    is_finished = state_id in FINISHED_STATES

    # Score
    score = None
    if is_live or is_finished:
        home_score = fixture.get("home_score", "-")
        away_score = fixture.get("away_score", "-")
        if home_score is not None and away_score is not None:
            score = f"{home_score} - {away_score}"

    # Live minute
    minute = None
    if is_live:
        periods = fixture.get("periods", [])
        if isinstance(periods, dict):
            periods = periods.get("data", [])
        if periods:
            last = periods[-1]
            m = last.get("minutes", "") or last.get("time_added", "")
            if m:
                minute = str(m)

    return {
        "match_id":  str(fixture.get("id", "")),
        "home":      home_name,
        "home_id":   home_id,
        "away":      away_name,
        "away_id":   away_id,
        "home_logo": sm_team_logo(home_id),
        "away_logo": sm_team_logo(away_id),
        "kickoff":   kickoff_utc,
        "date":      date_str,
        "live":      is_live,
        "finished":  is_finished,
        "score":     score,
        "minute":    minute,
        "league":    league_name,
    }


def get_sm_fixtures(days=7):
    """
    Fetch fixtures from Sportmonks for the next N days across all tracked leagues.
    Returns dict matching FotMob fixtures format: {league_name: [fixture, ...]}

    Drop-in replacement for get_fixtures_for_dates() in fixtures.py.
    """
    fixtures_by_league = {ln: [] for ln in SM_LEAGUES}

    today = datetime.utcnow()
    dates = [(today + timedelta(days=i)) for i in range(-2, days)]
    date_pairs = [(d.strftime("%Y-%m-%d"), d.strftime("%Y%m%d")) for d in dates]

    for league_name, config in SM_LEAGUES.items():
        league_id = config["league_id"]
        log.info(f"Fetching SM fixtures: {league_name} (league {league_id})")

        for iso_date, yyyymmdd in date_pairs:
            try:
                resp = _sm_get(
                    endpoint=f"/fixtures/between/{iso_date}/{iso_date}",
                    filters=f"fixtureLeagues:{league_id}",
                    include="participants",
                )
                raw_fixtures = resp.get("data", [])
                if isinstance(raw_fixtures, dict):
                    raw_fixtures = raw_fixtures.get("data", [])

                matched = 0
                for raw in raw_fixtures:
                    parsed = _parse_fixture(raw, league_name)
                    if parsed:
                        fixtures_by_league[league_name].append(parsed)
                        matched += 1

                if matched:
                    log.info(f"  {iso_date}: {matched} fixtures ({league_name})")

            except Exception as e:
                log.error(f"SM fixtures {league_name} {iso_date}: {e}")

    # Sort each league by kickoff
    for ln in fixtures_by_league:
        fixtures_by_league[ln].sort(key=lambda x: x.get("kickoff", "") or "")

    total = sum(len(v) for v in fixtures_by_league.values())
    log.info(f"SM fixtures total: {total} across {len(SM_LEAGUES)} leagues")

    return fixtures_by_league


def get_sm_live_fixtures():
    """
    Pull only live fixtures across all leagues.
    Used for live match updates.
    """
    live = []
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for league_name, config in SM_LEAGUES.items():
        try:
            resp = _sm_get(
                endpoint=f"/fixtures/between/{today}/{today}",
                filters=f"fixtureLeagues:{config['league_id']}",
                include="participants",
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
            log.error(f"SM live fixtures {league_name}: {e}")

    return live
