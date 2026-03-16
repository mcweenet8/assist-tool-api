# =============================================================================
# fixtures.py — Fixture fetching logic
# =============================================================================

import asyncio, logging
from datetime import datetime, timedelta
from .utils import LEAGUES, FIXTURE_ONLY_LEAGUES, team_logo_url

log = logging.getLogger(__name__)


async def get_fixtures_for_dates(fotmob, days=7):
    """Get live + upcoming fixtures for the next N days across all leagues."""
    all_fixture_leagues = {**LEAGUES, **FIXTURE_ONLY_LEAGUES}
    fixtures_by_league  = {ln: [] for ln in all_fixture_leagues}
    league_ids   = set(str(info.get("fixture_id", info["id"])) for info in all_fixture_leagues.values())
    id_to_league = {str(info.get("fixture_id", info["id"])): ln for ln, info in all_fixture_leagues.items()}

    today = datetime.utcnow()
    dates = [(today + timedelta(days=i)).strftime("%Y%m%d") for i in range(-2, days)]
    log.info(f"Fetching fixtures for dates: {dates[:4]}...")

    for date_str in dates:
        await asyncio.sleep(0.4)
        try:
            data    = await fotmob.get_matches_by_date(date_str)
            leagues = data.get("leagues", []) if isinstance(data, dict) else []
            matched = 0
            for league in leagues:
                if not isinstance(league, dict): continue
                lid = str(league.get("id", ""))
                if lid not in league_ids:
                    ccode = league.get("ccode", "").lower()
                    if ccode in ("eng", "gb", "gbr", "uk"):
                        log.info(f"  UNMATCHED ENG: id={lid} name={league.get('name','')} ccode={ccode}")
                    continue
                ln = id_to_league[lid]
                for match in league.get("matches", []):
                    if not isinstance(match, dict): continue
                    status    = match.get("status", {})
                    utc_time  = status.get("utcTime", "")
                    finished  = status.get("finished", False)
                    live      = status.get("ongoing", False)
                    cancelled = status.get("cancelled", False)
                    if cancelled: continue
                    home = match.get("home", {})
                    away = match.get("away", {})
                    h_id = str(home.get("id", ""))
                    a_id = str(away.get("id", ""))
                    fixtures_by_league[ln].append({
                        "match_id":  str(match.get("id", "")),
                        "home":      home.get("name", ""),
                        "home_id":   h_id,
                        "away":      away.get("name", ""),
                        "away_id":   a_id,
                        "home_logo": team_logo_url(h_id),
                        "away_logo": team_logo_url(a_id),
                        "kickoff":   utc_time,
                        "date":      date_str,
                        "live":      live,
                        "finished":  finished,
                        "score":     f"{home.get('score','-')} - {away.get('score','-')}"
                                     if (live or finished) else None,
                        "minute":    status.get("liveTime", {}).get("short", "") if live else None,
                    })
                    matched += 1
        except Exception as e:
            log.error(f"fixtures {date_str}: {e}")
        else:
            log.info(f"fixtures {date_str}: {matched} matched")

    for ln in fixtures_by_league:
        fixtures_by_league[ln].sort(key=lambda x: x.get("kickoff", "") or "")

    return fixtures_by_league
