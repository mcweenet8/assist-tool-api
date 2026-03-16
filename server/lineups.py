# =============================================================================
# lineups.py — API-Football lineup fetching
# =============================================================================

import asyncio, aiohttp, os, logging
log = logging.getLogger(__name__)

AF_KEY     = os.environ.get("API_FOOTBALL_KEY", "")
AF_BASE    = "https://v3.football.api-sports.io"
AF_HEADERS = {"x-apisports-key": AF_KEY}

AF_LEAGUE_MAP = {
    47:     39,   # Premier League
    48:     40,   # Championship
    87:     140,  # La Liga
    55:     135,  # Serie A
    54:     78,   # Bundesliga
    53:     61,   # Ligue 1
    130:    253,  # MLS
    901954: 188,  # A-League Men
}

_lineup_cache   = {}  # match_key → lineup data
_fixtures_cache = {}  # date_str  → list of AF fixtures


async def af_fixture_id(home_team, away_team, match_date):
    """Find API-Football fixture ID by team names and date."""
    try:
        date_str = match_date[:10]
        if date_str in _fixtures_cache:
            fixtures = _fixtures_cache[date_str]
            log.info(f"AF: using cached {len(fixtures)} fixtures for {date_str}")
        else:
            url    = f"{AF_BASE}/fixtures"
            params = {"date": date_str, "timezone": "UTC"}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=AF_HEADERS, params=params,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        log.warning(f"AF fixtures: HTTP {resp.status}")
                        return None
                    data = await resp.json(content_type=None)
            fixtures = data.get("response", [])
            if fixtures:
                _fixtures_cache[date_str] = fixtures
            log.info(f"AF: fetched {len(fixtures)} fixtures for {date_str} (cached)")

        def clean_name(s):
            return (s.lower()
                    .replace(" fc","").replace(" cf","").replace(" afc","")
                    .replace("stade ","").replace(" sc","")
                    .replace("manchester ","man ").replace("atletico ","atl ")
                    .replace("athletic ","").replace("inter ","")
                    .strip())

        home_c = clean_name(home_team)
        away_c = clean_name(away_team)
        best_id, best_score = None, 0
        for f in fixtures:
            h = clean_name(f.get("teams",{}).get("home",{}).get("name",""))
            a = clean_name(f.get("teams",{}).get("away",{}).get("name",""))
            hs  = len(set(h.split()) & set(home_c.split()))
            as_ = len(set(a.split()) & set(away_c.split()))
            if not hs and (home_c in h or h in home_c): hs = 1
            if not as_ and (away_c in a or a in away_c): as_ = 1
            if hs > 0 and as_ > 0 and hs + as_ > best_score:
                best_score = hs + as_
                best_id = f.get("fixture",{}).get("id")
        if best_id:
            log.info(f"  MATCHED: {home_team} vs {away_team} → {best_id}")
            return best_id
        log.warning(f"AF: no match for '{home_team}' vs '{away_team}' on {match_date[:10]}")
        return None
    except Exception as e:
        log.error(f"af_fixture_id: {e}")
        return None


async def af_lineups(fixture_id):
    """Fetch confirmed lineups from API-Football."""
    try:
        url = f"{AF_BASE}/fixtures/lineups"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=AF_HEADERS,
                                   params={"fixture": fixture_id},
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    log.warning(f"AF lineups: HTTP {resp.status}")
                    return None
                data = await resp.json(content_type=None)
        teams = data.get("response", [])
        if not teams:
            return None
        result = {}
        for team_data in teams:
            team_name = team_data.get("team", {}).get("name", "")
            formation = team_data.get("formation", "")
            starters  = [{"name":   p.get("player",{}).get("name",""),
                           "number": p.get("player",{}).get("number",""),
                           "pos":    p.get("player",{}).get("pos",""),
                           "grid":   p.get("player",{}).get("grid","")}
                          for p in team_data.get("startXI", [])]
            bench     = [{"name":   p.get("player",{}).get("name",""),
                           "number": p.get("player",{}).get("number",""),
                           "pos":    p.get("player",{}).get("pos","")}
                          for p in team_data.get("substitutes", [])]
            result[team_name] = {"formation": formation, "starters": starters, "bench": bench}
        return result if result else None
    except Exception as e:
        log.error(f"af_lineups {fixture_id}: {e}")
        return None



