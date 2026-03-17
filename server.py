# =============================================================================
# ASSIST RESEARCH TOOL — RAILWAY SERVER v2 (touch)
# =============================================================================
# Endpoints:
#   GET  /status     — health check
#   GET  /data       — cached player + team data (includes form + opponent flag)
#   POST /refresh    — re-run full scraper
#   GET  /fixtures   — live + upcoming games next 7 days, by league
# =============================================================================

import asyncio, aiohttp, os, logging
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

MIN_GOALS_PG    = 1.2
MAX_PLAYERS     = 25
WEAK_DEF_THRESH = 1.5   # GA/G above this = weak defence flag
FORM_MATCHES    = 5     # last N matches for form

LEAGUES = {
    "Premier League": {"id": 47,     "short": "PL"},
    "Championship":   {"id": 48,     "short": "Champ",    "fixture_id": 900638},
    "La Liga":        {"id": 87,     "short": "LaLiga"},
    "Serie A":        {"id": 55,     "short": "SerieA"},
    "Bundesliga":     {"id": 54,     "short": "Bundesliga"},
    "Ligue 1":        {"id": 53,     "short": "Ligue1"},
    "MLS":            {"id": 130,    "short": "MLS",     "fixture_id": 913550},
    "A-League Men":   {"id": 901954, "short": "ALeague", "fixture_id": 901954},
}

# Fixtures only — no player stats scraping
FIXTURE_ONLY_LEAGUES = {
    "Champions League":  {"id": 42,    "short": "UCL"},
    "Europa League":     {"id": 73,    "short": "UEL"},
    "Conference League": {"id": 10216, "short": "UECL"},
}

STATS = {
    "goal_assist":             "assists",
    "expected_assists":        "xa",
    "big_chance_created":      "big_chances",
    "total_att_assist":        "chances_created",
    "expected_assists_per_90": "xa_per90",
    "penalty_won":             "penalties_won",
}

GS_STATS = {
    "goals":                   "goals",
    "expected_goals":          "xg",
    "expected_goals_per_90":   "xg_per90",
    "expected_goalsontarget":  "xgot",
    "ontarget_scoring_att":    "sot_per90",
    "total_scoring_att":       "shots_per90",
    "big_chance_missed":       "big_chances_missed",
}

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)"
                   " AppleWebKit/605.1.15"),
    "Accept":  "application/json",
    "Referer": "https://www.fotmob.com/",
}

# ── Image URLs (no API call needed) ──────────────────────────────────────────

def player_img_url(player_id):
    if not player_id: return None
    return f"https://images.fotmob.com/image_resources/playerimages/{player_id}.png"

def team_logo_url(team_id):
    if not team_id: return None
    return f"https://images.fotmob.com/image_resources/logo/teamlogo/{team_id}.png"

def league_logo_url(league_id):
    if not league_id: return None
    return f"https://images.fotmob.com/image_resources/logo/leaguelogo/{league_id}.png"

# ── Cache ─────────────────────────────────────────────────────────────────────

_cache = {
    "last_updated":     None,
    "top25":            [],
    "by_league":        {},
    "fixtures":         {},
    "fixtures_updated": None,
    "all_players":      [],
    "gs_top25":         [],
    "gs_all":           [],
    "gs_by_league":     {},
    "tsoa_top25":       [],
    "tsoa_all":         [],
    "tsoa_by_league":   {},
    "teams":            [],
    "status":           "never_run",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_float(v):
    try:
        return float(str(v).replace(",","").strip()) if v not in [None,""," ","-","N/A"] else 0.0
    except:
        return 0.0

def tsoa_score(xg_per90, xa_per90, xgot_gap, xa_gap, bc_combined):
    """TSOA score — rewards dual threats (scorers AND creators)."""
    raw = (
        (xg_per90   * 0.25) +
        (xa_per90   * 0.25) +
        (xgot_gap   * 0.20) +
        (xa_gap     * 0.20) +
        (bc_combined * 0.10)
    )
    # Dual threat multiplier — penalizes one-dimensional players
    if xg_per90 > 0 and xa_per90 > 0:
        dual = min(xg_per90, xa_per90) / max(xg_per90, xa_per90)
    elif xg_per90 > 0 or xa_per90 > 0:
        dual = 0.6  # one-dimensional player
    else:
        return 0.0
    return round(raw * (0.7 + dual * 0.3) * 2.0, 2)


def gs_score(xgot_gap, sot_per90, xg_per90, big_chances_missed):
    """Goal scorer score formula."""
    return round(
        (xgot_gap          * 0.35) +
        (sot_per90         * 0.25) +
        (xg_per90          * 0.20) +
        (big_chances_missed * 0.20), 2)


def combined_score(xa_gap, cc_pg, big_chances, penalties_won, opp_ga_pg=0.0, l5_xa=0.0):
    return round(
        (xa_gap        * 0.40) +
        (cc_pg         * 0.30) +
        (big_chances   * 0.20) +
        (penalties_won * 0.10), 2)

def form_score(last5):
    """Convert last 5 results to a 0-1 form score. W=1, D=0.5, L=0"""
    if not last5:
        return None
    pts = sum({"w": 1.0, "d": 0.5, "l": 0.0}.get(r.lower(), 0) for r in last5)
    return round(pts / len(last5), 2)

# ── FotMob fetchers ───────────────────────────────────────────────────────────

async def get_standings(fotmob, league_name, league_id):
    await asyncio.sleep(1.5)
    rows = []
    log.info(f"standings {league_name} (id={league_id}): fetching...")
    try:
        data = await fotmob.standings(league_id)
        log.info(f"standings {league_name}: type={type(data).__name__} len={len(data) if isinstance(data,list) else str(data)[:50]}")
        if not isinstance(data, list) or not data:
            log.warning(f"standings {league_name}: empty response")
            return []
        # Collect all team rows across all table items (handles MLS conferences)
        all_team_rows_in_league = []
        home_lkp_combined = {}
        away_lkp_combined = {}
        for item in data:
            if not isinstance(item, dict): continue
            item_data = item.get("data", {})
            table = item_data.get("table", {})

            # For MLS, skip the top-level table and use tables_list for conference names
            has_sub_tables = bool(item_data.get("tables", []))
            all_rows = [] if has_sub_tables else table.get("all", [])

            # MLS format: data.tables is a list of conference tables
            if not all_rows:
                tables_list = item_data.get("tables", [])
                log.info(f"  {league_name} tables_list len: {len(tables_list)}")
                for t_idx, t_item in enumerate(tables_list):
                    if isinstance(t_item, dict):
                        log.info(f"  {league_name} table[{t_idx}] keys: {list(t_item.keys())[:8]}")
                        sub_table  = t_item.get("table", {})
                        conf_name  = t_item.get("leagueName", t_item.get("name", t_item.get("title", "")))
                        log.info(f"  {league_name} table[{t_idx}] conf_name='{conf_name}' all_len={len(sub_table.get('all',[]))}")
                        conf_teams = sub_table.get("all", [])
                        # Tag each team with their conference
                        for t in conf_teams:
                            if isinstance(t, dict):
                                t["_conference"] = conf_name
                        all_rows.extend(conf_teams)
                        home_lkp_combined.update({str(t.get("id","")): t for t in sub_table.get("home", []) if isinstance(t, dict)})
                        away_lkp_combined.update({str(t.get("id","")): t for t in sub_table.get("away", []) if isinstance(t, dict)})

            # Log MLS structure for debugging
            if not all_rows and league_name == "MLS":
                log.warning(f"MLS item_data keys: {list(item_data.keys())[:10]}")
                tables_list = item_data.get("tables", [])
                log.warning(f"MLS tables_list len: {len(tables_list)}, first keys: {list(tables_list[0].keys())[:5] if tables_list else 'empty'}")

            home_lkp_combined.update({str(t.get("id","")): t for t in table.get("home", []) if isinstance(t, dict)})
            away_lkp_combined.update({str(t.get("id","")): t for t in table.get("away", []) if isinstance(t, dict)})
            all_team_rows_in_league.extend([t for t in all_rows if isinstance(t, dict)])
        home_lkp = home_lkp_combined
        away_lkp = away_lkp_combined
        # Deduplicate by team ID (MLS conference format can produce duplicates)
        seen_ids = set()
        deduped = []
        for t in all_team_rows_in_league:
            tid = str(t.get("id",""))
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                deduped.append(t)
        all_team_rows_in_league = deduped
        log.info(f"standings {league_name}: {len(all_team_rows_in_league)} raw rows to process")
        for team in all_team_rows_in_league:
                if not isinstance(team, dict): continue
                name   = (team.get("name") or team.get("shortName","")).strip()
                tid    = str(team.get("id",""))
                played = safe_float(team.get("played", 0))
                try:    gf, ga = [safe_float(x) for x in team.get("scoresStr","0-0").split("-")]
                except: gf, ga = 0.0, 0.0
                ht   = home_lkp.get(tid, {})
                at   = away_lkp.get(tid, {})
                h_pl = safe_float(ht.get("played", played/2))
                a_pl = safe_float(at.get("played", played/2))
                try:    gf_h, ga_h = [safe_float(x) for x in ht.get("scoresStr","0-0").split("-")]
                except: gf_h, ga_h = 0.0, 0.0
                try:    gf_a, ga_a = [safe_float(x) for x in at.get("scoresStr","0-0").split("-")]
                except: gf_a, ga_a = 0.0, 0.0
                if name and played > 0:
                    ga_pg   = round(ga / played, 2)
                    ga_h_pg = round(ga_h / h_pl, 2) if h_pl > 0 else 0.0
                    ga_a_pg = round(ga_a / a_pl, 2) if a_pl > 0 else 0.0
                    gf_pg   = round(safe_float(gf) / played, 2)
                    gf_h_pg = round(gf_h / h_pl, 2) if h_pl > 0 else 0.0
                    gf_a_pg = round(gf_a / a_pl, 2) if a_pl > 0 else 0.0
                    rows.append({
                        "team":      name,
                        "team_id":   tid,
                        "league":    league_name,
                        "conference": team.get("_conference", ""),
                        "table_pos": safe_float(team.get("idx", 99)),
                        "played":    int(played),
                        "gf":        int(safe_float(gf)),
                        "ga":        int(safe_float(ga)),
                        "gf_pg":     gf_pg,
                        "ga_pg":     ga_pg,
                        "gf_h_pg":   gf_h_pg,
                        "ga_h_pg":   ga_h_pg,
                        "gf_a_pg":   gf_a_pg,
                        "ga_a_pg":   ga_a_pg,
                        "home_adv":  round(gf_h_pg - gf_a_pg, 2),
                        "away_vuln": round(ga_a_pg - ga_h_pg, 2),
                        "goals_scored_pg": gf_pg,
                        "weak_def":  ga_pg >= WEAK_DEF_THRESH,
                    })
        if len(rows) == 0:
            log.warning(f"standings {league_name}: 0 teams — items={len(data) if isinstance(data,list) else 0}, first_item_keys={list(data[0].keys())[:8] if isinstance(data,list) and data else 'none'}")
            if isinstance(data, list) and data:
                first = data[0]
                table = first.get("data",{}).get("table",{})
                log.warning(f"  table keys: {list(table.keys())[:10]}, all_rows_len={len(table.get('all',[]))}")
        else:
            log.info(f"standings {league_name}: {len(rows)} teams")
    except Exception as e:
        log.error(f"standings {league_name} EXCEPTION: {type(e).__name__}: {e}")
    return rows


async def get_season_id(fotmob, team_id):
    try:
        data = await fotmob.get_team(int(team_id))
        sid  = str(data.get("stats", {}).get("primarySeasonId", ""))
        if sid and sid.isdigit():
            return sid
        for entry in data.get("stats", {}).get("seasonStatLinks", []):
            s = str(entry.get("seasonId", ""))
            if s and s.isdigit():
                return s
        return ""
    except Exception as e:
        log.error(f"season_id {team_id}: {e}")
        return ""


async def fetch_stat(sess, league_id, season_id, stat_name, all_team_ids):
    url = (f"https://data.fotmob.com/stats/{league_id}"
           f"/season/{season_id}/{stat_name}.json")
    rows = []
    try:
        async with sess.get(url, headers=HEADERS,
                            timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return rows
            data      = await resp.json(content_type=None)
            stat_list = data.get("TopLists", [{}])[0].get("StatList", [])
            friendly  = STATS.get(stat_name, stat_name)
            for p in stat_list:
                tid = str(int(p.get("TeamId", 0)))
                if tid not in all_team_ids: continue
                rows.append({
                    "player":     p.get("ParticipantName", "").strip(),
                    "player_id":  str(p.get("ParticiantId", "")),
                    "team":       p.get("TeamName", "").strip(),
                    "team_id":    tid,
                    "stat_type":  friendly,
                    "stat_value": safe_float(p.get("StatValue", 0)),
                })
    except Exception as e:
        log.error(f"fetch_stat {stat_name}: {e}")
    return rows


async def get_team_form(fotmob, team_id, team_name):
    """Get last 5 results for a team."""
    try:
        fixtures = await fotmob.get_team_last_fixtures(int(team_id))
        results  = []
        matches  = fixtures if isinstance(fixtures, list) else []
        for match in matches[-FORM_MATCHES:]:
            if not isinstance(match, dict): continue
            home_id    = str(match.get("home", {}).get("id", ""))
            away_id    = str(match.get("away", {}).get("id", ""))
            home_score = safe_float(match.get("home", {}).get("score", -1))
            away_score = safe_float(match.get("away", {}).get("score", -1))
            if home_score < 0 or away_score < 0: continue
            is_home = (home_id == str(team_id))
            if is_home:
                results.append("w" if home_score > away_score
                                else "d" if home_score == away_score else "l")
            else:
                results.append("w" if away_score > home_score
                                else "d" if home_score == away_score else "l")
        return results[-FORM_MATCHES:]
    except Exception as e:
        log.error(f"form {team_name}: {e}")
        return []


async def get_next_opponent(fotmob, team_id, team_name, teams_df):
    """Get next fixture opponent and their defensive weakness."""
    try:
        fixture = await fotmob.get_team_next_fixture(int(team_id))
        if not fixture or not isinstance(fixture, dict):
            return None, False
        home    = fixture.get("home", {})
        away    = fixture.get("away", {})
        home_id = str(home.get("id", ""))
        away_id = str(away.get("id", ""))
        opp_id  = away_id if home_id == str(team_id) else home_id
        opp_name= away.get("name","") if home_id == str(team_id) else home.get("name","")
        # Check opponent GA/G
        opp_row = teams_df[teams_df["team_id"] == opp_id]
        if not opp_row.empty:
            opp_ga_pg   = float(opp_row.iloc[0]["ga_pg"])
            is_weak_def = opp_ga_pg >= WEAK_DEF_THRESH
        else:
            is_weak_def = False
        # Kickoff time
        kickoff = fixture.get("status", {}).get("utcTime", "")
        return {
            "opponent":    opp_name,
            "opponent_id": opp_id,
            "kickoff":     kickoff,
            "weak_def":    is_weak_def,
        }, is_weak_def
    except Exception as e:
        log.error(f"next_opponent {team_name}: {e}")
        return None, False


async def get_fixtures_for_dates(fotmob, days=7):
    """Get live + upcoming fixtures for the next N days across all leagues."""
    # Combine regular leagues + fixture-only leagues
    all_fixture_leagues = {**LEAGUES, **FIXTURE_ONLY_LEAGUES}
    fixtures_by_league = {ln: [] for ln in all_fixture_leagues}
    # Use fixture_id if defined (some leagues use different IDs for fixtures vs standings)
    league_ids   = set(str(info.get("fixture_id", info["id"])) for info in all_fixture_leagues.values())
    id_to_league = {str(info.get("fixture_id", info["id"])): ln for ln, info in all_fixture_leagues.items()}

    today = datetime.utcnow()
    # Include 2 days back to catch timezone differences + recent results
    dates = [(today + timedelta(days=i)).strftime("%Y%m%d") for i in range(-2, days)]
    log.info(f"Fetching fixtures for dates: {dates[:4]}...")

    for date_str in dates:
        await asyncio.sleep(0.4)
        try:
            data = await fotmob.get_matches_by_date(date_str)
            leagues = data.get("leagues", []) if isinstance(data, dict) else []
            matched = 0
            for league in leagues:
                if not isinstance(league, dict): continue
                lid = str(league.get("id", ""))
                if lid not in league_ids:
                    name_lower = league.get("name","").lower()
                    if any(x in name_lower for x in ["mls","a-league","aleague","australia","major league","championship","portsmouth","derby","portsmouth"]):
                        log.info(f"  UNMATCHED: id={lid} name={league.get('name','')}")
                    # Log ALL unmatched English leagues for debugging
                    ccode = league.get("ccode","").lower()
                    if ccode in ("eng","gb","gbr","uk"):
                        log.info(f"  UNMATCHED ENG: id={lid} name={league.get('name','')} ccode={ccode}")
                    continue
                ln  = id_to_league[lid]
                for match in league.get("matches", []):
                    if not isinstance(match, dict): continue
                    status     = match.get("status", {})
                    utc_time   = status.get("utcTime", "")
                    finished   = status.get("finished", False)
                    live       = status.get("ongoing", False)
                    cancelled  = status.get("cancelled", False)
                    if cancelled: continue
                    home = match.get("home", {})
                    away = match.get("away", {})
                    h_id = str(home.get("id", ""))
                    a_id = str(away.get("id", ""))
                    fixtures_by_league[ln].append({
                        "match_id":   str(match.get("id", "")),
                        "home":       home.get("name", ""),
                        "home_id":    h_id,
                        "away":       away.get("name", ""),
                        "away_id":    a_id,
                        "home_logo":  team_logo_url(h_id),
                        "away_logo":  team_logo_url(a_id),
                        "kickoff":    utc_time,
                        "date":       date_str,
                        "live":       live,
                        "finished":   finished,
                        "score":      f"{home.get('score','-')} - {away.get('score','-')}" if (live or finished) else None,
                        "minute":     status.get("liveTime", {}).get("short", "") if live else None,
                    })
                    matched += 1
        except Exception as e:
            log.error(f"fixtures {date_str}: {e}")
        else:
            log.info(f"fixtures {date_str}: {matched} matched")

    # Sort each league by kickoff
    for ln in fixtures_by_league:
        fixtures_by_league[ln].sort(key=lambda x: x.get("kickoff","") or "")

    return fixtures_by_league


# ── Player L5 goals + assists fetcher ────────────────────────────────────────

async def get_player_l5(fotmob, player_id, team_id, player_name):
    """
    Fetch last 5 matches for a player's team.
    Extract goals + assists from match events (public endpoint, no auth needed).
    Returns list of {opponent, result, score, goals, assists, date} dicts.
    """
    games = []
    try:
        fixtures = await fotmob.get_team_last_fixtures(int(team_id))
        matches  = fixtures if isinstance(fixtures, list) else []
        recent   = [m for m in matches if isinstance(m, dict)][-5:]

        for match in recent:
            match_id = match.get("id") or match.get("matchId")
            home     = match.get("home", {})
            away     = match.get("away", {})
            home_id  = str(home.get("id", ""))
            is_home  = home_id == str(team_id)
            opponent = away.get("name","") if is_home else home.get("name","")
            h_score  = safe_float(home.get("score", -1))
            a_score  = safe_float(away.get("score", -1))

            if is_home:
                result = "W" if h_score > a_score else "D" if h_score == a_score else "L"
                score  = f"{int(h_score)}-{int(a_score)}"
            else:
                result = "W" if a_score > h_score else "D" if h_score == a_score else "L"
                score  = f"{int(a_score)}-{int(h_score)}"

            goals   = 0
            assists = 0

            if match_id:
                try:
                    # get_match() uses public endpoint — no auth token needed
                    match_data = await fotmob.get_match(int(match_id))
                    if isinstance(match_data, dict):
                        # Events are in content.matchFacts.events or header.events
                        events = []
                        content_data = match_data.get("content", {})
                        match_facts  = content_data.get("matchFacts", {})

                        # Try matchFacts.events first
                        raw_events = match_facts.get("events", {})
                        if isinstance(raw_events, dict):
                            events = raw_events.get("events", [])
                        elif isinstance(raw_events, list):
                            events = raw_events

                        # Fallback: header events
                        if not events:
                            events = match_data.get("header", {}).get("events", [])

                        log.info(f"  match {match_id}: {len(events)} events")

                        for event in events:
                            if not isinstance(event, dict): continue
                            etype = str(event.get("type","") or event.get("eventType","")).lower()

                            # Goal event
                            if any(g in etype for g in ["goal", "addedgoal", "penaltygoal"]):
                                # Check if this player scored
                                scorer_id = str(event.get("playerId","") or
                                               event.get("player",{}).get("id",""))
                                if scorer_id == str(player_id):
                                    goals += 1
                                # Check if this player assisted
                                assist_id = str(event.get("assistId","") or
                                               event.get("assist",{}).get("id","") or
                                               event.get("assistPlayerId",""))
                                if assist_id and assist_id == str(player_id):
                                    assists += 1

                        log.info(f"  player {player_id}: {goals}G {assists}A in match {match_id}")

                except Exception as e:
                    log.warning(f"  match {match_id} events error: {e}")

            utc = match.get("status", {}).get("utcTime", "") or match.get("utcTime", "")
            games.append({
                "opponent": opponent,
                "result":   result,
                "score":    score,
                "goals":    goals,
                "assists":  assists,
                "date":     utc[:10] if utc else "",
            })
    except Exception as e:
        log.error(f"player_l5 {player_name}: {e}")

    return games


# ── Main scraper ──────────────────────────────────────────────────────────────

async def run_scraper():
    import pandas as pd
    from fotmob import FotMob

    log.info("Scraper v3 starting (fully parallel)...")

    async with FotMob() as fotmob:

        # Pass 1 — Standings: European first (parallel), then others sequentially
        log.info("Pass 1: standings...")
        EURO_LEAGUES = {k: v for k, v in LEAGUES.items()
                        if k not in ("MLS", "A-League Men")}
        OTHER_LEAGUES = {k: v for k, v in LEAGUES.items()
                         if k in ("MLS", "A-League Men")}

        # European leagues in parallel
        euro_tasks = [get_standings(fotmob, ln, info["id"])
                      for ln, info in EURO_LEAGUES.items()]
        euro_results = await asyncio.gather(*euro_tasks, return_exceptions=True)
        all_team_rows = []
        for r in euro_results:
            if isinstance(r, list):
                all_team_rows.extend(r)

        # MLS + A-League sequentially with extra delay
        for ln, info in OTHER_LEAGUES.items():
            await asyncio.sleep(3)
            rows = await get_standings(fotmob, ln, info["id"])
            all_team_rows.extend(rows)

        if not all_team_rows:
            raise RuntimeError("No standings data")

        teams_df     = pd.DataFrame(all_team_rows)
        all_team_ids = set(str(tid) for tid in teams_df["team_id"].tolist())
        log.info(f"Standings: {len(teams_df)} teams")

        # Pass 2 — All season IDs in parallel
        log.info("Pass 2: season IDs (parallel)...")
        async def fetch_season(league_name, info):
            lt = teams_df[teams_df["league"] == league_name]
            if lt.empty: return info["id"], None
            sid = await get_season_id(fotmob, lt.iloc[0]["team_id"])
            return info["id"], sid

        season_tasks    = [fetch_season(ln, info) for ln, info in LEAGUES.items()]
        season_results  = await asyncio.gather(*season_tasks, return_exceptions=True)
        league_seasons  = {}
        for r in season_results:
            if isinstance(r, tuple):
                lid, sid = r
                if sid:
                    league_seasons[lid] = sid
                    log.info(f"  Season {lid}: {sid}")

        # Pass 3 — All player stats in parallel
        log.info("Pass 3: player stats (parallel)...")
        fotmob_session = getattr(fotmob, "_session", None) or getattr(fotmob, "session", None)
        sess = fotmob_session or aiohttp.ClientSession()

        async def fetch_league_stats(league_name, info):
            lid  = info["id"]
            sid  = league_seasons.get(lid)
            if not sid:
                log.warning(f"Pass 3: {league_name} skipped — no season ID")
                return []
            log.info(f"Pass 3: fetching {league_name} (lid={lid} sid={sid})")
            tasks = [
                fetch_stat(sess, lid, sid, stat_name, all_team_ids)
                for stat_name in STATS
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            rows = []
            for r in results:
                if isinstance(r, list):
                    rows.extend(r)
            if not rows:
                log.warning(f"  {league_name}: 0 player rows — may not have stats on data.fotmob.com")
            else:
                log.info(f"  {league_name}: {len(rows)} player rows")
            return rows

        # Fetch European + MLS stats in parallel, A-League after with delay
        PRIORITY_LEAGUES = {k: v for k, v in LEAGUES.items() if k != "A-League Men"}
        DELAYED_LEAGUES  = {k: v for k, v in LEAGUES.items() if k == "A-League Men"}

        league_stat_tasks = [
            fetch_league_stats(ln, info) for ln, info in PRIORITY_LEAGUES.items()
        ]
        league_stat_results = await asyncio.gather(*league_stat_tasks, return_exceptions=True)
        all_player_rows = []
        for r in league_stat_results:
            if isinstance(r, list):
                all_player_rows.extend(r)

        # A-League after a brief pause
        for ln, info in DELAYED_LEAGUES.items():
            await asyncio.sleep(2)
            rows = await fetch_league_stats(ln, info)
            all_player_rows.extend(rows)

        # Pass 3b — Goal scorer stats (same leagues, separate stat set)
        log.info("Pass 3b: goal scorer stats...")
        async def fetch_gs_stats(league_name, info):
            lid = info["id"]
            sid = league_seasons.get(lid)
            if not sid:
                return []
            tasks = [
                fetch_stat(sess, lid, sid, stat_name, all_team_ids)
                for stat_name in GS_STATS
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            rows = []
            for r in results:
                if isinstance(r, list):
                    # Remap stat_type from raw key to friendly GS name
                    for row in r:
                        raw = row.get("stat_type","")
                        row["stat_type"] = GS_STATS.get(raw, raw)
                    rows.extend(r)
            log.info(f"  GS {league_name}: {len(rows)} rows")
            return rows

        gs_tasks = [fetch_gs_stats(ln, info) for ln, info in PRIORITY_LEAGUES.items()]
        gs_results = await asyncio.gather(*gs_tasks, return_exceptions=True)
        all_gs_rows = []
        for r in gs_results:
            if isinstance(r, list):
                all_gs_rows.extend(r)
        # A-League GS with delay
        for ln, info in DELAYED_LEAGUES.items():
            await asyncio.sleep(2)
            rows = await fetch_gs_stats(ln, info)
            all_gs_rows.extend(rows)

        if fotmob_session is None and sess:
            await sess.close()

        # Pass 4 — Form + next opponent + fixtures all in parallel
        log.info("Pass 4: form + opponents + fixtures (parallel)...")

        async def fetch_team_context(team_row):
            tid   = team_row["team_id"]
            tname = team_row["team"]
            try:
                form = await get_team_form(fotmob, tid, tname)
            except Exception as e:
                log.error(f"form {tname}: {e}")
                form = []
            try:
                opp, _ = await get_next_opponent(fotmob, tid, tname, teams_df)
            except Exception as e:
                log.error(f"next_opp {tname}: {e}")
                opp = None
            return tname, form, opp

        context_tasks = [fetch_team_context(row) for _, row in teams_df.iterrows()]
        # Run form/opponent + fixtures simultaneously
        context_results, fixtures = await asyncio.gather(
            asyncio.gather(*context_tasks, return_exceptions=True),
            get_fixtures_for_dates(fotmob, days=7),
            return_exceptions=True
        )

        team_form     = {}
        team_next_opp = {}
        if isinstance(context_results, (list, tuple)):
            for result in context_results:
                if isinstance(result, Exception):
                    continue
                tname, form, opp = result
                team_form[tname]     = form
                team_next_opp[tname] = opp

        if isinstance(fixtures, Exception):
            log.error(f"fixtures failed: {fixtures}")
            fixtures = {}

        log.info(f"Form: {len(team_form)} teams | Fixtures: {sum(len(v) for v in fixtures.values())} matches")

    if not all_player_rows:
        raise RuntimeError("No player data")

    # Aggregate players
    import pandas as pd
    raw  = pd.DataFrame(all_player_rows)
    tgp  = {str(r["team_id"]): r["played"]    for _, r in teams_df.iterrows()}
    tlg  = {str(r["team_id"]): r["league"]    for _, r in teams_df.iterrows()}
    tpos = {str(r["team_id"]): r["table_pos"] for _, r in teams_df.iterrows()}

    agg = {}
    for _, row in raw.iterrows():
        key = (row["player_id"], row["team_id"])
        if key not in agg:
            agg[key] = {
                "player":          row["player"],
                "player_id":       row["player_id"],
                "team":            row["team"],
                "team_id":         row["team_id"],
                "league":          tlg.get(row["team_id"], ""),
                "table_pos":       tpos.get(row["team_id"], 99),
                "games":           tgp.get(row["team_id"], 30),
                "assists":         0.0, "xa":              0.0,
                "xa_per90":        0.0, "big_chances":     0.0,
                "chances_created": 0.0, "penalties_won":   0.0,
                "l5_xa":           0.0,
            }
        st = row["stat_type"]
        if st in agg[key]:
            agg[key][st] = safe_float(row["stat_value"])

    df = pd.DataFrame(list(agg.values()))
    # Use xa/xa_per90 to derive player minutes/90 denominator
    # This matches FotMob's "chances created per 90" exactly
    df["chances_per_game"] = df.apply(
        lambda r: round(r["chances_created"] / (r["xa"] / r["xa_per90"]), 2)
        if r["xa"] > 0 and r["xa_per90"] > 0 and r["chances_created"] > 0
        else (round(r["xa_per90"] * 2.5, 2) if r["xa_per90"] > 0 else 0.0), axis=1)
    df["xa_gap"] = (df["xa"] - df["assists"]).round(1)

    df["score"]  = df.apply(
        lambda r: combined_score(
            r["xa_gap"], r["chances_per_game"],
            r["big_chances"], r["penalties_won"],
            r.get("opp_ga_pg", 0.0))
        if (r["xa"] > 0 or r["chances_per_game"] > 0) else 0.0, axis=1)

    df = df[df["score"] > 0].sort_values("score", ascending=False).reset_index(drop=True)

    def player_dict(p):
        tname    = p["team"]
        form     = team_form.get(tname, [])
        next_opp = team_next_opp.get(tname)
        return {
            "player":           p["player"],
            "team":             p["team"],
            "team_id":          str(p["team_id"]),
            "league":           p["league"],
            "score":            round(float(p["score"]), 2),
            "assists":          int(p["assists"]),
            "xa":               round(float(p["xa"]), 2),
            "xa_gap":           round(float(p["xa_gap"]), 2),
            "chances_per_game": round(float(p["chances_per_game"]), 2),
            "big_chances":      int(p["big_chances"]),
            "penalties_won":    int(p["penalties_won"]),
            "opp_ga_pg":        round(float(p.get("opp_ga_pg", 0.0)), 2),
            # Form
            "form":             form,
            "form_score":       form_score(form),
            # Next opponent
            "next_opponent":    next_opp.get("opponent")    if next_opp else None,
            "next_kickoff":     next_opp.get("kickoff")     if next_opp else None,
            "weak_opp_def":     next_opp.get("weak_def")    if next_opp else False,
            # Images
            "player_id":        str(p.get("player_id", "")),
            "player_img":       player_img_url(str(p.get("player_id", ""))),
            "team_logo":        team_logo_url(str(p.get("team_id", ""))),
        }

    # Top 25
    qt    = teams_df[teams_df["gf_pg"] >= MIN_GOALS_PG]["team"].tolist()
    top25 = df[df["team"].isin(qt)].head(MAX_PLAYERS)
    top25_list = [player_dict(p) for _, p in top25.iterrows()]

    # By league → team → players
    by_league = {}
    for league_name in LEAGUES:
        lg_df = df[df["league"] == league_name]
        if lg_df.empty: continue
        teams_in_league = {}
        for _, p in lg_df.iterrows():
            t = p["team"]
            if t not in teams_in_league:
                teams_in_league[t] = []
            teams_in_league[t].append(player_dict(p))
        sorted_teams = sorted(
            teams_in_league.items(),
            key=lambda x: safe_float(
                teams_df[teams_df["team"] == x[0]]["table_pos"].values[0]
                if not teams_df[teams_df["team"] == x[0]].empty else 99))
        by_league[league_name] = [
            {"team": team, "players": players}
            for team, players in sorted_teams
        ]

    # Full player list for match screen squad lookups
    all_players_list = [player_dict(p) for _, p in df.iterrows()]

    # Team list for weak def lookups
    teams_list = [
        {
            "team":      r["team"],
            "team_id":   str(r["team_id"]),
            "league":    r["league"],
            "table_pos": safe_float(r.get("table_pos", 99)),
            "played":    int(r.get("played", 0)),
            "gf_pg":     round(float(r.get("gf_pg", 0)), 2),
            "ga_pg":     round(float(r.get("ga_pg", 0)), 2),
            "gf_h_pg":   round(float(r.get("gf_h_pg", 0)), 2),
            "ga_h_pg":   round(float(r.get("ga_h_pg", 0)), 2),
            "gf_a_pg":   round(float(r.get("gf_a_pg", 0)), 2),
            "ga_a_pg":   round(float(r.get("ga_a_pg", 0)), 2),
            "home_adv":  round(float(r.get("home_adv", 0)), 2),
            "away_vuln": round(float(r.get("away_vuln", 0)), 2),
            "weak_def":   bool(r.get("weak_def", False)),
            "conference": r.get("conference", ""),
            "team_logo":  team_logo_url(str(r.get("team_id",""))),
        }
        for _, r in teams_df.iterrows()
    ]

    # ── Goal Scorer aggregation ─────────────────────────────────────────────
    team_name_map = {r["team"]: r.get("ga_pg", 0.0) for _, r in teams_df.iterrows()}
    gs_agg = {}
    for row in all_gs_rows:
        pid   = str(row.get("player_id",""))
        tid   = str(row.get("team_id",""))
        key   = (pid, tid)
        stype = row.get("stat_type","")
        val   = safe_float(row.get("stat_value", 0))
        if key not in gs_agg:
            gs_agg[key] = {
                "player": row["player"], "player_id": pid,
                "team": row["team"],     "team_id": tid,
                "league": tlg.get(tid, ""),
                "goals": 0.0, "xg": 0.0, "xg_per90": 0.0,
                "xgot": 0.0,  "sot_per90": 0.0, "shots_per90": 0.0,
                "big_chances_missed": 0.0,
            }
        if stype in gs_agg[key]:
            gs_agg[key][stype] = val

    gs_top25_list    = []
    gs_all_list      = []
    gs_by_league     = {}

    if gs_agg:
        gs_df = pd.DataFrame(list(gs_agg.values()))
        gs_df["xgot_gap"] = (gs_df["xgot"] - gs_df["goals"]).round(2)
        gs_df["gs_score"] = gs_df.apply(
            lambda r: gs_score(r["xgot_gap"], r["sot_per90"],
                               r["xg_per90"], r["big_chances_missed"])
            if r["xg_per90"] > 0 else 0.0, axis=1)
        gs_df = gs_df[gs_df["gs_score"] > 0].sort_values("gs_score", ascending=False)

        def gs_player_dict(p):
            tname    = p["team"]
            form     = team_form.get(tname, [])
            next_opp = team_next_opp.get(tname)
            return {
                "player":             p["player"],
                "player_id":          str(p.get("player_id","")),
                "team":               p["team"],
                "team_id":            str(p["team_id"]),
                "league":             p["league"],
                "gs_score":           round(float(p["gs_score"]), 2),
                "goals":              int(p["goals"]),
                "xg":                 round(float(p["xg"]), 2),
                "xg_per90":           round(float(p["xg_per90"]), 2),
                "xgot":               round(float(p["xgot"]), 2),
                "xgot_gap":           round(float(p["xgot_gap"]), 2),
                "sot_per90":          round(float(p["sot_per90"]), 2),
                "shots_per90":        round(float(p["shots_per90"]), 2),
                "big_chances_missed": int(p["big_chances_missed"]),
                "form":               form,
                "form_score":         form_score(form),
                "next_opponent":      next_opp.get("opponent") if next_opp else None,
                "next_kickoff":       next_opp.get("kickoff")  if next_opp else None,
                "weak_opp_def":       next_opp.get("weak_def") if next_opp else False,
                "player_img":         player_img_url(str(p.get("player_id",""))),
                "team_logo":          team_logo_url(str(p.get("team_id",""))),
            }

        gs_top25_list = [gs_player_dict(p) for _, p in gs_df.head(25).iterrows()]
        gs_all_list   = [gs_player_dict(p) for _, p in gs_df.iterrows()]

        for _, p in gs_df.iterrows():
            lg = p["league"]
            if not lg: continue
            if lg not in gs_by_league: gs_by_league[lg] = {}
            tm = p["team"]
            if tm not in gs_by_league[lg]: gs_by_league[lg][tm] = []
            gs_by_league[lg][tm].append(gs_player_dict(p))

        gs_by_league = {
            lg: [{"team": tm, "players": pls} for tm, pls in sorted(tms.items())]
            for lg, tms in gs_by_league.items()
        }

    log.info(f"Goal scorer: {len(gs_top25_list)} top25, {len(gs_all_list)} total")

    # ── TSOA aggregation — join assist + GS data by player_id ────────────────
    tsoa_top25_list   = []
    tsoa_all_list     = []
    tsoa_by_league    = {}

    if not df.empty and not gs_df.empty:
        # Build lookup dicts keyed by player_id
        assist_lookup = {}
        for _, row in df.iterrows():
            pid = str(row.get("player_id",""))
            if pid:
                assist_lookup[pid] = row

        gs_lookup = {}
        for _, row in gs_df.iterrows():
            pid = str(row.get("player_id",""))
            if pid:
                gs_lookup[pid] = row

        # Union of all player_ids
        all_pids = set(assist_lookup.keys()) | set(gs_lookup.keys())

        tsoa_rows = []
        for pid in all_pids:
            ar = assist_lookup.get(pid)
            gr = gs_lookup.get(pid)

            # Get base info from whichever source has it
            base = ar if ar is not None else gr
            tname   = str(base.get("team",""))
            tid     = str(base.get("team_id",""))
            league  = str(base.get("league","") or tlg.get(tid,""))
            player  = str(base.get("player",""))

            # Extract metrics from each side
            xg_per90  = safe_float(gr.get("xg_per90",0))  if gr is not None else 0.0
            xgot_gap  = safe_float(gr.get("xgot_gap",0))  if gr is not None else 0.0
            xa_per90  = safe_float(ar.get("xa_per90",0))  if ar is not None else 0.0
            xa_gap    = safe_float(ar.get("xa_gap",0))     if ar is not None else 0.0
            big_chances      = safe_float(ar.get("big_chances",0))       if ar is not None else 0.0
            big_chances_miss = safe_float(gr.get("big_chances_missed",0)) if gr is not None else 0.0
            bc_combined = big_chances + big_chances_miss

            score = tsoa_score(xg_per90, xa_per90, xgot_gap, xa_gap, bc_combined)
            if score <= 0: continue

            form     = team_form.get(tname, [])
            next_opp = team_next_opp.get(tname)

            tsoa_rows.append({
                "player":        player,
                "player_id":     pid,
                "team":          tname,
                "team_id":       tid,
                "league":        league,
                "tsoa_score":    score,
                "xg_per90":      round(xg_per90, 2),
                "xa_per90":      round(xa_per90, 2),
                "xgot_gap":      round(xgot_gap, 2),
                "xa_gap":        round(xa_gap, 2),
                "bc_combined":   int(bc_combined),
                "goals":         int(safe_float(gr.get("goals",0))) if gr is not None else 0,
                "assists":       int(safe_float(ar.get("assists",0))) if ar is not None else 0,
                "form":          form,
                "form_score":    form_score(form),
                "next_opponent": next_opp.get("opponent") if next_opp else None,
                "next_kickoff":  next_opp.get("kickoff")  if next_opp else None,
                "weak_opp_def":  next_opp.get("weak_def") if next_opp else False,
                "player_img":    player_img_url(pid),
                "team_logo":     team_logo_url(tid),
            })

        tsoa_rows.sort(key=lambda x: x["tsoa_score"], reverse=True)
        tsoa_top25_list = tsoa_rows[:25]
        tsoa_all_list   = tsoa_rows

        for p in tsoa_rows:
            lg = p["league"]
            if not lg: continue
            if lg not in tsoa_by_league: tsoa_by_league[lg] = {}
            tm = p["team"]
            if tm not in tsoa_by_league[lg]: tsoa_by_league[lg][tm] = []
            tsoa_by_league[lg][tm].append(p)

        tsoa_by_league = {
            lg: [{"team": tm, "players": pls} for tm, pls in sorted(tms.items())]
            for lg, tms in tsoa_by_league.items()
        }

    log.info(f"TSOA: {len(tsoa_top25_list)} top25, {len(tsoa_all_list)} total")

    log.info(f"Scraper done — {len(top25_list)} players, {len(by_league)} leagues: {list(by_league.keys())}")
    return top25_list, by_league, fixtures, all_players_list, teams_list, gs_top25_list, gs_all_list, gs_by_league, tsoa_top25_list, tsoa_all_list, tsoa_by_league


# ── Flask app ─────────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)

@app.route("/")
@app.route("/status")
def status():
    return jsonify({
        "status":          _cache["status"],
        "last_updated":    _cache["last_updated"],
        "refresh_started": _cache.get("refresh_started"),
        "top25_count":     len(_cache["top25"]),
        "leagues":         list(_cache["by_league"].keys()),
    })

@app.route("/data")
def data():
    if not _cache["top25"]:
        return jsonify({"error": "No data yet — call /refresh first"}), 503
    return jsonify({
        "last_updated": _cache["last_updated"],
        "top25":        _cache["top25"],
        "by_league":    _cache["by_league"],
        "all_players":  _cache.get("all_players", []),
        "gs_top25":      _cache.get("gs_top25", []),
        "gs_all":        _cache.get("gs_all", []),
        "gs_by_league":  _cache.get("gs_by_league", {}),
        "tsoa_top25":    _cache.get("tsoa_top25", []),
        "tsoa_all":      _cache.get("tsoa_all", []),
        "tsoa_by_league":_cache.get("tsoa_by_league", {}),
    })

@app.route("/fixtures")
def fixtures():
    if not _cache["fixtures"]:
        return jsonify({"error": "No fixtures yet — call /refresh first"}), 503
    return jsonify({
        "last_updated": _cache["last_updated"],
        "fixtures":     _cache["fixtures"],
    })

@app.route("/refresh", methods=["GET", "POST"])
def refresh():
    _cache["status"]           = "refreshing"
    _cache["refresh_started"]  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        top25, by_league, fixtures, all_players_full, teams_list, gs_top25, gs_all, gs_by_league, tsoa_top25, tsoa_all, tsoa_by_league = loop.run_until_complete(run_scraper())
        loop.close()
        _cache["top25"]        = top25
        _cache["by_league"]    = by_league
        _cache["fixtures"]     = fixtures
        _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        _cache["status"]       = "ok"
        _cache["all_players"]  = all_players_full
        _cache["teams"]        = teams_list
        _cache["gs_top25"]      = gs_top25
        _cache["gs_all"]        = gs_all
        _cache["gs_by_league"]  = gs_by_league
        _cache["tsoa_top25"]    = tsoa_top25
        _cache["tsoa_all"]      = tsoa_all
        _cache["tsoa_by_league"]= tsoa_by_league
        return jsonify({
            "success":      True,
            "last_updated": _cache["last_updated"],
            "top25":        top25,
            "by_league":    by_league,
        })
    except Exception as e:
        _cache["status"] = f"error: {str(e)}"
        log.error(f"Refresh failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ── Match screen endpoint ─────────────────────────────────────────────────────

@app.route("/match/<match_id>")
def match_screen(match_id):
    """
    Returns squad intelligence for a fixture.
    - Pre-kickoff: top ranked players from each squad + weak opp flag
    - Live/finished: same + live lineups from FotMob
    """
    from flask import request

    is_live     = request.args.get("live", "false").lower() == "true"
    is_finished = request.args.get("finished", "false").lower() == "true"
    home_id     = request.args.get("home_id", "")
    away_id     = request.args.get("away_id", "")
    home_name   = request.args.get("home", "Home")
    away_name   = request.args.get("away", "Away")

    all_players = _cache.get("all_players", [])
    teams       = _cache.get("teams", [])

    if not all_players:
        return jsonify({"error": "No data yet — call /refresh first"}), 503

    # Build team GA/G lookup + full team stats
    ga_lookup   = {t["team_id"]: t["ga_pg"] for t in teams}
    team_lookup = {t["team_id"]: t for t in teams}
    home_ga     = ga_lookup.get(home_id, 0)
    away_ga     = ga_lookup.get(away_id, 0)
    home_stats  = team_lookup.get(home_id, {})
    away_stats  = team_lookup.get(away_id, {})

    # Fuzzy team name matching — fixture names often differ from standings names
    # e.g. "Man Utd" vs "Manchester United", "Spurs" vs "Tottenham"
    def match_team(players, fixture_name, team_id):
        # Try exact match first
        exact = [p for p in players if p["team"] == fixture_name]
        if exact: return exact
        # Try team_id match
        by_id = [p for p in players if p.get("team_id") == str(team_id)]
        if by_id: return by_id
        # Try case-insensitive partial match
        fname_lower = fixture_name.lower()
        partial = [p for p in players
                   if fname_lower in p["team"].lower()
                   or p["team"].lower() in fname_lower]
        if partial: return partial
        # Try first word match (e.g. "Manchester" matches "Manchester United")
        first_word = fname_lower.split()[0] if fname_lower.split() else ""
        if len(first_word) > 4:
            word_match = [p for p in players
                         if first_word in p["team"].lower()]
            if word_match: return word_match
        return []

    # Build lookup dicts for GS and TSOA scores by player_id
    gs_lookup   = {p["player_id"]: p for p in _cache.get("gs_all",   []) if p.get("player_id")}
    tsoa_lookup = {p["player_id"]: p for p in _cache.get("tsoa_all", []) if p.get("player_id")}

    home_players = sorted(
        match_team(all_players, home_name, home_id),
        key=lambda x: x["score"], reverse=True)
    away_players = sorted(
        match_team(all_players, away_name, away_id),
        key=lambda x: x["score"], reverse=True)

    # Top 25 player names for highlighting
    top25_names = {p["player"] for p in _cache.get("top25", [])}

    def enrich(players, opp_ga):
        enriched = []
        for p in players:
            pid = p.get("player_id","")
            gs   = gs_lookup.get(pid, {})
            tsoa = tsoa_lookup.get(pid, {})
            enriched.append({
                **p,
                "in_top25":    p["player"] in top25_names,
                "weak_opp":    opp_ga >= WEAK_DEF_THRESH,
                # GS fields
                "gs_score":           gs.get("gs_score"),
                "xgot_gap":           gs.get("xgot_gap"),
                "xg":                 gs.get("xg"),
                "xg_per90":           gs.get("xg_per90"),
                "xgot":               gs.get("xgot"),
                "sot_per90":          gs.get("sot_per90"),
                "shots_per90":        gs.get("shots_per90"),
                "goals":              gs.get("goals"),
                "big_chances_missed": gs.get("big_chances_missed"),
                "big_chances_missed": gs.get("big_chances_missed"),
                # TSOA fields
                "tsoa_score":   tsoa.get("tsoa_score"),
                "xa_per90":     tsoa.get("xa_per90") or p.get("xa_per90"),
                "bc_combined":  tsoa.get("bc_combined"),
            })
        return enriched

    result = {
        "match_id":     match_id,
        "home":         home_name,
        "away":         away_name,
        "home_id":      home_id,
        "away_id":      away_id,
        "home_ga_pg":    home_ga,
        "away_ga_pg":    away_ga,
        "home_weak_def": home_ga >= WEAK_DEF_THRESH,
        "away_weak_def": away_ga >= WEAK_DEF_THRESH,
        "home_stats":    home_stats,
        "away_stats":    away_stats,
        "home_players": enrich(home_players, away_ga),   # away = opponent of home players
        "away_players": enrich(away_players, home_ga),   # home = opponent of away players
        "lineups":      None,
    }

    # If live or finished — fetch lineups
    if is_live or is_finished:
        try:
            from fotmob import FotMob
            async def fetch_lineups():
                async with FotMob() as fotmob:
                    return await fotmob.get_match_details(int(match_id))
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            details = loop.run_until_complete(fetch_lineups())
            loop.close()

            # Extract lineups
            lineup_data = details.get("lineup", {}) if isinstance(details, dict) else {}
            lineups = {}
            for side in ["home", "away"]:
                side_data = lineup_data.get(side, {})
                players   = side_data.get("players", [])
                starters  = []
                for group in players:
                    if isinstance(group, list):
                        for p in group:
                            if isinstance(p, dict):
                                name = p.get("name", {})
                                pname = (name.get("fullName") or
                                         name.get("lastName") or
                                         str(name)) if isinstance(name, dict) else str(name)
                                starters.append({
                                    "name":     pname.strip(),
                                    "in_top25": pname.strip() in top25_names,
                                })
                lineups[side] = starters
            result["lineups"] = lineups
        except Exception as e:
            log.error(f"lineups {match_id}: {e}")
            result["lineup_error"] = str(e)

    return jsonify(result)


# ── Standings endpoint ───────────────────────────────────────────────────────

def build_team_row(t):
    return {
        "team":       t["team"],
        "team_id":    t["team_id"],
        "table_pos":  t.get("table_pos", ""),
        "played":     t.get("played", 0),
        "gf_pg":      t.get("gf_pg", 0),
        "ga_pg":      t.get("ga_pg", 0),
        "gf_h_pg":    t.get("gf_h_pg", 0),
        "ga_h_pg":    t.get("ga_h_pg", 0),
        "gf_a_pg":    t.get("gf_a_pg", 0),
        "ga_a_pg":    t.get("ga_a_pg", 0),
        "home_adv":   t.get("home_adv", 0),
        "away_vuln":  t.get("away_vuln", 0),
        "weak_def":   t.get("weak_def", False),
        "conference": t.get("conference", ""),
        "team_logo":  team_logo_url(t.get("team_id", "")),
    }

@app.route("/standings")
def standings():
    """Return full team standings data by league."""
    all_players = _cache.get("all_players", [])
    teams       = _cache.get("teams", [])

    if not teams:
        return jsonify({"error": "No data yet — call /refresh first"}), 503

    import pandas as pd
    teams_df = pd.DataFrame(teams) if teams else pd.DataFrame()

    by_league = {}
    for league_name in LEAGUES:
        lg = [t for t in teams if t.get("league") == league_name]
        if not lg: continue

        # MLS: split into Eastern/Western conferences
        if league_name == "MLS":
            conferences = {"Eastern": [], "Western": []}
            for t in lg:
                conf = t.get("conference", "")
                if conf in conferences:
                    conferences[conf].append(t)
            if any(conferences.values()):
                for conf_name in ["Eastern", "Western"]:
                    if not conferences[conf_name]: continue
                    conf_sorted = sorted(conferences[conf_name],
                                        key=lambda x: safe_float(x.get("table_pos", 99)))
                    by_league[f"MLS {conf_name}"] = [build_team_row(t) for t in conf_sorted]
                log.info(f"MLS conferences: {[k for k in by_league if 'MLS' in k]}")
            else:
                lg_sorted = sorted(lg, key=lambda x: safe_float(x.get("table_pos", 99)))
                by_league["MLS"] = [build_team_row(t) for t in lg_sorted]
                log.warning("MLS: no conference data, showing combined")
            continue

        lg_sorted = sorted(lg, key=lambda x: safe_float(x.get("table_pos", 99)))
        by_league[league_name] = [build_team_row(t) for t in lg_sorted]

    return jsonify({
        "last_updated": _cache["last_updated"],
        "standings":    by_league,
    })


# ── Confirmed Lineups via API-Football ───────────────────────────────────────

AF_KEY     = os.environ.get("API_FOOTBALL_KEY", "")
AF_BASE    = "https://v3.football.api-sports.io"
AF_HEADERS = {"x-apisports-key": AF_KEY}

# League ID mapping FotMob → API-Football
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
_fixtures_cache = {}  # date_str  → list of AF fixtures (saves API calls)

async def af_fixture_id(home_team, away_team, match_date):
    """Find API-Football fixture ID by team names and date.
    Caches fixture list per date so only 1 API call per day regardless of lookups."""
    try:
        date_str = match_date[:10]
        # Use cached fixture list if available
        if date_str in _fixtures_cache:
            fixtures = _fixtures_cache[date_str]
            log.info(f"AF: using cached {len(fixtures)} fixtures for {date_str}")
        else:
            url = f"{AF_BASE}/fixtures"
            params = {"date": date_str, "timezone": "UTC"}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=AF_HEADERS, params=params,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        log.warning(f"AF fixtures: HTTP {resp.status}")
                        return None
                    data = await resp.json(content_type=None)
            fixtures = data.get("response", [])
            if fixtures:  # Only cache non-empty results
                _fixtures_cache[date_str] = fixtures
            log.info(f"AF: fetched {len(fixtures)} fixtures for {date_str} (cached)")
        def clean_name(s):
            return (s.lower()
                    .replace(" fc","").replace(" cf","").replace(" afc","")
                    .replace("stade ","").replace(" 29","").replace(" sc","")
                    .replace("manchester ","man ").replace("atletico ","atl ")
                    .replace("athletic ","").replace("inter ","")
                    .strip())
        home_c = clean_name(home_team)
        away_c = clean_name(away_team)
        best_id = None
        best_score = 0
        for f in fixtures:
            h = clean_name(f.get("teams",{}).get("home",{}).get("name",""))
            a = clean_name(f.get("teams",{}).get("away",{}).get("name",""))
            h_words = set(h.split())
            a_words = set(a.split())
            home_words = set(home_c.split())
            away_words = set(away_c.split())
            hs  = len(h_words & home_words)
            as_ = len(a_words & away_words)
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
            starters  = []
            for p in team_data.get("startXI", []):
                player = p.get("player", {})
                starters.append({
                    "name":   player.get("name", ""),
                    "number": player.get("number", ""),
                    "pos":    player.get("pos", ""),
                    "grid":   player.get("grid", ""),
                })
            bench = []
            for p in team_data.get("substitutes", []):
                player = p.get("player", {})
                bench.append({
                    "name":   player.get("name", ""),
                    "number": player.get("number", ""),
                    "pos":    player.get("pos", ""),
                })
            result[team_name] = {"formation": formation, "starters": starters, "bench": bench}
        return result if result else None
    except Exception as e:
        log.error(f"af_lineups {fixture_id}: {e}")
        return None


@app.route("/debug/stats/<league_id>/<season_id>")
def debug_stats(league_id, season_id):
    """Test which stats are available for a given league/season."""
    GOAL_STATS_TO_TEST = [
        "goals", "goals_per_90", "expected_goals", "expected_goals_per_90",
        "expected_goalsontarget", "ontarget_scoring_att", "total_scoring_att",
        "big_chance_missed", "penalty_scored", "penalty_won",
    ]

    async def fetch_all():
        results = {}
        async with aiohttp.ClientSession() as session:
            for stat in GOAL_STATS_TO_TEST:
                url = f"https://data.fotmob.com/stats/{league_id}/season/{season_id}/{stat}.json"
                try:
                    async with session.get(url, headers=HEADERS,
                                          timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            top = data.get("TopLists", [{}])[0].get("StatList", [])
                            sample = top[0] if top else {}
                            results[stat] = {
                                "available": True,
                                "count": len(top),
                                "sample_player": sample.get("ParticipantName",""),
                                "sample_value": sample.get("StatValue",""),
                            }
                        else:
                            results[stat] = {"available": False, "status": resp.status}
                except Exception as e:
                    results[stat] = {"available": False, "error": str(e)}
        return results

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(fetch_all())
    loop.close()

    available = [s for s, v in results.items() if v.get("available")]
    print(f"Available for {league_id}/{season_id}: {available}")
    return jsonify({"available": available, "details": results})


@app.route("/debug/corners/<league_id>/<season_id>")
def debug_corners(league_id, season_id):
    """Test corner stat availability for a league/season."""
    CORNER_STATS = [
        "corners_won", "corner_won", "corners", "total_corners",
        "corners_taken", "corner_taken", "corners_per_game",
        "corners_conceded", "corner_conceded",
    ]
    async def fetch_all():
        results = {}
        async with aiohttp.ClientSession() as session:
            for stat in CORNER_STATS:
                url = f"https://data.fotmob.com/stats/{league_id}/season/{season_id}/{stat}.json"
                try:
                    async with session.get(url, headers=HEADERS,
                                          timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            top = data.get("TopLists", [{}])[0].get("StatList", [])
                            sample = top[0] if top else {}
                            results[stat] = {
                                "available": True,
                                "count": len(top),
                                "sample": sample.get("ParticipantName",""),
                                "value": sample.get("StatValue",""),
                            }
                        else:
                            results[stat] = {"available": False, "status": resp.status}
                except Exception as e:
                    results[stat] = {"available": False, "error": str(e)}
        return results
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(fetch_all())
    loop.close()
    available = [s for s, v in results.items() if v.get("available")]
    return jsonify({"available": available, "details": results})


@app.route("/debug/fixtures/<date>")
def debug_fixtures(date):
    """List all fixtures API-Football knows about for a date."""
    async def fetch():
        url = f"{AF_BASE}/fixtures"
        params = {"date": date, "timezone": "UTC"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=AF_HEADERS, params=params,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return {"error": f"HTTP {resp.status}"}
                data = await resp.json(content_type=None)
        return [{"id": f["fixture"]["id"],
                 "home": f["teams"]["home"]["name"],
                 "away": f["teams"]["away"]["name"],
                 "league": f["league"]["name"]}
                for f in data.get("response", [])]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(fetch())
    loop.close()
    return jsonify(result)


@app.route("/lineups/<match_id>")
def lineups(match_id):
    """Fetch confirmed lineups for a match via API-Football."""
    if not AF_KEY:
        return jsonify({"error": "API_FOOTBALL_KEY not configured"}), 503

    from flask import request as freq
    home      = freq.args.get("home", "")
    away      = freq.args.get("away", "")
    kickoff   = freq.args.get("kickoff", "")

    if not home or not away or not kickoff:
        return jsonify({"error": "home, away, kickoff required"}), 400

    # Check cache (lineups don't change once confirmed)
    cache_key = f"{home}_{away}_{kickoff[:10]}"
    if cache_key in _lineup_cache:
        cached = _lineup_cache[cache_key]
        return jsonify(cached)

    # Find API-Football fixture ID
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    fixture_id = loop.run_until_complete(af_fixture_id(home, away, kickoff))
    if not fixture_id:
        loop.close()
        return jsonify({"confirmed": False, "message": "Match not found on API-Football"}), 200

    lineup_data = loop.run_until_complete(af_lineups(fixture_id))
    loop.close()
    if not lineup_data:
        return jsonify({"confirmed": False,
                        "message": "Lineups not yet released",
                        "fixture_id": fixture_id}), 200

    result = {
        "confirmed":  True,
        "fixture_id": fixture_id,
        "lineups":    lineup_data,
    }
    _lineup_cache[cache_key] = result
    log.info(f"Lineups confirmed: {home} vs {away} (fixture {fixture_id})")
    return jsonify(result)


# ── Live match stats endpoint ────────────────────────────────────────────────

@app.route("/live/<match_id>")
def live_match(match_id):
    """Fetch live match events and stats via fotmob wrapper (handles x-mas token)."""
    async def fetch():
        from fotmob import FotMob
        fotmob = FotMob()
        try:
            return await fotmob.get_match(int(match_id))
        except Exception as e:
            log.error(f"fotmob.get_match {match_id}: {e}")
            return {"error": str(e)}

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        data = loop.run_until_complete(fetch())
        loop.close()

        if "error" in data:
            return jsonify(data), 503

        # Log full structure to find events/stats
        log.info(f"live {match_id} ALL keys: {list(data.keys())}")
        for k, v in data.items():
            if isinstance(v, dict):
                log.info(f"  {k} (dict): {list(v.keys())[:8]}")
            elif isinstance(v, list) and v:
                log.info(f"  {k} (list[{len(v)}]): first={str(v[0])[:80]}")
        # Flat structure: home{}, away{}, incidents[], stats[]
        home_data = data.get("home", {})
        away_data = data.get("away", {})
        home_id   = str(home_data.get("id",""))
        away_id   = str(away_data.get("id",""))

        # Events — try all possible keys
        evt_list = (data.get("incidents") or
                    data.get("events") or
                    data.get("goals") or [])
        if not isinstance(evt_list, list): evt_list = []
        log.info(f"live {match_id} evt_list={len(evt_list)}")

        events = []
        for e in evt_list:
            etype = str(e.get("type","") or e.get("eventType","")).lower()
            if not etype: continue
            team_id = str(e.get("teamId","") or e.get("team",{}).get("id",""))
            side    = "home" if team_id == home_id else "away"

            if any(g in etype for g in ["goal","penalty"]) and "own" not in etype:
                events.append({
                    "type":    "goal",
                    "minute":  e.get("timeStr") or e.get("min",""),
                    "player":  e.get("playerName") or e.get("player",{}).get("name",""),
                    "assist":  e.get("assistPlayerName") or e.get("assist",{}).get("name",""),
                    "side":    side,
                })
            elif "owngoal" in etype or "own_goal" in etype:
                events.append({
                    "type":    "owngoal",
                    "minute":  e.get("timeStr") or e.get("min",""),
                    "player":  e.get("playerName") or e.get("player",{}).get("name",""),
                    "side":    side,
                })
            elif "yellow" in etype or "card" in etype:
                card = "red" if "red" in etype or e.get("isRed") else "yellow"
                events.append({
                    "type":    card,
                    "minute":  e.get("timeStr") or e.get("min",""),
                    "player":  e.get("playerName") or e.get("player",{}).get("name",""),
                    "side":    side,
                })
            elif "sub" in etype or "substitut" in etype:
                events.append({
                    "type":    "sub",
                    "minute":  e.get("timeStr") or e.get("min",""),
                    "player":  e.get("playerName") or e.get("player",{}).get("name",""),
                    "playerOut": e.get("playerOutName") or e.get("playerOut",{}).get("name",""),
                    "side":    side,
                })

        # Stats: data['stats']['stats'] = list of stat items
        # Each: {'title': 'Possession', 'stats': [62, 38], 'highlighted': 'home'}
        stats_block = data.get("stats", {})
        stats_raw   = stats_block.get("stats", []) if isinstance(stats_block, dict) else []
        log.info(f"live {match_id} stats_raw len={len(stats_raw)}, first={str(stats_raw[0])[:200] if stats_raw else 'empty'}")
        team_colors_raw = stats_block.get("teamColors", {})
        log.info(f"live {match_id} teamColors={str(team_colors_raw)[:200]}")
        stats_out   = {}
        for stat in stats_raw:
            if not isinstance(stat, dict): continue
            title = (stat.get("title","") or "").lower()
            vals  = stat.get("stats", [])
            if not isinstance(vals, list) or len(vals) < 2: continue
            h, a = safe_float(vals[0]), safe_float(vals[1])
            if "possession" in title:
                stats_out["possession"] = [h, a]
            elif "shot" in title and "target" in title:
                stats_out["shots_on_target"] = [h, a]
            elif "shot" in title and "target" not in title and "block" not in title:
                stats_out["shots"] = [h, a]
            elif "xg" in title or ("expected" in title and "goal" in title):
                stats_out["xg"] = [round(h,2), round(a,2)]
        log.info(f"live {match_id} stats found: {list(stats_out.keys())}")

        # Colors: data['stats']['teamColors']['darkMode'] = {'home': '#...', 'away': '#...'}
        team_colors  = stats_block.get("teamColors", {}) if isinstance(stats_block, dict) else {}
        dark_colors  = team_colors.get("darkMode", {})
        home_color   = dark_colors.get("home") or team_colors.get("home")
        away_color   = dark_colors.get("away") or team_colors.get("away")
        if home_color and not home_color.startswith("#"): home_color = f"#{home_color}"
        if away_color and not away_color.startswith("#"): away_color = f"#{away_color}"

        # Score: data['home']['score'] / data['away']['score']
        home_score = safe_float(home_data.get("score", 0))
        away_score = safe_float(away_data.get("score", 0))

        # Minute: data['liveTime']['short']
        live_time  = data.get("liveTime", {}) or {}
        minute     = live_time.get("short","") or live_time.get("long","")

        log.info(f"live {match_id}: {len(events)} events, stats={list(stats_out.keys())}, colors={home_color},{away_color}, score={home_score}-{away_score}")

        return jsonify({
            "events":     events,
            "stats":      stats_out,
            "minute":     minute,
            "score":      [int(home_score), int(away_score)],
            "home_color": home_color,
            "away_color": away_color,
        })
    except Exception as e:
        log.error(f"live_match {match_id}: {e}")
        return jsonify({"error": str(e)}), 500


# ── Player detail endpoint ───────────────────────────────────────────────────

@app.route("/player/<player_id>")
def player_detail(player_id):
    """Fetch L5 xA for a specific player on demand."""
    from flask import request

    team_id     = request.args.get("team_id", "")
    player_name = request.args.get("name", "")

    if not team_id:
        return jsonify({"error": "team_id required"}), 400

    # Find player in cache for base stats
    all_players = _cache.get("all_players", [])
    player_data = next((p for p in all_players
                        if str(p.get("player_id","")) == str(player_id)), None)

    try:
        from fotmob import FotMob
        async def fetch():
            async with FotMob() as fotmob:
                return await get_player_l5(fotmob, player_id, team_id, player_name)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        l5_games = loop.run_until_complete(fetch())
        loop.close()

        # Calculate L5 xA total
        l5_g = sum(g.get("goals",0) for g in l5_games)
        l5_a = sum(g.get("assists",0) for g in l5_games)

        return jsonify({
            "player_id":        player_id,
            "player_name":      player_name,
            "l5_games":         l5_games,
            "l5_goals_total":   l5_g,
            "l5_assists_total": l5_a,
            "base_stats":       player_data,
        })
    except Exception as e:
        log.error(f"player_detail {player_id}: {e}")
        return jsonify({"error": str(e)}), 500


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Starting server on port {port}")
    app.run(host="0.0.0.0", port=port)
from positional_concessions import bootstrap_season, update_after_match, get_multipliers
from sm_baseline import bootstrap_baselines, refresh_baselines
from sm_scorer import score_todays_fixtures
from pipeline_comparison import build_comparison_for_date, record_outcomes, get_running_totals

@app.route('/api/concessions/bootstrap', methods=['POST'])
def concessions_bootstrap():
    data = request.json
    bootstrap_season(data['season_id'], data['league_id'])
    return jsonify({"status": "ok"})

@app.route('/api/baseline/bootstrap', methods=['POST'])
def baseline_bootstrap():
    bootstrap_baselines()
    return jsonify({"status": "ok"})

@app.route('/api/sm/score-today', methods=['POST'])
def sm_score_today():
    score_todays_fixtures()
    return jsonify({"status": "ok"})

@app.route('/api/comparison/build', methods=['PO
