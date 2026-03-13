# =============================================================================
# ASSIST RESEARCH TOOL — RAILWAY SERVER v2
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
WEAK_DEF_THRESH = 1.3   # GA/G above this = weak defence flag
FORM_MATCHES    = 5     # last N matches for form

LEAGUES = {
    "Premier League": {"id": 47, "short": "PL"},
    "Championship":   {"id": 48, "short": "Champ"},
    "La Liga":        {"id": 87, "short": "LaLiga"},
    "Serie A":        {"id": 55, "short": "SerieA"},
    "Bundesliga":     {"id": 54, "short": "Bundesliga"},
    "Ligue 1":        {"id": 53, "short": "Ligue1"},
}

STATS = {
    "goal_assist":             "assists",
    "expected_assists":        "xa",
    "big_chance_created":      "big_chances",
    "total_att_assist":        "chances_created",
    "expected_assists_per_90": "xa_per90",
    "penalty_won":             "penalties_won",
}

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)"
                   " AppleWebKit/605.1.15"),
    "Accept":  "application/json",
    "Referer": "https://www.fotmob.com/",
}

# ── Cache ─────────────────────────────────────────────────────────────────────

_cache = {
    "last_updated":  None,
    "top25":         [],
    "by_league":     {},
    "fixtures":      {},
    "all_players":   [],   # full unfiltered list for squad lookups
    "teams":         [],   # team list with ga_pg for weak def lookup
    "status":        "never_run",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_float(v):
    try:
        return float(str(v).replace(",","").strip()) if v not in [None,""," ","-","N/A"] else 0.0
    except:
        return 0.0

def combined_score(xa_gap, cc_pg, big_chances, penalties_won):
    return round(
        (xa_gap        * 0.35) +
        (cc_pg         * 0.30) +
        (big_chances   * 0.20) +
        (penalties_won * 0.15), 2)

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
    try:
        data = await fotmob.standings(league_id)
        if not isinstance(data, list) or not data:
            return []
        for item in data:
            if not isinstance(item, dict): continue
            table    = item.get("data", {}).get("table", {})
            all_rows = table.get("all", [])
            home_lkp = {str(t.get("id","")): t for t in table.get("home", []) if isinstance(t, dict)}
            away_lkp = {str(t.get("id","")): t for t in table.get("away", []) if isinstance(t, dict)}
            for team in all_rows:
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
                try:    _, ga_h = [safe_float(x) for x in ht.get("scoresStr","0-0").split("-")]
                except: ga_h = 0.0
                try:    _, ga_a = [safe_float(x) for x in at.get("scoresStr","0-0").split("-")]
                except: ga_a = 0.0
                if name and played > 0:
                    ga_pg   = round(ga / played, 2)
                    ga_h_pg = round(ga_h / h_pl, 2) if h_pl > 0 else 0.0
                    ga_a_pg = round(ga_a / a_pl, 2) if a_pl > 0 else 0.0
                    rows.append({
                        "team":      name,
                        "team_id":   tid,
                        "league":    league_name,
                        "table_pos": safe_float(team.get("idx", 99)),
                        "played":    int(played),
                        "gf_pg":     round(safe_float(gf) / played, 2),
                        "ga_pg":     ga_pg,
                        "ga_h_pg":   ga_h_pg,
                        "ga_a_pg":   ga_a_pg,
                        "weak_def":  ga_pg >= WEAK_DEF_THRESH,
                    })
        log.info(f"standings {league_name}: {len(rows)} teams")
    except Exception as e:
        log.error(f"standings {league_name}: {e}")
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
    fixtures_by_league = {ln: [] for ln in LEAGUES}
    league_ids = set(str(info["id"]) for info in LEAGUES.values())
    id_to_league = {str(info["id"]): ln for ln, info in LEAGUES.items()}

    today = datetime.utcnow()
    dates = [(today + timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]

    for date_str in dates:
        await asyncio.sleep(0.4)
        try:
            data = await fotmob.get_matches_by_date(date_str)
            leagues = data.get("leagues", []) if isinstance(data, dict) else []
            for league in leagues:
                if not isinstance(league, dict): continue
                lid = str(league.get("id", ""))
                if lid not in league_ids: continue
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
                    fixtures_by_league[ln].append({
                        "match_id":   str(match.get("id", "")),
                        "home":       home.get("name", ""),
                        "home_id":    str(home.get("id", "")),
                        "away":       away.get("name", ""),
                        "away_id":    str(away.get("id", "")),
                        "kickoff":    utc_time,
                        "date":       date_str,
                        "live":       live,
                        "finished":   finished,
                        "score":      f"{home.get('score','-')} - {away.get('score','-')}" if (live or finished) else None,
                        "minute":     status.get("liveTime", {}).get("short", "") if live else None,
                    })
        except Exception as e:
            log.error(f"fixtures {date_str}: {e}")

    # Sort each league by kickoff
    for ln in fixtures_by_league:
        fixtures_by_league[ln].sort(key=lambda x: x.get("kickoff","") or "")

    return fixtures_by_league


# ── Main scraper ──────────────────────────────────────────────────────────────

async def run_scraper():
    import pandas as pd
    from fotmob import FotMob

    log.info("Scraper v2 starting...")
    all_team_rows   = []
    all_player_rows = []

    async with FotMob() as fotmob:

        # Pass 1 — Standings
        for league_name, info in LEAGUES.items():
            rows = await get_standings(fotmob, league_name, info["id"])
            all_team_rows.extend(rows)

        if not all_team_rows:
            raise RuntimeError("No standings data")

        teams_df     = pd.DataFrame(all_team_rows)
        all_team_ids = set(str(tid) for tid in teams_df["team_id"].tolist())
        log.info(f"Standings: {len(teams_df)} teams")

        # Pass 2 — Season IDs
        league_seasons = {}
        for league_name, info in LEAGUES.items():
            lt = teams_df[teams_df["league"] == league_name]
            if lt.empty: continue
            await asyncio.sleep(0.8)
            sid = await get_season_id(fotmob, lt.iloc[0]["team_id"])
            if sid:
                league_seasons[info["id"]] = sid
                log.info(f"Season ID {league_name}: {sid}")

        # Pass 3 — Player stats
        fotmob_session = getattr(fotmob, "_session", None) or getattr(fotmob, "session", None)
        sess = fotmob_session or aiohttp.ClientSession()

        for league_name, info in LEAGUES.items():
            lid = info["id"]
            sid = league_seasons.get(lid)
            if not sid: continue
            for stat_name in STATS:
                await asyncio.sleep(0.3)
                rows = await fetch_stat(sess, lid, sid, stat_name, all_team_ids)
                all_player_rows.extend(rows)
            log.info(f"Players fetched: {league_name}")

        if fotmob_session is None and sess:
            await sess.close()

        # Pass 4 — Team form (last 5) + next opponent
        log.info("Fetching team form + next opponent...")
        team_form     = {}
        team_next_opp = {}
        for _, team in teams_df.iterrows():
            tid   = team["team_id"]
            tname = team["team"]
            await asyncio.sleep(0.5)
            form = await get_team_form(fotmob, tid, tname)
            team_form[tname] = form
            await asyncio.sleep(0.5)
            opp, _ = await get_next_opponent(fotmob, tid, tname, teams_df)
            team_next_opp[tname] = opp
            log.info(f"  {tname}: form={form} opp={opp['opponent'] if opp else 'N/A'}")

        # Pass 5 — Fixtures
        log.info("Fetching fixtures...")
        fixtures = await get_fixtures_for_dates(fotmob, days=7)

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
                "team":            row["team"],
                "team_id":         row["team_id"],
                "league":          tlg.get(row["team_id"], ""),
                "table_pos":       tpos.get(row["team_id"], 99),
                "games":           tgp.get(row["team_id"], 30),
                "assists":         0.0, "xa":              0.0,
                "xa_per90":        0.0, "big_chances":     0.0,
                "chances_created": 0.0, "penalties_won":   0.0,
            }
        st = row["stat_type"]
        if st in agg[key]:
            agg[key][st] = safe_float(row["stat_value"])

    df = pd.DataFrame(list(agg.values()))
    df["chances_per_game"] = df.apply(
        lambda r: round(r["chances_created"] / r["games"], 2)
        if r["games"] > 0 and r["chances_created"] > 0
        else round(r["xa_per90"] * 3.5, 2), axis=1)
    df["xa_gap"] = (df["xa"] - df["assists"]).round(1)
    df["score"]  = df.apply(
        lambda r: combined_score(
            r["xa_gap"], r["chances_per_game"],
            r["big_chances"], r["penalties_won"])
        if (r["xa"] > 0 or r["chances_per_game"] > 0) else 0.0, axis=1)

    df = df[df["score"] > 0].sort_values("score", ascending=False).reset_index(drop=True)

    def player_dict(p):
        tname    = p["team"]
        form     = team_form.get(tname, [])
        next_opp = team_next_opp.get(tname)
        return {
            "player":           p["player"],
            "team":             p["team"],
            "league":           p["league"],
            "score":            round(float(p["score"]), 2),
            "assists":          int(p["assists"]),
            "xa":               round(float(p["xa"]), 2),
            "xa_gap":           round(float(p["xa_gap"]), 2),
            "chances_per_game": round(float(p["chances_per_game"]), 2),
            "big_chances":      int(p["big_chances"]),
            "penalties_won":    int(p["penalties_won"]),
            # Form
            "form":             form,
            "form_score":       form_score(form),
            # Next opponent
            "next_opponent":    next_opp.get("opponent")    if next_opp else None,
            "next_kickoff":     next_opp.get("kickoff")     if next_opp else None,
            "weak_opp_def":     next_opp.get("weak_def")    if next_opp else False,
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
        {"team": r["team"], "team_id": r["team_id"],
         "ga_pg": r["ga_pg"], "weak_def": r["weak_def"]}
        for _, r in teams_df.iterrows()
    ]

    log.info(f"Scraper done — {len(top25_list)} players, {len(by_league)} leagues")
    return top25_list, by_league, fixtures, all_players_list, teams_list


# ── Flask app ─────────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)

@app.route("/")
@app.route("/status")
def status():
    return jsonify({
        "status":       _cache["status"],
        "last_updated": _cache["last_updated"],
        "top25_count":  len(_cache["top25"]),
        "leagues":      list(_cache["by_league"].keys()),
    })

@app.route("/data")
def data():
    if not _cache["top25"]:
        return jsonify({"error": "No data yet — call /refresh first"}), 503
    return jsonify({
        "last_updated": _cache["last_updated"],
        "top25":        _cache["top25"],
        "by_league":    _cache["by_league"],
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
    _cache["status"] = "refreshing"
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        top25, by_league, fixtures, all_players_full, teams_list = loop.run_until_complete(run_scraper())
        loop.close()
        _cache["top25"]        = top25
        _cache["by_league"]    = by_league
        _cache["fixtures"]     = fixtures
        _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        _cache["status"]       = "ok"
        # Store full player + team lists for match screen lookups
        _cache["all_players"]  = all_players_full
        _cache["teams"]        = teams_list
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

    # Build team GA/G lookup
    ga_lookup = {t["team_id"]: t["ga_pg"] for t in teams}
    home_ga   = ga_lookup.get(home_id, 0)
    away_ga   = ga_lookup.get(away_id, 0)

    # Get ranked players for each squad
    home_players = sorted(
        [p for p in all_players if p["team"] == home_name],
        key=lambda x: x["score"], reverse=True)
    away_players = sorted(
        [p for p in all_players if p["team"] == away_name],
        key=lambda x: x["score"], reverse=True)

    # Top 25 player names for highlighting
    top25_names = {p["player"] for p in _cache.get("top25", [])}

    def enrich(players, opp_ga):
        return [{
            **p,
            "in_top25":    p["player"] in top25_names,
            "weak_opp":    opp_ga >= WEAK_DEF_THRESH,
        } for p in players]

    result = {
        "match_id":     match_id,
        "home":         home_name,
        "away":         away_name,
        "home_id":      home_id,
        "away_id":      away_id,
        "home_ga_pg":   home_ga,
        "away_ga_pg":   away_ga,
        "home_weak_def": home_ga >= WEAK_DEF_THRESH,
        "away_weak_def": away_ga >= WEAK_DEF_THRESH,
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


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Starting server on port {port}")
    app.run(host="0.0.0.0", port=port)
