# =============================================================================
# main.py — Deep Current Football API v3
# =============================================================================
# Endpoints:
#   GET  /            — health check + version
#   GET  /status      — health check (alias)
#   GET  /version     — version info
#   GET  /data        — cached player + team data
#   POST /refresh     — re-run full scraper
#   GET  /fixtures    — upcoming + live fixtures
#   GET  /standings   — team standings by league
#   GET  /match/<id>  — squad intelligence for a fixture
#   GET  /lineups/<id>— confirmed lineups via API-Football
#   GET  /live/<id>   — live match events + stats
#   GET  /player/<id> — L5 goals + assists for a player
# =============================================================================

import asyncio, os, logging
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

from .utils import (_cache, LEAGUES, WEAK_DEF_THRESH, APP_VERSION,
                    safe_float, player_img_url, team_logo_url)
from .scraper  import run_scraper, get_player_l5
from .standings import build_team_row
from .lineups  import af_fixture_id, af_lineups, AF_KEY, _lineup_cache

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app  = Flask(__name__)
CORS(app)

# ── Health / version ──────────────────────────────────────────────────────────

@app.route("/")
@app.route("/status")
def status():
    return jsonify({
        "status":          _cache["status"],
        "last_updated":    _cache["last_updated"],
        "refresh_started": _cache.get("refresh_started"),
        "top25_count":     len(_cache["top25"]),
        "leagues":         list(_cache["by_league"].keys()),
        "version":         APP_VERSION,
    })


@app.route("/version")
def version():
    return jsonify({
        "version":    APP_VERSION,
        "name":       "Deep Current Football API",
        "status":     _cache["status"],
        "built_with": "fotmob + flask + railway",
    })


# ── Player data ───────────────────────────────────────────────────────────────

@app.route("/data")
def data():
    if not _cache["top25"]:
        return jsonify({"error": "No data yet — call /refresh first"}), 503
    return jsonify({
        "last_updated":  _cache["last_updated"],
        "top25":         _cache["top25"],
        "by_league":     _cache["by_league"],
        "all_players":   _cache.get("all_players", []),
        "gs_top25":      _cache.get("gs_top25", []),
        "gs_all":        _cache.get("gs_all", []),
        "gs_by_league":  _cache.get("gs_by_league", {}),
        "tsoa_top25":    _cache.get("tsoa_top25", []),
        "tsoa_all":      _cache.get("tsoa_all", []),
        "tsoa_by_league":_cache.get("tsoa_by_league", {}),
        "version":       APP_VERSION,
    })


# ── Fixtures ──────────────────────────────────────────────────────────────────

@app.route("/fixtures")
def fixtures():
    if not _cache["fixtures"]:
        return jsonify({"error": "No fixtures yet — call /refresh first"}), 503
    return jsonify({
        "last_updated": _cache["last_updated"],
        "fixtures":     _cache["fixtures"],
    })


# ── Refresh ───────────────────────────────────────────────────────────────────

@app.route("/refresh", methods=["GET", "POST"])
def refresh():
    _cache["status"]          = "refreshing"
    _cache["refresh_started"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        (top25, by_league, fix, all_players_full, teams_list,
         gs_top25, gs_all, gs_by_league,
         tsoa_top25, tsoa_all, tsoa_by_league) = loop.run_until_complete(run_scraper())
        loop.close()
        _cache.update({
            "top25":         top25,
            "by_league":     by_league,
            "fixtures":      fix,
            "last_updated":  datetime.now().strftime("%Y-%m-%d %H:%M"),
            "status":        "ok",
            "all_players":   all_players_full,
            "teams":         teams_list,
            "gs_top25":      gs_top25,
            "gs_all":        gs_all,
            "gs_by_league":  gs_by_league,
            "tsoa_top25":    tsoa_top25,
            "tsoa_all":      tsoa_all,
            "tsoa_by_league":tsoa_by_league,
        })
        return jsonify({
            "success":      True,
            "last_updated": _cache["last_updated"],
            "top25":        top25,
            "by_league":    by_league,
            "version":      APP_VERSION,
        })
    except Exception as e:
        _cache["status"] = f"error: {str(e)}"
        log.error(f"Refresh failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ── Match screen ──────────────────────────────────────────────────────────────

@app.route("/match/<match_id>")
def match_screen(match_id):
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

    ga_lookup   = {t["team_id"]: t["ga_pg"] for t in teams}
    team_lookup = {t["team_id"]: t for t in teams}
    home_ga     = ga_lookup.get(home_id, 0)
    away_ga     = ga_lookup.get(away_id, 0)

    def match_team(players, fixture_name, team_id):
        exact = [p for p in players if p["team"] == fixture_name]
        if exact: return exact
        by_id = [p for p in players if p.get("team_id") == str(team_id)]
        if by_id: return by_id
        fname_lower = fixture_name.lower()
        partial = [p for p in players
                   if fname_lower in p["team"].lower()
                   or p["team"].lower() in fname_lower]
        if partial: return partial
        first_word = fname_lower.split()[0] if fname_lower.split() else ""
        if len(first_word) > 4:
            word_match = [p for p in players if first_word in p["team"].lower()]
            if word_match: return word_match
        return []

    gs_lkp   = {p["player_id"]: p for p in _cache.get("gs_all",   []) if p.get("player_id")}
    tsoa_lkp = {p["player_id"]: p for p in _cache.get("tsoa_all", []) if p.get("player_id")}

    home_players = sorted(match_team(all_players, home_name, home_id),
                          key=lambda x: x["score"], reverse=True)
    away_players = sorted(match_team(all_players, away_name, away_id),
                          key=lambda x: x["score"], reverse=True)
    top25_names  = {p["player"] for p in _cache.get("top25", [])}

    def enrich(players, opp_ga):
        out = []
        for p in players:
            pid  = p.get("player_id","")
            gs   = gs_lkp.get(pid, {})
            tsoa = tsoa_lkp.get(pid, {})
            out.append({
                **p,
                "in_top25":           p["player"] in top25_names,
                "weak_opp":           opp_ga >= WEAK_DEF_THRESH,
                "gs_score":           gs.get("gs_score"),
                "xgot_gap":           gs.get("xgot_gap"),
                "xg":                 gs.get("xg"),
                "xg_per90":           gs.get("xg_per90"),
                "xgot":               gs.get("xgot"),
                "sot_per90":          gs.get("sot_per90"),
                "shots_per90":        gs.get("shots_per90"),
                "goals":              gs.get("goals"),
                "big_chances_missed": gs.get("big_chances_missed"),
                "tsoa_score":         tsoa.get("tsoa_score"),
                "xa_per90":           tsoa.get("xa_per90") or p.get("xa_per90"),
                "bc_combined":        tsoa.get("bc_combined"),
            })
        return out

    result = {
        "match_id":      match_id,
        "home":          home_name,
        "away":          away_name,
        "home_id":       home_id,
        "away_id":       away_id,
        "home_ga_pg":    home_ga,
        "away_ga_pg":    away_ga,
        "home_weak_def": home_ga >= WEAK_DEF_THRESH,
        "away_weak_def": away_ga >= WEAK_DEF_THRESH,
        "home_stats":    team_lookup.get(home_id, {}),
        "away_stats":    team_lookup.get(away_id, {}),
        "home_players":  enrich(home_players, away_ga),
        "away_players":  enrich(away_players, home_ga),
        "lineups":       None,
        "version":       APP_VERSION,
    }

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
            lineup_data = details.get("lineup", {}) if isinstance(details, dict) else {}
            lineups = {}
            for side in ["home", "away"]:
                side_data = lineup_data.get(side, {})
                starters  = []
                for group in side_data.get("players", []):
                    if isinstance(group, list):
                        for p in group:
                            if isinstance(p, dict):
                                name  = p.get("name", {})
                                pname = (name.get("fullName") or name.get("lastName") or str(name)) \
                                        if isinstance(name, dict) else str(name)
                                starters.append({"name": pname.strip(),
                                                 "in_top25": pname.strip() in top25_names})
                lineups[side] = starters
            result["lineups"] = lineups
        except Exception as e:
            log.error(f"lineups {match_id}: {e}")
            result["lineup_error"] = str(e)

    return jsonify(result)


# ── Standings ─────────────────────────────────────────────────────────────────

@app.route("/standings")
def standings():
    teams = _cache.get("teams", [])
    if not teams:
        return jsonify({"error": "No data yet — call /refresh first"}), 503

    by_league = {}
    for league_name in LEAGUES:
        lg = [t for t in teams if t.get("league") == league_name]
        if not lg: continue

        if league_name == "MLS":
            conferences = {"Eastern": [], "Western": []}
            for t in lg:
                conf = t.get("conference","")
                if conf in conferences:
                    conferences[conf].append(t)
            if any(conferences.values()):
                for conf_name in ["Eastern", "Western"]:
                    if not conferences[conf_name]: continue
                    by_league[f"MLS {conf_name}"] = [
                        build_team_row(t) for t in
                        sorted(conferences[conf_name],
                               key=lambda x: safe_float(x.get("table_pos", 99)))]
            else:
                by_league["MLS"] = [build_team_row(t) for t in
                                    sorted(lg, key=lambda x: safe_float(x.get("table_pos",99)))]
            continue

        by_league[league_name] = [build_team_row(t) for t in
                                  sorted(lg, key=lambda x: safe_float(x.get("table_pos",99)))]

    return jsonify({"last_updated": _cache["last_updated"], "standings": by_league,
                    "version": APP_VERSION})


# ── Confirmed lineups ─────────────────────────────────────────────────────────

@app.route("/lineups/<match_id>")
def lineups(match_id):
    if not AF_KEY:
        return jsonify({"error": "API_FOOTBALL_KEY not configured"}), 503

    home    = request.args.get("home", "")
    away    = request.args.get("away", "")
    kickoff = request.args.get("kickoff", "")

    if not home or not away or not kickoff:
        return jsonify({"error": "home, away, kickoff required"}), 400

    cache_key = f"{home}_{away}_{kickoff[:10]}"
    if cache_key in _lineup_cache:
        return jsonify(_lineup_cache[cache_key])

    loop       = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    fixture_id = loop.run_until_complete(af_fixture_id(home, away, kickoff))

    if not fixture_id:
        loop.close()
        return jsonify({"confirmed": False,
                        "message": "Match not found on API-Football"}), 200

    lineup_data = loop.run_until_complete(af_lineups(fixture_id))
    loop.close()

    if not lineup_data:
        return jsonify({"confirmed": False,
                        "message": "Lineups not yet released",
                        "fixture_id": fixture_id}), 200

    result = {"confirmed": True, "fixture_id": fixture_id, "lineups": lineup_data}
    _lineup_cache[cache_key] = result
    log.info(f"Lineups confirmed: {home} vs {away} (fixture {fixture_id})")
    return jsonify(result)


# ── Live match ────────────────────────────────────────────────────────────────

@app.route("/live/<match_id>")
def live_match(match_id):
    async def fetch():
        from fotmob import FotMob
        fotmob = FotMob()
        try:    return await fotmob.get_match(int(match_id))
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

        home_data = data.get("home", {})
        away_data = data.get("away", {})
        home_id   = str(home_data.get("id",""))
        away_id   = str(away_data.get("id",""))

        evt_list = (data.get("incidents") or data.get("events") or data.get("goals") or [])
        if not isinstance(evt_list, list): evt_list = []

        events = []
        for e in evt_list:
            etype   = str(e.get("type","") or e.get("eventType","")).lower()
            if not etype: continue
            team_id = str(e.get("teamId","") or e.get("team",{}).get("id",""))
            side    = "home" if team_id == home_id else "away"

            if any(g in etype for g in ["goal","penalty"]) and "own" not in etype:
                events.append({"type": "goal",
                                "minute": e.get("timeStr") or e.get("min",""),
                                "player": e.get("playerName") or e.get("player",{}).get("name",""),
                                "assist": e.get("assistPlayerName") or e.get("assist",{}).get("name",""),
                                "side": side})
            elif "owngoal" in etype or "own_goal" in etype:
                events.append({"type": "owngoal",
                                "minute": e.get("timeStr") or e.get("min",""),
                                "player": e.get("playerName") or e.get("player",{}).get("name",""),
                                "side": side})
            elif "yellow" in etype or "card" in etype:
                card = "red" if "red" in etype or e.get("isRed") else "yellow"
                events.append({"type": card,
                                "minute": e.get("timeStr") or e.get("min",""),
                                "player": e.get("playerName") or e.get("player",{}).get("name",""),
                                "side": side})
            elif "sub" in etype or "substitut" in etype:
                events.append({"type": "sub",
                                "minute": e.get("timeStr") or e.get("min",""),
                                "player": e.get("playerName") or e.get("player",{}).get("name",""),
                                "playerOut": e.get("playerOutName") or e.get("playerOut",{}).get("name",""),
                                "side": side})

        stats_block = data.get("stats", {})
        stats_raw   = stats_block.get("stats", []) if isinstance(stats_block, dict) else []
        stats_out   = {}
        for stat in stats_raw:
            if not isinstance(stat, dict): continue
            title = (stat.get("title","") or "").lower()
            vals  = stat.get("stats", [])
            if not isinstance(vals, list) or len(vals) < 2: continue
            h, a = safe_float(vals[0]), safe_float(vals[1])
            if "possession" in title:      stats_out["possession"] = [h, a]
            elif "shot" in title and "target" in title: stats_out["shots_on_target"] = [h, a]
            elif "shot" in title and "target" not in title and "block" not in title:
                stats_out["shots"] = [h, a]
            elif "xg" in title or ("expected" in title and "goal" in title):
                stats_out["xg"] = [round(h,2), round(a,2)]

        team_colors = stats_block.get("teamColors", {}) if isinstance(stats_block, dict) else {}
        dark_colors = team_colors.get("darkMode", {})
        home_color  = dark_colors.get("home") or team_colors.get("home")
        away_color  = dark_colors.get("away") or team_colors.get("away")
        if home_color and not home_color.startswith("#"): home_color = f"#{home_color}"
        if away_color and not away_color.startswith("#"): away_color = f"#{away_color}"

        live_time  = data.get("liveTime", {}) or {}
        minute     = live_time.get("short","") or live_time.get("long","")

        return jsonify({
            "events":     events,
            "stats":      stats_out,
            "minute":     minute,
            "score":      [int(safe_float(home_data.get("score",0))),
                           int(safe_float(away_data.get("score",0)))],
            "home_color": home_color,
            "away_color": away_color,
        })
    except Exception as e:
        log.error(f"live_match {match_id}: {e}")
        return jsonify({"error": str(e)}), 500


# ── Player detail ─────────────────────────────────────────────────────────────

@app.route("/player/<player_id>")
def player_detail(player_id):
    team_id     = request.args.get("team_id", "")
    player_name = request.args.get("name", "")

    if not team_id:
        return jsonify({"error": "team_id required"}), 400

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

        return jsonify({
            "player_id":        player_id,
            "player_name":      player_name,
            "l5_games":         l5_games,
            "l5_goals_total":   sum(g.get("goals",0) for g in l5_games),
            "l5_assists_total": sum(g.get("assists",0) for g in l5_games),
            "base_stats":       player_data,
        })
    except Exception as e:
        log.error(f"player_detail {player_id}: {e}")
        return jsonify({"error": str(e)}), 500


# ── Debug routes ──────────────────────────────────────────────────────────────

@app.route("/debug/stats/<league_id>/<season_id>")
def debug_stats(league_id, season_id):
    from .utils import HEADERS
    GOAL_STATS_TO_TEST = [
        "goals","goals_per_90","expected_goals","expected_goals_per_90",
        "expected_goalsontarget","ontarget_scoring_att","total_scoring_att",
        "big_chance_missed","penalty_scored","penalty_won",
    ]
    import aiohttp

    async def fetch_all():
        results = {}
        async with aiohttp.ClientSession() as session:
            for stat in GOAL_STATS_TO_TEST:
                url = f"https://data.fotmob.com/stats/{league_id}/season/{season_id}/{stat}.json"
                try:
                    async with session.get(url, headers=HEADERS,
                                           timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status == 200:
                            d   = await resp.json(content_type=None)
                            top = d.get("TopLists",[{}])[0].get("StatList",[])
                            s   = top[0] if top else {}
                            results[stat] = {"available": True, "count": len(top),
                                             "sample_player": s.get("ParticipantName",""),
                                             "sample_value":  s.get("StatValue","")}
                        else:
                            results[stat] = {"available": False, "status": resp.status}
                except Exception as e:
                    results[stat] = {"available": False, "error": str(e)}
        return results

    loop    = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(fetch_all())
    loop.close()
    return jsonify({"available": [s for s, v in results.items() if v.get("available")],
                    "details": results})


@app.route("/debug/fixtures/<date>")
def debug_fixtures(date):
    from .lineups import AF_BASE, AF_HEADERS
    import aiohttp

    async def fetch():
        url = f"{AF_BASE}/fixtures"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=AF_HEADERS, params={"date": date, "timezone": "UTC"},
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return {"error": f"HTTP {resp.status}"}
                d = await resp.json(content_type=None)
        return [{"id": f["fixture"]["id"],
                 "home": f["teams"]["home"]["name"],
                 "away": f["teams"]["away"]["name"],
                 "league": f["league"]["name"]}
                for f in d.get("response", [])]

    loop   = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(fetch())
    loop.close()
    return jsonify(result)
# ── NEW ROUTES ────────────────────────────────────────────────────────────────
from .positional_concessions import bootstrap_season, update_after_match, get_multipliers
from .sm_baseline import bootstrap_baselines, refresh_baselines
from .sm_scorer import score_todays_fixtures
from .pipeline_comparison import build_comparison_for_date, record_outcomes, get_running_totals

@app.route('/api/concessions/bootstrap', methods=['POST'])
def concessions_bootstrap():
    data = request.json
    bootstrap_season(data['season_id'], data['league_id'])
    return jsonify({"status": "ok"})

@app.route('/api/baseline/bootstrap', methods=['POST'])
def baseline_bootstrap():
    import threading
    thread = threading.Thread(target=bootstrap_baselines)
    thread.daemon = True
    thread.start()
    return jsonify({"status": "ok", "message": "Bootstrap started in background"})

@app.route('/api/sm/score-today', methods=['POST'])
def sm_score_today():
    score_todays_fixtures()
    return jsonify({"status": "ok"})

@app.route('/api/comparison/build', methods=['POST'])
def comparison_build():
    build_comparison_for_date()
    return jsonify({"status": "ok"})

@app.route('/api/comparison/outcomes', methods=['POST'])
def comparison_outcomes():
    record_outcomes()
    return jsonify({"status": "ok"})

@app.route('/api/comparison/results', methods=['GET'])
def comparison_results():
    get_running_totals()
    return jsonify({"status": "ok"})

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Deep Current Football API v{APP_VERSION} starting on port {port}")
    app.run(host="0.0.0.0", port=port)
