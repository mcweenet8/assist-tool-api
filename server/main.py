# =============================================================================
# main.py — Deep Current Football API v4 — SM Only
# =============================================================================

import os, logging, threading
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

from .utils import _cache, APP_VERSION, safe_float

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ── Sportmonks imports ────────────────────────────────────────────────────────

from .positional_concessions import bootstrap_season, update_after_match, get_multipliers
from .sm_baseline import bootstrap_baselines, refresh_baselines
from .sm_scorer import score_todays_fixtures, get_latest_scores, get_season_scores
from .pipeline_comparison import build_comparison_for_date, record_outcomes, get_running_totals
from .sm_fixtures import get_sm_fixtures


# ── Health / version ──────────────────────────────────────────────────────────

@app.route("/")
@app.route("/status")
def status():
    return jsonify({
        "status":          _cache.get("status", "never_run"),
        "last_updated":    _cache.get("last_updated"),
        "refresh_started": _cache.get("refresh_started"),
        "version":         APP_VERSION,
        "source":          "sportmonks",
    })


@app.route("/version")
def version():
    return jsonify({
        "version":    APP_VERSION,
        "name":       "Deep Current Football API",
        "status":     _cache.get("status", "never_run"),
        "built_with": "sportmonks + flask + railway",
    })


# ── Fixtures ──────────────────────────────────────────────────────────────────

@app.route("/fixtures")
def fixtures():
    cached = _cache.get("fixtures")
    if not cached:
        # Trigger background fetch and return empty — app will retry
        def bg():
            try:
                f = get_sm_fixtures(days=7)
                _cache["fixtures"] = f
                _cache["fixtures_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                _cache["status"] = "ok"
                _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            except Exception as e:
                log.error(f"bg fixtures: {e}")
        threading.Thread(target=bg, daemon=True).start()
        return jsonify({"fixtures": {}, "last_updated": "", "loading": True})
    return jsonify({
        "last_updated": _cache.get("fixtures_last_updated", _cache.get("last_updated", "")),
        "fixtures":     cached,
    })


# ── Refresh ───────────────────────────────────────────────────────────────────

@app.route("/refresh", methods=["GET", "POST"])
def refresh():
    _cache["status"]          = "refreshing"
    _cache["refresh_started"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def run_sm_refresh():
        try:
            # Set ok immediately so app stops spinning
            _cache["status"]       = "ok"
            _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")

            # 1. Refresh SM fixtures
            log.info("SM fixtures refresh starting...")
            fix = get_sm_fixtures(days=7)
            _cache["fixtures"] = fix
            _cache["fixtures_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            log.info(f"SM fixtures: {sum(len(v) for v in fix.values())} total")

            # 2. Score today + snapshot
            score_todays_fixtures()
            build_comparison_for_date()

            # 3. Refresh standings
            try:
                import requests as req
                token = os.environ.get("SPORTMONKS_API_TOKEN")
                base  = "https://api.sportmonks.com/v3/football"
                LEAGUES_S = [
                    {"season_id": 25583, "name": "Premier League"},
                    {"season_id": 25648, "name": "Championship"},
                    {"season_id": 25659, "name": "La Liga"},
                    {"season_id": 25533, "name": "Serie A"},
                    {"season_id": 25646, "name": "Bundesliga"},
                    {"season_id": 25651, "name": "Ligue 1"},
                    {"season_id": 26720, "name": "MLS"},
                    {"season_id": 26529, "name": "A-League Men"},
                ]
                GP=129;W=130;L=131;D=132;GF=133;GA=134;CS=135;PTS=187
                def gv(dets,tid):
                    for d in dets:
                        if d.get("type_id")==tid: return d.get("value",0)
                    return 0
                standings_result = {}
                for lg in LEAGUES_S:
                    r = req.get(f"{base}/standings/seasons/{lg['season_id']}",
                        params={"api_token":token,"include":"participant;details"},timeout=30)
                    if r.status_code!=200: continue
                    rows=r.json().get("data",[])
                    teams=[]
                    for row in rows:
                        p=row.get("participant",{});dets=row.get("details",[])
                        gp=gv(dets,GP);gf=gv(dets,GF);ga=gv(dets,GA)
                        teams.append({"position":row.get("position"),"team_id":str(p.get("id","")),"team":p.get("name",""),"short_code":p.get("short_code",""),"logo":p.get("image_path",""),"played":gp,"wins":gv(dets,W),"draws":gv(dets,D),"losses":gv(dets,L),"goals_for":gf,"goals_against":ga,"clean_sheets":gv(dets,CS),"points":row.get("points",0),"gf_pg":round(gf/gp,2) if gp else 0,"ga_pg":round(ga/gp,2) if gp else 0,"goal_diff":gf-ga,"weak_def":(round(ga/gp,2) if gp else 0)>=1.5,"league":lg["name"]})
                    teams.sort(key=lambda x:x["position"] or 99)
                    standings_result[lg["name"]]=teams
                _cache["standings"]=standings_result
                _cache["standings_last_updated"]=datetime.now().strftime("%Y-%m-%d %H:%M")
                log.info("Standings refreshed")
            except Exception as e:
                log.error(f"standings refresh error: {e}")

            _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            log.info("SM refresh complete")
        except Exception as e:
            _cache["status"] = f"error: {str(e)}"
            log.error(f"SM refresh failed: {e}")

    threading.Thread(target=run_sm_refresh, daemon=True).start()

    return jsonify({
        "success":  True,
        "message":  "SM refresh started in background",
        "version":  APP_VERSION,
    })


# ── Standings (stub — returns empty until SM standings built) ─────────────────

@app.route("/standings")
def standings():
    return jsonify({
        "last_updated": _cache.get("last_updated", ""),
        "standings":    {},
        "version":      APP_VERSION,
    })


# ── SM Player data ────────────────────────────────────────────────────────────

@app.route('/api/sm/data', methods=['GET'])
def sm_data():
    return jsonify(get_latest_scores())


@app.route('/api/sm/season', methods=['GET'])
def sm_season():
    return jsonify(get_season_scores())


@app.route('/api/sm/fixtures', methods=['GET'])
def sm_fixtures_route():
    cached = _cache.get("fixtures")
    if cached:
        return jsonify({
            "fixtures":     cached,
            "last_updated": _cache.get("fixtures_last_updated", ""),
            "source":       "sportmonks",
        })
    return jsonify({"fixtures": {}, "last_updated": "", "source": "sportmonks", "loading": True})


@app.route('/api/sm/score-today', methods=['POST'])
def sm_score_today():
    score_todays_fixtures()
    return jsonify({"status": "ok"})


@app.route('/api/sm/refresh-today', methods=['POST'])
def sm_refresh_today():
    def run():
        score_todays_fixtures()
        build_comparison_for_date()
        try:
            fix = get_sm_fixtures(days=7)
            _cache["fixtures"] = fix
            _cache["fixtures_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        except Exception as e:
            log.error(f"SM refresh-today fixtures: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "ok", "message": "SM refresh started in background"})


# League → season_id mapping for team stats lookup
LEAGUE_SEASON_MAP = {
    8:    25583,  # Premier League
    9:    25648,  # Championship
    564:  25659,  # La Liga
    384:  25533,  # Serie A
    82:   25646,  # Bundesliga
    301:  25651,  # Ligue 1
    779:  26720,  # MLS
    1356: 26529,  # A-League Men
}

def _get_team_ha_stats(team_id, season_id):
    """
    Pull home/away splits for a team from Sportmonks team statistics.
    Returns dict with gf/ga home/away splits and per-game rates.
    """
    import requests as req
    token = os.environ.get("SPORTMONKS_API_TOKEN")
    base  = "https://api.sportmonks.com/v3/football"

    try:
        r = req.get(
            f"{base}/statistics/seasons/teams/{team_id}",
            params={"api_token": token},
            timeout=15
        )
        if r.status_code != 200:
            return {}

        records = r.json().get("data", [])
        # Find the correct season
        season_record = next((rec for rec in records if rec.get("season_id") == season_id and rec.get("has_values")), None)
        if not season_record:
            return {}

        details = {d["type_id"]: d["value"] for d in season_record.get("details", [])}

        def get_count(type_id, scope="all"):
            val = details.get(type_id, {})
            if isinstance(val, dict):
                return val.get(scope, {}).get("count", 0) or val.get(scope, {}).get("average", 0) or 0
            return 0

        def get_avg(type_id, scope="all"):
            val = details.get(type_id, {})
            if isinstance(val, dict):
                return val.get(scope, {}).get("average", 0) or 0
            return 0

        # Goals for (52) and goals against (88)
        gf_home = get_count(52, "home")
        gf_away = get_count(52, "away")
        gf_all  = get_count(52, "all")
        ga_home = get_count(88, "home")
        ga_away = get_count(88, "away")
        ga_all  = get_count(88, "all")

        # Wins (214), Losses (192), Clean sheets (194)
        wins_home   = get_count(214, "home")
        wins_away   = get_count(214, "away")
        losses_home = get_count(192, "home")
        losses_away = get_count(192, "away")
        cs_home     = get_count(194, "home")
        cs_away     = get_count(194, "away")

        # Calculate games played home/away from wins+draws+losses
        # Use overall played from standings cache as fallback
        # Approximate home/away games as total/2
        total_games = gf_all  # proxy
        home_games  = wins_home + losses_home + (get_count(214, "all") - wins_home - wins_away)  # rough
        # Simpler — use average goals which SM provides
        gf_pg_home = get_avg(52, "home")
        gf_pg_away = get_avg(52, "away")
        ga_pg_home = get_avg(88, "home")
        ga_pg_away = get_avg(88, "away")

        return {
            "gf_all":       gf_all,
            "gf_home":      gf_home,
            "gf_away":      gf_away,
            "ga_all":       ga_all,
            "ga_home":      ga_home,
            "ga_away":      ga_away,
            "gf_pg_home":   round(gf_pg_home, 2),
            "gf_pg_away":   round(gf_pg_away, 2),
            "ga_pg_home":   round(ga_pg_home, 2),
            "ga_pg_away":   round(ga_pg_away, 2),
            "wins_home":    wins_home,
            "wins_away":    wins_away,
            "losses_home":  losses_home,
            "losses_away":  losses_away,
            "cs_home":      cs_home,
            "cs_away":      cs_away,
            "weak_def_home": ga_pg_home >= 1.5,
            "weak_def_away": ga_pg_away >= 1.5,
        }
    except Exception as e:
        log.error(f"team ha stats error {team_id}: {e}")
        return {}


@app.route('/api/sm/match/<int:fixture_id>', methods=['GET'])
def sm_match(fixture_id):
    """
    Return home + away squad DC scores + home/away stats for a fixture.
    """
    try:
        # Find fixture in cache
        fixtures = _cache.get("fixtures", {})
        home_id = away_id = home_name = away_name = league_id = None
        for league, matches in fixtures.items():
            for m in matches:
                if str(m.get("match_id")) == str(fixture_id):
                    home_id    = m.get("home_id")
                    away_id    = m.get("away_id")
                    home_name  = m.get("home")
                    away_name  = m.get("away")
                    # Find league_id from league name
                    for lid, lname in {8:"Premier League",9:"Championship",564:"La Liga",384:"Serie A",82:"Bundesliga",301:"Ligue 1",779:"MLS",1356:"A-League Men"}.items():
                        if lname == league:
                            league_id = lid
                            break
                    break
            if home_id:
                break

        if not home_id or not away_id:
            return jsonify({"error": "Fixture not found in cache — hit Refresh"}), 404

        # Get season_id for this league
        season_id = LEAGUE_SEASON_MAP.get(league_id)

        # Pull squad DC scores
        season_data = get_season_scores()
        all_players = season_data.get("players", [])
        home_players = sorted([p for p in all_players if str(p.get("team_id")) == str(home_id)], key=lambda x: x.get("tsoa_score") or 0, reverse=True)
        away_players = sorted([p for p in all_players if str(p.get("team_id")) == str(away_id)], key=lambda x: x.get("tsoa_score") or 0, reverse=True)

        # Pull home/away stats on demand
        home_ha = _get_team_ha_stats(home_id, season_id) if season_id else {}
        away_ha = _get_team_ha_stats(away_id, season_id) if season_id else {}

        return jsonify({
            "fixture_id":   fixture_id,
            "home":         home_name,
            "away":         away_name,
            "home_id":      home_id,
            "away_id":      away_id,
            "home_players": home_players[:15],
            "away_players": away_players[:15],
            "total_home":   len(home_players),
            "total_away":   len(away_players),
            "home_ha":      home_ha,
            "away_ha":      away_ha,
        })

    except Exception as e:
        log.error(f"sm_match {fixture_id}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/sm/standings', methods=['GET'])
def sm_standings():
    """
    Return league standings for all tracked leagues from Sportmonks.
    Cached for 1 hour — refreshed on /refresh call.
    """
    # Serve from cache if available
    if _cache.get("standings"):
        return jsonify({
            "standings":    _cache["standings"],
            "last_updated": _cache.get("standings_last_updated", ""),
            "source":       "sportmonks",
        })

    # Build standings in background and return empty
    import threading
    def fetch_standings():
        try:
            import requests as req
            token = os.environ.get("SPORTMONKS_API_TOKEN")
            base  = "https://api.sportmonks.com/v3/football"

            LEAGUES = [
                {"league_id": 8,    "season_id": 25583, "name": "Premier League"},
                {"league_id": 9,    "season_id": 25648, "name": "Championship"},
                {"league_id": 564,  "season_id": 25659, "name": "La Liga"},
                {"league_id": 384,  "season_id": 25533, "name": "Serie A"},
                {"league_id": 82,   "season_id": 25646, "name": "Bundesliga"},
                {"league_id": 301,  "season_id": 25651, "name": "Ligue 1"},
                {"league_id": 779,  "season_id": 26720, "name": "MLS"},
                {"league_id": 1356, "season_id": 26529, "name": "A-League Men"},
            ]

            # Standing detail type_ids
            GP  = 129  # games played
            W   = 130  # wins
            L   = 131  # losses
            D   = 132  # draws (home)
            GF  = 133  # goals for
            GA  = 134  # goals against
            CS  = 135  # clean sheets
            PTS = 187  # points

            def get_val(details, type_id):
                for d in details:
                    if d.get("type_id") == type_id:
                        return d.get("value", 0)
                return 0

            result = {}

            for league in LEAGUES:
                r = req.get(
                    f"{base}/standings/seasons/{league['season_id']}",
                    params={"api_token": token, "include": "participant;details"},
                    timeout=30
                )
                if r.status_code != 200:
                    continue

                rows = r.json().get("data", [])
                teams = []
                for row in rows:
                    p    = row.get("participant", {})
                    dets = row.get("details", [])
                    gp   = get_val(dets, GP)
                    gf   = get_val(dets, GF)
                    ga   = get_val(dets, GA)
                    ga_pg = round(ga / gp, 2) if gp else 0
                    gf_pg = round(gf / gp, 2) if gp else 0
                    teams.append({
                        "position":   row.get("position"),
                        "team_id":    str(p.get("id","")),
                        "team":       p.get("name",""),
                        "short_code": p.get("short_code",""),
                        "logo":       p.get("image_path",""),
                        "played":     gp,
                        "wins":       get_val(dets, W),
                        "draws":      get_val(dets, D),
                        "losses":     get_val(dets, L),
                        "goals_for":  gf,
                        "goals_against": ga,
                        "clean_sheets":  get_val(dets, CS),
                        "points":     row.get("points", 0),
                        "gf_pg":      gf_pg,
                        "ga_pg":      ga_pg,
                        "goal_diff":  gf - ga,
                        "weak_def":   ga_pg >= 1.5,
                        "league_id":  league["league_id"],
                        "league":     league["name"],
                    })

                teams.sort(key=lambda x: x["position"] or 99)
                result[league["name"]] = teams
                log.info(f"Standings loaded: {league['name']} ({len(teams)} teams)")

            _cache["standings"]             = result
            _cache["standings_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            log.info("All standings cached")

        except Exception as e:
            log.error(f"standings fetch error: {e}")

    threading.Thread(target=fetch_standings, daemon=True).start()
    return jsonify({"standings": {}, "last_updated": "", "source": "sportmonks", "loading": True})


# ── Baseline / concessions bootstrap ─────────────────────────────────────────

@app.route('/api/baseline/bootstrap', methods=['POST'])
def baseline_bootstrap():
    threading.Thread(target=bootstrap_baselines, daemon=True).start()
    return jsonify({"status": "ok", "message": "Bootstrap started in background"})


@app.route('/api/concessions/bootstrap', methods=['POST'])
def concessions_bootstrap():
    data = request.json
    bootstrap_season(data['season_id'], data['league_id'])
    return jsonify({"status": "ok"})


# ── Comparison / outcomes ─────────────────────────────────────────────────────

@app.route('/api/comparison/build', methods=['POST'])
def comparison_build():
    data      = request.json or {}
    game_date = data.get('date', None)
    threading.Thread(target=build_comparison_for_date, args=(game_date,), daemon=True).start()
    return jsonify({"status": "ok", "message": "Comparison build started in background"})


@app.route('/api/comparison/outcomes', methods=['POST'])
def comparison_outcomes():
    data      = request.json or {}
    game_date = data.get('date', None)
    threading.Thread(target=record_outcomes, args=(game_date,), daemon=True).start()
    return jsonify({"status": "ok", "message": "Outcomes recording started in background"})


@app.route('/api/comparison/results', methods=['GET'])
def comparison_results():
    return jsonify(get_running_totals())


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Deep Current Football API v{APP_VERSION} starting on port {port}")
    app.run(host="0.0.0.0", port=port)
