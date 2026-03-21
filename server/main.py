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

from .positional_concessions import bootstrap_season, update_after_match, get_multipliers, process_fixture
from .sm_baseline import bootstrap_baselines, refresh_baselines
from .sm_scorer import score_todays_fixtures, get_latest_scores, get_season_scores
from .pipeline_comparison import build_comparison_for_date, record_outcomes, get_running_totals
from .sm_fixtures import get_sm_fixtures


# ── Boot — pre-warm season scores cache ──────────────────────────────────────

def _prewarm_cache():
    try:
        log.info("Pre-warming season scores cache...")
        _cache["season_scores"] = get_season_scores()
        _cache["season_scores_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        log.info(f"Season scores cached: {_cache['season_scores'].get('count', 0)} players")
    except Exception as e:
        log.error(f"Pre-warm error: {e}")

threading.Thread(target=_prewarm_cache, daemon=True).start()


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
            _cache["status"]       = "ok"
            _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")

            log.info("SM fixtures refresh starting...")
            fix = get_sm_fixtures(days=7)
            _cache["fixtures"] = fix
            _cache["fixtures_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            log.info(f"SM fixtures: {sum(len(v) for v in fix.values())} total")

            score_todays_fixtures()
            build_comparison_for_date()

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

            try:
                _cache["season_scores"] = get_season_scores()
                _cache["season_scores_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                log.info("Season scores cache refreshed")
            except Exception as e:
                log.error(f"Season scores cache error: {e}")

            _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            log.info("SM refresh complete")
        except Exception as e:
            _cache["status"] = f"error: {str(e)}"
            log.error(f"SM refresh failed: {e}")

    threading.Thread(target=run_sm_refresh, daemon=True).start()
    return jsonify({"success": True, "message": "SM refresh started in background", "version": APP_VERSION})


@app.route('/api/sm/today-context', methods=['GET'])
def sm_today_context():
    try:
        from .positional_concessions import apply_concession_multiplier, GRANULAR_POSITION_MAP, BROAD_MAP
        from supabase import create_client
        import pytz

        sb = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY"))

        fixtures    = _cache.get("fixtures", {})
        season_data = _cache.get("season_scores", {})
        players     = season_data.get("players", [])
        today       = datetime.now(pytz.timezone("America/New_York")).strftime("%Y-%m-%d")

        LEAGUE_SEASON_MAP_LOCAL = {
            8:25583, 9:25648, 564:25659, 384:25533,
            82:25646, 301:25651, 779:26720, 1356:26529
        }
        LEAGUE_NAME_MAP = {
            "Premier League":8,"Championship":9,"La Liga":564,"Serie A":384,
            "Bundesliga":82,"Ligue 1":301,"MLS":779,"A-League Men":1356
        }

        # Absolute thresholds (same as positional_concessions.py)
        ABS_GOAL_THRESH   = {"GK": 99, "DEF": 0.27, "MID": 0.75, "FWD": 0.85}
        ABS_ASSIST_THRESH = {"GK": 99, "DEF": 0.30, "MID": 0.70, "FWD": 0.40}
        THRESHOLD_HIGH    = 2.0
        THRESHOLD_MEDIUM  = 1.5

        BROAD_POSITION_MAP = {24:"GK", 25:"DEF", 26:"MID", 27:"FWD"}

        # Find today's fixtures from cache
        today_fixtures = []
        for league_name, matches in fixtures.items():
            league_id = LEAGUE_NAME_MAP.get(league_name)
            if not league_id: continue
            season_id = LEAGUE_SEASON_MAP_LOCAL.get(league_id)
            if not season_id: continue
            for m in matches:
                ko = m.get("kickoff", "")
                if not ko: continue
                try:
                    ko_dt      = datetime.fromisoformat(ko.replace("Z", "+00:00"))
                    local_date = ko_dt.astimezone(pytz.timezone("America/New_York")).strftime("%Y-%m-%d")
                except:
                    local_date = ko[:10]
                if local_date == today or m.get("live"):
                    today_fixtures.append({
                        **m,
                        "league_id": league_id,
                        "season_id": season_id,
                    })

        log.info(f"today-context: today={today} cache_leagues={list(fixtures.keys())} today_fixtures={len(today_fixtures)}")

        if not today_fixtures:
            return jsonify({"context": {}, "count": 0})

        # Get all relevant league averages in one query
        season_ids = list({f["season_id"] for f in today_fixtures})
        avg_rows   = sb.table("positional_concessions_league_avg")\
            .select("*").eq("granularity", "broad")\
            .in_("season_id", season_ids).execute().data

        # Build avg lookup: season_id -> broad_position -> row
        avg_map = {}
        for row in avg_rows:
            sid = row["season_id"]
            if sid not in avg_map: avg_map[sid] = {}
            avg_map[sid][row["broad_position"]] = row

        # Get all team concession data in one query per season
        team_ids_by_season = {}
        for f in today_fixtures:
            sid = f["season_id"]
            if sid not in team_ids_by_season: team_ids_by_season[sid] = set()
            if f.get("home_id"): team_ids_by_season[sid].add(int(f["home_id"]))
            if f.get("away_id"): team_ids_by_season[sid].add(int(f["away_id"]))

        broad_map = {}  # (team_id, season_id, broad_pos) -> row
        for sid, tids in team_ids_by_season.items():
            rows = sb.table("positional_concessions_broad")\
                .select("*").eq("season_id", sid)\
                .in_("team_id", list(tids)).execute().data
            for row in rows:
                broad_map[(row["team_id"], sid, row["broad_position"])] = row

        # Build multipliers per team per season inline
        def get_team_multipliers(team_id, season_id):
            result = {"broad": {}}
            avgs   = avg_map.get(season_id, {})
            for bp in ["GK", "DEF", "MID", "FWD"]:
                row = broad_map.get((int(team_id), season_id, bp))
                if not row: continue
                avg = avgs.get(bp, {})
                gp  = row["games_played"] or 1
                gpg = row["goals_conceded"]   / gp
                apg = row["assists_conceded"] / gp
                avg_g = avg.get("avg_goals_per_game",   0.001) or 0.001
                avg_a = avg.get("avg_assists_per_game", 0.001) or 0.001

                goal_mult   = gpg / avg_g
                assist_mult = apg / avg_a

                abs_goal   = gpg >= ABS_GOAL_THRESH.get(bp, 99)
                abs_assist = apg >= ABS_ASSIST_THRESH.get(bp, 99)

                flag = None
                if goal_mult >= THRESHOLD_HIGH or assist_mult >= THRESHOLD_HIGH or abs_goal or abs_assist:
                    flag = "HIGH"
                elif goal_mult >= THRESHOLD_MEDIUM or assist_mult >= THRESHOLD_MEDIUM:
                    flag = "MEDIUM"

                result["broad"][bp] = {
                    "goal_multiplier":   round(goal_mult, 2),
                    "assist_multiplier": round(assist_mult, 2),
                    "flag":              flag,
                }
            return result

        context = {}

        for fixture in today_fixtures:
            home_id    = fixture.get("home_id")
            away_id    = fixture.get("away_id")
            fixture_id = fixture.get("match_id")
            season_id  = fixture["season_id"]

            if not home_id or not away_id: continue

            for team_id, opponent_id in [(home_id, away_id), (away_id, home_id)]:
                opponent_mults = get_team_multipliers(opponent_id, season_id)
                if not opponent_mults["broad"]: continue

                team_players = [p for p in players if str(p.get("team_id")) == str(team_id)]

                for player in team_players:
                    pid          = str(player.get("player_id"))
                    detailed_pos = player.get("detailed_position_id")
                    pos_code     = GRANULAR_POSITION_MAP.get(detailed_pos, (None, None))[0]
                    if not pos_code: continue

                    broad = BROAD_MAP.get(pos_code, "MID")
                    broad_data = opponent_mults["broad"].get(broad, {})
                    flag = broad_data.get("flag")
                    if not flag: continue

                    assist_mult = broad_data.get("assist_multiplier", 1.0)
                    goal_mult   = broad_data.get("goal_multiplier", 1.0)
                    mult        = round(max(assist_mult, goal_mult), 2)

                    context[pid] = {
                        "concession_flag":       flag,
                        "concession_multiplier": mult,
                        "opponent_id":           opponent_id,
                        "fixture_id":            fixture_id,
                    }

        return jsonify({"context": context, "count": len(context)})

    except Exception as e:
        log.error(f"today-context error: {e}")
        return jsonify({"context": {}, "count": 0, "error": str(e)})


@app.route('/api/sm/season/refresh', methods=['POST'])
def sm_season_refresh():
    def run():
        _cache["season_scores"] = get_season_scores()
        _cache["season_scores_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        log.info("Season scores cache force refreshed")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "ok", "message": "Season scores refreshing"})


@app.route("/standings")
def standings():
    return jsonify({"last_updated": _cache.get("last_updated", ""), "standings": {}, "version": APP_VERSION})


# ── SM Player data ────────────────────────────────────────────────────────────

@app.route('/api/sm/data', methods=['GET'])
def sm_data():
    return jsonify(get_latest_scores())


@app.route('/api/sm/season', methods=['GET'])
def sm_season():
    if not _cache.get("season_scores"):
        _cache["season_scores"] = get_season_scores()
        _cache["season_scores_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    return jsonify(_cache["season_scores"])


@app.route('/api/sm/fixtures', methods=['GET'])
def sm_fixtures_route():
    cached = _cache.get("fixtures")
    if cached:
        return jsonify({"fixtures": cached, "last_updated": _cache.get("fixtures_last_updated", ""), "source": "sportmonks"})
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


# ── Match Day Results ─────────────────────────────────────────────────────────

@app.route('/api/sm/results', methods=['GET'])
def sm_results():
    try:
        from supabase import create_client
        import statistics
        from collections import defaultdict

        sb = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY"))

        rows = sb.table("sm_matchday_results")\
            .select("*")\
            .eq("outcome_recorded", True)\
            .order("game_date", desc=True)\
            .execute().data

        if not rows:
            return jsonify({"results": [], "dates": [], "summaries": {}})

        by_date = defaultdict(list)
        for r in rows:
            by_date[r["game_date"]].append(r)

        summaries = {}
        for date, date_rows in by_date.items():
            contributors = [r for r in date_rows if r.get("had_contribution")]
            assist_ranks = [r["sm_assist_rank"] for r in contributors if r.get("sm_assist_rank")]
            goal_ranks   = [r["sm_goal_rank"]   for r in contributors if r.get("sm_goal_rank")]
            tsoa_ranks   = [r["sm_tsoa_rank"]   for r in contributors if r.get("sm_tsoa_rank")]

            def median(lst):
                return round(statistics.median(lst), 1) if lst else None

            def top_n_rate(lst, n=20):
                if not lst: return None
                return round(sum(1 for r in lst if r <= n) / len(lst), 2)

            summaries[date] = {
                "total_fixtures":     len(set(r["fixture_id"] for r in date_rows)),
                "total_contributors": len(contributors),
                "assist": {"median_rank": median(assist_ranks), "top20_rate": top_n_rate(assist_ranks), "top20_count": sum(1 for r in assist_ranks if r <= 20), "total": len(assist_ranks)},
                "goal":   {"median_rank": median(goal_ranks),   "top20_rate": top_n_rate(goal_ranks),   "top20_count": sum(1 for r in goal_ranks   if r <= 20), "total": len(goal_ranks)},
                "tsoa":   {"median_rank": median(tsoa_ranks),   "top20_rate": top_n_rate(tsoa_ranks),   "top20_count": sum(1 for r in tsoa_ranks   if r <= 20), "total": len(tsoa_ranks)},
            }

        return jsonify({"results": rows, "dates": sorted(by_date.keys(), reverse=True), "summaries": summaries})

    except Exception as e:
        log.error(f"sm_results error: {e}")
        return jsonify({"results": [], "dates": [], "summaries": {}, "error": str(e)}), 500


# ── League → season_id mapping ────────────────────────────────────────────────

LEAGUE_SEASON_MAP = {
    8:    25583, 9:    25648, 564:  25659, 384:  25533,
    82:   25646, 301:  25651, 779:  26720, 1356: 26529,
}


def _get_team_ha_stats(team_id, season_id):
    import requests as req
    token = os.environ.get("SPORTMONKS_API_TOKEN")
    base  = "https://api.sportmonks.com/v3/football"
    try:
        r = req.get(f"{base}/statistics/seasons/teams/{team_id}", params={"api_token": token}, timeout=15)
        if r.status_code != 200: return {}
        records = r.json().get("data", [])
        season_record = next((rec for rec in records if rec.get("season_id") == season_id and rec.get("has_values")), None)
        if not season_record: return {}
        details = {d["type_id"]: d["value"] for d in season_record.get("details", [])}

        def get_count(type_id, scope="all"):
            val = details.get(type_id, {})
            if isinstance(val, dict): return val.get(scope, {}).get("count", 0) or val.get(scope, {}).get("average", 0) or 0
            return 0

        def get_avg(type_id, scope="all"):
            val = details.get(type_id, {})
            if isinstance(val, dict): return val.get(scope, {}).get("average", 0) or 0
            return 0

        return {
            "gf_all": get_count(52,"all"), "gf_home": get_count(52,"home"), "gf_away": get_count(52,"away"),
            "ga_all": get_count(88,"all"), "ga_home": get_count(88,"home"), "ga_away": get_count(88,"away"),
            "gf_pg_home": round(get_avg(52,"home"),2), "gf_pg_away": round(get_avg(52,"away"),2),
            "ga_pg_home": round(get_avg(88,"home"),2), "ga_pg_away": round(get_avg(88,"away"),2),
            "wins_home": get_count(214,"home"), "wins_away": get_count(214,"away"),
            "losses_home": get_count(192,"home"), "losses_away": get_count(192,"away"),
            "cs_home": get_count(194,"home"), "cs_away": get_count(194,"away"),
            "weak_def_home": get_avg(88,"home") >= 1.5, "weak_def_away": get_avg(88,"away") >= 1.5,
        }
    except Exception as e:
        log.error(f"team ha stats error {team_id}: {e}")
        return {}


@app.route('/api/sm/match/<int:fixture_id>', methods=['GET'])
def sm_match(fixture_id):
    try:
        fixtures = _cache.get("fixtures", {})
        home_id = away_id = home_name = away_name = league_id = None
        for league, matches in fixtures.items():
            for m in matches:
                if str(m.get("match_id")) == str(fixture_id):
                    home_id = m.get("home_id"); away_id = m.get("away_id")
                    home_name = m.get("home"); away_name = m.get("away")
                    for lid, lname in {8:"Premier League",9:"Championship",564:"La Liga",384:"Serie A",82:"Bundesliga",301:"Ligue 1",779:"MLS",1356:"A-League Men"}.items():
                        if lname == league: league_id = lid; break
                    break
            if home_id: break

        if not home_id or not away_id:
            return jsonify({"error": "Fixture not found in cache — hit Refresh"}), 404

        season_id   = LEAGUE_SEASON_MAP.get(league_id)
        season_data = _cache.get("season_scores") or get_season_scores()
        all_players = season_data.get("players", [])
        home_players = sorted([p for p in all_players if str(p.get("team_id")) == str(home_id)], key=lambda x: x.get("tsoa_score") or 0, reverse=True)
        away_players = sorted([p for p in all_players if str(p.get("team_id")) == str(away_id)], key=lambda x: x.get("tsoa_score") or 0, reverse=True)

        home_ha = _get_team_ha_stats(home_id, season_id) if season_id else {}
        away_ha = _get_team_ha_stats(away_id, season_id) if season_id else {}

        lineup_data = {"starters": [], "subs": [], "home_formation": None, "away_formation": None, "confirmed": False}
        try:
            import requests as req
            token = os.environ.get("SPORTMONKS_API_TOKEN")
            base  = "https://api.sportmonks.com/v3/football"
            r = req.get(f"{base}/fixtures/{fixture_id}", params={"api_token": token, "include": "lineups;formations"}, timeout=15)
            if r.status_code == 200:
                fdata = r.json().get("data", {})
                lineups = fdata.get("lineups", [])
                if isinstance(lineups, dict): lineups = lineups.get("data", [])
                formations = fdata.get("formations", [])
                if isinstance(formations, dict): formations = formations.get("data", [])
                starters  = [p for p in lineups if p.get("type_id") == 11]
                subs      = [p for p in lineups if p.get("type_id") == 12]
                score_map = {str(p["player_id"]): p for p in all_players}

                def enrich(player):
                    pid = str(player.get("player_id"))
                    dc  = score_map.get(pid, {})
                    return {**player, "assist_index": dc.get("assist_index"), "goal_score": dc.get("goal_score"), "tsoa_score": dc.get("tsoa_score"), "position_id": dc.get("position_id"), "detailed_position_id": dc.get("detailed_position_id")}

                lineup_data = {
                    "starters": [enrich(p) for p in starters], "subs": [enrich(p) for p in subs],
                    "home_formation": next((f["formation"] for f in formations if f.get("participant_id") == home_id), None),
                    "away_formation": next((f["formation"] for f in formations if f.get("participant_id") == away_id), None),
                    "confirmed": len(starters) >= 20,
                }
        except Exception as e:
            log.error(f"lineup pull error {fixture_id}: {e}")

        return jsonify({
            "fixture_id": fixture_id, "home": home_name, "away": away_name,
            "home_id": home_id, "away_id": away_id,
            "home_players": home_players[:15], "away_players": away_players[:15],
            "total_home": len(home_players), "total_away": len(away_players),
            "home_ha": home_ha, "away_ha": away_ha, "lineup": lineup_data,
        })

    except Exception as e:
        log.error(f"sm_match {fixture_id}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/sm/standings', methods=['GET'])
def sm_standings():
    if _cache.get("standings"):
        return jsonify({"standings": _cache["standings"], "last_updated": _cache.get("standings_last_updated", ""), "source": "sportmonks"})

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
            GP=129;W=130;L=131;D=132;GF=133;GA=134;CS=135;PTS=187
            def get_val(details, type_id):
                for d in details:
                    if d.get("type_id") == type_id: return d.get("value", 0)
                return 0
            result = {}
            for league in LEAGUES:
                r = req.get(f"{base}/standings/seasons/{league['season_id']}", params={"api_token": token, "include": "participant;details"}, timeout=30)
                if r.status_code != 200: continue
                rows = r.json().get("data", [])
                teams = []
                for row in rows:
                    p = row.get("participant", {}); dets = row.get("details", [])
                    gp = get_val(dets,GP); gf = get_val(dets,GF); ga = get_val(dets,GA)
                    ga_pg = round(ga/gp,2) if gp else 0; gf_pg = round(gf/gp,2) if gp else 0
                    teams.append({"position": row.get("position"), "team_id": str(p.get("id","")), "team": p.get("name",""), "short_code": p.get("short_code",""), "logo": p.get("image_path",""), "played": gp, "wins": get_val(dets,W), "draws": get_val(dets,D), "losses": get_val(dets,L), "goals_for": gf, "goals_against": ga, "clean_sheets": get_val(dets,CS), "points": row.get("points",0), "gf_pg": gf_pg, "ga_pg": ga_pg, "goal_diff": gf-ga, "weak_def": ga_pg>=1.5, "league_id": league["league_id"], "league": league["name"]})
                teams.sort(key=lambda x: x["position"] or 99)
                result[league["name"]] = teams
                log.info(f"Standings loaded: {league['name']} ({len(teams)} teams)")
            _cache["standings"] = result
            _cache["standings_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            log.info("All standings cached")
        except Exception as e:
            log.error(f"standings fetch error: {e}")

    threading.Thread(target=fetch_standings, daemon=True).start()
    return jsonify({"standings": {}, "last_updated": "", "source": "sportmonks", "loading": True})


@app.route('/api/sm/team-stats/<int:team_id>', methods=['GET'])
def sm_team_stats(team_id):
    try:
        season_id = request.args.get('season_id', type=int)
        if not season_id: return jsonify({"error": "season_id required"}), 400
        stats = _get_team_ha_stats(team_id, season_id)
        return jsonify({"stats": stats, "team_id": team_id, "season_id": season_id})
    except Exception as e:
        log.error(f"team_stats {team_id}: {e}")
        return jsonify({"error": str(e)}), 500


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


@app.route('/api/concessions/process-fixture', methods=['POST'])
def concessions_process_fixture():
    """
    Process a single fixture for positional concessions.
    Accepts: { fixture_id, season_id, league_id }
    Called by the Colab per-league bootstrap cell.
    """
    try:
        data       = request.json or {}
        fixture_id = data.get("fixture_id")
        season_id  = data.get("season_id")
        league_id  = data.get("league_id")

        if not fixture_id or not season_id or not league_id:
            return jsonify({"error": "fixture_id, season_id and league_id required"}), 400

        process_fixture(int(fixture_id), int(season_id), int(league_id))
        return jsonify({"status": "ok", "fixture_id": fixture_id})

    except Exception as e:
        log.error(f"process-fixture error: {e}")
        return jsonify({"error": str(e)}), 500


# ── Comparison / outcomes ─────────────────────────────────────────────────────

@app.route('/api/comparison/build', methods=['POST'])
def comparison_build():
    data = request.json or {}
    threading.Thread(target=build_comparison_for_date, args=(data.get('date'),), daemon=True).start()
    return jsonify({"status": "ok", "message": "Comparison build started in background"})


@app.route('/api/comparison/outcomes', methods=['POST'])
def comparison_outcomes():
    data = request.json or {}
    threading.Thread(target=record_outcomes, args=(data.get('date'),), daemon=True).start()
    return jsonify({"status": "ok", "message": "Outcomes recording started in background"})


@app.route('/api/comparison/results', methods=['GET'])
def comparison_results():
    return jsonify(get_running_totals())


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Deep Current Football API v{APP_VERSION} starting on port {port}")
    app.run(host="0.0.0.0", port=port)
