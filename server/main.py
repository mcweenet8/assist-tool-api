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


# ── Cache TTL helper ─────────────────────────────────────────────────────────

def _cache_valid(key, ttl_seconds):
    ts = _cache.get(f"{key}_ts")
    if not ts: return False
    return (datetime.now() - ts).total_seconds() < ttl_seconds

def _cache_set(key, value):
    _cache[key] = value
    _cache[f"{key}_ts"] = datetime.now()


# ── Sidelined helpers ────────────────────────────────────────────────────────

def get_sidelined_player_ids(team_ids=None):
    try:
        from supabase import create_client
        sb = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY"))
        q = sb.table("player_sidelined")            .select("player_id")            .eq("completed", False)            .gte("start_date", "2025-07-01")            .gte("games_missed", 5)
        if team_ids:
            q = q.in_("team_id", list(team_ids))
        rows = q.execute().data
        return {r["player_id"] for r in rows}
    except Exception as e:
        log.warning(f"get_sidelined_player_ids error: {e}")
        return set()

def get_sidelined_data(player_id):
    try:
        from supabase import create_client
        sb = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY"))
        rows = sb.table("player_sidelined")            .select("injury_type, start_date, end_date, games_missed, completed")            .eq("player_id", player_id)            .eq("completed", False)            .gte("start_date", "2025-07-01")            .order("start_date", desc=True)            .limit(1).execute().data
        return rows[0] if rows else None
    except Exception as e:
        log.warning(f"get_sidelined_data error: {e}")
        return None


# ── Boot — pre-warm season scores cache ──────────────────────────────────────

def _prewarm_cache():
    try:
        log.info("Pre-warming season scores cache...")
        # Check Redis first — survives restarts
        existing = _cache.get("season_scores")
        if existing and existing.get("players"):
            log.info(f"Season scores loaded from Redis: {existing.get('count', 0)} players")
            return
        log.info("Redis cold — computing season scores from Supabase...")
        result = get_season_scores()
        _cache.set("season_scores", result, ttl=86400)  # 24hr Redis TTL
        _cache["season_scores_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        log.info(f"Season scores cached: {result.get('count', 0)} players")
    except Exception as e:
        log.error(f"Pre-warm error: {e}")

threading.Thread(target=_prewarm_cache, daemon=True).start()


def _nightly_scheduler():
    """Background thread — fires nightly pipeline at 2:00 AM Eastern every day."""
    import time as _time
    import pytz as _pytz
    from datetime import timedelta

    eastern = _pytz.timezone("America/New_York")
    log.info("Nightly scheduler started — fires at 2:00 AM Eastern")

    while True:
        try:
            now    = datetime.now(_pytz.utc).astimezone(eastern)
            target = now.replace(hour=2, minute=0, second=0, microsecond=0)
            if now >= target:
                target = target + timedelta(days=1)
            seconds_until = (target - now).total_seconds()
            log.info(f"Nightly scheduler: next run in {round(seconds_until/3600,1)}h ({target.strftime('%Y-%m-%d %H:%M ET')})")
            _time.sleep(seconds_until)

            log.info("Nightly scheduler: triggering nightly pipeline...")
            try:
                import requests as _req
                railway_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN") or "web-production-0e482.up.railway.app"
                url = f"https://{railway_url}/api/nightly/run"
                _req.post(url, timeout=30)
                log.info(f"Nightly scheduler: triggered via {url}")
            except Exception as e:
                log.error(f"Nightly scheduler trigger error: {e}")

        except Exception as e:
            log.error(f"Nightly scheduler error: {e}")
            _time.sleep(60)

threading.Thread(target=_nightly_scheduler, daemon=True).start()


def _refresh_lineup_availability():
    """Background thread — refreshes lineup/sidelined availability every 5 minutes."""
    import time as _time
    import pytz as _pytz

    # Wait 90 seconds on startup to let fixtures load first
    _time.sleep(90)

    while True:
        try:
            fixtures  = _cache.get("fixtures", {})
            token     = os.environ.get("SPORTMONKS_API_TOKEN")
            if not fixtures or not token:
                _time.sleep(60)
                continue

            now_utc   = datetime.now(_pytz.utc)
            today_str = now_utc.astimezone(_pytz.timezone("America/New_York")).strftime("%Y-%m-%d")
            availability = dict(_cache.get("lineup_availability", {}))

            for league_matches in fixtures.values():
                for m in league_matches:
                    ko  = m.get("kickoff", "")
                    fid = str(m.get("match_id", ""))
                    if not fid or not ko: continue

                    is_live     = m.get("live", False)
                    is_finished = m.get("finished", False)

                    try:
                        ko_dt      = datetime.fromisoformat(ko.replace("Z", "+00:00"))
                        local_date = ko_dt.astimezone(_pytz.timezone("America/New_York")).strftime("%Y-%m-%d")
                        mins_to_ko = (ko_dt.replace(tzinfo=_pytz.utc) - now_utc).total_seconds() / 60
                    except:
                        continue

                    if local_date != today_str: continue

                    is_active = is_live or is_finished or (-60 <= mins_to_ko <= 120)
                    if not is_active:
                        availability[fid] = {"confirmed": False, "starters": [], "sidelined": []}
                        continue

                    try:
                        import requests as _req_bg
                        r = _req_bg.get(
                            f"https://api.sportmonks.com/v3/football/fixtures/{fid}",
                            params={"api_token": token, "include": "lineups;sidelined"},
                            timeout=15
                        )
                        if r.status_code == 429:
                            log.warning(f"Lineup refresh rate limited, sleeping 30s")
                            _time.sleep(30)
                            continue
                        data     = r.json().get("data", {})
                        lineups  = data.get("lineups", [])
                        if isinstance(lineups, dict): lineups = lineups.get("data", [])
                        sidelined = data.get("sidelined", [])
                        if isinstance(sidelined, dict): sidelined = sidelined.get("data", [])

                        starters  = [p["player_id"] for p in lineups if p.get("type_id") == 11]
                        confirmed = len(starters) >= 11
                        sidelined_ids = [p["player_id"] for p in sidelined]

                        availability[fid] = {
                            "confirmed": confirmed,
                            "starters":  starters,
                            "sidelined": sidelined_ids,
                        }
                        log.info(f"Lineup refresh {fid}: confirmed={confirmed} starters={len(starters)}")
                        _time.sleep(1.0)  # 1 second between calls to avoid rate limiting
                    except Exception as e:
                        log.warning(f"Lineup fetch error {fid}: {e}")

            _cache["lineup_availability"] = availability
            _cache["lineup_availability_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        except Exception as e:
            log.error(f"Lineup refresh thread error: {e}")

        _time.sleep(300)  # refresh every 5 minutes

# threading.Thread(target=_refresh_lineup_availability, daemon=True).start()


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
        # Only start a background fetch if one isn't already running
        if not _cache.get("fixtures_loading"):
            _cache["fixtures_loading"] = True
            def bg():
                try:
                    f = get_sm_fixtures(days=7)
                    _cache["fixtures"] = f
                    _cache["fixtures_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    _cache["status"] = "ok"
                    _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                except Exception as e:
                    log.error(f"bg fixtures: {e}")
                finally:
                    _cache["fixtures_loading"] = False
            threading.Thread(target=bg, daemon=True).start()
        return jsonify({"fixtures": {}, "last_updated": "", "loading": True})
    return jsonify({
        "last_updated": _cache.get("fixtures_last_updated", _cache.get("last_updated", "")),
        "fixtures":     cached,
    })


# ── Refresh ───────────────────────────────────────────────────────────────────

@app.route("/refresh", methods=["GET", "POST"])
def refresh():
    """
    Lightweight fixtures-only refresh — synchronous, fast.
    Used by pull-to-refresh on the frontend.
    Returns fresh fixtures immediately so the UI can update.
    """
    if _cache.get("fixtures_loading"):
        # Already refreshing — return current cache
        cached = _cache.get("fixtures", {})
        return jsonify({
            "success": True,
            "message": "Already refreshing",
            "fixtures": cached,
            "last_updated": _cache.get("fixtures_last_updated", ""),
        })

    try:
        _cache["fixtures_loading"] = True
        fix = get_sm_fixtures(days=7)
        _cache["fixtures"] = fix
        _cache["fixtures_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        log.info(f"Fixtures refresh: {sum(len(v) for v in fix.values())} total")
        return jsonify({
            "success": True,
            "fixtures": fix,
            "last_updated": _cache["fixtures_last_updated"],
        })
    except Exception as e:
        log.error(f"Fixtures refresh error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        _cache["fixtures_loading"] = False


@app.route("/refresh/full", methods=["GET", "POST"])
def refresh_full():
    """
    Full refresh — fixtures + standings + season scores.
    Heavy, runs in background. Called by nightly or manual admin trigger.
    """
    _cache["status"]          = "refreshing"
    _cache["refresh_started"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def run_sm_refresh():
        try:
            _cache["status"]       = "ok"
            _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")

            log.info("Full refresh starting...")
            if not _cache.get("fixtures_loading"):
                _cache["fixtures_loading"] = True
                try:
                    fix = get_sm_fixtures(days=7)
                    _cache["fixtures"] = fix
                    _cache["fixtures_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    log.info(f"SM fixtures: {sum(len(v) for v in fix.values())} total")
                finally:
                    _cache["fixtures_loading"] = False
            else:
                log.info("SM fixtures already loading — skipping duplicate fetch")

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
                        teams.append({"position":row.get("position"),"team_id":str(p.get("id","")),"team":p.get("name",""),"short_code":p.get("short_code",""),"logo":p.get("image_path",""),"played":gp,"wins":gv(dets,W),"draws":gv(dets,D),"losses":gv(dets,L),"goals_for":gf,"goals_against":ga,"clean_sheets":gv(dets,CS),"points":row.get("points",0),"gf_pg":round(gf/gp,2) if gp else 0,"ga_pg":round(ga/gp,2) if gp else 0,"goal_diff":gf-ga,"weak_def":(round(ga/gp,2) if gp else 0)>=1.5,"league":lg["name"],"season_id":lg["season_id"]})
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
    # Serve from cache if fresh (5 min TTL) — keyed by date
    date_param = request.args.get("date", "today")
    cache_key  = f"today_context_{date_param}"
    if _cache_valid(cache_key, 1800) and _cache.get(cache_key):
        return jsonify(_cache[cache_key])
    try:
        from .positional_concessions import apply_concession_multiplier, GRANULAR_POSITION_MAP, BROAD_MAP
        from supabase import create_client
        import pytz

        sb = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY"))

        fixtures    = _cache.get("fixtures", {})
        season_data = _cache.get("season_scores", {})
        players     = season_data.get("players", [])
        date_param  = request.args.get("date", "today")
        tz          = pytz.timezone("America/New_York")
        now_local   = datetime.now(tz)
        if date_param == "tomorrow":
            from datetime import timedelta
            target_date = (now_local + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            target_date = now_local.strftime("%Y-%m-%d")
        today       = target_date  # keep variable name for rest of function

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

        log.info(f"today-context: {len(today_fixtures)} fixtures found")

        # Use pre-cached lineup availability (refreshed every 5min by background thread)
        fixture_availability = {}
        cached_avail = _cache.get("lineup_availability", {})
        for fixture in today_fixtures:
            fid = str(fixture.get("match_id", ""))
            if not fid: continue
            if fid in cached_avail:
                fixture_availability[fid] = cached_avail[fid]
            else:
                fixture_availability[fid] = {"confirmed": False, "starters": [], "sidelined": []}

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
                    "goal_multiplier":   round(min(goal_mult, 5.0), 2),
                    "assist_multiplier": round(min(assist_mult, 5.0), 2),
                    "flag":              flag,
                }
            return result

        context_map = {}

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
                    position_id  = player.get("position_id")
                    pos_code     = GRANULAR_POSITION_MAP.get(detailed_pos, (None, None))[0]
                    if not pos_code and position_id:
                        # Fallback to broad position
                        _broad = {24:"GK",25:"DEF",26:"MID",27:"FWD"}.get(position_id)
                        pos_code = _broad
                    if not pos_code: continue

                    broad = BROAD_MAP.get(pos_code, "MID")
                    broad_data = opponent_mults["broad"].get(broad, {})

                    assist_mult = broad_data.get("assist_multiplier", 1.0)
                    goal_mult   = broad_data.get("goal_multiplier", 1.0)

                    # Derive separate flags for assist and goal
                    def _flag(mult):
                        if mult >= THRESHOLD_HIGH:   return "HIGH"
                        if mult >= THRESHOLD_MEDIUM: return "MEDIUM"
                        return None

                    assist_flag = _flag(assist_mult)
                    goal_flag   = _flag(goal_mult)

                    # Also check absolute thresholds per broad position
                    gpg = broad_data.get("goals_conceded", 0) / max(broad_data.get("games_played", 1), 1) if broad_data.get("games_played") else 0
                    apg = broad_data.get("assists_conceded", 0) / max(broad_data.get("games_played", 1), 1) if broad_data.get("games_played") else 0

                    if not assist_flag and apg >= ABS_ASSIST_THRESH.get(broad, 99):
                        assist_flag = "HIGH"
                    if not goal_flag and gpg >= ABS_GOAL_THRESH.get(broad, 99):
                        goal_flag = "HIGH"

                    if not assist_flag and not goal_flag: continue

                    # Overall flag = highest of the two (backwards compat)
                    overall_flag = "HIGH" if (assist_flag == "HIGH" or goal_flag == "HIGH") else "MEDIUM"
                    overall_mult = round(max(assist_mult, goal_mult), 2)

                    context_map[pid] = {
                        "concession_flag":       overall_flag,
                        "concession_multiplier": overall_mult,
                        "assist_flag":           assist_flag,
                        "assist_multiplier":     round(assist_mult, 2),
                        "goal_flag":             goal_flag,
                        "goal_multiplier":       round(goal_mult, 2),
                        "opponent_id":           opponent_id,
                        "fixture_id":            fixture_id,
                    }

        log.info(f"today-context: {len(today_fixtures)} fixtures, {len(context_map)} flagged players")

        # Build team -> fixture map for availability lookup
        team_to_fixture = {}
        for fixture in today_fixtures:
            fid = str(fixture.get("match_id",""))
            if fixture.get("home_id"): team_to_fixture[str(fixture["home_id"])] = fid
            if fixture.get("away_id"): team_to_fixture[str(fixture["away_id"])] = fid

        response = {
            "context":              context_map,
            "count":                len(context_map),
            "fixture_availability": fixture_availability,
            "team_to_fixture":      team_to_fixture,
        }
        _cache_set(cache_key, response)
        return jsonify(response)

    except Exception as e:
        log.error(f"today-context error: {e}")
        return jsonify({"context": {}, "count": 0, "error": str(e)})


@app.route('/api/sm/season/refresh', methods=['POST'])
def sm_season_refresh():
    def run():
        result = get_season_scores()
        _cache.set("season_scores", result, ttl=86400)
        _cache["season_scores_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        log.info("Season scores cache force refreshed and written to Redis")
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
    cached = _cache.get("season_scores")
    if cached and cached.get("players"):
        return jsonify(cached)
    # Cache miss — compute and store with 24hr Redis TTL
    result = get_season_scores()
    _cache.set("season_scores", result, ttl=86400)
    _cache["season_scores_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    return jsonify(result)


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
        if not _cache.get("fixtures_loading"):
            _cache["fixtures_loading"] = True
            try:
                fix = get_sm_fixtures(days=7)
                _cache["fixtures"] = fix
                _cache["fixtures_last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            except Exception as e:
                log.error(f"SM refresh-today fixtures: {e}")
            finally:
                _cache["fixtures_loading"] = False
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
            # Exclude DNPs from all calculations
            played       = [r for r in date_rows if not r.get("dnp")]
            contributors = [r for r in played if r.get("had_contribution")]

            # Market-exclusive contributors
            assist_contributors = [r for r in played if (r.get("actual_assists") or 0) > 0]
            goal_contributors   = [r for r in played if (r.get("actual_goals")   or 0) > 0]
            tsoa_contributors   = [r for r in played if r.get("had_contribution")]

            # Rank lists — market-exclusive
            assist_ranks = [r["sm_assist_rank"] for r in assist_contributors if r.get("sm_assist_rank")]
            goal_ranks   = [r["sm_goal_rank"]   for r in goal_contributors   if r.get("sm_goal_rank")]
            tsoa_ranks   = [r["sm_tsoa_rank"]   for r in tsoa_contributors   if r.get("sm_tsoa_rank")]

            # Count DNPs that were in our top 20 — shown separately for transparency
            dnp_rows = [r for r in date_rows if r.get("dnp")]
            dnp_top20_assist = sum(1 for r in dnp_rows if r.get("sm_assist_rank") and r["sm_assist_rank"] <= 20)
            dnp_top20_goal   = sum(1 for r in dnp_rows if r.get("sm_goal_rank")   and r["sm_goal_rank"]   <= 20)

            def median(lst):
                return round(statistics.median(lst), 1) if lst else None

            def top_n_rate(lst, n=20):
                if not lst: return None
                return round(sum(1 for r in lst if r <= n) / n, 2)

            def top_n_count(lst, n):
                return sum(1 for r in lst if r <= n)

            summaries[date] = {
                "total_fixtures":     len(set(r["fixture_id"] for r in date_rows)),
                "total_contributors": len(tsoa_contributors),
                "dnp_count":          len(dnp_rows),
                "assist": {
                    "median_rank":   median(assist_ranks),
                    "top20_rate":    top_n_rate(assist_ranks),
                    "top20_count":   top_n_count(assist_ranks, 20),
                    "top100_count":  top_n_count(assist_ranks, 100),
                    "outside_count": len(assist_contributors) - top_n_count(assist_ranks, 100),
                    "total":         len(assist_contributors),
                    "dnp_top20":     dnp_top20_assist,
                },
                "goal": {
                    "median_rank":   median(goal_ranks),
                    "top20_rate":    top_n_rate(goal_ranks),
                    "top20_count":   top_n_count(goal_ranks, 20),
                    "top100_count":  top_n_count(goal_ranks, 100),
                    "outside_count": len(goal_contributors) - top_n_count(goal_ranks, 100),
                    "total":         len(goal_contributors),
                    "dnp_top20":     dnp_top20_goal,
                },
                "tsoa": {
                    "median_rank":   median(tsoa_ranks),
                    "top20_rate":    top_n_rate(tsoa_ranks),
                    "top20_count":   top_n_count(tsoa_ranks, 20),
                    "top100_count":  top_n_count(tsoa_ranks, 100),
                    "outside_count": len(tsoa_contributors) - top_n_count(tsoa_ranks, 100),
                    "total":         len(tsoa_contributors),
                },
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


# ── Continental competitions ──────────────────────────────────────────────────

CONTINENTAL_COMPETITIONS = {
    "ucl":  {"league_id": 2,    "season_id": 25580, "name": "Champions League",        "short": "UCL"},
    "uel":  {"league_id": 5,    "season_id": 25582, "name": "Europa League",            "short": "UEL"},
    "uecl": {"league_id": 2286, "season_id": 25581, "name": "Europa Conference League", "short": "UECL"},
}


def _fetch_continental_schedule(competition_key):
    """
    Fetches full schedule for a continental competition from SM.
    Returns structured data: stages, bracket, upcoming_fixtures.
    """
    import requests as req

    comp   = CONTINENTAL_COMPETITIONS[competition_key]
    token  = os.environ.get("SPORTMONKS_API_TOKEN")
    url    = f"https://api.sportmonks.com/v3/football/schedules/seasons/{comp['season_id']}"
    params = {"api_token": token}

    r = req.get(url, params=params, timeout=30)
    r.raise_for_status()
    raw_stages = r.json().get("data", [])

    stages            = []
    upcoming_fixtures = []
    bracket           = []

    for stage in raw_stages:
        stage_obj = {
            "stage_id":    stage["id"],
            "name":        stage["name"],
            "finished":    stage["finished"],
            "is_current":  stage["is_current"],
            "sort_order":  stage["sort_order"],
            "starting_at": stage.get("starting_at"),
            "ending_at":   stage.get("ending_at"),
        }
        stages.append(stage_obj)

        aggregates = stage.get("aggregates", [])
        if not aggregates:
            continue

        for agg in aggregates:
            fixtures = agg.get("fixtures", [])

            leg1 = next((f for f in fixtures if f.get("leg") == "1/2"), None)
            leg2 = next((f for f in fixtures if f.get("leg") == "2/2"), None)
            # Handle single-leg fixtures (e.g. Final)
            if not leg1 and not leg2 and fixtures:
                leg1 = fixtures[0]

            def parse_participants(fix):
                if not fix: return None, None, None, None, None, None
                parts = fix.get("participants", [])
                home  = next((p for p in parts if p.get("meta", {}).get("location") == "home"), None)
                away  = next((p for p in parts if p.get("meta", {}).get("location") == "away"), None)
                return (
                    home.get("id")         if home else None,
                    home.get("name")       if home else None,
                    home.get("image_path") if home else None,
                    away.get("id")         if away else None,
                    away.get("name")       if away else None,
                    away.get("image_path") if away else None,
                )

            def parse_score(fix):
                if not fix: return None, None
                scores  = fix.get("scores", [])
                current = [s for s in scores if s.get("description") == "CURRENT"]
                home_g = away_g = None
                for s in current:
                    sd = s.get("score", {})
                    if sd.get("participant") == "home": home_g = sd.get("goals")
                    elif sd.get("participant") == "away": away_g = sd.get("goals")
                return home_g, away_g

            def parse_state(fix):
                if not fix: return "upcoming"
                state_id = fix.get("state_id")
                if state_id == 5:         return "finished"
                if state_id in [2, 3, 4]: return "live"
                if state_id == 1:         return "upcoming"
                return "finished"  # state_id 7, 8 etc = cancelled/awarded/other — treat as finished

            h_id1, h_name1, h_logo1, a_id1, a_name1, a_logo1 = parse_participants(leg1)
            h_id2, h_name2, h_logo2, a_id2, a_name2, a_logo2 = parse_participants(leg2)

            # Canonical team1 = home team of leg1
            team1_id   = h_id1   or a_id2
            team1_name = h_name1 or a_name2
            team1_logo = h_logo1 or a_logo2
            team2_id   = a_id1   or h_id2
            team2_name = a_name1 or h_name2
            team2_logo = a_logo1 or h_logo2

            leg1_h, leg1_a = parse_score(leg1)
            leg2_h, leg2_a = parse_score(leg2)

            bracket.append({
                "aggregate_id":     agg["id"],
                "stage_name":       stage["name"],
                "stage_id":         stage["id"],
                "competition":      comp["short"],
                "team1_id":         team1_id,
                "team1_name":       team1_name,
                "team1_logo":       team1_logo,
                "team2_id":         team2_id,
                "team2_name":       team2_name,
                "team2_logo":       team2_logo,
                "aggregate_result": agg.get("result"),
                "aggregate_detail": agg.get("detail", ""),
                "winner_id":        agg.get("winner_participant_id"),
                "leg1": {
                    "fixture_id": leg1["id"] if leg1 else None,
                    "home_goals": leg1_h,
                    "away_goals": leg1_a,
                    "date":       leg1.get("starting_at", "")[:10] if leg1 else None,
                    "state":      parse_state(leg1),
                } if leg1 else None,
                "leg2": {
                    "fixture_id": leg2["id"] if leg2 else None,
                    "home_goals": leg2_h,
                    "away_goals": leg2_a,
                    "date":       leg2.get("starting_at", "")[:10] if leg2 else None,
                    "state":      parse_state(leg2),
                } if leg2 else None,
            })

            # Add upcoming/live legs to flat fixtures list
            for fix in fixtures:
                state = parse_state(fix)
                if state in ["upcoming", "live"]:
                    parts  = fix.get("participants", [])
                    home_p = next((p for p in parts if p.get("meta", {}).get("location") == "home"), {})
                    away_p = next((p for p in parts if p.get("meta", {}).get("location") == "away"), {})
                    upcoming_fixtures.append({
                        "match_id":         fix["id"],
                        "kickoff":          fix.get("starting_at"),
                        "home":             home_p.get("name", ""),
                        "away":             away_p.get("name", ""),
                        "home_id":          home_p.get("id"),
                        "away_id":          away_p.get("id"),
                        "home_logo":        home_p.get("image_path", ""),
                        "away_logo":        away_p.get("image_path", ""),
                        "competition":      comp["short"],
                        "competition_name": comp["name"],
                        "league_id":        comp["league_id"],
                        "leg":              fix.get("leg"),
                        "stage":            stage["name"],
                        "live":             state == "live",
                        "finished":         False,
                        "state_id":         fix.get("state_id"),
                    })

    # Current stage = first unfinished by sort_order
    current_stage = next(
        (s for s in sorted(stages, key=lambda x: x["sort_order"]) if not s["finished"]),
        stages[-1] if stages else None
    )

    return {
        "competition":       comp["short"],
        "competition_name":  comp["name"],
        "league_id":         comp["league_id"],
        "season_id":         comp["season_id"],
        "stages":            stages,
        "current_stage":     current_stage,
        "bracket":           bracket,
        "upcoming_fixtures": upcoming_fixtures,
    }


@app.route('/api/sm/continental/schedule', methods=['GET'])
def sm_continental_schedule():
    """
    Returns full schedule + bracket for a continental competition.
    Query param: ?competition=ucl|uel|uecl
    Cached 1 hour per competition.
    """
    competition = request.args.get("competition", "ucl").lower()
    if competition not in CONTINENTAL_COMPETITIONS:
        return jsonify({"error": f"Unknown competition. Use: {list(CONTINENTAL_COMPETITIONS.keys())}"}), 400

    cache_key      = f"continental_schedule_{competition}"
    cache_time_key = f"continental_schedule_{competition}_updated"

    cached = _cache.get(cache_key)
    if cached:
        try:
            age = (datetime.now() - datetime.strptime(_cache[cache_time_key], "%Y-%m-%d %H:%M")).total_seconds()
            if age < 3600:
                return jsonify({**cached, "cached": True, "age_seconds": int(age)})
        except:
            pass

    try:
        data = _fetch_continental_schedule(competition)
        _cache[cache_key]      = data
        _cache[cache_time_key] = datetime.now().strftime("%Y-%m-%d %H:%M")
        return jsonify({**data, "cached": False})
    except Exception as e:
        log.error(f"continental schedule error ({competition}): {e}")
        if cached:
            return jsonify({**cached, "cached": True, "stale": True})
        return jsonify({"error": str(e)}), 500


# ── Team helpers ──────────────────────────────────────────────────────────────

def _get_team_ha_stats(team_id, season_id):
    # Cache per team/season — 6 hour TTL, stats don't change mid-day
    cache_key = f"ha_stats_{team_id}_{season_id}"
    cached = _cache.get(cache_key)
    if _cache_valid(cache_key, 21600) and cached:
        return cached

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
            "wins_home":   get_count(214,"home"), "wins_away":   get_count(214,"away"),
            "losses_home": get_count(215,"home"), "losses_away": get_count(215,"away"),
            "draws_home":  get_count(216,"home"), "draws_away":  get_count(216,"away"),
            "cs_home": get_count(194,"home"), "cs_away": get_count(194,"away"),
            "weak_def_home": get_avg(88,"home") >= 1.5, "weak_def_away": get_avg(88,"away") >= 1.5,
        }
        _cache_set(cache_key, result)
        return result
    except Exception as e:
        log.error(f"team ha stats error {team_id}: {e}")
        return {}


@app.route('/api/sm/lineups/today', methods=['GET'])
def sm_lineups_today():
    """
    Fetch lineup availability for today's active fixtures only.
    Cached for 5 minutes. Called by app when Today tab opens.
    Only fetches live, finished, or within-2hr fixtures.
    """
    import time as _time
    import pytz as _pytz

    # Return cache if fresh (within 5 minutes)
    cached     = _cache.get("lineup_availability", {})
    cache_time = _cache.get("lineup_availability_updated")
    if cache_time:
        try:
            from datetime import datetime as _dt
            age = (_dt.now() - _dt.strptime(cache_time, "%Y-%m-%d %H:%M")).total_seconds()
            if age < 300:
                return jsonify({"fixture_availability": cached, "cached": True, "age_seconds": int(age)})
        except:
            pass

    fixtures = _cache.get("fixtures", {})
    token    = os.environ.get("SPORTMONKS_API_TOKEN")
    if not fixtures or not token:
        return jsonify({"fixture_availability": {}, "error": "no fixtures or token"})

    import requests as _req
    import pytz as _pytz

    now_utc   = datetime.now(_pytz.utc)
    today_str = now_utc.astimezone(_pytz.timezone("America/New_York")).strftime("%Y-%m-%d")
    availability = dict(cached)

    # Find today's active fixtures
    active_fids = []
    for league_matches in fixtures.values():
        for m in league_matches:
            ko  = m.get("kickoff", "")
            fid = str(m.get("match_id", ""))
            if not fid or not ko: continue

            is_live     = m.get("live", False)
            is_finished = m.get("finished", False)

            try:
                ko_dt      = datetime.fromisoformat(ko.replace("Z", "+00:00"))
                local_date = ko_dt.astimezone(_pytz.timezone("America/New_York")).strftime("%Y-%m-%d")
                mins_to_ko = (ko_dt.replace(tzinfo=_pytz.utc) - now_utc).total_seconds() / 60
            except:
                continue

            if local_date != today_str: continue

            is_active = is_live or is_finished or (-60 <= mins_to_ko <= 120)
            if not is_active:
                availability[fid] = {"confirmed": False, "starters": [], "sidelined": []}
            else:
                active_fids.append(fid)

    # Fetch lineups sequentially for active fixtures
    for fid in active_fids:
        try:
            r = _req.get(
                f"https://api.sportmonks.com/v3/football/fixtures/{fid}",
                params={"api_token": token, "include": "lineups;sidelined"},
                timeout=15
            )
            if r.status_code == 429:
                log.warning(f"Lineup fetch rate limited for {fid} — stopping")
                break
            # Check remaining calls from headers
            remaining = int(r.headers.get("x-ratelimit-remaining", 999))
            if remaining < 50:
                log.warning(f"Rate limit low ({remaining}) — stopping lineup fetch")
                break
            data      = r.json().get("data", {})
            lineups   = data.get("lineups", [])
            if isinstance(lineups, dict): lineups = lineups.get("data", [])
            sidelined = data.get("sidelined", [])
            if isinstance(sidelined, dict): sidelined = sidelined.get("data", [])

            starters      = [p["player_id"] for p in lineups if p.get("type_id") == 11]
            subs          = [p["player_id"] for p in lineups if p.get("type_id") == 12]
            sidelined_ids = [p["player_id"] for p in sidelined]
            confirmed     = len(starters) >= 11

            availability[fid] = {
                "confirmed": confirmed,
                "starters":  starters,
                "subs":      subs,
                "sidelined": sidelined_ids,
            }
            log.info(f"Lineup fetched {fid}: confirmed={confirmed} starters={len(starters)} subs={len(subs)}")
            _time.sleep(0.5)
        except Exception as e:
            log.warning(f"Lineup fetch error {fid}: {e}")

    _cache["lineup_availability"] = availability
    _cache["lineup_availability_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    return jsonify({"fixture_availability": availability, "cached": False, "active_count": len(active_fids)})




def _apply_pe_flags(players, opponent_team_id, concession_mults):
    try:
        opp_id = int(opponent_team_id)
    except (TypeError, ValueError):
        opp_id = opponent_team_id
    opp_mults = concession_mults.get(opp_id, {})
    if not opp_mults:
        return players

    from .positional_concessions import GRANULAR_POSITION_MAP as _GPM, BROAD_MAP as _BM, apply_concession_multiplier as _acm

    result = []
    for p in players:
        detailed_pos = p.get("detailed_position_id")
        position_id  = p.get("position_id")

        # Determine position code and whether player has detailed position
        has_detailed = detailed_pos is not None and detailed_pos in _GPM
        pos_code = _GPM.get(detailed_pos, (None, None))[0]
        if not pos_code and position_id:
            pos_code = {24:"GK", 25:"DEF", 26:"MID", 27:"FWD"}.get(position_id)
        if not pos_code:
            result.append(p)
            continue

        has_granular = pos_code in opp_mults.get("granular", {})

        # Use broad-only (has_detailed_position=False) to match today-context behaviour
        a_adj, _, a_flag = _acm(p.get("assist_index") or 0, pos_code, opp_mults, "assist", False)
        g_adj, _, g_flag = _acm(p.get("goal_score")   or 0, pos_code, opp_mults, "goal",   False)
        _,     _, s_flag = _acm(p.get("sot_score")    or 0, pos_code, opp_mults, "sot",    False)

        overall_flag = None
        if a_flag == "HIGH" or g_flag == "HIGH":    overall_flag = "HIGH"
        elif a_flag or g_flag:                      overall_flag = "MEDIUM"

        result.append({
            **p,
            "assist_index":    round(a_adj, 3),
            "goal_score":      round(g_adj, 3),
            "assist_flag":     a_flag,
            "goal_flag":       g_flag,
            "shots_flag":      s_flag,
            "concession_flag": overall_flag,
            "pe_granular":     has_granular,  # flag whether granular data was used
        })
    return result


@app.route('/api/sm/live-state', methods=['GET'])
def sm_live_state():
    import requests as _req
    token    = os.environ.get("SPORTMONKS_API_TOKEN")
    sm_base  = "https://api.sportmonks.com/v3/football"
    fixtures = _cache.get("fixtures", {})
    live_fids = []
    for league_matches in fixtures.values():
        for m in league_matches:
            if m.get("live"):
                fid = str(m.get("match_id",""))
                if fid: live_fids.append(fid)

    if not live_fids:
        return jsonify({"live": {}, "count": 0})

    # Check Redis cache first — 30s TTL
    cache_key = "live_state_all"
    cached = _cache.get(cache_key)
    if _cache_valid(cache_key, 30) and cached:
        return jsonify(cached)

    live = {}
    for fid in live_fids:
        try:
            r = _req.get(
                f"{sm_base}/fixtures/{fid}",
                headers={"Authorization": token},
                params={"include": "scores;state"},
                timeout=8
            )
            if r.status_code != 200: continue
            data     = r.json().get("data", {})
            minute   = data.get("minute")
            state_id = data.get("state_id")
            scores   = data.get("scores", [])
            if isinstance(scores, dict): scores = scores.get("data", [])
            home_g = away_g = None
            for s in scores:
                if s.get("description") == "CURRENT":
                    sd = s.get("score", {})
                    if sd.get("participant") == "home": home_g = sd.get("goals")
                    elif sd.get("participant") == "away": away_g = sd.get("goals")
            live[fid] = {
                "minute":   minute,
                "state_id": state_id,
                "score":    f"{home_g} - {away_g}" if home_g is not None else None,
            }
        except Exception as e:
            log.warning(f"live-state fetch {fid}: {e}")

    result = {"live": live, "count": len(live)}
    _cache_set(cache_key, result)
    return jsonify(result)


@app.route('/api/sm/match/<int:fixture_id>', methods=['GET'])
def sm_match(fixture_id):
    # Cache match response — TTL varies by match state
    match_cache_key = f"match_response_{fixture_id}"
    cached_match = _cache.get(match_cache_key)
    if cached_match and cached_match.get("home"):
        state_id = cached_match.get("live", {}).get("state_id")
        if state_id == 5:
            ttl = 7200    # Full time — 2 hours, data never changes
        elif state_id in [2, 3, 4]:
            ttl = 300     # Live — 5 minutes
        else:
            ttl = 1800    # Upcoming / not started — 30 minutes
        if _cache_valid(match_cache_key, ttl):
            return jsonify(cached_match)
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

        # ── Parallel fetch: H/A stats + SM fixture ──────────────────────────
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import requests as req

        home_ha = {}
        away_ha = {}
        fdata   = None

        def fetch_home_ha():
            return _get_team_ha_stats(home_id, season_id) if season_id else {}
        def fetch_away_ha():
            return _get_team_ha_stats(away_id, season_id) if season_id else {}
        def fetch_fixture():
            token = os.environ.get("SPORTMONKS_API_TOKEN")
            r = req.get(
                f"https://api.sportmonks.com/v3/football/fixtures/{fixture_id}",
                headers={"Authorization": token},
                params={"include": "lineups;formations;scores;events;state;participants"},
                timeout=20
            )
            return r.json().get("data", {}) if r.status_code == 200 else {}

        with ThreadPoolExecutor(max_workers=3) as executor:
            f_home_ha  = executor.submit(fetch_home_ha)
            f_away_ha  = executor.submit(fetch_away_ha)
            f_fixture  = executor.submit(fetch_fixture)
            home_ha = f_home_ha.result()
            away_ha = f_away_ha.result()
            fdata   = f_fixture.result()

        # Extract team IDs from fixture participants — reuse for get_multipliers
        participants = fdata.get("participants", [])
        if isinstance(participants, dict): participants = participants.get("data", [])
        fix_home_id = fix_away_id = None
        for p in participants:
            loc = p.get("meta", {}).get("location", "")
            if loc == "home": fix_home_id = p.get("id")
            elif loc == "away": fix_away_id = p.get("id")

        # ── Positional concession multipliers — pass team IDs to skip SM call ─
        concession_mults = {}
        try:
            if season_id and league_id:
                cache_key = f"mults_{fixture_id}"
                if cache_key not in _cache:
                    _cache[cache_key] = get_multipliers(
                        fixture_id, season_id, league_id,
                        home_team_id=fix_home_id or home_id,
                        away_team_id=fix_away_id or away_id
                    )
                concession_mults = _cache[cache_key]
        except Exception as e:
            log.warning(f"get_multipliers error {fixture_id}: {e}")

        home_players = _apply_pe_flags(sorted([p for p in all_players if str(p.get("team_id")) == str(home_id)], key=lambda x: x.get("tsoa_score") or 0, reverse=True), away_id, concession_mults)
        away_players = _apply_pe_flags(sorted([p for p in all_players if str(p.get("team_id")) == str(away_id)], key=lambda x: x.get("tsoa_score") or 0, reverse=True), home_id, concession_mults)

        lineup_data  = {"starters": [], "subs": [], "home_formation": None, "away_formation": None, "confirmed": False}
        live_data    = {"state": None, "state_id": None, "minute": None, "score": {"home": None, "away": None}, "ht_score": {"home": None, "away": None}, "events": []}
        try:
            if fdata:
                # ── Lineups ───────────────────────────────────────────────────
                lineups = fdata.get("lineups", [])
                if isinstance(lineups, dict): lineups = lineups.get("data", [])
                formations = fdata.get("formations", [])
                if isinstance(formations, dict): formations = formations.get("data", [])
                starters  = [p for p in lineups if p.get("type_id") == 11]
                subs      = [p for p in lineups if p.get("type_id") == 12]
                score_map = {str(p["player_id"]): p for p in all_players}
                name_map  = {str(p["player_id"]): p.get("player_name") for p in all_players}

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

                # ── State / clock ─────────────────────────────────────────────
                state_obj = fdata.get("state", {})
                if isinstance(state_obj, dict) and "data" in state_obj: state_obj = state_obj["data"]
                state_id  = fdata.get("state_id") or (state_obj.get("id") if state_obj else None)
                is_live   = state_id in [2, 3, 4, 6, 7, 10, 11, 12, 13, 14, 15]
                live_data["state"]    = state_obj.get("name") if state_obj else None
                live_data["state_id"] = state_id
                live_data["minute"]   = fdata.get("minute")  # do not fall back to length (seconds)

                # ── Scores ────────────────────────────────────────────────────
                scores = fdata.get("scores", [])
                if isinstance(scores, dict): scores = scores.get("data", [])
                for s in scores:
                    desc = s.get("description", "")
                    part = s.get("score", {}).get("participant")
                    goals = s.get("score", {}).get("goals")
                    if desc == "CURRENT":
                        if part == "home": live_data["score"]["home"] = goals
                        elif part == "away": live_data["score"]["away"] = goals
                    elif desc == "1ST_HALF":
                        if part == "home": live_data["ht_score"]["home"] = goals
                        elif part == "away": live_data["ht_score"]["away"] = goals

                # ── Events ────────────────────────────────────────────────────
                events = fdata.get("events", [])
                if isinstance(events, dict): events = events.get("data", [])

                # Collect pids missing from name_map and fetch from SM
                goal_events = [e for e in events if e.get("type_id") in [14, 15]]
                missing_pids = set()
                for e in goal_events:
                    pid  = str(e.get("player_id", ""))
                    apid = str(e.get("related_player_id", ""))
                    if pid  and pid  not in name_map: missing_pids.add(pid)
                    if apid and apid not in name_map and apid != "None": missing_pids.add(apid)

                import requests as _req2
                _token = os.environ.get("SPORTMONKS_API_TOKEN")
                for mpid in missing_pids:
                    try:
                        nr = _req2.get(
                            f"https://api.sportmonks.com/v3/football/players/{mpid}",
                            headers={"Authorization": _token},
                            timeout=5
                        )
                        if nr.status_code == 200:
                            pdata = nr.json().get("data", {})
                            name  = pdata.get("display_name") or pdata.get("name")
                            if name: name_map[mpid] = name
                    except: pass

                key_events = []
                for e in goal_events:
                    pid      = str(e.get("player_id", ""))
                    apid     = str(e.get("related_player_id", ""))
                    team_id  = e.get("participant_id")
                    pname    = name_map.get(pid) or f"Player {pid}"
                    aname    = name_map.get(apid) if apid and apid != "None" else None
                    key_events.append({
                        "minute":      e.get("minute"),
                        "player_name": pname,
                        "assist_name": aname,
                        "team_id":     team_id,
                        "is_home":     str(team_id) == str(home_id),
                    })
                key_events.sort(key=lambda x: x.get("minute") or 0)
                live_data["events"] = key_events

        except Exception as e:
            log.error(f"lineup pull error {fixture_id}: {e}")

        # ── Add sidelined data to players ────────────────────────────────────
        sidelined_home = get_sidelined_player_ids(team_ids={int(home_id)})
        sidelined_away = get_sidelined_player_ids(team_ids={int(away_id)})
        all_sidelined  = sidelined_home | sidelined_away

        def add_sidelined(players):
            result = []
            for p in players:
                pid = p.get("player_id")
                if pid in all_sidelined:
                    p = {**p, "sidelined": get_sidelined_data(pid)}
                result.append(p)
            return result

        match_response = {
            "fixture_id": fixture_id, "home": home_name, "away": away_name,
            "home_id": home_id, "away_id": away_id,
            "home_players": add_sidelined(home_players[:15]),
            "away_players": add_sidelined(away_players[:15]),
            "total_home": len(home_players), "total_away": len(away_players),
            "home_ha": home_ha, "away_ha": away_ha,
            "lineup": lineup_data, "live": live_data,
        }
        # Only cache if we have valid data — never cache empty/error responses
        if home_name and away_name:
            _cache_set(match_cache_key, match_response)
        return jsonify(match_response)

    except Exception as e:
        log.error(f"sm_match {fixture_id}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/sm/match/<int:fixture_id>/live', methods=['GET'])
def sm_match_live(fixture_id):
    import requests as _req
    token   = os.environ.get("SPORTMONKS_API_TOKEN")
    sm_base = "https://api.sportmonks.com/v3/football"

    cache_key = f"match_live_{fixture_id}"
    cached = _cache.get(cache_key)
    if _cache_valid(cache_key, 20) and cached:
        return jsonify(cached)

    # Build name map from season_scores cache — covers all scored players
    # Fall back to Supabase player_baselines if cache is cold
    try:
        season_data = _cache.get("season_scores") or {}
        all_players = season_data.get("players", [])
        name_map = {str(p["player_id"]): p["player_name"] for p in all_players if p.get("player_name")}
        if not name_map:
            pb = supabase.table("player_baselines").select("player_id,player_name").execute().data
            name_map = {str(r["player_id"]): r["player_name"] for r in pb if r.get("player_name")}
    except:
        name_map = {}

    # Also check fixtures cache for home/away IDs
    fixtures = _cache.get("fixtures", {})
    home_id = away_id = None
    for league_matches in fixtures.values():
        for m in league_matches:
            if str(m.get("match_id")) == str(fixture_id):
                home_id = m.get("home_id")
                away_id = m.get("away_id")
                break
        if home_id: break

    live_data = {
        "state": None, "state_id": None, "minute": None,
        "score": {"home": None, "away": None},
        "ht_score": {"home": None, "away": None},
        "events": []
    }

    try:
        r = _req.get(
            f"{sm_base}/fixtures/{fixture_id}",
            headers={"Authorization": token},
            params={"include": "scores;state;events"},
            timeout=10
        )
        if r.status_code == 200:
            fdata = r.json().get("data", {})

            state_obj = fdata.get("state", {})
            if isinstance(state_obj, dict) and "data" in state_obj:
                state_obj = state_obj["data"]
            state_id = fdata.get("state_id") or (state_obj.get("id") if state_obj else None)
            live_data["state"]    = state_obj.get("name") if state_obj else None
            live_data["state_id"] = state_id
            live_data["minute"]   = fdata.get("minute")

            scores = fdata.get("scores", [])
            if isinstance(scores, dict): scores = scores.get("data", [])
            for s in scores:
                desc  = s.get("description", "")
                part  = s.get("score", {}).get("participant")
                goals = s.get("score", {}).get("goals")
                if desc == "CURRENT":
                    if part == "home": live_data["score"]["home"] = goals
                    elif part == "away": live_data["score"]["away"] = goals
                elif desc == "1ST_HALF":
                    if part == "home": live_data["ht_score"]["home"] = goals
                    elif part == "away": live_data["ht_score"]["away"] = goals

            events = fdata.get("events", [])
            if isinstance(events, dict): events = events.get("data", [])
            key_events = []
            pids_to_fetch = set()
            raw_events = []
            for e in events:
                if e.get("type_id") not in [14, 15]: continue
                pid  = str(e.get("player_id", ""))
                apid = str(e.get("related_player_id", ""))
                if pid and pid not in name_map: pids_to_fetch.add(pid)
                if apid and apid not in name_map and apid != "None": pids_to_fetch.add(apid)
                raw_events.append(e)

            # Fetch missing names from SM in one pass
            for pid in pids_to_fetch:
                try:
                    nr = _req.get(
                        f"{sm_base}/players/{pid}",
                        headers={"Authorization": token},
                        timeout=5
                    )
                    if nr.status_code == 200:
                        pdata = nr.json().get("data", {})
                        name = pdata.get("display_name") or pdata.get("name")
                        if name: name_map[pid] = name
                except: pass

            for e in raw_events:
                pid  = str(e.get("player_id", ""))
                apid = str(e.get("related_player_id", ""))
                tid  = e.get("participant_id")
                key_events.append({
                    "minute":      e.get("minute"),
                    "player_name": name_map.get(pid) or f"Player {pid}",
                    "assist_name": name_map.get(apid) if apid and apid != "None" else None,
                    "team_id":     tid,
                    "is_home":     str(tid) == str(home_id) if home_id else None,
                })
            key_events.sort(key=lambda x: x.get("minute") or 0)
            live_data["events"] = key_events

    except Exception as e:
        log.error(f"sm_match_live {fixture_id}: {e}")

    _cache_set(cache_key, live_data)
    return jsonify(live_data)


@app.route('/api/sm/player-sidelined/<int:player_id>', methods=['GET'])
def sm_player_sidelined(player_id):
    try:
        data = get_sidelined_data(player_id)
        return jsonify({"sidelined": data})
    except Exception as e:
        return jsonify({"sidelined": None, "error": str(e)}), 500


@app.route('/api/sm/team-sidelined/<int:team_id>', methods=['GET'])
def sm_team_sidelined(team_id):
    try:
        from supabase import create_client
        sb = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY"))
        rows = sb.table("player_sidelined")\
            .select("player_id,player_name,injury_type,start_date,end_date,games_missed")\
            .eq("team_id", team_id)\
            .eq("completed", False)\
            .gte("start_date", "2025-07-01")\
            .execute().data
        return jsonify({"sidelined": rows or []})
    except Exception as e:
        return jsonify({"sidelined": [], "error": str(e)}), 500


@app.route('/api/sm/team-form/<int:team_id>', methods=['GET'])
def sm_team_form(team_id):
    """
    Returns last 5 finished results for a team.
    Looks up team's league_id from player_baselines in Supabase.
    Fetches last 60 days for that league, filters by team client-side.
    Cached 24hrs per team.
    """
    import requests as _req
    from supabase import create_client as _create_client
    from datetime import datetime as _dt, timedelta as _td

    cache_key = f"team_form_{team_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return jsonify({"form": cached, "cached": True})

    token = os.environ.get("SPORTMONKS_API_TOKEN")
    if not token:
        return jsonify({"form": [], "error": "no token"})

    try:
        # Look up league_id from player_baselines
        sb = _create_client(
            os.environ.get("SUPABASE_URL"),
            os.environ.get("SUPABASE_SERVICE_KEY")
        )
        res = sb.table("player_baselines")\
            .select("league_id")\
            .eq("team_id", team_id)\
            .limit(1)\
            .execute()

        if not res.data:
            return jsonify({"form": [], "error": "team not found in baselines"})

        league_id = res.data[0]["league_id"]

        today = _dt.utcnow()
        start = (today - _td(days=60)).strftime("%Y-%m-%d")
        end   = today.strftime("%Y-%m-%d")

        # Fetch all pages
        raw  = []
        page = 1
        while True:
            r = _req.get(
                f"https://api.sportmonks.com/v3/football/fixtures/between/{start}/{end}",
                params={
                    "api_token": token,
                    "filters":   f"fixtureLeagues:{league_id}",
                    "include":   "participants;scores;state",
                    "per_page":  25,
                    "page":      page,
                },
                timeout=15
            )
            if r.status_code != 200:
                return jsonify({"form": [], "error": f"SM returned {r.status_code}"})
            data = r.json()
            raw.extend(data.get("data", []))
            if not data.get("pagination", {}).get("has_more"):
                break
            page += 1
            if page > 10: break  # safety limit

        results = []
        for fix in raw:
            if fix.get("state_id") != 5: continue

            participants = fix.get("participants", [])
            if isinstance(participants, dict): participants = participants.get("data", [])

            home_team = away_team = None
            for p in participants:
                loc = p.get("meta", {}).get("location", "")
                if loc == "home": home_team = p
                elif loc == "away": away_team = p

            if not home_team or not away_team: continue

            home_id = home_team.get("id")
            away_id = away_team.get("id")

            if str(home_id) != str(team_id) and str(away_id) != str(team_id):
                continue

            home_name = home_team.get("name", "")
            away_name = away_team.get("name", "")

            scores  = fix.get("scores", [])
            if isinstance(scores, dict): scores = scores.get("data", [])
            current = [s for s in scores if s.get("description") == "CURRENT"]
            home_goals = away_goals = None
            for s in current:
                sd = s.get("score", {})
                if sd.get("participant") == "home": home_goals = sd.get("goals", 0)
                elif sd.get("participant") == "away": away_goals = sd.get("goals", 0)

            if home_goals is None or away_goals is None: continue

            is_home  = str(home_id) == str(team_id)
            gf       = home_goals if is_home else away_goals
            ga       = away_goals if is_home else home_goals
            opponent = away_name if is_home else home_name
            opp_id   = away_id if is_home else home_id

            if gf > ga:   result = "W"
            elif gf < ga: result = "L"
            else:         result = "D"

            results.append({
                "fixture_id":  fix.get("id"),
                "date":        fix.get("starting_at", "")[:10],
                "opponent":    opponent,
                "opponent_id": opp_id,
                "gf":          gf,
                "ga":          ga,
                "result":      result,
                "home":        is_home,
            })

        results.sort(key=lambda x: x["date"], reverse=True)
        form = results[:5]

        _cache[cache_key] = form
        return jsonify({"form": form, "cached": False})

    except Exception as e:
        log.error(f"team_form {team_id}: {e}")
        return jsonify({"form": [], "error": str(e)})


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


# ── Nightly automation ────────────────────────────────────────────────────────

@app.route('/api/nightly/run', methods=['GET', 'POST'])
def nightly_run():
    """
    Nightly pipeline — runs after matches finish.
    1. Find today's completed fixtures
    2. Collect match log (player_match_log)
    3. Record outcomes (sm_matchday_results)
    4. Update concessions for each new fixture
    5. Recalculate league averages
    6. Refresh season scores cache
    """
    target_date = request.args.get("date") or (request.get_json(silent=True) or {}).get("date")

    def run_nightly():
        import requests as req
        import pytz
        from supabase import create_client
        from collections import defaultdict

        sb = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY"))
        token  = os.environ.get("SPORTMONKS_API_TOKEN")
        sm     = "https://api.sportmonks.com/v3/football"
        today  = target_date or datetime.now(pytz.timezone("America/New_York")).strftime("%Y-%m-%d")

        LEAGUE_MAP = {
            8:    {"season_id": 25583, "name": "Premier League"},
            9:    {"season_id": 25648, "name": "Championship"},
            564:  {"season_id": 25659, "name": "La Liga"},
            384:  {"season_id": 25533, "name": "Serie A"},
            82:   {"season_id": 25646, "name": "Bundesliga"},
            301:  {"season_id": 25651, "name": "Ligue 1"},
            779:  {"season_id": 26720, "name": "MLS"},
            1356: {"season_id": 26529, "name": "A-League Men"},
        }
        LEAGUE_NAME_MAP = {v["name"]: k for k, v in LEAGUE_MAP.items()}

        MATCH_LOG_TYPES = {
            119:"minutes_played", 117:"key_passes", 86:"shots_on_target",
            42:"shots_total", 99:"acc_crosses", 98:"total_crosses",
            52:"goals", 79:"assists", 580:"big_chances_created",
            116:"acc_passes", 80:"total_passes", 5304:"xg",
        }

        BROAD_POSITION_MAP    = {24:"GK", 25:"DEF", 26:"MID", 27:"FWD"}
        GRANULAR_POSITION_MAP = {
            24:("GK","GK"), 144:("GK","GK"),
            148:("DEF","DEF"), 154:("DEF","DEF"), 155:("DEF","DEF"),
            149:("MID","MID"), 150:("MID","MID"), 153:("MID","MID"),
            157:("MID","MID"), 158:("MID","MID"),
            151:("FWD","FWD"), 152:("FWD","FWD"), 156:("FWD","FWD"),
            159:("FWD","FWD"), 160:("FWD","FWD"), 161:("FWD","FWD"),
            162:("FWD","FWD"), 163:("FWD","FWD"),
        }

        summary = {
            "date":             today,
            "fixtures_found":   0,
            "match_log_rows":   0,
            "outcome_rows":     0,
            "concessions_updated": 0,
            "errors":           [],
        }

        log.info(f"Nightly run starting for {today}")

        # ── Step 1: Find today's completed fixtures ───────────────────────────
        fixtures = _cache.get("fixtures", {})
        today_fixtures = []
        for league_name, matches in fixtures.items():
            league_id = LEAGUE_NAME_MAP.get(league_name)
            if not league_id: continue
            season_id = LEAGUE_MAP[league_id]["season_id"]
            for m in matches:
                ko = m.get("kickoff", "")
                if not ko: continue
                try:
                    ko_dt = datetime.fromisoformat(ko.replace("Z", "+00:00"))
                    local_date = ko_dt.astimezone(pytz.timezone("America/New_York")).strftime("%Y-%m-%d")
                except:
                    local_date = ko[:10]
                if (local_date == today) and (m.get("finished") or m.get("state_id") == 5):
                    today_fixtures.append({
                        **m,
                        "league_id": league_id,
                        "season_id": season_id,
                    })

        summary["fixtures_found"] = len(today_fixtures)
        log.info(f"Nightly: {len(today_fixtures)} completed fixtures from cache for {today}")

        # If cache is empty, fetch directly from SM
        if not today_fixtures:
            log.info("Nightly: cache empty, fetching fixtures directly from SM...")
            try:
                for league_id, info in LEAGUE_MAP.items():
                    r = req.get(
                        f"{sm}/fixtures/between/{today}/{today}",
                        params={"api_token": token, "filters": f"fixtureLeagues:{league_id}", "per_page": 25},
                        timeout=15
                    )
                    for f in r.json().get("data", []):
                        if f.get("state_id") == 5 and f.get("season_id") == info["season_id"]:
                            participants = f.get("participants", {})
                            if isinstance(participants, dict): participants = participants.get("data", [])
                            home_id = away_id = None
                            for p in participants:
                                loc = p.get("meta", {}).get("location", "")
                                if loc == "home": home_id = p.get("id")
                                elif loc == "away": away_id = p.get("id")
                            today_fixtures.append({
                                "match_id":  f["id"],
                                "home_id":   str(home_id) if home_id else None,
                                "away_id":   str(away_id) if away_id else None,
                                "home":      f.get("name","").split(" vs ")[0],
                                "away":      f.get("name","").split(" vs ")[-1],
                                "league_id": league_id,
                                "season_id": info["season_id"],
                                "finished":  True,
                            })
                    import time as _t; _t.sleep(0.3)
                log.info(f"Nightly: {len(today_fixtures)} completed fixtures from SM direct")
            except Exception as e:
                log.error(f"Nightly SM direct fetch error: {e}")
                summary["errors"].append(f"fixtures_fetch:{str(e)}")

        summary["fixtures_found"] = len(today_fixtures)

        if not today_fixtures:
            log.info("Nightly: no completed fixtures found, exiting")
            _cache["nightly_last_run"] = today
            _cache["nightly_summary"]  = summary
            return

        # Get season scores for player lookup — fetch directly if cache empty
        season_data = _cache.get("season_scores", {})
        if not season_data.get("players"):
            log.info("Nightly: season scores cache empty, fetching...")
            try:
                season_data = get_season_scores()
                _cache["season_scores"] = season_data
            except Exception as e:
                log.error(f"Nightly season scores fetch error: {e}")
                season_data = {}
        all_players   = season_data.get("players", [])
        player_lookup = {p["player_id"]: p for p in all_players if p.get("player_id")}

        # ── Step 2: Match log collection ──────────────────────────────────────
        log.info("Nightly: collecting match logs...")
        match_log_rows = []
        existing_logs  = set()
        try:
            ex = sb.table("player_match_log")\
                .select("fixture_id,player_id")\
                .eq("game_date", today).execute().data
            existing_logs = {(r["fixture_id"], r["player_id"]) for r in ex}
        except: pass

        for match in today_fixtures:
            fixture_id = int(match["match_id"])
            league_id  = match["league_id"]
            season_id  = match["season_id"]
            try:
                r = req.get(
                    f"{sm}/fixtures/{fixture_id}",
                    params={"api_token": token, "include": "lineups.details;participants"},
                    timeout=30
                )
                r.raise_for_status()
                fixture = r.json().get("data", {})
                lineups = fixture.get("lineups", [])
                if isinstance(lineups, dict): lineups = lineups.get("data", [])

                for player in lineups:
                    pid = player.get("player_id")
                    if not pid or (fixture_id, pid) in existing_logs: continue
                    details = player.get("details", [])
                    if isinstance(details, dict): details = details.get("data", [])
                    if not details: continue

                    stats = {}
                    for d in details:
                        tid = d.get("type_id")
                        val = d.get("data", {}).get("value")
                        if tid in MATCH_LOG_TYPES and val is not None:
                            stats[MATCH_LOG_TYPES[tid]] = val

                    minutes = stats.get("minutes_played", 0) or 0
                    if minutes < 10: continue

                    nineties    = minutes / 90
                    total_passes = stats.get("total_passes", 0) or 0
                    acc_passes   = stats.get("acc_passes", 0) or 0
                    pass_acc     = round(acc_passes / total_passes, 3) if total_passes > 0 else None
                    p_info       = player_lookup.get(pid, {})

                    match_log_rows.append({
                        "player_id":           pid,
                        "fixture_id":          fixture_id,
                        "game_date":           today,
                        "league_id":           league_id,
                        "season_id":           season_id,
                        "team_id":             player.get("team_id"),
                        "player_name":         player.get("player_name") or p_info.get("player_name"),
                        "team_name":           p_info.get("team_name"),
                        "minutes_played":      minutes,
                        "goals":               int(stats.get("goals", 0) or 0),
                        "assists":             int(stats.get("assists", 0) or 0),
                        "key_passes":          int(stats.get("key_passes", 0) or 0),
                        "acc_crosses":         int(stats.get("acc_crosses", 0) or 0),
                        "shots_on_target":     int(stats.get("shots_on_target", 0) or 0),
                        "shots_total":         int(stats.get("shots_total", 0) or 0),
                        "big_chances_created": int(stats.get("big_chances_created", 0) or 0),
                        "kp_per90":            round((stats.get("key_passes", 0) or 0) / nineties, 3),
                        "cross_per90":         round((stats.get("acc_crosses", 0) or 0) / nineties, 3),
                        "sot_per90":           round((stats.get("shots_on_target", 0) or 0) / nineties, 3),
                        "goals_per90":         round((stats.get("goals", 0) or 0) / nineties, 3),
                        "pass_accuracy":       pass_acc,
                        "xg":                  stats.get("xg"),
                    })
            except Exception as e:
                log.error(f"Nightly match log error fixture {fixture_id}: {e}")
                summary["errors"].append(f"match_log:{fixture_id}:{str(e)}")

        if match_log_rows:
            for i in range(0, len(match_log_rows), 100):
                sb.table("player_match_log").upsert(
                    match_log_rows[i:i+100],
                    on_conflict="fixture_id,player_id"
                ).execute()
            summary["match_log_rows"] = len(match_log_rows)
            log.info(f"Nightly: {len(match_log_rows)} match log rows written")

        # ── Step 3: Record outcomes ───────────────────────────────────────────
        log.info("Nightly: recording outcomes...")

        today_team_ids = set()
        for m in today_fixtures:
            today_team_ids.add(str(m.get("home_id", "")))
            today_team_ids.add(str(m.get("away_id", "")))

        # Build sub player_ids from lineup availability cache
        sub_player_ids = set()
        lineup_avail = _cache.get("lineup_availability", {})
        for fid, avail in lineup_avail.items():
            for pid in (avail.get("subs") or []):
                sub_player_ids.add(pid)

        today_players   = [p for p in all_players if str(p.get("team_id", "")) in today_team_ids]

        # ── Exclude sidelined players from today rankings ─────────────────────
        today_team_id_ints = {int(t) for t in today_team_ids if t and t != 'None' and str(t).isdigit()}
        sidelined_ids = get_sidelined_player_ids(team_ids=today_team_id_ints)
        if sidelined_ids:
            log.info(f"Nightly: excluding {len(sidelined_ids)} sidelined players from today rankings")
            today_players = [p for p in today_players if p.get("player_id") not in sidelined_ids]

        # Build PE context inline — same logic as today-context endpoint but without SM API calls
        nightly_context = {}
        try:
            from .positional_concessions import GRANULAR_POSITION_MAP as GPM, BROAD_MAP as BM
            from .positional_concessions import THRESHOLD_HIGH, THRESHOLD_MEDIUM
            from .positional_concessions import apply_concession_multiplier as acm
            ABS_GOAL_THRESH   = {"GK":99,"DEF":0.27,"MID":0.75,"FWD":0.85}
            ABS_ASSIST_THRESH = {"GK":99,"DEF":0.30,"MID":0.70,"FWD":0.40}

            season_ids = list({f["season_id"] for f in today_fixtures})
            avg_rows   = sb.table("positional_concessions_league_avg")\
                .select("*").eq("granularity","broad").in_("season_id", season_ids).execute().data
            avg_map = {}
            for row in avg_rows:
                sid = row["season_id"]
                if sid not in avg_map: avg_map[sid] = {}
                avg_map[sid][row["broad_position"]] = row

            tids_by_season = {}
            for f in today_fixtures:
                sid = f["season_id"]
                if sid not in tids_by_season: tids_by_season[sid] = set()
                if f.get("home_id"): tids_by_season[sid].add(int(f["home_id"]))
                if f.get("away_id"): tids_by_season[sid].add(int(f["away_id"]))

            broad_conc = {}
            for sid, tids in tids_by_season.items():
                rows = sb.table("positional_concessions_broad")\
                    .select("*").eq("season_id",sid).in_("team_id",list(tids)).execute().data
                for row in rows:
                    broad_conc[(row["team_id"],sid,row["broad_position"])] = row

            # Build opp_mults structure matching apply_concession_multiplier expectations
            fixture_mults = {}  # fixture_id -> {team_id -> {broad: {flag, mult}}}
            for f in today_fixtures:
                sid = f["season_id"]
                fid = f.get("match_id")
                fixture_mults[fid] = {}
                for team_id, opp_id in [(f.get("home_id"), f.get("away_id")), (f.get("away_id"), f.get("home_id"))]:
                    if not team_id or not opp_id: continue
                    opp_mults = {"broad": {}, "granular": {}}
                    avgs = avg_map.get(sid, {})
                    for bp in ["GK","DEF","MID","FWD"]:
                        row = broad_conc.get((int(opp_id), sid, bp))
                        if not row: continue
                        gp    = row["games_played"] or 1
                        gpg   = row["goals_conceded"]   / gp
                        apg   = row["assists_conceded"] / gp
                        avg_g = avgs.get(bp,{}).get("avg_goals_per_game",   0.001) or 0.001
                        avg_a = avgs.get(bp,{}).get("avg_assists_per_game", 0.001) or 0.001
                        g_mult = gpg / avg_g
                        a_mult = apg / avg_a
                        abs_g  = gpg >= ABS_GOAL_THRESH.get(bp, 99)
                        abs_a  = apg >= ABS_ASSIST_THRESH.get(bp, 99)
                        flag = None
                        if g_mult >= THRESHOLD_HIGH or a_mult >= THRESHOLD_HIGH or abs_g or abs_a:
                            flag = "HIGH"
                        elif g_mult >= THRESHOLD_MEDIUM or a_mult >= THRESHOLD_MEDIUM:
                            flag = "MEDIUM"
                        if flag:
                            opp_mults["broad"][bp] = {
                                "goal_flag":        flag,
                                "assist_flag":      flag,
                                "goal_multiplier":  g_mult,
                                "assist_multiplier":a_mult,
                            }
                    fixture_mults[fid][str(team_id)] = opp_mults

            # Apply to each player
            for f in today_fixtures:
                fid = f.get("match_id")
                for p in today_players:
                    tid = str(p.get("team_id",""))
                    opp_mults = fixture_mults.get(fid, {}).get(tid)
                    if not opp_mults: continue
                    dp     = p.get("detailed_position_id")
                    pos_id = p.get("position_id")
                    pc     = GPM.get(dp, (None,None))[0]
                    if not pc and pos_id:
                        pc = {24:"GK",25:"DEF",26:"MID",27:"FWD"}.get(pos_id)
                    if not pc: continue
                    adj_a, mult_a, a_flag = acm(p.get("assist_index") or 0, pc, opp_mults, "assist", has_detailed_position=False)
                    adj_g, mult_g, g_flag = acm(p.get("goal_score")   or 0, pc, opp_mults, "goal",   has_detailed_position=False)
                    overall = None
                    if a_flag == "HIGH" or g_flag == "HIGH":       overall = "HIGH"
                    elif a_flag == "MEDIUM" or g_flag == "MEDIUM": overall = "MEDIUM"
                    if overall:
                        conc_mult = max(mult_a, mult_g)
                        nightly_context[str(p["player_id"])] = {
                            "concession_flag":       overall,
                            "concession_multiplier": conc_mult,
                        }
        except Exception as e:
            log.error(f"Nightly context error: {e}")
            nightly_context = {}

        # Apply PE multipliers to scores before ranking — use actual multiplier matching Today tab
        ranked_players = []
        for p in today_players:
            ctx  = nightly_context.get(str(p["player_id"]), {})
            mult = ctx.get("concession_multiplier", 1.0) or 1.0
            ranked_players.append({
                **p,
                "assist_index_adj": round((p.get("assist_index") or 0) * mult, 3),
                "goal_score_adj":   round((p.get("goal_score")   or 0) * mult, 3),
                "tsoa_score_adj":   round((p.get("tsoa_score")   or 0) * mult, 3),
            })

        assist_ranked   = sorted(ranked_players, key=lambda x: x.get("assist_index_adj") or 0, reverse=True)
        goal_ranked     = sorted(ranked_players, key=lambda x: x.get("goal_score_adj")   or 0, reverse=True)
        tsoa_ranked     = sorted(ranked_players, key=lambda x: x.get("tsoa_score_adj")   or 0, reverse=True)
        shots_ranked    = sorted(ranked_players, key=lambda x: x.get("shots_score")      or 0, reverse=True)
        sot_ranked      = sorted(ranked_players, key=lambda x: x.get("sot_score")        or 0, reverse=True)
        assist_rank_map = {str(p["player_id"]): i+1 for i, p in enumerate(assist_ranked)}
        goal_rank_map   = {str(p["player_id"]): i+1 for i, p in enumerate(goal_ranked)}
        tsoa_rank_map   = {str(p["player_id"]): i+1 for i, p in enumerate(tsoa_ranked)}
        shots_rank_map  = {str(p["player_id"]): i+1 for i, p in enumerate(shots_ranked)}
        sot_rank_map    = {str(p["player_id"]): i+1 for i, p in enumerate(sot_ranked)}
        player_lookup   = {str(p["player_id"]): p for p in ranked_players}

        actual_goals   = {}
        actual_assists = {}
        player_fixture = {}

        for match in today_fixtures:
            fixture_id = match.get("match_id")
            try:
                r = req.get(
                    f"{sm}/fixtures/{fixture_id}",
                    params={"api_token": token, "include": "events"},
                    timeout=30
                )
                r.raise_for_status()
                events = r.json().get("data", {}).get("events", [])
                if isinstance(events, dict): events = events.get("data", [])
                for e in events:
                    if e.get("type_id") != 14: continue
                    pid = e.get("player_id")
                    rid = e.get("related_player_id")
                    if pid:
                        actual_goals[pid] = actual_goals.get(pid, 0) + 1
                        if pid not in player_fixture: player_fixture[pid] = fixture_id
                    if rid:
                        actual_assists[rid] = actual_assists.get(rid, 0) + 1
                        if rid not in player_fixture: player_fixture[rid] = fixture_id
            except Exception as e:
                log.error(f"Nightly outcomes error fixture {fixture_id}: {e}")
                summary["errors"].append(f"outcomes:{fixture_id}:{str(e)}")

        # Build minutes + shots/SOT lookup from match log
        minutes_lookup = {}
        shots_lookup   = {}
        sot_lookup     = {}
        for row in match_log_rows:
            pid_ml = row["player_id"]
            minutes_lookup[pid_ml] = row.get("minutes_played", 0)
            shots_lookup[pid_ml]   = int(row.get("shots_total", 0) or 0)
            sot_lookup[pid_ml]     = int(row.get("shots_on_target", 0) or 0)

        # Include all players with goals, assists, or shots recorded
        all_pids      = set(
            list(actual_goals.keys()) +
            list(actual_assists.keys()) +
            list(shots_lookup.keys())
        )
        outcome_rows  = []
        for pid in all_pids:
            p    = player_lookup.get(str(pid), {})
            g    = actual_goals.get(pid, 0)
            a    = actual_assists.get(pid, 0)
            sh   = shots_lookup.get(pid, 0)
            sot  = sot_lookup.get(pid, 0)
            ctx  = nightly_context.get(str(pid), {})
            mins = minutes_lookup.get(pid, None)

            # Baseline comparison — did player exceed their per90 expectation?
            baseline  = player_lookup.get(str(pid), {})
            nineties_played = (mins / 90) if mins else None
            shots_baseline  = (baseline.get("shots_per90") or 0) * nineties_played if nineties_played else None
            sot_baseline    = (baseline.get("sot_per90")   or 0) * nineties_played if nineties_played else None

            outcome_rows.append({
                "game_date":        today,
                "fixture_id":       player_fixture.get(pid),
                "player_id":        pid,
                "player_name":      p.get("player_name"),
                "team_name":        p.get("team_name"),
                "league_id":        p.get("league_id"),
                "sm_assist_rank":   assist_rank_map.get(str(pid)),
                "sm_goal_rank":     goal_rank_map.get(str(pid)),
                "sm_tsoa_rank":     tsoa_rank_map.get(str(pid)),
                "sm_shots_rank":    shots_rank_map.get(str(pid)),
                "sm_sot_rank":      sot_rank_map.get(str(pid)),
                "assist_index":     p.get("assist_index_adj") or p.get("assist_index"),
                "goal_score":       p.get("goal_score_adj")   or p.get("goal_score"),
                "tsoa_score":       p.get("tsoa_score_adj")   or p.get("tsoa_score"),
                "shots_score":      p.get("shots_score"),
                "sot_score":        p.get("sot_score"),
                "concession_flag":  ctx.get("concession_flag"),
                "actual_goals":     g,
                "actual_assists":   a,
                "actual_shots":     sh,
                "actual_sot":       sot,
                "had_contribution": (g > 0 or a > 0),
                "minutes_played":   mins,
                "dnp":              mins is not None and mins == 0,
                "sub_on":           pid in sub_player_ids and mins is not None and mins > 0,
                "outcome_recorded": True,
            })

        # Also record top 20 non-contributors so Hit% is accurate
        recorded_pids = set(all_pids)
        for market_name, ranked_list, rank_map in [
            ("assist", assist_ranked, assist_rank_map),
            ("goal",   goal_ranked,   goal_rank_map),
        ]:
            for i, p in enumerate(ranked_list[:20]):
                pid = p.get("player_id")
                if not pid or pid in recorded_pids: continue
                tid = str(p.get("team_id", ""))
                fid = next((
                    m["match_id"] for m in today_fixtures
                    if str(m.get("home_id","")) == tid or str(m.get("away_id","")) == tid
                ), None)
                if not fid: continue
                ctx  = nightly_context.get(str(pid), {})
                mins = minutes_lookup.get(pid, None)
                sh   = shots_lookup.get(pid, 0)
                sot  = sot_lookup.get(pid, 0)
                outcome_rows.append({
                    "game_date":        today,
                    "fixture_id":       fid,
                    "player_id":        pid,
                    "player_name":      p.get("player_name"),
                    "team_name":        p.get("team_name"),
                    "league_id":        p.get("league_id"),
                    "sm_assist_rank":   assist_rank_map.get(str(pid)),
                    "sm_goal_rank":     goal_rank_map.get(str(pid)),
                    "sm_tsoa_rank":     tsoa_rank_map.get(str(pid)),
                    "sm_shots_rank":    shots_rank_map.get(str(pid)),
                    "sm_sot_rank":      sot_rank_map.get(str(pid)),
                    "assist_index":     p.get("assist_index_adj") or p.get("assist_index"),
                    "goal_score":       p.get("goal_score_adj")   or p.get("goal_score"),
                    "tsoa_score":       p.get("tsoa_score_adj")   or p.get("tsoa_score"),
                    "shots_score":      p.get("shots_score"),
                    "sot_score":        p.get("sot_score"),
                    "concession_flag":  ctx.get("concession_flag"),
                    "actual_goals":     0,
                    "actual_assists":   0,
                    "actual_shots":     sh,
                    "actual_sot":       sot,
                    "had_contribution": False,
                    "minutes_played":   mins,
                    "dnp":              mins is not None and mins == 0,
                    "sub_on":           pid in sub_player_ids and mins is not None and mins > 0,
                    "outcome_recorded": True,
                })
                recorded_pids.add(pid)

        if outcome_rows:
            for i in range(0, len(outcome_rows), 100):
                sb.table("sm_matchday_results").upsert(
                    outcome_rows[i:i+100],
                    on_conflict="fixture_id,player_id"
                ).execute()
            summary["outcome_rows"] = len(outcome_rows)
            log.info(f"Nightly: {len(outcome_rows)} outcome rows written")

        # ── Step 4: Update concessions for new fixtures ───────────────────────
        log.info("Nightly: updating concessions...")
        from .positional_concessions import process_fixture as pf
        conc_updated = 0
        for match in today_fixtures:
            try:
                pf(int(match["match_id"]), match["season_id"], match["league_id"])
                conc_updated += 1
            except Exception as e:
                log.error(f"Nightly concessions error {match['match_id']}: {e}")
        summary["concessions_updated"] = conc_updated

        # ── Step 5: Recalculate league averages ───────────────────────────────
        log.info("Nightly: recalculating league averages...")
        try:
            updated_seasons = {(f["league_id"], f["season_id"]) for f in today_fixtures}
            for league_id, season_id in updated_seasons:
                broad_rows = sb.table("positional_concessions_broad")\
                    .select("*").eq("season_id", season_id).eq("league_id", league_id).execute().data
                if not broad_rows: continue
                groups = defaultdict(lambda: defaultdict(float))
                for row in broad_rows:
                    bp = row["broad_position"]
                    gp = row["games_played"] or 0
                    groups[bp]["goals"]        += row["goals_conceded"]
                    groups[bp]["assists"]      += row["assists_conceded"]
                    groups[bp]["goals_home"]   += row.get("goals_conceded_home", 0)
                    groups[bp]["goals_away"]   += row.get("goals_conceded_away", 0)
                    groups[bp]["assists_home"] += row.get("assists_conceded_home", 0)
                    groups[bp]["assists_away"] += row.get("assists_conceded_away", 0)
                    groups[bp]["bc"]           += row.get("bc_conceded", 0)
                    groups[bp]["bc_home"]      += row.get("bc_conceded_home", 0)
                    groups[bp]["bc_away"]      += row.get("bc_conceded_away", 0)
                    groups[bp]["games"]        += gp
                    groups[bp]["games_home"]   += gp / 2
                    groups[bp]["games_away"]   += gp / 2
                    groups[bp]["teams"]        += 1
                for bp, t in groups.items():
                    gp      = t["games"]      or 1
                    gp_home = t["games_home"] or 1
                    gp_away = t["games_away"] or 1
                    sb.table("positional_concessions_league_avg").upsert({
                        "league_id":            league_id,
                        "season_id":            season_id,
                        "broad_position":       bp,
                        "position_code":        None,
                        "granularity":          "broad",
                        "avg_goals_per_game":   t["goals"]        / gp,
                        "avg_goals_home":       t["goals_home"]   / gp_home,
                        "avg_goals_away":       t["goals_away"]   / gp_away,
                        "avg_assists_per_game": t["assists"]      / gp,
                        "avg_assists_home":     t["assists_home"] / gp_home,
                        "avg_assists_away":     t["assists_away"] / gp_away,
                        "avg_bc_per_game":      t["bc"]           / gp,
                        "avg_bc_home":          t["bc_home"]      / gp_home,
                        "avg_bc_away":          t["bc_away"]      / gp_away,
                        "sample_size":          int(t["teams"]),
                        "last_updated":         datetime.utcnow().isoformat()
                    }, on_conflict="league_id,season_id,granularity,broad_position").execute()
        except Exception as e:
            log.error(f"Nightly league averages error: {e}")
            summary["errors"].append(f"league_avgs:{str(e)}")

        # ── Step 6: Sync sidelined players ───────────────────────────────────
        log.info("Nightly: syncing sidelined players...")
        try:
            import requests as _req
            all_team_ids = set()
            for f in today_fixtures:
                if f.get("home_id"): all_team_ids.add(int(f["home_id"]))
                if f.get("away_id"): all_team_ids.add(int(f["away_id"]))
            sidelined_updated = 0
            for team_id in all_team_ids:
                try:
                    r = _req.get(
                        f"https://api.sportmonks.com/v3/football/teams/{team_id}",
                        headers={"Authorization": os.environ.get("SPORTMONKS_API_TOKEN")},
                        params={"include": "sidelined.type;sidelined.player"},
                        timeout=15
                    )
                    if r.status_code != 200: continue
                    data      = r.json().get("data", {})
                    sidelined = data.get("sidelined", [])
                    if isinstance(sidelined, dict): sidelined = sidelined.get("data", [])

                    existing = sb.table("player_sidelined")                        .select("player_id,start_date")                        .eq("team_id", team_id).eq("completed", False).execute().data
                    existing_keys = {(r["player_id"], r["start_date"]) for r in existing}
                    current_keys  = set()

                    for s in sidelined:
                        player    = s.get("player", {})
                        if isinstance(player, dict) and "data" in player: player = player["data"]
                        type_data = s.get("type", {})
                        if isinstance(type_data, dict) and "data" in type_data: type_data = type_data["data"]
                        pid        = player.get("id") or s.get("player_id")
                        pname      = player.get("display_name") or player.get("name")
                        start_date = s.get("start_date")
                        if not pid: continue
                        current_keys.add((pid, start_date))
                        record = {
                            "player_id":    pid, "team_id": team_id,
                            "player_name":  pname,
                            "injury_type":  type_data.get("name", "Unknown"),
                            "start_date":   start_date,
                            "end_date":     s.get("end_date"),
                            "completed":    s.get("completed") or False,
                            "games_missed": s.get("games_missed") or 0,
                            "last_updated": datetime.now().isoformat()
                        }
                        existing_row = sb.table("player_sidelined")                            .select("id").eq("player_id", pid).eq("team_id", team_id)                            .eq("start_date", start_date or "1900-01-01").execute().data
                        if existing_row:
                            sb.table("player_sidelined").update(record).eq("id", existing_row[0]["id"]).execute()
                        else:
                            sb.table("player_sidelined").insert({**record, "league_id": next(
                                (f["league_id"] for f in today_fixtures
                                 if int(f.get("home_id",0))==team_id or int(f.get("away_id",0))==team_id), 0
                            )}).execute()
                        sidelined_updated += 1

                    # Mark stale records as completed
                    for (pid, sdate) in (existing_keys - current_keys):
                        sb.table("player_sidelined").update({"completed": True, "last_updated": datetime.now().isoformat()})                            .eq("player_id", pid).eq("team_id", team_id)                            .eq("start_date", sdate or "1900-01-01").execute()
                    import time as _time
                    _time.sleep(0.3)
                except Exception as te:
                    log.warning(f"Sidelined sync team {team_id}: {te}")
            log.info(f"Nightly: sidelined sync complete — {sidelined_updated} records")
            summary["sidelined_updated"] = sidelined_updated
        except Exception as e:
            log.error(f"Nightly sidelined sync error: {e}")

        # ── Step 7: Refresh season scores cache ───────────────────────────────
        log.info("Nightly: refreshing season scores cache...")
        try:
            result = get_season_scores()
            _cache.set("season_scores", result, ttl=86400)  # 24hr Redis TTL — survives restarts
            _cache["season_scores_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            log.info(f"Nightly: season scores written to Redis — {result.get('count', 0)} players")
        except Exception as e:
            log.error(f"Nightly season scores refresh error: {e}")

        _cache["nightly_last_run"] = today
        _cache["nightly_summary"]  = summary
        log.info(f"Nightly complete: {summary}")

    threading.Thread(target=run_nightly, daemon=True).start()
    return jsonify({"status": "ok", "message": "Nightly pipeline started", "date": datetime.now().__class__.__name__})


@app.route('/api/season/archive', methods=['POST'])
def archive_season_baselines():
    try:
        log.info("Season archive: starting...")
        rows = supabase.table("player_baselines").select("*").execute().data
        if not rows:
            return jsonify({"error": "No rows in player_baselines"}), 400

        log.info(f"Season archive: copying {len(rows)} rows...")

        from datetime import datetime
        now = datetime.utcnow().isoformat()
        archive_rows = []
        for r in rows:
            row = {k: v for k, v in r.items() if k != "id"}
            row["copied_at"] = now
            archive_rows.append(row)

        inserted = 0
        for i in range(0, len(archive_rows), 100):
            batch = archive_rows[i:i+100]
            supabase.table("player_baselines_historical").upsert(
                batch,
                on_conflict="player_id,season_id"
            ).execute()
            inserted += len(batch)
            log.info(f"Season archive: {inserted}/{len(archive_rows)} rows written")

        log.info(f"Season archive: complete — {inserted} rows archived")
        return jsonify({
            "success": True,
            "rows_archived": inserted,
            "copied_at": now
        })

    except Exception as e:
        log.error(f"Season archive error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/nightly/status', methods=['GET'])
def nightly_status():
    return jsonify({
        "last_run": _cache.get("nightly_last_run"),
        "summary":  _cache.get("nightly_summary"),
    })


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Deep Current Football API v{APP_VERSION} starting on port {port}")
    app.run(host="0.0.0.0", port=port)
