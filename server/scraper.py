# =============================================================================
# scraper.py — Full FotMob data pipeline (run_scraper)
# =============================================================================

import asyncio, aiohttp, logging
import pandas as pd
from fotmob import FotMob

from .utils import (LEAGUES, STATS, GS_STATS, HEADERS, MIN_GOALS_PG, MAX_PLAYERS,
                    WEAK_DEF_THRESH, FORM_MATCHES, safe_float,
                    player_img_url, team_logo_url)
from .formulas import combined_score, gs_score, tsoa_score, form_score
from .standings import get_standings
from .fixtures import get_fixtures_for_dates

log = logging.getLogger(__name__)


# ── Season ID ─────────────────────────────────────────────────────────────────

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


# ── Stat fetcher ──────────────────────────────────────────────────────────────

async def fetch_stat(sess, league_id, season_id, stat_name, all_team_ids):
    url  = (f"https://data.fotmob.com/stats/{league_id}"
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


# ── Team context ──────────────────────────────────────────────────────────────

async def get_team_form(fotmob, team_id, team_name):
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
    try:
        fixture = await fotmob.get_team_next_fixture(int(team_id))
        if not fixture or not isinstance(fixture, dict):
            return None, False
        home    = fixture.get("home", {})
        away    = fixture.get("away", {})
        home_id = str(home.get("id", ""))
        away_id = str(away.get("id", ""))
        opp_id  = away_id if home_id == str(team_id) else home_id
        opp_name = away.get("name","") if home_id == str(team_id) else home.get("name","")
        opp_row = teams_df[teams_df["team_id"] == opp_id]
        is_weak_def = float(opp_row.iloc[0]["ga_pg"]) >= WEAK_DEF_THRESH \
                      if not opp_row.empty else False
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


# ── Player L5 ─────────────────────────────────────────────────────────────────

async def get_player_l5(fotmob, player_id, team_id, player_name):
    """Fetch last 5 matches for a player, extract goals + assists from events."""
    games = []
    try:
        fixtures = await fotmob.get_team_last_fixtures(int(team_id))
        matches  = fixtures if isinstance(fixtures, list) else []
        recent   = [m for m in matches if isinstance(m, dict)][-5:]

        for match in recent:
            match_id = match.get("id") or match.get("matchId")
            home     = match.get("home", {})
            away     = match.get("away", {})
            home_id  = str(home.get("id",""))
            is_home  = home_id == str(team_id)
            opponent = away.get("name","") if is_home else home.get("name","")
            h_score  = safe_float(home.get("score",-1))
            a_score  = safe_float(away.get("score",-1))

            if is_home:
                result = "W" if h_score > a_score else "D" if h_score == a_score else "L"
                score  = f"{int(h_score)}-{int(a_score)}"
            else:
                result = "W" if a_score > h_score else "D" if h_score == a_score else "L"
                score  = f"{int(a_score)}-{int(h_score)}"

            goals, assists = 0, 0
            if match_id:
                try:
                    match_data = await fotmob.get_match(int(match_id))
                    if isinstance(match_data, dict):
                        content_data = match_data.get("content", {})
                        match_facts  = content_data.get("matchFacts", {})
                        raw_events   = match_facts.get("events", {})
                        if isinstance(raw_events, dict):
                            events = raw_events.get("events", [])
                        elif isinstance(raw_events, list):
                            events = raw_events
                        else:
                            events = []
                        if not events:
                            events = match_data.get("header", {}).get("events", [])
                        for event in events:
                            if not isinstance(event, dict): continue
                            etype = str(event.get("type","") or event.get("eventType","")).lower()
                            if any(g in etype for g in ["goal","addedgoal","penaltygoal"]):
                                scorer_id = str(event.get("playerId","") or
                                               event.get("player",{}).get("id",""))
                                if scorer_id == str(player_id): goals += 1
                                assist_id = str(event.get("assistId","") or
                                               event.get("assist",{}).get("id","") or
                                               event.get("assistPlayerId",""))
                                if assist_id and assist_id == str(player_id): assists += 1
                except Exception as e:
                    log.warning(f"  match {match_id} events error: {e}")

            utc = match.get("status",{}).get("utcTime","") or match.get("utcTime","")
            games.append({"opponent": opponent, "result": result, "score": score,
                          "goals": goals, "assists": assists,
                          "date": utc[:10] if utc else ""})
    except Exception as e:
        log.error(f"player_l5 {player_name}: {e}")
    return games


# ── Main scraper ──────────────────────────────────────────────────────────────

async def run_scraper():
    log.info("Scraper v3 starting (fully parallel)...")

    async with FotMob() as fotmob:

        # ── Pass 1: Standings ────────────────────────────────────────────────
        log.info("Pass 1: standings...")
        EURO_LEAGUES  = {k: v for k, v in LEAGUES.items() if k not in ("MLS","A-League Men")}
        OTHER_LEAGUES = {k: v for k, v in LEAGUES.items() if k in ("MLS","A-League Men")}

        euro_results = await asyncio.gather(
            *[get_standings(fotmob, ln, info["id"]) for ln, info in EURO_LEAGUES.items()],
            return_exceptions=True)
        all_team_rows = [row for r in euro_results if isinstance(r, list) for row in r]

        for ln, info in OTHER_LEAGUES.items():
            await asyncio.sleep(3)
            all_team_rows.extend(await get_standings(fotmob, ln, info["id"]))

        if not all_team_rows:
            raise RuntimeError("No standings data")

        teams_df     = pd.DataFrame(all_team_rows)
        all_team_ids = set(str(tid) for tid in teams_df["team_id"].tolist())
        log.info(f"Standings: {len(teams_df)} teams")

        # ── Pass 2: Season IDs ───────────────────────────────────────────────
        log.info("Pass 2: season IDs (parallel)...")

        async def fetch_season(league_name, info):
            lt = teams_df[teams_df["league"] == league_name]
            if lt.empty: return info["id"], None
            sid = await get_season_id(fotmob, lt.iloc[0]["team_id"])
            return info["id"], sid

        season_results = await asyncio.gather(
            *[fetch_season(ln, info) for ln, info in LEAGUES.items()],
            return_exceptions=True)
        league_seasons = {lid: sid for r in season_results
                          if isinstance(r, tuple) for lid, sid in [r] if sid}
        for lid, sid in league_seasons.items():
            log.info(f"  Season {lid}: {sid}")

        # ── Pass 3: Assist stats ─────────────────────────────────────────────
        log.info("Pass 3: player stats (parallel)...")
        fotmob_session = getattr(fotmob, "_session", None) or getattr(fotmob, "session", None)
        sess           = fotmob_session or aiohttp.ClientSession()

        async def fetch_league_stats(league_name, info):
            lid = info["id"]
            sid = league_seasons.get(lid)
            if not sid:
                log.warning(f"Pass 3: {league_name} skipped — no season ID")
                return []
            results = await asyncio.gather(
                *[fetch_stat(sess, lid, sid, sn, all_team_ids) for sn in STATS],
                return_exceptions=True)
            rows = [row for r in results if isinstance(r, list) for row in r]
            log.info(f"  {league_name}: {len(rows)} player rows")
            return rows

        PRIORITY_LEAGUES = {k: v for k, v in LEAGUES.items() if k != "A-League Men"}
        DELAYED_LEAGUES  = {k: v for k, v in LEAGUES.items() if k == "A-League Men"}

        priority_results = await asyncio.gather(
            *[fetch_league_stats(ln, info) for ln, info in PRIORITY_LEAGUES.items()],
            return_exceptions=True)
        all_player_rows = [row for r in priority_results if isinstance(r, list) for row in r]

        for ln, info in DELAYED_LEAGUES.items():
            await asyncio.sleep(2)
            all_player_rows.extend(await fetch_league_stats(ln, info))

        # ── Pass 3b: Goal scorer stats ───────────────────────────────────────
        log.info("Pass 3b: goal scorer stats...")

        async def fetch_gs_stats(league_name, info):
            lid = info["id"]
            sid = league_seasons.get(lid)
            if not sid: return []
            results = await asyncio.gather(
                *[fetch_stat(sess, lid, sid, sn, all_team_ids) for sn in GS_STATS],
                return_exceptions=True)
            rows = []
            for r in results:
                if not isinstance(r, list): continue
                for row in r:
                    row["stat_type"] = GS_STATS.get(row.get("stat_type",""), row.get("stat_type",""))
                    rows.append(row)
            log.info(f"  GS {league_name}: {len(rows)} rows")
            return rows

        gs_results  = await asyncio.gather(
            *[fetch_gs_stats(ln, info) for ln, info in PRIORITY_LEAGUES.items()],
            return_exceptions=True)
        all_gs_rows = [row for r in gs_results if isinstance(r, list) for row in r]

        for ln, info in DELAYED_LEAGUES.items():
            await asyncio.sleep(2)
            all_gs_rows.extend(await fetch_gs_stats(ln, info))

        if fotmob_session is None and sess:
            await sess.close()

        # ── Pass 4: Form + opponents + fixtures ──────────────────────────────
        log.info("Pass 4: form + opponents + fixtures (parallel)...")

        async def fetch_team_context(team_row):
            tid, tname = team_row["team_id"], team_row["team"]
            try:    form = await get_team_form(fotmob, tid, tname)
            except: form = []
            try:    opp, _ = await get_next_opponent(fotmob, tid, tname, teams_df)
            except: opp = None
            return tname, form, opp

        context_results, fixtures = await asyncio.gather(
            asyncio.gather(*[fetch_team_context(row)
                             for _, row in teams_df.iterrows()],
                           return_exceptions=True),
            get_fixtures_for_dates(fotmob, days=7),
            return_exceptions=True)

        team_form, team_next_opp = {}, {}
        if isinstance(context_results, (list, tuple)):
            for result in context_results:
                if isinstance(result, Exception): continue
                tname, form, opp = result
                team_form[tname]     = form
                team_next_opp[tname] = opp

        if isinstance(fixtures, Exception):
            log.error(f"fixtures failed: {fixtures}")
            fixtures = {}

        log.info(f"Form: {len(team_form)} teams | "
                 f"Fixtures: {sum(len(v) for v in fixtures.values())} matches")

    if not all_player_rows:
        raise RuntimeError("No player data")

    # ── Aggregate assist players ──────────────────────────────────────────────
    tgp  = {str(r["team_id"]): r["played"]    for _, r in teams_df.iterrows()}
    tlg  = {str(r["team_id"]): r["league"]    for _, r in teams_df.iterrows()}
    tpos = {str(r["team_id"]): r["table_pos"] for _, r in teams_df.iterrows()}

    agg = {}
    for row in all_player_rows:
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
    df["chances_per_game"] = df.apply(
        lambda r: round(r["chances_created"] / (r["xa"] / r["xa_per90"]), 2)
        if r["xa"] > 0 and r["xa_per90"] > 0 and r["chances_created"] > 0
        else (round(r["xa_per90"] * 2.5, 2) if r["xa_per90"] > 0 else 0.0), axis=1)
    df["xa_gap"] = (df["xa"] - df["assists"]).round(1)
    df["score"]  = df.apply(
        lambda r: combined_score(r["xa_gap"], r["chances_per_game"],
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
            "form":             form,
            "form_score":       form_score(form),
            "next_opponent":    next_opp.get("opponent")  if next_opp else None,
            "next_kickoff":     next_opp.get("kickoff")   if next_opp else None,
            "weak_opp_def":     next_opp.get("weak_def")  if next_opp else False,
            "player_id":        str(p.get("player_id", "")),
            "player_img":       player_img_url(str(p.get("player_id", ""))),
            "team_logo":        team_logo_url(str(p.get("team_id", ""))),
        }

    qt         = teams_df[teams_df["gf_pg"] >= MIN_GOALS_PG]["team"].tolist()
    top25_list = [player_dict(p) for _, p in df[df["team"].isin(qt)].head(MAX_PLAYERS).iterrows()]

    by_league = {}
    for league_name in LEAGUES:
        lg_df = df[df["league"] == league_name]
        if lg_df.empty: continue
        teams_in_league = {}
        for _, p in lg_df.iterrows():
            teams_in_league.setdefault(p["team"], []).append(player_dict(p))
        sorted_teams = sorted(
            teams_in_league.items(),
            key=lambda x: safe_float(
                teams_df[teams_df["team"] == x[0]]["table_pos"].values[0]
                if not teams_df[teams_df["team"] == x[0]].empty else 99))
        by_league[league_name] = [{"team": t, "players": pl} for t, pl in sorted_teams]

    all_players_list = [player_dict(p) for _, p in df.iterrows()]

    teams_list = [{
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
        "weak_def":  bool(r.get("weak_def", False)),
        "conference": r.get("conference", ""),
        "team_logo":  team_logo_url(str(r.get("team_id", ""))),
    } for _, r in teams_df.iterrows()]

    # ── Goal scorer aggregation ───────────────────────────────────────────────
    gs_agg = {}
    for row in all_gs_rows:
        pid, tid = str(row.get("player_id","")), str(row.get("team_id",""))
        key      = (pid, tid)
        stype    = row.get("stat_type","")
        if key not in gs_agg:
            gs_agg[key] = {
                "player": row["player"], "player_id": pid,
                "team":   row["team"],   "team_id":   tid,
                "league": tlg.get(tid, ""),
                "goals": 0.0, "xg": 0.0, "xg_per90": 0.0,
                "xgot":  0.0, "sot_per90": 0.0, "shots_per90": 0.0,
                "big_chances_missed": 0.0,
            }
        if stype in gs_agg[key]:
            gs_agg[key][stype] = safe_float(row.get("stat_value", 0))

    gs_top25_list, gs_all_list, gs_by_league = [], [], {}

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
            gs_by_league.setdefault(lg, {}).setdefault(p["team"], []).append(gs_player_dict(p))
        gs_by_league = {lg: [{"team": t, "players": pl} for t, pl in sorted(tms.items())]
                        for lg, tms in gs_by_league.items()}

    log.info(f"Goal scorer: {len(gs_top25_list)} top25, {len(gs_all_list)} total")

    # ── TSOA aggregation ──────────────────────────────────────────────────────
    tsoa_top25_list, tsoa_all_list, tsoa_by_league = [], [], {}

    if not df.empty and not gs_df.empty:
        assist_lookup = {str(r.get("player_id","")): r for _, r in df.iterrows()
                         if r.get("player_id")}
        gs_lookup     = {str(r.get("player_id","")): r for _, r in gs_df.iterrows()
                         if r.get("player_id")}
        all_pids      = set(assist_lookup) | set(gs_lookup)

        tsoa_rows = []
        for pid in all_pids:
            ar, gr = assist_lookup.get(pid), gs_lookup.get(pid)
            base   = ar if ar is not None else gr
            tname  = str(base.get("team",""))
            tid    = str(base.get("team_id",""))
            league = str(base.get("league","") or tlg.get(tid,""))
            player = str(base.get("player",""))

            xg_per90  = safe_float(gr.get("xg_per90",0))  if gr is not None else 0.0
            xgot_gap  = safe_float(gr.get("xgot_gap",0))  if gr is not None else 0.0
            xa_per90  = safe_float(ar.get("xa_per90",0))  if ar is not None else 0.0
            xa_gap    = safe_float(ar.get("xa_gap",0))     if ar is not None else 0.0
            bc_combined = (safe_float(ar.get("big_chances",0)) if ar is not None else 0.0) + \
                          (safe_float(gr.get("big_chances_missed",0)) if gr is not None else 0.0)

            score = tsoa_score(xg_per90, xa_per90, xgot_gap, xa_gap, bc_combined)
            if score <= 0: continue

            form, next_opp = team_form.get(tname, []), team_next_opp.get(tname)
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
            tsoa_by_league.setdefault(lg, {}).setdefault(p["team"], []).append(p)
        tsoa_by_league = {lg: [{"team": t, "players": pl} for t, pl in sorted(tms.items())]
                          for lg, tms in tsoa_by_league.items()}

    log.info(f"TSOA: {len(tsoa_top25_list)} top25, {len(tsoa_all_list)} total")
    log.info(f"Scraper done — {len(top25_list)} players, {len(by_league)} leagues")

    return (top25_list, by_league, fixtures, all_players_list, teams_list,
            gs_top25_list, gs_all_list, gs_by_league,
            tsoa_top25_list, tsoa_all_list, tsoa_by_league)
