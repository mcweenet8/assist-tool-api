# =============================================================================
# ASSIST RESEARCH TOOL — RAILWAY SERVER
# =============================================================================
# Permanent backend — runs 24/7 on Railway
# No Google Drive, no Colab — pure Python + Flask
#
# Endpoints:
#   GET  /status   — health check + last refresh time
#   GET  /data     — cached player + team data as JSON
#   POST /refresh  — re-run scraper, update cache, return fresh data
# =============================================================================

import asyncio, aiohttp, os, logging
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

MIN_GOALS_PG = 1.2
MAX_PLAYERS  = 25

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

STAT_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)"
                   " AppleWebKit/605.1.15"),
    "Accept":  "application/json",
    "Referer": "https://www.fotmob.com/",
    "x-mas":   "",  # populated at runtime from fotmob wrapper
}

# ── Cache ─────────────────────────────────────────────────────────────────────

_cache = {
    "last_updated": None,
    "top25":        [],
    "by_league":    {},
    "status":       "never_run",
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

# ── FotMob data fetchers ──────────────────────────────────────────────────────

async def get_standings(fotmob, league_name, league_id):
    await asyncio.sleep(1.5)
    rows = []
    try:
        data = await fotmob.standings(league_id)
        if not isinstance(data, list) or not data:
            log.warning(f"standings {league_name}: no data")
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
                try:    gf_h, _ = [safe_float(x) for x in ht.get("scoresStr","0-0").split("-")]
                except: gf_h = 0.0
                if name and played > 0:
                    rows.append({
                        "team":      name,
                        "team_id":   tid,
                        "league":    league_name,
                        "table_pos": safe_float(team.get("idx", 99)),
                        "played":    int(played),
                        "gf_pg":     round(gf / played, 2),
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


async def fetch_stat(session, league_id, season_id, stat_name, all_team_ids):
    url = (f"https://data.fotmob.com/stats/{league_id}"
           f"/season/{season_id}/{stat_name}.json")
    rows = []
    try:
        async with session.get(url, headers=HEADERS,
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                log.warning(f"fetch_stat {stat_name}: HTTP {resp.status}")
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

# ── Scraper ───────────────────────────────────────────────────────────────────

async def run_scraper():
    import pandas as pd
    from fotmob import FotMob

    log.info("Scraper starting...")
    all_team_rows   = []
    all_player_rows = []

    async with FotMob() as fotmob:

        # Pass 1 — Standings via fotmob wrapper (handles auth token)
        for league_name, info in LEAGUES.items():
            rows = await get_standings(fotmob, league_name, info["id"])
            all_team_rows.extend(rows)

        if not all_team_rows:
            raise RuntimeError("No standings data — FotMob may be blocking requests")

        teams_df     = pd.DataFrame(all_team_rows)
        all_team_ids = set(str(tid) for tid in teams_df["team_id"].tolist())
        log.info(f"Standings: {len(teams_df)} teams")

        # Pass 2 — Season IDs via fotmob wrapper
        league_seasons = {}
        for league_name, info in LEAGUES.items():
            lt = teams_df[teams_df["league"] == league_name]
            if lt.empty: continue
            await asyncio.sleep(0.8)
            sid = await get_season_id(fotmob, lt.iloc[0]["team_id"])
            if sid:
                league_seasons[info["id"]] = sid
                log.info(f"Season ID {league_name}: {sid}")
            else:
                log.warning(f"No season ID for {league_name}")

        # Pass 3 — Player stats using fotmob wrapper session for auth
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

    if not all_player_rows:
        raise RuntimeError("No player data — season IDs may be wrong or endpoint blocked")

    # Aggregate
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
                "assists":         0.0, "xa": 0.0, "xa_per90":       0.0,
                "big_chances":     0.0, "chances_created": 0.0,
                "penalties_won":   0.0,
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

    log.info(f"Scraper done — {len(top25_list)} top players, {len(by_league)} leagues")
    return top25_list, by_league

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

@app.route("/refresh", methods=["GET", "POST"])
def refresh():
    _cache["status"] = "refreshing"
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        top25, by_league = loop.run_until_complete(run_scraper())
        loop.close()
        _cache["top25"]        = top25
        _cache["by_league"]    = by_league
        _cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        _cache["status"]       = "ok"
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

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Starting server on port {port}")
    app.run(host="0.0.0.0", port=port)
