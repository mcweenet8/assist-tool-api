# =============================================================================
# standings.py — Standings fetching and formatting
# =============================================================================

import asyncio, logging
from .utils import LEAGUES, WEAK_DEF_THRESH, safe_float, team_logo_url

log = logging.getLogger(__name__)


async def get_standings(fotmob, league_name, league_id):
    await asyncio.sleep(1.5)
    rows = []
    log.info(f"standings {league_name} (id={league_id}): fetching...")
    try:
        data = await fotmob.standings(league_id)
        if not isinstance(data, list) or not data:
            log.warning(f"standings {league_name}: empty response")
            return []

        all_team_rows_in_league = []
        home_lkp_combined = {}
        away_lkp_combined = {}

        for item in data:
            if not isinstance(item, dict): continue
            item_data = item.get("data", {})
            table     = item_data.get("table", {})
            has_sub   = bool(item_data.get("tables", []))
            all_rows  = [] if has_sub else table.get("all", [])

            if not all_rows:
                tables_list = item_data.get("tables", [])
                for t_idx, t_item in enumerate(tables_list):
                    if isinstance(t_item, dict):
                        sub_table = t_item.get("table", {})
                        conf_name = t_item.get("leagueName",
                                    t_item.get("name", t_item.get("title", "")))
                        conf_teams = sub_table.get("all", [])
                        for t in conf_teams:
                            if isinstance(t, dict):
                                t["_conference"] = conf_name
                        all_rows.extend(conf_teams)
                        home_lkp_combined.update({str(t.get("id","")): t
                            for t in sub_table.get("home", []) if isinstance(t, dict)})
                        away_lkp_combined.update({str(t.get("id","")): t
                            for t in sub_table.get("away", []) if isinstance(t, dict)})

            home_lkp_combined.update({str(t.get("id","")): t
                for t in table.get("home", []) if isinstance(t, dict)})
            away_lkp_combined.update({str(t.get("id","")): t
                for t in table.get("away", []) if isinstance(t, dict)})
            all_team_rows_in_league.extend([t for t in all_rows if isinstance(t, dict)])

        home_lkp = home_lkp_combined
        away_lkp = away_lkp_combined

        # Deduplicate by team ID
        seen_ids = set()
        deduped  = []
        for t in all_team_rows_in_league:
            tid = str(t.get("id", ""))
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                deduped.append(t)
        all_team_rows_in_league = deduped

        log.info(f"standings {league_name}: {len(all_team_rows_in_league)} raw rows")

        for team in all_team_rows_in_league:
            if not isinstance(team, dict): continue
            name   = (team.get("name") or team.get("shortName", "")).strip()
            tid    = str(team.get("id", ""))
            played = safe_float(team.get("played", 0))
            try:    gf, ga = [safe_float(x) for x in team.get("scoresStr","0-0").split("-")]
            except: gf, ga = 0.0, 0.0
            ht   = home_lkp.get(tid, {})
            at   = away_lkp.get(tid, {})
            h_pl = safe_float(ht.get("played", played / 2))
            a_pl = safe_float(at.get("played", played / 2))
            try:    gf_h, ga_h = [safe_float(x) for x in ht.get("scoresStr","0-0").split("-")]
            except: gf_h, ga_h = 0.0, 0.0
            try:    gf_a, ga_a = [safe_float(x) for x in at.get("scoresStr","0-0").split("-")]
            except: gf_a, ga_a = 0.0, 0.0
            if name and played > 0:
                ga_pg   = round(ga   / played, 2)
                ga_h_pg = round(ga_h / h_pl, 2) if h_pl > 0 else 0.0
                ga_a_pg = round(ga_a / a_pl, 2) if a_pl > 0 else 0.0
                gf_pg   = round(gf   / played, 2)
                gf_h_pg = round(gf_h / h_pl, 2) if h_pl > 0 else 0.0
                gf_a_pg = round(gf_a / a_pl, 2) if a_pl > 0 else 0.0
                rows.append({
                    "team":       name,
                    "team_id":    tid,
                    "league":     league_name,
                    "conference": team.get("_conference", ""),
                    "table_pos":  safe_float(team.get("idx", 99)),
                    "played":     int(played),
                    "gf":         int(safe_float(gf)),
                    "ga":         int(safe_float(ga)),
                    "gf_pg":      gf_pg,
                    "ga_pg":      ga_pg,
                    "gf_h_pg":    gf_h_pg,
                    "ga_h_pg":    ga_h_pg,
                    "gf_a_pg":    gf_a_pg,
                    "ga_a_pg":    ga_a_pg,
                    "home_adv":   round(gf_h_pg - gf_a_pg, 2),
                    "away_vuln":  round(ga_a_pg - ga_h_pg, 2),
                    "goals_scored_pg": gf_pg,
                    "weak_def":   ga_pg >= WEAK_DEF_THRESH,
                })

        log.info(f"standings {league_name}: {len(rows)} teams")

    except Exception as e:
        log.error(f"standings {league_name} EXCEPTION: {type(e).__name__}: {e}")
    return rows


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
Done
Step 4: Commit changes.

Tell me when done.



