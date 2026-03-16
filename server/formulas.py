# =============================================================================
# formulas.py — Scoring formulas for Assists, Goals, TSOA
# =============================================================================


def tsoa_score(xg_per90, xa_per90, xgot_gap, xa_gap, bc_combined):
    """TSOA score — rewards dual threats (scorers AND creators)."""
    raw = (
        (xg_per90    * 0.25) +
        (xa_per90    * 0.25) +
        (xgot_gap    * 0.20) +
        (xa_gap      * 0.20) +
        (bc_combined * 0.10)
    )
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
        (xgot_gap           * 0.35) +
        (sot_per90          * 0.25) +
        (xg_per90           * 0.20) +
        (big_chances_missed * 0.20), 2)


def combined_score(xa_gap, cc_pg, big_chances, penalties_won,
                   opp_ga_pg=0.0, l5_xa=0.0):
    """Assist score formula."""
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
Done
Step 4: Commit changes.

Tell me when done.



Want to be notified when
