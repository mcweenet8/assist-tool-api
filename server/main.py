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
