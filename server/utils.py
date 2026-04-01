# =============================================================================
# utils.py — Constants, config, image URLs, cache, helpers
# =============================================================================
import os, json, logging
from datetime import datetime
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
APP_VERSION     = "3.0.0"
MIN_GOALS_PG    = 1.2
MAX_PLAYERS     = 25
WEAK_DEF_THRESH = 1.5
FORM_MATCHES    = 5
LEAGUES = {
    "Premier League": {"id": 47,     "short": "PL"},
    "Championship":   {"id": 48,     "short": "Champ",    "fixture_id": 900638},
    "La Liga":        {"id": 87,     "short": "LaLiga"},
    "Serie A":        {"id": 55,     "short": "SerieA"},
    "Bundesliga":     {"id": 54,     "short": "Bundesliga"},
    "Ligue 1":        {"id": 53,     "short": "Ligue1"},
    "MLS":            {"id": 130,    "short": "MLS",      "fixture_id": 913550},
    "A-League Men":   {"id": 901954, "short": "ALeague",  "fixture_id": 901954},
}
FIXTURE_ONLY_LEAGUES = {
    "Champions League":  {"id": 42,    "short": "UCL"},
    "Europa League":     {"id": 73,    "short": "UEL"},
    "Conference League": {"id": 10216, "short": "UECL"},
}
STATS = {
    "goal_assist":             "assists",
    "expected_assists":        "xa",
    "big_chance_created":      "big_chances",
    "total_att_assist":        "chances_created",
    "expected_assists_per_90": "xa_per90",
    "penalty_won":             "penalties_won",
}
GS_STATS = {
    "goals":                   "goals",
    "expected_goals":          "xg",
    "expected_goals_per_90":   "xg_per90",
    "expected_goalsontarget":  "xgot",
    "ontarget_scoring_att":    "sot_per90",
    "total_scoring_att":       "shots_per90",
    "big_chance_missed":       "big_chances_missed",
}
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)"
                   " AppleWebKit/605.1.15"),
    "Accept":  "application/json",
    "Referer": "https://www.fotmob.com/",
}

# ── Cache — Redis-backed with in-memory fallback ──────────────────────────────

class RedisCache:
    _DEFAULTS = {
        "last_updated":     None,
        "top25":            [],
        "by_league":        {},
        "fixtures":         {},
        "fixtures_updated": None,
        "all_players":      [],
        "gs_top25":         [],
        "gs_all":           [],
        "gs_by_league":     {},
        "tsoa_top25":       [],
        "tsoa_all":         [],
        "tsoa_by_league":   {},
        "teams":            [],
        "status":           "never_run",
    }

    def __init__(self):
        self._local = dict(self._DEFAULTS)
        self._redis = None
        self._connect()

    def _connect(self):
        redis_url = os.environ.get("REDIS_URL")
        if not redis_url:
            log.info("No REDIS_URL — using in-memory cache")
            return
        try:
            import redis
            self._redis = redis.from_url(
                redis_url, decode_responses=True,
                socket_timeout=2, socket_connect_timeout=2,
            )
            self._redis.ping()
            log.info("Redis cache connected")
        except Exception as e:
            log.warning(f"Redis unavailable, using in-memory cache: {e}")
            self._redis = None

    def _serialize(self, value):
        try:
            return json.dumps(value, default=str)
        except Exception:
            return None

    def _deserialize(self, value):
        try:
            return json.loads(value)
        except Exception:
            return value

    def get(self, key, default=None):
        if key in self._local and self._local[key] is not None:
            return self._local[key]
        if self._redis:
            try:
                val = self._redis.get(f"dc:{key}")
                if val is not None:
                    result = self._deserialize(val)
                    self._local[key] = result
                    return result
            except Exception:
                pass
        return default if default is not None else self._DEFAULTS.get(key)

    def set(self, key, value, ttl=None):
        self._local[key] = value
        if self._redis and not isinstance(value, datetime):
            try:
                serialized = self._serialize(value)
                if serialized:
                    if ttl:
                        self._redis.setex(f"dc:{key}", ttl, serialized)
                    else:
                        self._redis.set(f"dc:{key}", serialized)
            except Exception as e:
                log.warning(f"Redis write error {key}: {e}")

    def __setitem__(self, key, value):
        self.set(key, value)

    def __getitem__(self, key):
        val = self.get(key)
        if val is None and key not in self._local:
            raise KeyError(key)
        return val

    def __contains__(self, key):
        if key in self._local and self._local[key] is not None:
            return True
        if self._redis:
            try:
                return self._redis.exists(f"dc:{key}") > 0
            except Exception:
                pass
        return False

    def __delitem__(self, key):
        self._local.pop(key, None)
        if self._redis:
            try:
                self._redis.delete(f"dc:{key}")
            except Exception:
                pass

    def pop(self, key, *args):
        val = self._local.pop(key, *args)
        if self._redis:
            try:
                self._redis.delete(f"dc:{key}")
            except Exception:
                pass
        return val


_cache = RedisCache()

# ── Image URLs ────────────────────────────────────────────────────────────────
def player_img_url(player_id):
    if not player_id: return None
    return f"https://images.fotmob.com/image_resources/playerimages/{player_id}.png"

def team_logo_url(team_id):
    if not team_id: return None
    return f"https://images.fotmob.com/image_resources/logo/teamlogo/{team_id}.png"

def league_logo_url(league_id):
    if not league_id: return None
    return f"https://images.fotmob.com/image_resources/logo/leaguelogo/{league_id}.png"

# ── Helpers ───────────────────────────────────────────────────────────────────
def safe_float(v):
    try:
        return float(str(v).replace(",", "").strip()) \
            if v not in [None, "", " ", "-", "N/A"] else 0.0
    except:
        return 0.0
