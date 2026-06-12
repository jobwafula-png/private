"""
Polymarket Weather Discord Bot
fps.ms | discord.py | Python 3.10+

Sources (all free, no keys except Discord token):
  1. aviationweather.gov  — METAR obs per station
  2. Open-Meteo           — M1 ECMWF + M2 regional forecast
  3. National NWS APIs    — M3 local per city (see dispatcher)
  4. Polymarket Gamma API — open max-temp markets

No Weather Underground. No Polynimbus. No auto-trading.
Bot observes real data, computes consensus max temp,
compares to market price, posts edge alerts to Discord.
"""

import asyncio, os, re, logging, sys, json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# ── aiodns nuclear fix (must be before aiohttp is used) ──────────────────────
# aiodns crashes on Python 3.12+ Windows. Force aiohttp to use the threaded
# resolver (Python's built-in getaddrinfo) everywhere, including discord.py's
# internal HTTP session which we cannot access directly.
try:
    import aiodns as _aiodns_mod          # confirm it exists
    import aiohttp.resolver as _res
    # Swap AsyncResolver → ThreadedResolver at class level
    _res.AsyncResolver = _res.ThreadedResolver
    # Also neutralise the aiodns module so nothing can re-enable it
    sys.modules['aiodns'] = None          # type: ignore
except (ImportError, AttributeError):
    pass  # aiodns not installed — already safe
# ─────────────────────────────────────────────────────────────────────────────

import aiohttp
import discord
from discord.ext import tasks
from dotenv import load_dotenv
from scipy import stats

load_dotenv()

DISCORD_TOKEN    = os.getenv("DISCORD_TOKEN")
ALERT_CHANNEL_ID = int(os.getenv("ALERT_CHANNEL_ID", "0"))
MIN_EDGE         = float(os.getenv("MIN_EDGE", "0.08"))
POLL_MINUTES     = int(os.getenv("POLL_MINUTES", "30"))

# ── Demo / dry-run mode ───────────────────────────────────────────────────────
# DEMO_MODE=true  → paper-trade only; no real Polymarket CLOB orders ever sent.
# All "trades" are logged to data/demo_trades.json and posted to Discord with
# a [DEMO] prefix so you can track performance without risking capital.
# Set DEMO_MODE=false only when you have connected a real Polymarket wallet.
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() != "false"

# ── Persistent data directories ───────────────────────────────────────────────
DATA_DIR             = Path(os.getenv("DATA_DIR", "data"))
CALIBRATION_FILE     = DATA_DIR / "calibration.json"
DEMO_TRADES_FILE     = DATA_DIR / "demo_trades.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Deduplication intentionally REMOVED (v16).
# Every poll re-runs every city so the bot picks up market price shifts
# and updated METAR observations on every cycle.
_alerted: set[tuple[str, str]] = set()   # kept for compatibility — never written to

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")

# ─────────────────────────────────────────────────────────────
# 38 CITIES
# m2  = Open-Meteo model string for regional (M2)
# m3  = dispatcher key for local NWS function (M3)
# unit= F (US) or C (rest)
# ─────────────────────────────────────────────────────────────
CITIES = [
    # ── AMERICAS (same-day market analysis) ─────────────────────────────────
    # next_day=False: these cities share UTC-4 to UTC-8 offset → today's market
    dict(city="New York",      icao="KLGA", lat=40.7773,  lon=-73.8726, tz="America/New_York",               unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Los Angeles",   icao="KLAX", lat=33.9425,  lon=-118.408, tz="America/Los_Angeles",            unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Chicago",       icao="KORD", lat=41.9742,  lon=-87.9073, tz="America/Chicago",                unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Miami",         icao="KMIA", lat=25.7959,  lon=-80.2870, tz="America/New_York",               unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Dallas",        icao="KDFW", lat=32.8998,  lon=-97.0403, tz="America/Chicago",                unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Houston",       icao="KIAH", lat=29.9844,  lon=-95.3414, tz="America/Chicago",                unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Atlanta",       icao="KATL", lat=33.6367,  lon=-84.4281, tz="America/New_York",               unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Denver",        icao="KDEN", lat=39.8561,  lon=-104.676, tz="America/Denver",                 unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Seattle",       icao="KSEA", lat=47.4502,  lon=-122.309, tz="America/Los_Angeles",            unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="San Francisco", icao="KSFO", lat=37.6213,  lon=-122.379, tz="America/Los_Angeles",            unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Austin",        icao="KAUS", lat=30.1975,  lon=-97.6664, tz="America/Chicago",                unit="F", m2="best_match",                m3="nws",          region="americas", next_day=False),
    dict(city="Toronto",       icao="CYYZ", lat=43.6777,  lon=-79.6248, tz="America/Toronto",                unit="C", m2="gem_seamless",              m3="eccc",         region="americas", next_day=False),
    dict(city="Buenos Aires",  icao="SAEZ", lat=-34.8222, lon=-58.5358, tz="America/Argentina/Buenos_Aires", unit="C", m2="gfs_seamless",              m3="smn_ar",       region="americas", next_day=False),
    dict(city="Sao Paulo",     icao="SBGR", lat=-23.4356, lon=-46.4731, tz="America/Sao_Paulo",              unit="C", m2="gfs_seamless",              m3="inmet",        region="americas", next_day=False),
    dict(city="Mexico City",   icao="MMMX", lat=19.4363,  lon=-99.0721, tz="America/Mexico_City",            unit="C", m2="gfs_seamless",              m3="smn_mx",       region="americas", next_day=False),
    dict(city="Panama City",   icao="MPTO", lat=9.0714,   lon=-79.3835, tz="America/Panama",                  unit="C", m2="gfs_seamless",              m3="brightsky",    region="americas", next_day=False, bs_lat=9.0714,   bs_lon=-79.3835),

    # ── EUROPE (same-day market analysis) ───────────────────────────────────
    # next_day=False: UTC to UTC+3 — their trading day is today from our perspective
    dict(city="London",        icao="EGLC", lat=51.5048,  lon=0.0495,   tz="Europe/London",                  unit="C", m2="icon_eu",                   m3="brightsky",    region="europe",   next_day=False, bs_lat=51.5048, bs_lon=0.0495),
    dict(city="Paris",         icao="LFPG", lat=49.0097,  lon=2.5479,   tz="Europe/Paris",                   unit="C", m2="meteofrance_arome_france_hd",m3="brightsky",    region="europe",   next_day=False, bs_lat=49.0097, bs_lon=2.5479),
    dict(city="Madrid",        icao="LEMD", lat=40.4719,  lon=-3.5626,  tz="Europe/Madrid",                  unit="C", m2="meteofrance_arome_france_hd",m3="brightsky",    region="europe",   next_day=False, bs_lat=40.4719, bs_lon=-3.5626),
    dict(city="Milan",         icao="LIMC", lat=45.6306,  lon=8.7236,   tz="Europe/Rome",                    unit="C", m2="icon_d2",                   m3="brightsky",    region="europe",   next_day=False, bs_lat=45.6306, bs_lon=8.7236),
    dict(city="Munich",        icao="EDDM", lat=48.3537,  lon=11.7750,  tz="Europe/Berlin",                  unit="C", m2="icon_d2",                   m3="brightsky",    region="europe",   next_day=False, bs_lat=48.3537, bs_lon=11.7750),
    dict(city="Warsaw",        icao="EPWA", lat=52.1657,  lon=20.9671,  tz="Europe/Warsaw",                  unit="C", m2="icon_d2",                   m3="imgw",         region="europe",   next_day=False),
    dict(city="Amsterdam",     icao="EHAM", lat=52.3086,  lon=4.7639,   tz="Europe/Amsterdam",                unit="C", m2="icon_d2",                   m3="brightsky",    region="europe",   next_day=False, bs_lat=52.3086,  bs_lon=4.7639),
    dict(city="Helsinki",      icao="EFHK", lat=60.3172,  lon=24.9633,  tz="Europe/Helsinki",                 unit="C", m2="icon_eu",                   m3="brightsky",    region="europe",   next_day=False, bs_lat=60.3172,  bs_lon=24.9633),
    dict(city="Istanbul",      icao="LTFM", lat=41.2753,  lon=28.7519,  tz="Europe/Istanbul",                unit="C", m2="icon_eu",                   m3="mgm",          region="europe",   next_day=False, mgm_id=17060),
    dict(city="Ankara",        icao="LTAC", lat=40.1281,  lon=32.9951,  tz="Europe/Istanbul",                unit="C", m2="icon_eu",                   m3="mgm",          region="europe",   next_day=False, mgm_id=17130),
    dict(city="Tel Aviv",      icao="LLBG", lat=31.9965,  lon=34.8854,  tz="Asia/Jerusalem",                 unit="C", m2="gfs_seamless",              m3="ims",          region="europe",   next_day=False),
    dict(city="Moscow",        icao="UUWW", lat=55.5915,  lon=37.2615,  tz="Europe/Moscow",                  unit="C", m2="icon_eu",                   m3="gismeteo",     region="europe",   next_day=False, gis_id=4368),

    # Asia-Pacific cities removed — replaced by US cities
    # ── AFRICA (UTC+2 — same-day) ───────────────────────────────────────────
    dict(city="Cape Town",     icao="FACT", lat=-33.9715, lon=18.6021,  tz="Africa/Johannesburg",             unit="C", m2="gfs_seamless",              m3="brightsky",    region="europe",   next_day=False, bs_lat=-33.9715, bs_lon=18.6021),
]

CITY_MAP  = {c["city"]: c for c in CITIES}
ICAO_MAP  = {c["icao"]: c for c in CITIES}


# ─────────────────────────────────────────────────────────────
# HTTP HELPER
# ─────────────────────────────────────────────────────────────
async def get(session: aiohttp.ClientSession, url: str) -> dict | list | None:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            log.warning(f"HTTP {r.status}: {url[:80]}")
    except aiohttp.ClientConnectorDNSError as e:
        log.error(f"DNS resolution failed for {url[:60]}: {e}. Check network/DNS settings.")
    except Exception as e:
        log.warning(f"fetch error {url[:60]}: {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# CALIBRATION TRACKER
# ─────────────────────────────────────────────────────────────────────────────
# Every time the bot generates a signal, it logs:
#   {date, mu (consensus forecast), sigma (used), actual (filled in later)}
#
# Once a city has ≥ MIN_CALIB_SAMPLES resolved markets:
#   • Rolling 14-day MAE   → replaces fixed sigma=1.0 with empirical forecast error
#   • Rolling 14-day bias  → mean signed error (mu − actual); added to mu as correction
#
# Usage in _process_one_city():
#   mu_adj, sigma_adj = calib.adjusted(city_name, mu, sigma)
#
# To record an actual outcome (call this manually or via a future !resolve command):
#   calib.record_actual(city_name, date_str, actual_temp)
#
# Progress is tracked in data/calibration.json — one entry per city per market.
# ─────────────────────────────────────────────────────────────────────────────
MIN_CALIB_SAMPLES = 30   # minimum resolved markets before calibration activates
CALIB_WINDOW      = 14   # rolling window in calendar days for MAE + bias calc


class CalibrationTracker:
    """
    Persistent per-city forecast calibration log.

    JSON schema (data/calibration.json):
    {
      "Austin": [
        {"date": "2026-06-10", "mu": 93.4, "sigma": 1.0,
         "actual": 92.0, "error": 1.4, "unit": "F"},
        ...
      ],
      ...
    }
    Records without "actual" are pending (market not yet resolved).

    Once a city has ≥ MIN_CALIB_SAMPLES resolved entries:
      rolling_bias = mean(mu - actual) over last CALIB_WINDOW days
      rolling_mae  = mean(|mu - actual|) over last CALIB_WINDOW days
      → adjusted mu  = mu - rolling_bias
      → adjusted sigma = max(rolling_mae, 0.5)   (floor 0.5 to avoid over-confidence)
    """

    def __init__(self, path: Path):
        self._path = path
        self._data: dict[str, list] = {}
        self._load()

    def _load(self):
        try:
            if self._path.exists():
                with open(self._path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
        except Exception as e:
            log.warning(f"CalibrationTracker: could not load {self._path}: {e}")
            self._data = {}

    def _save(self):
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.warning(f"CalibrationTracker: could not save {self._path}: {e}")

    def log_prediction(self, city: str, date_str: str, mu: float,
                       sigma: float, unit: str):
        """Record a new prediction. Call once per signal, before sending embed."""
        if city not in self._data:
            self._data[city] = []
        # Avoid duplicating the same city+date on every 30-min poll
        for entry in self._data[city]:
            if entry.get("date") == date_str:
                entry["mu"]    = round(mu, 2)     # update with latest consensus
                entry["sigma"] = round(sigma, 2)
                self._save()
                return
        self._data[city].append({
            "date":   date_str,
            "mu":     round(mu, 2),
            "sigma":  round(sigma, 2),
            "actual": None,
            "error":  None,
            "unit":   unit,
        })
        self._save()
        log.info(f"  Calibration: logged prediction {city} {date_str} mu={mu:.2f} sigma={sigma:.2f}")

    def record_actual(self, city: str, date_str: str, actual: float):
        """
        Fill in the actual resolved temperature for a city on a given date.
        Call this after Polymarket resolves, either manually or via a bot command.
        """
        entries = self._data.get(city, [])
        for entry in entries:
            if entry.get("date") == date_str:
                entry["actual"] = round(actual, 2)
                entry["error"]  = round(entry["mu"] - actual, 2)
                self._save()
                log.info(f"  Calibration: recorded actual {city} {date_str} actual={actual:.2f} error={entry['error']:+.2f}")
                return
        log.warning(f"  Calibration: no prediction found for {city} {date_str} to record actual")

    def _resolved_window(self, city: str) -> list[dict]:
        """Return resolved entries (have 'actual') within the last CALIB_WINDOW days."""
        from datetime import date as _date, timedelta as _td
        cutoff = (_date.today() - _td(days=CALIB_WINDOW)).isoformat()
        return [
            e for e in self._data.get(city, [])
            if e.get("actual") is not None and e.get("date", "") >= cutoff
        ]

    def resolved_count(self, city: str) -> int:
        """Total number of resolved (actual known) entries for a city (all time)."""
        return sum(1 for e in self._data.get(city, []) if e.get("actual") is not None)

    def adjusted(self, city: str, mu: float, sigma: float) -> tuple[float, float]:
        """
        Return (mu_adjusted, sigma_adjusted).

        If the city has < MIN_CALIB_SAMPLES resolved markets:
          → returns (mu, sigma) unchanged (fixed 1.0°F/°C sigma from consensus())

        Once it has ≥ MIN_CALIB_SAMPLES:
          → mu    adjusted by rolling 14-day bias (signed mean error)
          → sigma replaced by rolling 14-day MAE (capped at minimum 0.5)

        The bias correction is the exact mechanism described by the ABC paper:
        site- and date-specific offsets that minimize historical forecasting error
        over an adaptively selected window.
        """
        resolved_all = [e for e in self._data.get(city, []) if e.get("actual") is not None]
        if len(resolved_all) < MIN_CALIB_SAMPLES:
            return mu, sigma   # not enough data yet

        window = self._resolved_window(city)
        if not window:
            return mu, sigma   # all data is older than CALIB_WINDOW days

        errors        = [e["error"] for e in window]          # mu − actual
        abs_errors    = [abs(e) for e in errors]
        rolling_bias  = sum(errors) / len(errors)             # positive = bot over-forecasts
        rolling_mae   = sum(abs_errors) / len(abs_errors)

        mu_adj    = round(mu - rolling_bias, 2)
        sigma_adj = round(max(rolling_mae, 0.5), 2)

        log.info(
            f"  Calibration: {city} n={len(resolved_all)} "
            f"bias={rolling_bias:+.2f} mae={rolling_mae:.2f} "
            f"→ mu {mu:.2f}→{mu_adj:.2f} sigma {sigma:.2f}→{sigma_adj:.2f}"
        )
        return mu_adj, sigma_adj

    def summary(self, city: str) -> str:
        """Short one-line calibration status for embed footer / Discord."""
        n = self.resolved_count(city)
        if n < MIN_CALIB_SAMPLES:
            return f"📊 Calibration: {n}/{MIN_CALIB_SAMPLES} resolved markets (fixed σ)"
        window = self._resolved_window(city)
        if not window:
            return f"📊 Calibration: {n} resolved, window empty — fixed σ"
        mae  = sum(abs(e["error"]) for e in window) / len(window)
        bias = sum(e["error"] for e in window) / len(window)
        return (
            f"📊 Calibration: {n} resolved | "
            f"MAE={mae:.2f}° (σ) | bias={bias:+.2f}° | "
            f"window={len(window)}d"
        )


# Global calibration tracker (single instance for the bot process)
calib = CalibrationTracker(CALIBRATION_FILE)


# ─────────────────────────────────────────────────────────────────────────────
# DEMO TRADE LOGGER (paper trading / dry-run)
# ─────────────────────────────────────────────────────────────────────────────
# Records every "trade" to data/demo_trades.json.
# NEVER touches Polymarket CLOB. NEVER sends real orders.
# Posts a [DEMO] embed to Discord so you can evaluate performance.
#
# JSON schema (data/demo_trades.json):
# [
#   {
#     "ts":         "2026-06-10T11:54:00Z",
#     "city":       "Austin",
#     "date":       "2026-06-10",
#     "market_q":   "Will the highest temperature in Austin be...",
#     "action":     "BUY YES",
#     "label":      "92–93°F",
#     "edge":       0.545,
#     "kelly_pct":  5.0,
#     "mu":         93.4,
#     "sigma":      1.0,
#     "unit":       "F",
#     "calib_n":    12,
#     "resolved":   null,     ← filled by record_actual later
#     "pnl":        null
#   },
#   ...
# ]
# ─────────────────────────────────────────────────────────────────────────────

class DemoTradeLogger:
    """
    Paper-trade ledger for dry-run mode.

    All methods are synchronous (no async needed — JSON I/O is fast).
    Call log_trade() at the point where a real bot would call the CLOB API.
    """

    def __init__(self, path: Path):
        self._path = path
        self._trades: list[dict] = []
        self._load()

    def _load(self):
        try:
            if self._path.exists():
                with open(self._path, "r", encoding="utf-8") as f:
                    self._trades = json.load(f)
        except Exception as e:
            log.warning(f"DemoTradeLogger: could not load {self._path}: {e}")
            self._trades = []

    def _save(self):
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._trades, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.warning(f"DemoTradeLogger: could not save {self._path}: {e}")

    def log_trade(self, city: str, date_str: str, market_q: str,
                  action: str, label: str, edge: float, kelly_pct: float,
                  mu: float, sigma: float, unit: str, calib_n: int) -> dict:
        """Record a paper trade and return the trade dict."""
        trade = {
            "ts":        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "city":      city,
            "date":      date_str,
            "market_q":  market_q,
            "action":    action,
            "label":     label,
            "edge":      round(edge, 4),
            "kelly_pct": round(kelly_pct, 2),
            "mu":        round(mu, 2),
            "sigma":     round(sigma, 2),
            "unit":      unit,
            "calib_n":   calib_n,
            "resolved":  None,
            "pnl":       None,
        }
        self._trades.append(trade)
        self._save()
        log.info(
            f"  [DEMO] Trade logged: {city} {action} {label} "
            f"edge={edge:.1%} kelly={kelly_pct:.1f}%"
        )
        return trade

    def stats(self) -> dict:
        """Return summary stats: total, resolved, win_rate, mean_pnl."""
        resolved = [t for t in self._trades if t.get("pnl") is not None]
        wins     = [t for t in resolved if (t.get("pnl") or 0) > 0]
        return {
            "total":    len(self._trades),
            "resolved": len(resolved),
            "wins":     len(wins),
            "win_rate": len(wins) / len(resolved) if resolved else None,
            "mean_pnl": sum(t["pnl"] for t in resolved) / len(resolved) if resolved else None,
        }


# Global demo trade logger
demo_ledger = DemoTradeLogger(DEMO_TRADES_FILE)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — POLYMARKET GAMMA API
# ─────────────────────────────────────────────────────────────

# Title keywords that confirm a market is about air temperature
_TEMP_KEYWORDS = [
    "°f", "°c", "℃", "℉",
    "degrees fahrenheit", "degrees celsius",
    "high temperature", "max temperature", "maximum temperature",
    "daily high", "daily max", "highest temp",
    "high temp", "max temp", "temp exceed", "daily maximum",
    "will the high", "will it be above", "will it reach", "will it hit",
    "temperature above", "temperature exceed", "temperature reach",
    "fahrenheit", "celsius",
]

# Slugs / titles that are definitely NOT weather
_EXCLUDE_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "crypto", "album", "movie",
    "trump", "election", "president", "senate", "congress",
    "nba", "nhl", "nfl", "fifa", "world cup", "oscar",
    "grammy", "prison", "sentenced", "invasion", "airdrop",
    "gta", "stock", "rate hike", "gdp", "inflation",
    # Exclude lowest/minimum temperature markets — bot only trades highest/max
    "lowest temperature", "lowest temp", "minimum temperature", "min temperature",
    "will the lowest", "lowest-temperature", "minimum-temp",
    # Note: "war" and "award" removed — "war" hits Warsaw, "award" hits unrelated
]

# Known weather event slug patterns Polymarket uses — highest/max only
# (slugs are stable; if they change, the /events pagination fallback catches them)
_WEATHER_SLUG_PATTERNS = [
    "highest-temperature", "max-temperature", "maximum-temperature",
    "high-temperature", "daily-high", "temperature-above",
    "temperature-exceed", "degrees-fahrenheit", "degrees-celsius",
    "weather-", "-high-temp", "fahrenheit", "celsius",
    # Note: "-temperature-" intentionally omitted — too broad, matches lowest-temperature
    # Note: "temp-" intentionally omitted — matches lowest-temp slugs
]

# Slug patterns that belong to lowest/min temp markets — always exclude these
_LOWEST_SLUG_PATTERNS = [
    "lowest-temperature", "minimum-temperature", "min-temperature",
    "lowest-temp", "min-temp",
]


async def _fetch_events_page(session, offset: int, limit: int = 100) -> list:
    """Fetch one page of active events from Gamma /events endpoint."""
    url = (
        f"https://gamma-api.polymarket.com/events"
        f"?active=true&closed=false&limit={limit}&offset={offset}"
        f"&order=startDate&ascending=false"
    )
    data = await get(session, url)
    if not data:
        return []
    return data if isinstance(data, list) else []


def _is_weather_market(m: dict) -> bool:
    """Return True if market question is a HIGHEST/MAX temperature market.
    Explicitly excludes lowest/minimum temperature markets.
    """
    q    = m.get("question", "").lower()
    slug = m.get("slug",     "").lower()

    # Fast exclude: known non-weather topics + lowest/min temp markets
    if any(ex in q for ex in _EXCLUDE_KEYWORDS):
        return False

    # Exclude lowest-temp slugs before any slug-match accept
    if any(p in slug for p in _LOWEST_SLUG_PATTERNS):
        return False

    # Slug pattern match (highest/max only)
    if any(p in slug for p in _WEATHER_SLUG_PATTERNS):
        return True

    # Title keyword match — also guard against lowest/min in question
    if "lowest" in q or "minimum" in q or "will the low" in q:
        return False

    return any(k in q for k in _TEMP_KEYWORDS)


async def get_markets(session) -> list[dict]:
    """
    Fetch open Polymarket temperature/weather markets.

    Strategy (3-tier, most precise first):
    ─────────────────────────────────────
    Tier 1: /events?tag_slug=weather  — official weather tag (fastest)
    Tier 2: /events?slug=<known patterns>  — direct slug lookups for common series
    Tier 3: Paginate /events up to 500 events — catches anything not tagged

    All tiers extract the nested `markets` array from each event object,
    then apply _is_weather_market() filter before returning.

    The `/markets` search endpoint is NOT used — its `q=` param performs
    full-text search over all markets and returns random unrelated results
    regardless of the query string.
    """
    seen_event_ids: set = set()
    candidate_markets: list = []

    # ── TIER 1: official weather tag ────────────────────────────────────────
    # Polymarket tags events with 'weather'; tag_id for weather is typically
    # in the range used by their science/nature category.
    # We try tag_slug first (works even if tag_id changes).
    for slug_param in ["weather", "weather-markets", "temperature"]:
        url = (
            f"https://gamma-api.polymarket.com/events"
            f"?tag_slug={slug_param}&active=true&closed=false&limit=100"
        )
        data = await get(session, url)
        if not data:
            continue
        events = data if isinstance(data, list) else []
        for ev in events:
            eid = ev.get("id", "")
            if eid not in seen_event_ids:
                seen_event_ids.add(eid)
                candidate_markets.extend(ev.get("markets", []))

    # ── TIER 2: Search by city slug prefix — handles date-suffixed slugs ───────
    # Polymarket slugs are like "highest-temperature-in-los-angeles-on-june-8-2026"
    # Static exact slug lookups never match. Use slug_contains prefix instead.
    city_slug_prefixes = [
        "highest-temperature-in-los-angeles",
        "highest-temperature-in-chicago",
        "highest-temperature-in-miami",
        "highest-temperature-in-dallas",
        "highest-temperature-in-houston",
        "highest-temperature-in-atlanta",
        "highest-temperature-in-denver",
        "highest-temperature-in-seattle",
        "highest-temperature-in-san-francisco",
        "highest-temperature-in-austin",
        "highest-temperature-in-new-york",
        "highest-temperature-in-london",
        "highest-temperature-in-paris",
        "highest-temperature-in-madrid",
        "highest-temperature-in-amsterdam",
        "highest-temperature-in-helsinki",
        "highest-temperature-in-cape-town",
        "highest-temperature-in-panama",
        "highest-temperature-in-mexico-city",
        "highest-temperature-in-toronto",
        "highest-temperature-in-buenos-aires",
        "highest-temperature-in-sao-paulo",
        "highest-temperature-in-istanbul",
        "highest-temperature-in-ankara",
        "highest-temperature-in-moscow",
        "highest-temperature-in-warsaw",
        "highest-temperature-in-milan",
        "highest-temperature-in-munich",
        "highest-temperature-in-tel-aviv",
    ]
    for prefix in city_slug_prefixes:
        url = (f"https://gamma-api.polymarket.com/events"
               f"?slug_contains={prefix}&active=true&closed=false&limit=10")
        data = await get(session, url)
        if not data:
            continue
        events = data if isinstance(data, list) else []
        for ev in events:
            eid = ev.get("id", "")
            if eid not in seen_event_ids:
                seen_event_ids.add(eid)
                candidate_markets.extend(ev.get("markets", []))

    # ── TIER 3: paginate all recent events, filter by title ─────────────────
    # Scan up to 2000 events (20 pages × 100). Events are ordered by startDate
    # descending so fresh weather markets appear first.
    # v17: expanded city name list, fixed early-exit (was quitting after 5 pages
    # which missed most US cities), fixed broken line continuation syntax.
    _T3_CITY_NAMES = [
        "new york", "los angeles", "chicago", "miami", "atlanta",
        "dallas", "denver", "seattle", "san francisco", "houston", "austin",
        "amsterdam", "helsinki", "cape town", "panama", "mexico city",
        "london", "paris", "madrid", "milan", "munich", "warsaw",
        "toronto", "buenos aires", "sao paulo", "istanbul", "ankara",
        "tel aviv", "moscow",
    ]
    _T3_WEATHER_KEYS = [
        "temperature", "weather", "°f", "°c",
        "fahrenheit", "celsius", "daily high",
        "highest temp", "high temp", "max temp",
    ]
    consecutive_empty = 0
    for offset in range(0, 2000, 100):
        events = await _fetch_events_page(session, offset)
        if not events:
            break
        added = 0
        for ev in events:
            eid = ev.get("id", "")
            if eid in seen_event_ids:
                continue
            ev_title = (ev.get("title") or ev.get("slug") or "").lower()
            is_weather = any(k in ev_title for k in _T3_WEATHER_KEYS)
            is_city    = any(cn in ev_title for cn in _T3_CITY_NAMES)
            if is_weather or is_city:
                seen_event_ids.add(eid)
                candidate_markets.extend(ev.get("markets", []))
                added += 1
        if added == 0:
            consecutive_empty += 1
        else:
            consecutive_empty = 0
        # Only exit early after 5 consecutive empty pages AND past page 10
        if consecutive_empty >= 5 and offset >= 1000:
            break

    log.info(f"Gamma: {len(candidate_markets)} candidate markets from all tiers")

    # ── FINAL FILTER: apply _is_weather_market to deduplicated candidates ────
    seen_mids: set = set()
    out = []
    for m in candidate_markets:
        mid = m.get("id") or m.get("conditionId") or m.get("question", "")
        if mid in seen_mids:
            continue
        seen_mids.add(mid)
        if _is_weather_market(m):
            log.debug(f"  ✅ Weather market: {m.get('question','')[:90]}")
            out.append(m)

    if not out:
        log.info(
            "Gamma: 0 weather/temp markets found.\n"
            "  This likely means Polymarket has no open weather markets right now.\n"
            "  They typically post new daily markets around 12:00–14:00 UTC.\n"
            "  Bot will retry next poll."
        )
        # Log raw event titles from tier-3 scan to help diagnose
        for m in candidate_markets[:8]:
            log.info(f"  [raw candidate] {m.get('question','(no question)')[:100]}")
    else:
        log.info(f"Gamma: {len(out)} weather/temp markets matched")
        # Log first 20 matched market questions so we can see what US city names look like
        for m in out[:20]:
            log.info(f"  [matched] {m.get('question','')[:100]}")
    return out

def _parse_json_field(val):
    """
    Polymarket /events returns outcomes/prices/tokens as JSON strings, e.g.:
      '["Yes", "No"]'  or  '["0.6", "0.4"]'
    The /markets endpoint returns actual lists.
    Handle both so the bot works regardless of which endpoint sourced the market.
    """
    import json as _json
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        val = val.strip()
        if val.startswith("["):
            try:
                return _json.loads(val)
            except Exception:
                pass
    return []


def city_from_question(q: str) -> dict | None:
    """
    Match question text to a city config.
    Handles Polymarket's naming quirks:
      "New York City" → matches "New York"
      "Sao Paulo"     → matches "Sao Paulo"  (accent-stripped)
    """
    import unicodedata
    def _norm(s):
        # strip accents, lowercase
        return unicodedata.normalize("NFD", s.lower()).encode("ascii", "ignore").decode()

    q_norm = _norm(q)

    # Extra aliases Polymarket uses that differ from CITIES keys
    _ALIASES = {
        # New York variants
        "new york city":        "New York",
        "nyc":                  "New York",
        "new york, ny":         "New York",
        "new york (lga)":       "New York",
        # Los Angeles variants
        "los angeles, ca":      "Los Angeles",
        "los angeles (lax)":    "Los Angeles",
        "la, ca":               "Los Angeles",
        # Chicago variants
        "chicago, il":          "Chicago",
        "chicago (ord)":        "Chicago",
        # Miami variants
        "miami, fl":            "Miami",
        "miami (mia)":          "Miami",
        # Atlanta variants
        "atlanta, ga":          "Atlanta",
        "atlanta (atl)":        "Atlanta",
        # Dallas variants
        "dallas, tx":           "Dallas",
        "dallas (dfw)":         "Dallas",
        "dallas-fort worth":    "Dallas",
        "dfw":                  "Dallas",
        # Denver variants
        "denver, co":           "Denver",
        "denver (den)":         "Denver",
        # Seattle variants
        "seattle, wa":          "Seattle",
        "seattle (sea)":        "Seattle",
        # San Francisco variants
        "san francisco, ca":    "San Francisco",
        "san francisco (sfo)":  "San Francisco",
        "sf, ca":               "San Francisco",
        # Houston variants
        "houston, tx":          "Houston",
        "houston (iah)":        "Houston",
        # Austin variants
        "austin, tx":           "Austin",
        "austin (aus)":         "Austin",
        # South American variants
        "sao paulo":            "Sao Paulo",
        "são paulo":            "Sao Paulo",
        "buenos aires":         "Buenos Aires",
        # New city variants
        "amsterdam, nl":        "Amsterdam",
        "amsterdam (ams)":      "Amsterdam",
        "helsinki, fi":         "Helsinki",
        "helsinki (hel)":       "Helsinki",
        "cape town, sa":        "Cape Town",
        "cape town (cpt)":      "Cape Town",
        "capetown":             "Cape Town",
        "panama city, pa":      "Panama City",
        "panama city (pty)":    "Panama City",
        "panama":               "Panama City",
        "mexico city, mx":      "Mexico City",
        "mexico city (mex)":    "Mexico City",
        "ciudad de mexico":     "Mexico City",
        "cdmx":                 "Mexico City",
    }
    for alias, canonical in _ALIASES.items():
        if _norm(alias) in q_norm:
            c = CITY_MAP.get(canonical)
            if c:
                return c

    for c in CITIES:
        if _norm(c["city"]) in q_norm:
            return c
    return None


def date_from_question(q: str) -> datetime.date | None:
    """
    Extract the resolution date from a Polymarket question title.

    Handles all observed Polymarket formats:
      "...on June 4?"         → date(current_year, 6, 4)
      "...on June 4, 2025?"   → date(2025, 6, 4)
      "...on 2025-06-04?"     → date(2025, 6, 4)
      "...on Jun 4?"          → date(current_year, 6, 4)
      "...on the 4th of June" → date(current_year, 6, 4)

    Returns None if no date found.
    """
    from datetime import date as _date
    import calendar

    now_utc = datetime.now(timezone.utc)
    year    = now_utc.year

    MONTHS = {
        "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
        "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
        "january":1,"february":2,"march":3,"april":4,"june":6,
        "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
    }

    q_l = q.lower()

    # ISO format: 2025-06-04
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", q)
    if m:
        try:
            return _date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # "Month D, YYYY" or "Month D YYYY"
    m = re.search(
        r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
        r"[\s.]+(\d{1,2})(?:[,\s]+(\d{4}))?",
        q_l
    )
    if m:
        mon = MONTHS.get(m.group(1)[:3], None) or MONTHS.get(m.group(1), None)
        day = int(m.group(2))
        yr  = int(m.group(3)) if m.group(3) else year
        if mon:
            try:
                d = _date(yr, mon, day)
                # If parsed year is this year and the date already passed,
                # it might be next year — but Polymarket markets are same-day/next-day
                # so just return as-is; caller decides.
                return d
            except ValueError:
                pass

    # "D Month" or "D of Month"
    m = re.search(
        r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?"
        r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)",
        q_l
    )
    if m:
        day = int(m.group(1))
        mon = MONTHS.get(m.group(2)[:3], None) or MONTHS.get(m.group(2), None)
        if mon:
            try:
                return _date(year, mon, day)
            except ValueError:
                pass

    return None


def _is_binary_market(outcomes) -> bool:
    """Return True if outcomes are just ['Yes','No'] or ['No','Yes']."""
    norm = {str(o).strip().lower() for o in outcomes}
    return norm == {"yes", "no"}


def _threshold_from_question(question: str) -> tuple[float | None, str | None]:
    """
    For binary Yes/No markets, extract the numeric threshold AND direction
    from the question title itself.

    Also handles "between X-Y" range questions (common for Seattle, NYC, Dallas etc):
      "Will the highest temperature in Seattle be between 70-71°F on June 5?"
          → (70.0, 71.0) returned as ("range", lo, hi) via extended return
    """
    q = question.strip()
    s = re.sub(r"[°ºFfCc℃℉]", "", q)

    # "between X-Y" or "between X–Y" — range binary
    m = re.search(r"between\s*([\d.]+)\s*[-–]\s*([\d.]+)", s, re.I)
    if m:
        return float(m.group(1)), "range_hi", float(m.group(2))

    # "X or below" / "X or lower" / "X or less"
    m = re.search(r"([\d.]+)\s+or\s+(below|lower|less)", s, re.I)
    if m:
        return float(m.group(1)), "below", None

    # "X or above" / "X or higher" / "X or more"
    m = re.search(r"([\d.]+)\s+or\s+(above|higher|more)", s, re.I)
    if m:
        return float(m.group(1)), "above", None

    # "at least X" / "no less than X"
    m = re.search(r"(?:at least|no less than)\s*([\d.]+)", s, re.I)
    if m:
        return float(m.group(1)), "above", None

    # "no more than X" / "at most X"
    m = re.search(r"(?:no more than|at most)\s*([\d.]+)", s, re.I)
    if m:
        return float(m.group(1)), "below", None

    # "exceed X" / "exceeds X" / "over X"
    m = re.search(r"(?:exceed[s]?|over)\s*([\d.]+)", q, re.I)
    if m:
        return float(m.group(1)), "above", None
    m = re.search(r"(?:exceed[s]?|over)\s*([\d.]+)", s, re.I)
    if m:
        return float(m.group(1)), "above", None

    # "reach X" / "reaches X" / "hit X" / "hits X"
    m = re.search(r"(?:reach(?:es)?|hit[s]?)\s*([\d.]+)", s, re.I)
    if m:
        return float(m.group(1)), "above", None

    # "above X"
    m = re.search(r"above\s*([\d.]+)", s, re.I)
    if m:
        return float(m.group(1)), "above", None

    # "below X"
    m = re.search(r"below\s*([\d.]+)", s, re.I)
    if m:
        return float(m.group(1)), "below", None

    # "be X" — exact single-degree binary
    m = re.search(r"\bbe\s+([\d.]+)", s, re.I)
    if m:
        return float(m.group(1)), "exact", None

    return None, None, None


def parse_buckets(market: dict) -> list[dict]:
    """
    Parse market outcomes into (label, low, high, price, token) buckets.

    Handles two market structures:
      A) Multi-bucket  — outcomes like ["70-71°F","72-73°F","74-75°F","76°F or above"]
      B) Binary Yes/No — outcomes ["Yes","No"]; threshold lives in question title
         Including "between X-Y" range binary markets (Seattle, NYC, Dallas etc.)
    """
    outcomes = _parse_json_field(market.get("outcomes", []))
    prices   = _parse_json_field(market.get("outcomePrices", []))
    tokens   = _parse_json_field(market.get("clobTokenIds", []))
    question = market.get("question", "")

    buckets = []

    if _is_binary_market(outcomes):
        # ── Binary Yes/No ────────────────────────────────────────────────────
        result = _threshold_from_question(question)
        # result is (threshold, direction, extra) — unpack safely
        if len(result) == 3:
            threshold, direction, range_hi = result
        else:
            threshold, direction = result[0], result[1]
            range_hi = None

        if threshold is None:
            log.warning(f"Binary market but can't parse threshold: {question[:100]}")
            return []

        for i, label in enumerate(outcomes):
            try:
                price = float(prices[i]) if i < len(prices) else 0.5
            except (ValueError, TypeError):
                price = 0.5
            tok = tokens[i] if i < len(tokens) else None
            lbl = str(label).strip().lower()

            if direction == "range_hi":
                # "between X-Y" — Yes = (threshold, range_hi), No = everything else
                if lbl == "yes":
                    lo, hi = threshold, range_hi
                else:
                    # No = below threshold OR above range_hi — treat as complement
                    # For edge calc, split into two conceptual zones but represent as
                    # a single "not in range" bucket: we use sentinel bounds
                    lo, hi = -9999.0, 9999.0  # will get ~(1 - Yes prob)
            elif direction == "below":
                if lbl == "yes":
                    lo, hi = -9999.0, threshold
                else:
                    lo, hi = threshold, 9999.0
            elif direction == "above":
                if lbl == "yes":
                    lo, hi = threshold, 9999.0
                else:
                    lo, hi = -9999.0, threshold
            else:
                # "exact"
                if lbl == "yes":
                    lo, hi = threshold, threshold
                else:
                    lo, hi = -9999.0, 9999.0

            buckets.append(dict(label=label, low=lo, high=hi, price=price, token=tok))

        # For "range_hi" No bucket: set probability as complement of Yes
        if direction == "range_hi":
            yes_b = next((b for b in buckets if str(b["label"]).strip().lower() == "yes"), None)
            no_b  = next((b for b in buckets if str(b["label"]).strip().lower() == "no"),  None)
            if yes_b and no_b:
                no_b["low"]  = -9999.0   # will be computed as 1 - yes_prob after bucket_edge

        log.debug(
            f"  Binary market parsed: '{question[:70]}' "
            f"threshold={threshold} dir={direction} range_hi={range_hi} "
            f"buckets={[(b['label'],b['low'],b['high']) for b in buckets]}"
        )
    else:
        # ── Multi-bucket ─────────────────────────────────────────────────────
        for i, label in enumerate(outcomes):
            try:
                price = float(prices[i]) if i < len(prices) else 0.5
            except (ValueError, TypeError):
                price = 0.5
            tok = tokens[i] if i < len(tokens) else None
            lo, hi = parse_range(str(label))
            buckets.append(dict(label=label, low=lo, high=hi, price=price, token=tok))

    return buckets


def parse_range(label: str) -> tuple[float, float]:
    """
    Parse a multi-bucket Polymarket label into (low, high) numeric bounds.
    Only called for non-binary (multi-bucket) markets.

    Examples:
      "25°C or higher on June 4"   → (25, 9999)
      "13°C or below on June 4"    → (-9999, 13)
      "between 76-77°F on June 4"  → (76, 77)
      "76–77°F"                    → (76, 77)
      "be 25°C on June 4"          → (25, 25)
      "above 85°F"                 → (85, 9999)
      "below 60°F"                 → (-9999, 60)
    """
    s = label.strip()
    s_clean = re.sub(r"[°ºFfCc℃℉]", "", s)

    # "X or higher" / "X or above" / "X or more"
    m = re.search(r"([\d.]+)\s+or\s+(higher|above|more)", s_clean, re.I)
    if m: return float(m.group(1)), 9999.0

    # "X or lower" / "X or below" / "X or less"
    m = re.search(r"([\d.]+)\s+or\s+(lower|below|less)", s_clean, re.I)
    if m: return -9999.0, float(m.group(1))

    # "above X" / "over X"
    m = re.search(r"(?:above|over)\s*([\d.]+)", s_clean, re.I)
    if m: return float(m.group(1)), 9999.0

    # "below X" / "under X"
    m = re.search(r"(?:below|under)\s*([\d.]+)", s_clean, re.I)
    if m: return -9999.0, float(m.group(1))

    # "between X-Y" or "between X–Y"
    m = re.search(r"between\s*([\d.]+)\s*[-–]\s*([\d.]+)", s_clean, re.I)
    if m: return float(m.group(1)), float(m.group(2))

    # bare range "X-Y" or "X–Y"
    m = re.search(r"([\d.]+)\s*[-–]\s*([\d.]+)", s_clean)
    if m: return float(m.group(1)), float(m.group(2))

    # "be X" — single-degree bucket
    m = re.search(r"\bbe\s+([\d.]+)", s_clean, re.I)
    if m:
        v = float(m.group(1))
        return v, v

    # Last resort: lone number → single-degree point
    # NOTE: do NOT return (0, 9999) — that was the original bug causing 100% My_prob
    m = re.search(r"([\d.]+)", s_clean)
    if m:
        v = float(m.group(1))
        return v, v

    # Truly unparseable — return sentinel that will produce ~0 probability
    # so it doesn't get traded. Log it so we can fix the regex.
    log.warning(f"parse_range: unparseable label '{label}' — returning sentinel")
    return 9998.0, 9999.0   # near-impossible range, not 0–9999


# ─────────────────────────────────────────────────────────────
# STEP 2 — METAR (aviationweather.gov)
# ─────────────────────────────────────────────────────────────
async def get_metar(session, icao: str) -> dict | None:
    """
    STEP 2 — Live METAR Pull (aviationweather.gov)
    Per guide: extract temp, dewpoint, wind direction, wind speed,
    cloud layers (BKN/OVC/FEW/SCT/CLR), pressure (altimeter → hPa),
    and visibility.
    Updates every 30 min; SPECI issued on significant condition change.
    """
    url  = f"https://aviationweather.gov/api/data/metar?ids={icao}&format=json"
    data = await get(session, url)
    if not data or not isinstance(data, list) or len(data) == 0:
        return None
    o = data[0]

    # wdir from METAR can be:
    #   - int (e.g. 190) — normal numeric direction
    #   - "VRB"          — variable wind (no fixed direction)
    #   - "///" or None  — missing
    # Downstream code does  135 <= wdir <= 250  which crashes on a string.
    # Normalise: int if parseable, else None.
    raw_wdir = o.get("wdir")
    try:
        wdir_clean = int(raw_wdir) if raw_wdir not in (None, "", "VRB", "///") else None
    except (TypeError, ValueError):
        wdir_clean = None
    altim = o.get("altim")           # inHg float (US stations) OR hPa float (non-US, post-Sep 2025 API)
    if altim:
        # aviationweather.gov returns inHg for US stations (28–32 range)
        # and hPa for international stations (950–1050 range) after Sep 2025 schema change.
        # Detect by magnitude: inHg values are always < 35, hPa values are always > 800.
        if altim < 200:
            press_hpa = round(altim * 33.8639, 1)  # inHg → hPa
        else:
            press_hpa = round(altim, 1)             # already hPa
    else:
        press_hpa = None

    # Visibility: "visib" in statute miles; convert to km for non-US display
    visib_sm = o.get("visib")        # statute miles float or "10+"
    try:
        visib_km = round(float(visib_sm) * 1.60934, 1) if visib_sm else None
    except (TypeError, ValueError):
        visib_km = None

    return dict(
        temp_c   = o.get("temp"),        # °C
        dewp_c   = o.get("dewp"),        # °C
        wdir     = wdir_clean,           # int degrees or None (VRB/missing → None)
        wspd_kt  = o.get("wspd"),        # knots
        wgst_kt  = o.get("wgst"),        # gust knots (None if no gust)
        clouds   = o.get("clouds", []),  # list of {cover, base} dicts
        altim_inhg = altim,              # inHg
        press_hpa  = press_hpa,          # hPa
        visib_sm   = visib_sm,           # statute miles
        visib_km   = visib_km,           # km
        raw      = o.get("rawOb", ""),
    )


# ─────────────────────────────────────────────────────────────
# STEP 3a — Open-Meteo M1 (ECMWF) + M2 (regional)
# Single call per city — both models in one request
# ─────────────────────────────────────────────────────────────
async def get_openmeteo(session, city: dict, next_day: bool = False) -> dict | None:
    """
    STEP 3a — Forecast Pull: M1 (ECMWF IFS) + M2 (regional best) via Open-Meteo.

    Open-Meteo always returns °C regardless of city unit.
    next_day=True  → use daily index [1] (tomorrow) — for Asian/Pacific cities that
                     are UTC+8 to UTC+13 ahead, already in their next calendar day.
    next_day=False → use daily index [0] (today) — Americas + Europe.

    Focus window for peak CC: 13:00–17:00 LOCAL station time (guide Step 3).
    Consensus weight: M1=50%, M2=30% (M3=20% handled separately).
    """
    tz       = ZoneInfo(city["tz"])
    day_idx  = 1 if next_day else 0   # index into daily arrays

    async def _fetch_model(model_str: str) -> tuple[float | None, float]:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={city['lat']}&longitude={city['lon']}"
            f"&daily=temperature_2m_max"
            f"&hourly=cloud_cover"
            f"&models={model_str}"
            f"&forecast_days=3&timezone=auto"   # 3 days so index 1 is always populated
            f"&temperature_unit=celsius"
        )
        data = await get(session, url)
        if not data:
            return None, 50.0
        try:
            daily   = data.get("daily", {})
            maxvals = daily.get("temperature_2m_max", [])
            target_max = maxvals[day_idx] if len(maxvals) > day_idx else None

            # Peak-hour cloud cover 13–17 LOCAL on the target day
            hourly  = data.get("hourly", {})
            times   = hourly.get("time", [])
            clouds  = hourly.get("cloud_cover", [])
            peak_cc = []
            for t_str, cc in zip(times, clouds):
                try:
                    dt = datetime.fromisoformat(t_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                    local = dt.astimezone(tz)
                    # Only pick hours on the target calendar day (today or tomorrow)
                    offset_days = local.date() - datetime.now(tz).date()
                    if offset_days.days == day_idx and 13 <= local.hour <= 17 and cc is not None:
                        peak_cc.append(cc)
                except Exception:
                    pass
            avg_cc = sum(peak_cc) / len(peak_cc) if peak_cc else 50.0
            return target_max, avg_cc
        except Exception as e:
            log.warning(f"Open-Meteo parse [{model_str}] {city['city']}: {e}")
            return None, 50.0

    # M1 = ECMWF IFS 025 (global anchor, weight 50%)
    m1_max, cc1 = await _fetch_model("ecmwf_ifs025")
    # M2 = city-specific regional model (weight 30%)
    m2_str = city["m2"] if city["m2"] != "ecmwf_ifs025" else "gfs_seamless"
    m2_max, cc2 = await _fetch_model(m2_str)

    if m1_max is None and m2_max is None:
        log.warning(f"  Open-Meteo: no data for {city['city']} (next_day={next_day})")
        return None

    avg_cc = cc1 if m1_max is not None else cc2
    log.debug(f"  {city['city']}: M1={m1_max}°C M2={m2_max}°C CC={avg_cc:.0f}% next_day={next_day}")
    return dict(m1_max=m1_max, m2_max=m2_max, avg_cloud_pct=avg_cc)


# ─────────────────────────────────────────────────────────────
# STEP 3b — M3 local NWS APIs (one function per source)
# ─────────────────────────────────────────────────────────────

async def m3_nws(session, lat, lon, next_day: bool = False) -> float | None:
    """US — api.weather.gov — returns °F.
    next_day=True  → find tomorrow's daytime high (periods 24-48h)
    next_day=False → find today's daytime high (periods 0-24h)
    """
    pts = await get(session, f"https://api.weather.gov/points/{lat},{lon}")
    if not pts:
        return None
    fc_url = pts.get("properties", {}).get("forecastHourly")
    if not fc_url:
        return None
    fc = await get(session, fc_url)
    if not fc:
        return None
    periods = fc.get("properties", {}).get("periods", [])
    # periods are hourly; slice appropriately
    if next_day:
        relevant = periods[24:48]   # tomorrow's hours
    else:
        relevant = periods[:24]     # today's hours
    temps = [p["temperature"] for p in relevant if p.get("isDaytime") and p.get("temperature")]
    if not temps:
        # fallback: all periods in window regardless of isDaytime
        temps = [p["temperature"] for p in relevant if p.get("temperature")]
    return max(temps) if temps else None

async def m3_eccc(session) -> float | None:
    """Toronto — weather.gc.ca — returns °C"""
    data = await get(session, "https://weather.gc.ca/api/app/en/location/s0000458/forecast/hourly")
    if not data:
        return None
    try:
        vals = [float(h["temperature"]["value"]) for h in data if h.get("temperature")]
        return max(vals[:24]) if vals else None
    except Exception:
        return None

async def m3_brightsky(session, lat, lon) -> float | None:
    """Europe (London/Paris/Madrid/Milan/Munich via DWD) — returns °C"""
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = await get(session, f"https://api.brightsky.dev/weather?lat={lat}&lon={lon}&date={date}")
    if not data:
        return None
    try:
        vals = [h["temperature"] for h in data.get("weather", []) if h.get("temperature") is not None]
        return max(vals) if vals else None
    except Exception:
        return None

async def m3_imgw(session) -> float | None:
    """Warsaw — IMGW Poland — returns current obs °C"""
    data = await get(session, "https://danepubliczne.imgw.pl/api/data/synop/station/12374")
    if not data:
        return None
    try:
        return float(data.get("temperatura", 0))
    except Exception:
        return None

async def m3_mgm(session, merkezid: int) -> float | None:
    """Istanbul/Ankara — Turkish MGM — returns °C"""
    data = await get(session, f"https://servis.mgm.gov.tr/servis/tahmin5gunluk?merkezid={merkezid}")
    if not data:
        return None
    try:
        today = data[0] if isinstance(data, list) else data
        return float(today.get("enYuksekSicaklik", today.get("maxTemp", 0)))
    except Exception:
        return None

async def m3_ims(session) -> float | None:
    """Tel Aviv — IMS Israel — returns °C"""
    data = await get(session, "https://ims.gov.il/sites/default/files/ims_data/md_files/forecast.json")
    if not data:
        return None
    try:
        for loc in data.get("data", []):
            if "tel aviv" in str(loc.get("stn_name", "")).lower():
                return float(loc.get("TMP_Max", 0))
        return None
    except Exception:
        return None

async def m3_gismeteo(session, city_id: int) -> float | None:
    """Moscow — Gismeteo (Roshydromet) — returns °C"""
    data = await get(session, f"https://api.gismeteo.net/v2/weather/forecast/?city_id={city_id}")
    if not data:
        return None
    try:
        day = data.get("response", {}).get("days", [{}])[0]
        return float(day.get("temperature", {}).get("max", {}).get("C", 0))
    except Exception:
        return None

async def m3_cma(session, lat, lon) -> float | None:
    """China cities — CMA GRAPES via Open-Meteo CMA endpoint — returns °C"""
    url = (f"https://api.open-meteo.com/v1/forecast"
           f"?latitude={lat}&longitude={lon}"
           f"&daily=temperature_2m_max&models=cma_grapes_global"
           f"&forecast_days=2&timezone=auto")
    data = await get(session, url)
    if not data:
        return None
    try:
        return data["daily"]["temperature_2m_max"][0]
    except Exception:
        return None

async def m3_hko(session) -> float | None:
    """Hong Kong — HKO Open Data — returns °C"""
    data = await get(session, "https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=fnd&lang=en")
    if not data:
        return None
    try:
        return float(data.get("weatherForecast", [{}])[0].get("forecastMaxtemp", {}).get("value", 0))
    except Exception:
        return None

async def m3_kma_open(session) -> float | None:
    """Seoul — KMA Open Data (no-key endpoint) — returns °C"""
    # KMA provides a public RSS/JSON without auth for current conditions
    data = await get(session, "https://www.kma.go.kr/wid/queryDFSS.jsp?zone=1159081500&mode=1")
    if not data:
        return None
    try:
        # Returns XML-like but wrapped — parse max temp from forecast
        items = data.get("result", {}).get("body", {}).get("data", [])
        temps = [float(i.get("tmx", 0)) for i in items if i.get("tmx")]
        return max(temps) if temps else None
    except Exception:
        return None

async def m3_jma(session) -> float | None:
    """Tokyo — JMA bosai public JSON — returns °C"""
    data = await get(session, "https://www.jma.go.jp/bosai/forecast/data/forecast/130000.json")
    if not data:
        return None
    try:
        for series in data[1].get("timeSeries", []):
            for area in series.get("areas", []):
                tmax = area.get("tempsMax", [])
                vals = [float(t) for t in tmax if t not in ("", None)]
                if vals:
                    return max(vals[:2])
        return None
    except Exception:
        return None

async def m3_nea(session) -> float | None:
    """Singapore — NEA open data — returns °C"""
    data = await get(session,
        "https://api2.nea.gov.sg/api/action/datastore/resource.action"
        "?resource_id=9526ec00-dbf5-11e8-b83c-9b00b5d21d03")
    if not data:
        return None
    try:
        rec = data.get("result", {}).get("records", [{}])[0]
        return float(rec.get("Maximum", rec.get("maximum", 0)))
    except Exception:
        return None

async def m3_cwa(session) -> float | None:
    """Taipei — CWA Taiwan open endpoint (no key needed for basic) — returns °C"""
    # CWA provides a public no-key endpoint for current obs
    data = await get(session,
        "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0003-001"
        "?StationName=臺北")
    if not data:
        return None
    try:
        recs = data.get("records", {}).get("Station", [])
        temps = [float(r["WeatherElement"]["AirTemperature"]) for r in recs
                 if r.get("WeatherElement", {}).get("AirTemperature") not in ("", None)]
        return max(temps) if temps else None
    except Exception:
        return None

async def m3_imd(session) -> float | None:
    """Lucknow — IMD India public endpoint — returns °C"""
    data = await get(session,
        "https://mausam.imd.gov.in/mausam/appstatic/home_city_data.php?city_id=23")
    if not data:
        return None
    try:
        return float(data.get("max_temp", 0))
    except Exception:
        return None

async def m3_smn_ar(session) -> float | None:
    """Buenos Aires — SMN Argentina public JSON — returns °C.

    The API returns a list of city objects. We find Buenos Aires by name
    and extract `temp_max` from its `weather` block.

    Bug fixed: never fall back to 0 — if the key is absent or the value
    is 0 / None, return None so consensus() drops it rather than treating
    0°C as a valid forecast (which blew sigma up to 23°C).
    """
    data = await get(session, "https://ws.smn.gob.ar/map_items/forecast/1")
    if not data:
        return None
    try:
        for item in (data if isinstance(data, list) else []):
            if "buenos" in str(item.get("name", "")).lower():
                raw = item.get("weather", {}).get("temp_max")
                if raw is None:
                    return None
                val = float(raw)
                # Explicit zero almost certainly means missing data, not 0°C in BA
                return val if val != 0.0 else None
        return None
    except Exception:
        return None

async def m3_inmet(session) -> float | None:
    """Sao Paulo — INMET Brazil — returns °C"""
    data = await get(session, "https://apitempo.inmet.gov.br/api/v2/previsao/1/A771")
    if not data:
        return None
    try:
        today = (data if isinstance(data, list) else [data])[0]
        raw = today.get("TempMaxima", today.get("temp_max"))
        if raw is None:
            return None
        val = float(raw)
        return val if val != 0.0 else None
    except Exception:
        return None

async def m3_metservice_nz(session) -> float | None:
    """Wellington — MetService NZ public JSON — returns °C"""
    data = await get(session, "https://www.metservice.com/publicData/localForecast/Wellington")
    if not data:
        return None
    try:
        days = data.get("days", [])
        if not days:
            return None
        raw = days[0].get("max", days[0].get("highTemp"))
        if raw is None:
            return None
        val = float(raw)
        return val if val != 0.0 else None
    except Exception:
        return None

async def m3_smn_mx(session) -> float | None:
    """Mexico City — SMN Mexico public JSON — returns °C"""
    data = await get(session,
        "https://smn.conagua.gob.mx/tools/DATA/Condiciones_Actuales_Vuelo.json")
    if not data:
        return None
    try:
        for rec in (data if isinstance(data, list) else []):
            if "mmmx" in str(rec.get("id", "")).lower():
                raw = rec.get("tmax", rec.get("temp_max"))
                if raw is None:
                    return None
                val = float(raw)
                return val if val != 0.0 else None
        return None
    except Exception:
        return None


# M3 dispatcher
async def get_m3(session, city: dict, next_day: bool = False) -> float | None:
    k = city["m3"]
    if k == "nws":           return await m3_nws(session, city["lat"], city["lon"], next_day=next_day)
    if k == "eccc":          return await m3_eccc(session)
    if k == "brightsky":     return await m3_brightsky(session, city["bs_lat"], city["bs_lon"])
    if k == "imgw":          return await m3_imgw(session)
    if k == "mgm":           return await m3_mgm(session, city["mgm_id"])
    if k == "ims":           return await m3_ims(session)
    if k == "gismeteo":      return await m3_gismeteo(session, city["gis_id"])
    if k == "cma":           return await m3_cma(session, city["lat"], city["lon"])
    if k == "hko":           return await m3_hko(session)
    if k == "kma_open":      return await m3_kma_open(session)
    if k == "jma":           return await m3_jma(session)
    if k == "nea":           return await m3_nea(session)
    if k == "cwa":           return await m3_cwa(session)
    if k == "imd":           return await m3_imd(session)
    if k == "smn_ar":        return await m3_smn_ar(session)
    if k == "inmet":         return await m3_inmet(session)
    if k == "metservice_nz": return await m3_metservice_nz(session)
    if k == "smn_mx":        return await m3_smn_mx(session)
    return None


# ─────────────────────────────────────────────────────────────
# STEP 4 — WIND + CLOUD ADJUSTMENT (guide tables)
# ─────────────────────────────────────────────────────────────
def cloud_adj(clouds: list, unit: str) -> float:
    """Return temp adjustment based on METAR cloud layers."""
    if not clouds:
        return 0.0
    for lyr in clouds:
        cover = lyr.get("cover", "")
        raw_base = lyr.get("base", 9999)
        # base can be string ("VRB", "///") or None — always convert safely
        try:
            base = int(raw_base) if raw_base is not None else 9999
        except (ValueError, TypeError):
            base = 9999
        delta_c = 0.0
        if cover == "OVC":
            delta_c = -4.0 if base < 30 else -2.0
        elif cover == "BKN":
            delta_c = -1.5 if base < 30 else -1.0
        if delta_c:
            return delta_c * (9/5) if unit == "F" else delta_c
    return 0.0

def wind_adj(wdir, wspd, unit: str, lat: float = 45.0) -> float:
    """
    Return temp adjustment (°C, converted to °F if unit=='F') based on
    wind direction advection.

    Speed tiers (kt):
      calm/light  0–5   : ±0.0°C  (too weak to advect meaningfully)
      light-mod   6–14  : warm +0.5 / cold −0.5°C
      moderate   15–24  : warm +1.0 / cold −1.0°C
      strong      ≥25   : warm +1.5 / cold −2.5°C  ← asymmetric: cold advection
                                                       is stronger than warm at
                                                       the same wind speed
      E/W (46–134, 251–314): 0.0°C (parallel to isotherms → no advection signal)

    Southern Hemisphere (lat < 0): warm/cold sectors are reversed —
    N/NW = Berg/foehn = warm; S/SW = Southern Ocean = cold.
    """
    if wdir is None:
        return 0.0
    spd = wspd if wspd else 0
    sh  = lat < 0

    warm = 135 <= wdir <= 250
    cold = wdir >= 315 or wdir <= 45
    if sh:
        warm, cold = cold, warm   # flip for Southern Hemisphere

    if not warm and not cold:
        return 0.0   # E/W quadrant — neutral

    if spd <= 5:
        delta_c = 0.0
    elif spd <= 14:
        delta_c = +0.5 if warm else -0.5
    elif spd <= 24:
        delta_c = +1.0 if warm else -1.0
    else:  # ≥25 kt — asymmetric: cold stronger than warm
        delta_c = +1.5 if warm else -2.5

    return delta_c * (9/5) if unit == "F" else delta_c


# ─────────────────────────────────────────────────────────────
# STEP 5 — CONSENSUS + EDGE
# ─────────────────────────────────────────────────────────────
def c_to_f(c: float) -> float:
    return c * 9/5 + 32

def consensus(m1_c, m2_c, m3_val, unit, cloud_pct, wdir, wspd,
              metar_temp_c=None, metar_local_hour=None, lat: float = 45.0):
    """
    Weighted consensus max-temp forecast → (mu, sigma) in city's native unit.

    Inputs:
      m1_c, m2_c        : Open-Meteo forecasts, ALWAYS in °C
      m3_val            : local NWS forecast in city's native unit (°F or °C)
      unit              : "F" or "C"
      cloud_pct         : peak-hour cloud cover 0–100
      wdir/wspd         : METAR wind direction (°) and speed (kt)
      metar_temp_c      : live METAR observed temp in °C (used as hard floor)
      metar_local_hour  : local hour (0–23) of the METAR obs; floor only applied
                          if hour < 14 (ascending portion of diurnal cycle).
                          None = apply floor unconditionally (safe fallback).

    FLOOR RULES (v17 fix — prevents consensus < live obs):
      1. consensus_c  ≥  metar_temp_c        (can't be colder than current obs)
         — v23 fix: only when metar_local_hour < 14; overnight readings must NOT
           floor the next day's max (fixes Moscow-style stale-hot-floor bug)
      2. consensus_c  ≥  min(model values)   (can't be below ALL models combined)
    These two floors are applied AFTER adjustments so cloud/wind never
    push the estimate below what's already been measured or forecast.
    """
    def to_c(v, u):
        return (v - 32) * 5/9 if u == "F" else v

    vals = []   # list of (temp_in_celsius, weight)
    if m1_c   is not None: vals.append((float(m1_c),  0.50))
    if m2_c   is not None: vals.append((float(m2_c),  0.30))
    if m3_val is not None: vals.append((to_c(float(m3_val), unit), 0.20))

    if not vals:
        return None, None

    w_sum    = sum(w for _, w in vals)
    weighted = sum(v * w for v, w in vals) / w_sum

    # Model floor: consensus_c can't drop below the LOWEST individual model
    # (cloud/wind adjustments can suppress but not obliterate the model signal)
    model_min_c = min(v for v, _ in vals)

    # Cloud suppression (°C) — applied to DAYTIME peak CC 13–17h
    # Cap suppression so it can't drop below the model floor
    cc_adj = 0.0
    if   cloud_pct >= 80: cc_adj = -3.0
    elif cloud_pct >= 50: cc_adj = -1.5
    elif cloud_pct >= 30: cc_adj = -0.8

    # Wind advection (°C) — mirrors wind_adj() tiers, with SH reversal
    wd_adj = 0.0
    if wdir is not None:
        _spd  = wspd if wspd else 0
        _sh   = lat < 0
        _warm = 135 <= wdir <= 250
        _cold = wdir >= 315 or wdir <= 45
        if _sh:
            _warm, _cold = _cold, _warm  # flip for Southern Hemisphere
        if _warm or _cold:
            if _spd <= 5:
                wd_adj = 0.0
            elif _spd <= 14:
                wd_adj = +0.5 if _warm else -0.5
            elif _spd <= 24:
                wd_adj = +1.0 if _warm else -1.0
            else:  # >=25 kt — asymmetric
                wd_adj = +1.5 if _warm else -2.5

    adj_c = weighted + cc_adj + wd_adj

    # ── FLOOR 1: never below the lowest model ───────────────────────────────
    adj_c = max(adj_c, model_min_c)

    # ── FLOOR 2: never below the live METAR observed temp ──────────────────
    # Only apply during the ascending portion of the diurnal cycle (before
    # 14:00 LOCAL). An overnight/late-evening reading (e.g. 29°C at 11 PM)
    # must NOT floor the next-day maximum — a front could arrive and cool it.
    # None → apply floor conservatively (backward-compatible default).
    if metar_temp_c is not None:
        apply_floor = (metar_local_hour is None) or (metar_local_hour < 14)
        if apply_floor:
            adj_c = max(adj_c, float(metar_temp_c))

    mu    = c_to_f(adj_c) if unit == "F" else adj_c

    # Sigma: fixed ±1 in the city's native unit.
    sigma = 1.0

    return round(mu, 2), round(sigma, 2)

def bucket_edge(buckets, mu, sigma):
    for b in buckets:
        lo, hi = b["low"], b["high"]
        p_hi = stats.norm.cdf(hi, mu, sigma) if hi < 9000 else 1.0
        p_lo = stats.norm.cdf(lo, mu, sigma) if lo > -9000 else 0.0
        b["my_prob"] = round(max(0.0, min(1.0, p_hi - p_lo)), 4)
        b["edge"]    = round(b["my_prob"] - b["price"], 4)
    return buckets


def synthesize_display_buckets(mu: float, sigma: float, unit: str) -> list:
    """
    Build 3 display-only temperature range buckets centred on the consensus
    forecast (mu) so traders see where the model puts the probability mass.

    Boundaries are set at mu ± 0.5*sigma, rounded to whole degrees:
      Low  : below  lo_bound      e.g. "Below 21°C"
      Mid  : lo_bound – hi_bound  e.g. "21–24°C"
      High : above  hi_bound      e.g. "Above 24°C"

    No market price or edge — these are model-only display buckets.
    The real Yes/No buckets remain the source of the trade recommendation.
    """
    sym  = "°F" if unit == "F" else "°C"
    half = max(sigma * 0.5, 1.0)   # at least ±1 degree spread

    lo_bound = round(mu - half)
    hi_bound = round(mu + half)
    if lo_bound >= hi_bound:
        lo_bound = round(mu) - 1
        hi_bound = round(mu) + 1

    def bucket_prob(lo, hi):
        p_hi = stats.norm.cdf(hi, mu, sigma) if hi < 9000 else 1.0
        p_lo = stats.norm.cdf(lo, mu, sigma) if lo > -9000 else 0.0
        return round(max(0.0, min(1.0, p_hi - p_lo)), 4)

    return [
        dict(label=f"Below {lo_bound}{sym}",
             low=-9999.0, high=float(lo_bound),
             my_prob=bucket_prob(-9999.0, float(lo_bound)), display_only=True),
        dict(label=f"{lo_bound}–{hi_bound}{sym}",
             low=float(lo_bound), high=float(hi_bound),
             my_prob=bucket_prob(float(lo_bound), float(hi_bound)), display_only=True),
        dict(label=f"Above {hi_bound}{sym}",
             low=float(hi_bound), high=9999.0,
             my_prob=bucket_prob(float(hi_bound), 9999.0), display_only=True),
    ]


# ─────────────────────────────────────────────────────────────
# ATMOSPHERIC STABILITY SCORE (Concept 2 — physical atmosphere)
#
# Uses only data already fetched: METAR obs + 3-model forecasts.
# Returns an integer 1–10 and a short label.
#
# Science basis:
#   Lifted Index (LI) proxy — we approximate LI from the METAR
#   dewpoint depression and temperature, following the relationship
#   that a moist, warm boundary layer (small T-Td spread) lowers
#   the LCL and increases convective instability.
#
#   Cloud cover from METAR (OVC/BKN) indicates existing convective
#   or stratiform activity → instability or suppression.
#
#   Model spread (sigma across M1/M2/M3) reflects forecast
#   uncertainty which itself tracks atmospheric complexity.
#
#   All three inputs are combined into a 1–10 integer score:
#     10 = very stable (dry, clear, model agreement, high pressure)
#      1 = very unstable (moist, stormy, high model spread)
# ─────────────────────────────────────────────────────────────
def atmospheric_stability_score(
    temp_c: float | None,
    dewp_c: float | None,
    clouds: list,
    m1_c: float | None,
    m2_c: float | None,
    m3_val: float | None,
    unit: str,
) -> tuple[int, str]:
    """
    Compute an atmospheric stability score (1–10) from METAR obs
    and the three model forecasts already available in the pipeline.

    Returns (score: int, label: str).

    Component 1 — Dewpoint Depression proxy (T - Td):
      The T-Td spread is a direct proxy for boundary-layer moisture.
      A small spread (< 5°C) = moist BL → low LCL → convective risk.
      A large spread (> 15°C) = dry air → stable, clear skies.
      Source: NOAA SPC operational use of T-Td spread for convective
      outlook analysis (www.spc.noaa.gov/exper/soundings/).

    Component 2 — Cloud cover penalty:
      OVC / BKN at low bases indicates existing convective or thick
      stratiform cloud → reduces stability score.
      CLR / FEW → high stability contribution.
      Source: METAR cloud cover interpretation per FAA AIM Chapter 7.

    Component 3 — Model spread penalty:
      When M1, M2, M3 disagree significantly (σ > 2°C), the atmosphere
      is harder to predict, implying complex/unstable dynamics.
      Source: ECMWF Predictability & Ensemble Forecasting documentation.

    Score mapping (each component contributes 0–10, averaged):
      T-Td spread:
        < 3°C  → 1   (very moist, highly unstable)
        3–5°C  → 3
        5–10°C → 5
        10–15°C→ 7
        > 15°C → 9   (very dry, stable)
      Cloud:
        OVC (base<30)  → 2
        OVC (base≥30)  → 3
        BKN (base<30)  → 4
        BKN (base≥30)  → 5
        SCT            → 7
        FEW/CLR/SKC    → 10
      Model spread (°C across available models in native unit):
        > 5    → 1
        3–5    → 3
        2–3    → 5
        1–2    → 7
        < 1    → 10
    """
    def to_c(v, u):
        return (v - 32) * 5 / 9 if u == "F" else v

    scores = []

    # ── Component 1: T-Td spread ─────────────────────────────────────────────
    if temp_c is not None and dewp_c is not None:
        spread = float(temp_c) - float(dewp_c)   # always in °C from METAR
        if   spread < 3:   td_score = 1
        elif spread < 5:   td_score = 3
        elif spread < 10:  td_score = 5
        elif spread < 15:  td_score = 7
        else:              td_score = 9
        scores.append(td_score)

    # ── Component 2: Cloud cover ─────────────────────────────────────────────
    if clouds:
        top = clouds[0]
        cover = top.get("cover", "CLR")
        raw_base = top.get("base", 9999)
        try:
            base = int(raw_base) if raw_base is not None else 9999
        except (ValueError, TypeError):
            base = 9999

        if   cover == "OVC" and base < 30:   cl_score = 2
        elif cover == "OVC":                  cl_score = 3
        elif cover == "BKN" and base < 30:   cl_score = 4
        elif cover == "BKN":                  cl_score = 5
        elif cover == "SCT":                  cl_score = 7
        else:                                 cl_score = 10   # FEW / CLR / SKC / NSC
        scores.append(cl_score)
    else:
        scores.append(10)   # no clouds reported → clear sky

    # ── Component 3: Model spread ─────────────────────────────────────────────
    model_vals = []
    if m1_c  is not None: model_vals.append(float(m1_c))          # always °C
    if m2_c  is not None: model_vals.append(float(m2_c))          # always °C
    if m3_val is not None: model_vals.append(to_c(float(m3_val), unit))

    if len(model_vals) >= 2:
        spread_m = max(model_vals) - min(model_vals)
        if   spread_m > 5:   ms_score = 1
        elif spread_m > 3:   ms_score = 3
        elif spread_m > 2:   ms_score = 5
        elif spread_m > 1:   ms_score = 7
        else:                ms_score = 10
        scores.append(ms_score)

    if not scores:
        return 5, "Moderate ⚠️"

    raw = sum(scores) / len(scores)
    score = max(1, min(10, round(raw)))

    if   score >= 9: label = "Very Stable 🟢"
    elif score >= 7: label = "Stable 🟡"
    elif score >= 5: label = "Moderate ⚠️"
    elif score >= 3: label = "Unstable 🔴"
    else:            label = "Very Unstable ⛈"

    return score, label


# ─────────────────────────────────────────────────────────────
# STEP 6 — DISCORD EMBED
# Guide field order (bot architecture + wind/cloud):
#  Section A: Market question
#  Section B: METAR Live Obs — temp | dewpoint | wind dir+speed+gust |
#             pressure | visibility | all cloud layers
#  Section C: Wind+Cloud adjustments applied (per guide table)
#  Section D: Forecast — Peak CC 13–17h | M1 ECMWF | M2 Regional | M3 Local NWS
#  Section E: Buckets + Edge
#  Section F: Action + Kelly sizing
# ─────────────────────────────────────────────────────────────
def _wind_dir_label(deg) -> str:
    """Convert wind direction degrees to cardinal label."""
    if deg is None: return "—"
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    idx = round(deg / 22.5) % 16
    return dirs[idx]

def _cloud_layers_str(clouds: list) -> str:
    """Format all cloud layers as 'OVC012 BKN035 FEW080' etc."""
    if not clouds:
        return "CLR"
    parts = []
    for lyr in clouds:
        cov  = lyr.get("cover", "")
        base = lyr.get("base")   # hundreds of feet
        if cov in ("CLR", "SKC", "NSC"):
            parts.append(cov)
        elif base is not None:
            parts.append(f"{cov}{int(base):03d}")
        else:
            parts.append(cov)
    return " ".join(parts) if parts else "CLR"

def _advection_label(wdir, lat: float = 45.0, wspd: int | None = None) -> str:
    """
    Per guide: S/SW=warm advection; N/NW=cold; else neutral.
    Fix 3: when wind speed ≤ 5kt, always return NEUTRAL regardless of direction —
    calm winds can't advect meaningfully, so warm/cold labels are misleading.
    Southern Hemisphere reversal (lat < 0): N/NW = Berg/foehn = WARM;
    S/SW = Southern Ocean = COLD.
    """
    if wdir is None: return "—"
    spd = wspd if wspd else 0
    if spd <= 5:
        return "→ NEUTRAL (calm)"

    sh = lat < 0
    warm_sector = (135 <= wdir <= 250)
    cold_sector = (wdir >= 315 or wdir <= 45)
    if sh:
        warm_sector, cold_sector = cold_sector, warm_sector

    if warm_sector: return "⬆ WARM (N/NW Berg)" if sh else "⬆ WARM (S/SW)"
    if cold_sector: return "⬇ COLD (S/SW Ocean)" if sh else "⬇ COLD (N/NW)"
    if 250 < wdir < 315: return "↙ NW-transition"
    return "→ NEUTRAL"

def build_embed(city, metar, om, m3_val, mu, sigma, buckets, market,
                next_day: bool = False,
                stab_score: int = 5, stab_label: str = "Moderate ⚠️",
                calib_summary: str = "") -> discord.Embed:
    unit  = city["unit"]
    sym   = "°F" if unit == "F" else "°C"
    best  = max(buckets, key=lambda b: abs(b["edge"]))
    act   = "BUY YES ✅" if best["edge"] > 0 else "BUY NO ❌"
    day_s = "TOMORROW" if next_day else "TODAY"

    # ── METAR field extraction ────────────────────────────────────────────────
    t_obs   = metar.get("temp_c")
    d_obs   = metar.get("dewp_c")
    wdir    = metar.get("wdir")
    wspd    = metar.get("wspd_kt")
    wgst    = metar.get("wgst_kt")
    press   = metar.get("press_hpa")
    visib   = metar.get("visib_sm")
    visib_k = metar.get("visib_km")
    clouds  = metar.get("clouds", [])

    # Temp: display in city's native unit
    if unit == "F" and t_obs is not None:
        obs_s = f"{c_to_f(t_obs):.1f}°F"
        dewp_s = f"{c_to_f(d_obs):.1f}°F" if d_obs is not None else "—"
    else:
        obs_s = f"{t_obs:.1f}°C" if t_obs is not None else "—"
        dewp_s = f"{d_obs:.1f}°C" if d_obs is not None else "—"

    # Wind
    card    = _wind_dir_label(wdir)
    wdir_s  = f"{wdir}°({card})" if wdir is not None else "Calm"
    wspd_s  = f"{wspd}kt" if wspd else "0kt"
    wgst_s  = f" G{wgst}kt" if wgst else ""
    wind_s  = f"{wdir_s} @ {wspd_s}{wgst_s}"

    # Pressure
    press_s = f"{press}hPa" if press else "—"

    # Visibility
    if visib is not None:
        visib_s = f"{visib}SM ({visib_k}km)" if visib_k else f"{visib}SM"
    else:
        visib_s = "—"

    # Cloud layers — ALL layers per guide
    cloud_s = _cloud_layers_str(clouds)
    top_cover = (clouds[0].get("cover","CLR") if clouds else "CLR")

    # Advection label per guide wind/cloud table — SH-aware
    adv_s   = _advection_label(wdir, lat=city["lat"])

    # ── Build wind+cloud adjustment summary (guide Step 4) ───────────────────
    adj_notes = []
    if top_cover in ("OVC",):
        adj_notes.append(f"OVC → −2 to −8{sym} solar suppression")
    elif top_cover in ("BKN",):
        adj_notes.append(f"BKN → −1 to −4{sym} partial suppression")
    elif top_cover in ("CLR","SKC","FEW","NSC"):
        adj_notes.append(f"CLR → baseline/+1–3{sym} solar max")
    if wdir is not None:
        _spd = metar.get("wspd_kt") or 0
        _sh  = city["lat"] < 0
        _warm_dir = 135 <= wdir <= 250
        _cold_dir = wdir >= 315 or wdir <= 45
        if _sh:
            _warm_dir, _cold_dir = _cold_dir, _warm_dir

        if _spd <= 5:
            # Fix 3: when calm, show NEUTRAL regardless of direction
            if _warm_dir or _cold_dir:
                adj_notes.append(f"{'N/NW' if _sh and _warm_dir else 'S/SW' if _warm_dir else 'N/NW'} wind → advection 0° (calm — no signal)")
        elif _warm_dir:
            dir_lbl = "N/NW (Berg)" if _sh else "S/SW"
            if _spd <= 14:
                adj_notes.append(f"{dir_lbl} wind → warm advection +0.5° (light-mod)")
            elif _spd <= 24:
                adj_notes.append(f"{dir_lbl} wind → warm advection +1.0° (moderate)")
            else:
                adj_notes.append(f"{dir_lbl} wind → warm advection +1.5° (strong ≥25kt)")
        elif _cold_dir:
            dir_lbl = "S/SW (Southern Ocean)" if _sh else "N/NW"
            if _spd <= 14:
                adj_notes.append(f"{dir_lbl} wind → cold advection −0.5° (light-mod)")
            elif _spd <= 24:
                adj_notes.append(f"{dir_lbl} wind → cold advection −1.0° (moderate)")
            else:
                adj_notes.append(f"{dir_lbl} wind → cold advection −2.5° (strong ≥25kt)")
    if not adj_notes:
        adj_notes = ["Neutral — no strong modifying factor"]

    # ── Discord embed ─────────────────────────────────────────────────────────
    color = 0x2ECC71 if best["edge"] > 0 else 0xE74C3C

    # Build a clean general title: "Highest temperature in Seoul on June 5?"
    # For next_day cities (Asia/Pacific) always show utc_tomorrow's date,
    # regardless of what date the Polymarket question text contains.
    # For today cities (Americas/Europe) extract the date from the question.
    from datetime import timedelta as _td
    if next_day:
        # Asia/Pacific: market resolves on UTC tomorrow
        _tomorrow = (datetime.now(timezone.utc) + _td(days=1))
        date_str = f" on {_tomorrow.strftime('%B')} {_tomorrow.day}"
    else:
        # Americas/Europe: use the city's LOCAL date — not the question text.
        # Polymarket posts markets the evening before UTC, so the question title
        # already says tomorrow's date (e.g. "June 12") even though the market
        # resolves TODAY in the city's local timezone. Parsing the question text
        # was the root cause of the "showing tomorrow's date" bug.
        try:
            city_tz   = ZoneInfo(city["tz"])
            local_now = datetime.now(city_tz)
            date_str  = f" on {local_now.strftime('%B')} {local_now.day}"
        except Exception:
            _today = datetime.now(timezone.utc)
            date_str = f" on {_today.strftime('%B')} {_today.day}"
    clean_title = f"Highest temperature in {city['city']}{date_str}?"

    e = discord.Embed(
        title=f"⚡ {city['city']} — Edge Alert ({day_s})",
        description=f"*{clean_title}*",
        color=color,
        timestamp=datetime.now(timezone.utc)
    )

    # ── Section B: Full METAR Live Observation ────────────────────────────────
    e.add_field(
        name="🛬 METAR Live Obs",
        value=(
            f"**Temp:** `{obs_s}`  **Dewpoint:** `{dewp_s}`\n"
            f"**Wind:** `{wind_s}`\n"
            f"**Pressure:** `{press_s}`  **Visibility:** `{visib_s}`\n"
            f"**Clouds:** `{cloud_s}`"
        ),
        inline=False
    )

    # ── Section C: Wind + Cloud Adjustment (guide Step 4) ────────────────────
    e.add_field(
        name="🌬 Wind & Cloud Adj",
        value="\n".join(f"• {n}" for n in adj_notes) + f"\n**Advection:** `{adv_s}`",
        inline=False
    )

    # ── Section C2: Atmospheric Stability Score ───────────────────────────────
    # Score 1–10 derived from three physical components already in the pipeline:
    #   1. METAR T-Td spread (boundary-layer moisture / LI proxy)
    #   2. METAR cloud cover & base height
    #   3. M1/M2/M3 model spread (forecast uncertainty = atmospheric complexity)
    stab_bar = "█" * stab_score + "░" * (10 - stab_score)
    _tc = metar.get("temp_c")
    _td = metar.get("dewp_c")
    _ttd_s = f"{float(_tc) - float(_td):.1f}°C" if (_tc is not None and _td is not None) else "—"
    e.add_field(
        name="🌪 Atmospheric Stability",
        value=(
            f"`{stab_bar}` **{stab_score}/10** — {stab_label}\n"
            f"• T−Td spread: `{_ttd_s}` "
            f"• Clouds: `{_cloud_layers_str(clouds)}`"
        ),
        inline=False
    )
    # ── Section D: 3-Model Forecast Stack ────────────────────────────────────
    m1_s = f"`{c_to_f(om['m1_max']):.1f}°F`" if unit == "F" and om.get('m1_max') else f"`{om.get('m1_max','—')}°C`" if om.get('m1_max') else "`—`"
    m2_s = f"`{c_to_f(om['m2_max']):.1f}°F`" if unit == "F" and om.get('m2_max') else f"`{om.get('m2_max','—')}°C`" if om.get('m2_max') else "`—`"
    m3_s = f"`{m3_val:.1f}{sym}`" if m3_val else "`unavailable`"

    e.add_field(name="🌡 M1 ECMWF (50%)",    value=m1_s,  inline=True)
    e.add_field(name="🗺 M2 Regional (30%)",  value=m2_s,  inline=True)
    e.add_field(name="📡 M3 Local NWS (20%)", value=m3_s,  inline=True)
    e.add_field(name="☁ Peak CC 13–17h",     value=f"`{om['avg_cloud_pct']:.0f}%`", inline=True)
    e.add_field(name="📊 Consensus Max",      value=f"`{mu:.1f}{sym}` ±{sigma:.1f}", inline=True)
    e.add_field(name="\u200b", value="\u200b", inline=True)  # spacer

    # ── Section E: All Buckets ────────────────────────────────────────────────
    # Show every bucket with market price, model probability, and edge.
    # Sorted low→high. Best bucket marked with ◀ TRADE.
    lines = ""
    for b in sorted(buckets, key=lambda x: x.get("low", 0)):
        mk = " ◀ **TRADE**" if b is best else ""
        lines += f"`{b['label']}` Mkt:{b['price']:.1%} Mine:{b['my_prob']:.1%} Edge:{b['edge']:+.1%}{mk}\n"
    e.add_field(name="📋 All Buckets", value=lines or "N/A", inline=False)

    # ── Section F: Action + Sizing ────────────────────────────────────────────
    e.add_field(name="⚡ Action",        value=f"**{act}** `{best['label']}`", inline=True)
    e.add_field(name="📈 Edge",          value=f"**{abs(best['edge'])*100:.1f}%**", inline=True)
    e.add_field(name="💰 Kelly (×0.15)", value=f"`{min(abs(best['edge'])*15, 5):.1f}%` of roll", inline=True)

    e.set_footer(
        text=(
            f"Station:{city['icao']} | {day_s} | "
            f"ECMWF 50%+Regional 30%+LocalNWS 20% | "
            f"{calib_summary + ' | ' if calib_summary else ''}"
            f"Not financial advice"
        )
    )
    return e


# ─────────────────────────────────────────────────────────────
# BOT
# ─────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
bot     = discord.Client(intents=intents)

@bot.event
async def on_ready():
    log.info(f"Logged in as {bot.user}")
    poll.start()


@bot.event
async def on_message(message: discord.Message):
    """
    Handle manual commands for calibration and demo ledger management.

    Commands:
      !resolve <City> <date YYYY-MM-DD> <actual_temp>
          Record the actual resolved temperature for a city+date.
          Example: !resolve Austin 2026-06-10 92.0
          This fills in the 'actual' field in calibration.json so the
          rolling bias/MAE can be computed for that city.

      !demo_stats
          Post a summary of the demo trade ledger to the channel.

      !calib <City>
          Show calibration status for one city.
    """
    if message.author.bot:
        return

    content = message.content.strip()

    # ── !resolve <City> <YYYY-MM-DD> <actual> ────────────────────────────────
    if content.startswith("!resolve "):
        parts = content.split()
        # Support city names with spaces: "!resolve New York 2026-06-10 88.0"
        # Date is always penultimate, actual is last, city is everything between
        if len(parts) < 4:
            await message.channel.send(
                "❌ Usage: `!resolve <City> <YYYY-MM-DD> <actual_temp>`\n"
                "Example: `!resolve Austin 2026-06-10 92.0`"
            )
            return
        try:
            actual_temp = float(parts[-1])
            date_str    = parts[-2]
            city_name   = " ".join(parts[1:-2])
            # Validate date format
            datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, IndexError):
            await message.channel.send(
                "❌ Could not parse command. Format: `!resolve <City> <YYYY-MM-DD> <actual>`\n"
                "Example: `!resolve New York 2026-06-10 88.5`"
            )
            return

        calib.record_actual(city_name, date_str, actual_temp)
        n    = calib.resolved_count(city_name)
        summ = calib.summary(city_name)
        await message.channel.send(
            f"✅ **Recorded actual for {city_name} on {date_str}:** `{actual_temp}°`\n"
            f"{summ}\n"
            f"{'🎉 Calibration now **ACTIVE** — dynamic σ and bias correction enabled!' if n >= MIN_CALIB_SAMPLES else f'⏳ {MIN_CALIB_SAMPLES - n} more resolved markets needed.'}"
        )
        return

    # ── !demo_stats ───────────────────────────────────────────────────────────
    if content.startswith("!demo_stats"):
        stats = demo_ledger.stats()
        wr_s  = f"{stats['win_rate']:.0%}" if stats['win_rate'] is not None else "pending (no resolved trades yet)"
        pnl_s = f"{stats['mean_pnl']:+.4f}" if stats['mean_pnl'] is not None else "pending"
        lines = [
            f"**📊 Demo Trade Ledger Summary**",
            f"Total signals logged: **{stats['total']}**",
            f"Resolved: **{stats['resolved']}**",
            f"Wins: **{stats['wins']}**",
            f"Win rate: **{wr_s}**",
            f"Mean P&L per trade: **{pnl_s}**",
            f"Ledger file: `{DEMO_TRADES_FILE}`",
        ]
        await message.channel.send("\n".join(lines))
        return

    # ── !calib <City> ─────────────────────────────────────────────────────────
    if content.startswith("!calib "):
        city_name = content[7:].strip()
        n    = calib.resolved_count(city_name)
        summ = calib.summary(city_name)
        # Show last 5 resolved entries
        entries = [e for e in calib._data.get(city_name, []) if e.get("actual") is not None]
        recent  = entries[-5:] if entries else []
        rows    = "\n".join(
            f"`{e['date']}` mu={e['mu']:.1f} actual={e['actual']:.1f} err={e['error']:+.1f}"
            for e in recent
        ) or "No resolved entries yet."
        await message.channel.send(
            f"**📊 Calibration: {city_name}**\n"
            f"{summ}\n"
            f"**Last {len(recent)} resolved:**\n{rows}"
        )

async def _fetch_tomorrow_markets(session, city: dict, city_name: str,
                                   prefetched_markets: list | None = None) -> list:
    """
    Return tomorrow's market pairs for one city.

    NEVER calls get_markets() again — that caused hundreds of log lines per
    resolved city as all 500+ markets were re-fetched and re-logged.

    Instead:
      1. If prefetched_markets is provided (from poll's city_markets_nextday),
         filter it for this city. This is the normal path.
      2. Only if prefetched_markets is None (shouldn't happen in normal flow)
         do a SILENT targeted fetch — a single Gamma query for this city,
         with no per-market logging.
    """
    from datetime import date as _date, timedelta as _td
    utc_tomorrow = (datetime.now(timezone.utc) + _td(days=1)).date()

    # ── Path 1: use already-fetched next-day markets (zero extra API calls) ──
    if prefetched_markets is not None:
        pairs = []
        for city_obj, market in prefetched_markets:
            q        = market.get("question", "")
            mkt_date = date_from_question(q)
            if mkt_date is None or mkt_date == utc_tomorrow:
                pairs.append((city_obj, market))
        log.info(f"  {city_name}: tomorrow markets from prefetch → {len(pairs)} buckets")
        return pairs

    # ── Path 2: targeted single-city Gamma query (no full market log) ────────
    # Only reached if poll() didn't pre-build city_markets_nextday (safety net).
    log.info(f"  {city_name}: no prefetch — targeted Gamma query for tomorrow")
    city_slug = city_name.lower().replace(" ", "-")
    urls = [
        f"https://gamma-api.polymarket.com/events?tag_slug=weather&active=true&closed=false&limit=100",
    ]
    pairs = []
    seen: set = set()
    import aiohttp as _aiohttp
    for url in urls:
        try:
            async with session.get(url, timeout=_aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    continue
                events = await r.json()
                if not isinstance(events, list):
                    continue
                for ev in events:
                    for market in ev.get("markets", []):
                        mid = market.get("id") or market.get("question", "")
                        if mid in seen:
                            continue
                        seen.add(mid)
                        q = market.get("question", "")
                        if city_name.lower() not in q.lower():
                            continue
                        mkt_date = date_from_question(q)
                        if mkt_date is None or mkt_date == utc_tomorrow:
                            pairs.append((city, market))
        except Exception as e:
            log.warning(f"  {city_name}: targeted fetch error — {e}")

    log.info(f"  {city_name}: targeted fetch → {len(pairs)} tomorrow buckets")
    return pairs


async def _process_one_city(s, ch, city_name: str, city_market_pairs: list,
                           next_day_pairs: list | None = None,
                           force_next_day: bool = False) -> bool:
    """
    Bot Architecture Steps 1–6 (guide) for one city:

    Step 1  Market Discovery  — already done in poll(); markets passed in
    Step 2  METAR Pull        — temp, dewpoint, wind dir/speed/gust,
                                pressure, visibility, ALL cloud layers
    Step 3  Forecast Pull     — M1 ECMWF (50%) + M2 regional (30%) + M3 local NWS (20%)
                                next_day=True for Asian/Pacific cities (UTC+5:30..+13)
    Step 4  Wind+Cloud Adj    — cloud suppression + advection correction
    Step 5  Edge Calculation  — Normal CDF per bucket, Kelly ×0.15
    Step 6  Alert to Discord  — full embed with all METAR fields + 3-model stack
    """
    city     = city_market_pairs[0][0]
    sym      = "°F" if city["unit"] == "F" else "°C"
    next_day = city.get("next_day", False) or force_next_day
    day_lbl  = "TOMORROW" if next_day else "TODAY"

    # ── Step 1 scan announcement ─────────────────────────────────────────────
    scanning_msg = await ch.send(
        f"🔍 **Scanning {city_name}** ({day_lbl}) — "
        f"METAR `{city['icao']}` + Open-Meteo M1/M2 + local NWS M3…"
    )
    await asyncio.sleep(0.5)

    # ── Step 2+3: fetch METAR, M1+M2 (Open-Meteo), M3 (local NWS) in parallel ─
    metar, om, m3_val = await asyncio.gather(
        get_metar(s, city["icao"]),
        get_openmeteo(s, city, next_day=next_day),
        get_m3(s, city, next_day=next_day),
    )

    if not metar or not om:
        await scanning_msg.edit(
            content=(
                f"⚠️ **{city_name}** ({day_lbl}) — "
                f"data unavailable (METAR or forecast missing), skipped."
            )
        )
        log.info(f"  {city_name}: missing weather data, skip")
        return False

    # ── Step 3 log: full METAR obs ───────────────────────────────────────────
    log.info(
        f"  {city_name} [{day_lbl}]: "
        f"temp={metar.get('temp_c')}°C dew={metar.get('dewp_c')}°C "
        f"wind={metar.get('wdir')}°@{metar.get('wspd_kt')}kt "
        f"press={metar.get('press_hpa')}hPa vis={metar.get('visib_sm')}SM "
        f"clouds={[l.get('cover') for l in metar.get('clouds',[])]} "
        f"M1={om['m1_max']}°C M2={om['m2_max']}°C CC={om['avg_cloud_pct']:.0f}% M3={m3_val}"
    )

    # ── Step 4+5: consensus + edge ───────────────────────────────────────────
    # Compute local hour of METAR observation for the METAR floor guard (Fix 1).
    # METAR obs timestamps are "now" — use the city's local time as a proxy.
    try:
        _city_tz_obj   = ZoneInfo(city["tz"])
        _metar_local_h = datetime.now(_city_tz_obj).hour
    except Exception:
        _metar_local_h = None

    mu_raw, sigma_raw = consensus(
        om["m1_max"], om["m2_max"], m3_val,
        city["unit"],
        om["avg_cloud_pct"],
        metar.get("wdir"), metar.get("wspd_kt"),
        metar_temp_c=metar.get("temp_c"),      # hard floor — consensus ≥ live obs
        metar_local_hour=_metar_local_h,        # v23: only apply floor before 14:00 local
        lat=city["lat"],                         # v23: SH advection reversal for lat < 0
    )
    if mu_raw is None:
        await scanning_msg.edit(
            content=f"⚠️ **{city_name}** — forecast models returned no data, skipped."
        )
        log.info(f"  {city_name}: consensus failed, skip")
        return False

    # ── Calibration adjustment (v20) ──────────────────────────────────────────
    # Apply rolling 14-day bias + MAE from resolved markets as dynamic sigma.
    # Before 30 resolved markets: mu/sigma unchanged (fixed 1.0° sigma).
    # After 30 resolved markets:  mu adjusted by rolling bias; sigma = rolling MAE.
    from datetime import date as _cdate
    _target_date_str = (
        (datetime.now(timezone.utc).date() + __import__("datetime").timedelta(days=1)).isoformat()
        if next_day else
        datetime.now(timezone.utc).date().isoformat()
    )
    mu, sigma = calib.adjusted(city_name, mu_raw, sigma_raw)
    calib_n   = calib.resolved_count(city_name)
    calib_sum = calib.summary(city_name)

    # Log this prediction to the calibration file (upserts if already logged today)
    calib.log_prediction(city_name, _target_date_str, mu, sigma, city["unit"])

    log.info(f"  {city_name}: consensus mu={mu:.1f}{sym} sigma={sigma:.1f} (calib n={calib_n})")

    best_market  = None
    best_edge    = 0.0
    best_buckets = []

    # Resolution threshold: if any bucket's market price ≥ this, the market is
    # effectively resolved — skip it and look for the next-day market instead.
    RESOLUTION_THRESHOLD = 0.90

    for _, market in city_market_pairs:
        buckets = parse_buckets(market)
        if not buckets:
            continue
        buckets  = bucket_edge(buckets, mu, sigma)
        top_edge = max(abs(b["edge"]) for b in buckets)
        if top_edge > best_edge:
            best_edge    = top_edge
            best_market  = market
            best_buckets = buckets

    # ── 90% Resolution check: market effectively resolved → switch to next day ──
    # If the highest market price across all buckets is ≥ 90%, the market has
    # priced in the outcome. Immediately retry with next-day markets if available.
    # SKIP this check when force_next_day=True — we already switched; just run it.
    if not force_next_day and best_market is not None and best_buckets:
        max_mkt_price = max(b.get("price", 0.0) or 0.0 for b in best_buckets)
        if max_mkt_price >= RESOLUTION_THRESHOLD:
            resolved_label = next(
                (b["label"] for b in best_buckets if (b.get("price") or 0.0) >= RESOLUTION_THRESHOLD),
                "unknown"
            )
            log.info(
                f"  {city_name}: market resolved at {max_mkt_price:.0%} "
                f"({resolved_label}) — switching to next-day market immediately"
            )
            if next_day_pairs:
                # Retry immediately with next-day markets in the same poll
                await scanning_msg.edit(
                    content=(
                        f"🔄 **{city_name}** ({day_lbl}) resolved at `{max_mkt_price:.0%}` "
                        f"→ switching to **next-day market** now…"
                    )
                )
                await asyncio.sleep(0.3)
                return await _process_one_city(
                    s, ch, city_name, next_day_pairs,
                    next_day_pairs=None,    # no further fallback
                    force_next_day=True     # fetch tomorrow's forecast + show TOMORROW
                )
            else:
                # No tomorrow markets pre-fetched — use the nd_pairs collected
                # by poll() if available, otherwise do a targeted single-city query.
                # NEVER call get_markets() here — that logs all 500+ markets again.
                await scanning_msg.edit(
                    content=(
                        f"🔄 **{city_name}** resolved at `{max_mkt_price:.0%}` "
                        f"→ checking **tomorrow's market**…"
                    )
                )
                tomorrow_pairs = await _fetch_tomorrow_markets(
                    s, city, city_name,
                    prefetched_markets=next_day_pairs  # None triggers targeted query
                )
                if tomorrow_pairs:
                    log.info(f"  {city_name}: found {len(tomorrow_pairs)} tomorrow markets via live fetch")
                    return await _process_one_city(
                        s, ch, city_name, tomorrow_pairs,
                        next_day_pairs=None,
                        force_next_day=True
                    )
                else:
                    await scanning_msg.edit(
                        content=(
                            f"🔒 **{city_name}** — today resolved at `{max_mkt_price:.0%}`. "
                            f"Tomorrow's market not open yet on Polymarket."
                        )
                    )
                    log.info(f"  {city_name}: no tomorrow market available on Gamma")
                    return False

    # ── Step 6: alert or silent ───────────────────────────────────────────────
    if best_market is None or best_edge < MIN_EDGE:
        await scanning_msg.edit(
            content=(
                f"✅ **{city_name}** ({day_lbl}) scanned — "
                f"consensus max `{mu:.1f}{sym}` ±{sigma:.1f} | "
                f"best edge `{best_edge:.1%}` < threshold `{MIN_EDGE:.0%}`, no trade."
            )
        )
        log.info(f"  {city_name}: edge {best_edge:.1%} < threshold, no alert")
        return False

    try:
        await scanning_msg.delete()
    except Exception:
        pass

    # ── Step 5b: atmospheric stability score ─────────────────────────────────
    stab_score, stab_label = atmospheric_stability_score(
        temp_c  = metar.get("temp_c"),
        dewp_c  = metar.get("dewp_c"),
        clouds  = metar.get("clouds", []),
        m1_c    = om.get("m1_max"),
        m2_c    = om.get("m2_max"),
        m3_val  = m3_val,
        unit    = city["unit"],
    )
    log.info(f"  {city_name}: stability={stab_score}/10 ({stab_label})")

    embed = build_embed(
        city, metar, om, m3_val, mu, sigma,
        best_buckets, best_market, next_day=next_day,
        stab_score=stab_score, stab_label=stab_label,
        calib_summary=calib_sum,
    )
    await ch.send(embed=embed)
    log.info(f"  ✅ Alert: {city_name} ({day_lbl}) edge={best_edge:.1%} mu={mu:.1f}{sym}")

    # ── Demo trade (v20) ──────────────────────────────────────────────────────
    # DEMO_MODE=true (default) → paper-trade only, no real CLOB orders.
    # Logs to data/demo_trades.json and posts a [DEMO] summary to Discord.
    if DEMO_MODE:
        best_b   = max(best_buckets, key=lambda b: abs(b["edge"]))
        action   = "BUY YES" if best_b["edge"] > 0 else "BUY NO"
        kelly    = min(abs(best_b["edge"]) * 15, 5.0)
        trade    = demo_ledger.log_trade(
            city       = city_name,
            date_str   = _target_date_str,
            market_q   = best_market.get("question", ""),
            action     = action,
            label      = best_b["label"],
            edge       = best_b["edge"],
            kelly_pct  = kelly,
            mu         = mu,
            sigma      = sigma,
            unit       = city["unit"],
            calib_n    = calib_n,
        )
        stats = demo_ledger.stats()
        sym_u = "°F" if city["unit"] == "F" else "°C"
        demo_embed = discord.Embed(
            title=f"🧾 [DEMO] Paper Trade — {city_name}",
            description=(
                f"*No real order placed — demo mode active (`DEMO_MODE=true`)*\n"
                f"Set `DEMO_MODE=false` in `.env` only when connected to a real wallet."
            ),
            color=0x95A5A6,
            timestamp=datetime.now(timezone.utc),
        )
        demo_embed.add_field(
            name="📋 Trade Details",
            value=(
                f"**Action:** `{action}` `{best_b['label']}`\n"
                f"**Edge:** `{abs(best_b['edge'])*100:.1f}%`\n"
                f"**Kelly (×0.15):** `{kelly:.1f}%` of bankroll\n"
                f"**Consensus:** `{mu:.1f}{sym_u}` ±{sigma:.1f}"
            ),
            inline=False,
        )
        demo_embed.add_field(
            name="📊 Calibration Status",
            value=(
                f"{calib_sum}\n"
                f"{'⚡ *Bias+MAE corrections active*' if calib_n >= MIN_CALIB_SAMPLES else f'⏳ *{MIN_CALIB_SAMPLES - calib_n} more resolved markets needed to activate dynamic σ*'}"
            ),
            inline=False,
        )
        _wr = f"{stats['win_rate']:.0%}" if stats['win_rate'] is not None else "pending"
        demo_embed.add_field(
            name="📈 Demo Ledger",
            value=(
                f"**Total signals:** {stats['total']} | "
                f"**Resolved:** {stats['resolved']} | "
                f"**Win rate:** {_wr}"
            ),
            inline=False,
        )
        demo_embed.set_footer(text=f"Logged to {DEMO_TRADES_FILE} | Not financial advice")
        await ch.send(embed=demo_embed)
        log.info(f"  [DEMO] Logged trade: {city_name} {action} {best_b['label']} kelly={kelly:.1f}%")

    return True


@tasks.loop(minutes=POLL_MINUTES)
async def poll():
    log.info("── poll start ──")
    ch = bot.get_channel(ALERT_CHANNEL_ID)
    if not ch:
        try:
            ch = await bot.fetch_channel(ALERT_CHANNEL_ID)
        except discord.NotFound:
            log.error(f"Channel {ALERT_CHANNEL_ID} not found — check the ID and bot permissions"); return
        except discord.Forbidden:
            log.error(f"Channel {ALERT_CHANNEL_ID} found but bot has no access — grant bot View Channel + Send Messages permissions"); return
        except Exception as e:
            log.error(f"Channel fetch failed: {e}"); return

    connector = aiohttp.TCPConnector(use_dns_cache=False, resolver=aiohttp.DefaultResolver())
    async with aiohttp.ClientSession(headers={"User-Agent": "WeatherBot/1.0"}, connector=connector) as s:

        # ── Fetch all open weather markets ───────────────────────────────────
        await ch.send("📡 **Poll started** — fetching open Polymarket weather markets…")
        markets = await get_markets(s)
        if not markets:
            await ch.send("😴 No open weather markets found on Polymarket right now. Will retry next poll.")
            log.info("No matching Polymarket markets — bot will retry next poll.")
            return

        # ── Group markets by city, filtered to the correct TARGET DATE ─────────
        # Americas + Europe (next_day=False) → target = today (UTC date only)
        # Asia + Pacific   (next_day=True)   → Polymarket labels these markets with
        #   the LOCAL Asian date (e.g. "June 4") which equals UTC today when Asian
        #   cities are already in June 4 local time. Accept BOTH utc_today AND
        #   utc_tomorrow for next_day cities, since Polymarket may use either label.
        # If the question has no parseable date, we accept the market (fallback).
        from collections import defaultdict
        from datetime import date as _date, timedelta as _td

        utc_today    = datetime.now(timezone.utc).date()
        utc_tomorrow = utc_today + _td(days=1)

        # Bucket A: utc_tomorrow markets for next_day=True cities (preferred)
        # Bucket B: utc_today  markets for next_day=True cities (fallback)
        # Bucket C: utc_today  markets for next_day=False cities (primary)
        # Bucket D: utc_tomorrow markets for next_day=False cities (resolution fallback)
        #           — collected but NOT used as primary; passed as nd_pairs so that
        #             when today's market resolves ≥90%, we switch immediately.
        city_markets_tomorrow: dict[str, list] = defaultdict(list)  # A: asian next-day preferred
        city_markets_today:    dict[str, list] = defaultdict(list)  # B: asian today fallback
        city_markets: dict[str, list] = defaultdict(list)           # C: americas/europe today
        city_markets_nextday:  dict[str, list] = defaultdict(list)  # D: americas/europe tomorrow

        date_skip = 0
        for market in markets:
            q    = market.get("question", "")
            city = city_from_question(q)
            if not city:
                log.info(f"  [no city match] {q[:100]}")
                continue

            mkt_date = date_from_question(q)

            if city.get("next_day", False):
                # Asian/Pacific: prefer utc_tomorrow, fall back to utc_today
                if mkt_date is None:
                    city_markets_tomorrow[city["city"]].append((city, market))
                elif mkt_date == utc_tomorrow:
                    city_markets_tomorrow[city["city"]].append((city, market))
                elif mkt_date == utc_today:
                    city_markets_today[city["city"]].append((city, market))
                else:
                    log.debug(f"  Date skip: {city['city']} (TOMORROW) mkt_date={mkt_date}")
                    date_skip += 1
            else:
                # Americas + Europe:
                # Primary bucket:  markets dated utc_today OR no date → city_markets
                # Fallback bucket: markets dated utc_tomorrow          → city_markets_nextday
                #   (used when today's market resolves ≥90% mid-day)
                # Do NOT lump utc_tomorrow into the primary bucket — that was the root
                # cause of the "showing tomorrow's date / wrong forecast day" bug.
                # Polymarket posts US city markets ~evening UTC; at that time utc_today
                # in the city's LOCAL timezone is still the correct resolution date even
                # though UTC has ticked over.  The embed title now uses the city's local
                # timezone (Fix 1) so it will always display the right date regardless.
                if mkt_date is None or mkt_date == utc_today:
                    city_markets[city["city"]].append((city, market))
                elif mkt_date == utc_tomorrow:
                    city_markets_nextday[city["city"]].append((city, market))
                    log.debug(f"  {city['city']}: utc_tomorrow market queued as next-day fallback")
                else:
                    log.info(f"  Date skip: {city['city']} (TODAY) mkt_date={mkt_date} today={utc_today} tomorrow={utc_tomorrow}")
                    date_skip += 1

        # For Americas/Europe cities: if no utc_today market exists yet, promote
        # utc_tomorrow markets into the primary bucket so the bot still runs.
        # This handles the window (typically 20:00–02:00 UTC) when Polymarket has
        # already posted tomorrow's markets but today's have all resolved/closed.
        for city_name_key in list(city_markets_nextday.keys()):
            if city_name_key not in city_markets or not city_markets[city_name_key]:
                city_markets[city_name_key] = city_markets_nextday[city_name_key]
                log.info(
                    f"  {city_name_key}: no utc_today market — promoted "
                    f"{len(city_markets_nextday[city_name_key])} utc_tomorrow bucket(s) to primary"
                )

        # Merge next_day cities: prefer utc_tomorrow bucket; use utc_today only as fallback
        for city_name_key in set(list(city_markets_tomorrow.keys()) + list(city_markets_today.keys())):
            if city_markets_tomorrow.get(city_name_key):
                city_markets[city_name_key] = city_markets_tomorrow[city_name_key]
                log.debug(f"  {city_name_key}: using utc_tomorrow markets ({len(city_markets_tomorrow[city_name_key])} buckets)")
            else:
                city_markets[city_name_key] = city_markets_today[city_name_key]
                log.info(f"  {city_name_key}: no utc_tomorrow market found — falling back to utc_today")

        # Log what US cities were found (or not) — critical for debugging
        us_cities = ["New York","Los Angeles","Chicago","Miami","Dallas",
                     "Houston","Atlanta","Denver","Seattle","San Francisco","Austin"]
        for uc in us_cities:
            if uc in city_markets:
                log.info(f"  ✅ US city found: {uc} ({len(city_markets[uc])} buckets)")
            else:
                log.info(f"  ❌ US city NOT found in markets: {uc}")

        total_cities = len(city_markets)
        today_cities    = [n for n in city_markets if not CITY_MAP.get(n, {}).get("next_day", False)]
        tomorrow_cities = [n for n in city_markets if CITY_MAP.get(n, {}).get("next_day", False)]
        await ch.send(
            f"📋 Found **{len(markets)} markets** → **{date_skip}** skipped (wrong date) → "
            f"**{sum(len(v) for v in city_markets.values())} matched** across **{total_cities} cities**.\n"
            f"📅 **TODAY** `{utc_today}` (Americas+Europe): {', '.join(today_cities) or 'none'}\n"
            f"📅 **TOMORROW** `{utc_tomorrow}` (Asia+Pacific): {', '.join(tomorrow_cities) or 'none'}\n"
            f"Scanning one city at a time…"
        )
        await asyncio.sleep(1)

        alerts_sent = 0
        # ── ONE CITY AT A TIME — full pipeline per city before moving on ──────
        # v16: NO deduplication — every city re-runs every poll so METAR updates
        # and market price shifts are caught on each cycle.
        for idx, (city_name, city_market_pairs) in enumerate(city_markets.items(), 1):
            log.info(f"  [{idx}/{total_cities}] Processing {city_name}…")

            city_cfg = CITY_MAP.get(city_name, {})

            try:
                if not city_cfg.get("next_day", False):
                    nd_pairs = city_markets_nextday.get(city_name) or None
                else:
                    nd_pairs = None

                sent = await _process_one_city(s, ch, city_name, city_market_pairs,
                                               next_day_pairs=nd_pairs)
                if sent:
                    alerts_sent += 1
                    await asyncio.sleep(3)
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                log.error(f"  {city_name}: unexpected error — {e}")
                await asyncio.sleep(1)

        # ── Poll summary ──────────────────────────────────────────────────────
        summary = (
            f"✅ **Poll complete** — scanned {total_cities} cities, "
            f"sent **{alerts_sent} alert{'s' if alerts_sent != 1 else ''}**. "
            f"Next scan in {POLL_MINUTES} min."
        )
        await ch.send(summary)
        log.info(f"── poll end — {alerts_sent}/{total_cities} alerts sent ──")

@poll.before_loop
async def before_poll():
    await bot.wait_until_ready()


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("❌ DISCORD_TOKEN missing — add it to your .env file:\n  DISCORD_TOKEN=your_token_here")
    if not ALERT_CHANNEL_ID:
        raise SystemExit("❌ ALERT_CHANNEL_ID missing — add it to your .env file:\n  ALERT_CHANNEL_ID=your_channel_id_here")

    bot.run(DISCORD_TOKEN)
