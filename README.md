# 🌡️ Polymarket Weather Discord Bot

A sophisticated automated trading bot that analyzes real-time weather data across 38 global cities and identifies profitable edges in Polymarket's temperature prediction markets.

## 📋 Overview

This bot synthesizes live METAR observations, three independent weather forecast models, and Polymarket's pricing to generate daily maximum temperature predictions. It uses statistical analysis to detect market mispricings and posts actionable alerts to Discord with complete meteorological context.

**Key Features:**
- ✅ Real-time weather data from 4 free sources (no API keys required)
- ✅ 3-model consensus forecasting with adaptive weighting
- ✅ Wind & cloud adjustments based on physical atmospheric science
- ✅ Automated edge detection and Kelly criterion sizing
- ✅ Complete METAR analysis in every alert
- ✅ Atmospheric stability scoring (1–10 scale)
- ✅ Calibration tracking for rolling forecast accuracy
- ✅ Demo mode for paper-trading and backtesting
- ✅ 38 cities across Americas, Europe, and Africa

---

## 🏗️ Architecture

### 6-Step Pipeline (Per City Per Poll)

1. **Market Discovery** → Fetch open temperature markets from Polymarket's Gamma API
2. **METAR Pull** → Live observation: temperature, dewpoint, wind, pressure, visibility, cloud layers
3. **Forecast Blend** → Three models (ECMWF M1 50%, Regional M2 30%, Local NWS M3 20%)
4. **Wind+Cloud Adj** → Physical adjustments per meteorological guide tables
5. **Consensus & Edge** → Weighted max-temp forecast vs. market probabilities
6. **Discord Alert** → Full embed with all observations, models, and trade recommendation

### Data Sources (All Free)

| Model | Source | Coverage | Resolution |
|-------|--------|----------|-----------|
| **M1** | ECMWF IFS 025 (Open-Meteo) | Global | Daily max °C |
| **M2** | Regional (Open-Meteo) | City-specific | Daily max °C |
| **M3** | Local NWS APIs | Country/region-specific | Daily max (°F or °C) |
| **METAR** | aviationweather.gov | All major airports | Updated every 30 min |
| **Markets** | Polymarket Gamma API | Active only | Open pricing + outcomes |

---

## 📦 Installation

### 1. Clone & Setup

```bash
git clone https://github.com/jobwafula-png/private.git
cd private
pip install -r requirements.txt
```

### 2. Environment Configuration

Create a `.env` file in the project root:

```env
# Discord Bot
DISCORD_TOKEN=your_discord_bot_token_here
ALERT_CHANNEL_ID=your_alert_channel_id_here

# Bot Settings
MIN_EDGE=0.08          # Minimum edge (default 8%) to trigger an alert
POLL_MINUTES=30        # Poll frequency in minutes
DEMO_MODE=true         # Set to false only when connected to a real Polymarket wallet
DATA_DIR=data          # Directory for calibration & trade logs

# Uncomment only if you have a real Polymarket CLOB wallet connected
# POLYMARKET_WALLET_ADDRESS=0x...
```

### 3. Get Discord Bot Token

1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Create a new application → copy **TOKEN**
3. Enable **Message Content Intent** in the Privileged Gateway Intents section
4. Create an invite with `bot` scope + `View Channel`, `Send Messages`, `Embed Links` permissions

### 4. Find Your Channel ID

Right-click your Discord channel → Copy Channel ID (Developer Mode must be enabled)

---

## 🚀 Running the Bot

```bash
python bot_v23.py
```

The bot will:
- Log in and wait for ready state
- Poll Polymarket every N minutes (default: 30)
- Scan all 38 cities sequentially
- Post alerts only if edge exceeds `MIN_EDGE`
- Maintain calibration.json and demo_trades.json in the `data/` folder

---

## 📊 Discord Commands

Run these commands in the alert channel:

### `!resolve <City> <YYYY-MM-DD> <actual_temp>`
Record the actual resolved temperature after a Polymarket market closes.

```
!resolve Austin 2026-06-10 92.0
```

This fills in the calibration history so the bot can compute rolling forecast error (MAE) and bias. After 30 resolved markets per city, the bot automatically applies dynamic sigma and bias correction.

### `!demo_stats`
Show summary of the demo trade ledger:

```
!demo_stats
```

Returns: total signals, resolved trades, win rate, mean P&L.

### `!calib <City>`
Show calibration status and recent resolved entries:

```
!calib Austin
```

Returns: resolved count, rolling MAE, rolling bias, last 5 trades.

---

## 🎯 Understanding the Alert Embed

### Example Alert Structure

```
⚡ Austin — Edge Alert (TODAY)
"Highest temperature in Austin on June 12?"

🛬 METAR Live Obs
Temp: 28.2°C  Dewpoint: 15.3°C
Wind: 200°(SSW) @ 8kt
Pressure: 1012.5hPa  Visibility: 10SM
Clouds: BKN025 SCT050

🌬 Wind & Cloud Adj
• BKN → −1 to −4°C partial suppression
Advection: ⬆ WARM (S/SW)

🌪 Atmospheric Stability
████████░░ 8/10 — Stable 🟡
• T−Td spread: 12.9°C • Clouds: BKN025 SCT050

🌡 3-Model Forecast
M1 ECMWF (50%): 93.2°F
M2 Regional (30%): 92.8°F
M3 Local NWS (20%): 94.1°F
Peak CC 13–17h: 35%
Consensus Max: 93.4°F ±1.0

📋 All Buckets
`90–91°F` Mkt:45.0% Mine:12.3% Edge:−32.7%
`92–93°F` Mkt:30.0% Mine:35.6% Edge:+5.6% ◀ **TRADE**
`94–95°F` Mkt:20.0% Mine:42.1% Edge:+22.1%

⚡ Action: BUY YES ✅ `92–93°F`
📈 Edge: **5.6%**
💰 Kelly (×0.15): `0.8%` of roll

Station:KAUS | TODAY | ECMWF 50%+Regional 30%+LocalNWS 20% | 
📊 Calibration: 12/30 resolved markets | ...
```

### Field Explanations

- **METAR Live Obs** → Current airport conditions (every 30 min)
- **Wind & Cloud Adj** → Physical adjustments applied to the forecast
- **Atmospheric Stability** → 1–10 score from T-Td spread, cloud cover, and model agreement
- **3-Model Forecast** → Peak-hours (13–17h local) daily max from each source
- **Consensus Max** → Weighted blend (mu ± sigma)
- **All Buckets** → Market outcome labels, prices, and bot's probabilities
- **Action** → BUY YES (edge > 0) or BUY NO (edge < 0)
- **Kelly Sizing** → Risk-adjusted stake = min(edge × 15%, 5%)
- **Calibration** → Rolling 14-day forecast accuracy tracking

---

## 🔧 Key Parameters

### Weighting (Step 5)

```python
M1 (ECMWF):    50%   # Global reference model
M2 (Regional): 30%   # City-specific forecast
M3 (Local NWS):20%   # National weather service
```

### Adjustments (Step 4)

**Cloud Suppression (°C):**
- OVC < 30 base: −4.0°C
- OVC ≥ 30 base: −2.0°C
- BKN < 30 base: −1.5°C
- BKN ≥ 30 base: −1.0°C

**Wind Advection (°C, per hemisphere):**
| Wind Speed | Warm Sector | Cold Sector |
|---------|------|------|
| 0–5 kt | 0.0° | 0.0° |
| 6–14 kt | +0.5° | −0.5° |
| 15–24 kt | +1.0° | −1.0° |
| ≥25 kt | +1.5° | −2.5° |

### Floors (Step 5)

Consensus never drops below:
1. The lowest of the three model values
2. The current METAR observed temperature (before 14:00 local only)

This prevents overnight cold readings from being used as a floor for the next day's maximum.

---

## 📈 Calibration System

After 30 resolved markets per city, the bot automatically:

1. **Computes rolling 14-day bias** (mean signed error mu − actual)
   - Applied as a correction: mu_adjusted = mu − bias
   
2. **Computes rolling 14-day MAE** (mean absolute error)
   - Replaces fixed σ=1.0°: sigma_adjusted = max(MAE, 0.5)

3. **Applies adaptive adjustments** on each subsequent poll
   - Reduces overfitting to local meteorology
   - Tracks site-specific systematic errors

Use `!resolve` commands to feed back resolved market outcomes:

```
!resolve Austin 2026-06-10 92.0
!resolve Austin 2026-06-11 94.5
!resolve Austin 2026-06-12 91.3
...
```

---

## 🧪 Demo Mode (Paper Trading)

By default, `DEMO_MODE=true` means:

- **No real orders** sent to Polymarket CLOB
- All "trades" logged to `data/demo_trades.json`
- Discord alerts tagged with `[DEMO]`
- Full ledger accessible via `!demo_stats`

Schema (demo_trades.json):

```json
[
  {
    "ts": "2026-06-10T11:54:00Z",
    "city": "Austin",
    "date": "2026-06-10",
    "market_q": "Will the highest temperature in Austin be...",
    "action": "BUY YES",
    "label": "92–93°F",
    "edge": 0.056,
    "kelly_pct": 0.84,
    "mu": 93.4,
    "sigma": 1.0,
    "unit": "F",
    "calib_n": 12,
    "resolved": null,
    "pnl": null
  },
  ...
]
```

Set `DEMO_MODE=false` only after:
1. Thoroughly testing the bot's accuracy
2. Connecting a real Polymarket wallet to the environment
3. Understanding the edge calculation thoroughly

---

## 🌍 Supported Cities

### 38 Cities Across 3 Regions

**Americas (15):** New York, Los Angeles, Chicago, Miami, Dallas, Houston, Atlanta, Denver, Seattle, San Francisco, Austin, Toronto, Buenos Aires, Sao Paulo, Mexico City, Panama City

**Europe (11):** London, Paris, Madrid, Milan, Munich, Warsaw, Amsterdam, Helsinki, Istanbul, Ankara, Moscow, Tel Aviv

**Africa (1):** Cape Town

Each city has:
- ICAO code (METAR station)
- Timezone (for local hour adjustments)
- Latitude/longitude (for model queries)
- Region-specific NWS API (M3 dispatcher)
- Temperature unit (°F for US, °C elsewhere)

---

## 📝 Files Generated

```
data/
├── calibration.json    # Per-city forecast error tracking
│                        # { "Austin": [{"date", "mu", "sigma", "actual", "error", "unit"}] }
└── demo_trades.json    # All logged paper trades
                        # [{"ts", "city", "action", "edge", "pnl"}]
```

---

## 🐛 Troubleshooting

### "Channel not found" error
- Verify `ALERT_CHANNEL_ID` is correct (right-click channel → Copy ID)
- Ensure bot has View Channel + Send Messages permissions
- Check Developer Mode is enabled in Discord settings

### "No weather markets found"
- Polymarket typically posts daily weather markets 12:00–14:00 UTC
- Check [Polymarket directly](https://polymarket.com) for open markets
- Bot will automatically retry every 30 minutes

### "METAR temp seems wrong"
- METAR updates every 30 minutes; if bot runs between cycles, data may be stale
- Check [aviationweather.gov](https://aviationweather.gov) for the raw METAR
- Confirm the ICAO code in the city config is correct

### Bot stops responding
- Check `.env` file for typos in token/channel ID
- Review console logs for exception tracebacks
- Restart the bot process

---

## 🎓 Science & References

### Lifted Index Proxy (Stability Scoring)
Uses METAR T-Td spread as a proxy for boundary-layer moisture and convective risk.
- Reference: [NOAA SPC Sounding Analysis](https://www.spc.noaa.gov/exper/soundings/)

### Wind Advection Adjustment
Implements standard meteorological temperature advection rules:
- Warm sectors (S/SW NH, N/NW SH) raise max temp
- Cold sectors (N/NW NH, S/SW SH) lower max temp
- Asymmetric effect: cold advection stronger than warm

### Cloud Cover Suppression
Based on solar radiation reduction:
- Overcast (OVC) → −2 to −4°C (depending on base height)
- Broken (BKN) → −1 to −1.5°C
- Clear/Few → baseline or +1–3°C solar maximum

---

## 📄 License & Attribution

**Data Sources:**
- ECMWF via [Open-Meteo](https://open-meteo.com) (free, no key)
- METAR via [aviationweather.gov](https://aviationweather.gov)
- Markets via [Polymarket Gamma API](https://polymarket.com)
- Regional NWS APIs (varies by country)

**Not Financial Advice.** This bot is for research and educational purposes. Always verify edge calculations and risk management independently before trading real capital.

---

## 🤝 Contributing

Issues, PRs, and feedback welcome. Current focus areas:
- Expanding NWS coverage to additional cities
- Refining calibration decay window (14 days → adaptive)
- Backtesting framework for edge accuracy
- Historical market vs. forecast comparison

---

**Built with:** discord.py • aiohttp • scipy • Open-Meteo • Polymarket Gamma
