import requests
from datetime import datetime, timezone

BINANCE_URL = "https://api.binance.com/api/v3/klines"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
INTERVAL = "1m"

def _fetch_latest_kline(symbol: str, interval: str = INTERVAL):
    try:
        resp = requests.get(
            BINANCE_URL,
            params={"symbol": symbol, "interval": interval, "limit": 1},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        return data[-1]
    except Exception:
        return None

def get_data(symbol: str):
    return _fetch_latest_kline(symbol, INTERVAL)

def format_data(symbol: str, kline):
    open_time_ms = int(kline[0])
    dt_utc = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
    return {
        "symbol": symbol,
        "interval": INTERVAL,
        "datetime": dt_utc.isoformat().replace("+00:00", "Z"),
        "epoch_ms": open_time_ms,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "volume": float(kline[5]),
    }
