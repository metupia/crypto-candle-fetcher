import requests
import pandas as pd
import time
from data_processing import DataProcessor


TIMEFRAMES = {
    "5m": "5min", "15m": "15min", "30m": "30min", "1h": "60min",
    "2h": None, "4h": "4hour", "8h": None, "12h": None,
    "1d": "1day", "3d": None, "1w": "1week"
}

# Number of candles per day based on the timeframe
CANDLES_PER_DAY = {
    "5m": 288,  # 24h * 60m / 5m
    "15m": 96,  # 24h * 60m / 15m
    "30m": 48,  # 24h * 60m / 30m
    "1h": 24,  # 24h / 1h
    "2h": 12,  # 24h / 2h
    "4h": 6,   # 24h / 4h
    "8h": 3,   # 24h / 8h
    "12h": 2,  # 24h / 12h
    "1d": 1,   # 1 day = 1 candle
    "3d": 1/3,  # 3 days = 1 candle
    "1w": 1/7   # 7 days = 1 candle
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    """ Fetch candlestick data from Huobi without `from` and convert `size` based on timeframe """

    processor = DataProcessor(time_unit="s", debug=fetcher.Debug)

    if interval not in CANDLES_PER_DAY:
        processor.log_message(f"❌ Timeframe {interval} is not supported.")
        return None

    if TIMEFRAMES[interval] is None:
        base_interval = max(
            [key for key in TIMEFRAMES.keys() if fetcher.REQUIRED_TIMEFRAMES[key] < fetcher.REQUIRED_TIMEFRAMES[interval] and TIMEFRAMES[key] is not None],
            key=lambda x: fetcher.REQUIRED_TIMEFRAMES[x]
        )
        df_base = fetch_candles(fetcher, pair, base_interval, period)
        df = processor.aggregate_candles(df_base, interval, base_interval, fetcher.REQUIRED_TIMEFRAMES)

    else:
        base_url = "https://api.huobi.pro/market/history/kline"

        # Calculate the required number of candles based on timeframe
        size = int(period * CANDLES_PER_DAY[interval])

        # If `size` exceeds 2000, limit the value and log a warning
        if size > 2000:
            processor.log_message(f"⚠️ Huobi only provides the last 2000 candles. Requested {size} candles, but only 2000 will be retrieved.")
            size = 2000

        params = {
            "symbol": pair.replace("/", "").lower(),  # Convert BTC/USDT → btcusdt
            "period": TIMEFRAMES[interval],
            "size": size
        }

        candles = []

        processor.log_message(f"🔄 Requesting {size} candles ({period} days with timeframe {interval}) from Huobi...")

        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json().get("data", [])
            if not data:
                processor.log_message("⚠️ No data received!")
                return None

            processor.log_message(f"✅ Retrieved {len(data)} new candles")

            for row in data:
                timestamp = int(row["id"])
                open_price = float(row["open"])
                high_price = float(row["high"])
                low_price = float(row["low"])
                close_price = float(row["close"])
                volume = float(row["amount"])  

                candles.append([timestamp, open_price, high_price, low_price, close_price, volume])

        except requests.exceptions.Timeout:
            processor.log_message("⏳ API request to Huobi took too long! Retrying in 5 seconds...")
            time.sleep(5)
            return None

        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:
                processor.log_message("⚠️ Request limit reached (Too Many Requests). Pausing for 10 seconds...")
                time.sleep(10)
                return None
            processor.log_message(f"❌ HTTP error: {http_err}")
            return None  

        except requests.exceptions.RequestException as err:
            processor.log_message(f"❌ General request error: {err}")
            return None  

        except ValueError as json_err:
            processor.log_message(f"⚠️ Invalid JSON error: {json_err}")
            return None  

        if not candles:
            return None

        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df = processor.normalize_candle_df(df)

    df = df.sort_values(by="timestamp")
    return processor.save_dataframe(df, "huobi", pair, interval, fetcher.csv_save, fetcher.json_save)
