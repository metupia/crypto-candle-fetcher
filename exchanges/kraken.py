import requests
import pandas as pd
import datetime
import time
from data_processing import DataProcessor


TIMEFRAMES = {
    "5m": "5", "15m": "15", "30m": "30", "1h": "60",
    "2h": None, "4h": "240", "8h": None, "12h": None,
    "1d": "1440", "3d": None, "1w": None
}

# Number of required candles per day based on the timeframe
CANDLES_PER_DAY = {
    "5m": 288, "15m": 96, "30m": 48, "1h": 24, "2h": 12,
    "4h": 6, "8h": 3, "12h": 2, "1d": 1, "3d": 1/3, "1w": 1/7
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    """ Fetch candlestick data from Kraken with correct `since` conversion """

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
        df = df.sort_values(by="timestamp")
        processor.save_dataframe(df, "kraken", pair, interval, fetcher.csv_save, fetcher.json_save)

    else:
        base_url = "https://api.kraken.com/0/public/OHLC"

        # Calculate the number of candles needed based on the timeframe
        size = int(period * CANDLES_PER_DAY[interval])

        # Kraken only provides the last 720 candles, so we limit the request and log a warning
        if size > 720:
            processor.log_message(f"⚠️ Kraken only provides the last 720 candles. Requested {size} candles, but only 720 will be retrieved.")
            size = 720

        # Calculate `since` timestamp based on the requested number of candles
        since_timestamp = int((datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=int(size * int(TIMEFRAMES[interval])))).timestamp())

        params = {
            "pair": pair.replace("/", ""),  # Convert BTC/USDT → XBTUSD
            "interval": TIMEFRAMES[interval],
            "since": since_timestamp
        }

        processor.log_message(f"🔄 Requesting {size} candles ({period} days with timeframe {interval}) from Kraken...")

        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json().get("result", {}).get(fetcher._convert_pair_format(pair), [])
            if not data:
                processor.log_message("⚠️ No data received!")
                return None

            processor.log_message(f"✅ Retrieved {len(data)} new candles")

            clean_data = [[row[0], row[1], row[2], row[3], row[4], row[6]] for row in data]
            df = pd.DataFrame(clean_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"]), unit="s", utc=True)
            df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

        except requests.exceptions.Timeout:
            processor.log_message("⏳ API request to Kraken took too long! Retrying in 5 seconds...")
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

        if not df.empty:
            df = df.sort_values(by="timestamp")
            return processor.save_dataframe(df, "kraken", pair, interval, fetcher.csv_save, fetcher.json_save)

        return None
