import requests
import pandas as pd
import datetime
import time
from data_processing import DataProcessor

TIMEFRAMES = {
    "5m": "5m", "15m": "15m", "30m": "30m", "1h": None,
    "2h": "2h", "4h": "4h", "8h": None, "12h": "12h",
    "1d": "1d", "3d": None, "1w": "1w"
}

# Number of required candles per day based on the timeframe
CANDLES_PER_DAY = {
    "5m": 288, "15m": 96, "30m": 48, "1h": 24, "2h": 12,
    "4h": 6, "8h": 3, "12h": 2, "1d": 1, "3d": 1/3, "1w": 1/7
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    """ Fetch candlestick data from MEXC using `startTime` only, without `endTime` and `limit` """

    processor = DataProcessor(time_unit="ms", debug=fetcher.Debug)

    if interval not in CANDLES_PER_DAY:
        processor.log_message(f"❌ Timeframe {interval} is not supported.")
        return None

    # If the requested timeframe is not directly supported, select a smaller one
    if TIMEFRAMES[interval] is None:
        base_interval = max(
            [key for key in TIMEFRAMES.keys() if 
             TIMEFRAMES[key] is not None and fetcher.REQUIRED_TIMEFRAMES[key] < fetcher.REQUIRED_TIMEFRAMES[interval]],
            key=lambda x: fetcher.REQUIRED_TIMEFRAMES[x]
        )

        processor.log_message(f"⚠️  Timeframe {interval} is not directly supported, using {base_interval} and aggregating data...")

        df_base = fetch_candles(fetcher, pair, base_interval, period)
        if df_base is None:
            return None

        df = processor.aggregate_candles(df_base, interval, base_interval, fetcher.REQUIRED_TIMEFRAMES)
        return processor.save_dataframe(df, "mexc", pair, interval, fetcher.csv_save, fetcher.json_save)

    base_url = "https://api.mexc.com/api/v3/klines"

    # Initial `startTime` based on the number of days specified in `period`
    startTime = int((datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=period)).timestamp() * 1000)
    endTime = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)

    all_candles = []

    while True:
        params = {
            "symbol": pair.replace("/", ""),  # Convert BTC/USDT → BTCUSDT
            "interval": TIMEFRAMES[interval],
            "startTime": startTime,  # Send only `startTime`
            "endTime": endTime
        }

        processor.log_message(f"🔄 Requesting data from {startTime} from MEXC...")

        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            if not data:
                processor.log_message("⚠️  No data received! `startTime` might be too old, or no more data is available.")
                break

            processor.log_message(f"✅ Received {len(data)} new candles")

            for row in data:
                timestamp = int(row[0])  # Open time
                open_price = float(row[1])
                high_price = float(row[2])
                low_price = float(row[3])
                close_price = float(row[4])
                volume = float(row[5])

                all_candles.append([timestamp, open_price, high_price, low_price, close_price, volume])

            # Update `startTime` to fetch newer data
            startTime = all_candles[-1][0] + 1  # New `startTime` is the `Open Time` of the last received candle

            if len(data) < 500:
                processor.log_message("✅ All available data has been retrieved.")
                break

        except requests.exceptions.Timeout:
            processor.log_message("⏳ API request to MEXC took too long! Retrying in 5 seconds...")
            time.sleep(5)
            continue  

        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:
                processor.log_message("⚠️  Request limit reached (Too Many Requests). Pausing for 10 seconds...")
                time.sleep(10)
                continue  
            processor.log_message(f"❌ HTTP error: {http_err}")
            return None  

        except requests.exceptions.RequestException as err:
            processor.log_message(f"❌ General request error: {err}")
            return None  

        except ValueError as json_err:
            processor.log_message(f"⚠️  Invalid JSON error: {json_err}")
            return None  

    if not all_candles:
        return None

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    df = processor.normalize_candle_df(df)
    df = df.sort_values(by="timestamp")
    
    return processor.save_dataframe(df, "mexc", pair, interval, fetcher.csv_save, fetcher.json_save)
