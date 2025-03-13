import requests
import pandas as pd
import time
from data_processing import DataProcessor

TIMEFRAMES = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": None,  
    "4h": None,  
    "8h": None,
    "12h": 43200,
    "1d": 86400,
    "3d": None,
    "1w": None
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    processor = DataProcessor(time_unit="s", debug=fetcher.Debug)
    """ Fetch OHLCV data from Bitstamp with proper time-based pagination (no duplicates). """

    if interval not in TIMEFRAMES:
        raise ValueError(f"Interval {interval} is not supported by Bitstamp.")

    if TIMEFRAMES[interval] is None:
        # Find the closest smaller timeframe for aggregation
        base_interval = max(
            [key for key in TIMEFRAMES.keys() if key in fetcher.REQUIRED_TIMEFRAMES and fetcher.REQUIRED_TIMEFRAMES[key] < fetcher.REQUIRED_TIMEFRAMES[interval] and TIMEFRAMES[key] is not None],
            key=lambda x: fetcher.REQUIRED_TIMEFRAMES[x]
        )
        processor.log_message(f"⚠️ Aggregating {base_interval} candles to form {interval} candles.")

        # Fetch smaller timeframe data
        df_base = fetch_candles(fetcher, pair, base_interval, period * (fetcher.REQUIRED_TIMEFRAMES[interval] // fetcher.REQUIRED_TIMEFRAMES[base_interval]))

        # Aggregate smaller candles into the requested timeframe
        df = processor.aggregate_candles(df_base, interval, base_interval, fetcher.REQUIRED_TIMEFRAMES)
    
    else:
        base_url = f"https://www.bitstamp.net/api/v2/ohlc/{pair.replace('/', '').lower()}/"

        # Calculate the exact number of candles required
        required_candles = period * (86400 // TIMEFRAMES[interval])
        processor.log_message(f"🔄 Fetching {required_candles} candles for {interval} from Bitstamp.")

        all_data = []
        oldest_timestamp = int(time.time())  # Start from current timestamp
        request_count = 0

        while required_candles > 0:
            candle_limit = min(required_candles, 1000)  # Max 1000 candles per request

            params = {
                "step": TIMEFRAMES[interval],
                "limit": candle_limit
            }

            processor.log_message(f"🔄 Request {request_count + 1}: Fetching {candle_limit} candles before {oldest_timestamp}...")
            try:
                response = requests.get(base_url, params=params)
                if response.status_code != 200:
                    processor.log_message(f"❌ Error fetching Bitstamp data: {response.json()}")
                    break

                data = response.json().get("data", {}).get("ohlc", [])
                if not data:
                    processor.log_message("⚠️ No data returned from Bitstamp API.")
                    break

                # Extract only required columns
                clean_data = [[int(row["timestamp"]), float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]), float(row["volume"])] for row in data]

                # Sort & filter duplicates
                clean_data.sort(reverse=True, key=lambda x: x[0])  # Sort by timestamp descending
                if all_data and clean_data[-1][0] >= all_data[-1][0]:  
                    processor.log_message("⚠️ Duplicate data detected, stopping pagination.")
                    break  # Avoid infinite loop due to duplicate candles

                all_data.extend(clean_data)
                oldest_timestamp = clean_data[-1][0]  # Move the request back in time
                required_candles -= len(clean_data)
                request_count += 1

                if len(clean_data) < 1000:
                    processor.log_message("✅ All available data has been fetched.")
                    break
                if required_candles > 1000:
                    processor.log_message("⚠️  Bitstamp does not support more than 1000 candles but All available data has been fetched.")
                    break
            except requests.exceptions.Timeout:
                processor.log_message("⏳ درخواست به API Bitstamp زمان زیادی برد! تلاش مجدد پس از 5 ثانیه...")
                time.sleep(5)
                continue  

        if not all_data:
            processor.log_message("⚠️ No data fetched after multiple attempts!")
            return None

        # Convert to DataFrame
        df = pd.DataFrame(all_data, columns=["timestamp", "open", "high", "low", "close", "volume"])

        # Trim excess data
        df = df.sort_values(by="timestamp").iloc[-(period * (86400 // TIMEFRAMES[interval])):]

        # Normalize timestamp format
        df = processor.normalize_candle_df(df)

    # Ensure data is sorted by timestamp
    df = df.sort_values(by="timestamp")

    # Save the final DataFrame
    return processor.save_dataframe(df, "bitstamp", pair, interval, fetcher.csv_save, fetcher.json_save)
