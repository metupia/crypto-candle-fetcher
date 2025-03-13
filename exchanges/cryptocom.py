import requests
import pandas as pd
import datetime
from data_processing import DataProcessor

TIMEFRAMES = {
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "8h": None,  
    "12h": "12h",
    "1d": "1D",
    "3d": None,
    "1w": "7D"
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    """ Fetch OHLCV data from Crypto.com with proper pagination and duplicate filtering. """
    processor = DataProcessor(time_unit="ms", debug=fetcher.Debug)

    # Convert pair format for Crypto.com (BTC/USDT → BTC_USDT)
    pair = pair.replace("/", "_")

    if TIMEFRAMES[interval] is None:
        # If timeframe is not directly supported, use a smaller available timeframe
        base_interval = max(
            [key for key in TIMEFRAMES.keys() if fetcher.REQUIRED_TIMEFRAMES[key] < fetcher.REQUIRED_TIMEFRAMES[interval] and TIMEFRAMES[key] is not None],
            key=lambda x: fetcher.REQUIRED_TIMEFRAMES[x]
        )
        processor.log_message(f"⚠️ Aggregating {base_interval} candles to form {interval} candles.")

        # Fetch smaller timeframe data
        df_base = fetch_candles(fetcher, pair, base_interval, period)

        if df_base is None or df_base.empty:
            processor.log_message(f"❌ Error: No data available for {pair} in {base_interval} timeframe.")
            return None

        # Aggregate smaller candles into the requested timeframe
        df = processor.aggregate_candles(df_base, interval, base_interval, fetcher.REQUIRED_TIMEFRAMES)
    
    else:
        base_url = "https://api.crypto.com/v2/public/get-candlestick"
        
        # Get time range using DataProcessor
        start_time, end_time = processor.get_time_range(period)

        processor.log_message(f"🔍 Fetching data from {start_time} to {end_time} for {pair} ({interval} timeframe)")

        all_data = set()  # Using a set to avoid duplicate entries
        last_candle_time = None  # Store last candle's timestamp to prevent duplicates
        request_count = 0
        interval_seconds = fetcher.REQUIRED_TIMEFRAMES[interval]
        total_candles = (period * 24 * 3600) // interval_seconds  # Total candles needed
        count = min(total_candles, 300)
        while start_time < end_time:
            request_count += 1

            if request_count >1:
                processor.log_message(f"⚠️ No more data available from Crypto.com. (only last 300 candle is available)")
                break


            processor.log_message(f"🔄 Request {request_count}: Fetching candles from {start_time} to {end_time}...")

            params = {
                "instrument_name": pair,
                "timeframe": TIMEFRAMES[interval],
                "count": count,  # Max candles per request
                "start_time": start_time,
                "end_time": end_time
            }

            response = requests.get(base_url, params=params)
            if response.status_code != 200:
                processor.log_message(f"❌ Error fetching Crypto.com data: {response.json()}")
                break

            data = response.json().get("result", {}).get("data", [])
            if not data:
                processor.log_message("⚠️ No more data available from Crypto.com.")
                break

            processor.log_message(f"✅ Retrieved {len(data)} candles.")

            # Process and store unique candles
            for row in data:
                if row["t"] != last_candle_time:  # Avoid duplicate entries
                    all_data.add((row["t"], row["o"], row["h"], row["l"], row["c"], row["v"]))
            last_candle_time = data[-1]["t"]  # Store last timestamp to prevent duplicates

            # Update start_time for next request
            start_time = data[-1]["t"]
            
            if len(data) < 1000:
                if total_candles > 300:
                    processor.log_message(f"⚠️ (only last 300 candle is available)")
                else:
                    processor.log_message(f"✅ {count} candles fetched.")
                processor.log_message("✅ All available data saved.")
                break

        if not all_data:
            processor.log_message("⚠️ No data fetched after multiple attempts!")
            return None

        # Convert the collected data into DataFrame
        df = pd.DataFrame(sorted(all_data, key=lambda x: x[0]), columns=["timestamp", "open", "high", "low", "close", "volume"])
        df = df.tail(count)
        
        if df.empty:
            processor.log_message(f"❌ Error: No valid data fetched for {pair} ({interval})")
            return None

        # Normalize timestamps and numerical values
        df = processor.normalize_candle_df(df)

    # Ensure data is sorted by timestamp
    df = df.sort_values(by="timestamp")

    # Save the final DataFrame
    return processor.save_dataframe(df, "crypto.com", pair, interval, fetcher.csv_save, fetcher.json_save)
