import requests
import pandas as pd
import datetime
import time
from data_processing import DataProcessor

TIMEFRAMES = {
    "5m": "300",
    "15m": "900",
    "30m": None,  
    "1h": "3600",
    "2h": None,
    "4h": None,  
    "8h": None,  
    "12h": None,
    "1d": "86400",
    "3d": None,  
    "1w": None
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    """ Fetch OHLCV data from Coinbase with proper pagination and timeframe handling. """
    processor = DataProcessor(time_unit="s", debug=fetcher.Debug)
    
    # Convert pair format for Coinbase (BTC/USDT → BTC-USD)
    pair = pair.replace("/", "-")

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
        base_url = f"https://api.exchange.coinbase.com/products/{pair}/candles"
        interval_seconds = int(TIMEFRAMES[interval])

        # Get time range using DataProcessor
        start_time, end_time = processor.get_time_range(period)  

        processor.log_message(f"🔍 Fetching data from {start_time} to {end_time} for {pair} ({interval} timeframe)")

        all_data = []
        max_candles = 300  
        max_time_range = max_candles * interval_seconds  # Calculate max time range for each request
        request_count = 0

        while start_time < end_time:
            request_count += 1
            current_end_time = min(start_time + max_time_range, end_time)  

            processor.log_message(f"🔄 Request {request_count}: Fetching candles from {start_time} to {current_end_time}...")

            params = {
                "start": datetime.datetime.fromtimestamp(start_time, datetime.timezone.utc).isoformat(),
                "end": datetime.datetime.fromtimestamp(current_end_time, datetime.timezone.utc).isoformat(),
                "granularity": interval_seconds
            }

            response = requests.get(base_url, params=params)
            if response.status_code != 200:
                processor.log_message(f"❌ Error fetching Coinbase data: {response.json()}")
                break

            data = response.json()
            if not data:
                processor.log_message("⚠️ No more data available from Coinbase.")
                break

            processor.log_message(f"✅ Retrieved {len(data)} candles.")

            # Convert data to DataFrame
            df = pd.DataFrame(data, columns=["timestamp", "low", "high", "open", "close", "volume"])
            df = df[["timestamp", "open", "high", "low", "close", "volume"]]  # Reorder columns

            # Ensure timestamp is numeric
            df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")

            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True, errors="coerce")

            # Convert numeric values to float
            df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

            all_data.extend(df.values.tolist())

            # Update start time for next request
            start_time = current_end_time
            time.sleep(0.2)  # Avoid hitting rate limits

            if len(data) < max_candles:
                processor.log_message("✅ All available data fetched.")
                break

        if not all_data:
            processor.log_message("⚠️ No data fetched after multiple attempts!")
            return None

        # Convert the collected data into DataFrame
        df = pd.DataFrame(all_data, columns=["timestamp", "open", "high", "low", "close", "volume"])

        if df.empty:
            processor.log_message(f"❌ Error: No valid data fetched for {pair} ({interval})")
            return None

        # Normalize timestamps and numerical values
        df = processor.normalize_candle_df(df)

    # Ensure data is sorted by timestamp
    df = df.sort_values(by="timestamp")

    # Save the final DataFrame
    return processor.save_dataframe(df, "coinbase", pair, interval, fetcher.csv_save, fetcher.json_save)
