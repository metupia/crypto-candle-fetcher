import requests
import pandas as pd
from data_processing import DataProcessor

TIMEFRAMES = {
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1d",
    "3d": None,  
    "1w": "7d"
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    """ Fetch candlestick data from BingX with pagination handling """

    processor = DataProcessor(time_unit="ms", debug=fetcher.Debug)

    if period > 365:
        processor.log_message("⚠️ BingX only supports data from the last year.")
        period = 365
        
    if TIMEFRAMES[interval] is None:
        # If the timeframe is not directly supported, select a smaller timeframe and aggregate the candles.
        base_interval = max(
            [key for key in TIMEFRAMES.keys() if fetcher.REQUIRED_TIMEFRAMES[key] < fetcher.REQUIRED_TIMEFRAMES[interval] and TIMEFRAMES[key] is not None],
            key=lambda x: fetcher.REQUIRED_TIMEFRAMES[x]
        )
        df_base = fetch_candles(fetcher, pair, base_interval, period)
        df = processor.aggregate_candles(df_base, interval, base_interval, fetcher.REQUIRED_TIMEFRAMES)
    
    else:
        base_url = "https://open-api.bingx.com/openApi/spot/v1/market/kline"
        
        # Retrieve time range from DataProcessor class
        start_time, end_time = processor.get_time_range(period)
        start_time*=1000
        end_time*=1000
        all_data = []
        current_start_time = start_time
        request_count = 0  # Request counter to monitor the number of requests

        while current_start_time < end_time:
            request_count += 1
            processor.log_message(f"🔄 Request {request_count}: Fetching data from {current_start_time} to {end_time}")

            params = {
                "symbol": pair,
                "interval": TIMEFRAMES[interval],
                "startTime": current_start_time,
                "limit": 200  # Fetch a maximum of 200 candles per request
            }
            
            response = requests.get(base_url, params=params)
            if response.status_code != 200:
                processor.log_message(f"❌ Error fetching data from BingX: {response.json()}")
                break
            
            data = response.json().get("data", [])
            if not data:
                processor.log_message("⚠️ Data retrieval complete. No more data available.")
                break  # Exit the loop if no more data is available
            
            processor.log_message(f"✅ Retrieved {len(data)} new candles")

            # Append new data to the list
            all_data.extend(data)

            # Update `current_start_time` based on the last received candle
            last_timestamp = int(data[-1][0])  # Timestamp of the last received candle
            current_start_time = last_timestamp + 1  # Increase by 1ms to prevent duplicate data
            
            # If fewer than 200 candles were retrieved, we have reached the end of the data
            if len(data) < 200:
                processor.log_message("✅ All data has been retrieved.")
                break

        if not all_data:
            processor.log_message("⚠️ No data received!")
            return None
        
        # Extract necessary data fields
        all_data = [[row[0], row[1], row[2], row[3], row[4], row[5]] for row in all_data]
        df = pd.DataFrame(all_data, columns=["timestamp", "open", "high", "low", "close", "volume"])

        # Process data using `normalize_candle_df` function
        df = processor.normalize_candle_df(df)
    
    # Sort data by timestamp
    df = df.sort_values(by="timestamp")
    
    # Save the data using `save_dataframe` function
    return processor.save_dataframe(df, "bingx", pair, interval, fetcher.csv_save, fetcher.json_save)
