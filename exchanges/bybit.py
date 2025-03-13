import requests
import pandas as pd
import time
import datetime
from data_processing import DataProcessor

# Create an instance of DataProcessor

TIMEFRAMES = {
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "8h": None,  
    "12h": "720",
    "1d": "D",
    "3d": None,  
    "1w": "10080"
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    """ Fetch OHLCV data from Bybit using start timestamp and paginating until the latest data. """
    processor = DataProcessor(time_unit="ms", debug=fetcher.Debug)
    pair = pair.replace("/", "")  # Convert symbol to Bybit format (e.g., BTC/USDT → BTCUSDT)

    if TIMEFRAMES[interval] is None:
        base_interval = max(
            [key for key in TIMEFRAMES.keys() if fetcher.REQUIRED_TIMEFRAMES[key] < fetcher.REQUIRED_TIMEFRAMES[interval] and TIMEFRAMES[key] is not None],
            key=lambda x: fetcher.REQUIRED_TIMEFRAMES[x]
        )
        processor.log_message(f"⚠️ Aggregating {base_interval} candles to form {interval} candles.")
        df_base = fetch_candles(fetcher, pair, base_interval, period)
        if df_base is None or df_base.empty:
            processor.log_message(f"❌ Error: No data available for {pair} in {base_interval} timeframe.")
            return None
        df = processor.aggregate_candles(df_base, interval, base_interval, fetcher.REQUIRED_TIMEFRAMES)
    else:
        base_url = "https://api.bybit.com/v5/market/kline"
        
        # Calculate start_time based on period in days
        start_time = int((datetime.datetime.utcnow() - datetime.timedelta(days=period)).timestamp() * 1000)  # Oldest time in milliseconds
        processor.log_message(f"🔍 Fetching data from {start_time} until now for {pair} ({interval} timeframe)")
        
        all_data = []
        current_start_time = start_time
        request_count = 0
        
        while True:
            request_count += 1
            processor.log_message(f"🔄 Request {request_count}: Fetching candles from {current_start_time}...")
            
            params = {
                "symbol": pair,
                "interval": TIMEFRAMES[interval],
                "start": current_start_time,
                "limit": 1000  # Maximum allowed by API
            }
            
            try:
                response = requests.get(base_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json().get("result", {}).get("list", [])
                
                if not data:
                    processor.log_message("⚠️ No more data available from Bybit.")
                    break
                
                processor.log_message(f"✅ Retrieved {len(data)} candles.")
                df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
                df = df[["timestamp", "open", "high", "low", "close", "volume"]]
                df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"]), unit="ms", utc=True)
                df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
                all_data.extend(df.values.tolist())
                last_timestamp = int(data[0][0])
                if last_timestamp >= int(datetime.datetime.utcnow().timestamp() * 1000):
                    processor.log_message("✅ All requested data fetched.")
                    break
                
                current_start_time = last_timestamp + 1  # Move to the next batch
                time.sleep(0.1)  # Small delay to prevent API rate limiting
                
            except requests.exceptions.Timeout:
                processor.log_message("⏳ Timeout! Retrying in 5 seconds...")
                time.sleep(5)
                continue
            except requests.exceptions.HTTPError as http_err:
                if response.status_code == 429:
                    processor.log_message("⚠️ Rate limit hit! Waiting 10 seconds...")
                    time.sleep(10)
                    continue
                processor.log_message(f"❌ HTTP Error: {http_err}")
                return None
            except requests.exceptions.RequestException as req_err:
                processor.log_message(f"❌ Request Error: {req_err}")
                return None
            except Exception as e:
                processor.log_message(f"❌ Unexpected Error: {e}")
                return None
        
        if not all_data:
            processor.log_message("⚠️ No data fetched after multiple attempts!")
            return None
        
        df = pd.DataFrame(all_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        if df.empty:
            processor.log_message(f"❌ Error: No data fetched for {pair} ({interval})")
            return None
        df = processor.normalize_candle_df(df)
    
    df = df.sort_values(by="timestamp")
    return processor.save_dataframe(df, "bybit", pair, interval, fetcher.csv_save, fetcher.json_save)
