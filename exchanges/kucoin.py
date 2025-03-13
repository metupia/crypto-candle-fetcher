import requests
import pandas as pd
import datetime
import time
from data_processing import DataProcessor

TIMEFRAMES = {
    "5m": "5min", "15m": "15min", "30m": None,  
    "1h": "1hour", "2h": "2hour", "4h": "4hour", "8h": "8hour",
    "12h": "12hour", "1d": "1day", "3d": None, "1w": "1week"
}

# Number of required candles per day based on the timeframe
CANDLES_PER_DAY = {
    "5m": 288, "15m": 96, "30m": 48, "1h": 24, "2h": 12,
    "4h": 6, "8h": 3, "12h": 2, "1d": 1, "3d": 1/3, "1w": 1/7
}

def convert_interval_to_minutes(interval):
    """Convert `interval` to minutes for calculating `startAt`"""
    if interval is None:
        return None  # Return as is if `None`
    elif "min" in interval:
        return int(interval.replace("min", ""))
    elif "hour" in interval:
        return int(interval.replace("hour", "")) * 60
    elif "day" in interval:
        return int(interval.replace("day", "")) * 1440  # 24 * 60
    elif "week" in interval:
        return int(interval.replace("week", "")) * 10080  # 7 * 24 * 60
    else:
        raise ValueError(f"Invalid timeframe format: {interval}")

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    """ Fetch candlestick data from KuCoin, handling `pagination` and converting `startAt` """
    
    processor = DataProcessor(time_unit="s", debug=fetcher.Debug)
    
    if interval not in CANDLES_PER_DAY:
        processor.log_message(f"❌ Timeframe {interval} is not supported.")
        return None

    # If timeframe is `None`, use the closest smaller available timeframe
    if TIMEFRAMES[interval] is None:
        base_interval = max(
            [key for key in TIMEFRAMES.keys() if 
             TIMEFRAMES[key] is not None and fetcher.REQUIRED_TIMEFRAMES[key] < fetcher.REQUIRED_TIMEFRAMES[interval]],
            key=lambda x: fetcher.REQUIRED_TIMEFRAMES[x]
        )

        processor.log_message(f"⚠️ Timeframe {interval} is not directly supported, using {base_interval} and aggregating data...")

        df_base = fetch_candles(fetcher, pair, base_interval, period)
        if df_base is None:
            return None

        df = processor.aggregate_candles(df_base, interval, base_interval, fetcher.REQUIRED_TIMEFRAMES)
        return processor.save_dataframe(df, "kucoin", pair, interval, fetcher.csv_save, fetcher.json_save)

    base_url = "https://api.kucoin.com/api/v1/market/candles"

    # Calculate the total number of candles needed based on timeframe
    total_candles_needed = int(period * CANDLES_PER_DAY[interval])

    # KuCoin supports a maximum of 1500 candles per request, so we must use `pagination`
    max_candles_per_request = 1500
    num_requests = (total_candles_needed // max_candles_per_request) + 1

    all_candles = []

    # `endAt` is always `now`
    endAt = int(datetime.datetime.now(datetime.UTC).timestamp())

    # Convert timeframe to minutes
    interval_minutes = convert_interval_to_minutes(TIMEFRAMES[interval])

    for i in range(num_requests):
        candles_to_fetch = min(max_candles_per_request, total_candles_needed - len(all_candles))
        if candles_to_fetch <= 0:
            break

        # Calculate `startAt` based on the required number of candles
        startAt = endAt - (candles_to_fetch * interval_minutes * 60)

        params = {
            "symbol": pair.replace("/", "-"),  # Convert BTC/USDT → BTC-USDT
            "type": TIMEFRAMES[interval],
            "startAt": startAt,
            "endAt": endAt
        }

        processor.log_message(f"🔄 Requesting {candles_to_fetch} candles from KuCoin from {startAt} to {endAt}...")

        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json().get("data", [])
            if not data:
                processor.log_message("⚠️ No data received!")
                break

            processor.log_message(f"✅ Received {len(data)} new candles")

            for row in data:
                timestamp = int(row[0])
                open_price = float(row[1])
                close_price = float(row[2])
                high_price = float(row[3])
                low_price = float(row[4])
                volume = float(row[5])
                turnover = float(row[6])  

                # Adjust trade volume if necessary
                if turnover > 0 and volume > 0:
                    volume = turnover / close_price

                all_candles.append([timestamp, open_price, high_price, low_price, close_price, volume])

            # Update `endAt` to fetch older data
            endAt = startAt

            if len(data) < candles_to_fetch:
                processor.log_message("✅ All available data has been retrieved.")
                break

        except requests.exceptions.Timeout:
            processor.log_message("⏳ API request to KuCoin took too long! Retrying in 5 seconds...")
            time.sleep(5)
            continue  

        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:
                processor.log_message("⚠️ Request limit reached (Too Many Requests). Pausing for 10 seconds...")
                time.sleep(10)
                continue  
            processor.log_message(f"❌ HTTP error: {http_err}")
            return None  

        except requests.exceptions.RequestException as err:
            processor.log_message(f"❌ General request error: {err}")
            return None  

        except ValueError as json_err:
            processor.log_message(f"⚠️ Invalid JSON error: {json_err}")
            return None  

    if not all_candles:
        return None

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    df = processor.normalize_candle_df(df)
    df = df.sort_values(by="timestamp")
    
    return processor.save_dataframe(df, "kucoin", pair, interval, fetcher.csv_save, fetcher.json_save)
