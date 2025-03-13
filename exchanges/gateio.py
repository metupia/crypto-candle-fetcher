import requests
import pandas as pd
import time
from data_processing import DataProcessor

TIMEFRAMES = {
    "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h",
    "2h": None, "4h": "4h", "8h": None, "12h": "12h",
    "1d": "1d", "3d": None, "1w": "7d"
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    """ Fetch candlestick data from Gate.io and convert volume from USDT to Base Asset """
    processor = DataProcessor(time_unit="s", debug=fetcher.Debug)

    if TIMEFRAMES[interval] is None:
        base_interval = max(
            [key for key in TIMEFRAMES.keys() if fetcher.REQUIRED_TIMEFRAMES[key] < fetcher.REQUIRED_TIMEFRAMES[interval] and TIMEFRAMES[key] is not None],
            key=lambda x: fetcher.REQUIRED_TIMEFRAMES[x]
        )
        df_base = fetch_candles(fetcher, pair, base_interval, period)
        df = processor.aggregate_candles(df_base, interval, base_interval, fetcher.REQUIRED_TIMEFRAMES)
    
    else:
        base_url = "https://api.gateio.ws/api/v4/spot/candlesticks"
        start_time, end_time = processor.get_time_range(period)

        params = {
            "currency_pair": pair.replace("/", "_"),  # Convert BTC/USDT → BTC_USDT
            "interval": TIMEFRAMES[interval],
            "limit": 1000,
            "from": start_time
        }

        candles = []
        last_timestamp = 0  

        while True:
            processor.log_message(f"🔄 Requesting data from {params['from']} to {end_time}")

            try:
                response = requests.get(base_url, params=params, timeout=10)  # Added `timeout`
                response.raise_for_status()  # Check for successful response (200)

                data = response.json()
                if not data:
                    processor.log_message("⚠️ No data received!")
                    break

                processor.log_message(f"✅ Received {len(data)} new candles")

                for row in data:
                    timestamp = int(row[0])
                    open_price = float(row[5])
                    high_price = float(row[3])
                    low_price = float(row[4])
                    close_price = float(row[2])
                    volume_usdt = float(row[1])

                    volume_base = volume_usdt / close_price if close_price > 0 else 0
                    candles.append([timestamp, open_price, high_price, low_price, close_price, volume_base])

                    last_timestamp = timestamp

                if last_timestamp == params["from"]:
                    processor.log_message("⚠️ Duplicate data detected, stopping further requests.")
                    break

                params["from"] = last_timestamp + 1

                if len(data) < 1000:
                    processor.log_message("✅ All data has been retrieved.")
                    break

            except requests.exceptions.Timeout:
                processor.log_message("⏳ API request to Gate.io took too long! Retrying in 5 seconds...")
                time.sleep(5)
                continue  # Retry request

            except requests.exceptions.HTTPError as http_err:
                if response.status_code == 429:
                    processor.log_message("⚠️ Request limit reached (Too Many Requests). Pausing for 10 seconds...")
                    time.sleep(10)
                    continue  # Retry request
                processor.log_message(f"❌ HTTP error: {http_err}")
                return None  # Stop on serious error

            except requests.exceptions.RequestException as err:
                processor.log_message(f"❌ General request error: {err}")
                return None  # Stop on network error

            except ValueError as json_err:
                processor.log_message(f"⚠️ Invalid JSON error: {json_err}")
                return None  # Stop on JSON conversion error

        if not candles:
            return None

        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df = processor.normalize_candle_df(df)

    df = df.sort_values(by="timestamp")
    return processor.save_dataframe(df, "gateio", pair, interval, fetcher.csv_save, fetcher.json_save)
