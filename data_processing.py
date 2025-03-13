import pandas as pd
import datetime

class DataProcessor:
    """ Data processing class for handling candlestick data transformation and storage """

    def __init__(self, time_unit: str = "ms", debug: bool = False):
        """
        Initialize the class.
        :param time_unit: Default time unit (ms = milliseconds, s = seconds)
        :param debug: If True, processing logs will be displayed.
        """
        self.time_unit = time_unit
        self.Debug = debug  

    def get_time_range(self, period: int) -> tuple:
        """
        Calculate the time range for exchange API requests.
        :param period: Number of days to fetch data for
        :return: `(start_time, end_time)` in the specified time unit
        """
        now = datetime.datetime.now(datetime.UTC)
        start_time = now - datetime.timedelta(days=period)

        return int(start_time.timestamp()), int(now.timestamp())

    def normalize_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert timestamps to a standardized datetime format for all exchanges.
        :param df: DataFrame containing a `timestamp` column.
        :return: Updated DataFrame
        """
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"]), unit=self.time_unit, utc=True)
        return df

    def log_message(self, message: str):
        """
        Log messages if debugging is enabled.
        :param message: Log message
        """
        if self.Debug:
            print(f"[LOG] {message}")

    def normalize_candle_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize a candlestick DataFrame by converting timestamps to datetime and 
        casting OHLCV columns to float.
        :param df: DataFrame with columns ["timestamp", "open", "high", "low", "close", "volume"]
        :return: Normalized DataFrame with timestamp as datetime (UTC) and other columns as float.
        """

        # Ensure timestamp is numeric and drop NaN values
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])  # Remove rows with NaN timestamps

        # Debugging logs
        self.log_message(f"🔎 Min timestamp before conversion: {df['timestamp'].min()}")
        self.log_message(f"🔎 Max timestamp before conversion: {df['timestamp'].max()}")

        # Detect and fix timestamp unit
        if df["timestamp"].max() > 1e15:  # If timestamp is in nanoseconds
            self.log_message("⚠️  Fixing timestamp unit: Converting from ns to ms.")
            df["timestamp"] = df["timestamp"] // 1_000_000  # Convert ns to ms
        elif df["timestamp"].max() < 1e10:  # If timestamp is in seconds
            self.log_message("⚠️  Fixing timestamp unit: Converting from s to ms.")
            df["timestamp"] = df["timestamp"] * 1000  # Convert seconds to milliseconds

        # Debugging logs after conversion
        self.log_message(f"🔎 Min timestamp after conversion: {df['timestamp'].min()}")
        self.log_message(f"🔎 Max timestamp after conversion: {df['timestamp'].max()}")

        # Define valid timestamp range
        min_timestamp = 946684800000  # 2000-01-01 in milliseconds
        max_timestamp = int(pd.Timestamp.now().timestamp() * 1000) + (2 * 365 * 24 * 3600 * 1000)  # Allow 2 years future data

        # Filter out invalid timestamps
        df = df[(df["timestamp"] >= min_timestamp) & (df["timestamp"] <= max_timestamp)]

        if df.empty:
            self.log_message("❌ Error: No valid timestamps left. Returning empty DataFrame.")
            return df  # Return empty DataFrame to prevent further errors

        # Convert timestamp to datetime (UTC)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True, errors="coerce")

        # Convert OHLCV columns to float
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].astype(float)

        return df


    def aggregate_candles(self, df, target_interval, base_interval, required_timeframes):
        """
        Aggregate smaller candlesticks into larger ones.
        :param df: Original candlestick DataFrame
        :param target_interval: Desired timeframe
        :param base_interval: The smaller timeframe to aggregate from
        :param required_timeframes: Dictionary containing numerical values for each timeframe
        :return: Aggregated candlestick DataFrame
        """
        factor = required_timeframes[target_interval] // required_timeframes[base_interval]
        df["group"] = df.index // factor
        df_agg = df.groupby("group").agg({
            "timestamp": "first",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).reset_index(drop=True)
        return df_agg

    def save_dataframe(self, df, exchange, pair, interval, csv_save=False, json_save=False):
        """
        Save the DataFrame to CSV or JSON files.
        :param df: Candlestick DataFrame
        :param exchange: Exchange name
        :param pair: Trading pair (e.g., BTC/USDT)
        :param interval: Timeframe
        :param csv_save: If True, data is saved as a CSV file
        :param json_save: If True, data is saved as a JSON file
        :return: Saved DataFrame
        """
        filename = f"{exchange}_{pair.replace('-', '').lower()}_{interval}"
        if csv_save:
            df.to_csv(f"data/{filename}.csv", index=False)
        if json_save:
            df.to_json(f"data/{filename}.json", orient="records", date_format="iso")
        return df
