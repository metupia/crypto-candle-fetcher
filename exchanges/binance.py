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
    "3d": "3d",
    "1w": "1w"
}

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    processor = DataProcessor(time_unit="ms", debug=fetcher.Debug)
    """ دریافت داده‌های کندل از Binance """
    base_url = "https://api.binance.com/api/v3/klines"
    
    # دریافت بازه زمانی از کلاس DataProcessor
    start_time, end_time = processor.get_time_range(period)
    
    params = {"symbol": pair, "interval": interval, "startTime": start_time, "endTime": end_time, "limit": 1000}
    candles = []
    
    while True:
        processor.log_message(f"🔄 درخواست دریافت داده از {params['startTime']} تا {params['endTime']}")
        
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            processor.log_message(f"❌ خطا در دریافت داده از Binance: {response.json()}")
            break
        
        data = response.json()
        if not data:
            processor.log_message("⚠️ دریافت داده به پایان رسید. داده‌ای موجود نیست.")
            break
        
        processor.log_message(f"✅ دریافت {len(data)} کندل جدید")
        
        # افزودن داده‌های جدید
        candles.extend(data)
        
        # به‌روزرسانی `startTime` برای دریافت داده‌های بیشتر
        params["startTime"] = data[-1][0] + 1  
        
        # اگر تعداد داده‌های دریافتی کمتر از 1000 باشد، یعنی به انتهای داده‌ها رسیده‌ایم
        if len(data) < 1000:
            processor.log_message("✅ تمام داده‌ها دریافت شدند.")
            break
    
    if not candles:
        processor.log_message("⚠️ هیچ داده‌ای دریافت نشد!")
        return None

    # تبدیل داده‌ها به DataFrame
    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume",
                                        "close_time", "quote_asset_volume", "trades",
                                        "taker_buy_base", "taker_buy_quote", "ignore"])
    
    # فقط ستون‌های ضروری را نگه داریم
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    
    # استفاده از تابع normalize_candle_df برای پردازش داده‌ها
    df = processor.normalize_candle_df(df)
    
    # مرتب‌سازی داده‌ها بر اساس timestamp
    df = df.sort_values(by="timestamp")
    
    # ذخیره‌سازی با تابع save_dataframe
    return processor.save_dataframe(df, "binance", pair, interval, fetcher.csv_save, fetcher.json_save)
