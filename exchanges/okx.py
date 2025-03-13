import requests
import pandas as pd
import datetime
import time
from pytz import timezone
from data_processing import DataProcessor

# تنظیم منطقه زمانی
HKT = timezone("Asia/Hong_Kong")  # منطقه زمانی هنگ‌کنگ
UTC = timezone("UTC")  # منطقه زمانی UTC

# ایجاد نمونه از DataProcessor


# نگاشت تایم‌فریم‌ها به فرمت OKX
TIMEFRAMES = {
    "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1H", "2h": "2H", "4h": "4H", "8h": None,  
    "12h": "12H", "1d": "1D", "3d": "3D", "1w": "1W"
}

# مقدار ساعت معادل هر تایم‌فریم
TIMEFRAMES_HOURS = {
    "5m": 0.0833, "15m": 0.25, "30m": 0.5, "1h": 1, "2h": 2, "4h": 4, "8h": 8,
    "12h": 12, "1d": 24, "3d": 72, "1w": 168
}

MAX_CANDLES = 1000  # محدودیت دریافت کندل در هر درخواست API

def fetch_candles(fetcher, pair: str, interval: str, period: int):
    processor = DataProcessor(time_unit="ms", debug=fetcher.Debug)
    """
    دریافت داده‌های کندل از OKX با مقدار دقیق درخواستی.
    """
    if interval not in TIMEFRAMES:
        processor.log_message(f"❌ تایم‌فریم {interval} پشتیبانی نمی‌شود.")
        return None

    if TIMEFRAMES[interval] is None:
        base_interval = max(
            [key for key in TIMEFRAMES.keys() if TIMEFRAMES[key] is not None and 
             fetcher.REQUIRED_TIMEFRAMES[key] < fetcher.REQUIRED_TIMEFRAMES[interval]],
            key=lambda x: fetcher.REQUIRED_TIMEFRAMES[x]
        )
        processor.log_message(f"⚠️ تایم‌فریم {interval} مستقیماً پشتیبانی نمی‌شود، استفاده از {base_interval} و تجمیع داده‌ها...")
        df_base = fetch_candles(fetcher, pair, base_interval, period)
        if df_base is None:
            return None
        df = processor.aggregate_candles(df_base, interval, base_interval, fetcher.REQUIRED_TIMEFRAMES)
        df = df.sort_values(by="timestamp")
        return processor.save_dataframe(df, "okx", pair, interval, fetcher.csv_save, fetcher.json_save)

    hours_per_candle = TIMEFRAMES_HOURS[interval]
    required_candles = min(int((period * 24) / hours_per_candle), MAX_CANDLES)
    base_url = "https://www.okx.com/api/v5/market/history-candles"

    # محاسبه زمان شروع `after` بر اساس HKT
    now_utc = datetime.datetime.now(UTC)  # دریافت زمان فعلی UTC
    now_hkt = now_utc.astimezone(HKT)  # تبدیل UTC به HKT
    after_hkt = now_hkt - datetime.timedelta(days=period)  # کم کردن دوره مورد نظر
    after = int(after_hkt.timestamp() * 1000)  # تبدیل به میلی‌ثانیه
    all_candles = []
    last_after = None
    request_count = 0

    while len(all_candles) < required_candles:
        request_count += 1
        if request_count == 1:
            base_url = "https://www.okx.com/api/v5/market/candles"
            params = {
                "instId": pair.replace("/", "-"),  
                "bar": TIMEFRAMES[interval],
            }
        else:
            params = {
                "instId": pair.replace("/", "-"),  
                "bar": TIMEFRAMES[interval],
                "after": after  
            }
        processor.log_message(f"🔄 دریافت کندل از after={after} ...")

        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            result = response.json()

            if result.get("code") != "0":
                processor.log_message(f"❌ خطای API OKX: {result.get('msg', 'No message')}")
                break

            data = result.get("data", [])
            if not data:
                processor.log_message("⚠️ داده‌ای دریافت نشد. احتمالاً after خیلی قدیمی است.")
                break

            processor.log_message(f"✅ دریافت {len(data)} کندل جدید")
            for row in data:
                timestamp = int(row[0])  
                open_price = float(row[1])
                high_price = float(row[2])
                low_price = float(row[3])
                close_price = float(row[4])
                volume = float(row[5])
                all_candles.append([timestamp, open_price, high_price, low_price, close_price, volume])
                
                if len(all_candles) >= required_candles:
                    processor.log_message(f"✅ تعداد مورد نیاز ({required_candles}) کندل دریافت شد. متوقف می‌شود.")
                    break  

            new_after = int(data[-1][0]) - 1  
            if last_after == new_after:
                processor.log_message("⚠️ مقدار after تغییر نکرد، توقف جلوگیری از لوپ بی‌نهایت.")
                break

            last_after = new_after  
            after = new_after  
            time.sleep(0.1)  

        except requests.exceptions.Timeout:
            processor.log_message("⏳ درخواست به API OKX زمان زیادی برد! تلاش مجدد پس از 5 ثانیه...")
            time.sleep(5)
            continue  

        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:
                processor.log_message("⚠️ محدودیت درخواست‌ها فعال شد. توقف 10 ثانیه‌ای...")
                time.sleep(10)
                continue  
            processor.log_message(f"❌ خطای HTTP: {http_err}")
            return None  

        except requests.exceptions.RequestException as err:
            processor.log_message(f"❌ خطای عمومی در درخواست: {err}")
            return None  

        except ValueError as json_err:
            processor.log_message(f"⚠️ خطای JSON نامعتبر: {json_err}")
            return None  

    if not all_candles:
        return None

    df = pd.DataFrame(all_candles[:required_candles], columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = processor.normalize_candle_df(df)
    df = df.sort_values(by="timestamp")

    return processor.save_dataframe(df, "okx", pair, interval, fetcher.csv_save, fetcher.json_save)
