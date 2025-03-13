import datetime
import exchanges.binance as binance
import exchanges.coinbase as coinbase
import exchanges.kucoin as kucoin
import exchanges.okx as okx
import exchanges.bybit as bybit
import exchanges.bingx as bingx
import exchanges.gateio as gateio
import exchanges.huobi as huobi
import exchanges.bitstamp as bitstamp
import exchanges.mexc as mexc
import exchanges.kraken as kraken
import exchanges.cryptocom as cryptocom
import data_processing

FIAT_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "HKD", "SGD"}
STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "GUSD", "UST", "FDUSD"}

class CryptoCandleFetcher:
    """ Class for fetching candlestick data from various exchanges (Binance, Coinbase, etc.) """
    REQUIRED_TIMEFRAMES = {
        "5m": 5 * 60,
        "15m": 15 * 60,
        "30m": 30 * 60,
        "1h": 60 * 60,
        "2h": 2 * 60 * 60,
        "4h": 4 * 60 * 60,
        "8h": 8 * 60 * 60,
        "12h": 12 * 60 * 60,
        "1d": 24 * 60 * 60,
        "3d": 3 * 24 * 60 * 60,
        "1w": 7 * 24 * 60 * 60
    }

    SUPPORTED_EXCHANGES = {
        "binance": binance.fetch_candles,
        "coinbase": coinbase.fetch_candles,
        "kucoin": kucoin.fetch_candles,
        "okx": okx.fetch_candles,
        "bybit": bybit.fetch_candles,
        "bingx": bingx.fetch_candles,
        "gateio": gateio.fetch_candles,
        "huobi": huobi.fetch_candles,
        "bitstamp": bitstamp.fetch_candles,
        "mexc": mexc.fetch_candles,
        "kraken": kraken.fetch_candles,
        "crypto.com": cryptocom.fetch_candles
    }

    def __init__(self, exchange: str, csv_save=False, json_save=False, Debug=False):
        """ Initialize the fetcher with the selected exchange """
        if exchange.lower() not in self.SUPPORTED_EXCHANGES:
            raise ValueError(f"Exchange {exchange} is not supported. Supported exchanges: {list(self.SUPPORTED_EXCHANGES.keys())}")
        self.exchange = exchange.lower()
        self.csv_save = csv_save
        self.json_save = json_save
        self.Debug = Debug

    def _convert_pair_format(self, pair: str):
        """ Convert the currency pair format to match each exchange's requirements """
        stablecoins = {"USDT", "TUSD", "USDC", "BUSD", "DAI", "UST", "GUSD"}
        if "/" not in pair:
            return pair  # Already in correct format
        pair_mappings = {
            "BTC": "XBT",  # Kraken uses XBT instead of BTC
        }
        base, quote = pair.split("/")
        if self.exchange in {"binance", "bybit", "mexc"}:
            return f"{base}{quote}"  # BTC/USDT → BTCUSDT
        elif self.exchange == "coinbase":
            if quote in stablecoins:
                quote = "USD"  
            return f"{base}-{quote}"  # BTC/USDT → BTC-USD
        elif self.exchange in {"kucoin", "okx", "bingx"}:
            return f"{base}-{quote}"  # BTC/USDT → BTC-USDT
        elif self.exchange in {"gateio", "crypto.com"}:
            return f"{base}_{quote}"  # BTC/USDT → BTC_USDT
        elif self.exchange in {"huobi", "bitstamp"}:
            return f"{base}{quote}".lower()  # BTC/USDT → btcusdt
        elif self.exchange == "kraken":
            base, quote = pair.split("/")
            base = pair_mappings.get(base, base)  # Replace BTC with XBT for Kraken
            return f"{base}{quote}"  # BTC/USDT → XBTUSDT
        else:
            return pair 

    def fetch_candles(self, pair: str, interval: str, period: int):
        """ Fetch candlestick data from the selected exchange """
        if self.exchange not in self.SUPPORTED_EXCHANGES:
            raise ValueError(f"Exchange {self.exchange} is not supported. Supported exchanges: {list(self.SUPPORTED_EXCHANGES.keys())}")

        formatted_pair = self._convert_pair_format(pair)
        fetch_func = self.SUPPORTED_EXCHANGES[self.exchange]
        return fetch_func(self, formatted_pair, interval, period)

# Example usage (for testing)
if __name__ == "__main__":
    gateio_fetcher = CryptoCandleFetcher("coinbase", csv_save=True, Debug=True)
    df_gateio = gateio_fetcher.fetch_candles(pair="BTC/USDT", interval="8h", period=366)
    print("Data saved")
