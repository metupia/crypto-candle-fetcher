# Crypto Candle Fetcher

A Python library for fetching OHLC (candlestick) data from multiple cryptocurrency exchanges.
By Metupia Group

## Features
- Supports multiple exchanges: Binance, Coinbase, KuCoin, OKX, Bybit, BingX, Gate.io, Huobi, Bitstamp, MEXC, Kraken, Crypto.com.
- Saves data in CSV and JSON formats.
- Normalizes and processes candlestick data.
- Provides easy-to-use API methods for fetching and handling OHLCV data.
- Supports multiple timeframes: `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `3d`, `1w`.
- Automatically converts timestamps to standard UTC datetime format.
- Compatible with Python 3.7+.

## Installation
Install the library using pip:
```sh
pip install crypto-candle-fetcher
```

## Usage

### 1. Initialize the Fetcher
```python
from crypto_candle_fetcher import CryptoCandleFetcher

fetcher = CryptoCandleFetcher(exchange="binance", csv_save=True, json_save=False, Debug=True)
```

### 2. Fetch Candlestick Data
```python
data = fetcher.fetch_candles(pair="BTC/USDT", interval="1h", period=30)
```

## Complete Example
```python
from crypto_candle_fetcher import CryptoCandleFetcher

fetcher = CryptoCandleFetcher(exchange="binance", csv_save=True, json_save=False, Debug=True)
data = fetcher.fetch_candles(pair="BTC/USDT", interval="1h", period=30)
```

## License
This project is licensed under the MIT License.
