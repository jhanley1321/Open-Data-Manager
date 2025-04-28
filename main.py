from binance_data import BinanceETL
def main():
    binance = BinanceETL()
    binance.get_ohlcv_clean(csv='BTC/USDT')
    # binance.get_clean_price(csv='BTC/USDT')
    # df = binance_etl.run_binance(ticker='BTCUSDT', interval='1d')
    # print(df.head())

if __name__ == "__main__":
    main()