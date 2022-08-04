import json
import websocket
import logging
import os
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


token = os.getenv("INFLUX_TOKEN")
org = "aticio-org"
bucket = "tickview"

def main():
    init_stream()


# Websocket functions
def init_stream():
    w_s = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/ws/!bookTicker",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
        )
    w_s.on_open = on_open
    w_s.run_forever()


def on_error(w_s, error):
    logging.error(error)


def on_close(w_s):
    logging.info("closing websocket connection, initiating again...")
    init_stream()


def on_open(w_s):
    logging.info("websocket connection opened...")


def on_message(w_s, message):
    book_ticker_data = json.loads(message)
    if "BUSD" in book_ticker_data['s'] and "USDT" not in book_ticker_data['s']:
        sell_side = float(book_ticker_data['b']) * float(book_ticker_data['B'])
        buy_side = float(book_ticker_data['a']) * float(book_ticker_data['A'])
        delta = buy_side - sell_side
        print(delta)
    # sequence = []
    # for t in ticker_data:
    #     if "BUSD" in t["s"]:
    #         sequence.append(f"tick,symbol={t['s']} price={t['c']},volume={t['v']}")

    # with InfluxDBClient(url="http://localhost:8086", token=token, org=org) as client:
    #     write_api = client.write_api(write_options=SYNCHRONOUS)
    #     write_api.write(bucket, org, sequence)
    #     client.close()


if __name__ == "__main__":
    main()