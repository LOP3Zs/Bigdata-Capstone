import time
import json
import logging
from kafka import KafkaProducer
from realtime_stock_crawler import SYMBOLS, get_data, format_data

TOPIC_NAME = "crypto_candles"
BOOTSTRAP_SERVERS = ["localhost:9092"]

def streaming_data(duration_sec: int = 120):
    start = time.time()
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, max_block_ms=5000)
    while True:
        if time.time() > start + duration_sec:
            break
        try:
            for sym in SYMBOLS:
                raw = get_data(sym)
                if not raw:
                    continue
                msg = format_data(sym, raw)
                producer.send(TOPIC_NAME, value=json.dumps(msg).encode("utf-8"))
                print(f"[Producing] {sym} -> {TOPIC_NAME}: {msg}")
            time.sleep(1)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(1)
            continue

if __name__ == "__main__":
    streaming_data(duration_sec=120)
