from collections import defaultdict
import pandas as pd
import threading
import queue
import requests
import time
import datetime

data_queue = queue.SimpleQueue()


def data_loader():
    for i in range(10):
        time.sleep(5)
        print(f'{datetime.datetime.now()} data_loader {i}')
        data_queue.put(i)
    data_queue.put(None)


def data_aggregator():
    while True:
        data = data_queue.get()
        if data is None:
            break
        time.sleep(3)
        print(f'{datetime.datetime.now()} data_aggregator {data}')


loader_thread = threading.Thread(target=data_loader)
aggregator_thread = threading.Thread(target=data_aggregator)

loader_thread.start()
aggregator_thread.start()

loader_thread.join()
aggregator_thread.join()
