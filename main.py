from lock import Lock, Client
import boto3
import threading
import time
import random
import logging
import sys

logger = logging.getLogger(__name__)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

client = Client(boto3_client=boto3.resource("dynamodb", region_name="us-east-2"), lock_table_name="distributed-locks", owner_name="testing_localhost")

idle_timeout = 1.5
timeout_update_heartbeat = 0.5
partition_key = "testing"
sort_key = "abcde#123456"

our_first_lock = Lock(
    client=client, 
    idle_timeout=idle_timeout, 
    timeout_update_heartbeat=timeout_update_heartbeat, 
    partition_key=partition_key, 
    sort_key=sort_key
    )

def worker(our_lock, index):
    for r in range(3):
        time.sleep(random.random())
        logger.info(f"waiting on acquire worker {index}")
        our_lock.acquire()
        logger.info(f"locked worker {index}")
        time.sleep(random.random())
        our_lock.release()
        logger.info(f"released worker {index}")

x = []
for i in range(5):
    x.append(threading.Thread(target=worker, args=(our_first_lock, i,)))

for i in range(5):
    time.sleep(random.random())
    x[i].start()


