from lock import Lock, Client
import boto3
import threading
import time

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
        time.sleep(0.7)
        print(f"in worker {index}")
        our_lock.acquire()
        time.sleep(0.7)
        our_lock.release()
        

lock_attempt = our_first_lock.acquire()
# try to aquire the lock
if lock_attempt:
    print("we got our lock")
    our_first_lock.release()
else:
    print("no lock aqcquired need to wait")

x = []
for i in range(5):
    x.append(threading.Thread(target=worker, args=(our_first_lock, i,)))

for i in range(5):
    time.sleep(0.2)
    x[i].start()

#for i in range(5):
#    x[i].join()


