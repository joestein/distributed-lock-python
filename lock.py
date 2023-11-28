import threading
import uuid
import time
from botocore.exceptions import ClientError
from decimal import Decimal
import sys
import logging

logger = logging.getLogger(__name__)

class Client():
    def __init__(self, boto3_client, lock_table_name, owner_name):
        self.boto3_client = boto3_client
        self.table_name = lock_table_name
        self.table = boto3_client.Table(lock_table_name)
        self.owner_name = owner_name

class LockedItem():
    def __init__(self, record_version_number, timeout_update):
        self.record_version_number = record_version_number
        self.progress = 1 #1 - acquired and 0 is released
        self.lock = threading.Lock()
        self.timeout_update = timeout_update
        
    
class Lock():
    def __init__(self, client, idle_timeout, timeout_update_heartbeat, partition_key, sort_key=None):
        if idle_timeout < 2 * timeout_update_heartbeat:
            raise Exception("idle_timeout needs to be at least twice as much as timeout_update_heartbeat")
        
        self.client = client
        self.idle_timeout = idle_timeout
        self.timeout_update = timeout_update_heartbeat
        self.partition_key = partition_key
        self.sort_key = sort_key

        #does the key exist? If not we want to create it
        if self.sort_key:
            self.key = {
                "PK": f"{self.partition_key}",
                "SK": f"{self.sort_key}"
            }
            self.condition = "attribute_not_exists(PK) AND attribute_not_exists(SK)"
        else:
            self.key = {
                "PK": f"{self.partition_key}",
            }
            self.condition = "attribute_not_exists(PK)"

        record_version_number = self.make_id()
        additional_items = {
            "idle_timeout": Decimal(idle_timeout),
            "record_version_number": record_version_number,
            "owner_name": self.client.owner_name
        }

        item = {}
        item.update(self.key)
        item.update(additional_items)
        
        try:
            self.client.table.put_item(TableName=self.client.table_name, Item=item, ConditionExpression=self.condition)
        except ClientError as e:  
            if e.response['Error']['Code']=='ConditionalCheckFailedException':  
                #this was for initialization only so we would expect this all the time after the first time.
                pass
            else:
                raise e
        
        self.record_version_number = record_version_number

    def make_id(self):
        return str(uuid.uuid4())
    
    def acquire(self):

        def get_item():
            db_item_lock = self.client.table.get_item(Key=self.key, ConsistentRead=True)
            db_item_lock_item = db_item_lock["Item"]
            existing_record_version_number = db_item_lock_item["record_version_number"]
            idle_timeout = db_item_lock_item["idle_timeout"]
            return existing_record_version_number, idle_timeout
        
        new_record_version_number = self.make_id()

        #def acquire_db_lock(idle_timeout):
        has_acquired_lock = False
        while not has_acquired_lock:
            previous_existing_record_version_number, idle_timeout = get_item()
            time.sleep(float(idle_timeout))
            existing_record_version_number, idle_timeout = get_item()
            if previous_existing_record_version_number == existing_record_version_number:
                try:
                    self.client.table.update_item(
                            Key=self.key,
                            ConditionExpression=f"attribute_exists(PK) AND attribute_exists(SK) AND record_version_number = :rrn_existing",
                            UpdateExpression=f"set record_version_number = :rrn_new",
                            ExpressionAttributeValues={":rrn_new": new_record_version_number,
                                                    ":rrn_existing": existing_record_version_number},
                            ReturnValues="UPDATED_NEW"
                        )
                    has_acquired_lock = True
                except ClientError as e:  
                    if e.response['Error']['Code']=='ConditionalCheckFailedException':  
                        #ok we do nothing now for time in idle_timeout to wait
                        pass
                    else:
                        raise e
            
        #ok, we have a lock now
        def update_idle_lock(locked_item):
            has_released_lock = False
            
            while not has_released_lock:
                locked_item.lock.acquire()
                progress = locked_item.progress
                timeout = float(locked_item.timeout_update)
                existing_record_version_number = locked_item.record_version_number
                locked_item.lock.release()
                
                if progress == 1: #acquired
                    #update the record version to hold the lock
                    new_record_version_number = self.make_id()
                    try:
                        self.client.table.update_item(
                                Key=self.key,
                                ConditionExpression=f"attribute_exists(PK) AND attribute_exists(SK) AND record_version_number = :rrn_existing",
                                UpdateExpression=f"set record_version_number = :rrn_new",
                                ExpressionAttributeValues={":rrn_new": new_record_version_number,
                                                        ":rrn_existing": existing_record_version_number},
                                ReturnValues="UPDATED_NEW"
                            )
                    except ClientError as e:  
                        if e.response['Error']['Code']=='ConditionalCheckFailedException':  
                            #this should never happen
                            raise Exception("someone else stole our lock")
                        else:
                            raise e
                    
                    locked_item.lock.acquire()
                    locked_item.record_version_number = new_record_version_number
                    locked_item.lock.release()
                    time.sleep(timeout)
                else:
                    has_released_lock = True
        
        self.locked_item = LockedItem(record_version_number=new_record_version_number, timeout_update=self.timeout_update)

        #start thread so while we are working and the lock is acquired we can update it so no one else gets it
        x = threading.Thread(target=update_idle_lock, args=(self.locked_item,))
        x.start()
        return has_acquired_lock

    def release(self):
        self.locked_item.lock.acquire()
        self.locked_item.progress = 0
        self.locked_item.lock.release()

