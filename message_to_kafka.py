import json
import os
import hashlib
import time
from confluent_kafka import Producer
import redis 

kafka_config = {
    'bootstrap.servers': os.environ.get('KAFKA_SERVERS', 'localhost:9092')
}
producer = Producer(kafka_config)
try:
    dragonfly_host = os.environ.get('DRAGONFLY_HOST', 'dragonfly')
    dragonfly_port = int(os.environ.get('DRAGONFLY_PORT', '6379'))
    db_client = redis.Redis(host=dragonfly_host, port=dragonfly_port, db=0, decode_responses=True)
    db_client.ping()
    print("✅ Successfully connected to DragonflyDB.")
except redis.ConnectionError as e:
    print(f"❌ Could not connect to DragonflyDB: {e}")
    print("Please ensure the DragonflyDB container is running.")
    exit(1) 
REDIS_CHECKSUM_SET_KEY = "dnse:processed_checksums"

def calculate_checksum(message: dict) -> str:
    key_payload = message.get("key", {}).get("payload", "")
    value_payload = message.get("value", {}).get("payload", {})
    business_data = {
        "key": key_payload,
        "value": value_payload
    }
    message_str = json.dumps(business_data, sort_keys=True)
    return hashlib.md5(message_str.encode('utf-8')).hexdigest()


def send_to_kafka(message: dict) -> bool:
    try:
        checksum = calculate_checksum(message)
        if db_client.sismember(REDIS_CHECKSUM_SET_KEY, checksum):
            return False 
        db_client.sadd(REDIS_CHECKSUM_SET_KEY, checksum)
        key = message.get("key", {}).get("payload", "")
        value = message.get("value", {}).get("payload", {})
        kafka_message = {
            "key": key,
            "value": value,
            "checksum": checksum, 
            "timestamp": message.get("timestamp", int(time.time() * 1000))
        }
        
        topic = "dnse.raw"
        producer.produce(
            topic=topic, 
            key=key.encode('utf-8'), 
            value=json.dumps(kafka_message).encode('utf-8') 
        )
        producer.flush() 
        return True
        
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        return False


def process_messages_from_file(file_path: str = "messages.json"):
    """
    Hàm chính để đọc và xử lý các message từ file JSON.
    """
    try:
        print(f"🔄 Reading messages from {file_path}...")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            messages = json.load(f)
        
        print(f"📊 Found {len(messages)} messages to process.")
        
        processed_count = 0
        duplicate_count = 0
        
        for message in messages:
            if send_to_kafka(message):
                processed_count += 1
            else:
                duplicate_count += 1

        print("\n" + "="*30)
        print("✅ Processing complete!")
        print(f"📊 New messages published to Kafka: {processed_count}")
        print(f"⚠️  Duplicates skipped: {duplicate_count}")
        total_checksums = db_client.scard(REDIS_CHECKSUM_SET_KEY)
        print(f"🔍 Total unique checksums in DragonflyDB: {total_checksums}")
        print("="*30)
        
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error in file {file_path}: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")


if __name__ == "__main__":
    try:
        print("🚀 Starting DNSE message processor...")
        print("📂 Data source: messages.json")
        print("📨 Kafka topic: dnse.raw")
        print("🔐 Checksum validation: enabled (using DragonflyDB)")
        
        process_messages_from_file()
        
        print("\n💤 Script finished. All messages have been processed.")
    
    except KeyboardInterrupt:
        print("\n⏹️  Process stopped by user.")
    finally:
        print("🧹 Cleanup completed.")

