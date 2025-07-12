import json
import os
import hashlib
import time
from confluent_kafka import Producer, Consumer, KafkaError
from typing import Set

kafka_config = {
    'bootstrap.servers': os.environ.get('KAFKA_SERVERS', 'localhost:9092')
}
producer = Producer(kafka_config)
processed_checksums: Set[str] = set()

def calculate_checksum(message: dict) -> str:
    """Calculate MD5 checksum for a message to detect duplicates"""
    key_payload = message.get("key", {}).get("payload", "")
    value_payload = message.get("value", {}).get("payload", {})
    business_data = {
        "key": key_payload,
        "value": value_payload
    }
    
    message_str = json.dumps(business_data, sort_keys=True)
    return hashlib.md5(message_str.encode('utf-8')).hexdigest()

def load_existing_checksums_from_kafka(topic: str = "dnse.raw") -> None:
    """Load checksums from existing messages in Kafka topic to avoid republishing"""
    global processed_checksums
    
    try:
        print(f"🔍 Checking existing messages in topic '{topic}'...")
        consumer_config = {
            'bootstrap.servers': os.environ.get('KAFKA_SERVERS', 'localhost:9092'),
            'group.id': f'checksum_loader_{int(time.time())}', 
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 60000,  
            'max.poll.interval.ms': 300000  
        }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        
        existing_count = 0
        timeout = 30.0 
        
        while True:
            msg = consumer.poll(timeout=timeout)
            
            if msg is None:
                # No more messages
                break
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    break
                else:
                    print(f"⚠️ Consumer error: {msg.error()}")
                    break
            try:
                kafka_message = json.loads(msg.value().decode('utf-8'))
                checksum = kafka_message.get('checksum')
                
                if checksum:
                    processed_checksums.add(checksum)
                    existing_count += 1
                    
            except Exception as e:
                print(f"⚠️ Error parsing existing message: {e}")
                continue
        
        consumer.close()
        print(f"📊 Loaded {existing_count} existing checksums from topic '{topic}'")
        
    except Exception as e:
        print(f"⚠️ Error loading existing checksums: {e}")
        print("🔄 Continuing with empty checksum set...")

def send_to_kafka(message: dict):
    """Send message to kafka topic 'dnse.raw' with checksum validation"""
    try:
        # Calculate checksum
        checksum = calculate_checksum(message)
        
        # Skip if already processed
        if checksum in processed_checksums:
            print(f"⚠️  Skipping duplicate message with checksum: {checksum}")
            return
        processed_checksums.add(checksum)
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
            key=key,
            value=json.dumps(kafka_message)
        )
        producer.flush()
        print(f"📤 Published message to topic '{topic}' - Symbol: {value.get('symbol', 'N/A')}")
        
    except Exception as e:
        print(f"❌ Error processing message: {e}")

def process_messages_from_file(file_path: str = "messages.json"):
    """Process messages from JSON file"""
    try:
        print(f"🔄 Reading messages from {file_path}...")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            messages = json.load(f)
        
        print(f"📊 Found {len(messages)} messages to process")
        
        processed_count = 0
        duplicate_count = 0
        
        for message in messages:
            initial_checksum_count = len(processed_checksums)
            send_to_kafka(message)
            if len(processed_checksums) > initial_checksum_count:
                processed_count += 1
            else:
                duplicate_count += 1
                
        print(f"✅ Processing complete!")
        print(f"📊 Processed: {processed_count} messages")
        print(f"⚠️  Duplicates skipped: {duplicate_count} messages")
        print(f"🔍 Total unique checksums: {len(processed_checksums)}")
        
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    try:
        print("🚀 Starting DNSE message processor...")
        print("📂 Data source: messages.json")
        print("📨 Kafka topic: dnse.raw")
        print("🔐 Checksum validation: enabled")
        load_existing_checksums_from_kafka()
        process_messages_from_file()
        print("💤 Processing complete. Waiting indefinitely to prevent restart...")
        while True:
            time.sleep(60)  
    except KeyboardInterrupt:
        print("\n⏹️  Stopping...")
    finally:
        print("🧹 Cleanup completed")
