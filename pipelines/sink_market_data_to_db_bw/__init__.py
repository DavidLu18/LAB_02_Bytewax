# pipelines/sink_market_data_to_db_bw/__init__.py
import json
import logging
from typing import Union, Optional, Any
from bytewax.connectors.kafka import KafkaSource, KafkaSourceMessage, KafkaError
from bytewax.dataflow import Dataflow
import bytewax.operators as bop
from pipelines import config
from pipelines.sink_market_data_to_db_bw.db_sink import OHLCVSink

KAFKA_TOPIC = "dnse.transform"

def parse_dnse_message(msg: Union[KafkaSourceMessage, KafkaError]) -> Optional[Any]:
    """Parse DNSE transformed message từ topic dnse.transform"""
    if isinstance(msg, KafkaError):
        logging.error(f"Kafka error: {msg}")
        return None
        
    try:
        data = json.loads(msg.value.decode('utf-8'))
        data_type = data.get("data_type", "")
        
        if data_type in ["SI", "PB", "OH", "ST", "TP", "RAW"]:
            if "time" in data and isinstance(data["time"], int):
                if data["time"] > 1000000000000:  
                    data["time"] = data["time"] / 1000.0
            return data
        else:
            logging.warning(f"Unknown data type: {data_type} - {data}")
            return None
            
    except Exception as e:
        logging.error(f"Failed to parse DNSE message: {e}")
        return None

logging.info("Starting DNSE PostgreSQL Sink Pipeline...")
flow = Dataflow("dnse-postgresql-sink")
kafka_source = KafkaSource([config.KAFKA_SERVERS], [KAFKA_TOPIC])
input_stream = bop.input("kafka-in", flow, kafka_source)
parsed = bop.map("parse-dnse", input_stream, parse_dnse_message)
filtered = bop.filter("filter-valid-dnse", parsed, lambda x: x is not None)
bop.output("postgresql-sink", filtered, OHLCVSink())
logging.info(f"Pipeline ready - consuming from: {KAFKA_TOPIC}")
logging.info("Target table: Multiple tables based on data_type")
