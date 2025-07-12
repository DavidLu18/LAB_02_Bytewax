import json
import os
import time
from datetime import datetime
from typing import Dict, Any, Union, Optional, List
from bytewax.connectors.kafka import KafkaSource, KafkaSinkMessage, KafkaSink, KafkaSourceMessage, KafkaError
from bytewax.dataflow import Dataflow
import bytewax.operators as op

STOCK_OHLC_PATTERN = "plaintext/quotes/krx/mdds/v2/ohlc"
STOCK_TOP_PRICE_PATTERN = "plaintext/quotes/krx/mdds/topprice"
STOCK_TICK_PATTERN = "plaintext/quotes/krx/mdds/tick"
STOCK_INFO_PATTERN = "plaintext/quotes/krx/mdds/stockinfo"
STOCK_MARKET_INDEX_PATTERN = "plaintext/quotes/krx/mdds/index"
STOCK_BOARD_EVENT_PATTERN = "plaintext/quotes/krx/mdds/boardevent"

def get_dnse_time_s_to_i(time_str: str) -> int:
    """Convert DNSE time string to Unix timestamp"""
    try:
        return int(datetime.fromisoformat(time_str.replace('Z', '+00:00')).timestamp())
    except Exception:
        return int(time.time())

def get_dnse_time_i_to_i(timestamp: int) -> int:
    """Convert DNSE timestamp to Unix timestamp"""
    return timestamp

def transform_stock_info(message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform stock info message similar to dnse_filter.rb"""
    try:
        symbol = message_data.get("symbol", "")
        msg_time = get_dnse_time_s_to_i(message_data.get('tradingTime', ''))
        stock_info = {
            "time": msg_time,
            "symbol": symbol,
            "open": float(message_data.get("openPrice", 0)),
            "high": float(message_data.get("highestPrice", 0)),
            "low": float(message_data.get("lowestPrice", 0)),
            "close": float(message_data.get("closePrice", 0)),
            "volume": float(message_data.get("totalVolumeTraded", 0)),
            "avg": float(message_data.get("averagePrice", 0)),
            "ceil": float(message_data.get("highLimitPrice", 0)),
            "floor": float(message_data.get("lowLimitPrice", 0)),
            "prior": float(message_data.get("referencePrice", 0)),
            "data_type": "SI"
        }
        price_board = {
            "time": msg_time,
            "symbol": symbol,
            "price": float(message_data.get("matchPrice", 0)),
            "vol": float(message_data.get("matchQuantity", 0)),
            "total_vol": float(message_data.get("totalVolumeTraded", 0)),
            "total_val": float(message_data.get("grossTradeAmount", 0)),
            "change": float(message_data.get("changedValue", 0)),
            "change_pct": float(message_data.get("changedRatio", 0)),
            "data_type": "PB"
        }
        
        return [stock_info, price_board]
    except Exception as e:
        print(f"Error transforming stock info: {e}")
        return []

def transform_tick(message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform tick message"""
    try:
        side = 'U'  
        if message_data.get("side") == "SIDE_SELL":
            side = 'S'
        elif message_data.get("side") == "SIDE_BUY":
            side = 'B'
            
        return [{
            "time": get_dnse_time_s_to_i(message_data.get('sendingTime', '')),
            "symbol": message_data.get("symbol", ""),
            "price": float(message_data.get("matchPrice", 0)),
            "vol": float(message_data.get("matchQtty", 0)),
            "side": side,
            "data_type": "ST"
        }]
    except Exception as e:
        print(f"Error transforming tick: {e}")
        return []

def transform_top_price(message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform top price message"""
    try:
        symbol = message_data.get("symbol", "")
        bid_ask_size = 10 if symbol.startswith("VN30F") else 3
        bp = []
        bq = []
        if message_data.get("bid"):
            for i in range(bid_ask_size):
                if i < len(message_data["bid"]) and message_data["bid"][i]:
                    bp.append(float(message_data["bid"][i].get("price", 0)))
                    bq.append(float(message_data["bid"][i].get("qtty", 0)))
        ap = []
        aq = []
        if message_data.get("offer"):
            for i in range(bid_ask_size):
                if i < len(message_data["offer"]) and message_data["offer"][i]:
                    ap.append(float(message_data["offer"][i].get("price", 0)))
                    aq.append(float(message_data["offer"][i].get("qtty", 0)))
        
        return [{
            "time": get_dnse_time_s_to_i(message_data.get('sendingTime', '')),
            "symbol": symbol,
            "bp": bp,
            "bq": bq,
            "ap": ap,
            "aq": aq,
            "total_bid": float(message_data.get('totalOfferQtty', 0)),
            "total_ask": float(message_data.get('totalBidQtty', 0)),
            "data_type": "TP"
        }]
    except Exception as e:
        print(f"Error transforming top price: {e}")
        return []

def transform_func(msg: KafkaSourceMessage) -> Optional[List[Dict[str, Any]]]:
    """Transform DNSE message based on key pattern"""
    if msg is None:
        return None
    
    try:
        dnse_message = json.loads(msg.value.decode('utf-8'))
        key = dnse_message.get("key", "")
        value = dnse_message.get("value", {})
        if STOCK_INFO_PATTERN in key:
            return transform_stock_info(value)
        elif STOCK_TICK_PATTERN in key:
            return transform_tick(value)
        elif STOCK_TOP_PRICE_PATTERN in key:
            return transform_top_price(value)
        elif STOCK_OHLC_PATTERN in key:
            resolution = value.get('resolution', 'MIN')
            return [{
                "time": get_dnse_time_i_to_i(value.get('time', 0)),
                "symbol": value.get("symbol", ""),
                "resolution": resolution,
                "open": float(value.get("open", 0)),
                "high": float(value.get("high", 0)),
                "low": float(value.get("low", 0)),
                "close": float(value.get("close", 0)),
                "volume": float(value.get("volume", 0)),
                "updated": get_dnse_time_i_to_i(value.get("lastUpdated", 0)),
                "data_type": "OH"
            }]
        else:
            if value.get("symbol"):
                return [{
                    "time": get_dnse_time_s_to_i(value.get('tradingTime', '')),
                    "symbol": value.get("symbol", ""),
                    "raw_data": value,
                    "data_type": "RAW"
                }]
            return None
            
    except Exception as e:
        print(f"Error parsing DNSE message: {e}")
        return None

def flatten_messages(messages: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Flatten list of messages"""
    if messages is None:
        return []
    return messages

def to_kafka_message(data: Dict[str, Any]) -> Optional[KafkaSinkMessage]:
    """Convert transformed data to KafkaSinkMessage"""
    if data is None:
        return None
    
    key = data.get("symbol", "unknown")
    return KafkaSinkMessage(
        key=key.encode('utf-8'), 
        value=json.dumps(data).encode('utf-8')
    )

brokers = os.environ.get('KAFKA_SERVERS', 'localhost:9092').split(',')  
flow = Dataflow("dnse-transform")
kinp = op.input("kafka-in", flow, KafkaSource(brokers, ["dnse.raw"]))
transformed = op.map("transform", kinp, transform_func)
flattened = op.flat_map("flatten", transformed, flatten_messages)
filtered = op.filter("filter-errors", flattened, lambda x: x is not None)
processed = op.map("to-kafka-msg", filtered, to_kafka_message)
final_filtered = op.filter("filter-none-messages", processed, lambda x: x is not None)
op.output("kafka-out", final_filtered, KafkaSink(brokers, "dnse.transform")) 